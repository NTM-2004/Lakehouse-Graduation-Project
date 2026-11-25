"""
spark/gatekeeper.py - Tác vụ Spark Gatekeeper đối soát số dòng Bronze với Raw và B-108 (B-205)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Design Bronze, Silver v2: Mục V (Bước 3. Gatekeeper)
- Design Build: B-205, Mục IV (Luồng chạy một batch - Bước 4)
- Requirement v3: F03, F11, F12

Quy tắc nghiệp vụ:
- So số dòng đủ 11 bảng Bronze của batch_id với:
  (1) Số dòng thực tế đọc từ Raw trên MinIO (bỏ qua thư mục _excluded/)
  (2) Số dòng kỳ vọng trong raw_prep/batch_row_counts.csv (B-108)
- Nếu cả 11 bảng đều khớp: in rõ ROWS_IN=<tổng_dòng_bronze_batch> và thoát mã 0.
- Nếu bất kỳ bảng nào lệch: in chi tiết ra stderr và thoát mã 1 (để Airflow thử lại).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from spark.bronze_common import BATCH_BULK, VALID_BATCHES, get_spark_session

# Danh sách đủ 11 bảng Bronze: (bronze_table, counts_csv_key, source_kind, relative_file_name)
BRONZE_TABLE_SPECS: List[Tuple[str, str, str, str]] = [
    ("orders", "orders", "csv", "olist_orders_dataset.csv"),
    ("order_items", "order_items", "csv", "olist_order_items_dataset.csv"),
    ("order_payments", "order_payments", "csv", "olist_order_payments_dataset.csv"),
    ("order_reviews", "order_reviews", "csv", "olist_order_reviews_dataset.csv"),
    ("customers", "customers", "csv", "olist_customers_dataset.csv"),
    ("products", "products", "csv", "olist_products_dataset.csv"),
    ("sellers", "sellers", "csv", "olist_sellers_dataset.csv"),
    ("product_category_translation", "product_category_translation", "csv", "product_category_name_translation.csv"),
    ("geolocation", "geolocation", "csv", "olist_geolocation_dataset.csv"),
    ("marketing_leads", "marketing_leads", "jsonl", "marketing_leads.jsonl"),
    ("clickstream_events", "clickstream", "text", "access_log.txt"),
]


def _default_counts_csv_path() -> str:
    candidates = [
        os.path.join(PROJECT_ROOT, "raw_prep", "batch_row_counts.csv"),
        "/home/raw_prep/batch_row_counts.csv",
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def load_expected_counts(batch_id: str, counts_csv_path: str) -> Dict[str, int]:
    """Đọc số dòng kỳ vọng của batch_id từ raw_prep/batch_row_counts.csv (B-108)."""
    col_key = "bulk" if batch_id == BATCH_BULK else batch_id
    if not os.path.isfile(counts_csv_path):
        raise FileNotFoundError(f"Không tìm thấy tệp số dòng chuẩn B-108: {counts_csv_path}")

    expected: Dict[str, int] = {}
    with open(counts_csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tname = row["table_name"].strip()
            expected[tname] = int(row[col_key])
    return expected


def _raw_path_for_table(raw_base_dir: str, source_kind: str, batch_id: str, filename: str) -> str:
    base = raw_base_dir.rstrip("/")
    if source_kind == "csv":
        return f"{base}/csv/{batch_id}/{filename}"
    if source_kind == "jsonl":
        # Chỉ đọc thư mục batch cụ thể; luôn bỏ qua thư mục _excluded/
        return f"{base}/marketing_funnel/{batch_id}/{filename}"
    return f"{base}/clickstream/{batch_id}/{filename}"


def _path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    return bool(fs.exists(jpath))


def _count_raw_rows(spark: SparkSession, path: str, source_kind: str) -> int:
    if not _path_exists(spark, path):
        return 0
    if source_kind == "csv":
        df = (
            spark.read.option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("encoding", "UTF-8")
            .csv(path)
        )
        return df.count()
    return spark.read.text(path).count()


def _count_bronze_rows(spark: SparkSession, bronze_table: str, batch_id: str) -> int:
    full_name = f"hive.bronze.{bronze_table}"
    if not spark.catalog.tableExists(full_name):
        return -1
    row = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {full_name} WHERE batch_id = '{batch_id}'"
    ).collect()[0]
    return int(row["cnt"])


def run_gatekeeper(
    spark: SparkSession,
    batch_id: str,
    raw_base_dir: str = "s3a://lakehouse/raw",
    counts_csv_path: str | None = None,
) -> Tuple[bool, int, List[Dict[str, object]]]:
    """
    Đối soát đủ 11 bảng Bronze với Raw và batch_row_counts.csv cho batch_id.
    Trả về (all_passed, total_rows_in, details).
    """
    csv_path = counts_csv_path or _default_counts_csv_path()
    expected_map = load_expected_counts(batch_id, csv_path)

    details: List[Dict[str, object]] = []
    total_bronze_rows = 0
    all_passed = True

    print(f"=== GATEKEEPER: ĐỐI SOÁT BATCH '{batch_id}' ===")
    for bronze_table, csv_key, source_kind, filename in BRONZE_TABLE_SPECS:
        expected_rows = expected_map.get(csv_key)
        if expected_rows is None:
            raise KeyError(f"Thiếu khóa '{csv_key}' trong {csv_path}")

        raw_path = _raw_path_for_table(raw_base_dir, source_kind, batch_id, filename)
        raw_rows = _count_raw_rows(spark, raw_path, source_kind)
        bronze_rows = _count_bronze_rows(spark, bronze_table, batch_id)

        # Với bảng tĩnh ở batch tăng trưởng (expected = 0), bronze_rows phải = 0
        matched = (bronze_rows == raw_rows) and (bronze_rows == expected_rows)
        if not matched:
            all_passed = False

        if bronze_rows > 0:
            total_bronze_rows += bronze_rows

        record = {
            "table": bronze_table,
            "expected": expected_rows,
            "raw": raw_rows,
            "bronze": bronze_rows,
            "matched": matched,
        }
        details.append(record)
        status_txt = "OK" if matched else "MISMATCH"
        print(
            f"  [{status_txt:8}] {bronze_table:28} | expected={expected_rows:>10,} | "
            f"raw={raw_rows:>10,} | bronze={bronze_rows:>10,}"
        )

    return all_passed, total_bronze_rows, details


def main() -> None:
    parser = argparse.ArgumentParser(description="Gatekeeper đối soát số dòng Bronze với Raw và B-108.")
    parser.add_argument(
        "--batch_id",
        required=True,
        choices=VALID_BATCHES,
        help=f"Tên batch cần kiểm tra ({', '.join(VALID_BATCHES)}).",
    )
    parser.add_argument(
        "--raw_base_dir",
        default="s3a://lakehouse/raw",
        help="Thư mục gốc tầng Raw (mặc định: s3a://lakehouse/raw).",
    )
    parser.add_argument(
        "--counts_csv",
        default=None,
        help="Đường dẫn tới tệp batch_row_counts.csv (mặc định tự tìm B-108).",
    )
    args = parser.parse_args()

    spark = get_spark_session(app_name=f"Gatekeeper_{args.batch_id}")
    try:
        passed, rows_in, details = run_gatekeeper(
            spark,
            batch_id=args.batch_id,
            raw_base_dir=args.raw_base_dir,
            counts_csv_path=args.counts_csv,
        )
        summary = {
            "batch_id": args.batch_id,
            "status": "PASSED" if passed else "FAILED",
            "rows_in": rows_in,
            "mismatched_tables": [d["table"] for d in details if not d["matched"]],
        }
        if not passed:
            print(json.dumps(summary, ensure_ascii=False), file=sys.stderr)
            sys.exit(1)

        # In đúng chuẩn để airflow/dags/spark_runner.py đọc rows_in (F12)
        print(f"ROWS_IN={rows_in}")
        print(json.dumps(summary, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

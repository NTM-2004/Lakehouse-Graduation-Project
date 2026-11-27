"""
spark/bronze_csv.py - Tác vụ Spark nạp 9 bảng Olist CSV từ tầng Raw vào Bronze Iceberg (B-202)
Tác giả phụ trách: Kiên
Reviewer: Minh (theo phân công Design Build)
Mẫu triển khai: M-10
Tham chiếu:
- Requirement v3 (F03, F04)
- Design Bronze, Silver v2 (Mục II: Partition key; Mục III: Nguồn csv)
- Design Build (B-202, M-10)

Quy tắc nghiệp vụ:
- Đọc 9 tệp CSV từ raw/csv/<batch_id>/ trên MinIO (s3a://lakehouse/raw/csv).
- Mọi cột nguồn đọc và lưu kiểu STRING (không dùng inferSchema -> giữ nguyên số 0 đứng đầu mã bưu chính).
- Bật multiLine=true cùng quote='"' và escape='"' cho order_reviews (3.852 bình luận nhiều dòng).
- Kiểm tra và bỏ ký tự BOM (\ufeff) ở tên cột đầu cho product_category_translation (và kiểm chung).
- Giữ nguyên cột nguồn kể cả lỗi chính tả gốc ('lenght' trong products); sửa tên chỉ làm ở Silver.
- Thêm 3 cột truy vết bắt buộc qua spark/bronze_common.py (B-201): batch_id, source_file, ingested_at.
- Ghi đè đúng partition batch_id vào 9 bảng Iceberg hive.bronze.* (idempotent, F04).
- Với 5 bảng tĩnh ở batch tăng trưởng (2018-06, 2018-07, 2018-08): đảm bảo bảng Iceberg tồn tại
  và bỏ qua ghi khi không có tệp nguồn (0 dòng, không làm thay đổi partition bulk).
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, TypedDict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Thêm thư mục gốc vào sys.path để import bronze_common (B-201)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from spark.bronze_common import (
    BATCH_BULK,
    VALID_BATCHES,
    add_audit_columns,
    get_spark_session,
    write_bronze_iceberg,
)


class CsvTableSpec(TypedDict):
    table: str
    filename: str
    columns: List[str]
    multiline: bool
    static: bool


# Danh sách chính xác 9 bảng Bronze Olist theo Design Bronze, Silver v2 (Mục III: Nguồn csv)
OLIST_BRONZE_SPECS: List[CsvTableSpec] = [
    {
        "table": "orders",
        "filename": "olist_orders_dataset.csv",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "multiline": False,
        "static": False,
    },
    {
        "table": "order_items",
        "filename": "olist_order_items_dataset.csv",
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
        "multiline": False,
        "static": False,
    },
    {
        "table": "order_payments",
        "filename": "olist_order_payments_dataset.csv",
        "columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
        "multiline": False,
        "static": False,
    },
    {
        "table": "order_reviews",
        "filename": "olist_order_reviews_dataset.csv",
        "columns": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
        "multiline": True,
        "static": False,
    },
    {
        "table": "customers",
        "filename": "olist_customers_dataset.csv",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "multiline": False,
        "static": True,
    },
    {
        "table": "products",
        "filename": "olist_products_dataset.csv",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "multiline": False,
        "static": True,
    },
    {
        "table": "sellers",
        "filename": "olist_sellers_dataset.csv",
        "columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
        "multiline": False,
        "static": True,
    },
    {
        "table": "product_category_translation",
        "filename": "product_category_name_translation.csv",
        "columns": [
            "product_category_name",
            "product_category_name_english",
        ],
        "multiline": False,
        "static": True,
    },
    {
        "table": "geolocation",
        "filename": "olist_geolocation_dataset.csv",
        "columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
        "multiline": False,
        "static": True,
    },
]


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Kiểm tra tồn tại đường dẫn trên S3A/MinIO hoặc hệ thống tệp cục bộ qua Hadoop FileSystem."""
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    return bool(fs.exists(jpath))


def _ensure_bronze_table(spark: SparkSession, table_name: str, columns: List[str]) -> None:
    """Đảm bảo database hive.bronze và bảng Iceberg tương ứng (mọi cột nguồn kiểu STRING) đã tồn tại."""
    warehouse_base = os.getenv("WAREHOUSE_PATH", "s3a://lakehouse/").rstrip("/")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS hive.bronze LOCATION '{warehouse_base}/bronze'")

    col_defs = [f"{col_name} STRING" for col_name in columns] + [
        "batch_id STRING",
        "source_file STRING",
        "ingested_at TIMESTAMP",
    ]
    cols_ddl = ",\n            ".join(col_defs)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS hive.bronze.{table_name} (
            {cols_ddl}
        )
        USING iceberg
        PARTITIONED BY (batch_id)
        TBLPROPERTIES ('write.format.default'='parquet')
        """
    )


def load_single_csv_table(
    spark: SparkSession,
    spec: CsvTableSpec,
    batch_id: str,
    raw_base_dir: str,
) -> int:
    """Nạp một bảng Olist CSV của batch_id vào bảng Iceberg hive.bronze.<table>."""
    table_name = spec["table"]
    filename = spec["filename"]
    expected_cols = spec["columns"]
    raw_path = f"{raw_base_dir.rstrip('/')}/{batch_id}/{filename}"

    _ensure_bronze_table(spark, table_name=table_name, columns=expected_cols)

    if not _path_exists(spark, raw_path):
        if spec["static"] and batch_id != BATCH_BULK:
            print(
                f"  [SKIP] {table_name:28} | Bảng tĩnh không có tệp nguồn ở batch '{batch_id}' (0 dòng)"
            )
            return 0
        raise FileNotFoundError(
            f"Thiếu tệp Raw CSV bắt buộc cho bảng '{table_name}' ở batch '{batch_id}': {raw_path}"
        )

    reader = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
    )
    if spec["multiline"]:
        reader = reader.option("multiLine", "true")

    df = reader.csv(raw_path)

    # Kiểm tra và bỏ ký tự BOM (\ufeff) ở tên cột đầu (đặc biệt với product_category_name_translation.csv)
    raw_cols = df.columns
    clean_cols = [c.lstrip("\ufeff") for c in raw_cols]
    if clean_cols != raw_cols:
        print(
            f"  [BOM] Bảng {table_name}: đổi tên cột dính BOM "
            f"{raw_cols[0]!r} -> {clean_cols[0]!r}"
        )
        df = df.toDF(*clean_cols)

    if df.columns != expected_cols:
        raise ValueError(
            f"Lệch danh sách cột ở bảng '{table_name}' ({raw_path}): "
            f"thực tế={df.columns}, kỳ vọng={expected_cols}"
        )

    # Ép tường minh toàn bộ cột nguồn sang STRING để giữ nguyên định dạng (kể cả số 0 đầu mã bưu chính)
    typed_df = df.select([F.col(c).cast("string").alias(c) for c in expected_cols])
    bronze_df = add_audit_columns(typed_df, batch_id=batch_id, source_file=filename)

    # Ghi đè idempotent đúng partition batch_id (F04)
    write_bronze_iceberg(bronze_df, table_name=table_name, partition_col="batch_id")

    written_rows = (
        spark.table(f"hive.bronze.{table_name}")
        .filter(F.col("batch_id") == batch_id)
        .count()
    )
    print(
        f"  [THÀNH CÔNG] hive.bronze.{table_name:28} | batch={batch_id} | {written_rows:>10,} dòng"
    )
    return written_rows


def load_bronze_csv(
    spark: SparkSession,
    batch_id: str,
    raw_base_dir: str = "s3a://lakehouse/raw/csv",
) -> Dict[str, int]:
    """Nạp toàn bộ 9 bảng Olist CSV của batch_id vào tầng Bronze Iceberg."""
    if batch_id not in VALID_BATCHES:
        raise ValueError(f"batch_id không hợp lệ: '{batch_id}'. Chỉ chấp nhận: {VALID_BATCHES}")

    print(f"=== BẮT ĐẦU NẠP BRONZE OLIST CSV CHO BATCH: {batch_id} ===")
    print(f"Thư mục gốc Raw CSV: {raw_base_dir}")

    counts: Dict[str, int] = {}
    for spec in OLIST_BRONZE_SPECS:
        cnt = load_single_csv_table(
            spark=spark,
            spec=spec,
            batch_id=batch_id,
            raw_base_dir=raw_base_dir,
        )
        counts[spec["table"]] = cnt

    batch_total = sum(counts.values())
    print(
        f"=== HOÀN TẤT NẠP BRONZE OLIST CSV (batch={batch_id}): {batch_total:,} dòng ==="
    )
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Nạp 9 bảng Olist CSV từ Raw vào tầng Bronze Iceberg (B-202)."
    )
    parser.add_argument(
        "--batch_id",
        required=True,
        choices=VALID_BATCHES + ["all"],
        help=f"Tên batch cần nạp ({', '.join(VALID_BATCHES)}) hoặc 'all' để nạp lần lượt cả 4 batch.",
    )
    parser.add_argument(
        "--raw_base_dir",
        default="s3a://lakehouse/raw/csv",
        help="Thư mục gốc chứa Raw CSV trên MinIO (mặc định: s3a://lakehouse/raw/csv).",
    )

    args = parser.parse_args()
    spark = get_spark_session(app_name="BronzeOlistCsvLoader")

    try:
        batches_to_run = VALID_BATCHES if args.batch_id == "all" else [args.batch_id]
        grand_total = 0
        for b in batches_to_run:
            batch_counts = load_bronze_csv(
                spark=spark,
                batch_id=b,
                raw_base_dir=args.raw_base_dir,
            )
            grand_total += sum(batch_counts.values())
        print(
            f"=== TỔNG CỘNG ĐÃ NẠP BRONZE CSV: {grand_total:,} DÒNG CHO {len(batches_to_run)} BATCH ==="
        )
    except Exception as exc:
        print(f"[LỖI] {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

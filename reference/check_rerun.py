"""
reference/check_rerun.py - Kiểm thử GT-16 và F04 khi chạy lại cùng một batch (B-705)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Requirement v3: F04 (Nạp lại batch theo cách idempotent)
- Design Gold v2: GT-16 (Chạy lại cùng batch)
- Design Orchestration: Kịch bản Chạy lại cùng batch
- Design Build: B-705

Quy tắc kiểm chứng:
1. Tầng Bronze (11 bảng Iceberg):
   - Với batch chạy lại: số dòng không đổi; tổng các cột tiền không đổi
     (Olist: price, freight_value, payment_value; Clickstream: amount);
     mã băm nội dung toàn batch không đổi (loại trừ cột ingested_at).
     Riêng Marketing dùng số dòng và mã băm nội dung (chứa raw_json gốc) thay cho tổng tiền.
   - Với mọi batch khác: số dòng và mã băm nội dung (loại trừ ingested_at) hoàn toàn không đổi.
2. Tầng Gold (16 bảng theo dbt/schema_contract.yml):
   - Số dòng ở mọi bảng Gold không đổi.
   - Tổng các cột tiền ở mọi bảng Gold có cột tiền không đổi.

Cách dùng:
- Bước 1 (trước khi chạy lại batch X):
    python reference/check_rerun.py snapshot --batch 2018-06 --output reference/rerun_before.json
- Bước 2: Kích hoạt lại dag_lakehouse_batch với batch = "2018-06".
- Bước 3 (sau khi chạy lại xong):
    python reference/check_rerun.py snapshot --batch 2018-06 --output reference/rerun_after.json
- Bước 4 (đối soát GT-16 & F04):
    python reference/check_rerun.py compare --batch 2018-06 --before reference/rerun_before.json --after reference/rerun_after.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 11 bảng Bronze và các cột tiền cần cộng đối soát theo F04 (Marketing không cộng tiền)
BRONZE_MONEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "orders": (),
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_value",),
    "order_reviews": (),
    "customers": (),
    "products": (),
    "sellers": (),
    "product_category_translation": (),
    "geolocation": (),
    "marketing_leads": (),
    "clickstream_events": ("amount",),
}

# 16 bảng Gold theo dbt/schema_contract.yml và các cột tiền cần đối soát theo GT-16
GOLD_MONEY_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "dim_date": (),
    "dim_customer": (),
    "dim_product": (),
    "dim_seller": (),
    "fact_order": ("order_revenue",),
    "fact_order_item": ("price",),
    "fact_lead": (),
    "fact_session": ("purchase_amount",),
    "kpi_sales_month": ("sum_revenue",),
    "kpi_sales_state_month": ("sum_revenue",),
    "kpi_delivery_month": (),
    "kpi_delivery_state_month": (),
    "kpi_lead_month": (),
    "kpi_lead_origin": (),
    "kpi_funnel_day": (),
    "kpi_funnel_month": (),
}

# Cột thời điểm nạp phải loại trừ khi tính mã băm nội dung Bronze (F04)
EXCLUDED_HASH_COLUMNS = ("ingested_at",)


def _build_spark_snapshot_code(catalog: str) -> str:
    """Sinh đoạn mã PySpark thu thập số dòng, tổng tiền và mã băm nội dung cho Bronze và Gold."""
    bronze_cfg = json.dumps(BRONZE_MONEY_COLUMNS)
    gold_cfg = json.dumps(GOLD_MONEY_COLUMNS)
    excluded_cfg = json.dumps(list(EXCLUDED_HASH_COLUMNS))
    return f"""
import hashlib
import json
from pyspark.sql import SparkSession, functions as F

catalog = {json.dumps(catalog)}
bronze_cfg = {bronze_cfg}
gold_cfg = {gold_cfg}
excluded_cols = set({excluded_cfg})

spark = SparkSession.builder.appName("check_rerun_b705").getOrCreate()
result = {{"bronze": {{}}, "gold": {{}}}}

try:
    for tbl, money_cols in bronze_cfg.items():
        full_name = f"{{catalog}}.bronze.{{tbl}}"
        if not spark.catalog.tableExists(full_name):
            result["bronze"][tbl] = {{"exists": False, "batches": {{}}}}
            continue

        df = spark.table(full_name)
        content_cols = sorted([c for c in df.columns if c not in excluded_cols])
        row_hash_col = F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in content_cols]),
            256,
        )
        df_h = df.withColumn("_row_hash", row_hash_col)

        batches_info = {{}}
        batch_rows = [r["batch_id"] for r in df.select("batch_id").distinct().collect()]
        for b_id in sorted([str(x) for x in batch_rows]):
            sub = df_h.filter(F.col("batch_id") == b_id)
            cnt = int(sub.count())
            money_sums = {{}}
            for mc in money_cols:
                if mc in df.columns:
                    val = sub.agg(F.round(F.sum(F.col(mc).cast("double")), 4).alias("s")).collect()[0]["s"]
                    money_sums[mc] = None if val is None else float(val)
            hashes = sorted(r["_row_hash"] for r in sub.select("_row_hash").collect())
            batch_digest = hashlib.sha256("\\n".join(hashes).encode("utf-8")).hexdigest()
            batches_info[b_id] = {{
                "row_count": cnt,
                "money_sums": money_sums,
                "content_hash": batch_digest,
            }}
        result["bronze"][tbl] = {{"exists": True, "batches": batches_info}}

    for tbl, money_cols in gold_cfg.items():
        full_name = f"{{catalog}}.gold.{{tbl}}"
        if not spark.catalog.tableExists(full_name):
            result["gold"][tbl] = {{"exists": False, "row_count": 0, "money_sums": {{}}}}
            continue
        df = spark.table(full_name)
        cnt = int(df.count())
        money_sums = {{}}
        for mc in money_cols:
            if mc in df.columns:
                val = df.agg(F.round(F.sum(F.col(mc).cast("double")), 4).alias("s")).collect()[0]["s"]
                money_sums[mc] = None if val is None else float(val)
        result["gold"][tbl] = {{"exists": True, "row_count": cnt, "money_sums": money_sums}}
finally:
    spark.stop()

print("__RERUN_JSON_BEGIN__")
print(json.dumps(result, ensure_ascii=False, sort_keys=True))
print("__RERUN_JSON_END__")
"""


def capture_snapshot(
    catalog: str = "hive",
    spark_mode: str = "auto",
    spark_container: str = "spark",
) -> Dict[str, Any]:
    """Thu thập ảnh chụp trạng thái Bronze (11 bảng) và Gold (16 bảng) từ Spark."""
    code = _build_spark_snapshot_code(catalog=catalog)
    mode = spark_mode
    if mode == "auto":
        try:
            import pyspark  # noqa: F401
            mode = "local"
        except ImportError:
            mode = "docker"

    cmd = (
        [sys.executable, "-c", code]
        if mode == "local"
        else ["docker", "exec", spark_container, "python3", "-c", code]
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Lỗi khi chụp trạng thái Bronze/Gold (mode={mode}, exit={proc.returncode}):\n{proc.stderr}"
        )

    begin_marker = "__RERUN_JSON_BEGIN__"
    end_marker = "__RERUN_JSON_END__"
    if begin_marker not in proc.stdout or end_marker not in proc.stdout:
        raise RuntimeError("Không đọc được kết quả JSON từ lệnh chụp trạng thái Spark.")

    payload = proc.stdout.split(begin_marker, 1)[1].split(end_marker, 1)[0].strip()
    return json.loads(payload)


def compare_snapshots(
    before: Dict[str, Any],
    after: Dict[str, Any],
    target_batch: str,
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Đối soát hai ảnh chụp trước/sau chạy lại `target_batch` theo GT-16 & F04.
    Trả về (all_passed, danh_sách_dòng_báo_cáo).
    """
    report_rows: List[Dict[str, Any]] = []
    all_passed = True

    # 1. Đối soát tầng Bronze (F04)
    for tbl in BRONZE_MONEY_COLUMNS:
        b_tbl = before.get("bronze", {}).get(tbl, {})
        a_tbl = after.get("bronze", {}).get(tbl, {})
        b_batches = b_tbl.get("batches", {})
        a_batches = a_tbl.get("batches", {})
        all_batch_ids = sorted(set(b_batches.keys()) | set(a_batches.keys()))

        if not b_tbl.get("exists") or not a_tbl.get("exists"):
            all_passed = False
            report_rows.append(
                {
                    "layer": "bronze",
                    "table_name": tbl,
                    "scope": target_batch,
                    "status": "KHÔNG ĐẠT",
                    "detail": f"Bảng không tồn tại (before={b_tbl.get('exists')}, after={a_tbl.get('exists')})",
                }
            )
            continue

        for b_id in all_batch_ids:
            scope = f"target:{b_id}" if b_id == target_batch else f"other:{b_id}"
            b_info = b_batches.get(b_id)
            a_info = a_batches.get(b_id)
            diffs: List[str] = []

            if b_info is None or a_info is None:
                diffs.append(f"Batch xuất hiện/biến mất (before={b_info is not None}, after={a_info is not None})")
            else:
                if b_info["row_count"] != a_info["row_count"]:
                    diffs.append(f"row_count: {b_info['row_count']} -> {a_info['row_count']}")
                if b_info["money_sums"] != a_info["money_sums"]:
                    diffs.append(f"money_sums: {b_info['money_sums']} -> {a_info['money_sums']}")
                if b_info["content_hash"] != a_info["content_hash"]:
                    diffs.append("content_hash (loại ingested_at) thay đổi")

            ok = len(diffs) == 0
            if not ok:
                all_passed = False
            report_rows.append(
                {
                    "layer": "bronze",
                    "table_name": tbl,
                    "scope": scope,
                    "status": "ĐẠT" if ok else "KHÔNG ĐẠT",
                    "detail": "Khớp hoàn toàn" if ok else " ; ".join(diffs),
                }
            )

    # 2. Đối soát tầng Gold (GT-16)
    for tbl in GOLD_MONEY_COLUMNS:
        b_tbl = before.get("gold", {}).get(tbl, {})
        a_tbl = after.get("gold", {}).get(tbl, {})
        diffs = []

        if not b_tbl.get("exists") or not a_tbl.get("exists"):
            diffs.append(f"Bảng không tồn tại (before={b_tbl.get('exists')}, after={a_tbl.get('exists')})")
        else:
            if b_tbl.get("row_count") != a_tbl.get("row_count"):
                diffs.append(f"row_count: {b_tbl.get('row_count')} -> {a_tbl.get('row_count')}")
            if b_tbl.get("money_sums") != a_tbl.get("money_sums"):
                diffs.append(f"money_sums: {b_tbl.get('money_sums')} -> {a_tbl.get('money_sums')}")

        ok = len(diffs) == 0
        if not ok:
            all_passed = False
        report_rows.append(
            {
                "layer": "gold",
                "table_name": tbl,
                "scope": "all",
                "status": "ĐẠT" if ok else "KHÔNG ĐẠT",
                "detail": "Khớp hoàn toàn" if ok else " ; ".join(diffs),
            }
        )

    return all_passed, report_rows


def write_rerun_report(rows: Sequence[Dict[str, Any]], output_path: str) -> None:
    """Ghi kết quả đối soát GT-16 và F04 ra tệp CSV."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    fieldnames = ["layer", "table_name", "scope", "status", "detail"]
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="B-705: Kiểm thử GT-16 & F04 khi chạy lại cùng một batch."
    )
    sub = parser.add_subparsers(dest="action", required=True)

    p_snap = sub.add_parser("snapshot", help="Chụp trạng thái Bronze & Gold hiện tại ra tệp JSON.")
    p_snap.add_argument("--batch", required=True, help="Tên batch chuẩn bị hoặc vừa chạy lại.")
    p_snap.add_argument("--catalog", default=os.getenv("DREMIO_CATALOG", "hive"))
    p_snap.add_argument("--spark-mode", choices=("auto", "local", "docker"), default="auto")
    p_snap.add_argument("--spark-container", default=os.getenv("SPARK_CONTAINER_NAME", "spark"))
    p_snap.add_argument("--output", required=True, help="Đường dẫn tệp JSON lưu ảnh chụp.")

    p_cmp = sub.add_parser("compare", help="So khớp hai ảnh chụp before/after theo GT-16 & F04.")
    p_cmp.add_argument("--batch", required=True, help="Tên batch vừa chạy lại.")
    p_cmp.add_argument("--before", required=True, help="Đường dẫn tệp JSON chụp trước khi chạy lại.")
    p_cmp.add_argument("--after", required=True, help="Đường dẫn tệp JSON chụp sau khi chạy lại.")
    p_cmp.add_argument(
        "--report",
        default=os.path.join(CURRENT_DIR, "check_rerun_report.csv"),
        help="Đường dẫn tệp CSV ghi Bảng so khớp GT-16 & F04.",
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    if args.action == "snapshot":
        print(f"[B-705] Đang chụp trạng thái Bronze (11 bảng) & Gold (16 bảng) cho batch '{args.batch}'...")
        snap = capture_snapshot(
            catalog=args.catalog,
            spark_mode=args.spark_mode,
            spark_container=args.spark_container,
        )
        snap["target_batch"] = args.batch
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, mode="w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=2)
        print(f"[B-705] Đã lưu ảnh chụp tại: {args.output}")
        return 0

    with open(args.before, mode="r", encoding="utf-8") as f:
        before = json.load(f)
    with open(args.after, mode="r", encoding="utf-8") as f:
        after = json.load(f)

    ok, rows = compare_snapshots(before, after, target_batch=args.batch)
    write_rerun_report(rows, args.report)

    failed = [r for r in rows if r["status"] != "ĐẠT"]
    print(f"[B-705] Đối soát GT-16 & F04 cho batch '{args.batch}': {len(rows) - len(failed)}/{len(rows)} mục ĐẠT.")
    for item in failed[:10]:
        print(f"  - [LỖI] {item['layer']}.{item['table_name']} ({item['scope']}): {item['detail']}")
    print(f"[B-705] Đã ghi báo cáo tại: {args.report}")
    print(f"[B-705] KẾT LUẬN: {'ĐẠT' if ok else 'KHÔNG ĐẠT'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

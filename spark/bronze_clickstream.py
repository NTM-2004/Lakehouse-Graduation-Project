"""
spark/bronze_clickstream.py - Tác vụ Spark nạp Raw Clickstream vào Bronze (B-204)
Tác giả phụ trách: Minh
Reviewer: Hoàng (theo phân công v1.2)
Mẫu triển khai: M-10
Tham chiếu:
- Requirement v3 (F03, F04)
- Design Bronze, Silver v2 (Mục III: Nguồn clickstream bronze.clickstream_events)
- Design Build (B-204)

Quy tắc nghiệp vụ:
- Đọc raw/clickstream/<batch_id>/access_log.txt
- Tách bằng regex: amount là số thực (double), event_ts là timestamp, N/A thành NULL
- Luôn giữ raw_line (nguyên văn dòng log) và is_parsed (boolean)
- Thêm 3 cột truy vết: batch_id, source_file, ingested_at
- Ghi đè vào bảng Iceberg hive.bronze.clickstream_events theo partition batch_id
"""

import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    when,
    to_timestamp,
)

# Thêm thư mục gốc vào sys.path để import bronze_common
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from spark.bronze_common import (
    get_spark_session,
    add_audit_columns,
    write_bronze_iceberg,
    VALID_BATCHES,
)

# Regex chuẩn bóc tách 9 trường từ log Clickstream
LOG_REGEX = (
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[INFO\] - "
    r"USER_ID: ([^,]+), SESSION_ID: ([^,]+), ORDER_ID: ([^,]+), "
    r"EVENT_TYPE: ([^,]+), PRODUCT_ID: ([^,]+), AMOUNT: ([^,]+), OUTCOME: (.+)$"
)


def load_bronze_clickstream(
    spark: SparkSession,
    batch_id: str,
    raw_base_dir: str = "s3a://lakehouse/raw/clickstream",
    target_table: str = "clickstream_events"
) -> int:
    """
    Thực hiện đọc tệp log thô của batch, bóc tách cấu trúc và nạp vào bảng Iceberg tầng Bronze.
    Trả về số dòng đã xử lý.
    """
    print(f"=== BẮT ĐẦU NẠP BRONZE CLICKSTREAM CHO BATCH: {batch_id} ===")
    source_file_name = "access_log.txt"
    raw_path = os.path.join(raw_base_dir, batch_id, source_file_name).replace("\\", "/")

    print(f"Đường dẫn đọc dữ liệu: {raw_path}")

    # Đọc tệp văn bản dưới dạng raw_line
    raw_df = spark.read.text(raw_path).withColumnRenamed("value", "raw_line")

    # Bóc tách các trường bằng biểu thức chính quy (Regex)
    parsed_df = raw_df.select(
        col("raw_line"),
        # Nhóm 1: event_ts
        regexp_extract(col("raw_line"), LOG_REGEX, 1).alias("ts_str"),
        # Nhóm 2: user_id
        regexp_extract(col("raw_line"), LOG_REGEX, 2).alias("user_id"),
        # Nhóm 3: session_id
        regexp_extract(col("raw_line"), LOG_REGEX, 3).alias("session_id"),
        # Nhóm 4: order_id
        regexp_extract(col("raw_line"), LOG_REGEX, 4).alias("order_id_raw"),
        # Nhóm 5: event_type
        regexp_extract(col("raw_line"), LOG_REGEX, 5).alias("event_type"),
        # Nhóm 6: product_id
        regexp_extract(col("raw_line"), LOG_REGEX, 6).alias("product_id_raw"),
        # Nhóm 7: amount
        regexp_extract(col("raw_line"), LOG_REGEX, 7).alias("amount_raw"),
        # Nhóm 8: outcome
        regexp_extract(col("raw_line"), LOG_REGEX, 8).alias("outcome_raw")
    )

    # Đánh giá is_parsed: true nếu khớp toàn bộ regex và có timestamp
    is_parsed_expr = (col("ts_str") != "") & (col("session_id") != "") & (col("user_id") != "")

    # Chuẩn hóa giá trị: N/A thành NULL, ép kiểu amount sang double, ép kiểu event_ts sang timestamp
    clean_df = parsed_df.select(
        col("user_id"),
        col("session_id"),
        when(col("order_id_raw") == "N/A", None).otherwise(col("order_id_raw")).alias("order_id"),
        when(col("product_id_raw") == "N/A", None).otherwise(col("product_id_raw")).alias("product_id"),
        when(col("outcome_raw") == "N/A", None).otherwise(col("outcome_raw")).alias("outcome"),
        col("event_type"),
        when(col("amount_raw") == "N/A", None).otherwise(col("amount_raw").cast("double")).alias("amount"),
        to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_ts"),
        col("raw_line"),
        is_parsed_expr.alias("is_parsed")
    )

    # Thêm 3 cột truy vết bắt buộc theo F03
    bronze_df = add_audit_columns(clean_df, batch_id=batch_id, source_file=source_file_name)

    # Đảm bảo schema bronze đã tồn tại trong catalog và trỏ thẳng vào s3a://lakehouse/bronze
    warehouse_base = os.getenv("WAREHOUSE_PATH", "s3a://lakehouse/").rstrip("/")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS hive.bronze LOCATION '{warehouse_base}/bronze'")

    # Đảm bảo bảng Iceberg được tạo với partition batch_id nếu chưa có
    spark.sql("""
        CREATE TABLE IF NOT EXISTS hive.bronze.clickstream_events (
            user_id STRING,
            session_id STRING,
            order_id STRING,
            product_id STRING,
            outcome STRING,
            event_type STRING,
            amount DOUBLE,
            event_ts TIMESTAMP,
            raw_line STRING,
            is_parsed BOOLEAN,
            batch_id STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (batch_id)
        TBLPROPERTIES ('write.format.default'='parquet')
    """)

    # Ghi đè vào partition của batch hiện tại
    write_bronze_iceberg(bronze_df, table_name=target_table, partition_col="batch_id")

    row_count = bronze_df.count()
    print(f"[THÀNH CÔNG] Đã nạp {row_count:,} dòng vào hive.bronze.{target_table} (batch: {batch_id})")
    return row_count


def main():
    parser = argparse.ArgumentParser(description="Nạp Raw Clickstream vào bảng Bronze Iceberg.")
    parser.add_argument(
        "--batch_id",
        required=True,
        choices=VALID_BATCHES + ["all"],
        help=f"Tên batch cần nạp ({', '.join(VALID_BATCHES)}) hoặc 'all' để nạp lần lượt cả 4 batch."
    )
    parser.add_argument(
        "--raw_base_dir",
        default="s3a://lakehouse/raw/clickstream",
        help="Thư mục gốc chứa Raw Clickstream (mặc định: s3a://lakehouse/raw/clickstream)."
    )

    args = parser.parse_args()
    spark = get_spark_session(app_name="BronzeClickstreamLoader")

    try:
        batches_to_run = VALID_BATCHES if args.batch_id == "all" else [args.batch_id]
        total_ingested = 0
        for b in batches_to_run:
            cnt = load_bronze_clickstream(spark, batch_id=b, raw_base_dir=args.raw_base_dir)
            total_ingested += cnt
        print(f"=== TỔNG CỘNG ĐÃ NẠP: {total_ingested:,} DÒNG CHO {len(batches_to_run)} BATCH ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

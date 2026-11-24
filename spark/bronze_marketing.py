"""
spark/bronze_marketing.py - Tác vụ Spark nạp Raw Marketing Funnel JSONL vào Bronze (B-203)
Tác giả phụ trách: Hoàng
Reviewer: Kiên (theo phân công Design Build v1.2)
Mẫu triển khai: M-10
Tham chiếu:
- Requirement v3 (F03, F04, F09)
- Design Bronze, Silver v2 (Mục III: Nguồn json bronze.marketing_leads)
- Design Build v1.2 (B-203)

Quy tắc nghiệp vụ:
- Đọc raw/marketing_funnel/<batch_id>/marketing_leads.jsonl (tuyệt đối bỏ qua thư mục _excluded/)
- Schema tường minh bắt buộc (mọi giá trị đơn là STRING; deal là struct lồng 13 trường gồm cả 4 trường thưa)
- Không duỗi phẳng JSON ở Bronze (duỗi phẳng là việc của dbt ở Silver)
- Giữ nguyên văn raw_json (STRING) và cờ is_parsed (BOOLEAN: false nếu dòng JSON lỗi cú pháp)
- Thêm 3 cột truy vết qua bronze_common.add_audit_columns: batch_id, source_file, ingested_at
- Ghi đè đúng partition batch_id vào bảng Iceberg hive.bronze.marketing_leads (idempotent, F04)
"""

import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

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

SOURCE_FILE_NAME = "marketing_leads.jsonl"
TARGET_TABLE = "marketing_leads"

LEAD_FIELDS = [
    "mql_id",
    "first_contact_date",
    "landing_page_id",
    "origin",
]

DEAL_FIELDS = [
    "seller_id",
    "sdr_id",
    "sr_id",
    "won_date",
    "business_segment",
    "lead_type",
    "lead_behaviour_profile",
    "has_company",
    "has_gtin",
    "average_stock",
    "business_type",
    "declared_product_catalog_size",
    "declared_monthly_revenue",
]

# Schema tường minh bắt buộc: mọi giá trị đơn là STRING, deal là struct lồng (NULL khi khóa vắng)
MARKETING_SCHEMA = StructType(
    [StructField(name, StringType(), nullable=True) for name in LEAD_FIELDS]
    + [
        StructField(
            "deal",
            StructType([StructField(name, StringType(), nullable=True) for name in DEAL_FIELDS]),
            nullable=True,
        )
    ]
)


def _create_bronze_table_if_not_exists(spark: SparkSession) -> None:
    """Đảm bảo schema hive.bronze và bảng Iceberg hive.bronze.marketing_leads đã tồn tại."""
    spark.sql("CREATE DATABASE IF NOT EXISTS hive.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hive.bronze.marketing_leads (
            mql_id STRING,
            first_contact_date STRING,
            landing_page_id STRING,
            origin STRING,
            deal STRUCT<
                seller_id: STRING,
                sdr_id: STRING,
                sr_id: STRING,
                won_date: STRING,
                business_segment: STRING,
                lead_type: STRING,
                lead_behaviour_profile: STRING,
                has_company: STRING,
                has_gtin: STRING,
                average_stock: STRING,
                business_type: STRING,
                declared_product_catalog_size: STRING,
                declared_monthly_revenue: STRING
            >,
            raw_json STRING,
            is_parsed BOOLEAN,
            batch_id STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (batch_id)
        TBLPROPERTIES ('write.format.default'='parquet')
        """
    )


def load_bronze_marketing(
    spark: SparkSession,
    batch_id: str,
    raw_base_dir: str = "s3a://lakehouse/raw/marketing_funnel",
    target_table: str = TARGET_TABLE,
) -> int:
    """
    Đọc tệp JSONL của một batch từ Raw Marketing, áp schema tường minh,
    gắn raw_json, is_parsed và 3 cột truy vết, rồi ghi đè partition vào Iceberg Bronze.
    Trả về số dòng đã nạp.
    """
    if batch_id not in VALID_BATCHES:
        raise ValueError(f"batch_id không hợp lệ: '{batch_id}'. Chỉ chấp nhận: {VALID_BATCHES}")

    print(f"=== BẮT ĐẦU NẠP BRONZE MARKETING CHO BATCH: {batch_id} ===")
    raw_path = os.path.join(raw_base_dir, batch_id, SOURCE_FILE_NAME).replace("\\", "/")
    print(f"Đường dẫn đọc dữ liệu: {raw_path}")

    # Đọc từng dòng văn bản gốc để luôn giữ nguyên văn raw_json (F03)
    raw_df = spark.read.text(raw_path).withColumnRenamed("value", "raw_json")

    # Parse JSON bằng schema tường minh (mode PERMISSIVE mặc định: dòng JSON lỗi -> struct NULL)
    parsed_df = raw_df.select(
        F.from_json(F.col("raw_json"), MARKETING_SCHEMA).alias("j"),
        F.col("raw_json"),
    ).select(
        F.col("j.mql_id").alias("mql_id"),
        F.col("j.first_contact_date").alias("first_contact_date"),
        F.col("j.landing_page_id").alias("landing_page_id"),
        F.col("j.origin").alias("origin"),
        F.col("j.deal").alias("deal"),
        F.col("raw_json"),
        F.col("j").isNotNull().alias("is_parsed"),
    )

    # Thêm 3 cột truy vết bắt buộc theo F03: batch_id, source_file, ingested_at
    bronze_df = add_audit_columns(parsed_df, batch_id=batch_id, source_file=SOURCE_FILE_NAME)

    _create_bronze_table_if_not_exists(spark)

    # Ghi đè đúng partition batch_id (idempotent, F04)
    write_bronze_iceberg(bronze_df, table_name=target_table, partition_col="batch_id")

    # Đếm kiểm tra nhanh từ partition vừa ghi
    partition_df = spark.table(f"hive.bronze.{target_table}").filter(F.col("batch_id") == batch_id)
    row_count = partition_df.count()
    deal_count = partition_df.filter(F.col("deal").isNotNull()).count()
    unparsed_count = partition_df.filter(~F.col("is_parsed")).count()

    print(
        f"[THÀNH CÔNG] Đã nạp {row_count:,} dòng vào hive.bronze.{target_table} "
        f"(batch: {batch_id} | có deal: {deal_count:,} | is_parsed=false: {unparsed_count:,})"
    )
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Nạp Raw Marketing Funnel JSONL vào bảng Bronze Iceberg (B-203)."
    )
    parser.add_argument(
        "--batch_id",
        required=True,
        choices=VALID_BATCHES + ["all"],
        help=f"Tên batch cần nạp ({', '.join(VALID_BATCHES)}) hoặc 'all' để nạp lần lượt cả 4 batch.",
    )
    parser.add_argument(
        "--raw_base_dir",
        default="s3a://lakehouse/raw/marketing_funnel",
        help="Thư mục gốc chứa Raw Marketing (mặc định: s3a://lakehouse/raw/marketing_funnel).",
    )

    args = parser.parse_args()
    spark = get_spark_session(app_name="BronzeMarketingLoader")

    try:
        batches_to_run = VALID_BATCHES if args.batch_id == "all" else [args.batch_id]
        total_ingested = 0
        for b in batches_to_run:
            cnt = load_bronze_marketing(spark, batch_id=b, raw_base_dir=args.raw_base_dir)
            total_ingested += cnt
        print(f"=== TỔNG CỘNG ĐÃ NẠP: {total_ingested:,} DÒNG CHO {len(batches_to_run)} BATCH ===")
    except Exception as exc:
        print(f"[LỖI] {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

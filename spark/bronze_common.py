"""
spark/bronze_common.py - Thư viện dùng chung cho các tác vụ nạp tầng Bronze (B-201)
Tác giả phụ trách: Minh
Reviewer: Hoàng (theo phân công v1.2)
Mục đích:
- Khởi tạo SparkSession chuẩn kết nối Iceberg Catalog qua Hive Metastore và MinIO s3a://
- Thêm 3 cột truy vết bắt buộc theo F03: batch_id (string), source_file (string), ingested_at (timestamp)
- Ghi đè đúng partition batch_id vào bảng Iceberg tầng Bronze (Partition key, F04, F11)
"""

import os
import sys
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

# Định nghĩa các mốc batch chuẩn
BATCH_BULK = "bulk_2016-09_2018-05"
BATCH_2018_06 = "2018-06"
BATCH_2018_07 = "2018-07"
BATCH_2018_08 = "2018-08"
VALID_BATCHES = [BATCH_BULK, BATCH_2018_06, BATCH_2018_07, BATCH_2018_08]


def get_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    """
    Khởi tạo hoặc lấy SparkSession có sẵn với cấu hình Iceberg Catalog và MinIO S3A.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Các cấu hình mặc định (nếu chưa truyền qua spark-defaults.conf)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "hive")
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive.type", "hive")
        .config("spark.sql.catalog.hive.uri", os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
        .config("spark.sql.catalog.hive.warehouse", os.getenv("WAREHOUSE_PATH", "s3a://lakehouse/"))
        .config("spark.sql.catalog.hive.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.hive.s3.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.sql.catalog.hive.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    return builder.getOrCreate()


def add_audit_columns(df: DataFrame, batch_id: str, source_file: str) -> DataFrame:
    """
    Thêm 3 cột truy vết theo tiêu chuẩn F03:
    - batch_id (string): Tên thư mục batch nạp
    - source_file (string): Tên tệp nguồn của dòng
    - ingested_at (timestamp): Thời điểm nạp
    """
    return (
        df.withColumn("batch_id", lit(batch_id))
          .withColumn("source_file", lit(source_file))
          .withColumn("ingested_at", current_timestamp())
    )


def write_bronze_iceberg(
    df: DataFrame,
    table_name: str,
    partition_col: str = "batch_id",
    catalog: str = "hive",
    db_name: str = "bronze"
) -> None:
    """
    Ghi đè dữ liệu DataFrame vào bảng Iceberg theo đúng partition_col.
    Sử dụng dynamic partition overwrite để chỉ ghi đè dữ liệu của batch hiện tại (idempotent, F04).
    Tên bảng đầy đủ: <catalog>.<db_name>.<table_name> (ví dụ: hive.bronze.clickstream_events)
    """
    full_table_name = f"{catalog}.{db_name}.{table_name}"
    spark = df.sparkSession

    # Đảm bảo schema bronze đã tồn tại trong catalog và trỏ thẳng vào s3a://lakehouse/<db_name>
    warehouse_base = os.getenv("WAREHOUSE_PATH", "s3a://lakehouse/").rstrip("/")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db_name} LOCATION '{warehouse_base}/{db_name}'")

    # Ghi đè đúng partition
    (
        df.writeTo(full_table_name)
          .tableProperty("write.format.default", "parquet")
          .overwritePartitions()
    )
    print(f"[OK] Đã ghi đè partition {partition_col} vào bảng Iceberg {full_table_name}")

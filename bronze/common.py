"""
bronze/common.py - shared Bronze conventions. Every per-source loader imports this,
so all Bronze tables follow the same contract:

  * namespace hive.bronze, one table per ENTITY (batches of the same entity go into the same table)
  * extra columns on every row: batch_id, source_file, ingested_at
  * partitioned by batch_id, written with overwritePartitions() -> re-running a batch is safe
  * a source folder is only read if it contains _SUCCESS; a file missing from a batch is skipped
"""
import argparse
import sys
from pyspark.sql import SparkSession, functions as F

RAW_ROOT = "s3a://lakehouse/raw"
NAMESPACE = "hive.bronze"


def path_exists(spark, path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def read_csv(spark, path):
    """All columns stay STRING (casting is Silver's job). multiLine+escape are required for
    olist_order_reviews: 3,852 comments contain line breaks inside quotes."""
    df = (spark.read.option("header", True).option("multiLine", True)
          .option("quote", '"').option("escape", '"').option("inferSchema", False).csv(path))
    return df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])   # strip BOM in headers


def write_batch(spark, df, table, batch, source_path):
    df = (df.withColumn("batch_id", F.lit(batch))
            .withColumn("source_file", F.lit(source_path))
            .withColumn("ingested_at", F.current_timestamp()))
    full = f"{NAMESPACE}.{table}"
    if not spark.catalog.tableExists(full):
        df.writeTo(full).using("iceberg").partitionedBy(F.col("batch_id")).create()
    else:
        df.writeTo(full).overwritePartitions()          # replaces ONLY this batch_id's rows
    return spark.table(full).filter(F.col("batch_id") == batch).count()


def run_loader(app, source, batch, registry, only=None):
    """registry: list of (file_name, bronze_table, reader_fn(spark, path) -> DataFrame)"""
    spark = SparkSession.builder.appName(f"{app}_{batch}").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    folder = f"{RAW_ROOT}/{source}/{batch}"
    if not path_exists(spark, f"{folder}/_SUCCESS"):
        raise SystemExit(f"[{app}] no _SUCCESS marker in {folder} - batch not ready, nothing loaded")

    summary = []
    for fname, table, reader in registry:
        if only and table not in only:
            continue
        fpath = f"{folder}/{fname}"
        if not path_exists(spark, fpath):
            summary.append((table, "SKIP", "file not in this batch"))
            continue
        n = write_batch(spark, reader(spark, fpath), table, batch, fpath)
        summary.append((table, f"{n:,} rows", ""))
    print(f"\n=== {app} | batch = {batch} ===")
    for t, s, note in summary:
        print(f"  {t:32s} {s:>16s}   {note}")
    if not any(s.endswith("rows") for _, s, _ in summary):
        raise SystemExit(f"[{app}] nothing was loaded")
    return spark


def cli(app, source, registry):
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    ap.add_argument("--tables", default="", help="comma-separated Bronze table names (default: all)")
    a = ap.parse_args()
    return a.batch, run_loader(app, source, a.batch, registry,
                               {t.strip() for t in a.tables.split(",") if t.strip()})

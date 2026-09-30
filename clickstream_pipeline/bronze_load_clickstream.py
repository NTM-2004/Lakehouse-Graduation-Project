"""Member C - Clickstream text log -> Bronze.
    spark-submit /home/clickstream_pipeline/bronze_load_clickstream.py --batch 2018-06
"""
import sys
sys.path.insert(0, "/home/bronze")
from pyspark.sql import functions as F
from common import cli, NAMESPACE

CLICK_REGEX = (
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[(\w+)\] - "
    r"USER_ID: (\S+), SESSION_ID: (\S+), ORDER_ID: (\S+), "
    r"EVENT_TYPE: (\w+), PRODUCT_ID: (\S+), AMOUNT: (\S+), OUTCOME: (\S+)$"
)


def read_clickstream(spark, path):
    raw = spark.read.text(path)
    g = lambda i: F.regexp_extract("value", CLICK_REGEX, i)
    na = lambda c: F.when(c == "N/A", F.lit(None)).otherwise(c)       # 'N/A' -> real NULL before any cast
    return (raw.select(
                g(1).alias("ts_str"), g(3).alias("user_id"), g(4).alias("session_id"),
                g(5).alias("order_id"), g(6).alias("event_type"), g(7).alias("product_id"),
                g(8).alias("amount"), g(9).alias("outcome"), F.col("value").alias("raw_line"))
            .withColumn("is_parsed", F.col("ts_str") != "")            # unparsed lines are flagged, not dropped
            .withColumn("event_ts", F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
            .withColumn("order_id", na(F.col("order_id")))
            .withColumn("product_id", na(F.col("product_id")))
            .withColumn("amount", na(F.col("amount")).cast("double"))
            .withColumn("outcome", na(F.col("outcome")))
            .drop("ts_str"))


REGISTRY = [("access_log.txt", "clickstream_events", read_clickstream)]

if __name__ == "__main__":
    batch, spark = cli("bronze_clickstream", "clickstream", REGISTRY)
    bad = (spark.table(f"{NAMESPACE}.clickstream_events")
           .filter((F.col("batch_id") == batch) & (~F.col("is_parsed"))).count())
    print(f"  unparsed lines: {bad}" + ("   <-- CHECK THE REGEX" if bad else "   (OK)"))

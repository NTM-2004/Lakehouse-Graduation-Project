"""Member B - Marketing funnel nested JSONL -> Bronze (hive.bronze.marketing_leads).

    spark-submit /home/json_pipeline/bronze_load_json.py --batch bulk_2016-09_2018-05
    spark-submit /home/json_pipeline/bronze_load_json.py --batch 2018-06

One row per JSON line = one lead. The closed deal (if any) stays as a nested `deal`
struct; a lead without a deal has the key absent in the file -> deal IS NULL here.
All leaf fields are STRING (casting is Silver's job), like the other Bronze loaders.
"""
import sys
sys.path.insert(0, "/home/bronze")
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from common import cli, NAMESPACE

# ---- CHECK THESE AGAINST YOUR RAW FILES --------------------------------------------
LEADS_FILE = "marketing_leads.jsonl"   # file name inside raw/marketing_funnel/<batch>/
DEAL_KEY = "deal"                      # name of the nested object
LEAD_FIELDS = ["mql_id", "first_contact_date", "landing_page_id", "origin"]
DEAL_FIELDS = [
    "seller_id", "sdr_id", "sr_id", "won_date", "business_segment", "lead_type",
    "lead_behaviour_profile", "has_company", "has_gtin", "average_stock", "business_type",
    "declared_product_catalog_size", "declared_monthly_revenue",
]
# ------------------------------------------------------------------------------------


def _strings(names):
    return [StructField(n, StringType()) for n in names]


# Explicit schema: every batch gets the SAME columns even if a batch has no deals at all,
# otherwise overwritePartitions() would fail on schema drift between batches.
SCHEMA = StructType(_strings(LEAD_FIELDS) + [StructField(DEAL_KEY, StructType(_strings(DEAL_FIELDS)))])


def _warn_unknown_fields(spark, path):
    """Fields present in the file but missing from SCHEMA would be silently dropped - say so."""
    inferred = spark.read.json(path).schema
    extra_top = set(inferred.fieldNames()) - set(LEAD_FIELDS) - {DEAL_KEY}
    extra_deal = set()
    if DEAL_KEY in inferred.fieldNames() and isinstance(inferred[DEAL_KEY].dataType, StructType):
        extra_deal = set(inferred[DEAL_KEY].dataType.fieldNames()) - set(DEAL_FIELDS)
    elif DEAL_KEY not in inferred.fieldNames():
        print(f"  [WARN] no '{DEAL_KEY}' key found in {path} (ok only if this batch has no deals)")
    if extra_top:
        print(f"  [WARN] top-level fields in data but not in SCHEMA (will be dropped): {sorted(extra_top)}")
    if extra_deal:
        print(f"  [WARN] {DEAL_KEY} fields in data but not in SCHEMA (will be dropped): {sorted(extra_deal)}")


def read_leads(spark, path):
    _warn_unknown_fields(spark, path)
    raw = spark.read.text(path)                                   # one JSON object per line
    return (raw.select(F.from_json("value", SCHEMA).alias("j"), F.col("value").alias("raw_json"))
               .select("j.*", "raw_json")                          # raw_json kept for lineage/debug
               .withColumn("is_parsed", F.col("mql_id").isNotNull()))   # flagged, not dropped


REGISTRY = [(LEADS_FILE, "marketing_leads", read_leads)]

if __name__ == "__main__":
    batch, spark = cli("bronze_json", "marketing_funnel", REGISTRY)
    t = spark.table(f"{NAMESPACE}.marketing_leads").filter(F.col("batch_id") == batch)
    bad = t.filter(~F.col("is_parsed")).count()
    deals = t.filter(F.col(DEAL_KEY).isNotNull()).count()
    print(f"  leads with a deal: {deals:,}")
    print(f"  unparsed lines: {bad}" + (" <-- CHECK LEADS_FILE / SCHEMA" if bad else "   (OK)"))
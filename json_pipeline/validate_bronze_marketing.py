"""Validate hive.bronze.marketing_leads against the original Olist CSVs.

    spark-submit /home/json_pipeline/validate_bronze_marketing.py

Prints PASS/FAIL per check. Exit code 1 if any check fails.
Reads the source CSVs from /home/raw/json/archive (mounted from ./raw/json/archive).
"""
import sys
from functools import reduce
from pyspark.sql import SparkSession, functions as F

T = "hive.bronze.marketing_leads"
SRC = "/home/raw/json/archive"
BULK = "bulk_2016-09_2018-05"
# batch -> (expected leads, expected leads-with-deal, won_date lower bound, won_date upper bound)
EXPECTED = {
    BULK:      (8000, 665, None,                  "2018-05-31 23:59:59"),
    "2018-06": (57,   57,  "2018-06-01 00:00:00", "2018-06-30 23:59:59"),
    "2018-07": (37,   37,  "2018-07-01 00:00:00", "2018-07-31 23:59:59"),
    "2018-08": (33,   33,  "2018-08-01 00:00:00", "2018-08-31 23:59:59"),
}
LEAD_FIELDS = ["mql_id", "first_contact_date", "landing_page_id", "origin"]
DEAL_FIELDS = [
    "seller_id", "sdr_id", "sr_id", "won_date", "business_segment", "lead_type",
    "lead_behaviour_profile", "has_company", "has_gtin", "average_stock", "business_type",
    "declared_product_catalog_size", "declared_monthly_revenue",
]

spark = SparkSession.builder.appName("validate_bronze_marketing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  ->  {detail}" if detail else ""))


def any_diff(pairs):
    return reduce(lambda x, y: x | y, [~a.eqNullSafe(b) for a, b in pairs])


df = spark.table(T).cache()
cols = set(df.columns)
bulk = df.filter(F.col("batch_id") == BULK)
later = df.filter(F.col("batch_id") != BULK)

print("\n--- 1. Structure ---")
required = set(LEAD_FIELDS) | {"deal", "raw_json", "is_parsed", "batch_id"}
check("required columns present", required <= cols, f"missing: {sorted(required - cols)}")
deal_type = dict(df.dtypes).get("deal", "")
check("deal is a nested struct", deal_type.startswith("struct"), deal_type[:60])
for c in ("source_file", "ingested_at"):
    if c in cols:
        n = df.filter(F.col(c).isNull()).count()
        check(f"{c} never null", n == 0, f"{n} nulls")
    else:
        print(f"[SKIP] column {c} not in table")

print("\n--- 2. Row counts per batch ---")
counts = {r["batch_id"]: (r["n"], r["d"]) for r in
          df.groupBy("batch_id").agg(F.count("*").alias("n"), F.count("deal").alias("d")).collect()}
for b, (n_exp, d_exp, _, _) in EXPECTED.items():
    got = counts.get(b, (0, 0))
    check(f"{b}: leads/with_deal", got == (n_exp, d_exp), f"got {got}, expected {(n_exp, d_exp)}")
check("no unexpected batches", set(counts) == set(EXPECTED), f"found {sorted(counts)}")
check("total rows = 8127", df.count() == 8127, str(df.count()))
check("distinct mql_id = 8000", df.select("mql_id").distinct().count() == 8000)

print("\n--- 3. Parsing and nulls ---")
check("no unparsed lines", df.filter(~F.col("is_parsed")).count() == 0)
for c in ("mql_id", "first_contact_date", "batch_id", "raw_json"):
    n = df.filter(F.col(c).isNull()).count()
    check(f"{c} never null", n == 0, f"{n} nulls")
check("mql_id unique inside each batch",
      df.groupBy("batch_id", "mql_id").count().filter("count > 1").count() == 0)

print("\n--- 4. Formats (Bronze keeps strings; Silver will cast) ---")
bad_d = df.filter(~F.col("first_contact_date").rlike(r"^\d{4}-\d{2}-\d{2}$")).count()
check("first_contact_date = yyyy-MM-dd", bad_d == 0, f"{bad_d} bad")
deals = df.filter(F.col("deal").isNotNull())
bad_w = deals.filter(~F.col("deal.won_date").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")).count()
check("won_date = yyyy-MM-dd HH:mm:ss", bad_w == 0, f"{bad_w} bad")
bad_k = deals.filter(F.col("deal.seller_id").isNull() | F.col("deal.won_date").isNull()).count()
check("every deal has seller_id and won_date", bad_k == 0, f"{bad_k} bad")

print("\n--- 5. Batch logic ---")
for b, (_, _, lo, hi) in EXPECTED.items():
    cond = F.col("deal.won_date") > hi
    if lo:
        cond = cond | (F.col("deal.won_date") < lo)
    n = df.filter((F.col("batch_id") == b) & F.col("deal").isNotNull() & cond).count()
    check(f"{b}: all won_date inside batch window", n == 0, f"{n} outside")
check("incremental batches: every row has a deal",
      later.filter(F.col("deal").isNull()).count() == 0)
ids_later = later.select("mql_id").distinct()
check("incremental leads all exist in bulk",
      ids_later.join(bulk.select("mql_id"), "mql_id", "left_anti").count() == 0)
check("leads closed later have NO deal in bulk row",
      bulk.filter(F.col("deal").isNotNull()).join(ids_later, "mql_id").count() == 0)

print("\n--- 6. Reconcile with source CSVs ---")
csv_leads = spark.read.option("header", True).csv(f"{SRC}/olist_marketing_qualified_leads_dataset.csv")
csv_deals = spark.read.option("header", True).csv(f"{SRC}/olist_closed_deals_dataset.csv")
check("source lead count = bulk count", csv_leads.count() == bulk.count(),
      f"csv {csv_leads.count()} vs bronze {bulk.count()}")
check("same set of mql_id as CSV",
      csv_leads.join(bulk, "mql_id", "left_anti").count() == 0
      and bulk.join(csv_leads, "mql_id", "left_anti").count() == 0)
jl = bulk.alias("b").join(csv_leads.alias("c"), F.col("b.mql_id") == F.col("c.mql_id"))
d = jl.filter(any_diff([(F.col(f"b.{f}"), F.col(f"c.{f}")) for f in LEAD_FIELDS])).count()
check("lead fields identical to CSV", d == 0, f"{d} rows differ")
n_null_src = csv_leads.filter(F.col("origin").isNull()).count()
n_null_brz = bulk.filter(F.col("origin").isNull()).count()
check("null origin count matches CSV", n_null_src == n_null_brz, f"csv {n_null_src} vs bronze {n_null_brz}")

in_window = csv_deals.filter(F.col("won_date") <= "2018-08-31 23:59:59")
dropped = csv_deals.count() - in_window.count()
check("deals in Bronze = CSV deals up to 2018-08-31", deals.count() == in_window.count(),
      f"bronze {deals.count()} vs csv {in_window.count()}")
check("deals dropped (post-window) = 50", dropped == 50, str(dropped))
check("each mql_id has its deal in exactly one batch",
      deals.groupBy("mql_id").count().filter("count > 1").count() == 0)
jd = deals.alias("b").join(in_window.alias("c"), F.col("b.mql_id") == F.col("c.mql_id"))
check("every Bronze deal matches a CSV deal", jd.count() == deals.count(), str(jd.count()))
d = jd.filter(any_diff([(F.col(f"b.deal.{f}"), F.col(f"c.{f}")) for f in DEAL_FIELDS])).count()
check("deal fields identical to CSV", d == 0, f"{d} rows differ")

print("\n--- 7. Iceberg metadata ---")
try:
    snaps = spark.sql(f"SELECT count(*) AS n FROM {T}.snapshots").collect()[0]["n"]
    files = spark.sql(f"SELECT count(*) AS n FROM {T}.files").collect()[0]["n"]
    check("table has snapshots and data files", snaps >= 4 and files >= 4, f"{snaps} snapshots, {files} files")
except Exception as e:
    print(f"[SKIP] could not read Iceberg metadata tables: {str(e)[:80]}")

print("\n--- sample (one lead with a deal) ---")
row = deals.select("batch_id", "raw_json").limit(1).collect()
if row:
    print(f"batch={row[0]['batch_id']}\n{row[0]['raw_json'][:400]}")

passed, total = sum(results), len(results)
print(f"\n=== {passed}/{total} checks passed ===")
spark.stop()
sys.exit(0 if passed == total else 1)
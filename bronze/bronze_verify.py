"""
bronze_verify.py - compare Bronze row counts per batch with the numbers in BATCH_SPLIT_GUIDE.md.
    spark-submit /home/bronze/bronze_verify.py
Exit code 1 if anything differs, so Airflow / CI can fail on it.
"""
from pyspark.sql import SparkSession, functions as F

B, J, L, G = "bulk_2016-09_2018-05", "2018-06", "2018-07", "2018-08"
EXPECTED = {
    "orders":             {B: 80450,  J: 6167,  L: 6292,  G: 6512},
    "order_items":        {B: 91231,  J: 7078,  L: 7092,  G: 7248},
    "order_payments":     {B: 84242,  J: 6419,  L: 6507,  G: 6698},
    "order_reviews":      {B: 80302,  J: 6147,  L: 6269,  G: 6487},
    "clickstream_events": {B: 802297, J: 61958, L: 63004, G: 64772},
    "closed_deals":       {B: 665,    J: 57,    L: 37,    G: 33},     # 50 deals after 2018-08-31 excluded by design
    "marketing_leads":    {B: 8000},
    "customers":          {B: 99441},
    "products":           {B: 32951},
    "sellers":            {B: 3095},
    "geolocation":        {B: 1000163},
    "product_category_translation": {B: 71},
}

spark = SparkSession.builder.appName("bronze_verify").getOrCreate()
ok = True
for table, exp in EXPECTED.items():
    try:
        got = {r["batch_id"]: r["n"] for r in
               spark.table(f"hive.bronze.{table}").groupBy("batch_id").agg(F.count("*").alias("n")).collect()}
    except Exception as e:
        print(f"[MISSING] {table}: {str(e)[:80]}"); ok = False; continue
    for batch, n in exp.items():
        g = got.get(batch, 0)
        flag = "OK  " if g == n else "FAIL"
        ok &= (g == n)
        print(f"[{flag}] {table:30s} {batch:24s} expected {n:>9,}  got {g:>9,}")
    extra = set(got) - set(exp)
    if extra:
        print(f"[WARN] {table}: unexpected batch_id(s) {extra}")
raise SystemExit(0 if ok else 1)

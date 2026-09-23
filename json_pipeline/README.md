# Marketing Funnel JSON pipeline — owned by Member B

Scope: Bước 2.2, Bước 3 (`bronze_load_json.py`), Bước 4 (`stg_marketing_leads`,
`seller_acquisition`), Bước 5 (`seller_performance`), Bước 8 (AI Insight),
Streamlit "Seller" tab.

## Before you start
- You depend on Member A's `orders_enriched` for `seller_acquisition`.
  Use `/fixtures/orders_enriched_fixture.csv` until A's real pipeline is ready.

## Files to create here
- `build_lead_json.py` — converts the 2 Olist CSVs into nested `.jsonl`
  (remember: missing deal = key absent, not null).
- `bronze_load_json.py` — Spark job, `.jsonl` → `hive.bronze.marketing_leads`.

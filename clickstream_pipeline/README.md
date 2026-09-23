# Clickstream pipeline — owned by Member C

Scope: Bước 2.3, Bước 3 (`bronze_load_clickstream.py`), Bước 4
(`stg_clickstream_events`, `clickstream_sessions`), Bước 5
(`funnel_conversion_by_month`), Bước 6 (Airflow DAGs), Streamlit "Funnel" tab.

## Before you start
- `clickstream_sessions` joins to Member A's `orders_enriched` on `order_id`.
  Use `/fixtures/orders_enriched_fixture.csv` until A's real pipeline is ready.

## Files to create here
- `generate_clickstream.py` — reused/adapted script, run per batch.
- `bronze_load_clickstream.py` — Spark job, text log → `hive.bronze.clickstream_events`
  (regexp_extract for ip/timestamp/method/path/status/session_id/event).

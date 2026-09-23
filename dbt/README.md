# dbt project (Silver + Gold)

`models/staging/` — one model file per source-owner (stg_orders*, stg_marketing_leads,
stg_clickstream_events). `models/marts/` — Gold tables (monthly_revenue_by_state,
seller_performance, delivery_performance, customer_rfm, funnel_conversion_by_month)
plus the cross-source marts (orders_enriched, seller_acquisition, clickstream_sessions).

Each person adds their own model files to avoid merge conflicts in the same file.
Use `materialized='incremental'` with a `unique_key` so re-running a batch doesn't duplicate rows.

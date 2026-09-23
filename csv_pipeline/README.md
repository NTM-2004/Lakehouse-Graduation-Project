# Olist CSV pipeline — owned by Member A

Scope: Bước 2.1, Bước 3 (`bronze_load_csv.py`), Bước 4 (`stg_orders`, `stg_order_items`,
..., `orders_enriched`), Bước 5 (`monthly_revenue_by_state`, `delivery_performance`,
`customer_rfm`), Streamlit "Doanh thu" tab.

## Before you start
1. Freeze the `orders_enriched` schema with the team (see `/fixtures/orders_enriched_SCHEMA.md`)
   and publish `fixtures/orders_enriched_fixture.csv` early — B and C are blocked on this.
2. Download the 9 Olist CSVs, put raw copies under `raw/csv/` (gitignored — goes to MinIO, not git).

## Files to create here
- `split_batches.py` — splits Olist CSVs into bulk (09/2016–05/2018) + 3 monthly batches.
- `bronze_load_csv.py` — Spark job, CSV → `hive.bronze.<table>`.

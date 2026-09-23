# `orders_enriched` — frozen interface contract

Owned by: **Member A (Olist CSV pipeline)**
Consumed by: Member B (`seller_acquisition`), Member C (`clickstream_sessions`)

⚠️ This schema is agreed in the team's first sync session and then frozen.
If A needs to change it later, they must flag it to B and C before merging —
don't silently rename/remove columns others depend on.

## Columns

| column | type | notes |
|---|---|---|
| order_id | string | primary key |
| customer_id | string | |
| seller_id | string | join key for Member B's seller_acquisition |
| order_purchase_timestamp | timestamp | |
| order_status | string | |
| customer_state | string | |
| product_category | string | |
| price | decimal | |
| freight_value | decimal | |
| review_score | int | nullable |
| batch_month | string | e.g. "2018-06", or "bulk" |

## How to use the fixture

`orders_enriched_fixture.csv` in this folder has a handful of made-up rows
matching the schema above. Until Member A's real Bronze→Silver→Gold pipeline
is ready, Member B and Member C should build and test their dbt models
against this fixture (load it as a seed / temporary source in dbt) instead
of waiting for the real table. Swap the fixture for the real
`hive.silver.orders_enriched` table only at integration time — the join
logic itself won't need to change if column names/types stay the same.

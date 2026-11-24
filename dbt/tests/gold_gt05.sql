{#
  dbt/tests/gold_gt05.sql - GT-05: Tổng tiền Gold khớp Silver (B-421)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F08, Design Gold v2 - Kiểm thử Gold GT-05)
  Kiểm tra:
  - SUM(fact_order_item.price) = SUM(silver.order_items.price)
  - SUM(fact_order.order_revenue) = SUM(silver.order_items.price)
  - SUM(fact_session.purchase_amount) = SUM(silver.clickstream_events.amount) (sự kiện purchase)
#}

{{ config(severity='error') }}

SELECT 'fact_order_item.price vs silver.order_items.price' AS check_name, g.total AS gold_total, s.total AS silver_total
FROM (SELECT COALESCE(SUM(price), 0) AS total FROM {{ ref('fact_order_item') }}) g
CROSS JOIN (SELECT COALESCE(SUM(price), 0) AS total FROM {{ ref('order_items') }}) s
WHERE ABS(g.total - s.total) > 0.005

UNION ALL
SELECT 'fact_order.order_revenue vs silver.order_items.price', g.total, s.total
FROM (SELECT COALESCE(SUM(order_revenue), 0) AS total FROM {{ ref('fact_order') }}) g
CROSS JOIN (SELECT COALESCE(SUM(price), 0) AS total FROM {{ ref('order_items') }}) s
WHERE ABS(g.total - s.total) > 0.005

UNION ALL
SELECT 'fact_session.purchase_amount vs silver.clickstream_events.amount', g.total, s.total
FROM (SELECT COALESCE(SUM(purchase_amount), 0) AS total FROM {{ ref('fact_session') }}) g
CROSS JOIN (SELECT COALESCE(SUM(amount), 0) AS total FROM {{ ref('clickstream_events') }} WHERE event_type = 'purchase') s
WHERE ABS(g.total - s.total) > 0.005

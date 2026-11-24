{#
  dbt/tests/gold_gt04.sql - GT-04: Số dòng các bảng Gold (dimension & fact) khớp Silver (B-420)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F08, Design Gold v2 - Kiểm thử Gold GT-04)
#}

{{ config(severity='error') }}

SELECT 'dim_customer vs silver.customers' AS check_name, g.cnt AS gold_cnt, s.cnt AS silver_cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('dim_customer') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('customers') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'dim_product vs silver.products', g.cnt, s.cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('dim_product') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('products') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'dim_seller vs silver.sellers', g.cnt, s.cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('dim_seller') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('sellers') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'fact_order vs silver.orders', g.cnt, s.cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('fact_order') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('orders') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'fact_order_item vs silver.order_items', g.cnt, s.cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('fact_order_item') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('order_items') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'fact_lead vs silver.leads', g.cnt, s.cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('fact_lead') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('leads') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'fact_session vs distinct sessions in silver.clickstream_events', g.cnt, s.cnt
FROM (SELECT COUNT(*) AS cnt FROM {{ ref('fact_session') }}) g
CROSS JOIN (SELECT COUNT(DISTINCT session_id) AS cnt FROM {{ ref('clickstream_events') }}) s
WHERE g.cnt != s.cnt

UNION ALL
SELECT 'sum(fact_session.n_events) vs silver.clickstream_events', g.cnt, s.cnt
FROM (SELECT COALESCE(SUM(n_events), 0) AS cnt FROM {{ ref('fact_session') }}) g
CROSS JOIN (SELECT COUNT(*) AS cnt FROM {{ ref('clickstream_events') }}) s
WHERE g.cnt != s.cnt

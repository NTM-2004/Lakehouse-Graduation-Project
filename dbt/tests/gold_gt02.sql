{#
  dbt/tests/gold_gt02.sql - GT-02: Mỗi bảng fact duy nhất theo đúng grain đã khai báo (B-418)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F08, F10, Design Gold v2 - Kiểm thử Gold GT-02)
  Grain:
  - fact_order: order_id
  - fact_order_item: (order_id, order_item_id)
  - fact_lead: mql_id
  - fact_session: session_id
#}

{{ config(severity='error') }}

SELECT 'fact_order' AS fact_table, order_id AS grain_key, COUNT(*) AS dup_cnt
FROM {{ ref('fact_order') }}
GROUP BY order_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'fact_order_item' AS fact_table, order_id || '|' || CAST(order_item_id AS VARCHAR) AS grain_key, COUNT(*) AS dup_cnt
FROM {{ ref('fact_order_item') }}
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'fact_lead' AS fact_table, mql_id AS grain_key, COUNT(*) AS dup_cnt
FROM {{ ref('fact_lead') }}
GROUP BY mql_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'fact_session' AS fact_table, session_id AS grain_key, COUNT(*) AS dup_cnt
FROM {{ ref('fact_session') }}
GROUP BY session_id
HAVING COUNT(*) > 1

{#
  dbt/tests/gold_gt08.sql - GT-08: Cờ loại khỏi KPI (D1, D2, D4 và C1) (B-424)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-07
  Mức: error (Design Gold v2 - Kiểm thử Gold GT-08; Bảng tra cờ)

  Kiểm tra:
  1. Không đơn nào ở silver.orders mang cờ D1, D2 hoặc D4 mà gold.fact_order.is_delivered_order = true.
  2. Số lead có is_close_time_excluded = true ở gold.fact_lead bằng đúng số lead mang cờ C1 ở silver.leads
     (và kiểm tra khớp từng bản ghi mql_id giữa fact_lead và silver.leads).
#}

{{ config(severity='error') }}

-- 1. Đơn mang cờ D1, D2 hoặc D4 không được tính là is_delivered_order
SELECT
    'fact_order_d1_d2_d4' AS check_name,
    fo.order_id AS record_key,
    CAST(so.dq_flag AS VARCHAR) AS detail
FROM {{ ref('fact_order') }} fo
INNER JOIN {{ ref('orders') }} so
    ON fo.order_id = so.order_id
WHERE fo.is_delivered_order = true
  AND (
      {{ has_flag('so.dq_flag', 'D1') }}
      OR {{ has_flag('so.dq_flag', 'D2') }}
      OR {{ has_flag('so.dq_flag', 'D4') }}
  )

UNION ALL

-- 2. Tổng số lead có is_close_time_excluded đúng bằng số lead mang C1 ở silver.leads
SELECT
    'fact_lead_c1_count' AS check_name,
    'TOTAL' AS record_key,
    'gold=' || CAST(g.cnt AS VARCHAR) || ',silver=' || CAST(s.cnt AS VARCHAR) AS detail
FROM (
    SELECT SUM(CASE WHEN is_close_time_excluded THEN 1 ELSE 0 END) AS cnt
    FROM {{ ref('fact_lead') }}
) g
CROSS JOIN (
    SELECT SUM(CASE WHEN {{ has_flag('dq_flag', 'C1') }} THEN 1 ELSE 0 END) AS cnt
    FROM {{ ref('leads') }}
) s
WHERE COALESCE(g.cnt, 0) != COALESCE(s.cnt, 0)

UNION ALL

-- 3. Khớp từng dòng mql_id giữa fact_lead.is_close_time_excluded và cờ C1 ở silver.leads
SELECT
    'fact_lead_c1_row_mismatch' AS check_name,
    fl.mql_id AS record_key,
    COALESCE(sl.dq_flag, 'NULL') AS detail
FROM {{ ref('fact_lead') }} fl
INNER JOIN {{ ref('leads') }} sl
    ON fl.mql_id = sl.mql_id
WHERE fl.is_close_time_excluded IS NULL
   OR fl.is_close_time_excluded != {{ has_flag('sl.dq_flag', 'C1') }}

{#
  dbt/tests/gold_gt13.sql - GT-13: Phễu chặt (B-429)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-07
  Mức: error (Design Gold v2 - Kiểm thử Gold GT-13, Tỷ lệ chuyển bước phễu)

  Kiểm tra:
  - Ở mọi dòng của gold.kpi_funnel_day và gold.kpi_funnel_month:
    n_reach_home >= n_reach_view >= n_reach_cart >= n_reach_purchase >= 0
    và n_sessions >= n_reach_home.
#}

{{ config(severity='error') }}

SELECT
    'kpi_funnel_day' AS table_name,
    CAST(session_date AS VARCHAR) AS slice_key,
    n_sessions,
    n_reach_home,
    n_reach_view,
    n_reach_cart,
    n_reach_purchase
FROM {{ ref('kpi_funnel_day') }}
WHERE n_sessions < n_reach_home
   OR n_reach_home < n_reach_view
   OR n_reach_view < n_reach_cart
   OR n_reach_cart < n_reach_purchase
   OR n_reach_purchase < 0

UNION ALL

SELECT
    'kpi_funnel_month' AS table_name,
    year_month AS slice_key,
    n_sessions,
    n_reach_home,
    n_reach_view,
    n_reach_cart,
    n_reach_purchase
FROM {{ ref('kpi_funnel_month') }}
WHERE n_sessions < n_reach_home
   OR n_reach_home < n_reach_view
   OR n_reach_view < n_reach_cart
   OR n_reach_cart < n_reach_purchase
   OR n_reach_purchase < 0

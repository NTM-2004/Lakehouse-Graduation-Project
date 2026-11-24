{#
  dbt/tests/gold_gt14.sql - GT-14: Cờ mẫu nhỏ, mẫu số bằng 0 trả NULL và order_growth (B-430)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (Design Gold v2 - Kiểm thử Gold GT-14; Mẫu nhỏ; Tăng trưởng đơn)
#}

{{ config(severity='error') }}

-- 1. kpi_sales_month: is_small_sample (< 100 đơn hợp lệ), chia 0 -> NULL, order_growth
WITH sales_with_prev AS (
    SELECT
        year_month,
        n_orders_total,
        n_valid_orders,
        avg_order_value,
        cancel_rate,
        order_growth,
        is_small_sample,
        LAG(n_valid_orders) OVER (ORDER BY year_month) AS prev_valid_orders
    FROM {{ ref('kpi_sales_month') }}
)
SELECT 'kpi_sales_month' AS table_name, year_month AS slice_key
FROM sales_with_prev
WHERE is_small_sample != (n_valid_orders < {{ var('small_sample_orders', 100) }})
   OR (n_valid_orders = 0 AND avg_order_value IS NOT NULL)
   OR (n_orders_total = 0 AND cancel_rate IS NOT NULL)
   OR ((prev_valid_orders IS NULL OR prev_valid_orders < {{ var('small_sample_orders', 100) }}) AND order_growth IS NOT NULL)

UNION ALL
-- 2. kpi_sales_state_month: is_small_sample (< 30 đơn hợp lệ) & chia 0 -> NULL
SELECT 'kpi_sales_state_month', customer_state || '|' || year_month
FROM {{ ref('kpi_sales_state_month') }}
WHERE is_small_sample != (n_valid_orders < {{ var('small_sample_group', 30) }})
   OR (n_valid_orders = 0 AND avg_order_value IS NOT NULL)
   OR (n_orders_total = 0 AND cancel_rate IS NOT NULL)

UNION ALL
-- 3. kpi_delivery_month: is_small_sample (< 100 đơn hợp lệ OR < 30 đơn đã giao), is_small_gap_group, chia 0
SELECT 'kpi_delivery_month', year_month
FROM {{ ref('kpi_delivery_month') }}
WHERE is_small_sample != (n_valid_orders < {{ var('small_sample_orders', 100) }} OR n_delivered < {{ var('small_sample_group', 30) }})
   OR is_small_gap_group != (n_scored_on_time < {{ var('small_sample_group', 30) }} OR n_scored_late < {{ var('small_sample_group', 30) }})
   OR (n_delivered = 0 AND (avg_delivery_days IS NOT NULL OR late_rate IS NOT NULL))
   OR (n_shipping_measured = 0 AND avg_shipping_days IS NOT NULL)
   OR (n_scored = 0 AND avg_score IS NOT NULL)
   OR ((n_scored_on_time = 0 OR n_scored_late = 0) AND score_gap IS NOT NULL)

UNION ALL
-- 4. kpi_delivery_state_month: is_small_sample (< 30 đơn đã giao), is_small_gap_group, chia 0
SELECT 'kpi_delivery_state_month', customer_state || '|' || year_month
FROM {{ ref('kpi_delivery_state_month') }}
WHERE is_small_sample != (n_delivered < {{ var('small_sample_group', 30) }})
   OR is_small_gap_group != (n_scored_on_time < {{ var('small_sample_group', 30) }} OR n_scored_late < {{ var('small_sample_group', 30) }})
   OR (n_delivered = 0 AND (avg_delivery_days IS NOT NULL OR late_rate IS NOT NULL))
   OR (n_shipping_measured = 0 AND avg_shipping_days IS NOT NULL)
   OR (n_scored = 0 AND avg_score IS NOT NULL)
   OR ((n_scored_on_time = 0 OR n_scored_late = 0) AND score_gap IS NOT NULL)

UNION ALL
-- 5. kpi_lead_month: is_small_sample (< 30 deal) & chia 0
SELECT 'kpi_lead_month', year_month
FROM {{ ref('kpi_lead_month') }}
WHERE is_small_sample != (n_leads_with_deal < {{ var('small_sample_group', 30) }})
   OR (n_leads = 0 AND lead_to_deal_rate IS NOT NULL)
   OR (n_close_time_valid_deals = 0 AND avg_days_to_close IS NOT NULL)
   OR (n_leads_not_excluded = 0 AND won_30d_rate IS NOT NULL)

UNION ALL
-- 6. kpi_lead_origin: is_small_sample (< 30 deal) & chia 0
SELECT 'kpi_lead_origin', origin
FROM {{ ref('kpi_lead_origin') }}
WHERE is_small_sample != (n_leads_with_deal < {{ var('small_sample_group', 30) }})
   OR (n_leads = 0 AND lead_to_deal_rate IS NOT NULL)
   OR (n_close_time_valid_deals = 0 AND avg_days_to_close IS NOT NULL)
   OR (n_leads_not_excluded = 0 AND won_30d_rate IS NOT NULL)

UNION ALL
-- 7. kpi_funnel_day: is_small_sample (< 30 phiên) & chia 0
SELECT 'kpi_funnel_day', CAST(session_date AS VARCHAR)
FROM {{ ref('kpi_funnel_day') }}
WHERE is_small_sample != (n_sessions < {{ var('small_sample_group', 30) }})
   OR (n_sessions = 0 AND conversion_rate IS NOT NULL)
   OR (n_cart_sessions = 0 AND abandon_rate IS NOT NULL)
   OR (n_sessions_ge2 = 0 AND avg_duration_seconds IS NOT NULL)
   OR (n_reach_home = 0 AND step_rate_home_view IS NOT NULL)
   OR (n_reach_view = 0 AND step_rate_view_cart IS NOT NULL)
   OR (n_reach_cart = 0 AND step_rate_cart_purchase IS NOT NULL)

UNION ALL
-- 8. kpi_funnel_month: is_small_sample (< 30 phiên) & chia 0
SELECT 'kpi_funnel_month', year_month
FROM {{ ref('kpi_funnel_month') }}
WHERE is_small_sample != (n_sessions < {{ var('small_sample_group', 30) }})
   OR (n_sessions = 0 AND conversion_rate IS NOT NULL)
   OR (n_cart_sessions = 0 AND abandon_rate IS NOT NULL)
   OR (n_sessions_ge2 = 0 AND avg_duration_seconds IS NOT NULL)
   OR (n_reach_home = 0 AND step_rate_home_view IS NOT NULL)
   OR (n_reach_view = 0 AND step_rate_view_cart IS NOT NULL)
   OR (n_reach_cart = 0 AND step_rate_cart_purchase IS NOT NULL)

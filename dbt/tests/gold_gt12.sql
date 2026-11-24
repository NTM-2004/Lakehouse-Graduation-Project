{#
  dbt/tests/gold_gt12.sql - GT-12: Thành phần cộng được của 8 bảng KPI khớp fact (B-428)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F08, Design Gold v2 - Kiểm thử Gold GT-12)
  Kiểm tra:
  - Tổng mỗi thành phần cộng được qua mọi dòng của từng bảng KPI bằng giá trị tính trực tiếp từ fact.
  - Hai bảng KPI cùng UC cho cùng tổng toàn quốc.
#}

{{ config(severity='error') }}

-- UC-01: kpi_sales_month vs fact_order
SELECT 'kpi_sales_month vs fact_order' AS check_name
FROM (
    SELECT
        COALESCE(SUM(n_orders_total), 0) AS n_orders_total,
        COALESCE(SUM(n_canceled_orders), 0) AS n_canceled_orders,
        COALESCE(SUM(n_valid_orders), 0) AS n_valid_orders,
        COALESCE(SUM(sum_revenue), 0) AS sum_revenue
    FROM {{ ref('kpi_sales_month') }}
) k
CROSS JOIN (
    SELECT
        COUNT(*) AS n_orders_total,
        SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) AS n_canceled_orders,
        SUM(CASE WHEN is_valid_order THEN 1 ELSE 0 END) AS n_valid_orders,
        COALESCE(SUM(CASE WHEN is_valid_order THEN order_revenue ELSE 0 END), 0) AS sum_revenue
    FROM {{ ref('fact_order') }}
) f
WHERE k.n_orders_total != f.n_orders_total
   OR k.n_canceled_orders != f.n_canceled_orders
   OR k.n_valid_orders != f.n_valid_orders
   OR ABS(k.sum_revenue - f.sum_revenue) > 0.005

UNION ALL
-- UC-01: kpi_sales_state_month vs fact_order
SELECT 'kpi_sales_state_month vs fact_order'
FROM (
    SELECT
        COALESCE(SUM(n_orders_total), 0) AS n_orders_total,
        COALESCE(SUM(n_canceled_orders), 0) AS n_canceled_orders,
        COALESCE(SUM(n_valid_orders), 0) AS n_valid_orders,
        COALESCE(SUM(sum_revenue), 0) AS sum_revenue
    FROM {{ ref('kpi_sales_state_month') }}
) k
CROSS JOIN (
    SELECT
        COUNT(*) AS n_orders_total,
        SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) AS n_canceled_orders,
        SUM(CASE WHEN is_valid_order THEN 1 ELSE 0 END) AS n_valid_orders,
        COALESCE(SUM(CASE WHEN is_valid_order THEN order_revenue ELSE 0 END), 0) AS sum_revenue
    FROM {{ ref('fact_order') }}
) f
WHERE k.n_orders_total != f.n_orders_total
   OR k.n_canceled_orders != f.n_canceled_orders
   OR k.n_valid_orders != f.n_valid_orders
   OR ABS(k.sum_revenue - f.sum_revenue) > 0.005

UNION ALL
-- UC-02: kpi_delivery_month vs fact_order
SELECT 'kpi_delivery_month vs fact_order'
FROM (
    SELECT
        COALESCE(SUM(n_delivered), 0) AS n_delivered,
        COALESCE(SUM(sum_delivery_days), 0) AS sum_delivery_days,
        COALESCE(SUM(n_late), 0) AS n_late,
        COALESCE(SUM(n_on_time), 0) AS n_on_time,
        COALESCE(SUM(n_shipping_measured), 0) AS n_shipping_measured,
        COALESCE(SUM(sum_shipping_days), 0) AS sum_shipping_days,
        COALESCE(SUM(n_scored), 0) AS n_scored,
        COALESCE(SUM(sum_score), 0) AS sum_score,
        COALESCE(SUM(n_overdue_undelivered), 0) AS n_overdue_undelivered
    FROM {{ ref('kpi_delivery_month') }}
) k
CROSS JOIN (
    SELECT
        SUM(CASE WHEN is_delivered_order THEN 1 ELSE 0 END) AS n_delivered,
        COALESCE(SUM(CASE WHEN is_delivered_order THEN delivery_days ELSE 0 END), 0) AS sum_delivery_days,
        SUM(CASE WHEN is_delivered_order AND is_late THEN 1 ELSE 0 END) AS n_late,
        SUM(CASE WHEN is_delivered_order AND NOT is_late THEN 1 ELSE 0 END) AS n_on_time,
        SUM(CASE WHEN is_delivered_order AND shipping_days IS NOT NULL THEN 1 ELSE 0 END) AS n_shipping_measured,
        COALESCE(SUM(CASE WHEN is_delivered_order AND shipping_days IS NOT NULL THEN shipping_days ELSE 0 END), 0) AS sum_shipping_days,
        SUM(CASE WHEN review_score IS NOT NULL THEN 1 ELSE 0 END) AS n_scored,
        COALESCE(SUM(CASE WHEN review_score IS NOT NULL THEN review_score ELSE 0 END), 0) AS sum_score,
        SUM(CASE WHEN is_overdue_undelivered THEN 1 ELSE 0 END) AS n_overdue_undelivered
    FROM {{ ref('fact_order') }}
) f
WHERE k.n_delivered != f.n_delivered
   OR k.sum_delivery_days != f.sum_delivery_days
   OR k.n_late != f.n_late
   OR k.n_on_time != f.n_on_time
   OR k.n_shipping_measured != f.n_shipping_measured
   OR k.sum_shipping_days != f.sum_shipping_days
   OR k.n_scored != f.n_scored
   OR k.sum_score != f.sum_score
   OR k.n_overdue_undelivered != f.n_overdue_undelivered

UNION ALL
-- UC-02: kpi_delivery_state_month vs kpi_delivery_month
SELECT 'kpi_delivery_state_month vs kpi_delivery_month'
FROM (
    SELECT
        COALESCE(SUM(n_delivered), 0) AS n_delivered,
        COALESCE(SUM(sum_delivery_days), 0) AS sum_delivery_days,
        COALESCE(SUM(n_late), 0) AS n_late,
        COALESCE(SUM(n_scored), 0) AS n_scored,
        COALESCE(SUM(sum_score), 0) AS sum_score,
        COALESCE(SUM(n_overdue_undelivered), 0) AS n_overdue_undelivered
    FROM {{ ref('kpi_delivery_state_month') }}
) a
CROSS JOIN (
    SELECT
        COALESCE(SUM(n_delivered), 0) AS n_delivered,
        COALESCE(SUM(sum_delivery_days), 0) AS sum_delivery_days,
        COALESCE(SUM(n_late), 0) AS n_late,
        COALESCE(SUM(n_scored), 0) AS n_scored,
        COALESCE(SUM(sum_score), 0) AS sum_score,
        COALESCE(SUM(n_overdue_undelivered), 0) AS n_overdue_undelivered
    FROM {{ ref('kpi_delivery_month') }}
) b
WHERE a.n_delivered != b.n_delivered
   OR a.sum_delivery_days != b.sum_delivery_days
   OR a.n_late != b.n_late
   OR a.n_scored != b.n_scored
   OR a.sum_score != b.sum_score
   OR a.n_overdue_undelivered != b.n_overdue_undelivered

UNION ALL
-- UC-03: kpi_lead_month vs fact_lead
SELECT 'kpi_lead_month vs fact_lead'
FROM (
    SELECT
        COALESCE(SUM(n_leads), 0) AS n_leads,
        COALESCE(SUM(n_leads_with_deal), 0) AS n_leads_with_deal,
        COALESCE(SUM(n_close_time_valid_deals), 0) AS n_close_time_valid_deals,
        COALESCE(SUM(sum_days_to_close), 0) AS sum_days_to_close,
        COALESCE(SUM(n_leads_not_excluded), 0) AS n_leads_not_excluded,
        COALESCE(SUM(n_won_30d), 0) AS n_won_30d
    FROM {{ ref('kpi_lead_month') }}
) k
CROSS JOIN (
    SELECT
        COUNT(*) AS n_leads,
        SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) AS n_leads_with_deal,
        SUM(CASE WHEN has_deal AND NOT is_close_time_excluded AND days_to_close IS NOT NULL THEN 1 ELSE 0 END) AS n_close_time_valid_deals,
        COALESCE(SUM(CASE WHEN has_deal AND NOT is_close_time_excluded AND days_to_close IS NOT NULL THEN days_to_close ELSE 0 END), 0) AS sum_days_to_close,
        SUM(CASE WHEN NOT is_close_time_excluded THEN 1 ELSE 0 END) AS n_leads_not_excluded,
        SUM(CASE WHEN has_deal AND NOT is_close_time_excluded AND days_to_close IS NOT NULL AND days_to_close <= 30 THEN 1 ELSE 0 END) AS n_won_30d
    FROM {{ ref('fact_lead') }}
) f
WHERE k.n_leads != f.n_leads
   OR k.n_leads_with_deal != f.n_leads_with_deal
   OR k.n_close_time_valid_deals != f.n_close_time_valid_deals
   OR k.sum_days_to_close != f.sum_days_to_close
   OR k.n_leads_not_excluded != f.n_leads_not_excluded
   OR k.n_won_30d != f.n_won_30d

UNION ALL
-- UC-03: kpi_lead_origin vs kpi_lead_month
SELECT 'kpi_lead_origin vs kpi_lead_month'
FROM (
    SELECT
        COALESCE(SUM(n_leads), 0) AS n_leads,
        COALESCE(SUM(n_leads_with_deal), 0) AS n_leads_with_deal,
        COALESCE(SUM(n_close_time_valid_deals), 0) AS n_close_time_valid_deals,
        COALESCE(SUM(sum_days_to_close), 0) AS sum_days_to_close,
        COALESCE(SUM(n_leads_not_excluded), 0) AS n_leads_not_excluded,
        COALESCE(SUM(n_won_30d), 0) AS n_won_30d
    FROM {{ ref('kpi_lead_origin') }}
) a
CROSS JOIN (
    SELECT
        COALESCE(SUM(n_leads), 0) AS n_leads,
        COALESCE(SUM(n_leads_with_deal), 0) AS n_leads_with_deal,
        COALESCE(SUM(n_close_time_valid_deals), 0) AS n_close_time_valid_deals,
        COALESCE(SUM(sum_days_to_close), 0) AS sum_days_to_close,
        COALESCE(SUM(n_leads_not_excluded), 0) AS n_leads_not_excluded,
        COALESCE(SUM(n_won_30d), 0) AS n_won_30d
    FROM {{ ref('kpi_lead_month') }}
) b
WHERE a.n_leads != b.n_leads
   OR a.n_leads_with_deal != b.n_leads_with_deal
   OR a.n_close_time_valid_deals != b.n_close_time_valid_deals
   OR a.sum_days_to_close != b.sum_days_to_close
   OR a.n_leads_not_excluded != b.n_leads_not_excluded
   OR a.n_won_30d != b.n_won_30d

UNION ALL
-- UC-04: kpi_funnel_day vs fact_session
SELECT 'kpi_funnel_day vs fact_session'
FROM (
    SELECT
        COALESCE(SUM(n_sessions), 0) AS n_sessions,
        COALESCE(SUM(n_purchase_sessions), 0) AS n_purchase_sessions,
        COALESCE(SUM(n_cart_sessions), 0) AS n_cart_sessions,
        COALESCE(SUM(n_cart_abandoned_sessions), 0) AS n_cart_abandoned_sessions,
        COALESCE(SUM(n_sessions_ge2), 0) AS n_sessions_ge2,
        COALESCE(SUM(sum_duration_seconds), 0) AS sum_duration_seconds
    FROM {{ ref('kpi_funnel_day') }}
) k
CROSS JOIN (
    SELECT
        COUNT(*) AS n_sessions,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS n_purchase_sessions,
        SUM(CASE WHEN has_add_to_cart THEN 1 ELSE 0 END) AS n_cart_sessions,
        SUM(CASE WHEN has_add_to_cart AND NOT has_purchase THEN 1 ELSE 0 END) AS n_cart_abandoned_sessions,
        SUM(CASE WHEN n_events >= 2 THEN 1 ELSE 0 END) AS n_sessions_ge2,
        COALESCE(SUM(CASE WHEN n_events >= 2 THEN duration_seconds ELSE 0 END), 0) AS sum_duration_seconds
    FROM {{ ref('fact_session') }}
) f
WHERE k.n_sessions != f.n_sessions
   OR k.n_purchase_sessions != f.n_purchase_sessions
   OR k.n_cart_sessions != f.n_cart_sessions
   OR k.n_cart_abandoned_sessions != f.n_cart_abandoned_sessions
   OR k.n_sessions_ge2 != f.n_sessions_ge2
   OR k.sum_duration_seconds != f.sum_duration_seconds

UNION ALL
-- UC-04: kpi_funnel_month vs kpi_funnel_day
SELECT 'kpi_funnel_month vs kpi_funnel_day'
FROM (
    SELECT
        COALESCE(SUM(n_sessions), 0) AS n_sessions,
        COALESCE(SUM(n_purchase_sessions), 0) AS n_purchase_sessions,
        COALESCE(SUM(n_cart_sessions), 0) AS n_cart_sessions,
        COALESCE(SUM(sum_duration_seconds), 0) AS sum_duration_seconds
    FROM {{ ref('kpi_funnel_month') }}
) a
CROSS JOIN (
    SELECT
        COALESCE(SUM(n_sessions), 0) AS n_sessions,
        COALESCE(SUM(n_purchase_sessions), 0) AS n_purchase_sessions,
        COALESCE(SUM(n_cart_sessions), 0) AS n_cart_sessions,
        COALESCE(SUM(sum_duration_seconds), 0) AS sum_duration_seconds
    FROM {{ ref('kpi_funnel_day') }}
) b
WHERE a.n_sessions != b.n_sessions
   OR a.n_purchase_sessions != b.n_purchase_sessions
   OR a.n_cart_sessions != b.n_cart_sessions
   OR a.sum_duration_seconds != b.sum_duration_seconds

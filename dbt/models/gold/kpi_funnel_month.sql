{#
  dbt/models/gold/kpi_funnel_month.sql - Bảng KPI phễu hành vi UC-04 theo tháng (B-416)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-06
  Tham chiếu:
  - Requirement v3 (UC-04; Mẫu nhỏ)
  - Design Gold v2 (Mục V: Bảng lát cắt, Mẫu cột chung, Thành phần theo UC-04; Mục VII: Đạt bước của phễu)
  - dbt/schema_contract.yml (gold_tables.kpi_funnel_month - 19 cột)

  Lát cắt: year_month (tháng của session_date, lấy từ dim_date).
  Dòng có mặt: mọi tháng có ít nhất một phiên.
  Cờ mẫu nhỏ: nhóm dưới 30 phiên (n_obs = n_sessions).
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH base AS (
    SELECT
        d.year_month,
        COUNT(*) AS n_sessions,
        SUM(CASE WHEN f.has_home THEN 1 ELSE 0 END) AS n_reach_home,
        SUM(CASE WHEN f.has_home AND f.has_product_view THEN 1 ELSE 0 END) AS n_reach_view,
        SUM(CASE WHEN f.has_home AND f.has_product_view AND f.has_add_to_cart THEN 1 ELSE 0 END) AS n_reach_cart,
        SUM(CASE WHEN f.has_home AND f.has_product_view AND f.has_add_to_cart AND f.has_purchase THEN 1 ELSE 0 END) AS n_reach_purchase,
        SUM(CASE WHEN f.has_purchase THEN 1 ELSE 0 END) AS n_purchase_sessions,
        SUM(CASE WHEN f.has_add_to_cart THEN 1 ELSE 0 END) AS n_cart_sessions,
        SUM(CASE WHEN f.has_add_to_cart AND NOT f.has_purchase THEN 1 ELSE 0 END) AS n_cart_abandoned_sessions,
        SUM(CASE WHEN f.n_events >= 2 THEN 1 ELSE 0 END) AS n_sessions_ge2,
        COALESCE(SUM(CASE WHEN f.n_events >= 2 THEN f.duration_seconds END), 0) AS sum_duration_seconds
    FROM {{ ref('fact_session') }} f
    INNER JOIN {{ ref('dim_date') }} d
        ON f.session_date = d."date"
    GROUP BY d.year_month
)

SELECT
    year_month,
    n_sessions,
    n_reach_home,
    n_reach_view,
    n_reach_cart,
    n_reach_purchase,
    n_purchase_sessions,
    n_cart_sessions,
    n_cart_abandoned_sessions,
    n_sessions_ge2,
    sum_duration_seconds,
    CASE
        WHEN n_sessions > 0 THEN CAST(n_purchase_sessions AS DOUBLE) / n_sessions
        ELSE NULL
    END AS conversion_rate,
    CASE
        WHEN n_cart_sessions > 0 THEN CAST(n_cart_abandoned_sessions AS DOUBLE) / n_cart_sessions
        ELSE NULL
    END AS abandon_rate,
    CASE
        WHEN n_sessions_ge2 > 0 THEN CAST(sum_duration_seconds AS DOUBLE) / n_sessions_ge2
        ELSE NULL
    END AS avg_duration_seconds,
    CASE
        WHEN n_reach_home > 0 THEN CAST(n_reach_view AS DOUBLE) / n_reach_home
        ELSE NULL
    END AS step_rate_home_view,
    CASE
        WHEN n_reach_view > 0 THEN CAST(n_reach_cart AS DOUBLE) / n_reach_view
        ELSE NULL
    END AS step_rate_view_cart,
    CASE
        WHEN n_reach_cart > 0 THEN CAST(n_reach_purchase AS DOUBLE) / n_reach_cart
        ELSE NULL
    END AS step_rate_cart_purchase,
    n_sessions AS n_obs,
    {{ is_small_sample('n_sessions') }} AS is_small_sample
FROM base

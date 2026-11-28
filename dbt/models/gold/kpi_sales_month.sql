{#
  dbt/models/gold/kpi_sales_month.sql - Bảng KPI UC-01 theo tháng mua (B-409)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-06
  Tham chiếu:
  - Requirement v3 (UC-01: Doanh thu đặt mua, Số đơn hợp lệ, Tăng trưởng đơn, Giá trị trung bình đơn, Tỷ lệ hủy; Mẫu nhỏ)
  - Design Gold v2 (Mục V: Bảng lát cắt, Mẫu cột chung, Thành phần theo UC-01; Mục VII: Tháng không có đơn)
  - dbt/schema_contract.yml (gold_tables.kpi_sales_month - 10 cột)

  Lát cắt: year_month (mọi tháng có mặt trong dim_date, 24 dòng kể cả tháng không có đơn).
  Cờ mẫu nhỏ: tháng dưới 100 đơn hợp lệ (n_obs = n_valid_orders).
  order_growth: NULL nếu không có tháng liền trước hoặc tháng trước dưới 100 đơn hợp lệ.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH all_months AS (
    SELECT DISTINCT year_month
    FROM {{ ref('dim_date') }}
),

monthly_agg AS (
    SELECT
        d.year_month,
        COUNT(*) AS n_orders_total,
        SUM(CASE WHEN f.is_canceled THEN 1 ELSE 0 END) AS n_canceled_orders,
        SUM(CASE WHEN f.is_valid_order THEN 1 ELSE 0 END) AS n_valid_orders,
        CAST(
            COALESCE(SUM(CASE WHEN f.is_valid_order THEN f.order_revenue ELSE 0 END), 0)
            AS DECIMAL(14, 2)
        ) AS sum_revenue
    FROM {{ ref('fact_order') }} f
    INNER JOIN {{ ref('dim_date') }} d
        ON f.purchase_date = d."date"
    GROUP BY d.year_month
),

base AS (
    SELECT
        m.year_month,
        COALESCE(a.n_orders_total, 0) AS n_orders_total,
        COALESCE(a.n_canceled_orders, 0) AS n_canceled_orders,
        COALESCE(a.n_valid_orders, 0) AS n_valid_orders,
        COALESCE(a.sum_revenue, CAST(0 AS DECIMAL(14, 2))) AS sum_revenue
    FROM all_months m
    LEFT JOIN monthly_agg a
        ON m.year_month = a.year_month
),

with_prev AS (
    SELECT
        year_month,
        n_orders_total,
        n_canceled_orders,
        n_valid_orders,
        sum_revenue,
        LAG(n_valid_orders) OVER (ORDER BY year_month) AS prev_valid_orders
    FROM base
)

SELECT
    year_month,
    n_orders_total,
    n_canceled_orders,
    n_valid_orders,
    sum_revenue,
    CASE
        WHEN n_valid_orders > 0 THEN CAST(sum_revenue AS DOUBLE) / n_valid_orders
        ELSE CAST(NULL AS DOUBLE)
    END AS avg_order_value,
    CASE
        WHEN n_orders_total > 0 THEN CAST(n_canceled_orders AS DOUBLE) / n_orders_total
        ELSE CAST(NULL AS DOUBLE)
    END AS cancel_rate,
    CASE
        WHEN prev_valid_orders IS NOT NULL
             AND prev_valid_orders >= {{ var('small_sample_orders', 100) }}
        THEN CAST(n_valid_orders - prev_valid_orders AS DOUBLE) / prev_valid_orders
        ELSE CAST(NULL AS DOUBLE)
    END AS order_growth,
    n_valid_orders AS n_obs,
    {{ is_small_sample('n_valid_orders', 'n_valid_orders') }} AS is_small_sample
FROM with_prev
ORDER BY year_month

{#
  dbt/models/gold/kpi_sales_state_month.sql - Bảng KPI UC-01 theo bang và tháng mua (B-410)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-06
  Tham chiếu:
  - Requirement v3 (UC-01; Mẫu nhỏ; Hiệu năng truy vấn Q1)
  - Design Gold v2 (Mục V: Bảng lát cắt, Mẫu cột chung, Thành phần theo UC-01; Mục VI: Truy vấn Q1)
  - dbt/schema_contract.yml (gold_tables.kpi_sales_state_month - 10 cột)

  Lát cắt: (customer_state, year_month) - chỉ giữ cặp bang và tháng có ít nhất một đơn.
  Cờ mẫu nhỏ: nhóm dưới 30 đơn hợp lệ (n_obs = n_valid_orders).
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH base AS (
    SELECT
        c.customer_state,
        d.year_month,
        COUNT(*) AS n_orders_total,
        SUM(CASE WHEN f.is_canceled THEN 1 ELSE 0 END) AS n_canceled_orders,
        SUM(CASE WHEN f.is_valid_order THEN 1 ELSE 0 END) AS n_valid_orders,
        CAST(
            COALESCE(SUM(CASE WHEN f.is_valid_order THEN f.order_revenue ELSE 0 END), 0)
            AS DECIMAL(14, 2)
        ) AS sum_revenue
    FROM {{ ref('fact_order') }} f
    INNER JOIN {{ ref('dim_customer') }} c
        ON f.customer_id = c.customer_id
    INNER JOIN {{ ref('dim_date') }} d
        ON f.purchase_date = d."date"
    GROUP BY c.customer_state, d.year_month
)

SELECT
    customer_state,
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
    n_valid_orders AS n_obs,
    {{ is_small_sample('n_valid_orders') }} AS is_small_sample
FROM base

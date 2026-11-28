{#
  dbt/models/gold/fact_order.sql - Bảng Fact đơn hàng Olist (B-405)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-05
  Tham chiếu:
  - Requirement v3 (Quy ước chung: Đơn hợp lệ, Đơn hủy, Đơn đã giao, Đơn quá hạn chưa giao, Đánh giá)
  - Design Gold v2 (Mục III: gold.fact_order; Mục IV: Xử lý cờ D1, D2, D4, R2, AN-04, AN-16; Mục VII: Hòa khi chọn đánh giá, Đơn đã giao)
  - dbt/schema_contract.yml (gold_tables.fact_order - 15 cột)

  Grain: 1 dòng = 1 đơn hàng (order_id), giữ mọi trạng thái trong phạm vi (kể cả đơn AN-04 không có dòng hàng).
  Ghi chú Dremio 25.0: review_score dùng kiểu INTEGER (Dremio không có kiểu TINYINT, đồng bộ với silver.order_reviews).
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH items_agg AS (
    SELECT
        order_id,
        CAST(COUNT(*) AS INTEGER) AS n_items,
        CAST(SUM(price) AS DECIMAL(14, 2)) AS order_revenue
    FROM {{ ref('order_items') }}
    GROUP BY order_id
),

eligible_reviews AS (
    SELECT
        order_id,
        review_score,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                review_answer_timestamp DESC NULLS LAST,
                review_creation_date DESC NULLS LAST,
                review_id DESC NULLS LAST
        ) AS rn
    FROM {{ ref('order_reviews') }}
    WHERE review_answer_timestamp IS NOT NULL
      AND review_score IS NOT NULL
      AND NOT {{ has_flag('dq_flag', 'R2') }}
),

chosen_reviews AS (
    SELECT
        order_id,
        review_score
    FROM eligible_reviews
    WHERE rn = 1
),

order_base AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        CAST(COALESCE(i.n_items, 0) AS INTEGER) AS n_items,
        CAST(COALESCE(i.order_revenue, 0) AS DECIMAL(14, 2)) AS order_revenue,
        CASE
            WHEN COALESCE(o.order_status, '') NOT IN ('canceled', 'unavailable')
                 AND COALESCE(i.n_items, 0) > 0
            THEN true
            ELSE false
        END AS is_valid_order,
        CASE
            WHEN o.order_status IN ('canceled', 'unavailable')
            THEN true
            ELSE false
        END AS is_canceled,
        CASE
            WHEN o.order_status = 'delivered'
                 AND o.order_delivered_customer_date IS NOT NULL
                 AND NOT {{ has_flag('o.dq_flag', 'D1') }}
                 AND NOT {{ has_flag('o.dq_flag', 'D2') }}
                 AND NOT {{ has_flag('o.dq_flag', 'D4') }}
            THEN true
            ELSE false
        END AS is_delivered_order,
        CASE
            WHEN o.order_status = 'shipped'
                 AND o.order_delivered_customer_date IS NULL
                 AND o.order_estimated_delivery_date IS NOT NULL
                 AND CAST(o.order_estimated_delivery_date AS DATE) <= CAST({{ get_cutoff_date() }} AS DATE)
            THEN true
            ELSE false
        END AS is_overdue_undelivered,
        CAST(r.review_score AS INTEGER) AS review_score,
        o.batch_id
    FROM {{ ref('orders') }} o
    LEFT JOIN items_agg i
        ON o.order_id = i.order_id
    LEFT JOIN chosen_reviews r
        ON o.order_id = r.order_id
)

SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS DATE) AS purchase_date,
    n_items,
    order_revenue,
    is_valid_order,
    is_canceled,
    is_delivered_order,
    CASE
        WHEN is_delivered_order
        THEN CAST({{ calendar_days('order_delivered_customer_date', 'order_purchase_timestamp') }} AS INTEGER)
        ELSE CAST(NULL AS INTEGER)
    END AS delivery_days,
    CASE
        WHEN is_delivered_order AND order_delivered_carrier_date IS NOT NULL
        THEN CAST({{ calendar_days('order_delivered_customer_date', 'order_delivered_carrier_date') }} AS INTEGER)
        ELSE CAST(NULL AS INTEGER)
    END AS shipping_days,
    CASE
        WHEN is_delivered_order
             AND order_delivered_customer_date IS NOT NULL
             AND order_estimated_delivery_date IS NOT NULL
        THEN CASE
            WHEN CAST(order_delivered_customer_date AS DATE) > CAST(order_estimated_delivery_date AS DATE)
            THEN true
            ELSE false
        END
        ELSE CAST(NULL AS BOOLEAN)
    END AS is_late,
    is_overdue_undelivered,
    review_score,
    batch_id
FROM order_base

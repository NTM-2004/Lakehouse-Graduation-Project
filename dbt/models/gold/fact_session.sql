{#
  dbt/models/gold/fact_session.sql - Bảng Fact Gold cho Clickstream Session (B-408)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-05
  Tham chiếu:
  - Design Gold v2 (Mục III: gold.fact_session; Mục VII: Phiên mang đơn, Phiên không gắn khách, Phân vùng và batch_id)
  - dbt/schema_contract.yml (gold_tables.fact_session - 11 cột)

  Grain: 1 dòng = 1 phiên duyệt web (session_id), gom trên toàn bảng silver.clickstream_events.
  Khóa chính: session_id. Không có batch_id vì một phiên có thể trải qua hai batch.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH session_agg AS (
    SELECT
        e.session_id,
        MIN(e.event_ts) AS first_event_ts,
        MAX(e.event_ts) AS last_event_ts,
        CAST(COUNT(e.event_id) AS INTEGER) AS n_events,
        MAX(CASE WHEN e.event_type = 'home' THEN 1 ELSE 0 END) AS has_home_int,
        MAX(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END) AS has_product_view_int,
        MAX(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_add_to_cart_int,
        MAX(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase_int,
        MAX(CASE WHEN e.event_type = 'purchase' THEN e.order_id END) AS order_id,
        MAX(CASE WHEN e.event_type = 'purchase' THEN e.amount END) AS purchase_amount
    FROM {{ ref('clickstream_events') }} e
    GROUP BY e.session_id
)

SELECT
    session_id,
    CAST(first_event_ts AS DATE) AS session_date,
    first_event_ts,
    CAST(TIMESTAMPDIFF(SECOND, first_event_ts, last_event_ts) AS INTEGER) AS duration_seconds,
    n_events,
    CASE WHEN has_home_int = 1 THEN true ELSE false END AS has_home,
    CASE WHEN has_product_view_int = 1 THEN true ELSE false END AS has_product_view,
    CASE WHEN has_add_to_cart_int = 1 THEN true ELSE false END AS has_add_to_cart,
    CASE WHEN has_purchase_int = 1 THEN true ELSE false END AS has_purchase,
    order_id,
    CAST(purchase_amount AS DECIMAL(10, 2)) AS purchase_amount
FROM session_agg

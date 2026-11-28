{#
  dbt/models/gold/fact_order_item.sql - Bảng Fact dòng hàng Olist (B-406)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-05
  Tham chiếu:
  - Design Gold v2 (Mục III: gold.fact_order_item; Mục VI: Truy vấn chuẩn Q2)
  - dbt/schema_contract.yml (gold_tables.fact_order_item - 7 cột)

  Grain: 1 dòng = 1 dòng hàng, khóa ghép (order_id, order_item_id).
  Nối LEFT JOIN silver.orders để lấy purchase_date của đơn cha (giữ đủ số dòng khớp GT-04;
  mọi đơn cha thiếu nếu có sẽ bị bắt ở kiểm thử khóa ngoại GT-03).
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    CAST(o.order_purchase_timestamp AS DATE) AS purchase_date,
    oi.batch_id
FROM {{ ref('order_items') }} oi
LEFT JOIN {{ ref('orders') }} o
    ON oi.order_id = o.order_id

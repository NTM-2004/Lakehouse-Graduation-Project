{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/order_items.sql - Mô hình Silver cho bảng dòng hàng Olist (B-311)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Cột bắt buộc mức 1, Bảng tra cờ I1 & T1; Mục IV: silver.order_items)
  - Requirement v3: Đơn hợp lệ (dòng hàng price <= 0 là lỗi mức 1, vào bảng reject)
  - dbt/schema_contract.yml (silver_tables.order_items - 10 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.order_items, tách lỗi mức 1:
     - Thiếu order_id
     - Thiếu hoặc không ép được order_item_id sang INTEGER
     - Thiếu, không ép được price sang DECIMAL(10, 2) hoặc price <= 0
  2. Ép kiểu an toàn (safe_cast) cho shipping_limit_date (TIMESTAMP) và freight_value (DECIMAL(10, 2)).
  3. Nối LEFT JOIN silver.orders để tính cờ I1 (SHIPPING_LIMIT_OUTLIER):
     chênh lệch ngày lịch (calendar_days) giữa shipping_limit_date và order_purchase_timestamp > 148 ngày.
  4. Gắn cờ I1 và T1:<cột> qua macro build_dq_flag.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(order_item_id), '') AS raw_order_item_id,
        {{ safe_cast("NULLIF(TRIM(order_item_id), '')", 'INTEGER') }} AS casted_order_item_id,
        NULLIF(TRIM(product_id), '') AS clean_product_id,
        NULLIF(TRIM(seller_id), '') AS clean_seller_id,
        NULLIF(TRIM(shipping_limit_date), '') AS raw_shipping_limit_date,
        NULLIF(TRIM(price), '') AS raw_price,
        {{ safe_cast("NULLIF(TRIM(price), '')", 'DECIMAL(10, 2)') }} AS casted_price,
        NULLIF(TRIM(freight_value), '') AS raw_freight_value,
        NULLIF(TRIM(source_file), '') AS clean_source_file,
        NULLIF(TRIM(batch_id), '') AS clean_batch_id
    FROM {{ source('bronze_olist', 'order_items') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_order_id IS NULL", "MISSING_ORDER_ID"),
            ("raw_order_item_id IS NULL", "MISSING_ORDER_ITEM_ID"),
            ("raw_order_item_id IS NOT NULL AND casted_order_item_id IS NULL", "INVALID_ORDER_ITEM_ID"),
            ("raw_price IS NULL", "MISSING_PRICE"),
            ("raw_price IS NOT NULL AND casted_price IS NULL", "INVALID_PRICE"),
            ("casted_price IS NOT NULL AND casted_price <= 0", "NON_POSITIVE_PRICE")
        ]) }} AS reject_reason
    FROM bronze_src
),

valid_rows AS (
    SELECT
        clean_order_id AS order_id,
        casted_order_item_id AS order_item_id,
        clean_product_id AS product_id,
        clean_seller_id AS seller_id,
        raw_shipping_limit_date,
        {{ safe_cast('raw_shipping_limit_date', 'TIMESTAMP') }} AS shipping_limit_date,
        casted_price AS price,
        raw_freight_value,
        {{ safe_cast('raw_freight_value', 'DECIMAL(10, 2)') }} AS freight_value,
        clean_source_file AS source_file,
        clean_batch_id AS batch_id
    FROM evaluated
    WHERE NOT {{ is_reject('reject_reason') }}
),

with_orders AS (
    SELECT
        v.order_id,
        v.order_item_id,
        v.product_id,
        v.seller_id,
        v.raw_shipping_limit_date,
        v.shipping_limit_date,
        v.price,
        v.raw_freight_value,
        v.freight_value,
        o.order_purchase_timestamp,
        v.source_file,
        v.batch_id
    FROM valid_rows v
    LEFT JOIN {{ ref('orders') }} o
        ON v.order_id = o.order_id
)

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    order_item_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(seller_id AS VARCHAR) AS seller_id,
    shipping_limit_date,
    price,
    freight_value,
    {{ build_dq_flag([
        ("I1", "shipping_limit_date IS NOT NULL AND order_purchase_timestamp IS NOT NULL AND " ~ calendar_days('shipping_limit_date', 'order_purchase_timestamp') ~ " > 148"),
        ("T1:shipping_limit_date", "raw_shipping_limit_date IS NOT NULL AND shipping_limit_date IS NULL"),
        ("T1:freight_value", "raw_freight_value IS NOT NULL AND freight_value IS NULL")
    ]) }} AS dq_flag,
    CAST(source_file AS VARCHAR) AS source_file,
    CAST(batch_id AS VARCHAR) AS batch_id
FROM with_orders

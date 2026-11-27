{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/order_items_rejects.sql - Bảng reject mức 1 cho silver.order_items (B-311)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.order_items)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.order_items):
  - Thiếu order_id, thiếu/không ép được order_item_id sang INTEGER,
    hoặc price thiếu, không ép được sang DECIMAL(10, 2), hoặc <= 0.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(order_item_id), '') AS raw_order_item_id,
        {{ safe_cast("NULLIF(TRIM(order_item_id), '')", 'INTEGER') }} AS casted_order_item_id,
        NULLIF(TRIM(price), '') AS raw_price,
        {{ safe_cast("NULLIF(TRIM(price), '')", 'DECIMAL(10, 2)') }} AS casted_price
    FROM {{ source('bronze_olist', 'order_items') }}
),

evaluated AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_order_id IS NULL", "MISSING_ORDER_ID"),
            ("raw_order_item_id IS NULL", "MISSING_ORDER_ITEM_ID"),
            ("raw_order_item_id IS NOT NULL AND casted_order_item_id IS NULL", "INVALID_ORDER_ITEM_ID"),
            ("raw_price IS NULL", "MISSING_PRICE"),
            ("raw_price IS NOT NULL AND casted_price IS NULL", "INVALID_PRICE"),
            ("casted_price IS NOT NULL AND casted_price <= 0", "NON_POSITIVE_PRICE")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

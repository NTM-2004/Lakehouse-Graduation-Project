{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/orders_rejects.sql - Bảng reject mức 1 cho silver.orders (B-309)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.orders)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc trước deduplicate của silver.orders):
  - Dòng thiếu order_id, hoặc thiếu/không ép được order_purchase_timestamp sang TIMESTAMP
    chuyển vào silver.orders_rejects.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(order_purchase_timestamp), '') AS raw_purchase_ts,
        {{ safe_cast("NULLIF(TRIM(order_purchase_timestamp), '')", 'TIMESTAMP') }} AS casted_purchase_ts
    FROM {{ source('bronze_olist', 'orders') }}
),

evaluated AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_order_id IS NULL", "MISSING_ORDER_ID"),
            ("raw_purchase_ts IS NULL", "MISSING_ORDER_PURCHASE_TIMESTAMP"),
            ("raw_purchase_ts IS NOT NULL AND casted_purchase_ts IS NULL", "INVALID_ORDER_PURCHASE_TIMESTAMP")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

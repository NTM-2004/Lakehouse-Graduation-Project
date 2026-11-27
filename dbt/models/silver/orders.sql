{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/orders.sql - Mô hình Silver cho bảng đơn hàng Olist (B-309)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Loại trùng, Bảng tra cờ D1-D7 & T1, Cột bắt buộc mức 1; Mục IV: silver.orders)
  - dbt/schema_contract.yml (silver_tables.orders - 11 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.orders, tách lỗi mức 1 (thiếu/hỏng order_id hoặc order_purchase_timestamp)
     TRƯỚC KHI gọi latest_snapshot (theo quy tắc của latest_snapshot.sql).
  2. Loại trùng theo order_id bằng macro latest_snapshot (JOIN batch_seq, giữ bản chụp batch_seq lớn nhất).
  3. Ép kiểu an toàn (safe_cast) các mốc thời gian phụ:
     - order_approved_at, order_delivered_carrier_date, order_delivered_customer_date -> TIMESTAMP
     - order_estimated_delivery_date (dạng 'YYYY-MM-DD 00:00:00' trong CSV) -> ép TIMESTAMP rồi CAST sang DATE.
  4. Gắn cờ D1–D7 và T1:<cột> qua macro build_dq_flag.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(customer_id), '') AS clean_customer_id,
        LOWER(NULLIF(TRIM(order_status), '')) AS clean_order_status,
        NULLIF(TRIM(order_purchase_timestamp), '') AS raw_purchase_ts,
        {{ safe_cast("NULLIF(TRIM(order_purchase_timestamp), '')", 'TIMESTAMP') }} AS casted_purchase_ts,
        NULLIF(TRIM(order_approved_at), '') AS raw_approved_at,
        NULLIF(TRIM(order_delivered_carrier_date), '') AS raw_delivered_carrier_date,
        NULLIF(TRIM(order_delivered_customer_date), '') AS raw_delivered_customer_date,
        NULLIF(TRIM(order_estimated_delivery_date), '') AS raw_estimated_delivery_date,
        NULLIF(TRIM(source_file), '') AS source_file,
        NULLIF(TRIM(batch_id), '') AS batch_id
    FROM {{ source('bronze_olist', 'orders') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_order_id IS NULL", "MISSING_ORDER_ID"),
            ("raw_purchase_ts IS NULL", "MISSING_ORDER_PURCHASE_TIMESTAMP"),
            ("raw_purchase_ts IS NOT NULL AND casted_purchase_ts IS NULL", "INVALID_ORDER_PURCHASE_TIMESTAMP")
        ]) }} AS reject_reason
    FROM bronze_src
),

valid_rows AS (
    SELECT
        clean_order_id AS order_id,
        clean_customer_id AS customer_id,
        clean_order_status AS order_status,
        casted_purchase_ts AS order_purchase_timestamp,
        raw_approved_at,
        raw_delivered_carrier_date,
        raw_delivered_customer_date,
        raw_estimated_delivery_date,
        source_file,
        batch_id
    FROM evaluated
    WHERE NOT {{ is_reject('reject_reason') }}
),

deduped AS (
    {{ latest_snapshot('valid_rows', 'order_id') }}
),

typed AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        raw_approved_at,
        {{ safe_cast('raw_approved_at', 'TIMESTAMP') }} AS order_approved_at,
        raw_delivered_carrier_date,
        {{ safe_cast('raw_delivered_carrier_date', 'TIMESTAMP') }} AS order_delivered_carrier_date,
        raw_delivered_customer_date,
        {{ safe_cast('raw_delivered_customer_date', 'TIMESTAMP') }} AS order_delivered_customer_date,
        raw_estimated_delivery_date,
        CAST({{ safe_cast('raw_estimated_delivery_date', 'TIMESTAMP') }} AS DATE) AS order_estimated_delivery_date,
        source_file,
        batch_id
    FROM deduped
)

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_status AS VARCHAR) AS order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    {{ build_dq_flag([
        ("D1", "order_status = 'delivered' AND order_delivered_customer_date IS NULL"),
        ("D2", "order_status != 'delivered' AND order_delivered_customer_date IS NOT NULL"),
        ("D3", "order_delivered_carrier_date IS NOT NULL AND order_delivered_carrier_date < order_purchase_timestamp"),
        ("D4", "order_delivered_customer_date IS NOT NULL AND order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date < order_delivered_carrier_date"),
        ("D5", "order_status = 'delivered' AND order_approved_at IS NULL"),
        ("D6", "order_delivered_carrier_date IS NOT NULL AND order_approved_at IS NOT NULL AND order_delivered_carrier_date < order_approved_at"),
        ("D7", "order_status = 'delivered' AND order_delivered_carrier_date IS NULL"),
        ("T1:order_approved_at", "raw_approved_at IS NOT NULL AND order_approved_at IS NULL"),
        ("T1:order_delivered_carrier_date", "raw_delivered_carrier_date IS NOT NULL AND order_delivered_carrier_date IS NULL"),
        ("T1:order_delivered_customer_date", "raw_delivered_customer_date IS NOT NULL AND order_delivered_customer_date IS NULL"),
        ("T1:order_estimated_delivery_date", "raw_estimated_delivery_date IS NOT NULL AND order_estimated_delivery_date IS NULL")
    ]) }} AS dq_flag,
    CAST(source_file AS VARCHAR) AS source_file,
    CAST(batch_id AS VARCHAR) AS batch_id
FROM typed

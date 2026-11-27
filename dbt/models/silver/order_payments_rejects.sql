{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/order_payments_rejects.sql - Bảng reject mức 1 cho silver.order_payments (B-313)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.order_payments)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.order_payments):
  - Thiếu order_id, hoặc thiếu/không ép được payment_sequential sang INTEGER.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(payment_sequential), '') AS raw_payment_sequential,
        {{ safe_cast("NULLIF(TRIM(payment_sequential), '')", 'INTEGER') }} AS casted_payment_sequential
    FROM {{ source('bronze_olist', 'order_payments') }}
),

evaluated AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_order_id IS NULL", "MISSING_ORDER_ID"),
            ("raw_payment_sequential IS NULL", "MISSING_PAYMENT_SEQUENTIAL"),
            ("raw_payment_sequential IS NOT NULL AND casted_payment_sequential IS NULL", "INVALID_PAYMENT_SEQUENTIAL")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

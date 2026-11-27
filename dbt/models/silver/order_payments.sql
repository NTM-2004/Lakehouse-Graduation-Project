{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/order_payments.sql - Mô hình Silver cho bảng thanh toán Olist (B-313)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Cột bắt buộc mức 1, Bảng tra cờ P1-P3 & T1; Mục IV: silver.order_payments)
  - dbt/schema_contract.yml (silver_tables.order_payments - 8 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.order_payments, tách lỗi mức 1:
     - Thiếu order_id
     - Thiếu hoặc không ép được payment_sequential sang INTEGER
  2. Ép kiểu an toàn (safe_cast) cho payment_installments (INTEGER) và payment_value (DECIMAL(10, 2)).
  3. Gắn cờ P1 (payment_type = 'not_defined'), P2 (payment_value = 0),
     P3 (payment_installments = 0), và T1:<cột> qua macro build_dq_flag.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(payment_sequential), '') AS raw_payment_sequential,
        {{ safe_cast("NULLIF(TRIM(payment_sequential), '')", 'INTEGER') }} AS casted_payment_sequential,
        LOWER(NULLIF(TRIM(payment_type), '')) AS clean_payment_type,
        NULLIF(TRIM(payment_installments), '') AS raw_payment_installments,
        NULLIF(TRIM(payment_value), '') AS raw_payment_value,
        NULLIF(TRIM(source_file), '') AS clean_source_file,
        NULLIF(TRIM(batch_id), '') AS clean_batch_id
    FROM {{ source('bronze_olist', 'order_payments') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_order_id IS NULL", "MISSING_ORDER_ID"),
            ("raw_payment_sequential IS NULL", "MISSING_PAYMENT_SEQUENTIAL"),
            ("raw_payment_sequential IS NOT NULL AND casted_payment_sequential IS NULL", "INVALID_PAYMENT_SEQUENTIAL")
        ]) }} AS reject_reason
    FROM bronze_src
),

valid_rows AS (
    SELECT
        clean_order_id AS order_id,
        casted_payment_sequential AS payment_sequential,
        clean_payment_type AS payment_type,
        raw_payment_installments,
        {{ safe_cast('raw_payment_installments', 'INTEGER') }} AS payment_installments,
        raw_payment_value,
        {{ safe_cast('raw_payment_value', 'DECIMAL(10, 2)') }} AS payment_value,
        clean_source_file AS source_file,
        clean_batch_id AS batch_id
    FROM evaluated
    WHERE NOT {{ is_reject('reject_reason') }}
)

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    payment_sequential,
    CAST(payment_type AS VARCHAR) AS payment_type,
    payment_installments,
    payment_value,
    {{ build_dq_flag([
        ("P1", "payment_type = 'not_defined'"),
        ("P2", "payment_value = 0"),
        ("P3", "payment_installments = 0"),
        ("T1:payment_installments", "raw_payment_installments IS NOT NULL AND payment_installments IS NULL"),
        ("T1:payment_value", "raw_payment_value IS NOT NULL AND payment_value IS NULL")
    ]) }} AS dq_flag,
    CAST(source_file AS VARCHAR) AS source_file,
    CAST(batch_id AS VARCHAR) AS batch_id
FROM valid_rows

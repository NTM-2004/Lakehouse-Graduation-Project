{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/customers_rejects.sql - Bảng reject mức 1 cho silver.customers (B-303)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.customers)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.customers):
  - Dòng thiếu khóa chính customer_id (NULL hoặc chuỗi rỗng sau TRIM) chuyển vào silver.customers_rejects.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(customer_id), '') AS clean_customer_id
    FROM {{ source('bronze_olist', 'customers') }}
),

evaluated AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_customer_id IS NULL", "MISSING_CUSTOMER_ID")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

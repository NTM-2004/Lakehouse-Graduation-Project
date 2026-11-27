{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/customers.sql - Mô hình Silver cho bảng khách hàng Olist (B-303)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Quy tắc Silver, Cột bắt buộc mức 1; Mục IV: silver.customers)
  - Design Build (B-303: không có cột dq_flag nên không gọi dq_flags)
  - dbt/schema_contract.yml (silver_tables.customers - 7 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.customers, chuẩn hóa chuỗi (TRIM, chuỗi rỗng thành NULL).
  2. Tách lỗi mức 1 (thiếu khóa chính customer_id) qua macro build_reject_reason / is_reject.
  3. Giữ nguyên customer_zip_code_prefix dạng chuỗi 5 ký tự (bảo toàn số 0 đứng đầu).
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(customer_id), '') AS clean_customer_id,
        NULLIF(TRIM(customer_unique_id), '') AS clean_customer_unique_id,
        NULLIF(TRIM(customer_zip_code_prefix), '') AS clean_customer_zip_code_prefix,
        NULLIF(TRIM(customer_city), '') AS clean_customer_city,
        UPPER(NULLIF(TRIM(customer_state), '')) AS clean_customer_state,
        NULLIF(TRIM(source_file), '') AS clean_source_file,
        NULLIF(TRIM(batch_id), '') AS clean_batch_id
    FROM {{ source('bronze_olist', 'customers') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_customer_id IS NULL", "MISSING_CUSTOMER_ID")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    CAST(clean_customer_id AS VARCHAR) AS customer_id,
    CAST(clean_customer_unique_id AS VARCHAR) AS customer_unique_id,
    CAST(clean_customer_zip_code_prefix AS VARCHAR) AS customer_zip_code_prefix,
    CAST(clean_customer_city AS VARCHAR) AS customer_city,
    CAST(clean_customer_state AS VARCHAR) AS customer_state,
    CAST(clean_source_file AS VARCHAR) AS source_file,
    CAST(clean_batch_id AS VARCHAR) AS batch_id
FROM evaluated
WHERE NOT {{ is_reject('reject_reason') }}

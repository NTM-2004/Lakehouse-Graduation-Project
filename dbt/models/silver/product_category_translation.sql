{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/product_category_translation.sql - Mô hình Silver bảng dịch danh mục sản phẩm (B-301)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Cột bắt buộc mức 1; Mục IV: silver.product_category_translation)
  - dbt/schema_contract.yml (silver_tables.product_category_translation - 4 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.product_category_translation (ký tự BOM đã được loại bỏ ở bước B-202).
  2. Chuẩn hóa chuỗi (TRIM, chuỗi rỗng thành NULL) và tách lỗi mức 1 (thiếu product_category_name).
  3. Không bổ sung bản dịch thiếu; kiểm thử AN-10 đếm số danh mục thiếu bản dịch ở mức warn.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(product_category_name), '') AS clean_product_category_name,
        NULLIF(TRIM(product_category_name_english), '') AS clean_product_category_name_english,
        NULLIF(TRIM(source_file), '') AS clean_source_file,
        NULLIF(TRIM(batch_id), '') AS clean_batch_id
    FROM {{ source('bronze_olist', 'product_category_translation') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_product_category_name IS NULL", "MISSING_PRODUCT_CATEGORY_NAME")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    CAST(clean_product_category_name AS VARCHAR) AS product_category_name,
    CAST(clean_product_category_name_english AS VARCHAR) AS product_category_name_english,
    CAST(clean_source_file AS VARCHAR) AS source_file,
    CAST(clean_batch_id AS VARCHAR) AS batch_id
FROM evaluated
WHERE NOT {{ is_reject('reject_reason') }}

{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/product_category_translation_rejects.sql - Bảng reject mức 1 cho silver.product_category_translation (B-301)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.product_category_translation)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.product_category_translation):
  - Dòng thiếu khóa chính product_category_name (NULL hoặc chuỗi rỗng sau TRIM) chuyển vào bảng reject.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        product_category_name,
        product_category_name_english,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(product_category_name), '') AS clean_product_category_name
    FROM {{ source('bronze_olist', 'product_category_translation') }}
),

evaluated AS (
    SELECT
        product_category_name,
        product_category_name_english,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_product_category_name IS NULL", "MISSING_PRODUCT_CATEGORY_NAME")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    product_category_name,
    product_category_name_english,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

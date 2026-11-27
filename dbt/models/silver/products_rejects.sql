{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/products_rejects.sql - Bảng reject mức 1 cho silver.products (B-307)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.products)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.products):
  - Dòng thiếu khóa chính product_id (NULL hoặc chuỗi rỗng sau TRIM) chuyển vào silver.products_rejects.
  - Giữ toàn bộ các cột Bronze của dòng (kể cả tên gốc product_name_lenght, product_description_lenght)
    và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(product_id), '') AS clean_product_id
    FROM {{ source('bronze_olist', 'products') }}
),

evaluated AS (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_product_id IS NULL", "MISSING_PRODUCT_ID")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

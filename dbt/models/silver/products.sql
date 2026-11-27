{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/products.sql - Mô hình Silver cho bảng sản phẩm Olist (B-307)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Quy tắc Silver, Bảng tra cờ PR1-PR3 & T1; Mục IV: silver.products)
  - dbt/schema_contract.yml (silver_tables.products - 13 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.products, tách lỗi mức 1 (thiếu product_id) qua build_reject_reason / is_reject.
  2. Sửa lỗi chính tả tên cột nguồn ('lenght' -> 'length').
  3. Chuẩn hóa danh mục: thiếu -> 'unknown' (cờ PR1); nối LEFT JOIN silver.product_category_translation:
     - product_category_name = 'unknown' -> product_category_en = 'unknown'
     - Có danh mục nhưng thiếu bản dịch -> dùng tên gốc tiếng Bồ Đào Nha
     - Còn lại -> lấy product_category_name_english
  4. Ép kiểu an toàn (safe_cast sang INTEGER, gắn cờ T1:<cột> nếu lỗi).
  5. Cân nặng bằng 0: gắn cờ PR2 và đổi giá trị thành NULL (kiểm tra T1 trước khi đổi 0 -> NULL).
  6. Thiếu ít nhất một trong ba kích thước (length, height, width): gắn cờ PR3.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(product_id), '') AS clean_product_id,
        NULLIF(TRIM(product_category_name), '') AS raw_category_name,
        NULLIF(TRIM(product_name_lenght), '') AS raw_name_length,
        NULLIF(TRIM(product_description_lenght), '') AS raw_description_length,
        NULLIF(TRIM(product_photos_qty), '') AS raw_photos_qty,
        NULLIF(TRIM(product_weight_g), '') AS raw_weight_g,
        NULLIF(TRIM(product_length_cm), '') AS raw_length_cm,
        NULLIF(TRIM(product_height_cm), '') AS raw_height_cm,
        NULLIF(TRIM(product_width_cm), '') AS raw_width_cm,
        NULLIF(TRIM(source_file), '') AS clean_source_file,
        NULLIF(TRIM(batch_id), '') AS clean_batch_id
    FROM {{ source('bronze_olist', 'products') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_product_id IS NULL", "MISSING_PRODUCT_ID")
        ]) }} AS reject_reason
    FROM bronze_src
),

valid_rows AS (
    SELECT
        clean_product_id AS product_id,
        raw_category_name,
        COALESCE(raw_category_name, 'unknown') AS product_category_name,
        raw_name_length,
        {{ safe_cast('raw_name_length', 'INTEGER') }} AS casted_name_length,
        raw_description_length,
        {{ safe_cast('raw_description_length', 'INTEGER') }} AS casted_description_length,
        raw_photos_qty,
        {{ safe_cast('raw_photos_qty', 'INTEGER') }} AS casted_photos_qty,
        raw_weight_g,
        {{ safe_cast('raw_weight_g', 'INTEGER') }} AS casted_weight_g,
        raw_length_cm,
        {{ safe_cast('raw_length_cm', 'INTEGER') }} AS casted_length_cm,
        raw_height_cm,
        {{ safe_cast('raw_height_cm', 'INTEGER') }} AS casted_height_cm,
        raw_width_cm,
        {{ safe_cast('raw_width_cm', 'INTEGER') }} AS casted_width_cm,
        clean_source_file AS source_file,
        clean_batch_id AS batch_id
    FROM evaluated
    WHERE NOT {{ is_reject('reject_reason') }}
),

joined_translation AS (
    SELECT
        p.product_id,
        p.raw_category_name,
        p.product_category_name,
        CASE
            WHEN p.product_category_name = 'unknown' THEN 'unknown'
            WHEN t.product_category_name_english IS NULL THEN p.product_category_name
            ELSE t.product_category_name_english
        END AS product_category_en,
        p.raw_name_length,
        p.casted_name_length,
        p.raw_description_length,
        p.casted_description_length,
        p.raw_photos_qty,
        p.casted_photos_qty,
        p.raw_weight_g,
        p.casted_weight_g,
        p.raw_length_cm,
        p.casted_length_cm,
        p.raw_height_cm,
        p.casted_height_cm,
        p.raw_width_cm,
        p.casted_width_cm,
        p.source_file,
        p.batch_id
    FROM valid_rows p
    LEFT JOIN {{ ref('product_category_translation') }} t
        ON p.product_category_name = t.product_category_name
)

SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(product_category_name AS VARCHAR) AS product_category_name,
    CAST(product_category_en AS VARCHAR) AS product_category_en,
    casted_name_length AS product_name_length,
    casted_description_length AS product_description_length,
    casted_photos_qty AS product_photos_qty,
    CASE
        WHEN casted_weight_g = 0 THEN NULL
        ELSE casted_weight_g
    END AS product_weight_g,
    casted_length_cm AS product_length_cm,
    casted_height_cm AS product_height_cm,
    casted_width_cm AS product_width_cm,
    {{ build_dq_flag([
        ("PR1", "raw_category_name IS NULL"),
        ("PR2", "casted_weight_g = 0"),
        ("PR3", "casted_length_cm IS NULL OR casted_height_cm IS NULL OR casted_width_cm IS NULL"),
        ("T1:product_name_length", "raw_name_length IS NOT NULL AND casted_name_length IS NULL"),
        ("T1:product_description_length", "raw_description_length IS NOT NULL AND casted_description_length IS NULL"),
        ("T1:product_photos_qty", "raw_photos_qty IS NOT NULL AND casted_photos_qty IS NULL"),
        ("T1:product_weight_g", "raw_weight_g IS NOT NULL AND casted_weight_g IS NULL"),
        ("T1:product_length_cm", "raw_length_cm IS NOT NULL AND casted_length_cm IS NULL"),
        ("T1:product_height_cm", "raw_height_cm IS NOT NULL AND casted_height_cm IS NULL"),
        ("T1:product_width_cm", "raw_width_cm IS NOT NULL AND casted_width_cm IS NULL")
    ]) }} AS dq_flag,
    CAST(source_file AS VARCHAR) AS source_file,
    CAST(batch_id AS VARCHAR) AS batch_id
FROM joined_translation

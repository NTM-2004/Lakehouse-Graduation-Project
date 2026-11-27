{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/sellers_rejects.sql - Bảng reject mức 1 cho silver.sellers (B-305)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.sellers)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.sellers):
  - Dòng thiếu khóa chính seller_id (NULL hoặc chuỗi rỗng sau TRIM) chuyển vào silver.sellers_rejects.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(seller_id), '') AS clean_seller_id
    FROM {{ source('bronze_olist', 'sellers') }}
),

evaluated AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_seller_id IS NULL", "MISSING_SELLER_ID")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/sellers.sql - Mô hình Silver cho bảng người bán Olist (B-305)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Nhóm AN không gắn cờ AN-11; Mục IV: silver.sellers & 4 bước chuẩn hóa seller_city)
  - dbt/schema_contract.yml (silver_tables.sellers - 6 cột)

  Luồng chuẩn hóa seller_city theo AN-11 (38 người bán đổi tên, không gắn cờ):
  1. Bước 1 (W và A): chữ thường, đổi ´ (CHR(180)) thành ', bỏ dấu dựng sẵn (TRANSLATE)
     và dấu tổ hợp NFD (REGEXP_REPLACE '\p{M}'), gộp nhiều khoảng trắng thành 1, cắt khoảng trắng đầu/cuối.
  2. Bước 2 (A): có '/', '\', ',' hoặc '(' -> lấy phần trước ký tự đầu tiên.
  3. Bước 3 (H): có '-' và phần sau là mã bang Brazil 2 chữ cái -> bỏ phần sau.
  4. Bước 4 (A và B): rỗng, chỉ có chữ số, có '@', hoặc chỉ là 'sp', 'sbc', 'poa' -> ghi 'unknown'.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(seller_id), '') AS clean_seller_id,
        NULLIF(TRIM(seller_zip_code_prefix), '') AS clean_seller_zip_code_prefix,
        seller_city AS raw_seller_city,
        UPPER(NULLIF(TRIM(seller_state), '')) AS clean_seller_state,
        NULLIF(TRIM(source_file), '') AS clean_source_file,
        NULLIF(TRIM(batch_id), '') AS clean_batch_id
    FROM {{ source('bronze_olist', 'sellers') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_seller_id IS NULL", "MISSING_SELLER_ID")
        ]) }} AS reject_reason
    FROM bronze_src
),

valid_rows AS (
    SELECT
        clean_seller_id AS seller_id,
        clean_seller_zip_code_prefix AS seller_zip_code_prefix,
        raw_seller_city,
        clean_seller_state AS seller_state,
        clean_source_file AS source_file,
        clean_batch_id AS batch_id
    FROM evaluated
    WHERE NOT {{ is_reject('reject_reason') }}
),

step1_basic AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRANSLATE(
                        REPLACE(LOWER(COALESCE(raw_seller_city, '')), '´', ''''),
                        'áàâãäéèêëíìîïóòôõöúùûüçñ',
                        'aaaaaeeeeiiiiooooouuuucn'
                    ),
                    '\p{M}',
                    ''
                ),
                '\s+',
                ' '
            )
        ) AS city_s1,
        seller_state,
        source_file,
        batch_id
    FROM valid_rows
),

step2_split AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        TRIM(REGEXP_REPLACE(city_s1, '[/\\,(].*$', '')) AS city_s2,
        seller_state,
        source_file,
        batch_id
    FROM step1_basic
),

step3_hyphen AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        TRIM(
            REGEXP_REPLACE(
                city_s2,
                '\s*-\s*(ac|al|am|ap|ba|ce|df|es|go|ma|mg|ms|mt|pa|pb|pe|pi|pr|rj|rn|ro|rr|rs|sc|se|sp|to)$',
                ''
            )
        ) AS city_s3,
        seller_state,
        source_file,
        batch_id
    FROM step2_split
)

SELECT
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(seller_zip_code_prefix AS VARCHAR) AS seller_zip_code_prefix,
    CAST(
        CASE
            WHEN city_s3 IS NULL OR city_s3 = '' THEN 'unknown'
            WHEN REGEXP_LIKE(city_s3, '^[0-9]+$') THEN 'unknown'
            WHEN city_s3 LIKE '%@%' THEN 'unknown'
            WHEN city_s3 IN ('sp', 'sbc', 'poa') THEN 'unknown'
            ELSE city_s3
        END
        AS VARCHAR
    ) AS seller_city,
    CAST(seller_state AS VARCHAR) AS seller_state,
    CAST(source_file AS VARCHAR) AS source_file,
    CAST(batch_id AS VARCHAR) AS batch_id
FROM step3_hyphen

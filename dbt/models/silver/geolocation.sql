{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/geolocation.sql - Mô hình Silver cho bảng tọa độ mã bưu chính Olist (B-317)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03 (không có bảng reject, không có cột dq_flag và source_file)
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Nhóm AN không gắn cờ AN-12; Mục IV: silver.geolocation)
  - dbt/schema_contract.yml (silver_tables.geolocation - 7 cột)

  Luồng xử lý:
  1. Đọc toàn bộ 1.000.163 dòng từ bronze_olist.geolocation (không loại bất kỳ điểm nào kể cả ngoài khung Brazil).
  2. Chuẩn hóa geolocation_city: viết thường, thay '´' bằng ''', bỏ dấu dựng sẵn (TRANSLATE)
     và dấu tổ hợp (REGEXP_REPLACE '\p{M}'), gộp nhiều khoảng trắng thành 1, TRIM.
  3. Gộp theo geolocation_zip_code_prefix (19.015 mã bưu chính):
     - geolocation_lat, geolocation_lng: trung bình (AVG) mọi điểm của mã.
     - geolocation_city: tên xuất hiện nhiều nhất; hòa lấy theo thứ tự chữ cái (ASC).
     - geolocation_state: bang xuất hiện nhiều nhất; hòa lấy theo thứ tự chữ cái (ASC).
     - n_points: số điểm gốc đã gộp (INTEGER).
     - batch_id: tên batch nguồn (chỉ có ở bulk).
#}

WITH cleaned AS (
    SELECT
        NULLIF(TRIM(geolocation_zip_code_prefix), '') AS geolocation_zip_code_prefix,
        {{ safe_cast('geolocation_lat', 'DOUBLE') }} AS lat_val,
        {{ safe_cast('geolocation_lng', 'DOUBLE') }} AS lng_val,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRANSLATE(
                        REPLACE(LOWER(COALESCE(geolocation_city, '')), '´', ''''),
                        'áàâãäéèêëíìîïóòôõöúùûüçñ',
                        'aaaaaeeeeiiiiooooouuuucn'
                    ),
                    '\p{M}',
                    ''
                ),
                '\s+',
                ' '
            )
        ) AS norm_city,
        UPPER(NULLIF(TRIM(geolocation_state), '')) AS norm_state,
        NULLIF(TRIM(batch_id), '') AS batch_id
    FROM {{ source('bronze_olist', 'geolocation') }}
    WHERE NULLIF(TRIM(geolocation_zip_code_prefix), '') IS NOT NULL
),

coord_agg AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(lat_val) AS geolocation_lat,
        AVG(lng_val) AS geolocation_lng,
        CAST(COUNT(*) AS INTEGER) AS n_points,
        MAX(batch_id) AS batch_id
    FROM cleaned
    GROUP BY geolocation_zip_code_prefix
),

city_mode AS (
    SELECT
        geolocation_zip_code_prefix,
        norm_city AS geolocation_city
    FROM (
        SELECT
            geolocation_zip_code_prefix,
            norm_city,
            ROW_NUMBER() OVER (
                PARTITION BY geolocation_zip_code_prefix
                ORDER BY COUNT(*) DESC, norm_city ASC
            ) AS rn
        FROM cleaned
        GROUP BY geolocation_zip_code_prefix, norm_city
    ) ranked_cities
    WHERE rn = 1
),

state_mode AS (
    SELECT
        geolocation_zip_code_prefix,
        norm_state AS geolocation_state
    FROM (
        SELECT
            geolocation_zip_code_prefix,
            norm_state,
            ROW_NUMBER() OVER (
                PARTITION BY geolocation_zip_code_prefix
                ORDER BY COUNT(*) DESC, norm_state ASC
            ) AS rn
        FROM cleaned
        GROUP BY geolocation_zip_code_prefix, norm_state
    ) ranked_states
    WHERE rn = 1
)

SELECT
    CAST(ca.geolocation_zip_code_prefix AS VARCHAR) AS geolocation_zip_code_prefix,
    ca.geolocation_lat,
    ca.geolocation_lng,
    CAST(cm.geolocation_city AS VARCHAR) AS geolocation_city,
    CAST(sm.geolocation_state AS VARCHAR) AS geolocation_state,
    ca.n_points,
    CAST(ca.batch_id AS VARCHAR) AS batch_id
FROM coord_agg ca
INNER JOIN city_mode cm
    ON ca.geolocation_zip_code_prefix = cm.geolocation_zip_code_prefix
INNER JOIN state_mode sm
    ON ca.geolocation_zip_code_prefix = sm.geolocation_zip_code_prefix

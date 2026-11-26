{#
  dbt/tests/flag_catalog_coverage.sql - Đối chiếu danh mục cờ lỗi F17 (B-322)
  Tác giả phụ trách: Minh | Reviewer: Kiên
  Tham chiếu:
  - Requirement v3: F17 (Mọi cờ dùng ở Silver có trong bảng tra và ngược lại)
  - Design Build: B-322, R319 (Chiều catalog -> Silver loại trừ T1 nếu dữ liệu thực có 0 dòng T1)
  - Seed: dbt/seeds/flag_catalog.csv (B-015)

  Kiểm tra 2 chiều trên đủ 7 bảng Silver có cột dq_flag:
  1. Chiều Silver -> Catalog (chạy ở mọi batch):
     Không có bất kỳ mã cờ lạ nào trong dq_flag nằm ngoài danh sách mã hợp lệ của bảng đó
     trong flag_catalog (bao gồm cả T1 hoặc T1:<cột>).
  2. Chiều Catalog -> Silver (chạy sau batch cuối theo is_final_batch(), loại trừ T1 theo R319):
     Mọi mã cờ khai báo trong flag_catalog đều phải xuất hiện ít nhất 1 dòng ở bảng Silver tương ứng.
#}

{{ config(severity='error') }}

WITH silver_dq_rows AS (
    SELECT 'orders' AS table_name, dq_flag FROM {{ ref('orders') }} WHERE dq_flag IS NOT NULL
    UNION ALL
    SELECT 'order_items' AS table_name, dq_flag FROM {{ ref('order_items') }} WHERE dq_flag IS NOT NULL
    UNION ALL
    SELECT 'order_payments' AS table_name, dq_flag FROM {{ ref('order_payments') }} WHERE dq_flag IS NOT NULL
    UNION ALL
    SELECT 'order_reviews' AS table_name, dq_flag FROM {{ ref('order_reviews') }} WHERE dq_flag IS NOT NULL
    UNION ALL
    SELECT 'products' AS table_name, dq_flag FROM {{ ref('products') }} WHERE dq_flag IS NOT NULL
    UNION ALL
    SELECT 'leads' AS table_name, dq_flag FROM {{ ref('leads') }} WHERE dq_flag IS NOT NULL
    UNION ALL
    SELECT 'clickstream_events' AS table_name, dq_flag FROM {{ ref('clickstream_events') }} WHERE dq_flag IS NOT NULL
),

-- Chiều 1 (Silver -> Catalog): tách động từng mã trong dq_flag và đối chiếu trực tiếp với seed flag_catalog
-- (không viết cứng danh sách mã cờ trong SQL; hỗ trợ chuẩn hóa T1:<cột> -> T1)
silver_flag_tokens AS (
    SELECT DISTINCT
        s.table_name,
        TRIM(CAST(t.flag_token AS VARCHAR)) AS raw_flag
    FROM (
        SELECT DISTINCT table_name, dq_flag FROM silver_dq_rows
    ) s,
    LATERAL (
        SELECT FLATTEN(
            CONVERT_FROM('["' || REPLACE(s.dq_flag, ',', '","') || '"]', 'JSON')
        ) AS flag_token
    ) t
),

normalized_silver_flags AS (
    SELECT DISTINCT
        table_name,
        raw_flag,
        CASE
            WHEN raw_flag LIKE 'T1:%' THEN 'T1'
            ELSE raw_flag
        END AS norm_flag
    FROM silver_flag_tokens
    WHERE raw_flag IS NOT NULL AND raw_flag != ''
),

unknown_flags_in_silver AS (
    SELECT
        'silver_to_catalog' AS check_direction,
        nf.table_name,
        nf.raw_flag AS detail_value
    FROM normalized_silver_flags nf
    LEFT JOIN {{ ref('flag_catalog') }} fc
        ON nf.norm_flag = fc.flag_id
       AND (fc.table_name = nf.table_name OR fc.table_name = 'mọi bảng')
    WHERE fc.flag_id IS NULL
),

-- Chiều 2 (Catalog -> Silver): mọi cờ trong flag_catalog (trừ T1 theo R319) phải xuất hiện sau batch cuối
missing_catalog_flags AS (
    {% if is_final_batch() %}
    SELECT
        'catalog_to_silver' AS check_direction,
        fc.table_name,
        fc.flag_id AS detail_value
    FROM {{ ref('flag_catalog') }} fc
    WHERE fc.flag_id != 'T1'
      AND NOT EXISTS (
          SELECT 1
          FROM silver_dq_rows s
          WHERE s.table_name = fc.table_name
            AND (',' || s.dq_flag || ',') LIKE '%,' || fc.flag_id || ',%'
      )
    {% else %}
    SELECT
        CAST(NULL AS VARCHAR) AS check_direction,
        CAST(NULL AS VARCHAR) AS table_name,
        CAST(NULL AS VARCHAR) AS detail_value
    WHERE 1 = 0
    {% endif %}
)

SELECT check_direction, table_name, detail_value FROM unknown_flags_in_silver
UNION ALL
SELECT check_direction, table_name, detail_value FROM missing_catalog_flags

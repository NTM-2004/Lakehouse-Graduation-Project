{#
  dbt/tests/silver_leads_error_checks.sql - Các kiểm thử mức ERROR cho silver.leads (B-318)
  Tham chiếu: Design Bronze, Silver v2 (Mục IV: silver.leads; Kiểm thử chung; Mốc sau ngày chốt) & Requirement v3 (F09)

  Kiểm tra:
  1. Không còn dòng is_parsed = false ở bronze.marketing_leads (F09).
  2. Không có mốc thời gian nào (first_contact_date, won_date) muộn hơn ngày chốt (cutoff_date).
  3. Số lead (tổng dòng) và số deal (has_deal = true) ở silver.leads khớp số mql_id
     và số mql_id có deal khác nhau của bronze.marketing_leads (khi reject = 0).
  4. Hàng rào phát lại: mỗi mql_id có tối đa 2 dòng ở Bronze; nếu có 2 dòng thì
     bản ở batch bulk (batch_seq = 0) chưa có deal và bản ở batch sau có deal.
#}

{{ config(severity='error') }}

-- 1. Dòng JSON lỗi (is_parsed = false) ở Bronze
SELECT
    'UNPARSED_BRONZE_JSON' AS check_name,
    CAST(COUNT(*) AS VARCHAR) AS detail
FROM {{ source('bronze', 'marketing_leads') }}
WHERE is_parsed = false
HAVING COUNT(*) > 0

UNION ALL

-- 2. Mốc thời gian sau ngày chốt ở Silver
SELECT
    'POST_CUTOFF_TIMESTAMP' AS check_name,
    CAST(COUNT(*) AS VARCHAR) AS detail
FROM {{ ref('leads') }}
WHERE first_contact_date > CAST({{ get_cutoff_date() }} AS DATE)
   OR (won_date IS NOT NULL AND CAST(won_date AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
HAVING COUNT(*) > 0

UNION ALL

-- 3. Đối soát số lead và số deal giữa Silver (+ rejects) và Bronze
SELECT
    'SILVER_BRONZE_COUNT_MISMATCH' AS check_name,
    'silver_leads=' || CAST(s.n_leads AS VARCHAR)
        || ',bronze_leads=' || CAST(b.n_leads AS VARCHAR)
        || ',silver_deals=' || CAST(s.n_deals AS VARCHAR)
        || ',bronze_deals=' || CAST(b.n_deals AS VARCHAR) AS detail
FROM (
    SELECT
        COUNT(*) AS n_leads,
        SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) AS n_deals
    FROM {{ ref('leads') }}
) s
CROSS JOIN (
    SELECT
        COUNT(DISTINCT NULLIF(TRIM(mql_id), '')) AS n_leads,
        COUNT(DISTINCT CASE WHEN deal IS NOT NULL THEN NULLIF(TRIM(mql_id), '') END) AS n_deals
    FROM {{ source('bronze', 'marketing_leads') }}
    WHERE is_parsed = true
      AND NULLIF(TRIM(mql_id), '') IS NOT NULL
) b
CROSS JOIN (
    SELECT COUNT(*) AS n_rejects FROM {{ ref('leads_rejects') }}
) r
WHERE (s.n_leads + r.n_rejects) != b.n_leads
   OR s.n_deals != b.n_deals

UNION ALL

-- 4. Hàng rào phát lại ở Bronze: mỗi mql_id tối đa 2 dòng; nếu 2 dòng thì bulk không có deal, bản sau có deal
SELECT
    'BRONZE_REPLAY_GUARDRAIL' AS check_name,
    mql_id AS detail
FROM (
    SELECT
        src.mql_id,
        COUNT(*) AS row_cnt,
        SUM(CASE WHEN bs.batch_seq = 0 AND src.deal IS NOT NULL THEN 1 ELSE 0 END) AS bulk_with_deal,
        SUM(CASE WHEN bs.batch_seq > 0 AND src.deal IS NULL THEN 1 ELSE 0 END) AS later_without_deal
    FROM {{ source('bronze', 'marketing_leads') }} src
    INNER JOIN {{ ref('batch_seq') }} bs
        ON src.batch_id = bs.batch_id
    WHERE src.is_parsed = true
    GROUP BY src.mql_id
) g
WHERE g.row_cnt > 2
   OR (g.row_cnt = 2 AND (g.bulk_with_deal > 0 OR g.later_without_deal > 0))

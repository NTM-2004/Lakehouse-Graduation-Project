{#
  dbt/tests/silver_leads_warn_checks.sql - Các kiểm thử mức WARN cho silver.leads (B-318)
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.leads; Kiểm thử chung)

  Kiểm tra:
  1. Số dòng reject ở silver.leads_rejects > 0 (mức warn).
  2. Sau batch cuối (is_final_batch):
     - Số dòng gắn cờ C1 khớp an_expected (AN-13, C1 = 1).
     - Tổng số lead = 8.000 và tổng số deal = 792 (khớp Data Analyze, Số dòng theo batch).
#}

{{ config(severity='warn') }}

-- 1. Cảnh báo nếu có dòng rơi vào silver.leads_rejects
SELECT
    'LEADS_REJECTS_NON_ZERO' AS check_name,
    CAST(COUNT(*) AS VARCHAR) AS detail
FROM {{ ref('leads_rejects') }}
HAVING COUNT(*) > 0

{% if is_final_batch() %}

UNION ALL

-- 2. Sau batch cuối: đếm cờ C1 khớp an_expected (AN-13)
SELECT
    'AN13_C1_COUNT_MISMATCH' AS check_name,
    'actual=' || CAST(a.cnt AS VARCHAR) || ',expected=' || CAST(e.expected_count AS VARCHAR) AS detail
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('leads') }}
    WHERE {{ has_flag('dq_flag', 'C1') }}
) a
INNER JOIN {{ ref('an_expected') }} e
    ON e.an_id = 'AN-13' AND e.sub_code = 'C1'
WHERE a.cnt != e.expected_count

UNION ALL

-- 3. Sau batch cuối: số lead = 8.000 và số deal = 792
SELECT
    'FINAL_BATCH_LEAD_DEAL_COUNT_MISMATCH' AS check_name,
    'leads=' || CAST(COUNT(*) AS VARCHAR)
        || ',deals=' || CAST(SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) AS VARCHAR) AS detail
FROM {{ ref('leads') }}
HAVING COUNT(*) != 8000
    OR SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) != 792

{% endif %}

{#
  dbt/tests/gold_gt11.sql - GT-11: Đơn quá hạn chưa giao khớp số Requirement (B-427)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-07
  Mức: warn (Design Gold v2 - Kiểm thử Gold GT-11; Design Orchestration - Kiểm thử theo lượt)
  Tham chiếu:
  - Requirement v3 (UC-02: Đơn quá hạn chưa giao = 1.160 đơn = 1.103 AN-16 + 57 AN-14)
  - Seed: dbt/seeds/an_expected.csv (B-017: an_id='AN-16', sub_code='total_overdue_undelivered')
  - Macro: dbt/macros/cutoff_batch.sql (B-018: is_final_batch)

  Chỉ chạy đối soát sau batch cuối (2018-08) ở mức warn; ở batch giữa trả về 0 dòng.
#}

{{ config(severity='warn') }}

{% if is_final_batch() %}
WITH expected AS (
    SELECT expected_count
    FROM {{ ref('an_expected') }}
    WHERE an_id = 'AN-16'
      AND sub_code = 'total_overdue_undelivered'
),

actual_fact AS (
    SELECT
        'fact_order' AS source_table,
        COALESCE(SUM(CASE WHEN is_overdue_undelivered THEN 1 ELSE 0 END), 0) AS actual_count
    FROM {{ ref('fact_order') }}
),

actual_kpi AS (
    SELECT
        'kpi_delivery_month' AS source_table,
        COALESCE(SUM(n_overdue_undelivered), 0) AS actual_count
    FROM {{ ref('kpi_delivery_month') }}
),

all_actuals AS (
    SELECT source_table, actual_count FROM actual_fact
    UNION ALL
    SELECT source_table, actual_count FROM actual_kpi
)

SELECT
    a.source_table,
    a.actual_count,
    e.expected_count
FROM all_actuals a
CROSS JOIN expected e
WHERE a.actual_count != e.expected_count
{% else %}
SELECT
    CAST(NULL AS VARCHAR) AS source_table,
    CAST(NULL AS BIGINT) AS actual_count,
    CAST(NULL AS INTEGER) AS expected_count
FROM (VALUES (1)) AS _empty(x)
WHERE 1 = 0
{% endif %}

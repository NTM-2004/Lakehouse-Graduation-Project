{#
  dbt/tests/gold_gt15.sql - GT-15: dim_date liên tục từ ngày đầu đến ngày chốt, không thiếu ngày (B-431)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F10, Design Gold v2 - Kiểm thử Gold GT-15)
#}

{{ config(severity='error') }}

WITH stats AS (
    SELECT
        COUNT(*) AS actual_rows,
        COUNT(DISTINCT "date") AS distinct_days,
        MIN("date") AS min_date,
        MAX("date") AS max_date,
        CAST({{ get_cutoff_date() }} AS DATE) AS expected_end_date
    FROM {{ ref('dim_date') }}
)
SELECT
    actual_rows,
    distinct_days,
    min_date,
    max_date,
    expected_end_date,
    {{ calendar_days('expected_end_date', 'min_date') }} + 1 AS expected_rows
FROM stats
WHERE actual_rows = 0
   OR actual_rows != distinct_days
   OR max_date != expected_end_date
   OR actual_rows != ({{ calendar_days('expected_end_date', 'min_date') }} + 1)

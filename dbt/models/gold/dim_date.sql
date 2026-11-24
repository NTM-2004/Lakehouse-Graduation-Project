{{
  config(
    materialized='table',
    schema='gold'
  )
}}

{#
  dbt/models/gold/dim_date.sql - Dimension ngày dùng chung duy nhất của 3 nguồn (B-404)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mẫu triển khai: M-05
  Tham chiếu:
  - Design Gold v2 (Mục II: gold.dim_date; Mục VII: Chiều ngày)
  - dbt/schema_contract.yml (gold_tables.dim_date - 4 cột: date, year, month, year_month)

  Grain: 1 dòng = 1 ngày, liên tục từ ngày nhỏ nhất trong 3 cột ngày nguồn
  (silver.orders.order_purchase_timestamp, silver.leads.first_contact_date,
  silver.clickstream_events.event_date) đến tham số ngày chốt (cutoff_date = 2018-08-31).
  Không phụ thuộc gói dbt_utils: sinh dãy số nguyên 0..9999 bằng tích Đề-các thuần SQL.
#}

WITH bounds AS (
    SELECT
        MIN(min_dt) AS start_date,
        CAST({{ get_cutoff_date() }} AS DATE) AS end_date
    FROM (
        SELECT MIN(CAST(order_purchase_timestamp AS DATE)) AS min_dt FROM {{ ref('orders') }}
        UNION ALL
        SELECT MIN(first_contact_date) AS min_dt FROM {{ ref('leads') }}
        UNION ALL
        SELECT MIN(event_date) AS min_dt FROM {{ ref('clickstream_events') }}
    ) src_dates
    WHERE min_dt IS NOT NULL
),

digits AS (
    SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),

day_offsets AS (
    SELECT
        (d3.d * 1000 + d2.d * 100 + d1.d * 10 + d0.d) AS day_offset
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
),

calendar AS (
    SELECT
        DATE_ADD(b.start_date, o.day_offset) AS cal_date
    FROM bounds b
    INNER JOIN day_offsets o
        ON DATE_ADD(b.start_date, o.day_offset) <= b.end_date
)

SELECT
    cal_date AS "date",
    CAST(EXTRACT(YEAR FROM cal_date) AS INTEGER) AS "year",
    CAST(EXTRACT(MONTH FROM cal_date) AS INTEGER) AS "month",
    CAST(TO_CHAR(cal_date, 'YYYY-MM') AS VARCHAR) AS year_month
FROM calendar
ORDER BY cal_date

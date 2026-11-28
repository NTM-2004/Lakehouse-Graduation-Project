{#
  dbt/models/gold/kpi_delivery_month.sql - Bảng KPI UC-02 theo tháng mua (B-411)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-06
  Tham chiếu:
  - Requirement v3 (UC-02: Thời gian giao TB, Tỷ lệ giao trễ, Điểm đánh giá TB, Thời gian vận chuyển, Chênh lệch điểm do giao trễ, Đơn quá hạn chưa giao; Mẫu nhỏ)
  - Design Gold v2 (Mục V: Bảng lát cắt, Mẫu cột chung, Thành phần theo UC-02; Mục VII: Quan sát của cờ mẫu nhỏ, Tháng không có đơn)
  - dbt/schema_contract.yml (gold_tables.kpi_delivery_month - 23 cột)

  Lát cắt: year_month (mọi tháng trong dim_date, 24 dòng kể cả tháng không có đơn).
  Cờ mẫu nhỏ: tháng dưới 100 đơn hợp lệ HOẶC dưới 30 đơn đã giao (n_obs = n_delivered).
  Cờ nhóm giao trễ (is_small_gap_group): n_scored_on_time < 30 HOẶC n_scored_late < 30.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH all_months AS (
    SELECT DISTINCT year_month
    FROM {{ ref('dim_date') }}
),

monthly_agg AS (
    SELECT
        d.year_month,
        SUM(CASE WHEN f.is_valid_order THEN 1 ELSE 0 END) AS n_valid_orders,
        SUM(CASE WHEN f.is_delivered_order THEN 1 ELSE 0 END) AS n_delivered,
        COALESCE(
            SUM(CASE WHEN f.is_delivered_order AND f.delivery_days IS NOT NULL THEN f.delivery_days ELSE 0 END),
            0
        ) AS sum_delivery_days,
        SUM(CASE WHEN f.is_delivered_order AND f.is_late THEN 1 ELSE 0 END) AS n_late,
        SUM(CASE WHEN f.is_delivered_order AND NOT f.is_late THEN 1 ELSE 0 END) AS n_on_time,
        SUM(CASE WHEN f.is_delivered_order AND f.shipping_days IS NOT NULL THEN 1 ELSE 0 END) AS n_shipping_measured,
        COALESCE(
            SUM(CASE WHEN f.is_delivered_order AND f.shipping_days IS NOT NULL THEN f.shipping_days ELSE 0 END),
            0
        ) AS sum_shipping_days,
        SUM(CASE WHEN f.review_score IS NOT NULL THEN 1 ELSE 0 END) AS n_scored,
        COALESCE(
            SUM(CASE WHEN f.review_score IS NOT NULL THEN f.review_score ELSE 0 END),
            0
        ) AS sum_score,
        SUM(
            CASE
                WHEN f.is_delivered_order AND NOT f.is_late AND f.review_score IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS n_scored_on_time,
        COALESCE(
            SUM(
                CASE
                    WHEN f.is_delivered_order AND NOT f.is_late AND f.review_score IS NOT NULL
                    THEN f.review_score
                    ELSE 0
                END
            ),
            0
        ) AS sum_score_on_time,
        SUM(
            CASE
                WHEN f.is_delivered_order AND f.is_late AND f.review_score IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS n_scored_late,
        COALESCE(
            SUM(
                CASE
                    WHEN f.is_delivered_order AND f.is_late AND f.review_score IS NOT NULL
                    THEN f.review_score
                    ELSE 0
                END
            ),
            0
        ) AS sum_score_late,
        SUM(CASE WHEN f.is_overdue_undelivered THEN 1 ELSE 0 END) AS n_overdue_undelivered
    FROM {{ ref('fact_order') }} f
    INNER JOIN {{ ref('dim_date') }} d
        ON f.purchase_date = d."date"
    GROUP BY d.year_month
),

base AS (
    SELECT
        m.year_month,
        COALESCE(a.n_valid_orders, 0) AS n_valid_orders,
        COALESCE(a.n_delivered, 0) AS n_delivered,
        COALESCE(a.sum_delivery_days, 0) AS sum_delivery_days,
        COALESCE(a.n_late, 0) AS n_late,
        COALESCE(a.n_on_time, 0) AS n_on_time,
        COALESCE(a.n_shipping_measured, 0) AS n_shipping_measured,
        COALESCE(a.sum_shipping_days, 0) AS sum_shipping_days,
        COALESCE(a.n_scored, 0) AS n_scored,
        COALESCE(a.sum_score, 0) AS sum_score,
        COALESCE(a.n_scored_on_time, 0) AS n_scored_on_time,
        COALESCE(a.sum_score_on_time, 0) AS sum_score_on_time,
        COALESCE(a.n_scored_late, 0) AS n_scored_late,
        COALESCE(a.sum_score_late, 0) AS sum_score_late,
        COALESCE(a.n_overdue_undelivered, 0) AS n_overdue_undelivered
    FROM all_months m
    LEFT JOIN monthly_agg a
        ON m.year_month = a.year_month
)

SELECT
    year_month,
    n_valid_orders,
    n_delivered,
    sum_delivery_days,
    n_late,
    n_on_time,
    n_shipping_measured,
    sum_shipping_days,
    n_scored,
    sum_score,
    n_scored_on_time,
    sum_score_on_time,
    n_scored_late,
    sum_score_late,
    n_overdue_undelivered,
    CASE
        WHEN n_delivered > 0 THEN CAST(sum_delivery_days AS DOUBLE) / n_delivered
        ELSE CAST(NULL AS DOUBLE)
    END AS avg_delivery_days,
    CASE
        WHEN n_delivered > 0 THEN CAST(n_late AS DOUBLE) / n_delivered
        ELSE CAST(NULL AS DOUBLE)
    END AS late_rate,
    CASE
        WHEN n_scored > 0 THEN CAST(sum_score AS DOUBLE) / n_scored
        ELSE CAST(NULL AS DOUBLE)
    END AS avg_score,
    CASE
        WHEN n_shipping_measured > 0 THEN CAST(sum_shipping_days AS DOUBLE) / n_shipping_measured
        ELSE CAST(NULL AS DOUBLE)
    END AS avg_shipping_days,
    CASE
        WHEN n_scored_on_time > 0 AND n_scored_late > 0
        THEN (CAST(sum_score_on_time AS DOUBLE) / n_scored_on_time)
             - (CAST(sum_score_late AS DOUBLE) / n_scored_late)
        ELSE CAST(NULL AS DOUBLE)
    END AS score_gap,
    n_delivered AS n_obs,
    {{ is_small_sample('n_delivered', 'n_valid_orders') }} AS is_small_sample,
    {{ is_small_gap_group('n_scored_on_time', 'n_scored_late') }} AS is_small_gap_group
FROM base
ORDER BY year_month

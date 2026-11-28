{#
  dbt/models/gold/kpi_delivery_state_month.sql - Bảng KPI UC-02 theo bang và tháng mua (B-412)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-06
  Tham chiếu:
  - Requirement v3 (UC-02; Mẫu nhỏ; Hiệu năng truy vấn Q3)
  - Design Gold v2 (Mục V: Bảng lát cắt, Mẫu cột chung, Thành phần theo UC-02; Mục VI: Truy vấn Q3)
  - dbt/schema_contract.yml (gold_tables.kpi_delivery_state_month - 23 cột)

  Lát cắt: (customer_state, year_month) - chỉ giữ cặp bang và tháng có ít nhất một đơn.
  Cờ mẫu nhỏ: nhóm dưới 30 đơn đã giao (n_obs = n_delivered).
  Cờ nhóm giao trễ (is_small_gap_group): n_scored_on_time < 30 HOẶC n_scored_late < 30.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH base AS (
    SELECT
        c.customer_state,
        d.year_month,
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
    INNER JOIN {{ ref('dim_customer') }} c
        ON f.customer_id = c.customer_id
    INNER JOIN {{ ref('dim_date') }} d
        ON f.purchase_date = d."date"
    GROUP BY c.customer_state, d.year_month
)

SELECT
    customer_state,
    year_month,
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
    {{ is_small_sample('n_delivered') }} AS is_small_sample,
    {{ is_small_gap_group('n_scored_on_time', 'n_scored_late') }} AS is_small_gap_group
FROM base

{#
  dbt/models/gold/kpi_lead_month.sql - Bảng KPI UC-03 theo tháng liên hệ đầu (B-413)
  Tác giả phụ trách: Hoàng | Reviewer: Kiên
  Tham chiếu:
  - Requirement v3 (UC-03; Mẫu nhỏ)
  - Design Gold v2 (Mục V: Bảng lát cắt, Mẫu cột chung, Thành phần theo UC-03)
  - dbt/schema_contract.yml (gold_tables.kpi_lead_month - 12 cột)

  Lát cắt: year_month (tháng của first_contact_date).
  Dòng có mặt: mọi tháng có lead, không chặn khoảng tháng.
  Cờ mẫu nhỏ: nhóm dưới 30 deal (n_obs = n_leads_with_deal).
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH base AS (
    SELECT
        d.year_month,
        COUNT(*) AS n_leads,
        SUM(CASE WHEN f.has_deal THEN 1 ELSE 0 END) AS n_leads_with_deal,
        SUM(
            CASE
                WHEN f.has_deal AND NOT f.is_close_time_excluded AND f.days_to_close IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS n_close_time_valid_deals,
        COALESCE(
            SUM(
                CASE
                    WHEN f.has_deal AND NOT f.is_close_time_excluded AND f.days_to_close IS NOT NULL
                    THEN f.days_to_close
                END
            ),
            0
        ) AS sum_days_to_close,
        SUM(CASE WHEN NOT f.is_close_time_excluded THEN 1 ELSE 0 END) AS n_leads_not_excluded,
        SUM(
            CASE
                WHEN f.has_deal
                     AND NOT f.is_close_time_excluded
                     AND f.days_to_close IS NOT NULL
                     AND f.days_to_close <= 30
                THEN 1
                ELSE 0
            END
        ) AS n_won_30d
    FROM {{ ref('fact_lead') }} f
    INNER JOIN {{ ref('dim_date') }} d
        ON f.first_contact_date = d."date"
    GROUP BY d.year_month
)

SELECT
    year_month,
    n_leads,
    n_leads_with_deal,
    n_close_time_valid_deals,
    sum_days_to_close,
    n_leads_not_excluded,
    n_won_30d,
    CASE
        WHEN n_leads > 0 THEN CAST(n_leads_with_deal AS DOUBLE) / n_leads
        ELSE NULL
    END AS lead_to_deal_rate,
    CASE
        WHEN n_close_time_valid_deals > 0 THEN CAST(sum_days_to_close AS DOUBLE) / n_close_time_valid_deals
        ELSE NULL
    END AS avg_days_to_close,
    CASE
        WHEN n_leads_not_excluded > 0 THEN CAST(n_won_30d AS DOUBLE) / n_leads_not_excluded
        ELSE NULL
    END AS won_30d_rate,
    n_leads_with_deal AS n_obs,
    {{ is_small_sample('n_leads_with_deal') }} AS is_small_sample
FROM base

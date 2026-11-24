{#
  dbt/models/gold/fact_lead.sql - Bảng Fact Gold cho Marketing Funnel (B-407)
  Tác giả phụ trách: Hoàng | Reviewer: Kiên
  Tham chiếu:
  - Design Gold v2 (Mục III: gold.fact_lead; Mục IV: Xử lý cờ C1; Mục VII: Lead không có seller)
  - dbt/schema_contract.yml (gold_tables.fact_lead - 8 cột)

  Grain: 1 dòng = 1 lead (mql_id).
  Không có seller_id (giữ đúng Out of scope: không nối Marketing với đơn hàng).
#}

{{ config(materialized='table') }}

SELECT
    l.mql_id,
    l.first_contact_date,
    l.origin,
    l.has_deal,
    l.won_date,
    CASE
        WHEN l.has_deal AND l.won_date IS NOT NULL
        THEN CAST({{ calendar_days('l.won_date', 'l.first_contact_date') }} AS INTEGER)
        ELSE NULL
    END AS days_to_close,
    {{ has_flag('l.dq_flag', 'C1') }} AS is_close_time_excluded,
    l.batch_id
FROM {{ ref('leads') }} l

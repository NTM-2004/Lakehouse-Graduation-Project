{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/leads_rejects.sql - Bảng reject mức 1 cho silver.leads (B-318)
  Tác giả phụ trách: Hoàng | Reviewer: Kiên
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.leads)

  Quy tắc:
  - Dòng JSON lỗi cú pháp (is_parsed = false) KHÔNG vào bảng reject (giữ ở Bronze, kiểm thử error F09).
  - Các dòng is_parsed = true nhưng thiếu hoặc không ép được cột bắt buộc mức 1
    (mql_id, first_contact_date, landing_page_id) chuyển vào silver.leads_rejects.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason.
#}

WITH parsed_bronze AS (
    SELECT
        src.mql_id,
        src.first_contact_date,
        src.landing_page_id,
        src.origin,
        src.deal,
        src.raw_json,
        src.is_parsed,
        src.batch_id,
        src.source_file,
        src.ingested_at,
        NULLIF(TRIM(src.mql_id), '') AS clean_mql_id,
        NULLIF(TRIM(src.first_contact_date), '') AS clean_first_contact_date,
        NULLIF(TRIM(src.landing_page_id), '') AS clean_landing_page_id,
        {{ safe_cast("NULLIF(TRIM(src.first_contact_date), '')", 'DATE') }} AS casted_first_contact_date
    FROM {{ source('bronze_marketing', 'marketing_leads') }} src
    WHERE src.is_parsed = true
),

evaluated AS (
    SELECT
        mql_id,
        first_contact_date,
        landing_page_id,
        origin,
        deal,
        raw_json,
        is_parsed,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_mql_id IS NULL", "MISSING_MQL_ID"),
            ("clean_first_contact_date IS NULL", "MISSING_FIRST_CONTACT_DATE"),
            ("clean_first_contact_date IS NOT NULL AND casted_first_contact_date IS NULL", "INVALID_FIRST_CONTACT_DATE"),
            ("clean_landing_page_id IS NULL", "MISSING_LANDING_PAGE_ID")
        ]) }} AS reject_reason
    FROM parsed_bronze
)

SELECT
    mql_id,
    first_contact_date,
    landing_page_id,
    origin,
    deal,
    raw_json,
    is_parsed,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

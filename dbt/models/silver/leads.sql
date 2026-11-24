{#
  dbt/models/silver/leads.sql - Mô hình Silver cho Marketing Funnel đã duỗi phẳng (B-318)
  Tác giả phụ trách: Hoàng | Reviewer: Kiên
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục IV: silver.leads; Bảng tra cờ; Cột bắt buộc mức 1)
  - dbt/schema_contract.yml (silver_tables.leads - 21 cột)

  Luồng xử lý:
  1. Lọc is_parsed = true từ bronze.marketing_leads (dòng lỗi JSON giữ ở Bronze, kiểm thử error F09).
  2. Tách lỗi mức 1 (thiếu/hỏng mql_id, first_contact_date, landing_page_id) bằng reject_rows macro
     TRƯỚC KHI gọi latest_snapshot (theo quy tắc của latest_snapshot.sql).
  3. Loại trùng theo mql_id, giữ bản chụp có batch_seq lớn nhất (127 lead phát lại kèm deal).
  4. Duỗi phẳng nhóm deal, ép kiểu an toàn (safe_cast + cờ T1), chuẩn hóa 'unknown' cho cột phân loại,
     gắn cờ C1 (WON_BEFORE_CONTACT) khi CAST(won_date AS DATE) < first_contact_date.
#}

{{ config(materialized='table') }}

WITH parsed_bronze AS (
    SELECT
        src.mql_id,
        src.first_contact_date,
        src.landing_page_id,
        src.origin,
        src.deal,
        src.source_file,
        src.batch_id,
        NULLIF(TRIM(src.mql_id), '') AS clean_mql_id,
        NULLIF(TRIM(src.first_contact_date), '') AS clean_first_contact_date,
        NULLIF(TRIM(src.landing_page_id), '') AS clean_landing_page_id,
        {{ safe_cast("NULLIF(TRIM(src.first_contact_date), '')", 'DATE') }} AS casted_first_contact_date
    FROM {{ source('bronze', 'marketing_leads') }} src
    WHERE src.is_parsed = true
),

with_reject_check AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_mql_id IS NULL", "MISSING_MQL_ID"),
            ("clean_first_contact_date IS NULL", "MISSING_FIRST_CONTACT_DATE"),
            ("clean_first_contact_date IS NOT NULL AND casted_first_contact_date IS NULL", "INVALID_FIRST_CONTACT_DATE"),
            ("clean_landing_page_id IS NULL", "MISSING_LANDING_PAGE_ID")
        ]) }} AS reject_reason
    FROM parsed_bronze
),

valid_rows AS (
    SELECT
        clean_mql_id AS mql_id,
        casted_first_contact_date AS first_contact_date,
        clean_landing_page_id AS landing_page_id,
        origin,
        deal,
        source_file,
        batch_id
    FROM with_reject_check
    WHERE NOT {{ is_reject('reject_reason') }}
),

deduped AS (
    {{ latest_snapshot('valid_rows', 'mql_id') }}
),

flattened AS (
    SELECT
        d.mql_id,
        d.first_contact_date,
        d.landing_page_id,
        COALESCE(LOWER(NULLIF(TRIM(d.origin), '')), 'unknown') AS origin,
        (d.deal IS NOT NULL) AS has_deal,

        -- Các trường từ struct deal (NULL nếu chưa có deal)
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.seller_id), '') ELSE NULL END AS seller_id,
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.sdr_id), '') ELSE NULL END AS sdr_id,
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.sr_id), '') ELSE NULL END AS sr_id,

        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.won_date), '') ELSE NULL END AS raw_won_date,
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.has_company), '') ELSE NULL END AS raw_has_company,
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.has_gtin), '') ELSE NULL END AS raw_has_gtin,
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.declared_product_catalog_size), '') ELSE NULL END AS raw_catalog_size,
        CASE WHEN d.deal IS NOT NULL THEN NULLIF(TRIM(d.deal.declared_monthly_revenue), '') ELSE NULL END AS raw_monthly_revenue,

        -- 5 cột phân loại của deal: 'unknown' nếu thiếu CHỈ KHI has_deal = true; không có deal thì NULL
        CASE
            WHEN d.deal IS NOT NULL THEN COALESCE(NULLIF(TRIM(d.deal.business_segment), ''), 'unknown')
            ELSE NULL
        END AS business_segment,
        CASE
            WHEN d.deal IS NOT NULL THEN COALESCE(NULLIF(TRIM(d.deal.lead_type), ''), 'unknown')
            ELSE NULL
        END AS lead_type,
        CASE
            WHEN d.deal IS NOT NULL THEN COALESCE(NULLIF(TRIM(d.deal.lead_behaviour_profile), ''), 'unknown')
            ELSE NULL
        END AS lead_behaviour_profile,
        CASE
            WHEN d.deal IS NOT NULL THEN COALESCE(NULLIF(TRIM(d.deal.average_stock), ''), 'unknown')
            ELSE NULL
        END AS average_stock,
        CASE
            WHEN d.deal IS NOT NULL THEN COALESCE(NULLIF(TRIM(d.deal.business_type), ''), 'unknown')
            ELSE NULL
        END AS business_type,

        d.source_file,
        d.batch_id
    FROM deduped d
),

typed AS (
    SELECT
        mql_id,
        first_contact_date,
        landing_page_id,
        origin,
        has_deal,
        seller_id,
        sdr_id,
        sr_id,

        raw_won_date,
        {{ safe_cast('raw_won_date', 'TIMESTAMP') }} AS won_date,

        business_segment,
        lead_type,
        lead_behaviour_profile,
        average_stock,
        business_type,

        raw_has_company,
        CASE
            WHEN raw_has_company IS NULL THEN NULL
            WHEN LOWER(raw_has_company) IN ('true', '1', '1.0') THEN true
            WHEN LOWER(raw_has_company) IN ('false', '0', '0.0') THEN false
            ELSE NULL
        END AS has_company,

        raw_has_gtin,
        CASE
            WHEN raw_has_gtin IS NULL THEN NULL
            WHEN LOWER(raw_has_gtin) IN ('true', '1', '1.0') THEN true
            WHEN LOWER(raw_has_gtin) IN ('false', '0', '0.0') THEN false
            ELSE NULL
        END AS has_gtin,

        raw_catalog_size,
        -- Olist CSV lưu declared_product_catalog_size dạng số thực (vd '100.0'); ép qua DOUBLE rồi sang INTEGER
        CASE
            WHEN raw_catalog_size IS NULL THEN NULL
            WHEN {{ safe_cast('raw_catalog_size', 'DOUBLE') }} IS NOT NULL
                 AND {{ safe_cast('raw_catalog_size', 'DOUBLE') }} = FLOOR({{ safe_cast('raw_catalog_size', 'DOUBLE') }})
            THEN CAST({{ safe_cast('raw_catalog_size', 'DOUBLE') }} AS INTEGER)
            ELSE NULL
        END AS declared_product_catalog_size,

        raw_monthly_revenue,
        {{ safe_cast('raw_monthly_revenue', 'DECIMAL(14, 2)') }} AS declared_monthly_revenue,

        source_file,
        batch_id
    FROM flattened
)

SELECT
    mql_id,
    first_contact_date,
    landing_page_id,
    origin,
    has_deal,
    seller_id,
    sdr_id,
    sr_id,
    won_date,
    business_segment,
    lead_type,
    lead_behaviour_profile,
    average_stock,
    business_type,
    has_company,
    has_gtin,
    declared_product_catalog_size,
    declared_monthly_revenue,
    {{ build_dq_flag([
        ("C1", "has_deal AND won_date IS NOT NULL AND CAST(won_date AS DATE) < first_contact_date"),
        ("T1:won_date", "raw_won_date IS NOT NULL AND won_date IS NULL"),
        ("T1:has_company", "raw_has_company IS NOT NULL AND has_company IS NULL"),
        ("T1:has_gtin", "raw_has_gtin IS NOT NULL AND has_gtin IS NULL"),
        ("T1:declared_product_catalog_size", "raw_catalog_size IS NOT NULL AND declared_product_catalog_size IS NULL"),
        ("T1:declared_monthly_revenue", "raw_monthly_revenue IS NOT NULL AND declared_monthly_revenue IS NULL")
    ]) }} AS dq_flag,
    source_file,
    batch_id
FROM typed

{#
  dbt/tests/generic/silver_leads_tests.sql - Các kiểm thử generic chuyên biệt cho Silver Marketing (B-318)
  Tác giả phụ trách: Hoàng | Reviewer: Kiên
  Tham chiếu:
  - Design Bronze, Silver v2: Mục I (Mốc sau ngày chốt, Kiểm thử chung), Mục IV (silver.leads)
  - Requirement v3: F08, F09
  - Đồng bộ mẫu triển khai với dbt/tests/generic/silver_clickstream_tests.sql (Minh)
#}

{% test leads_bronze_no_unparsed(model) %}
  {# F09 (mức error): Không còn dòng is_parsed = false ở bronze_marketing.marketing_leads #}
  SELECT
    batch_id,
    source_file,
    raw_json
  FROM {{ source('bronze_marketing', 'marketing_leads') }}
  WHERE is_parsed IS NULL OR is_parsed = false
{% endtest %}


{% test leads_no_post_cutoff_dates(model) %}
  {# Mốc sau ngày chốt (mức error): first_contact_date và won_date không vượt quá cutoff_date #}
  SELECT
    mql_id,
    first_contact_date,
    won_date
  FROM {{ model }}
  WHERE first_contact_date > CAST({{ get_cutoff_date() }} AS DATE)
     OR (won_date IS NOT NULL AND CAST(won_date AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
{% endtest %}


{% test leads_counts_match_bronze(model) %}
  {# Mức error: Số lead (+ rejects) và số deal ở Silver khớp Bronze #}
  WITH silver_cnt AS (
    SELECT
      COUNT(*) AS n_leads,
      SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) AS n_deals
    FROM {{ model }}
  ),
  bronze_cnt AS (
    SELECT
      COUNT(DISTINCT NULLIF(TRIM(mql_id), '')) AS n_leads,
      COUNT(DISTINCT CASE WHEN deal IS NOT NULL THEN NULLIF(TRIM(mql_id), '') END) AS n_deals
    FROM {{ source('bronze_marketing', 'marketing_leads') }}
    WHERE is_parsed = true
      AND NULLIF(TRIM(mql_id), '') IS NOT NULL
  ),
  reject_cnt AS (
    SELECT COUNT(*) AS n_rejects
    FROM {{ ref('leads_rejects') }}
  )
  SELECT
    s.n_leads AS silver_leads,
    b.n_leads AS bronze_leads,
    s.n_deals AS silver_deals,
    b.n_deals AS bronze_deals,
    r.n_rejects AS reject_leads
  FROM silver_cnt s
  CROSS JOIN bronze_cnt b
  CROSS JOIN reject_cnt r
  WHERE (s.n_leads + r.n_rejects) != b.n_leads
     OR s.n_deals != b.n_deals
{% endtest %}


{% test leads_bronze_replay_guardrail(model) %}
  {#
    Hàng rào phát lại ở Bronze (mức error):
    Mỗi mql_id có tối đa 2 dòng ở Bronze; nếu có 2 dòng thì dòng bulk (batch_seq = 0)
    chưa có deal và dòng ở batch sau phải có deal.
  #}
  WITH grouped AS (
    SELECT
      src.mql_id,
      COUNT(*) AS row_cnt,
      SUM(CASE WHEN bs.batch_seq = 0 AND src.deal IS NOT NULL THEN 1 ELSE 0 END) AS bulk_with_deal,
      SUM(CASE WHEN bs.batch_seq > 0 AND src.deal IS NULL THEN 1 ELSE 0 END) AS later_without_deal
    FROM {{ source('bronze_marketing', 'marketing_leads') }} src
    INNER JOIN {{ ref('batch_seq') }} bs
      ON src.batch_id = bs.batch_id
    WHERE src.is_parsed = true
    GROUP BY src.mql_id
  )
  SELECT
    mql_id,
    row_cnt,
    bulk_with_deal,
    later_without_deal
  FROM grouped
  WHERE row_cnt > 2
     OR (row_cnt = 2 AND (bulk_with_deal > 0 OR later_without_deal > 0))
{% endtest %}


{% test leads_an13_expected_counts(model) %}
  {# Mức warn, chỉ chạy sau batch cuối: số dòng cờ C1 khớp an_expected (AN-13, C1 = 1) #}
  {% if is_final_batch() %}
    WITH actual_cnt AS (
      SELECT COUNT(*) AS actual_count
      FROM {{ model }}
      WHERE {{ has_flag('dq_flag', 'C1') }}
    ),
    expected_cnt AS (
      SELECT expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-13'
        AND sub_code = 'C1'
        AND table_name = 'leads'
    )
    SELECT
      a.actual_count,
      e.expected_count
    FROM actual_cnt a
    CROSS JOIN expected_cnt e
    WHERE a.actual_count != e.expected_count
  {% else %}
    SELECT 1 AS dummy WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test leads_final_batch_counts(model) %}
  {# Mức warn, chỉ chạy sau batch cuối: tổng lead = 8.000 và tổng deal = 792 #}
  {% if is_final_batch() %}
    SELECT
      COUNT(*) AS n_leads,
      SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) AS n_deals
    FROM {{ model }}
    HAVING COUNT(*) != 8000
        OR SUM(CASE WHEN has_deal THEN 1 ELSE 0 END) != 792
  {% else %}
    SELECT 1 AS dummy WHERE 1 = 0
  {% endif %}
{% endtest %}

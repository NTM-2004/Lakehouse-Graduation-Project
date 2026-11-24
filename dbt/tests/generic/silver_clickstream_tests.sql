{#
  silver_clickstream_tests.sql - Các kiểm thử generic chuyên biệt cho Silver Clickstream (B-320)
  Tham chiếu:
  - Design Bronze, Silver v2: Mục I (Cột bắt buộc & Kiểm thử chung), Mục IV (silver.clickstream_events)
  - Requirement v3: F08, F09
#}

{% test reject_count_zero(model) %}
  SELECT *
  FROM {{ model }}
{% endtest %}


{% test timestamp_not_after_cutoff(model, column_name) %}
  SELECT
    {{ column_name }}
  FROM {{ model }}
  WHERE {{ column_name }} > CAST({{ get_cutoff_timestamp() }} AS TIMESTAMP)
{% endtest %}


{% test clickstream_order_orphans_exceed_expected(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (Ngoại lệ: log -> đơn):
    - Batch giữa: không được vượt số kỳ vọng trong seed orphan_expected (3 / 8 / 20).
    - Batch cuối (2018-08): số kỳ vọng = 0, bất kỳ mồ côi nào > 0 đều là lỗi mức error.
  #}
  {% set cur_batch = var('batch_id', var('final_batch_id', '2018-08')) %}
  {% if cur_batch == 'bulk_2016-09_2018-05' %}
    {% set expected_col = 'bulk_2016_09_2018_05' %}
  {% elif cur_batch == '2018-06' %}
    {% set expected_col = 'batch_2018_06' %}
  {% elif cur_batch == '2018-07' %}
    {% set expected_col = 'batch_2018_07' %}
  {% else %}
    {% set expected_col = 'batch_2018_08' %}
  {% endif %}

  WITH orphan_cnt AS (
    SELECT COUNT(*) AS actual_orphans
    FROM {{ model }} e
    LEFT JOIN {{ ref('orders') }} o
      ON e.order_id = o.order_id
    WHERE e.order_id IS NOT NULL
      AND o.order_id IS NULL
  ),
  expected AS (
    SELECT {{ expected_col }} AS expected_orphans
    FROM {{ ref('orphan_expected') }}
    WHERE relation = 'clickstream_to_orders'
  )
  SELECT
    oc.actual_orphans,
    ex.expected_orphans
  FROM orphan_cnt oc
  CROSS JOIN expected ex
  WHERE oc.actual_orphans > ex.expected_orphans
{% endtest %}


{% test clickstream_order_orphans_middle_warn(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (Ngoại lệ: log -> đơn):
    - Chỉ cảnh báo ở batch giữa khi có bản ghi mồ côi trong ngưỡng kỳ vọng (0 < actual <= expected).
    - Ở batch cuối (2018-08), kiểm thử mức error ở trên đã chặn mọi trường hợp > 0.
  #}
  {% if is_middle_batch() %}
    {% set cur_batch = var('batch_id', var('final_batch_id', '2018-08')) %}
    {% if cur_batch == 'bulk_2016-09_2018-05' %}
      {% set expected_col = 'bulk_2016_09_2018_05' %}
    {% elif cur_batch == '2018-06' %}
      {% set expected_col = 'batch_2018_06' %}
    {% else %}
      {% set expected_col = 'batch_2018_07' %}
    {% endif %}

    WITH orphan_cnt AS (
      SELECT COUNT(*) AS actual_orphans
      FROM {{ model }} e
      LEFT JOIN {{ ref('orders') }} o
        ON e.order_id = o.order_id
      WHERE e.order_id IS NOT NULL
        AND o.order_id IS NULL
    ),
    expected AS (
      SELECT {{ expected_col }} AS expected_orphans
      FROM {{ ref('orphan_expected') }}
      WHERE relation = 'clickstream_to_orders'
    )
    SELECT
      oc.actual_orphans,
      ex.expected_orphans
    FROM orphan_cnt oc
    CROSS JOIN expected ex
    WHERE oc.actual_orphans > 0
      AND oc.actual_orphans <= ex.expected_orphans
  {% else %}
    SELECT 1 AS dummy WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test clickstream_purchase_amount_matches_order(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (silver.clickstream_events):
    Giá trị purchase (amount) bằng tổng price của đơn trong silver.order_items.
  #}
  WITH purchase_events AS (
    SELECT
      event_id,
      order_id,
      amount
    FROM {{ model }}
    WHERE event_type = 'purchase'
      AND order_id IS NOT NULL
  ),
  order_totals AS (
    SELECT
      order_id,
      SUM(price) AS total_price
    FROM {{ ref('order_items') }}
    GROUP BY order_id
  )
  SELECT
    p.event_id,
    p.order_id,
    p.amount,
    ot.total_price
  FROM purchase_events p
  INNER JOIN order_totals ot
    ON p.order_id = ot.order_id
  WHERE p.amount IS NULL
     OR ABS(p.amount - ot.total_price) > 0.005
{% endtest %}


{% test clickstream_an15_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (Kiểm thử đếm theo AN):
    Chỉ chạy sau batch cuối (2018-08):
    - Số phiên có cờ E1 khớp an_expected (AN-15, E1 = 453)
    - Số sự kiện có cờ E2 khớp an_expected (AN-15, E2 = 6)
  #}
  {% if is_final_batch() %}
    WITH actual_counts AS (
      SELECT
        'E1' AS sub_code,
        COUNT(DISTINCT CASE WHEN {{ has_flag('dq_flag', 'E1') }} THEN session_id END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'E2' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'E2') }} THEN 1 END) AS actual_count
      FROM {{ model }}
    ),
    expected_counts AS (
      SELECT
        sub_code,
        expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-15'
        AND table_name = 'clickstream_events'
    )
    SELECT
      e.sub_code,
      a.actual_count,
      e.expected_count
    FROM expected_counts e
    LEFT JOIN actual_counts a
      ON e.sub_code = a.sub_code
    WHERE COALESCE(a.actual_count, -1) != e.expected_count
  {% else %}
    SELECT 1 AS dummy WHERE 1 = 0
  {% endif %}
{% endtest %}

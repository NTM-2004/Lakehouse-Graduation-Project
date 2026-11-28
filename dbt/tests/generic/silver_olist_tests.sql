{#
  dbt/tests/generic/silver_olist_tests.sql - Các kiểm thử generic cho Silver Olist (B-301..B-317)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Tham chiếu:
  - Design Bronze, Silver v2: Mục I (Cột bắt buộc mức 1, Kiểm thử chung, Bảng tra cờ, Nhóm AN không gắn cờ), Mục IV
  - Requirement v3: F08, F17
  - Seed: dbt/seeds/an_expected.csv (B-017)
  Lưu ý: Tái sử dụng macro reject_count_zero đã định nghĩa trong silver_clickstream_tests.sql (không định nghĩa trùng).
#}

{% test zip_code_length_five(model, column_name) %}
  {# Kiểm thử mức ERROR: mã bưu chính khác NULL, độ dài đúng 5 chữ số (giữ số 0 đứng đầu) #}
  SELECT
    {{ column_name }}
  FROM {{ model }}
  WHERE {{ column_name }} IS NULL
     OR LENGTH({{ column_name }}) != 5
     OR NOT REGEXP_LIKE({{ column_name }}, '^[0-9]{5}$')
{% endtest %}


{% test seller_city_valid_format(model, column_name) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (silver.sellers, AN-11):
    Tên thành phố sau chuẩn hóa phải khác NULL/rỗng, không có khoảng trắng đầu cuối hay khoảng trắng kép,
    chỉ gồm chữ thường ASCII, khoảng trắng, dấu nháy đơn hoặc gạch ngang, không là mã viết tắt (sp, sbc, poa)
    và không còn hậu tố '- <mã bang 2 chữ>'.
  #}
  SELECT
    seller_id,
    {{ column_name }}
  FROM {{ model }}
  WHERE {{ column_name }} IS NULL
     OR TRIM({{ column_name }}) = ''
     OR {{ column_name }} != TRIM({{ column_name }})
     OR {{ column_name }} LIKE '%  %'
     OR NOT REGEXP_LIKE({{ column_name }}, '^[a-z'' -]+$')
     OR {{ column_name }} IN ('sp', 'sbc', 'poa')
     OR REGEXP_LIKE({{ column_name }}, '-\s*(ac|al|am|ap|ba|ce|df|es|go|ma|mg|ms|mt|pa|pb|pe|pi|pr|rj|rn|ro|rr|rs|sc|se|sp|to)$')
{% endtest %}


{% test sellers_an11_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-305, AN-11):
    Chỉ chạy sau batch cuối (2018-08): số người bán có seller_city đổi tên sau chuẩn hóa
    khớp an_expected (AN-11, table_name='sellers', expected_count=38).
  #}
  {% if is_final_batch() %}
    WITH actual_cnt AS (
      SELECT COUNT(*) AS actual_count
      FROM {{ model }} s
      INNER JOIN {{ source('bronze_olist', 'sellers') }} b
        ON s.seller_id = b.seller_id
      WHERE COALESCE(s.seller_city, '') != COALESCE(b.seller_city, '')
    ),
    expected_cnt AS (
      SELECT expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-11'
        AND table_name = 'sellers'
    )
    SELECT
      a.actual_count,
      e.expected_count
    FROM actual_cnt a
    CROSS JOIN expected_cnt e
    WHERE a.actual_count != e.expected_count
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test translation_an10_missing_categories(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-301 / B-307, AN-10):
    Gắn trên silver.products để giữ phụ thuộc DAG một chiều (product_category_translation -> products).
    Chỉ chạy sau batch cuối (2018-08): số danh mục khác 'unknown' trong silver.products
    không có bản dịch trong silver.product_category_translation khớp an_expected (= 2).
  #}
  {% if is_final_batch() %}
    WITH actual_cnt AS (
      SELECT COUNT(DISTINCT p.product_category_name) AS actual_count
      FROM {{ model }} p
      LEFT JOIN {{ ref('product_category_translation') }} t
        ON p.product_category_name = t.product_category_name
      WHERE p.product_category_name != 'unknown'
        AND t.product_category_name IS NULL
    ),
    expected_cnt AS (
      SELECT expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-10'
        AND sub_code = 'missing_translation_categories'
        AND table_name = 'product_category_translation'
    )
    SELECT
      a.actual_count,
      e.expected_count
    FROM actual_cnt a
    CROSS JOIN expected_cnt e
    WHERE a.actual_count != e.expected_count
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test products_numeric_non_negative(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-307, silver.products):
    Mọi cột số không âm khi khác NULL; riêng product_weight_g phải > 0 khi khác NULL
    (vì giá trị 0 đã chuyển sang NULL kèm cờ PR2).
  #}
  SELECT
    product_id,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
  FROM {{ model }}
  WHERE (product_name_length IS NOT NULL AND product_name_length < 0)
     OR (product_description_length IS NOT NULL AND product_description_length < 0)
     OR (product_photos_qty IS NOT NULL AND product_photos_qty < 0)
     OR (product_weight_g IS NOT NULL AND product_weight_g <= 0)
     OR (product_length_cm IS NOT NULL AND product_length_cm < 0)
     OR (product_height_cm IS NOT NULL AND product_height_cm < 0)
     OR (product_width_cm IS NOT NULL AND product_width_cm < 0)
{% endtest %}


{% test products_an10_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-307, AN-10):
    Chỉ chạy sau batch cuối (2018-08): đối chiếu 4 mốc AN-10 của silver.products với an_expected:
    - PR1 = 610 (không có danh mục)
    - PR2 = 4 (cân nặng bằng 0)
    - PR3 = 2 (thiếu ít nhất một kích thước)
    - missing_translation_products = 13 (sản phẩm có danh mục nhưng thiếu bản dịch)
  #}
  {% if is_final_batch() %}
    WITH actual_counts AS (
      SELECT
        'PR1' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'PR1') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'PR2' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'PR2') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'PR3' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'PR3') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'missing_translation_products' AS sub_code,
        COUNT(CASE WHEN p.product_category_name != 'unknown' AND t.product_category_name IS NULL THEN 1 END) AS actual_count
      FROM {{ model }} p
      LEFT JOIN {{ ref('product_category_translation') }} t
        ON p.product_category_name = t.product_category_name
    ),
    expected_counts AS (
      SELECT
        sub_code,
        expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-10'
        AND table_name = 'products'
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
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test orders_no_post_cutoff_timestamps(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (Mục I: Mốc sau ngày chốt, AN-14):
    Không mốc thời gian đã xảy ra nào (order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date) muộn hơn hết ngày chốt.
    Loại trừ order_estimated_delivery_date vì là mốc kế hoạch.
  #}
  SELECT
    order_id,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date
  FROM {{ model }}
  WHERE CAST(order_purchase_timestamp AS DATE) > CAST({{ get_cutoff_date() }} AS DATE)
     OR (order_approved_at IS NOT NULL AND CAST(order_approved_at AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
     OR (order_delivered_carrier_date IS NOT NULL AND CAST(order_delivered_carrier_date AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
     OR (order_delivered_customer_date IS NOT NULL AND CAST(order_delivered_customer_date AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
{% endtest %}


{% test orders_approved_not_before_purchase(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-309, silver.orders):
    Ngày duyệt không sớm hơn ngày mua (hiện 0 đơn vi phạm).
  #}
  SELECT
    order_id,
    order_purchase_timestamp,
    order_approved_at
  FROM {{ model }}
  WHERE order_approved_at IS NOT NULL
    AND order_approved_at < order_purchase_timestamp
{% endtest %}


{% test orders_an_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-309, AN-01..03, AN-14, AN-16):
    Chỉ chạy sau batch cuối (2018-08): đối chiếu 9 mốc AN của silver.orders với an_expected:
    - AN-01/D4 = 23, AN-02/D3 = 166, AN-02/D6 = 1359
    - AN-03/D1 = 8, AN-03/D2 = 6, AN-03/D5 = 14, AN-03/D7 = 2
    - AN-14/silver_post_cutoff_expected = 0
    - AN-16/total_overdue_undelivered = 1160
    (Lưu ý: AN-04 thuộc kiểm thử liên bảng B-323 vì cần silver.order_items.)
  #}
  {% if is_final_batch() %}
    WITH actual_counts AS (
      SELECT
        'AN-01' AS an_id,
        'D4' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D4') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-02' AS an_id,
        'D3' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D3') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-02' AS an_id,
        'D6' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D6') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-03' AS an_id,
        'D1' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D1') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-03' AS an_id,
        'D2' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D2') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-03' AS an_id,
        'D5' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D5') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-03' AS an_id,
        'D7' AS sub_code,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'D7') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-14' AS an_id,
        'silver_post_cutoff_expected' AS sub_code,
        COUNT(CASE
          WHEN CAST(order_purchase_timestamp AS DATE) > CAST({{ get_cutoff_date() }} AS DATE)
            OR (order_approved_at IS NOT NULL AND CAST(order_approved_at AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
            OR (order_delivered_carrier_date IS NOT NULL AND CAST(order_delivered_carrier_date AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
            OR (order_delivered_customer_date IS NOT NULL AND CAST(order_delivered_customer_date AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
          THEN 1
        END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-16' AS an_id,
        'total_overdue_undelivered' AS sub_code,
        COUNT(CASE
          WHEN order_status = 'shipped'
           AND order_delivered_customer_date IS NULL
           AND order_estimated_delivery_date <= CAST({{ get_cutoff_date() }} AS DATE)
          THEN 1
        END) AS actual_count
      FROM {{ model }}
    ),
    expected_counts AS (
      SELECT
        an_id,
        sub_code,
        expected_count
      FROM {{ ref('an_expected') }}
      WHERE table_name = 'orders'
        AND (
          (an_id = 'AN-01' AND sub_code = 'D4')
          OR (an_id = 'AN-02' AND sub_code IN ('D3', 'D6'))
          OR (an_id = 'AN-03' AND sub_code IN ('D1', 'D2', 'D5', 'D7'))
          OR (an_id = 'AN-14' AND sub_code = 'silver_post_cutoff_expected')
          OR (an_id = 'AN-16' AND sub_code = 'total_overdue_undelivered')
        )
    )
    SELECT
      e.an_id,
      e.sub_code,
      a.actual_count,
      e.expected_count
    FROM expected_counts e
    LEFT JOIN actual_counts a
      ON e.an_id = a.an_id
     AND e.sub_code = a.sub_code
    WHERE COALESCE(a.actual_count, -1) != e.expected_count
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test silver_no_t1_cast_failures(model) %}
  {# Kiểm thử mức WARN theo F08: cảnh báo nếu có bất kỳ dòng nào gắn cờ T1 (lỗi ép kiểu) #}
  SELECT
    dq_flag,
    source_file,
    batch_id
  FROM {{ model }}
  WHERE {{ has_flag('dq_flag', 'T1') }}
{% endtest %}


{% test composite_key_unique(model, columns) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (Mục I: Khóa ghép):
    Đảm bảo tổ hợp các cột khóa ghép không bị trùng lặp (dùng cho order_items, order_payments, order_reviews).
  #}
  SELECT
    {% for col in columns %}
      {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %},
    COUNT(*) AS dup_count
  FROM {{ model }}
  GROUP BY
    {% for col in columns %}
      {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
  HAVING COUNT(*) > 1
{% endtest %}


{% test order_items_positive_price_non_neg_freight(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-311, silver.order_items):
    price > 0 và freight_value >= 0 (vì dòng price <= 0 đã bị đẩy vào bảng reject mức 1).
  #}
  SELECT
    order_id,
    order_item_id,
    price,
    freight_value
  FROM {{ model }}
  WHERE price IS NULL
     OR price <= 0
     OR (freight_value IS NULL AND NOT {{ has_flag('dq_flag', 'T1:freight_value') }})
     OR (freight_value IS NOT NULL AND freight_value < 0)
{% endtest %}


{% test order_items_sequential_item_id(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-311, silver.order_items):
    order_item_id phải đánh số liên tục từ 1 trong mỗi đơn hàng.
  #}
  SELECT
    order_id,
    MIN(order_item_id) AS min_item_id,
    MAX(order_item_id) AS max_item_id,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_item_id) AS distinct_item_ids
  FROM {{ model }}
  GROUP BY order_id
  HAVING MIN(order_item_id) != 1
      OR MAX(order_item_id) != COUNT(*)
      OR COUNT(DISTINCT order_item_id) != COUNT(*)
{% endtest %}


{% test order_items_an05_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-311, AN-05):
    Chỉ chạy sau batch cuối (2018-08): số dòng hàng gắn cờ I1 khớp an_expected (= 4).
  #}
  {% if is_final_batch() %}
    WITH actual_cnt AS (
      SELECT COUNT(*) AS actual_count
      FROM {{ model }}
      WHERE {{ has_flag('dq_flag', 'I1') }}
    ),
    expected_cnt AS (
      SELECT expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-05'
        AND sub_code = 'I1'
        AND table_name = 'order_items'
    )
    SELECT
      a.actual_count,
      e.expected_count
    FROM actual_cnt a
    CROSS JOIN expected_cnt e
    WHERE a.actual_count != e.expected_count
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test order_payments_non_negative_value(model) %}
  {# Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-313): payment_value >= 0 #}
  SELECT
    order_id,
    payment_sequential,
    payment_value
  FROM {{ model }}
  WHERE (payment_value IS NULL AND NOT {{ has_flag('dq_flag', 'T1:payment_value') }})
     OR (payment_value IS NOT NULL AND payment_value < 0)
{% endtest %}


{% test order_payments_an06_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-313, AN-06):
    Chỉ chạy sau batch cuối (2018-08): số dòng thanh toán gắn ít nhất một trong các cờ P1, P2, P3
    khớp an_expected (AN-06, P1_P2_P3 = 10).
  #}
  {% if is_final_batch() %}
    WITH actual_cnt AS (
      SELECT COUNT(*) AS actual_count
      FROM {{ model }}
      WHERE {{ has_flag('dq_flag', 'P1') }}
         OR {{ has_flag('dq_flag', 'P2') }}
         OR {{ has_flag('dq_flag', 'P3') }}
    ),
    expected_cnt AS (
      SELECT expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-06'
        AND sub_code = 'P1_P2_P3'
        AND table_name = 'order_payments'
    )
    SELECT
      a.actual_count,
      e.expected_count
    FROM actual_cnt a
    CROSS JOIN expected_cnt e
    WHERE a.actual_count != e.expected_count
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test order_reviews_score_and_answer_consistency(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-315, silver.order_reviews):
    - review_score phải thuộc thang 1..5 khi có giá trị.
    - review_score và review_answer_timestamp phải cùng NULL (chưa trả lời) hoặc cùng có giá trị.
  #}
  SELECT
    review_id,
    order_id,
    review_score,
    review_answer_timestamp
  FROM {{ model }}
  WHERE (review_score IS NOT NULL AND (review_score < 1 OR review_score > 5))
     OR (review_score IS NULL AND review_answer_timestamp IS NOT NULL)
     OR (review_score IS NOT NULL AND review_answer_timestamp IS NULL)
{% endtest %}


{% test order_reviews_no_post_cutoff_timestamps(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-315, AN-14):
    review_creation_date và review_answer_timestamp không vượt quá ngày chốt cutoff_date.
  #}
  SELECT
    review_id,
    order_id,
    review_creation_date,
    review_answer_timestamp
  FROM {{ model }}
  WHERE review_creation_date > CAST({{ get_cutoff_date() }} AS DATE)
     OR (review_answer_timestamp IS NOT NULL AND CAST(review_answer_timestamp AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
{% endtest %}


{% test order_reviews_answer_not_before_creation(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-315, silver.order_reviews):
    Ngày trả lời không sớm hơn ngày tạo, so theo ngày (hiện 0 dòng vi phạm).
  #}
  SELECT
    review_id,
    order_id,
    review_creation_date,
    review_answer_timestamp
  FROM {{ model }}
  WHERE review_answer_timestamp IS NOT NULL
    AND review_creation_date IS NOT NULL
    AND CAST(review_answer_timestamp AS DATE) < review_creation_date
{% endtest %}


{% test order_reviews_orphans_exceed_expected(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (Ngoại lệ: đánh giá -> đơn):
    - Batch giữa: không vượt số mồ côi kỳ vọng trong seed orphan_expected (2 / 1 / 9).
    - Batch cuối (2018-08): kỳ vọng = 0, mọi mồ côi > 0 là lỗi mức error.
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
    FROM {{ model }} r
    LEFT JOIN {{ ref('orders') }} o
      ON r.order_id = o.order_id
    WHERE o.order_id IS NULL
  ),
  expected AS (
    SELECT {{ expected_col }} AS expected_orphans
    FROM {{ ref('orphan_expected') }}
    WHERE relation = 'order_reviews_to_orders'
  )
  SELECT
    oc.actual_orphans,
    ex.expected_orphans
  FROM orphan_cnt oc
  CROSS JOIN expected ex
  WHERE oc.actual_orphans > ex.expected_orphans
{% endtest %}


{% test order_reviews_orphans_middle_warn(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (Ngoại lệ: đánh giá -> đơn):
    Chỉ cảnh báo ở batch giữa khi có đánh giá mồ côi nằm trong ngưỡng kỳ vọng (0 < actual <= expected).
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
      FROM {{ model }} r
      LEFT JOIN {{ ref('orders') }} o
        ON r.order_id = o.order_id
      WHERE o.order_id IS NULL
    ),
    expected AS (
      SELECT {{ expected_col }} AS expected_orphans
      FROM {{ ref('orphan_expected') }}
      WHERE relation = 'order_reviews_to_orders'
    )
    SELECT
      oc.actual_orphans,
      ex.expected_orphans
    FROM orphan_cnt oc
    CROSS JOIN expected ex
    WHERE oc.actual_orphans > 0
      AND oc.actual_orphans <= ex.expected_orphans
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test order_reviews_an_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-315, AN-08, AN-09, AN-14):
    Chỉ chạy sau batch cuối (2018-08): đối chiếu 3 mốc an_expected của order_reviews:
    - AN-08 (sub_code rỗng): 779 review_id dùng cho nhiều đơn
    - AN-09 (sub_code='R2'): 45 đánh giá tạo trước ngày mua
    - AN-14 (sub_code='reviews_silver_post_cutoff_expected'): 0 phản hồi sau ngày chốt ở Silver
  #}
  {% if is_final_batch() %}
    WITH actual_counts AS (
      SELECT
        'AN-08' AS an_id,
        '' AS sub_code_key,
        COUNT(*) AS actual_count
      FROM (
        SELECT review_id
        FROM {{ model }}
        GROUP BY review_id
        HAVING COUNT(DISTINCT order_id) > 1
      ) multi_order_reviews
      UNION ALL
      SELECT
        'AN-09' AS an_id,
        'R2' AS sub_code_key,
        COUNT(CASE WHEN {{ has_flag('dq_flag', 'R2') }} THEN 1 END) AS actual_count
      FROM {{ model }}
      UNION ALL
      SELECT
        'AN-14' AS an_id,
        'reviews_silver_post_cutoff_expected' AS sub_code_key,
        COUNT(CASE
          WHEN review_creation_date > CAST({{ get_cutoff_date() }} AS DATE)
            OR (review_answer_timestamp IS NOT NULL AND CAST(review_answer_timestamp AS DATE) > CAST({{ get_cutoff_date() }} AS DATE))
          THEN 1
        END) AS actual_count
      FROM {{ model }}
    ),
    expected_counts AS (
      SELECT
        an_id,
        COALESCE(sub_code, '') AS sub_code_key,
        expected_count
      FROM {{ ref('an_expected') }}
      WHERE table_name = 'order_reviews'
        AND (
          an_id = 'AN-08'
          OR (an_id = 'AN-09' AND sub_code = 'R2')
          OR (an_id = 'AN-14' AND sub_code = 'reviews_silver_post_cutoff_expected')
        )
    )
    SELECT
      e.an_id,
      e.sub_code_key,
      a.actual_count,
      e.expected_count
    FROM expected_counts e
    LEFT JOIN actual_counts a
      ON e.an_id = a.an_id
     AND e.sub_code_key = a.sub_code_key
    WHERE COALESCE(a.actual_count, -1) != e.expected_count
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test geolocation_matches_bronze_zips(model) %}
  {#
    Kiểm thử mức ERROR theo Design Bronze, Silver v2 (B-317, silver.geolocation):
    Mỗi mã bưu chính khác rỗng của bronze.geolocation phải có đúng 1 dòng trong silver.geolocation.
  #}
  WITH bronze_zips AS (
    SELECT DISTINCT NULLIF(TRIM(geolocation_zip_code_prefix), '') AS zip_code
    FROM {{ source('bronze_olist', 'geolocation') }}
    WHERE NULLIF(TRIM(geolocation_zip_code_prefix), '') IS NOT NULL
  ),
  silver_zips AS (
    SELECT geolocation_zip_code_prefix AS zip_code
    FROM {{ model }}
  )
  SELECT
    b.zip_code AS bronze_zip,
    s.zip_code AS silver_zip
  FROM bronze_zips b
  FULL OUTER JOIN silver_zips s
    ON b.zip_code = s.zip_code
  WHERE b.zip_code IS NULL
     OR s.zip_code IS NULL
{% endtest %}


{% test geolocation_an12_expected_counts(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-317, AN-12):
    Chỉ chạy sau batch cuối (2018-08):
    - Số dòng tọa độ trùng nguyên văn ở bronze.geolocation khớp an_expected (AN-12 = 261.831).
    - Số mã bưu chính thuộc nhiều bang ở bronze.geolocation = 8 và điểm ngoài khung Brazil = 42 (Data Analyze v2, AN-12).
  #}
  {% if is_final_batch() %}
    WITH bronze_dedup AS (
      SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT
          COALESCE(geolocation_zip_code_prefix, '')
          || '|' || COALESCE(geolocation_lat, '')
          || '|' || COALESCE(geolocation_lng, '')
          || '|' || COALESCE(geolocation_city, '')
          || '|' || COALESCE(geolocation_state, '')
        ) AS distinct_rows,
        SUM(CASE
          WHEN {{ safe_cast('geolocation_lat', 'DOUBLE') }} > 5.27
            OR {{ safe_cast('geolocation_lat', 'DOUBLE') }} < -33.75
            OR {{ safe_cast('geolocation_lng', 'DOUBLE') }} > -34.79
            OR {{ safe_cast('geolocation_lng', 'DOUBLE') }} < -73.99
          THEN 1
          ELSE 0
        END) AS out_of_brazil_points
      FROM {{ source('bronze_olist', 'geolocation') }}
    ),
    multi_state_zips AS (
      SELECT COUNT(*) AS multi_state_cnt
      FROM (
        SELECT NULLIF(TRIM(geolocation_zip_code_prefix), '') AS zip_code
        FROM {{ source('bronze_olist', 'geolocation') }}
        WHERE NULLIF(TRIM(geolocation_zip_code_prefix), '') IS NOT NULL
        GROUP BY NULLIF(TRIM(geolocation_zip_code_prefix), '')
        HAVING COUNT(DISTINCT UPPER(NULLIF(TRIM(geolocation_state), ''))) > 1
      ) t
    ),
    expected_dup AS (
      SELECT expected_count
      FROM {{ ref('an_expected') }}
      WHERE an_id = 'AN-12'
        AND table_name = 'geolocation'
    )
    SELECT
      (bd.total_rows - bd.distinct_rows) AS actual_duplicates,
      ed.expected_count AS expected_duplicates,
      ms.multi_state_cnt AS actual_multi_state_zips,
      bd.out_of_brazil_points AS actual_out_of_brazil_points
    FROM bronze_dedup bd
    CROSS JOIN multi_state_zips ms
    CROSS JOIN expected_dup ed
    WHERE (bd.total_rows - bd.distinct_rows) != ed.expected_count
       OR ms.multi_state_cnt != 8
       OR bd.out_of_brazil_points != 42
  {% else %}
    SELECT 1 AS dummy FROM (VALUES (1)) AS _empty(x) WHERE 1 = 0
  {% endif %}
{% endtest %}


{% test geolocation_out_of_brazil_warn(model) %}
  {#
    Kiểm thử mức WARN theo Design Bronze, Silver v2 (B-317, silver.geolocation):
    Ghi nhận các mã bưu chính có tọa độ đại diện (trung bình) nằm ngoài khung lãnh thổ Brazil
    (lat trong [-33.75, 5.27], lng trong [-73.99, -34.79]). Không loại bỏ các mã này.
  #}
  SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    n_points
  FROM {{ model }}
  WHERE geolocation_lat > 5.27
     OR geolocation_lat < -33.75
     OR geolocation_lng > -34.79
     OR geolocation_lng < -73.99
{% endtest %}

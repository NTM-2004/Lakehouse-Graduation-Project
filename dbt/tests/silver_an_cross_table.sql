{#
  dbt/tests/silver_an_cross_table.sql - Kiểm thử đếm bất thường liên bảng tầng Silver (B-323)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Tham chiếu:
  - Design Build: B-323 (AN-04 orders vs order_items; AN-07 order_payments vs order_items)
  - Design Bronze, Silver v2: Mục I (Nhóm AN không gắn cờ: AN-04, AN-07; Kiểm thử đếm theo AN)
  - Data Analyze v2: Mục 4 (AN-04 = 756 đơn; AN-07 = 303 đơn lệch hơn 1 xu tính theo xu nguyên)
  - Seed: dbt/seeds/an_expected.csv (B-017)

  Chỉ chạy sau batch cuối (2018-08) ở mức warn.
#}

{{ config(severity='warn') }}

{% if is_final_batch() %}
WITH orders_without_items AS (
    SELECT
        'AN-04' AS an_id,
        'orders' AS table_name,
        COUNT(*) AS actual_count
    FROM {{ ref('orders') }} o
    LEFT JOIN (
        SELECT DISTINCT order_id
        FROM {{ ref('order_items') }}
    ) oi
        ON o.order_id = oi.order_id
    WHERE oi.order_id IS NULL
),

item_totals_cents AS (
    SELECT
        order_id,
        SUM(
            CAST(ROUND(price * 100, 0) AS BIGINT)
            + CAST(ROUND(freight_value * 100, 0) AS BIGINT)
        ) AS items_total_cents
    FROM {{ ref('order_items') }}
    GROUP BY order_id
),

payment_totals_cents AS (
    SELECT
        order_id,
        SUM(CAST(ROUND(payment_value * 100, 0) AS BIGINT)) AS payments_total_cents
    FROM {{ ref('order_payments') }}
    GROUP BY order_id
),

payment_diff_gt_1_cent AS (
    SELECT
        'AN-07' AS an_id,
        'order_payments' AS table_name,
        COUNT(*) AS actual_count
    FROM item_totals_cents it
    INNER JOIN payment_totals_cents pt
        ON it.order_id = pt.order_id
    WHERE ABS(pt.payments_total_cents - it.items_total_cents) > 1
),

actual_counts AS (
    SELECT an_id, table_name, actual_count FROM orders_without_items
    UNION ALL
    SELECT an_id, table_name, actual_count FROM payment_diff_gt_1_cent
),

expected_counts AS (
    SELECT
        an_id,
        table_name,
        expected_count
    FROM {{ ref('an_expected') }}
    WHERE (an_id = 'AN-04' AND table_name = 'orders')
       OR (an_id = 'AN-07' AND table_name = 'order_payments')
)

SELECT
    e.an_id,
    e.table_name,
    a.actual_count,
    e.expected_count
FROM expected_counts e
LEFT JOIN actual_counts a
    ON e.an_id = a.an_id
   AND e.table_name = a.table_name
WHERE COALESCE(a.actual_count, -1) != e.expected_count
{% else %}
SELECT
    CAST(NULL AS VARCHAR) AS an_id,
    CAST(NULL AS VARCHAR) AS table_name,
    CAST(NULL AS BIGINT) AS actual_count,
    CAST(NULL AS INTEGER) AS expected_count
FROM (VALUES (1)) AS _empty(x)
WHERE 1 = 0
{% endif %}

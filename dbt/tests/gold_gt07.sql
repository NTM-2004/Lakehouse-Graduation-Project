{#
  dbt/tests/gold_gt07.sql - GT-07: Mỗi phiên có tối đa một sự kiện mua (B-423)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-07
  Mức: error (Design Gold v2 - Kiểm thử Gold GT-07, Quy tắc Kết quả lặp lại)

  Kiểm tra:
  - Mỗi phiên trong silver.clickstream_events có tối đa 1 sự kiện purchase,
    để order_id và purchase_amount ở gold.fact_session luôn xác định duy nhất.
  - Phiên có has_purchase = true ở gold.fact_session phải khớp đúng 1 sự kiện purchase ở Silver.
#}

{{ config(severity='error') }}

WITH silver_purchase_counts AS (
    SELECT
        session_id,
        COUNT(*) AS n_purchase_events
    FROM {{ ref('clickstream_events') }}
    WHERE event_type = 'purchase'
    GROUP BY session_id
)

SELECT
    fs.session_id,
    fs.has_purchase,
    COALESCE(sp.n_purchase_events, 0) AS n_purchase_events
FROM {{ ref('fact_session') }} fs
LEFT JOIN silver_purchase_counts sp
    ON fs.session_id = sp.session_id
WHERE COALESCE(sp.n_purchase_events, 0) > 1
   OR (fs.has_purchase AND COALESCE(sp.n_purchase_events, 0) != 1)
   OR (NOT fs.has_purchase AND COALESCE(sp.n_purchase_events, 0) != 0)

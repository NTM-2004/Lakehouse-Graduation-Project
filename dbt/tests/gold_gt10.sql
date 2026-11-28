{#
  dbt/tests/gold_gt10.sql - GT-10: Đơn hợp lệ và Đơn hủy trên gold.fact_order (B-426)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-07
  Mức: error (Design Gold v2 - Kiểm thử Gold GT-10; Requirement v3 - Đơn hợp lệ, Đơn hủy)

  Kiểm tra:
  1. is_canceled đúng khi và chỉ khi order_status thuộc ('canceled', 'unavailable').
  2. is_valid_order đúng khi và chỉ khi đơn không phải Đơn hủy (NOT is_canceled) và có ít nhất 1 dòng hàng (n_items > 0).
#}

{{ config(severity='error') }}

SELECT
    order_id,
    order_status,
    n_items,
    is_canceled,
    is_valid_order
FROM {{ ref('fact_order') }}
WHERE is_canceled IS NULL
   OR is_valid_order IS NULL
   OR is_canceled != (COALESCE(order_status, '') IN ('canceled', 'unavailable'))
   OR is_valid_order != (NOT is_canceled AND n_items > 0)

{#
  dbt/tests/gold_gt06.sql - GT-06: Tiền clickstream khớp đơn (B-422)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-07
  Mức: error (F08, Design Gold v2 - Kiểm thử Gold GT-06)

  Kiểm tra:
  - Với mọi phiên ở gold.fact_session có order_id mà đơn đó đã có mặt ở gold.fact_order:
    purchase_amount phải bằng order_revenue của đơn (trên DECIMAL(10, 2), dùng sai số 0.005
    đồng bộ với gold_gt05.sql của nhóm để tránh lệch dấu phẩy động nếu có).
  - Đơn chưa có mặt ở fact_order (mồ côi tạm thời ở batch giữa theo ngoại lệ log -> đơn) thì không xét.
#}

{{ config(severity='error') }}

SELECT
    fs.session_id,
    fs.order_id,
    fs.purchase_amount,
    fo.order_revenue
FROM {{ ref('fact_session') }} fs
INNER JOIN {{ ref('fact_order') }} fo
    ON fs.order_id = fo.order_id
WHERE fs.order_id IS NOT NULL
  AND (
      fs.purchase_amount IS NULL
      OR fo.order_revenue IS NULL
      OR ABS(fs.purchase_amount - fo.order_revenue) > 0.005
  )

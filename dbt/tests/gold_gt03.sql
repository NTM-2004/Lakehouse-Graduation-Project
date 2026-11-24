{#
  dbt/tests/gold_gt03.sql - GT-03: Mọi khóa ngoại fact -> dimension/fact đích đều truy được (B-419)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F08, F10, Design Gold v2 - Quan hệ fact - dimension)
#}

{{ config(severity='error') }}

SELECT 'fact_order.customer_id -> dim_customer' AS fk_name, COUNT(*) AS orphan_cnt
FROM {{ ref('fact_order') }} f
LEFT JOIN {{ ref('dim_customer') }} d ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_order.purchase_date -> dim_date', COUNT(*)
FROM {{ ref('fact_order') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.purchase_date = d."date"
WHERE d."date" IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_order_item.order_id -> fact_order', COUNT(*)
FROM {{ ref('fact_order_item') }} f
LEFT JOIN {{ ref('fact_order') }} p ON f.order_id = p.order_id
WHERE p.order_id IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_order_item.product_id -> dim_product', COUNT(*)
FROM {{ ref('fact_order_item') }} f
LEFT JOIN {{ ref('dim_product') }} d ON f.product_id = d.product_id
WHERE d.product_id IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_order_item.seller_id -> dim_seller', COUNT(*)
FROM {{ ref('fact_order_item') }} f
LEFT JOIN {{ ref('dim_seller') }} d ON f.seller_id = d.seller_id
WHERE d.seller_id IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_order_item.purchase_date -> dim_date', COUNT(*)
FROM {{ ref('fact_order_item') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.purchase_date = d."date"
WHERE d."date" IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_lead.first_contact_date -> dim_date', COUNT(*)
FROM {{ ref('fact_lead') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.first_contact_date = d."date"
WHERE d."date" IS NULL
HAVING COUNT(*) > 0

UNION ALL
SELECT 'fact_session.session_date -> dim_date', COUNT(*)
FROM {{ ref('fact_session') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.session_date = d."date"
WHERE d."date" IS NULL
HAVING COUNT(*) > 0

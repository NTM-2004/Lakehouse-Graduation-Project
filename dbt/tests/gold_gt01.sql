{#
  dbt/tests/gold_gt01.sql - GT-01: Khóa duy nhất và không rỗng cho mọi bảng Gold (B-417)
  Tác giả phụ trách: Hoàng | Reviewer: Minh
  Mức: error (F08, Design Gold v2 - Kiểm thử Gold GT-01)
  Phủ: 4 dimension, 4 fact và khóa lát cắt của 8 bảng KPI.
#}

{{ config(severity='error') }}

-- 4 Dimensions
SELECT 'dim_date' AS table_name, 'NULL_OR_DUP_KEY' AS issue, CAST(COUNT(*) - COUNT(DISTINCT "date") + SUM(CASE WHEN "date" IS NULL THEN 1 ELSE 0 END) AS VARCHAR) AS detail
FROM {{ ref('dim_date') }}
HAVING COUNT(*) != COUNT(DISTINCT "date") OR SUM(CASE WHEN "date" IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'dim_customer', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT customer_id) + SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('dim_customer') }}
HAVING COUNT(*) != COUNT(DISTINCT customer_id) OR SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'dim_product', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT product_id) + SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('dim_product') }}
HAVING COUNT(*) != COUNT(DISTINCT product_id) OR SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'dim_seller', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT seller_id) + SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('dim_seller') }}
HAVING COUNT(*) != COUNT(DISTINCT seller_id) OR SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) > 0

-- 4 Facts
UNION ALL
SELECT 'fact_order', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT order_id) + SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('fact_order') }}
HAVING COUNT(*) != COUNT(DISTINCT order_id) OR SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'fact_order_item', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT order_id || '|' || CAST(order_item_id AS VARCHAR)) + SUM(CASE WHEN order_id IS NULL OR order_item_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('fact_order_item') }}
HAVING COUNT(*) != COUNT(DISTINCT order_id || '|' || CAST(order_item_id AS VARCHAR)) OR SUM(CASE WHEN order_id IS NULL OR order_item_id IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'fact_lead', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT mql_id) + SUM(CASE WHEN mql_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('fact_lead') }}
HAVING COUNT(*) != COUNT(DISTINCT mql_id) OR SUM(CASE WHEN mql_id IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'fact_session', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT session_id) + SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('fact_session') }}
HAVING COUNT(*) != COUNT(DISTINCT session_id) OR SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END) > 0

-- 8 KPI tables
UNION ALL
SELECT 'kpi_sales_month', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT year_month) + SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_sales_month') }}
HAVING COUNT(*) != COUNT(DISTINCT year_month) OR SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_sales_state_month', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT customer_state || '|' || year_month) + SUM(CASE WHEN customer_state IS NULL OR year_month IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_sales_state_month') }}
HAVING COUNT(*) != COUNT(DISTINCT customer_state || '|' || year_month) OR SUM(CASE WHEN customer_state IS NULL OR year_month IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_delivery_month', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT year_month) + SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_delivery_month') }}
HAVING COUNT(*) != COUNT(DISTINCT year_month) OR SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_delivery_state_month', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT customer_state || '|' || year_month) + SUM(CASE WHEN customer_state IS NULL OR year_month IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_delivery_state_month') }}
HAVING COUNT(*) != COUNT(DISTINCT customer_state || '|' || year_month) OR SUM(CASE WHEN customer_state IS NULL OR year_month IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_lead_month', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT year_month) + SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_lead_month') }}
HAVING COUNT(*) != COUNT(DISTINCT year_month) OR SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_lead_origin', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT origin) + SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_lead_origin') }}
HAVING COUNT(*) != COUNT(DISTINCT origin) OR SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_funnel_day', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT session_date) + SUM(CASE WHEN session_date IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_funnel_day') }}
HAVING COUNT(*) != COUNT(DISTINCT session_date) OR SUM(CASE WHEN session_date IS NULL THEN 1 ELSE 0 END) > 0

UNION ALL
SELECT 'kpi_funnel_month', 'NULL_OR_DUP_KEY', CAST(COUNT(*) - COUNT(DISTINCT year_month) + SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) AS VARCHAR)
FROM {{ ref('kpi_funnel_month') }}
HAVING COUNT(*) != COUNT(DISTINCT year_month) OR SUM(CASE WHEN year_month IS NULL THEN 1 ELSE 0 END) > 0

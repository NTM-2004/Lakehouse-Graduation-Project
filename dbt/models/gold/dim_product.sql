{#
  dbt/models/gold/dim_product.sql - Dimension sản phẩm Olist (B-402)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-05
  Tham chiếu:
  - Design Gold v2 (Mục II: gold.dim_product; Mục VII: Danh mục sản phẩm, Thuộc tính dimension)
  - dbt/schema_contract.yml (gold_tables.dim_product - 3 cột)

  Grain: 1 dòng = 1 product_id.
  product_category_en đã được ghép bản dịch và xử lý 'unknown' ở tầng Silver.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    product_id,
    product_category_name,
    product_category_en
FROM {{ ref('products') }}

{#
  dbt/models/gold/dim_seller.sql - Dimension người bán Olist (B-403)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-05
  Tham chiếu:
  - Design Gold v2 (Mục II: gold.dim_seller; Mục VI: Truy vấn chuẩn Q2; Mục VII: Thuộc tính dimension)
  - dbt/schema_contract.yml (gold_tables.dim_seller - 3 cột)

  Grain: 1 dòng = 1 seller_id.
  seller_city đã được chuẩn hóa 4 bước theo AN-11 ở tầng Silver.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    seller_id,
    seller_city,
    seller_state
FROM {{ ref('sellers') }}

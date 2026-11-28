{#
  dbt/models/gold/dim_customer.sql - Dimension khách hàng Olist (B-401)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-05
  Tham chiếu:
  - Design Gold v2 (Mục II: gold.dim_customer; Mục VII: Thuộc tính dimension)
  - dbt/schema_contract.yml (gold_tables.dim_customer - 4 cột)

  Grain: 1 dòng = 1 customer_id (mỗi đơn có một customer_id, không gộp theo người).
  customer_unique_id chỉ giữ làm thuộc tính mô tả, không KPI nào đếm theo người.
#}

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ ref('customers') }}

{#
  small_sample.sql - Macro tính cờ mẫu nhỏ theo thiết kế Gold v2 (B-018)
  - Ngưỡng nhóm quan sát: n_obs < var('small_sample_group', 30)
  - Ngưỡng đơn hàng theo tháng: orders_col < var('small_sample_orders', 100)
    (Áp dụng khi bảng KPI theo tháng, ví dụ kpi_sales_month, kpi_delivery_month)
  - Cờ nhóm giao trễ is_small_gap_group: dành riêng cho Chênh lệch điểm do giao trễ
    (bật khi n_scored_on_time < 30 hoặc n_scored_late < 30)
#}

{% macro is_small_sample(n_obs_col, orders_col=none) %}
  (CASE
    WHEN {{ n_obs_col }} < {{ var('small_sample_group', 30) }}
    {% if orders_col is not none %}
      OR {{ orders_col }} < {{ var('small_sample_orders', 100) }}
    {% endif %}
    THEN true
    ELSE false
  END)
{% endmacro %}

{% macro is_small_gap_group(n_scored_on_time_col, n_scored_late_col) %}
  (CASE
    WHEN {{ n_scored_on_time_col }} < {{ var('small_sample_group', 30) }}
      OR {{ n_scored_late_col }} < {{ var('small_sample_group', 30) }}
    THEN true
    ELSE false
  END)
{% endmacro %}

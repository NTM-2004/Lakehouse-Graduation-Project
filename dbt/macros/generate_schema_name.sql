{#
  generate_schema_name.sql - Tùy biến định danh schema cho dbt (T-03)
  Mặc định dbt ghép target.schema với custom schema (ví dụ: silver_silver, silver_gold).
  Macro này ghi đè logic mặc định: nếu model chỉ định schema (ví dụ 'silver' hoặc 'gold')
  thì dùng trực tiếp schema đó thay vì ghép chuỗi.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{#
  cutoff_batch.sql - Macro quản lý ngày chốt và vị trí batch (B-018)
  Hỗ trợ cả ngữ cảnh Jinja (cho dbt test severity) và ngữ cảnh câu lệnh SQL.
#}

{% macro get_cutoff_date() %}
  '{{ var("cutoff_date", "2018-08-31") }}'
{% endmacro %}

{% macro get_cutoff_timestamp() %}
  '{{ var("cutoff_timestamp", "2018-08-31 23:59:59.999") }}'
{% endmacro %}

{# Kiểm tra batch cuối: trả về True/False trong Jinja hoặc biểu thức boolean trong SQL #}
{% macro is_final_batch(batch_col=none) %}
  {%- set final_batch = var("final_batch_id", "2018-08") -%}
  {%- if batch_col is none -%}
    {{ return(var("batch_id", final_batch) == final_batch) }}
  {%- else -%}
    ({{ batch_col }} = '{{ final_batch }}')
  {%- endif -%}
{% endmacro %}

{# Kiểm tra batch giữa: dùng cho kiểm thử mồ côi (chạy warn ở batch giữa, error ở batch cuối) #}
{% macro is_middle_batch(batch_col=none) %}
  {%- set final_batch = var("final_batch_id", "2018-08") -%}
  {%- if batch_col is none -%}
    {{ return(var("batch_id", final_batch) != final_batch) }}
  {%- else -%}
    ({{ batch_col }} != '{{ final_batch }}')
  {%- endif -%}
{% endmacro %}

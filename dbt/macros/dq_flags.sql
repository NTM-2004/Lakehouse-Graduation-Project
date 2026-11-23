{#
  dq_flags.sql - Macro xử lý cờ chất lượng dữ liệu dq_flag (B-019)
  Cột chứa một hoặc nhiều mã cờ cách nhau bằng dấu phẩy; NULL là sạch.
#}

{% macro has_flag(flag_col, flag_code) %}
  (CASE
    WHEN {{ flag_col }} IS NULL THEN false
    {% if flag_code == 'T1' %}
      {# T1 có dạng T1:<cột> hoặc đứng một mình #}
      WHEN (',' || {{ flag_col }} || ',') LIKE '%,T1,%' OR (',' || {{ flag_col }} || ',') LIKE '%,T1:%' THEN true
    {% else %}
      WHEN (',' || {{ flag_col }} || ',') LIKE '%,' || '{{ flag_code }}' || ',%' THEN true
    {% endif %}
    ELSE false
  END)
{% endmacro %}

{% macro has_flag_prefix(flag_col, prefix) %}
  (CASE
    WHEN {{ flag_col }} IS NULL THEN false
    WHEN (',' || {{ flag_col }} || ',') LIKE '%,' || '{{ prefix }}' || ':%' THEN true
    ELSE false
  END)
{% endmacro %}

{% macro build_dq_flag(flag_expr_pairs) %}
  {#
    flag_expr_pairs: danh sách các tuple (mã cờ, điều kiện boolean)
    Xử lý danh sách rỗng để tránh lỗi cú pháp SQL.
    Sử dụng toán tử ghép chuỗi chuẩn ANSI || để tương thích hoàn toàn với Dremio.
  #}
  {% if not flag_expr_pairs %}
    CAST(NULL AS VARCHAR)
  {% else %}
    NULLIF(
      TRIM(BOTH ',' FROM (
        {% for code, cond in flag_expr_pairs %}
          (CASE WHEN {{ cond }} THEN '{{ code }},' ELSE '' END)
          {%- if not loop.last %} || {% endif %}
        {% endfor %}
      )),
      ''
    )
  {% endif %}
{% endmacro %}

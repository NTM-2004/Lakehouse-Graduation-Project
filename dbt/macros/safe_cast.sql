{#
  safe_cast.sql - Macro ép kiểu an toàn trên Dremio (B-019, đã chốt Spike T-06)
  Dremio 25.0.0 không hỗ trợ TRY_CAST(... AS ...).
  Dùng CASE WHEN + REGEXP_LIKE trên chuỗi đã chuẩn hóa TRIM(CAST(... AS VARCHAR))
  để trả về NULL an toàn khi dữ liệu sai định dạng (áp dụng cho cả cột VARCHAR lẫn số).
#}

{% macro safe_cast(col_name, target_type) %}
  {%- set t = target_type | trim | upper -%}
  {%- set s = "TRIM(CAST(" ~ col_name ~ " AS VARCHAR))" -%}
  {%- if t == 'DATE' -%}
    (CASE
      WHEN {{ col_name }} IS NULL THEN NULL
      WHEN REGEXP_LIKE({{ s }}, '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$')
        THEN CAST({{ s }} AS DATE)
      ELSE NULL
    END)
  {%- elif t == 'TIMESTAMP' -%}
    (CASE
      WHEN {{ col_name }} IS NULL THEN NULL
      WHEN REGEXP_LIKE({{ s }}, '^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])( ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,9})?)?$')
        THEN CAST({{ s }} AS TIMESTAMP)
      ELSE NULL
    END)
  {%- elif t in ['INT', 'INTEGER', 'BIGINT', 'SMALLINT'] -%}
    (CASE
      WHEN {{ col_name }} IS NULL THEN NULL
      WHEN REGEXP_LIKE({{ s }}, '^[+-]?[0-9]+$')
        THEN CAST({{ s }} AS {{ target_type }})
      ELSE NULL
    END)
  {%- elif t.startswith('DECIMAL') or t.startswith('NUMERIC') -%}
    (CASE
      WHEN {{ col_name }} IS NULL THEN NULL
      WHEN REGEXP_LIKE({{ s }}, '^[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)$')
        THEN CAST({{ s }} AS {{ target_type }})
      WHEN REGEXP_LIKE({{ s }}, '^[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)[eE][+-]?[0-9]+$')
        THEN CAST(CAST({{ s }} AS DOUBLE) AS {{ target_type }})
      ELSE NULL
    END)
  {%- elif t in ['DOUBLE', 'FLOAT'] -%}
    (CASE
      WHEN {{ col_name }} IS NULL THEN NULL
      WHEN REGEXP_LIKE({{ s }}, '^[+-]?([0-9]+(\.[0-9]+)?|\.[0-9]+)([eE][+-]?[0-9]+)?$')
        THEN CAST({{ s }} AS {{ target_type }})
      ELSE NULL
    END)
  {%- elif t in ['BOOLEAN', 'BOOL'] -%}
    (CASE
      WHEN {{ col_name }} IS NULL THEN NULL
      WHEN LOWER({{ s }}) IN ('true', '1') THEN true
      WHEN LOWER({{ s }}) IN ('false', '0') THEN false
      ELSE NULL
    END)
  {%- else -%}
    CAST({{ col_name }} AS {{ target_type }})
  {%- endif -%}
{% endmacro %}

{% macro cast_failed_flag(raw_col, casted_col, col_code) %}
  CASE
    WHEN {{ raw_col }} IS NOT NULL AND TRIM(CAST({{ raw_col }} AS VARCHAR)) != '' AND {{ casted_col }} IS NULL
    THEN 'T1:{{ col_code }}'
    ELSE NULL
  END
{% endmacro %}

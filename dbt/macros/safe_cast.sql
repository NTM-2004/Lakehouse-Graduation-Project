{#
  safe_cast.sql - Macro ép kiểu an toàn (B-019, T-06)
  LƯU Ý: Đây là hạng mục T-06 (Việc cần thử) theo Design Deployment và Design Build.
  Trên Dremio, nếu hàm TRY_CAST không được hỗ trợ trong phiên bản hiện tại,
  cần chuyển sang dùng CASE WHEN kết hợp REGEXP_LIKE hoặc kiểm tra dữ liệu trước khi CAST.
#}

{% macro safe_cast(col_name, target_type) %}
  TRY_CAST({{ col_name }} AS {{ target_type }})
{% endmacro %}

{% macro cast_failed_flag(raw_col, casted_col, col_code) %}
  CASE
    WHEN {{ raw_col }} IS NOT NULL AND TRIM(CAST({{ raw_col }} AS VARCHAR)) != '' AND {{ casted_col }} IS NULL
    THEN 'T1:{{ col_code }}'
    ELSE NULL
  END
{% endmacro %}

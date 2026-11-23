{#
  reject_rows.sql - Macro xác định dòng lỗi mức 1 và lý do reject (B-019)
  Mức 1 là các dòng vi phạm cột bắt buộc hoặc khóa chính thiếu, được chuyển vào bảng <bảng>_rejects.
#}

{% macro build_reject_reason(conditions_with_reasons) %}
  {#
    conditions_with_reasons: danh sách các tuple (điều kiện lỗi, chuỗi lý do)
    Xử lý danh sách rỗng để tránh lỗi cú pháp SQL.
    Sử dụng toán tử ghép chuỗi chuẩn ANSI || để tương thích hoàn toàn với Dremio.
  #}
  {% if not conditions_with_reasons %}
    CAST(NULL AS VARCHAR)
  {% else %}
    NULLIF(
      TRIM(BOTH '; ' FROM (
        {% for cond, reason in conditions_with_reasons %}
          (CASE WHEN {{ cond }} THEN '{{ reason }}; ' ELSE '' END)
          {%- if not loop.last %} || {% endif %}
        {% endfor %}
      )),
      ''
    )
  {% endif %}
{% endmacro %}

{% macro is_reject(reject_reason_col) %}
  ({{ reject_reason_col }} IS NOT NULL AND TRIM({{ reject_reason_col }}) != '')
{% endmacro %}

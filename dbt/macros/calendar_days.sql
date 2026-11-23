{#
  calendar_days.sql - Macro tính chênh lệch số ngày lịch nguyên giữa 2 mốc ngày (B-018)
  Thứ tự: end_date trừ start_date.
  Kết quả dương nếu end_date muộn hơn start_date.
#}

{% macro calendar_days(end_date_col, start_date_col) %}
  DATEDIFF(CAST({{ end_date_col }} AS DATE), CAST({{ start_date_col }} AS DATE))
{% endmacro %}

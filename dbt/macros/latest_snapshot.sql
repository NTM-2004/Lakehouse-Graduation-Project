{#
  latest_snapshot.sql - Macro chọn bản chụp có batch_seq lớn nhất cho mỗi khóa (B-018)
  Áp dụng cho orders, order_reviews, leads khi có nhiều bản chụp qua các batch.
  Quy tắc (Design Bronze, Silver v2):
  - Bảng Bronze chỉ có batch_id, cần JOIN ref('batch_seq') để lấy batch_seq số nguyên (0 đến 3).
  - Tuyệt đối không sắp xếp theo chuỗi batch_id vì 'bulk_...' lớn hơn '2018-06'.
  - Không dùng SELECT * EXCLUDE vì Dremio không hỗ trợ cú pháp này; dùng INNER JOIN max_batch
    để trả về đúng và đủ các cột của bảng nguồn src.* mà không sinh thêm cột phụ.
#}

{% macro latest_snapshot(source_table, unique_key_cols, batch_id_col='batch_id') %}
  WITH max_batch AS (
    SELECT
      {% if unique_key_cols is string %}
        src.{{ unique_key_cols }},
      {% else %}
        {% for col in unique_key_cols %}
          src.{{ col }},
        {% endfor %}
      {% endif %}
      MAX(bs.batch_seq) AS max_batch_seq
    FROM {{ source_table }} src
    INNER JOIN {{ ref('batch_seq') }} bs
      ON src.{{ batch_id_col }} = bs.batch_id
    GROUP BY
      {% if unique_key_cols is string %}
        src.{{ unique_key_cols }}
      {% else %}
        {% for col in unique_key_cols %}
          src.{{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
      {% endif %}
  )
  SELECT
    src.*
  FROM {{ source_table }} src
  INNER JOIN {{ ref('batch_seq') }} bs
    ON src.{{ batch_id_col }} = bs.batch_id
  INNER JOIN max_batch mb
    ON {% if unique_key_cols is string %}
         src.{{ unique_key_cols }} = mb.{{ unique_key_cols }}
       {% else %}
         {% for col in unique_key_cols %}
           src.{{ col }} = mb.{{ col }}{% if not loop.last %} AND {% endif %}
         {% endfor %}
       {% endif %}
    AND bs.batch_seq = mb.max_batch_seq
{% endmacro %}

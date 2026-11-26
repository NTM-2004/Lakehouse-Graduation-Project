{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  silver.clickstream_rejects - Bảng bản ghi lỗi mức 1 của Clickstream (B-320)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-03 (ngoại lệ đặt tên: clickstream_rejects theo Design Bronze, Silver v2)
  Tham chiếu:
  - Design Bronze, Silver v2: Mục I (Cột bắt buộc mức 1 và bảng reject), Mục IV (silver.clickstream_events)
  - Requirement v3: F08, F09, F17

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc của silver.clickstream_events):
  - Dòng không đọc được (is_parsed IS NULL hoặc is_parsed = false)
  - Hoặc thiếu session_id, event_ts, event_type, user_id
  - Giữ toàn bộ các cột tầng Bronze của dòng và thêm cột reject_reason ở cuối.
#}

WITH bronze_src AS (
  SELECT
    user_id,
    session_id,
    order_id,
    product_id,
    outcome,
    event_type,
    amount,
    event_ts,
    raw_line,
    is_parsed,
    batch_id,
    source_file,
    ingested_at
  FROM {{ source('bronze_clickstream', 'clickstream_events') }}
),

evaluated AS (
  SELECT
    user_id,
    session_id,
    order_id,
    product_id,
    outcome,
    event_type,
    amount,
    event_ts,
    raw_line,
    is_parsed,
    batch_id,
    source_file,
    ingested_at,
    {{ build_reject_reason([
      ("is_parsed IS NULL OR is_parsed = false", "unparsed_log_line"),
      ("session_id IS NULL OR TRIM(session_id) = ''", "missing_session_id"),
      ("event_ts IS NULL", "missing_event_ts"),
      ("event_type IS NULL OR TRIM(event_type) = ''", "missing_event_type"),
      ("user_id IS NULL OR TRIM(user_id) = ''", "missing_user_id")
    ]) }} AS reject_reason
  FROM bronze_src
)

SELECT
  user_id,
  session_id,
  order_id,
  product_id,
  outcome,
  event_type,
  amount,
  event_ts,
  raw_line,
  is_parsed,
  batch_id,
  source_file,
  ingested_at,
  reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

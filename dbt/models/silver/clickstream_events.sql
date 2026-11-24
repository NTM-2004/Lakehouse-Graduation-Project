{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  silver.clickstream_events - Bảng sự kiện Clickstream đã làm sạch tầng Silver (B-320)
  Tác giả phụ trách: Minh | Reviewer: Hoàng
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2: Mục IV (silver.clickstream_events), Bảng tra cờ (E1, E2, T1)
  - dbt/schema_contract.yml: silver_tables.clickstream_events (13 cột)
  - Requirement v3: F08, F09, F17

  Quy tắc nghiệp vụ:
  1. Đọc bronze.clickstream_events, loại các dòng lỗi mức 1 (đã chuyển vào silver.clickstream_rejects).
  2. Gộp các dòng giống hệt từng chữ (theo raw_line) thành một sự kiện (F09).
     Không gộp theo event_id để hai dòng cùng event_id nhưng khác nội dung bị bắt ở kiểm thử unique (error).
  3. Bỏ cột raw_line ở đầu ra.
  4. Sinh khóa thay thế:
     event_id = session_id || '|' || TO_CHAR(event_ts, 'YYYY-MM-DD HH24:MI:SS') || '|' || event_type || '|' || COALESCE(NULLIF(TRIM(product_id), ''), 'none')
  5. Ép kiểu amount sang DECIMAL(10, 2) bằng macro safe_cast; gắn cờ T1:amount nếu ép thất bại.
  6. Gom phiên trên toàn bảng (không theo batch) để gắn cờ:
     - E1 (EVENT_AFTER_PURCHASE): event_ts > event_ts của sự kiện purchase cùng phiên
     - E2 (SAME_SECOND): cùng session_id và cùng event_ts với một sự kiện khác
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
    *,
    {{ build_reject_reason([
      ("is_parsed IS NULL OR is_parsed = false", "unparsed_log_line"),
      ("session_id IS NULL OR TRIM(session_id) = ''", "missing_session_id"),
      ("event_ts IS NULL", "missing_event_ts"),
      ("event_type IS NULL OR TRIM(event_type) = ''", "missing_event_type"),
      ("user_id IS NULL OR TRIM(user_id) = ''", "missing_user_id")
    ]) }} AS reject_reason
  FROM bronze_src
),

valid_rows AS (
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
    batch_id,
    source_file,
    ingested_at
  FROM evaluated
  WHERE NOT {{ is_reject('reject_reason') }}
),

-- F09: Hai dòng log giống hệt từng chữ (raw_line) gộp thành một sự kiện
dedup_exact AS (
  SELECT
    user_id,
    session_id,
    order_id,
    product_id,
    outcome,
    event_type,
    amount,
    event_ts,
    batch_id,
    source_file,
    ROW_NUMBER() OVER (
      PARTITION BY raw_line
      ORDER BY batch_id, source_file, ingested_at
    ) AS exact_rn
  FROM valid_rows
),

unique_events AS (
  SELECT
    TRIM(user_id) AS user_id,
    TRIM(session_id) AS session_id,
    NULLIF(TRIM(order_id), '') AS order_id,
    NULLIF(TRIM(product_id), '') AS product_id,
    NULLIF(TRIM(outcome), '') AS outcome,
    TRIM(event_type) AS event_type,
    amount AS amount_raw,
    {{ safe_cast('amount', 'DECIMAL(10, 2)') }} AS amount_casted,
    event_ts,
    source_file,
    batch_id
  FROM dedup_exact
  WHERE exact_rn = 1
),

-- Gom phiên trên toàn bảng (không theo batch) để đánh giá cờ E1, E2
session_metrics AS (
  SELECT
    user_id,
    session_id,
    order_id,
    product_id,
    outcome,
    event_type,
    amount_raw,
    amount_casted,
    event_ts,
    source_file,
    batch_id,
    MIN(CASE WHEN event_type = 'purchase' THEN event_ts END) OVER (
      PARTITION BY session_id
    ) AS purchase_event_ts,
    COUNT(*) OVER (
      PARTITION BY session_id, event_ts
    ) AS same_ts_cnt
  FROM unique_events
)

SELECT
  CAST(
    session_id
    || '|' || TO_CHAR(event_ts, 'YYYY-MM-DD HH24:MI:SS')
    || '|' || event_type
    || '|' || COALESCE(product_id, 'none')
    AS VARCHAR
  ) AS event_id,
  CAST(session_id AS VARCHAR) AS session_id,
  CAST(user_id AS VARCHAR) AS user_id,
  event_ts,
  CAST(event_ts AS DATE) AS event_date,
  CAST(event_type AS VARCHAR) AS event_type,
  CAST(product_id AS VARCHAR) AS product_id,
  CAST(order_id AS VARCHAR) AS order_id,
  amount_casted AS amount,
  CAST(outcome AS VARCHAR) AS outcome,
  {{ build_dq_flag([
    ('E1', "purchase_event_ts IS NOT NULL AND event_ts > purchase_event_ts"),
    ('E2', "same_ts_cnt > 1"),
    ('T1:amount', "amount_raw IS NOT NULL AND amount_casted IS NULL")
  ]) }} AS dq_flag,
  CAST(source_file AS VARCHAR) AS source_file,
  CAST(batch_id AS VARCHAR) AS batch_id
FROM session_metrics

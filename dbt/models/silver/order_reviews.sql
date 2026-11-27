{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/order_reviews.sql - Mô hình Silver cho bảng đánh giá đơn hàng Olist (B-315)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu:
  - Design Bronze, Silver v2 (Mục I: Loại trùng, Cột bắt buộc mức 1, Bảng tra cờ R2 & T1; Mục IV: silver.order_reviews)
  - dbt/schema_contract.yml (silver_tables.order_reviews - 10 cột)

  Luồng xử lý:
  1. Đọc bronze_olist.order_reviews, tách lỗi mức 1 (thiếu review_id hoặc order_id)
     TRƯỚC KHI gọi latest_snapshot (theo quy tắc của latest_snapshot.sql).
  2. Loại trùng theo khóa ghép (review_id, order_id) bằng macro latest_snapshot (giữ bản chụp batch_seq lớn nhất).
  3. Ép kiểu an toàn (safe_cast):
     - review_score -> INTEGER (Dremio 25.0 không có kiểu TINYINT; NULL khi chưa trả lời)
     - review_creation_date -> ép TIMESTAMP rồi CAST sang DATE để bỏ phần giờ (gồm 85 dòng 01:00:00)
     - review_answer_timestamp -> TIMESTAMP (NULL khi chưa trả lời)
  4. Nối LEFT JOIN silver.orders để gắn cờ R2 (REVIEW_BEFORE_PURCHASE):
     review_creation_date < CAST(order_purchase_timestamp AS DATE), cùng các cờ T1:<cột>.
#}

WITH bronze_src AS (
    SELECT
        NULLIF(TRIM(review_id), '') AS clean_review_id,
        NULLIF(TRIM(order_id), '') AS clean_order_id,
        NULLIF(TRIM(review_score), '') AS raw_review_score,
        NULLIF(TRIM(review_comment_title), '') AS clean_review_comment_title,
        NULLIF(TRIM(review_comment_message), '') AS clean_review_comment_message,
        NULLIF(TRIM(review_creation_date), '') AS raw_review_creation_date,
        NULLIF(TRIM(review_answer_timestamp), '') AS raw_review_answer_timestamp,
        NULLIF(TRIM(source_file), '') AS source_file,
        NULLIF(TRIM(batch_id), '') AS batch_id
    FROM {{ source('bronze_olist', 'order_reviews') }}
),

evaluated AS (
    SELECT
        *,
        {{ build_reject_reason([
            ("clean_review_id IS NULL", "MISSING_REVIEW_ID"),
            ("clean_order_id IS NULL", "MISSING_ORDER_ID")
        ]) }} AS reject_reason
    FROM bronze_src
),

valid_rows AS (
    SELECT
        clean_review_id AS review_id,
        clean_order_id AS order_id,
        raw_review_score,
        clean_review_comment_title AS review_comment_title,
        clean_review_comment_message AS review_comment_message,
        raw_review_creation_date,
        raw_review_answer_timestamp,
        source_file,
        batch_id
    FROM evaluated
    WHERE NOT {{ is_reject('reject_reason') }}
),

deduped AS (
    {{ latest_snapshot('valid_rows', ['review_id', 'order_id']) }}
),

typed AS (
    SELECT
        d.review_id,
        d.order_id,
        d.raw_review_score,
        {{ safe_cast('d.raw_review_score', 'INTEGER') }} AS review_score,
        d.review_comment_title,
        d.review_comment_message,
        d.raw_review_creation_date,
        CAST({{ safe_cast('d.raw_review_creation_date', 'TIMESTAMP') }} AS DATE) AS review_creation_date,
        d.raw_review_answer_timestamp,
        {{ safe_cast('d.raw_review_answer_timestamp', 'TIMESTAMP') }} AS review_answer_timestamp,
        o.order_purchase_timestamp,
        d.source_file,
        d.batch_id
    FROM deduped d
    LEFT JOIN {{ ref('orders') }} o
        ON d.order_id = o.order_id
)

SELECT
    CAST(review_id AS VARCHAR) AS review_id,
    CAST(order_id AS VARCHAR) AS order_id,
    review_score,
    CAST(review_comment_title AS VARCHAR) AS review_comment_title,
    CAST(review_comment_message AS VARCHAR) AS review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    {{ build_dq_flag([
        ("R2", "order_purchase_timestamp IS NOT NULL AND review_creation_date IS NOT NULL AND review_creation_date < CAST(order_purchase_timestamp AS DATE)"),
        ("T1:review_score", "raw_review_score IS NOT NULL AND review_score IS NULL"),
        ("T1:review_creation_date", "raw_review_creation_date IS NOT NULL AND review_creation_date IS NULL"),
        ("T1:review_answer_timestamp", "raw_review_answer_timestamp IS NOT NULL AND review_answer_timestamp IS NULL")
    ]) }} AS dq_flag,
    CAST(source_file AS VARCHAR) AS source_file,
    CAST(batch_id AS VARCHAR) AS batch_id
FROM typed

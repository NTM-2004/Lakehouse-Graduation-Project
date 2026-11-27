{{
  config(
    materialized='table',
    schema='silver'
  )
}}

{#
  dbt/models/silver/order_reviews_rejects.sql - Bảng reject mức 1 cho silver.order_reviews (B-315)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-03
  Tham chiếu: Design Bronze, Silver v2 (Cột bắt buộc mức 1 và bảng reject; silver.order_reviews)

  Quy tắc mức 1 (đối lập hoàn toàn với điều kiện lọc trước deduplicate của silver.order_reviews):
  - Dòng thiếu review_id hoặc thiếu order_id chuyển vào silver.order_reviews_rejects.
  - Giữ toàn bộ các cột Bronze của dòng và bổ sung cột reject_reason ở cuối.
#}

WITH bronze_src AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        batch_id,
        source_file,
        ingested_at,
        NULLIF(TRIM(review_id), '') AS clean_review_id,
        NULLIF(TRIM(order_id), '') AS clean_order_id
    FROM {{ source('bronze_olist', 'order_reviews') }}
),

evaluated AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        batch_id,
        source_file,
        ingested_at,
        {{ build_reject_reason([
            ("clean_review_id IS NULL", "MISSING_REVIEW_ID"),
            ("clean_order_id IS NULL", "MISSING_ORDER_ID")
        ]) }} AS reject_reason
    FROM bronze_src
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    batch_id,
    source_file,
    ingested_at,
    reject_reason
FROM evaluated
WHERE {{ is_reject('reject_reason') }}

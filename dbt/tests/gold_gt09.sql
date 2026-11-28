{#
  dbt/tests/gold_gt09.sql - GT-09: Chọn đánh giá cho gold.fact_order (B-425)
  Tác giả phụ trách: Kiên | Reviewer: Minh
  Mẫu triển khai: M-07
  Mức: error (Design Gold v2 - Kiểm thử Gold GT-09; Requirement v3 - Đánh giá)

  Kiểm tra:
  1. Mỗi đơn tối đa một dòng / một review_score trong gold.fact_order.
  2. Đơn có review_score IS NOT NULL thì phải có đánh giá đã trả lời, có điểm và không mang cờ R2 ở silver.order_reviews.
  3. Đơn có ít nhất một đánh giá đủ điều kiện ở silver.order_reviews thì review_score ở gold.fact_order không được NULL.
  4. Điểm review_score khớp đánh giá được chọn theo thứ tự giảm dần (review_answer_timestamp, review_creation_date, review_id).
#}

{{ config(severity='error') }}

WITH eligible_ranked AS (
    SELECT
        order_id,
        review_score,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                review_answer_timestamp DESC NULLS LAST,
                review_creation_date DESC NULLS LAST,
                review_id DESC NULLS LAST
        ) AS rn
    FROM {{ ref('order_reviews') }}
    WHERE review_answer_timestamp IS NOT NULL
      AND review_score IS NOT NULL
      AND NOT {{ has_flag('dq_flag', 'R2') }}
),

expected_review AS (
    SELECT
        order_id,
        CAST(review_score AS INTEGER) AS expected_score
    FROM eligible_ranked
    WHERE rn = 1
)

-- 1. Mỗi đơn tối đa một dòng trong fact_order
SELECT
    'DUPLICATE_ORDER_IN_FACT' AS issue,
    order_id,
    CAST(COUNT(*) AS VARCHAR) AS detail
FROM {{ ref('fact_order') }}
GROUP BY order_id
HAVING COUNT(*) > 1

UNION ALL

-- 2. Đơn có review_score nhưng không có đánh giá đủ điều kiện (đã trả lời, không R2)
SELECT
    'UNEXPECTED_REVIEW_SCORE' AS issue,
    fo.order_id,
    'actual=' || CAST(fo.review_score AS VARCHAR) AS detail
FROM {{ ref('fact_order') }} fo
LEFT JOIN expected_review er
    ON fo.order_id = er.order_id
WHERE fo.review_score IS NOT NULL
  AND er.order_id IS NULL

UNION ALL

-- 3. Đơn có đánh giá đủ điều kiện nhưng fact_order.review_score lại NULL
SELECT
    'MISSING_REVIEW_SCORE' AS issue,
    fo.order_id,
    'expected=' || CAST(er.expected_score AS VARCHAR) AS detail
FROM {{ ref('fact_order') }} fo
INNER JOIN expected_review er
    ON fo.order_id = er.order_id
WHERE fo.review_score IS NULL

UNION ALL

-- 4. Giá trị review_score lệch đánh giá đứng đầu theo quy tắc hòa
SELECT
    'MISMATCHED_REVIEW_SCORE' AS issue,
    fo.order_id,
    'actual=' || CAST(fo.review_score AS VARCHAR) || ',expected=' || CAST(er.expected_score AS VARCHAR) AS detail
FROM {{ ref('fact_order') }} fo
INNER JOIN expected_review er
    ON fo.order_id = er.order_id
WHERE fo.review_score IS NOT NULL
  AND fo.review_score != er.expected_score

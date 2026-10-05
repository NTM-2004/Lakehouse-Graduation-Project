#!/usr/bin/env python3
"""
split_olist_batches.py

Chia bộ dữ liệu Brazilian E-Commerce Public Dataset (Olist) thành các batch
bulk/incremental theo ĐÚNG ranh giới thời gian đã dùng trong
generate_clickstream.py, để hai nguồn dữ liệu (Olist + clickstream) khớp
batch với nhau khi nạp vào pipeline:

    bulk_2016-09_2018-05   : 09/2016 -> 05/2018  (lịch sử)
    2018-06, 2018-07, 2018-08 : 3 batch incremental

Nguyên tắc chia:

  - Bảng FACT (gắn với 1 đơn hàng, có order_purchase_timestamp):
        olist_orders_dataset.csv
        olist_order_items_dataset.csv
        olist_order_payments_dataset.csv
        olist_order_reviews_dataset.csv
    -> Lọc theo order_purchase_timestamp: mỗi batch chỉ chứa đúng các
       order_id (và các dòng liên quan) phát sinh trong khoảng thời gian
       của batch đó. Bốn bảng này có mặt ở CẢ bulk lẫn incremental.

  - Bảng DIMENSION / REFERENCE (không có timestamp riêng, là bản snapshot
    tĩnh, đầy đủ ngay từ đầu — không bị "giữ lại" theo thời gian):
        olist_customers_dataset.csv
        olist_products_dataset.csv
        olist_sellers_dataset.csv
        olist_geolocation_dataset.csv
        product_category_name_translation.csv
    -> Đẩy TOÀN BỘ, KHÔNG lọc, MỘT LẦN DUY NHẤT ở batch bulk.
       Batch incremental (2018-06/07/08) KHÔNG chứa các bảng này nữa,
       vì file gốc Olist là 1 bản export tĩnh, đầy đủ ngay từ đầu:
       mọi customer_id/product_id/seller_id xuất hiện ở bất kỳ order nào
       (kể cả order tháng 6-8/2018) đều đã có sẵn trong các file dimension
       này rồi -> không có rủi ro "orphan foreign key" khi downstream
       join incremental fact với dimension đã nạp ở bulk.

Output: mỗi batch là 1 thư mục con trong --output-root, chứa các file CSV
cùng tên gốc (chỉ những bảng thuộc về batch đó) + 1 file _SUCCESS đánh dấu
ghi xong (đúng convention đã dùng ở generate_clickstream.py).

Cách dùng:

    # chia 1 batch cụ thể
    python split_olist_batches.py \
        --olist-dir /path/to/olist_csv \
        --batch 2018-06 \
        --output-root raw/olist

    # chia cả 4 batch trong 1 lần chạy
    python split_olist_batches.py \
        --olist-dir /path/to/olist_csv \
        --all \
        --output-root raw/olist
"""

import argparse
import os

import pandas as pd

# ----------------------------------------------------------------------
# Tên file input gốc (đúng tên file Olist thật)
# ----------------------------------------------------------------------
FILE_ORDERS = "olist_orders_dataset.csv"
FILE_ITEMS = "olist_order_items_dataset.csv"
FILE_PAYMENTS = "olist_order_payments_dataset.csv"
FILE_REVIEWS = "olist_order_reviews_dataset.csv"

FACT_FILES = [FILE_ORDERS, FILE_ITEMS, FILE_PAYMENTS, FILE_REVIEWS]

FILE_CUSTOMERS = "olist_customers_dataset.csv"
FILE_PRODUCTS = "olist_products_dataset.csv"
FILE_SELLERS = "olist_sellers_dataset.csv"
FILE_GEOLOCATION = "olist_geolocation_dataset.csv"
FILE_CATEGORY_TRANSLATION = "product_category_name_translation.csv"

DIMENSION_FILES = [
    FILE_CUSTOMERS,
    FILE_PRODUCTS,
    FILE_SELLERS,
    FILE_GEOLOCATION,
    FILE_CATEGORY_TRANSLATION,
]

BULK_BATCH_NAME = "bulk_2016-09_2018-05"

# ----------------------------------------------------------------------
# Ranh giới batch — PHẢI khớp với BATCH_BOUNDARIES trong
# generate_clickstream.py để 2 nguồn dữ liệu join đúng ở downstream.
# ----------------------------------------------------------------------
BATCH_BOUNDARIES = {
    BULK_BATCH_NAME: (None, "2018-05-31 23:59:59"),
    "2018-06": ("2018-06-01 00:00:00", "2018-06-30 23:59:59"),
    "2018-07": ("2018-07-01 00:00:00", "2018-07-31 23:59:59"),
    "2018-08": ("2018-08-01 00:00:00", "2018-08-31 23:59:59"),
}


def load_fact_tables(olist_dir):
    """Đọc 4 bảng fact (đây là những bảng cần lọc theo thời gian)."""
    return {
        FILE_ORDERS: pd.read_csv(
            os.path.join(olist_dir, FILE_ORDERS),
            parse_dates=["order_purchase_timestamp"],
        ),
        FILE_ITEMS: pd.read_csv(os.path.join(olist_dir, FILE_ITEMS)),
        FILE_PAYMENTS: pd.read_csv(os.path.join(olist_dir, FILE_PAYMENTS)),
        FILE_REVIEWS: pd.read_csv(os.path.join(olist_dir, FILE_REVIEWS)),
    }


def load_dimension_tables(olist_dir):
    """Đọc 5 bảng dimension/reference (chỉ cần đẩy nguyên vẹn ở bulk)."""
    return {fname: pd.read_csv(os.path.join(olist_dir, fname)) for fname in DIMENSION_FILES}


def filter_fact_tables(fact_tables, start, end):
    """Lọc 4 bảng fact theo order_id thuộc đúng khoảng [start, end]."""
    orders = fact_tables[FILE_ORDERS]
    ts = orders["order_purchase_timestamp"]

    mask = pd.Series(True, index=orders.index)
    if start is not None:
        mask &= ts >= pd.Timestamp(start)
    if end is not None:
        mask &= ts <= pd.Timestamp(end)

    orders_b = orders[mask].reset_index(drop=True)
    order_ids_b = set(orders_b["order_id"])

    items_b = fact_tables[FILE_ITEMS][fact_tables[FILE_ITEMS]["order_id"].isin(order_ids_b)].reset_index(drop=True)
    payments_b = fact_tables[FILE_PAYMENTS][fact_tables[FILE_PAYMENTS]["order_id"].isin(order_ids_b)].reset_index(drop=True)
    reviews_b = fact_tables[FILE_REVIEWS][fact_tables[FILE_REVIEWS]["order_id"].isin(order_ids_b)].reset_index(drop=True)

    return {
        FILE_ORDERS: orders_b,
        FILE_ITEMS: items_b,
        FILE_PAYMENTS: payments_b,
        FILE_REVIEWS: reviews_b,
    }


def write_tables(tables, output_root, batch_name):
    out_dir = os.path.join(output_root, batch_name)
    os.makedirs(out_dir, exist_ok=True)

    print(f"[+] Batch '{batch_name}':")
    for fname, df in tables.items():
        out_path = os.path.join(out_dir, fname)
        df.to_csv(out_path, index=False)
        print(f"    -> {fname}: {len(df)} dòng")

    # file _SUCCESS rỗng đánh dấu batch đã ghi xong (đúng convention Bước 2)
    open(os.path.join(out_dir, "_SUCCESS"), "w").close()
    print(f"    -> {out_dir}/_SUCCESS")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--olist-dir", required=True, help="Thư mục chứa các file olist_*.csv gốc")
    parser.add_argument("--output-root", default="raw/olist", help="Thư mục gốc để ghi output")
    parser.add_argument(
        "--batch",
        choices=list(BATCH_BOUNDARIES.keys()),
        help="Chỉ chia 1 batch cụ thể (bỏ qua nếu dùng --all)",
    )
    parser.add_argument("--all", action="store_true", help="Chia cả 4 batch trong 1 lần chạy")
    args = parser.parse_args()

    if not args.batch and not args.all:
        parser.error("Phải chọn --batch <tên_batch> hoặc --all")

    fact_tables = load_fact_tables(args.olist_dir)

    batches = list(BATCH_BOUNDARIES.keys()) if args.all else [args.batch]

    for batch_name in batches:
        start, end = BATCH_BOUNDARIES[batch_name]
        tables = filter_fact_tables(fact_tables, start, end)

        if batch_name == BULK_BATCH_NAME:
            # Bulk batch: 4 bảng fact (đã lọc theo thời gian) + TOÀN BỘ
            # 5 bảng dimension/reference, không lọc, đẩy 1 lần duy nhất.
            dimension_tables = load_dimension_tables(args.olist_dir)
            tables.update(dimension_tables)
        # Batch incremental: chỉ 4 bảng fact, không kèm dimension nữa.

        write_tables(tables, args.output_root, batch_name)


if __name__ == "__main__":
    main()

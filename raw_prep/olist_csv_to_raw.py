"""
raw_prep/olist_csv_to_raw.py - Chia 9 tệp Olist CSV vào 4 batch tầng Raw (B-104)
Tác giả phụ trách: Kiên
Reviewer: Minh (theo phân công Design Build)
Mẫu triển khai: M-08
Tham chiếu:
- Requirement v3 (F01)
- Data Analyze v2 (mục 2.a, 2.b, 2.c, mục 5: Lưu ý khi đọc dữ liệu)
- Design Bronze, Silver v2 (Mục III: Nguồn csv)
- Design Build (B-104, M-08)

Mục đích:
- Đọc 9 tệp CSV nguồn Olist và chia vào 4 batch theo thời điểm sự kiện:
  + bulk_2016-09_2018-05 (<= 2018-05-31 23:59:59)
  + 2018-06 (2018-06-01 -> 2018-06-30 23:59:59)
  + 2018-07 (2018-07-01 -> 2018-07-31 23:59:59)
  + 2018-08 (2018-08-01 -> 2018-08-31 23:59:59)
- Cắt mốc ngày chốt 2018-08-31 23:59:59:
  + Loại hẳn 20 đơn mua sau ngày chốt cùng 1 dòng hàng, 20 thanh toán, 19 đánh giá liên quan.
  + Để trống các mốc xảy ra sau mốc kết thúc batch (kể cả 59 ngày giao khách / 2 ngày giao hãng
    và 666 ngày trả lời đánh giá sau ngày chốt — AN-14); giữ nguyên mốc kế hoạch
    (order_estimated_delivery_date, shipping_limit_date).
  + Tạo bản chụp ở các batch có sự kiện mới cho orders và order_reviews theo quy tắc Data Analyze v2.
  + Giữ nguyên định dạng CSV cho 5 bảng tĩnh ở batch bulk (giữ nguyên BOM ở
    product_category_name_translation.csv để B-202 xử lý khi nạp Bronze).
- Ghi tệp tín hiệu _SUCCESS kèm mã toàn vẹn SHA-256 của từng batch qua raw_prep/raw_common.py (B-101).
- Hỗ trợ cờ --verify đối soát số dòng, distinct_keys, mã toàn vẹn SHA-256 và quy tắc ngày chốt với B-108 (F01).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import sys
from typing import Dict, List, Optional, Set, Tuple

# Thêm thư mục gốc vào sys.path để import raw_common (B-101)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from raw_prep.raw_common import (
    BATCH_2018_06,
    BATCH_2018_07,
    BATCH_2018_08,
    BATCH_BULK,
    BATCH_NAMES,
    CUTOFF_TIMESTAMP,
    calculate_sha256,
    get_batch_id_by_event_timestamp,
    load_batch_row_counts,
    verify_row_counts,
    write_success_marker,
)

# Mốc thời điểm kết thúc của từng batch để cắt bản chụp (Data Analyze v2, mục 2.b)
BATCH_END_TS: Dict[str, str] = {
    BATCH_BULK: "2018-05-31 23:59:59",
    BATCH_2018_06: "2018-06-30 23:59:59",
    BATCH_2018_07: "2018-07-31 23:59:59",
    BATCH_2018_08: CUTOFF_TIMESTAMP[:19],
}
BATCH_INDEX: Dict[str, int] = {b: idx for idx, b in enumerate(BATCH_NAMES)}

# Danh sách 9 bảng Olist CSV: (table_key theo B-108, tên tệp nguồn, là bảng tĩnh chỉ ở bulk)
OLIST_TABLES: List[Tuple[str, str, bool]] = [
    ("orders", "olist_orders_dataset.csv", False),
    ("order_items", "olist_order_items_dataset.csv", False),
    ("order_payments", "olist_order_payments_dataset.csv", False),
    ("order_reviews", "olist_order_reviews_dataset.csv", False),
    ("customers", "olist_customers_dataset.csv", True),
    ("products", "olist_products_dataset.csv", True),
    ("sellers", "olist_sellers_dataset.csv", True),
    ("product_category_translation", "product_category_name_translation.csv", True),
    ("geolocation", "olist_geolocation_dataset.csv", True),
]

ORDER_EVENT_COLS = (
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
)

# Cột khóa để đối soát distinct_keys với B-108 (geolocation không có khóa -> đối soát số dòng)
KEY_COLS_BY_TABLE: Dict[str, Optional[Tuple[str, ...]]] = {
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
    "customers": ("customer_id",),
    "products": ("product_id",),
    "sellers": ("seller_id",),
    "product_category_translation": ("product_category_name",),
    "geolocation": None,
}


def _compute_batch_checksum(batch_dir: str) -> Tuple[str, Dict[str, str]]:
    """
    Tính mã SHA-256 của từng tệp CSV trong thư mục batch (theo thứ tự tên tệp)
    và ghép thành mã SHA-256 tổng hợp đại diện cho toàn bộ batch.
    """
    csv_files = sorted(
        fn for fn in os.listdir(batch_dir)
        if fn.endswith(".csv") and os.path.isfile(os.path.join(batch_dir, fn))
    )
    file_hashes: Dict[str, str] = {}
    manifest_lines: List[str] = []
    for fn in csv_files:
        sha = calculate_sha256(os.path.join(batch_dir, fn))
        file_hashes[fn] = sha
        manifest_lines.append(f"{fn}:{sha}")

    batch_sha = hashlib.sha256("\n".join(manifest_lines).encode("utf-8")).hexdigest()
    return batch_sha, file_hashes


def _read_success_checksum(success_path: str) -> Optional[str]:
    """Đọc giá trị SHA256=<hex> từ tệp _SUCCESS."""
    if not os.path.isfile(success_path):
        return None
    with open(success_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith("SHA256="):
                return stripped.split("=", 1)[1].strip()
    return None


def _build_order_snapshot(row: Dict[str, str], batch_id: str) -> Dict[str, str]:
    """
    Dựng bản chụp một đơn hàng tại thời điểm cuối batch_id theo Data Analyze v2 (R82-R83):
    - Các mốc (duyệt, giao hãng, giao khách) xảy ra sau cuối batch_id để trống.
    - Mốc kế hoạch (order_estimated_delivery_date) giữ nguyên.
    - Trạng thái gốc chỉ giữ khi mọi mốc đã biết của đơn đều <= cuối batch_id;
      ngược lại suy từ các mốc đã xảy ra (delivered -> shipped -> approved -> created).
    """
    end_ts = BATCH_END_TS[batch_id]
    snap = dict(row)

    all_known_occurred = True
    for col in ORDER_EVENT_COLS:
        val = (row.get(col) or "").strip()
        if val and val[:19] > end_ts:
            snap[col] = ""
            all_known_occurred = False

    if not all_known_occurred:
        if (snap.get("order_delivered_customer_date") or "").strip():
            snap["order_status"] = "delivered"
        elif (snap.get("order_delivered_carrier_date") or "").strip():
            snap["order_status"] = "shipped"
        elif (snap.get("order_approved_at") or "").strip():
            snap["order_status"] = "approved"
        else:
            snap["order_status"] = "created"

    return snap


def _build_review_snapshot(row: Dict[str, str], batch_id: str) -> Dict[str, str]:
    """
    Dựng bản chụp một đánh giá tại thời điểm cuối batch_id theo Data Analyze v2 (R84):
    - Nếu khách chưa trả lời tính đến cuối batch_id (review_answer_timestamp trống hoặc > cuối batch),
      để trống ngày trả lời, điểm và cả tiêu đề/nội dung bình luận.
    - Khi khách đã trả lời tính đến cuối batch_id, giữ đầy đủ giá trị gốc.
    """
    end_ts = BATCH_END_TS[batch_id]
    snap = dict(row)
    ans = (row.get("review_answer_timestamp") or "").strip()
    if not ans or ans[:19] > end_ts:
        snap["review_score"] = ""
        snap["review_comment_title"] = ""
        snap["review_comment_message"] = ""
        snap["review_answer_timestamp"] = ""
    return snap


def _split_orders(
    input_dir: str,
    staging_dir: str,
) -> Tuple[Dict[str, str], Set[str], Dict[str, int], int]:
    """
    Chia bảng orders vào 4 batch theo thời điểm mua và các mốc cập nhật (duyệt, giao hãng, giao khách).
    Trả về (order_purchase_batch, excluded_orders, batch_counts, excluded_count).
    """
    src_path = os.path.join(input_dir, "olist_orders_dataset.csv")
    if not os.path.isfile(src_path):
        raise FileNotFoundError(f"Không tìm thấy tệp CSV nguồn: {src_path}")

    order_purchase_batch: Dict[str, str] = {}
    excluded_orders: Set[str] = set()
    counts: Dict[str, int] = {b: 0 for b in BATCH_NAMES}

    with open(src_path, "r", encoding="utf-8-sig", newline="") as sf:
        reader = csv.DictReader(sf)
        fieldnames = list(reader.fieldnames or [])

        writers: Dict[str, csv.DictWriter] = {}
        handles = []
        try:
            for b in BATCH_NAMES:
                out_path = os.path.join(staging_dir, b, "olist_orders_dataset.csv")
                fh = open(out_path, "w", encoding="utf-8", newline="")
                handles.append(fh)
                w = csv.DictWriter(
                    fh,
                    fieldnames=fieldnames,
                    lineterminator="\n",
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                )
                w.writeheader()
                writers[b] = w

            for row in reader:
                order_id = (row.get("order_id") or "").strip()
                pb = get_batch_id_by_event_timestamp(row.get("order_purchase_timestamp", ""))
                if pb is None:
                    excluded_orders.add(order_id)
                    continue

                order_purchase_batch[order_id] = pb
                p_idx = BATCH_INDEX[pb]
                target_batches: Set[str] = {pb}

                for col in ORDER_EVENT_COLS:
                    val = (row.get(col) or "").strip()
                    if not val:
                        continue
                    tb = get_batch_id_by_event_timestamp(val)
                    # Không xuất hiện trước batch đầu tiên (AN-02: mốc sớm hơn ngày mua gộp vào batch mua)
                    if tb is not None and BATCH_INDEX[tb] >= p_idx:
                        target_batches.add(tb)

                for b in sorted(target_batches, key=lambda x: BATCH_INDEX[x]):
                    writers[b].writerow(_build_order_snapshot(row, b))
                    counts[b] += 1
        finally:
            for fh in handles:
                fh.close()

    return order_purchase_batch, excluded_orders, counts, len(excluded_orders)


def _split_order_child_table(
    input_dir: str,
    staging_dir: str,
    filename: str,
    order_purchase_batch: Dict[str, str],
    excluded_orders: Set[str],
) -> Tuple[Dict[str, int], int]:
    """Chia order_items hoặc order_payments theo batch ngày mua của đơn cha (không ghi lại ở batch sau)."""
    src_path = os.path.join(input_dir, filename)
    if not os.path.isfile(src_path):
        raise FileNotFoundError(f"Không tìm thấy tệp CSV nguồn: {src_path}")

    counts: Dict[str, int] = {b: 0 for b in BATCH_NAMES}
    excluded_count = 0

    with open(src_path, "r", encoding="utf-8-sig", newline="") as sf:
        reader = csv.DictReader(sf)
        fieldnames = list(reader.fieldnames or [])

        writers: Dict[str, csv.DictWriter] = {}
        handles = []
        try:
            for b in BATCH_NAMES:
                out_path = os.path.join(staging_dir, b, filename)
                fh = open(out_path, "w", encoding="utf-8", newline="")
                handles.append(fh)
                w = csv.DictWriter(
                    fh,
                    fieldnames=fieldnames,
                    lineterminator="\n",
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                )
                w.writeheader()
                writers[b] = w

            for row in reader:
                order_id = (row.get("order_id") or "").strip()
                if order_id in excluded_orders:
                    excluded_count += 1
                    continue
                pb = order_purchase_batch.get(order_id)
                if pb is None:
                    raise KeyError(
                        f"Đơn cha order_id='{order_id}' trong {filename} không tồn tại trong olist_orders_dataset.csv"
                    )
                writers[pb].writerow(row)
                counts[pb] += 1
        finally:
            for fh in handles:
                fh.close()

    return counts, excluded_count


def _split_order_reviews(
    input_dir: str,
    staging_dir: str,
    excluded_orders: Set[str],
) -> Tuple[Dict[str, int], int]:
    """
    Chia order_reviews theo ngày tạo (batch đầu tiên) và ngày trả lời (ghi lại nếu khác batch tạo và <= ngày chốt).
    Loại các đánh giá thuộc 20 đơn bị loại hoặc tạo sau ngày chốt.
    """
    filename = "olist_order_reviews_dataset.csv"
    src_path = os.path.join(input_dir, filename)
    if not os.path.isfile(src_path):
        raise FileNotFoundError(f"Không tìm thấy tệp CSV nguồn: {src_path}")

    counts: Dict[str, int] = {b: 0 for b in BATCH_NAMES}
    excluded_count = 0

    with open(src_path, "r", encoding="utf-8-sig", newline="") as sf:
        reader = csv.DictReader(sf)
        fieldnames = list(reader.fieldnames or [])

        writers: Dict[str, csv.DictWriter] = {}
        handles = []
        try:
            for b in BATCH_NAMES:
                out_path = os.path.join(staging_dir, b, filename)
                fh = open(out_path, "w", encoding="utf-8", newline="")
                handles.append(fh)
                w = csv.DictWriter(
                    fh,
                    fieldnames=fieldnames,
                    lineterminator="\n",
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                )
                w.writeheader()
                writers[b] = w

            for row in reader:
                order_id = (row.get("order_id") or "").strip()
                if order_id in excluded_orders:
                    excluded_count += 1
                    continue

                cb = get_batch_id_by_event_timestamp(row.get("review_creation_date", ""))
                if cb is None:
                    excluded_count += 1
                    continue

                c_idx = BATCH_INDEX[cb]
                target_batches: Set[str] = {cb}

                ans = (row.get("review_answer_timestamp") or "").strip()
                if ans:
                    ab = get_batch_id_by_event_timestamp(ans)
                    if ab is not None and BATCH_INDEX[ab] >= c_idx:
                        target_batches.add(ab)

                for b in sorted(target_batches, key=lambda x: BATCH_INDEX[x]):
                    writers[b].writerow(_build_review_snapshot(row, b))
                    counts[b] += 1
        finally:
            for fh in handles:
                fh.close()

    return counts, excluded_count


def _copy_static_table(input_dir: str, staging_dir: str, filename: str) -> int:
    """
    Sao chép nguyên văn tệp CSV tĩnh vào batch bulk_2016-09_2018-05
    (giữ nguyên byte gốc, kể cả BOM của product_category_name_translation.csv).
    """
    src_path = os.path.join(input_dir, filename)
    if not os.path.isfile(src_path):
        raise FileNotFoundError(f"Không tìm thấy tệp CSV nguồn: {src_path}")

    dst_path = os.path.join(staging_dir, BATCH_BULK, filename)
    shutil.copy2(src_path, dst_path)

    with open(dst_path, "r", encoding="utf-8-sig", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def verify_existing_raw(output_dir: str) -> bool:
    """
    Đối soát toàn bộ dữ liệu Raw Olist CSV (9 bảng x 4 batch) với:
    1) raw_prep/batch_row_counts.csv (B-108): số dòng từng batch, tổng dòng và distinct_keys.
    2) Tiêu chí F01:
       - Đủ 4 tệp _SUCCESS và mã SHA-256 tổng hợp của từng batch khớp nội dung _SUCCESS.
       - Không có mốc thời gian nào (trừ mốc kế hoạch) vượt quá mốc kết thúc batch / ngày chốt 2018-08-31.
       - Ở order_reviews: review_score và review_answer_timestamp luôn cùng trống hoặc cùng có giá trị.
       - 5 bảng tĩnh không xuất hiện ở các batch tăng trưởng (2018-06, 2018-07, 2018-08).
    """
    print("=== ĐỐI SOÁT DỮ LIỆU RAW OLIST CSV (--verify) ===")
    expected_all = load_batch_row_counts()
    all_valid = True

    # 1. Kiểm tra _SUCCESS và mã toàn vẹn SHA-256 cho cả 4 batch
    for batch_id in BATCH_NAMES:
        batch_dir = os.path.join(output_dir, batch_id)
        success_path = os.path.join(batch_dir, "_SUCCESS")
        if not os.path.isdir(batch_dir):
            print(f"  [LỆCH] Thiếu thư mục batch: {batch_dir}")
            all_valid = False
            continue

        recorded_sha = _read_success_checksum(success_path)
        actual_sha, file_hashes = _compute_batch_checksum(batch_dir)
        if recorded_sha is None:
            print(f"  [LỆCH] Batch {batch_id}: thiếu tệp _SUCCESS hoặc thiếu dòng SHA256=...")
            all_valid = False
        elif recorded_sha != actual_sha:
            print(
                f"  [LỆCH] Batch {batch_id}: SHA-256 không khớp! "
                f"Ghi trong _SUCCESS={recorded_sha[:16]}..., thực tế={actual_sha[:16]}..."
            )
            all_valid = False
        else:
            print(
                f"  [OK] Batch {batch_id}: {len(file_hashes)} tệp CSV | "
                f"_SUCCESS SHA256={actual_sha[:16]}... khớp 100%"
            )

    # 2. Đếm dòng, distinct_keys, kiểm tra ngày chốt và tính nhất quán từng bảng
    post_cutoff_violations = 0
    batch_boundary_violations = 0
    review_pair_violations = 0
    unexpected_static_files: List[str] = []

    for table_key, filename, is_static in OLIST_TABLES:
        actual_counts: Dict[str, int] = {b: 0 for b in BATCH_NAMES}
        distinct_keys: Set[Tuple[str, ...]] = set()
        key_cols = KEY_COLS_BY_TABLE[table_key]

        for batch_id in BATCH_NAMES:
            csv_path = os.path.join(output_dir, batch_id, filename)
            if not os.path.isfile(csv_path):
                actual_counts[batch_id] = 0
                continue

            if is_static and batch_id != BATCH_BULK:
                unexpected_static_files.append(f"{batch_id}/{filename}")

            end_ts = BATCH_END_TS[batch_id]
            row_count = 0
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row_count += 1
                    if key_cols is not None:
                        distinct_keys.add(tuple((row.get(k) or "").strip() for k in key_cols))

                    if table_key == "orders":
                        for ts_col in ("order_purchase_timestamp",) + ORDER_EVENT_COLS:
                            val = (row.get(ts_col) or "").strip()[:19]
                            if not val:
                                continue
                            if val > CUTOFF_TIMESTAMP[:19]:
                                post_cutoff_violations += 1
                            if val > end_ts:
                                batch_boundary_violations += 1

                    elif table_key == "order_reviews":
                        for ts_col in ("review_creation_date", "review_answer_timestamp"):
                            val = (row.get(ts_col) or "").strip()[:19]
                            if not val:
                                continue
                            if val > CUTOFF_TIMESTAMP[:19]:
                                post_cutoff_violations += 1
                            if val > end_ts:
                                batch_boundary_violations += 1

                        has_score = bool((row.get("review_score") or "").strip())
                        has_answer = bool((row.get("review_answer_timestamp") or "").strip())
                        if has_score != has_answer:
                            review_pair_violations += 1

            actual_counts[batch_id] = row_count

        table_ok, messages = verify_row_counts(table_key, actual_counts)
        expected_distinct = expected_all[table_key]["distinct_keys"]
        actual_distinct = sum(actual_counts.values()) if key_cols is None else len(distinct_keys)
        distinct_ok = actual_distinct == expected_distinct

        if not (table_ok and distinct_ok):
            all_valid = False

        status_label = "OK" if (table_ok and distinct_ok) else "LỆCH"
        print(
            f"  [{status_label}] {table_key:28} | "
            f"bulk={actual_counts[BATCH_BULK]:>9,} | "
            f"06={actual_counts[BATCH_2018_06]:>6,} | "
            f"07={actual_counts[BATCH_2018_07]:>6,} | "
            f"08={actual_counts[BATCH_2018_08]:>6,} | "
            f"tổng={sum(actual_counts.values()):>9,} | "
            f"khóa={actual_distinct:>9,}/{expected_distinct:,}"
        )
        if not table_ok:
            for msg in messages:
                if "Lệch" in msg:
                    print(f"       -> {msg}")
        if not distinct_ok:
            print(
                f"       -> Lệch distinct_keys: thực tế={actual_distinct:,}, chuẩn={expected_distinct:,}"
            )

    extra_checks = [
        (
            "F01: Không có mốc thời gian nào (trừ mốc kế hoạch) sau ngày chốt 2018-08-31",
            post_cutoff_violations == 0,
            f"vi phạm={post_cutoff_violations}",
        ),
        (
            "F01: Không có mốc sự kiện vượt quá thời điểm kết thúc của từng batch",
            batch_boundary_violations == 0,
            f"vi phạm={batch_boundary_violations}",
        ),
        (
            "order_reviews: review_score và review_answer_timestamp cùng trống hoặc cùng có giá trị",
            review_pair_violations == 0,
            f"vi phạm={review_pair_violations}",
        ),
        (
            "5 bảng tĩnh chỉ tồn tại ở batch bulk_2016-09_2018-05",
            len(unexpected_static_files) == 0,
            f"thừa={unexpected_static_files}" if unexpected_static_files else "hợp lệ",
        ),
    ]
    for label, ok, detail in extra_checks:
        if not ok:
            all_valid = False
        print(f"  [{'OK' if ok else 'LỆCH'}] {label} ({detail})")

    if all_valid:
        print("[THÀNH CÔNG] Toàn bộ 9 bảng Raw Olist CSV khớp 100% với B-108 và tiêu chí F01.")
    else:
        print("[THẤT BÀI] Dữ liệu Raw Olist CSV có sai lệch so với B-108 hoặc tiêu chí F01.")
    return all_valid


def process_olist_csv(
    input_dir: str,
    output_dir: str,
    verify: bool = True,
    clean: bool = True,
) -> bool:
    """
    Đọc 9 tệp CSV nguồn Olist từ input_dir, chia vào 4 batch trong output_dir,
    tạo tệp tín hiệu _SUCCESS kèm SHA-256 và đối soát với B-108.
    """
    print("=== BẮT ĐẦU CHIA 9 TỆP OLIST CSV VÀO 4 BATCH ===")
    print(f"Nguồn dữ liệu: {input_dir}")
    print(f"Đích lưu trữ:  {output_dir}")

    temp_out_dir = os.path.join(PROJECT_ROOT, "raw_prep", "_staging_csv")
    if os.path.exists(temp_out_dir):
        shutil.rmtree(temp_out_dir)

    try:
        for b in BATCH_NAMES:
            os.makedirs(os.path.join(temp_out_dir, b), exist_ok=True)

        # 1. Chia orders trước để có bản đồ order_id -> batch ngày mua và tập 20 đơn bị loại
        order_purchase_batch, excluded_orders, orders_counts, excl_orders = _split_orders(
            input_dir, temp_out_dir
        )
        print(
            f"  + orders: tổng={sum(orders_counts.values()):,} "
            f"(đơn duy nhất={len(order_purchase_batch):,}, loại sau ngày chốt={excl_orders})"
        )

        # 2. Chia order_items và order_payments theo batch ngày mua của đơn cha
        items_counts, excl_items = _split_order_child_table(
            input_dir,
            temp_out_dir,
            "olist_order_items_dataset.csv",
            order_purchase_batch,
            excluded_orders,
        )
        print(f"  + order_items: tổng={sum(items_counts.values()):,} (loại={excl_items})")

        payments_counts, excl_payments = _split_order_child_table(
            input_dir,
            temp_out_dir,
            "olist_order_payments_dataset.csv",
            order_purchase_batch,
            excluded_orders,
        )
        print(f"  + order_payments: tổng={sum(payments_counts.values()):,} (loại={excl_payments})")

        # 3. Chia order_reviews theo ngày tạo và ngày trả lời
        reviews_counts, excl_reviews = _split_order_reviews(
            input_dir, temp_out_dir, excluded_orders
        )
        print(f"  + order_reviews: tổng={sum(reviews_counts.values()):,} (loại={excl_reviews})")

        # 4. Sao chép nguyên văn 5 bảng tĩnh vào bulk
        for table_key, filename, is_static in OLIST_TABLES:
            if not is_static:
                continue
            cnt = _copy_static_table(input_dir, temp_out_dir, filename)
            print(f"  + {table_key}: {cnt:,} dòng (bảng tĩnh -> {BATCH_BULK})")

        # 5. Làm sạch thư mục đích (giữ lại .gitkeep) và chuyển dữ liệu từ staging sang output_dir
        if clean and os.path.exists(output_dir):
            print(f"Đang làm sạch thư mục đích: {output_dir}")
            for item in os.listdir(output_dir):
                if item == ".gitkeep":
                    continue
                item_path = os.path.join(output_dir, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)

        os.makedirs(output_dir, exist_ok=True)

        for b in BATCH_NAMES:
            src_batch_dir = os.path.join(temp_out_dir, b)
            dst_batch_dir = os.path.join(output_dir, b)
            os.makedirs(dst_batch_dir, exist_ok=True)
            for fn in sorted(os.listdir(src_batch_dir)):
                shutil.move(os.path.join(src_batch_dir, fn), os.path.join(dst_batch_dir, fn))

            batch_sha, file_hashes = _compute_batch_checksum(dst_batch_dir)
            write_success_marker(dst_batch_dir, checksum=batch_sha)
            print(
                f"Đã ghi batch {b}: {len(file_hashes)} tệp CSV & _SUCCESS "
                f"(SHA256={batch_sha[:16]}...)"
            )
    finally:
        if os.path.exists(temp_out_dir):
            shutil.rmtree(temp_out_dir)

    print("Hoàn tất ghi tệp Raw Olist CSV.")
    if verify:
        return verify_existing_raw(output_dir)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chia 9 tệp Olist CSV vào 4 batch tầng Raw theo thời điểm sự kiện (B-104)."
    )
    parser.add_argument(
        "--input-dir",
        default=os.path.join(PROJECT_ROOT, "raw_prep", "temp_source"),
        help="Thư mục chứa 9 tệp Olist CSV nguồn.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(PROJECT_ROOT, "raw", "csv"),
        help="Thư mục đích Raw CSV (mặc định: raw/csv).",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Chỉ đối soát các tệp Raw CSV hiện có trong output-dir với B-108 và F01.",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Không xóa các tệp cũ trong output-dir trước khi ghi.",
    )

    args = parser.parse_args()

    if args.verify:
        if not os.path.isfile(os.path.join(args.output_dir, BATCH_BULK, "olist_orders_dataset.csv")):
            print(
                f"[LỖI] Chưa có tệp Raw CSV trong {args.output_dir}. Cần chạy tạo Raw trước.",
                file=sys.stderr,
            )
            sys.exit(1)
        valid = verify_existing_raw(args.output_dir)
        sys.exit(0 if valid else 1)

    try:
        success = process_olist_csv(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            verify=True,
            clean=not args.no_clean,
        )
        sys.exit(0 if success else 1)
    except Exception as exc:
        print(f"[LỖI] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

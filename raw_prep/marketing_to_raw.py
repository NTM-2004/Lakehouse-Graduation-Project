"""
raw_prep/marketing_to_raw.py - Chuyển Marketing Funnel CSV sang JSON Lines lồng và chia 4 batch (B-103)
Tác giả phụ trách: Hoàng
Reviewer: Kiên (theo phân công Design Build v1.2)
Mẫu triển khai: M-08
Tham chiếu:
- Requirement v3 (F01, F02)
- Data Analyze v2 (mục 2.a, 2.b, 2.c)
- Design Bronze, Silver v2 (Mục III: Nguồn json bronze.marketing_leads)
- Design Build v1.2 (B-103)

Mục đích:
- Đọc 2 tệp CSV nguồn:
  + olist_marketing_qualified_leads_dataset.csv (8.000 lead)
  + olist_closed_deals_dataset.csv (842 deal)
- Chuyển thành JSON Lines lồng (1 dòng = 1 lead; deal nếu có lồng trong khóa "deal";
  giá trị thiếu -> khóa vắng mặt, không ghi null; mọi giá trị đơn là chuỗi).
- Chia vào 4 batch theo thời điểm sự kiện:
  + bulk_2016-09_2018-05 (<= 2018-05-31 23:59:59): 8.000 dòng (665 có deal)
  + 2018-06 (2018-06-01 -> 2018-06-30 23:59:59): 57 dòng phát lại kèm deal
  + 2018-07 (2018-07-01 -> 2018-07-31 23:59:59): 37 dòng phát lại kèm deal
  + 2018-08 (2018-08-01 -> 2018-08-31 23:59:59): 33 dòng phát lại kèm deal
- Ghi 50 deal chốt sau ngày chốt (won_date > 2018-08-31 23:59:59) vào thư mục riêng
  raw/marketing_funnel/_excluded/excluded_deals.jsonl (không ghi _SUCCESS; Loader/Airflow bỏ qua)
  để đối soát 792 + 50 = 842 deal nguồn từ trong hệ thống (F02).
- Ghi tệp tín hiệu _SUCCESS kèm mã toàn vẹn SHA-256 cho từng batch qua raw_common.py (B-101).
- Hỗ trợ cờ --verify đối soát với raw_prep/batch_row_counts.csv (B-108) và tiêu chí F02.
"""

import os
import sys
import csv
import json
import shutil
import argparse
from typing import Dict, List, Set, Tuple

# Thêm thư mục gốc vào sys.path để import raw_common
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from raw_prep.raw_common import (
    BATCH_BULK,
    BATCH_2018_06,
    BATCH_2018_07,
    BATCH_2018_08,
    BATCH_NAMES,
    CUTOFF_DATE,
    get_batch_id_by_event_timestamp,
    calculate_sha256,
    write_success_marker,
    load_batch_row_counts,
    verify_row_counts,
)

LEADS_CSV = "olist_marketing_qualified_leads_dataset.csv"
DEALS_CSV = "olist_closed_deals_dataset.csv"
OUT_FILE = "marketing_leads.jsonl"
EXCLUDED_DIR_NAME = "_excluded"
EXCLUDED_FILE = "excluded_deals.jsonl"
TABLE_KEY = "marketing_leads"


def _compact(row: Dict[str, str]) -> Dict[str, str]:
    """Loại bỏ trường rỗng/chỉ có khoảng trắng -> khóa vắng mặt (không ghi null)."""
    return {
        k: v.strip()
        for k, v in row.items()
        if v is not None and v.strip() != ""
    }


def _read_csv(filepath: str) -> List[Dict[str, str]]:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Không tìm thấy tệp CSV nguồn: {filepath}")
    with open(filepath, mode="r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _normalize_ts_for_batch(ts_str: str) -> str:
    """Chuẩn hóa ngày YYYY-MM-DD thành YYYY-MM-DD 00:00:00 để so sánh mốc batch."""
    s = ts_str.strip()
    if len(s) == 10:
        return f"{s} 00:00:00"
    return s


def verify_existing_raw(output_dir: str, input_dir: str = None) -> bool:
    """
    Đối soát dữ liệu Raw Marketing đã tạo với:
    1) raw_prep/batch_row_counts.csv (B-108): số dòng từng batch, tổng dòng, distinct_keys, excluded
    2) Tiêu chí F01/F02:
       - Tệp _SUCCESS tồn tại ở cả 4 batch
       - Không có mốc thời gian nào (first_contact_date, won_date) vượt quá ngày chốt 2018-08-31
       - Mọi deal trong batch và _excluded/ đều gắn với một lead hợp lệ
       - Tổng deal trong phạm vi (792) + deal bị loại ở _excluded/ (50) = 842
    """
    print("=== ĐỐI SOÁT DỮ LIỆU RAW MARKETING (--verify) ===")
    actual_counts: Dict[str, int] = {}
    all_mql_ids: Set[str] = set()
    in_scope_deal_ids: Set[str] = set()
    post_cutoff_violations = 0
    missing_success = []
    parse_errors = 0

    for batch_id in BATCH_NAMES:
        jsonl_file = os.path.join(output_dir, batch_id, OUT_FILE)
        success_file = os.path.join(output_dir, batch_id, "_SUCCESS")

        if not os.path.exists(jsonl_file):
            print(f"[LỆCH] Thiếu tệp JSONL: {jsonl_file}")
            actual_counts[batch_id] = 0
            continue

        if not os.path.exists(success_file):
            missing_success.append(batch_id)
            print(f"[LỆCH] Thiếu tệp tín hiệu _SUCCESS tại {batch_id}")

        count = 0
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                count += 1
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                mql_id = rec.get("mql_id")
                if mql_id:
                    all_mql_ids.add(mql_id)

                fcd = rec.get("first_contact_date", "")
                if fcd and fcd[:10] > CUTOFF_DATE:
                    post_cutoff_violations += 1

                deal = rec.get("deal")
                if isinstance(deal, dict):
                    if mql_id:
                        in_scope_deal_ids.add(mql_id)
                    won = deal.get("won_date", "")
                    if won and won[:10] > CUTOFF_DATE:
                        post_cutoff_violations += 1

        actual_counts[batch_id] = count
        sha = calculate_sha256(jsonl_file)
        print(f"Batch {batch_id}: {count:,} dòng | SHA256: {sha[:16]}...")

    # Kiểm tra thư mục _excluded/
    excluded_file = os.path.join(output_dir, EXCLUDED_DIR_NAME, EXCLUDED_FILE)
    excluded_deal_ids: Set[str] = set()
    excluded_count = 0
    excluded_orphan_deals = 0

    if not os.path.exists(excluded_file):
        print(f"[LỆCH] Thiếu tệp deal bị loại: {excluded_file}")
    else:
        with open(excluded_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                excluded_count += 1
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue
                mql_id = rec.get("mql_id")
                if mql_id:
                    excluded_deal_ids.add(mql_id)
                    if mql_id not in all_mql_ids:
                        excluded_orphan_deals += 1

    # Đối soát với B-108
    is_valid, messages = verify_row_counts(TABLE_KEY, actual_counts)
    for msg in messages:
        prefix = "[OK]" if "Khớp" in msg else "[LỆCH]"
        print(f"  {prefix} {msg}")

    expected_meta = load_batch_row_counts()[TABLE_KEY]
    expected_distinct = expected_meta["distinct_keys"]
    expected_excluded = expected_meta["excluded"]

    checks: List[Tuple[str, bool, str]] = [
        (
            "Tệp tín hiệu _SUCCESS đủ 4 batch",
            len(missing_success) == 0,
            f"thiếu: {missing_success}" if missing_success else "đủ 4/4",
        ),
        (
            "Số mql_id duy nhất khớp distinct_keys (B-108)",
            len(all_mql_ids) == expected_distinct,
            f"thực tế={len(all_mql_ids):,}, chuẩn={expected_distinct:,}",
        ),
        (
            "Số deal bị loại ở _excluded/ khớp excluded (B-108)",
            excluded_count == expected_excluded,
            f"thực tế={excluded_count:,}, chuẩn={expected_excluded:,}",
        ),
        (
            "F02: Deal trong phạm vi + deal bị loại = 842 (792 + 50)",
            len(in_scope_deal_ids) == 792 and len(excluded_deal_ids) == 50 and len(in_scope_deal_ids | excluded_deal_ids) == 842,
            f"trong phạm vi={len(in_scope_deal_ids)}, _excluded={len(excluded_deal_ids)}, hợp={len(in_scope_deal_ids | excluded_deal_ids)}",
        ),
        (
            "F02: Mọi deal bị loại đều gắn đúng một lead",
            excluded_orphan_deals == 0,
            f"mồ côi={excluded_orphan_deals}",
        ),
        (
            "F01: Không có mốc thời gian sau ngày chốt trong 4 batch",
            post_cutoff_violations == 0,
            f"vi phạm={post_cutoff_violations}",
        ),
        (
            "Không có dòng JSON lỗi cú pháp",
            parse_errors == 0,
            f"lỗi={parse_errors}",
        ),
    ]

    # Nếu truyền input_dir chứa CSV nguồn, đối soát thêm tổng deal nguồn
    if input_dir and os.path.exists(os.path.join(input_dir, DEALS_CSV)):
        src_deals_count = len(_read_csv(os.path.join(input_dir, DEALS_CSV)))
        checks.append((
            "F02: Đối soát tổng deal với CSV nguồn",
            len(in_scope_deal_ids) + excluded_count == src_deals_count,
            f"{len(in_scope_deal_ids)} + {excluded_count} vs nguồn {src_deals_count}",
        ))

    for label, ok, detail in checks:
        if not ok:
            is_valid = False
        print(f"  [{'OK' if ok else 'LỆCH'}] {label} ({detail})")

    if is_valid:
        print("[THÀNH CÔNG] Dữ liệu Raw Marketing khớp 100% với B-108 và tiêu chí F01/F02.")
    else:
        print("[THẤT BÀI] Dữ liệu Raw Marketing có sai lệch.")
    return is_valid


def process_marketing(
    input_dir: str,
    output_dir: str,
    verify: bool = True,
    clean: bool = True,
) -> bool:
    """
    Đọc 2 tệp CSV Marketing, chuyển sang JSONL lồng, chia 4 batch + thư mục _excluded/,
    ghi _SUCCESS kèm SHA-256 và đối soát với B-108.
    """
    print("=== BẮT ĐẦU CHUYỂN MARKETING FUNNEL SANG JSONL & CHIA 4 BATCH ===")
    print(f"Nguồn dữ liệu: {input_dir}")
    print(f"Đích lưu trữ:   {output_dir}")

    leads_rows = _read_csv(os.path.join(input_dir, LEADS_CSV))
    deals_rows = _read_csv(os.path.join(input_dir, DEALS_CSV))
    print(f"Đã đọc {len(leads_rows):,} lead và {len(deals_rows):,} deal từ CSV nguồn.")

    leads_by_id: Dict[str, Dict[str, str]] = {}
    for row in leads_rows:
        mql_id = row["mql_id"].strip()
        leads_by_id[mql_id] = row

    # batch_id -> {mql_id: json_record}
    batch_records: Dict[str, Dict[str, Dict[str, object]]] = {b: {} for b in BATCH_NAMES}
    excluded_deals: List[Dict[str, str]] = []

    def make_lead_record(mql_id: str) -> Dict[str, object]:
        return dict(_compact(leads_by_id[mql_id]))

    # 1. Xếp lead vào batch theo first_contact_date
    for mql_id, row in leads_by_id.items():
        b = get_batch_id_by_event_timestamp(_normalize_ts_for_batch(row["first_contact_date"]))
        if b is None:
            print(f"[Cảnh báo] Lead {mql_id} có first_contact_date sau ngày chốt - bỏ qua", file=sys.stderr)
            continue
        batch_records[b][mql_id] = make_lead_record(mql_id)

    # 2. Xếp deal vào batch theo won_date (hoặc _excluded/ nếu sau ngày chốt)
    seen_deals: Set[str] = set()
    for d in deals_rows:
        mql_id = d["mql_id"].strip()
        if mql_id in seen_deals:
            print(f"[Cảnh báo] Trùng deal cho mql_id {mql_id} - giữ bản đầu", file=sys.stderr)
            continue
        seen_deals.add(mql_id)

        if mql_id not in leads_by_id:
            print(f"[Cảnh báo] Deal {mql_id} không có lead tương ứng - bỏ qua", file=sys.stderr)
            continue

        b = get_batch_id_by_event_timestamp(_normalize_ts_for_batch(d["won_date"]))
        if b is None:
            # Sau ngày chốt (31/08/2018): ghi vào _excluded/ để đối soát 792 + 50 = 842 (F02)
            excluded_deals.append(_compact(d))
            continue

        rec = batch_records[b].get(mql_id) or make_lead_record(mql_id)
        rec["deal"] = _compact({k: v for k, v in d.items() if k != "mql_id"})
        batch_records[b][mql_id] = rec

    # 3. Ghi ra thư mục tạm trước khi chuyển vào output_dir (tránh để batch dở)
    temp_out_dir = os.path.join(PROJECT_ROOT, "raw_prep", "_staging_marketing")
    if os.path.exists(temp_out_dir):
        shutil.rmtree(temp_out_dir)

    try:
        for b in BATCH_NAMES:
            b_dir = os.path.join(temp_out_dir, b)
            os.makedirs(b_dir, exist_ok=True)
            out_path = os.path.join(b_dir, OUT_FILE)
            with open(out_path, "w", encoding="utf-8", newline="\n") as f:
                for rec in batch_records[b].values():
                    f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

        excl_temp_dir = os.path.join(temp_out_dir, EXCLUDED_DIR_NAME)
        os.makedirs(excl_temp_dir, exist_ok=True)
        excl_temp_path = os.path.join(excl_temp_dir, EXCLUDED_FILE)
        with open(excl_temp_path, "w", encoding="utf-8", newline="\n") as f:
            for rec in excluded_deals:
                f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

        # Làm sạch thư mục đích nếu clean=True
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

        # Di chuyển 4 batch vào output_dir và tạo _SUCCESS kèm SHA-256
        for b in BATCH_NAMES:
            target_batch_dir = os.path.join(output_dir, b)
            os.makedirs(target_batch_dir, exist_ok=True)
            dst_path = os.path.join(target_batch_dir, OUT_FILE)
            shutil.move(os.path.join(temp_out_dir, b, OUT_FILE), dst_path)

            sha = calculate_sha256(dst_path)
            write_success_marker(target_batch_dir, checksum=sha)
            n_rows = len(batch_records[b])
            n_deals = sum(1 for r in batch_records[b].values() if "deal" in r)
            print(f"Đã ghi {b}/{OUT_FILE} ({n_rows:,} dòng, {n_deals:,} deal) & _SUCCESS (SHA256={sha[:16]}...)")

        # Di chuyển _excluded/ (không ghi _SUCCESS)
        target_excl_dir = os.path.join(output_dir, EXCLUDED_DIR_NAME)
        os.makedirs(target_excl_dir, exist_ok=True)
        dst_excl_path = os.path.join(target_excl_dir, EXCLUDED_FILE)
        shutil.move(excl_temp_path, dst_excl_path)
        print(f"Đã ghi {EXCLUDED_DIR_NAME}/{EXCLUDED_FILE} ({len(excluded_deals):,} deal bị loại sau ngày chốt)")
    finally:
        if os.path.exists(temp_out_dir):
            shutil.rmtree(temp_out_dir)

    print("Hoàn tất ghi tệp Raw Marketing.")

    if verify:
        return verify_existing_raw(output_dir, input_dir=input_dir)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Chuyển Marketing Funnel CSV sang JSONL lồng và chia 4 batch theo thời điểm sự kiện (B-103)."
    )
    parser.add_argument(
        "--input-dir",
        default=os.path.join(PROJECT_ROOT, "raw_prep", "temp_source"),
        help="Thư mục chứa 2 tệp CSV Marketing nguồn.",
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(PROJECT_ROOT, "raw", "marketing_funnel"),
        help="Thư mục đích Raw Marketing (mặc định: raw/marketing_funnel).",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Chỉ đối soát các tệp Raw hiện có trong output-dir với B-108 và F02.",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Không xóa các tệp cũ trong output-dir trước khi ghi.",
    )

    args = parser.parse_args()

    if args.verify:
        if not os.path.exists(os.path.join(args.output_dir, BATCH_BULK, OUT_FILE)):
            print(f"[LỖI] Chưa có tệp Raw trong {args.output_dir}. Cần chạy tạo Raw trước.", file=sys.stderr)
            sys.exit(1)
        valid = verify_existing_raw(args.output_dir, input_dir=args.input_dir)
        sys.exit(0 if valid else 1)

    try:
        success = process_marketing(
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

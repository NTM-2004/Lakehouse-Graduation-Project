"""
raw_prep/clickstream_to_raw.py - Chia log Clickstream theo thời điểm sự kiện vào 4 batch (B-102)
Tác giả phụ trách: Minh
Mẫu triển khai: M-08
Tham chiếu: Requirement v3 (F01), Data Analyze v2 (mục 2.b, 2.c), Design Build (B-102)

Mục đích:
- Đọc dữ liệu log Clickstream gốc.
- Phân chia lại theo thời điểm sự kiện (event timestamp) vào 4 batch:
  + bulk_2016-09_2018-05 (<= 2018-05-31 23:59:59)
  + 2018-06 (2018-06-01 -> 2018-06-30 23:59:59)
  + 2018-07 (2018-07-01 -> 2018-07-31 23:59:59)
  + 2018-08 (2018-08-01 -> 2018-08-31 23:59:59)
- Ghi tệp access_log.txt nguyên văn vào raw/clickstream/<batch_id>/
- Ghi tệp tín hiệu _SUCCESS kèm mã toàn vẹn SHA-256.
- Hỗ trợ cờ --verify để đối soát với raw_prep/batch_row_counts.csv (B-108).
"""

import os
import sys
import glob
import shutil
import argparse
from typing import Dict, List

# Thêm thư mục gốc vào sys.path để import raw_common
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from raw_prep.raw_common import (
    BATCH_BULK,
    BATCH_NAMES,
    get_batch_id_by_event_timestamp,
    calculate_sha256,
    write_success_marker,
    verify_row_counts,
)


def verify_existing_raw(output_dir: str) -> bool:
    """Đối soát số dòng các tệp Raw Clickstream đã tạo với bảng chuẩn."""
    print("=== ĐỐI SOÁT DỮ LIỆU RAW CLICKSTREAM (--verify) ===")
    actual_counts: Dict[str, int] = {}

    for batch_id in BATCH_NAMES:
        log_file = os.path.join(output_dir, batch_id, "access_log.txt")
        success_file = os.path.join(output_dir, batch_id, "_SUCCESS")

        if not os.path.exists(log_file):
            print(f"[!] Thiếu tệp log: {log_file}")
            actual_counts[batch_id] = 0
            continue

        if not os.path.exists(success_file):
            print(f"[!] Cảnh báo: Thiếu tệp tín hiệu _SUCCESS tại {batch_id}")

        count = 0
        with open(log_file, "r", encoding="utf-8") as f:
            for _ in f:
                count += 1
        actual_counts[batch_id] = count

        sha = calculate_sha256(log_file)
        print(f"Batch {batch_id}: {count:,} dòng | SHA256: {sha[:16]}...")

    is_valid, messages = verify_row_counts("clickstream", actual_counts)
    for msg in messages:
        prefix = "[OK]" if "Khớp" in msg else "[LỆCH]"
        print(f"  {prefix} {msg}")

    if is_valid:
        print("[THÀNH CÔNG] Dữ liệu Raw Clickstream khớp 100% với thiết kế chuẩn.")
    else:
        print("[THẤT BÀI] Dữ liệu có sự sai lệch so với bảng chuẩn B-108.")
    return is_valid


def process_clickstream(
    input_dir: str,
    output_dir: str,
    verify: bool = True,
    clean: bool = True
) -> bool:
    """
    Xóa tệp cũ trong output_dir (nếu clean=True),
    đọc tệp nguồn từ input_dir và phân chia lại vào 4 batch theo thời điểm sự kiện.
    """
    print(f"=== BẮT ĐẦU CHIA LOG CLICKSTREAM VÀO 4 BATCH ===")
    print(f"Nguồn dữ liệu: {input_dir}")
    print(f"Đích lưu trữ:   {output_dir}")

    # Tìm các tệp log nguồn
    source_files: List[str] = []
    if os.path.isdir(input_dir):
        for pattern in ["*.txt", "*/*.txt"]:
            source_files.extend(glob.glob(os.path.join(input_dir, pattern)))
    elif os.path.isfile(input_dir):
        source_files = [input_dir]

    if not source_files:
        raise FileNotFoundError(f"Không tìm thấy tệp log nguồn nào trong {input_dir}")

    print(f"Tìm thấy {len(source_files)} tệp nguồn: {[os.path.basename(f) for f in source_files]}")

    # Tạo thư mục tạm để ghi trước khi chuyển sang output_dir
    temp_out_dir = os.path.join(PROJECT_ROOT, "raw_prep", "_staging_clickstream")
    if os.path.exists(temp_out_dir):
        shutil.rmtree(temp_out_dir)

    for b in BATCH_NAMES:
        os.makedirs(os.path.join(temp_out_dir, b), exist_ok=True)

    batch_handles = {
        b: open(os.path.join(temp_out_dir, b, "access_log.txt"), "w", encoding="utf-8")
        for b in BATCH_NAMES
    }

    counts = {b: 0 for b in BATCH_NAMES}
    excluded_count = 0
    total_processed = 0

    try:
        for sfile in source_files:
            print(f"Đang đọc: {sfile} ...")
            with open(sfile, "r", encoding="utf-8") as sf:
                for line in sf:
                    total_processed += 1
                    # Timestamp nằm ở đầu dòng (ví dụ: 2018-06-15 12:34:56.000000)
                    ts_prefix = line[:19]
                    batch_id = get_batch_id_by_event_timestamp(ts_prefix)

                    if batch_id is None:
                        excluded_count += 1
                        continue

                    batch_handles[batch_id].write(line)
                    counts[batch_id] += 1
    finally:
        for h in batch_handles.values():
            h.close()

    print("\n--- Kết quả phân loại dòng theo batch ---")
    for b in BATCH_NAMES:
        print(f"  + {b}: {counts[b]:,} dòng")
    print(f"  + Bị loại (> cutoff 31/08/2018): {excluded_count} dòng")
    print(f"  + Tổng dòng xử lý: {total_processed:,} dòng")

    # Xóa thư mục cũ nếu có yêu cầu clean
    if clean and os.path.exists(output_dir):
        print(f"Đang làm sạch thư mục đích: {output_dir}")
        for item in os.listdir(output_dir):
            item_path = os.path.join(output_dir, item)
            if item == ".gitkeep":
                continue
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)

    os.makedirs(output_dir, exist_ok=True)

    # Chuyển dữ liệu từ temp_out_dir sang output_dir và tạo _SUCCESS
    checksums = {}
    for b in BATCH_NAMES:
        target_batch_dir = os.path.join(output_dir, b)
        os.makedirs(target_batch_dir, exist_ok=True)

        src_log = os.path.join(temp_out_dir, b, "access_log.txt")
        dst_log = os.path.join(target_batch_dir, "access_log.txt")
        shutil.move(src_log, dst_log)

        # Tính checksum và tạo marker _SUCCESS
        sha = calculate_sha256(dst_log)
        checksums[b] = sha
        write_success_marker(target_batch_dir, checksum=sha)
        print(f"Đã ghi {b}/access_log.txt ({counts[b]:,} dòng) & _SUCCESS (SHA256={sha[:16]}...)")

    # Dọn dẹp thư mục tạm
    if os.path.exists(temp_out_dir):
        shutil.rmtree(temp_out_dir)

    print("Hoàn tất ghi tệp Raw Clickstream.")

    if verify:
        return verify_existing_raw(output_dir)
    return True


def main():
    parser = argparse.ArgumentParser(description="Chia log Clickstream vào 4 batch theo thời điểm sự kiện.")
    parser.add_argument(
        "--input-dir",
        default=os.path.join(PROJECT_ROOT, "raw_prep", "temp_source"),
        help="Thư mục chứa các tệp log nguồn."
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(PROJECT_ROOT, "raw", "clickstream"),
        help="Thư mục đích Raw Clickstream."
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Chỉ đối soát số dòng các tệp hiện có trong output-dir với B-108."
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Không xóa các file cũ trong output-dir trước khi ghi."
    )

    args = parser.parse_args()

    if args.verify and not os.path.exists(os.path.join(args.output_dir, BATCH_BULK, "access_log.txt")):
        print(f"[!] Chưa có tệp Raw trong {args.output_dir}. Cần chạy tạo Raw trước.")
        sys.exit(1)

    if args.verify and len(sys.argv) == 2:
        # Nếu chỉ truyền cờ --verify
        valid = verify_existing_raw(args.output_dir)
        sys.exit(0 if valid else 1)

    # Chạy quy trình phân chia và đối soát
    success = process_clickstream(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        verify=True,
        clean=not args.no_clean
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

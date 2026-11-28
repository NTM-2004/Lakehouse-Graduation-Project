"""
raw_prep/raw_common.py - Thư viện dùng chung cho các chương trình chuẩn bị Raw (B-101)
Tác giả phụ trách: Minh
Hỗ trợ:
- Định nghĩa các batch_id và mốc ngày chốt (cutoff date)
- Phân loại mốc thời gian sự kiện vào đúng batch
- Tính mã toàn vẹn (SHA-256)
- Ghi tệp tín hiệu _SUCCESS (kèm mã toàn vẹn nếu có)
- Đọc bảng số dòng chuẩn (B-108: batch_row_counts.csv) và hàm đối soát --verify
"""

import os
import csv
import hashlib
from typing import Dict, List, Optional, Tuple

CUTOFF_TIMESTAMP = "2018-08-31 23:59:59.999999"
CUTOFF_DATE = "2018-08-31"

BATCH_BULK = "bulk_2016-09_2018-05"
BATCH_2018_06 = "2018-06"
BATCH_2018_07 = "2018-07"
BATCH_2018_08 = "2018-08"

BATCH_NAMES = [BATCH_BULK, BATCH_2018_06, BATCH_2018_07, BATCH_2018_08]


def get_batch_id_by_event_timestamp(ts_str: str) -> Optional[str]:
    """
    Xác định batch_id dựa theo thời điểm sự kiện (chuỗi YYYY-MM-DD HH:MM:SS...).
    Trả về None nếu sự kiện xảy ra sau ngày chốt (cutoff).
    """
    clean_ts = str(ts_str).strip()[:19]
    if clean_ts > "2018-08-31 23:59:59":
        return None
    elif clean_ts <= "2018-05-31 23:59:59":
        return BATCH_BULK
    elif clean_ts <= "2018-06-30 23:59:59":
        return BATCH_2018_06
    elif clean_ts <= "2018-07-31 23:59:59":
        return BATCH_2018_07
    else:
        return BATCH_2018_08


def calculate_sha256(filepath: str, block_size: int = 65536) -> str:
    """Tính mã băm SHA-256 của tệp."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            sha256.update(block)
    return sha256.hexdigest()


def write_success_marker(batch_dir: str, checksum: Optional[str] = None) -> str:
    """
    Ghi tệp tín hiệu _SUCCESS vào thư mục batch.
    Nếu có checksum, ghi checksum vào tệp _SUCCESS hoặc _checksum.sha256.
    """
    os.makedirs(batch_dir, exist_ok=True)
    success_path = os.path.join(batch_dir, "_SUCCESS")
    with open(success_path, "w", encoding="utf-8") as f:
        if checksum:
            f.write(f"SHA256={checksum}\n")
    return success_path


def load_batch_row_counts(csv_path: str = "raw_prep/batch_row_counts.csv") -> Dict[str, Dict[str, int]]:
    """
    Đọc bảng B-108 batch_row_counts.csv thành dictionary:
    {
        'table_name': {
            'bulk': int,
            '2018-06': int,
            '2018-07': int,
            '2018-08': int,
            'total_rows': int,
            'distinct_keys': int,
            'excluded': int
        }
    }
    """
    if not os.path.isabs(csv_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, csv_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Không tìm thấy tệp đếm dòng chuẩn: {csv_path}")

    counts_map = {}
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tname = row["table_name"].strip()
            counts_map[tname] = {
                "bulk": int(row["bulk"]),
                "2018-06": int(row["2018-06"]),
                "2018-07": int(row["2018-07"]),
                "2018-08": int(row["2018-08"]),
                "total_rows": int(row["total_rows"]),
                "distinct_keys": int(row["distinct_keys"]),
                "excluded": int(row["excluded"]),
            }
    return counts_map


def verify_row_counts(
    table_name: str,
    actual_counts: Dict[str, int],
    csv_path: str = "raw_prep/batch_row_counts.csv",
) -> Tuple[bool, List[str]]:
    """
    Hàm đối soát số dòng cho tùy chọn --verify.
    So khớp actual_counts (gồm các key: bulk, 2018-06, 2018-07, 2018-08 hoặc tên batch đầy đủ)
    với số liệu chuẩn trong B-108.
    """
    expected_all = load_batch_row_counts(csv_path)
    if table_name not in expected_all:
        return False, [f"Bảng {table_name} không có trong {csv_path}"]

    expected = expected_all[table_name]
    messages = []
    is_valid = True

    # Chuẩn hóa key
    normalized_actual = {
        "bulk": actual_counts.get(BATCH_BULK, actual_counts.get("bulk", 0)),
        "2018-06": actual_counts.get(BATCH_2018_06, actual_counts.get("2018-06", 0)),
        "2018-07": actual_counts.get(BATCH_2018_07, actual_counts.get("2018-07", 0)),
        "2018-08": actual_counts.get(BATCH_2018_08, actual_counts.get("2018-08", 0)),
    }

    actual_total = sum(normalized_actual.values())

    for batch_key in ["bulk", "2018-06", "2018-07", "2018-08"]:
        exp = expected[batch_key]
        act = normalized_actual[batch_key]
        if exp != act:
            is_valid = False
            messages.append(f"Batch {batch_key}: Lệch! Thực tế={act:,}, Chuẩn={exp:,} (lệch {act - exp:+d})")
        else:
            messages.append(f"Batch {batch_key}: Khớp chuẩn = {act:,} dòng")

    if actual_total != expected["total_rows"]:
        is_valid = False
        messages.append(f"Tổng số dòng: Lệch! Thực tế={actual_total:,}, Chuẩn={expected['total_rows']:,}")
    else:
        messages.append(f"Tổng số dòng: Khớp chuẩn = {actual_total:,} dòng")

    return is_valid, messages

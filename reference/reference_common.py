"""
reference/reference_common.py - Thư viện dùng chung cho chương trình tham chiếu và so sánh (B-432)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Tham chiếu:
- Design Deployment (Mục III: Giá trị tham chiếu)
- Design Serving (Mục II: So khớp với Spark; Tiêu chí hòa Q2)
- Design Build v1.2 (B-432, đã gộp compare_common)

Chức năng:
- Hằng số thứ tự batch (batch_seq), ngày chốt (2018-08-31), danh sách 24 tháng dim_date
- Đọc tệp Raw CSV / JSONL / Clickstream và chọn bản chụp có batch_seq lớn nhất theo khóa
- Tính chênh lệch ngày lịch nguyên (bỏ phần giờ ở cả hai vế)
- Hàm chia an toàn (mẫu số = 0 -> None)
- Ghi/đọc tệp reference/reference_values.csv chuẩn hóa
- Hàm so sánh kết quả theo khóa lát cắt, làm tròn tỷ lệ đến 4 chữ số thập phân (So khớp với Spark)
  và tiêu chí hòa Q2 (doanh thu giảm dần, hòa thì seller_id tăng dần)
"""

import csv
import json
import os
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

CUTOFF_DATE_STR = "2018-08-31"
CUTOFF_DATE = date(2018, 8, 31)
START_DATE = date(2016, 9, 4)

BATCH_SEQ: Dict[str, int] = {
    "bulk_2016-09_2018-05": 0,
    "2018-06": 1,
    "2018-07": 2,
    "2018-08": 3,
}
BATCH_ORDER: List[str] = ["bulk_2016-09_2018-05", "2018-06", "2018-07", "2018-08"]

SMALL_MONTH_ORDERS = 100
RATE_DECIMALS = 4

REF_FIELDNAMES = ["table_name", "slice_key", "kpi_name", "value"]


def all_dim_months() -> List[str]:
    """Trả về đủ 24 tháng từ 2016-09 đến 2018-08 (khớp gold.dim_date)."""
    months: List[str] = []
    y, m = START_DATE.year, START_DATE.month
    while (y, m) <= (CUTOFF_DATE.year, CUTOFF_DATE.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def parse_date(val: Optional[str]) -> Optional[date]:
    """Chuyển chuỗi YYYY-MM-DD hoặc YYYY-MM-DD HH:MM:SS thành date (bỏ phần giờ)."""
    if val is None:
        return None
    s = val.strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_timestamp(val: Optional[str]) -> Optional[datetime]:
    """Chuyển chuỗi timestamp YYYY-MM-DD HH:MM:SS(.ffffff) thành datetime."""
    if val is None:
        return None
    s = val.strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def calendar_days_diff(end_val: Optional[str], start_val: Optional[str]) -> Optional[int]:
    """Chênh lệch ngày lịch nguyên (end - start), bỏ phần giờ ở cả hai vế."""
    d_end = parse_date(end_val)
    d_start = parse_date(start_val)
    if d_end is None or d_start is None:
        return None
    return (d_end - d_start).days


def safe_div(num: float, den: float) -> Optional[float]:
    """Chia an toàn: mẫu số bằng 0 trả về None."""
    if den == 0:
        return None
    return float(num) / float(den)


def round_rate(val: Optional[float], decimals: int = RATE_DECIMALS) -> Optional[str]:
    """Làm tròn số thực theo chuẩn HALF_UP đến `decimals` chữ số thập phân để so sánh."""
    if val is None:
        return None
    q = Decimal(10) ** (-decimals)
    return str(Decimal(str(val)).quantize(q, rounding=ROUND_HALF_UP))


def read_raw_csv_latest(
    raw_csv_root: str,
    filename: str,
    key_fn: Callable[[Dict[str, str]], Tuple[str, ...]],
) -> Dict[Tuple[str, ...], Dict[str, str]]:
    """
    Đọc một bảng CSV qua 4 thư mục batch trong raw/csv/<batch>/<filename>,
    giữ bản chụp của batch có batch_seq lớn nhất cho mỗi khóa.
    """
    records: Dict[Tuple[str, ...], Tuple[int, Dict[str, str]]] = {}
    for batch_id in BATCH_ORDER:
        seq = BATCH_SEQ[batch_id]
        path = os.path.join(raw_csv_root, batch_id, filename)
        if not os.path.exists(path):
            continue
        with open(path, mode="r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                k = key_fn(row)
                prev = records.get(k)
                if prev is None or seq >= prev[0]:
                    records[k] = (seq, row)
    return {k: v[1] for k, v in records.items()}


def read_raw_csv_all(raw_csv_root: str, filename: str) -> List[Dict[str, str]]:
    """Đọc toàn bộ dòng của một bảng CSV qua 4 batch (cho bảng không ghi lại nhiều bản chụp)."""
    rows: List[Dict[str, str]] = []
    for batch_id in BATCH_ORDER:
        path = os.path.join(raw_csv_root, batch_id, filename)
        if not os.path.exists(path):
            continue
        with open(path, mode="r", newline="", encoding="utf-8-sig") as f:
            rows.extend(csv.DictReader(f))
    return rows


def read_raw_marketing_latest(
    raw_marketing_root: str,
    filename: str = "marketing_leads.jsonl",
) -> Dict[str, Dict[str, Any]]:
    """
    Đọc 4 batch Raw Marketing JSONL (tự động bỏ qua _excluded/),
    giữ bản chụp có batch_seq lớn nhất theo mql_id.
    """
    records: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    for batch_id in BATCH_ORDER:
        seq = BATCH_SEQ[batch_id]
        path = os.path.join(raw_marketing_root, batch_id, filename)
        if not os.path.exists(path):
            continue
        with open(path, mode="r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                rec = json.loads(s)
                mql_id = str(rec.get("mql_id", "")).strip()
                if not mql_id:
                    continue
                prev = records.get(mql_id)
                if prev is None or seq >= prev[0]:
                    records[mql_id] = (seq, rec)
    return {k: v[1] for k, v in records.items()}


def format_ref_value(val: Any) -> str:
    """Định dạng giá trị lưu vào reference_values.csv (None -> chuỗi rỗng)."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, float):
        return f"{val:.10f}".rstrip("0").rstrip(".") or "0"
    return str(val)


def write_reference_csv(rows: Sequence[Dict[str, Any]], output_path: str) -> None:
    """Ghi danh sách dòng giá trị tham chiếu vào tệp CSV chuẩn (sắp xếp xác định)."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    sorted_rows = sorted(
        rows,
        key=lambda r: (str(r["table_name"]), str(r["slice_key"]), str(r["kpi_name"])),
    )
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REF_FIELDNAMES)
        writer.writeheader()
        for r in sorted_rows:
            writer.writerow(
                {
                    "table_name": r["table_name"],
                    "slice_key": r["slice_key"],
                    "kpi_name": r["kpi_name"],
                    "value": format_ref_value(r["value"]),
                }
            )


def load_reference_csv(csv_path: str) -> Dict[Tuple[str, str, str], Optional[str]]:
    """Đọc reference_values.csv thành map {(table_name, slice_key, kpi_name): value_or_None}."""
    result: Dict[Tuple[str, str, str], Optional[str]] = {}
    with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["table_name"], row["slice_key"], row["kpi_name"])
            raw_v = row["value"]
            result[key] = None if raw_v == "" else raw_v
    return result


def normalize_for_compare(val: Any, is_float: bool = False, decimals: int = RATE_DECIMALS) -> Optional[str]:
    """Chuẩn hóa giá trị để so sánh giữa hai nguồn (Spark vs Dremio, Gold vs Reference)."""
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return "true" if val else "false"
    if not is_float:
        try:
            f = float(val)
            if f.is_integer():
                return str(int(f))
        except (ValueError, TypeError):
            return str(val).strip()
    try:
        return round_rate(float(val), decimals=decimals)
    except (ValueError, TypeError):
        return str(val).strip()


def compare_keyed_maps(
    expected: Dict[Tuple[str, ...], Any],
    actual: Dict[Tuple[str, ...], Any],
    float_keys: Optional[Callable[[Tuple[str, ...]], bool]] = None,
    decimals: int = RATE_DECIMALS,
) -> Tuple[bool, List[str]]:
    """
    So khớp hai tập kết quả theo khóa lát cắt (không phụ thuộc thứ tự dòng).
    Tỷ lệ/trung bình làm tròn đến `decimals` chữ số thập phân (mặc định 4, theo So khớp với Spark).
    """
    diffs: List[str] = []
    all_keys = sorted(set(expected.keys()) | set(actual.keys()))

    for k in all_keys:
        if k not in actual:
            diffs.append(f"Thiếu khóa trong kết quả thực tế: {k} (chuẩn={expected[k]})")
            continue
        if k not in expected:
            diffs.append(f"Dư khóa trong kết quả thực tế: {k} (thực tế={actual[k]})")
            continue

        is_fl = float_keys(k) if float_keys else False
        e_norm = normalize_for_compare(expected[k], is_float=is_fl, decimals=decimals)
        a_norm = normalize_for_compare(actual[k], is_float=is_fl, decimals=decimals)
        if e_norm != a_norm:
            diffs.append(f"Lệch tại {k}: chuẩn={e_norm}, thực tế={a_norm}")

    return len(diffs) == 0, diffs


def sort_q2_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Tiêu chí hòa của truy vấn Q2 (Design Serving, Mục VI):
    Doanh thu giảm dần, hòa thì seller_id tăng dần.
    """
    return sorted(
        rows,
        key=lambda r: (-float(r["revenue"]), str(r["seller_id"])),
    )

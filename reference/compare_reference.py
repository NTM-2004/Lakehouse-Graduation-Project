"""
reference/compare_reference.py - So khớp 8 bảng KPI Gold với tệp giá trị tham chiếu (B-438)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Tham chiếu:
- Design Deployment (Mục III: Giá trị tham chiếu - Phạm vi so)
- Design Serving (Mục II: So khớp với Spark - làm tròn tỷ lệ đến 4 chữ số thập phân)
- Design Build v1.2 (B-438)

Hỗ trợ đọc bảng KPI Gold qua:
- Dremio REST API SQL (mặc định, đọc biến môi trường DREMIO_HOST/PORT/ADMIN_USER/ADMIN_PASSWORD)
- Hoặc thư mục CSV xuất từ Gold (--gold-csv-dir) để kiểm thử ngoại tuyến.
"""

import argparse
import csv
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.reference_common import compare_keyed_maps, load_reference_csv

KPI_TABLE_SLICE_COLS: Dict[str, List[str]] = {
    "kpi_sales_month": ["year_month"],
    "kpi_sales_state_month": ["customer_state", "year_month"],
    "kpi_delivery_month": ["year_month"],
    "kpi_delivery_state_month": ["customer_state", "year_month"],
    "kpi_lead_month": ["year_month"],
    "kpi_lead_origin": ["origin"],
    "kpi_funnel_day": ["session_date"],
    "kpi_funnel_month": ["year_month"],
}

FLOAT_METRIC_PREFIXES = ("avg_", "sum_revenue", "order_growth", "score_gap")
FLOAT_METRIC_SUFFIXES = ("_rate",)
SKIP_COLS = {"n_obs", "is_small_sample", "is_small_gap_group"}


def _is_float_metric(key: Tuple[str, ...]) -> bool:
    kpi_name = key[2]
    return kpi_name.startswith(FLOAT_METRIC_PREFIXES) or kpi_name.endswith(FLOAT_METRIC_SUFFIXES)


def _query_dremio_table(table_name: str, catalog: str = "hive", schema: str = "gold") -> List[Dict[str, Any]]:
    """Đọc toàn bộ dòng một bảng KPI Gold qua Dremio REST API."""
    import requests

    host = os.getenv("DREMIO_HOST", "localhost")
    port = os.getenv("DREMIO_PORT", "9047")
    user = os.getenv("DREMIO_ADMIN_USER", "").strip()
    password = os.getenv("DREMIO_ADMIN_PASSWORD", "").strip()
    if not user or not password:
        raise RuntimeError(
            "Thiếu biến môi trường DREMIO_ADMIN_USER hoặc DREMIO_ADMIN_PASSWORD."
        )
    base_url = f"http://{host}:{port}"

    session = requests.Session()
    login = session.post(f"{base_url}/apiv2/login", json={"userName": user, "password": password}, timeout=30)
    login.raise_for_status()
    session.headers.update({"Authorization": f"_dremio{login.json()['token']}"})

    sql = f'SELECT * FROM "{catalog}"."{schema}"."{table_name}"'
    resp = session.post(f"{base_url}/api/v3/sql", json={"sql": sql}, timeout=60)
    resp.raise_for_status()
    job_id = resp.json()["id"]

    for _ in range(120):
        st = session.get(f"{base_url}/api/v3/job/{job_id}", timeout=30)
        st.raise_for_status()
        state = st.json().get("jobState")
        if state == "COMPLETED":
            break
        if state in ("FAILED", "CANCELED", "CANCELLED"):
            raise RuntimeError(f"Dremio job {job_id} lỗi khi đọc {table_name}: {st.text}")
        time.sleep(1)

    rows: List[Dict[str, Any]] = []
    offset, limit = 0, 500
    while True:
        res = session.get(f"{base_url}/api/v3/job/{job_id}/results?offset={offset}&limit={limit}", timeout=30)
        res.raise_for_status()
        batch = res.json().get("rows", [])
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return rows


def _read_gold_csv_table(gold_csv_dir: str, table_name: str) -> List[Dict[str, Any]]:
    path = os.path.join(gold_csv_dir, f"{table_name}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy tệp Gold CSV: {path}")
    with open(path, mode="r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _rows_to_metric_map(table_name: str, rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Any]:
    slice_cols = KPI_TABLE_SLICE_COLS[table_name]
    out: Dict[Tuple[str, str, str], Any] = {}
    for row in rows:
        slice_key = "|".join(str(row[c]) for c in slice_cols)
        for col, val in row.items():
            if col in slice_cols or col in SKIP_COLS:
                continue
            out[(table_name, slice_key, col)] = None if val == "" else val
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="So khớp 8 bảng KPI Gold với reference/reference_values.csv (B-438)."
    )
    parser.add_argument(
        "--reference-csv",
        default=os.path.join(PROJECT_ROOT, "reference", "reference_values.csv"),
        help="Đường dẫn tệp giá trị tham chiếu.",
    )
    parser.add_argument(
        "--gold-csv-dir",
        default=None,
        help="Nếu cung cấp, đọc 8 bảng KPI từ thư mục CSV thay vì truy vấn Dremio.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(KPI_TABLE_SLICE_COLS.keys()),
        default=list(KPI_TABLE_SLICE_COLS.keys()),
        help="Danh sách bảng KPI cần so sánh (mặc định: cả 8 bảng).",
    )
    args = parser.parse_args()

    expected_all = load_reference_csv(args.reference_csv)
    expected: Dict[Tuple[str, str, str], Optional[str]] = {
        k: v for k, v in expected_all.items() if k[0] in args.tables
    }

    actual: Dict[Tuple[str, str, str], Any] = {}
    for tbl in args.tables:
        rows = (
            _read_gold_csv_table(args.gold_csv_dir, tbl)
            if args.gold_csv_dir
            else _query_dremio_table(tbl)
        )
        actual.update(_rows_to_metric_map(tbl, rows))
        print(f"Đã đọc {len(rows):,} dòng từ gold.{tbl}")

    ok, diffs = compare_keyed_maps(expected, actual, float_keys=_is_float_metric)
    if ok:
        print(f"[THÀNH CÔNG] Toàn bộ {len(expected):,} giá trị KPI khớp 100% với tệp tham chiếu.")
        sys.exit(0)

    print(f"[THẤT BÀI] Phát hiện {len(diffs):,} sai lệch:", file=sys.stderr)
    for d in diffs[:50]:
        print(f"  - {d}", file=sys.stderr)
    if len(diffs) > 50:
        print(f"  ... và {len(diffs) - 50} sai lệch khác.", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()

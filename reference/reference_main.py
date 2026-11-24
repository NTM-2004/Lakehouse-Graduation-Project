"""
reference/reference_main.py - Tổng hợp giá trị tham chiếu từ Raw thành reference/reference_values.csv (B-436)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Tham chiếu:
- Design Deployment (Mục III: Giá trị tham chiếu)
- Design Build v1.2 (B-436)

Cách dùng:
    python reference/reference_main.py --raw-root raw --output reference/reference_values.csv
    python reference/reference_main.py --raw-root raw --uc uc03
"""

import argparse
import os
import sys
from typing import Any, Dict, List

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.reference_common import write_reference_csv
from reference.reference_uc01_uc02 import compute_uc01_uc02_reference
from reference.reference_uc03 import compute_uc03_reference
from reference.reference_uc04 import compute_uc04_reference


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tính giá trị tham chiếu độc lập từ dữ liệu Raw (B-436)."
    )
    parser.add_argument(
        "--raw-root",
        default=os.path.join(PROJECT_ROOT, "raw"),
        help="Thư mục gốc Raw chứa csv/, marketing_funnel/, clickstream/ (mặc định: raw).",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(PROJECT_ROOT, "reference", "reference_values.csv"),
        help="Đường dẫn tệp CSV giá trị tham chiếu đầu ra.",
    )
    parser.add_argument(
        "--uc",
        nargs="+",
        choices=["uc01_uc02", "uc03", "uc04", "all"],
        default=["all"],
        help="Chọn nhóm UC cần tính (mặc định: all).",
    )
    args = parser.parse_args()

    selected = set(args.uc)
    run_all = "all" in selected

    all_rows: List[Dict[str, Any]] = []
    if run_all or "uc01_uc02" in selected:
        rows = compute_uc01_uc02_reference(os.path.join(args.raw_root, "csv"))
        print(f"[OK] UC-01 & UC-02: {len(rows):,} dòng giá trị tham chiếu")
        all_rows.extend(rows)

    if run_all or "uc03" in selected:
        rows = compute_uc03_reference(os.path.join(args.raw_root, "marketing_funnel"))
        print(f"[OK] UC-03: {len(rows):,} dòng giá trị tham chiếu")
        all_rows.extend(rows)

    if run_all or "uc04" in selected:
        rows = compute_uc04_reference(os.path.join(args.raw_root, "clickstream"))
        print(f"[OK] UC-04: {len(rows):,} dòng giá trị tham chiếu")
        all_rows.extend(rows)

    write_reference_csv(all_rows, args.output)
    print(f"[THÀNH CÔNG] Đã ghi {len(all_rows):,} dòng vào {args.output}")


if __name__ == "__main__":
    main()

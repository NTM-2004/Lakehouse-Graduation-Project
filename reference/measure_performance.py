"""
reference/measure_performance.py - Đo hiệu năng Q1..Q5 hai chế độ qua Arrow Flight SQL (B-703)
Tác giả phụ trách: Minh | Reviewer: Kiên
Tham chiếu:
- Requirement v3: Hiệu năng truy vấn (1 lần làm nóng + 5 lần chạy, trung vị, ngưỡng 10s / 3s / >= 2x ở >= 3/5 truy vấn)
- Design Serving: Mục IV (Cách đo, Ghi nhận kết quả đo)
- Design Build: B-703

Quy tắc bắt buộc:
- Đường đo: qua Arrow Flight SQL thông qua streamlit/dremio_client.py (cùng đường với Streamlit).
- Thời gian tính từ lúc gửi truy vấn đến lúc nhận đủ kết quả (DataFrame đầy đủ).
- Mỗi truy vấn chạy 1 lần làm nóng (không tính vào kết quả), sau đó chạy 5 lần đo và lấy trung vị.
- Đo hai chế độ: Không có Reflection và Có Reflection.
- Ghi đầy đủ các trường của Bảng kết quả đo theo Design Serving:
  Truy vấn | Chế độ | Các lần chạy (s) | Trung vị (s) | Tăng tốc (x) | Kết quả | Tổng hợp.
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.standard_queries import (
    QUERY_SPECS,
    STANDARD_QUERY_CODES,
    get_query_sql,
)

# Ngưỡng nghiệm thu đề xuất (Requirement v3: Hiệu năng truy vấn)
MAX_MEDIAN_WITHOUT_REFLECTION_S = 10.0
MAX_MEDIAN_WITH_REFLECTION_S = 3.0
MIN_SPEEDUP_FACTOR = 2.0
MIN_SPEEDUP_PASS_COUNT = 3

MODE_WITHOUT = "without_reflection"
MODE_WITH = "with_reflection"
MODE_LABELS = {
    MODE_WITHOUT: "Không có Reflection",
    MODE_WITH: "Có Reflection",
}


def measure_single_query(
    code: str,
    runs: int = 5,
    catalog: str = "hive",
    schema: str = "gold",
) -> Tuple[List[float], float, int]:
    """
    Đo một truy vấn chuẩn qua Arrow Flight SQL:
    - 1 lần làm nóng (bỏ qua thời gian).
    - `runs` lần đo liên tiếp (mặc định 5), trả về (danh_sách_thời_gian, trung_vị, số_dòng).
    """
    from streamlit.dremio_client import query_sql

    sql = get_query_sql(code, dialect="dremio", catalog=catalog, schema=schema)

    # 1 lần làm nóng (warm-up)
    _ = query_sql(sql)

    durations: List[float] = []
    row_count = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        df = query_sql(sql)
        elapsed = time.perf_counter() - t0
        durations.append(round(elapsed, 4))
        row_count = len(df)

    median_s = round(statistics.median(durations), 4)
    return durations, median_s, row_count


def run_benchmark_mode(
    mode: str,
    query_codes: Sequence[str],
    runs: int,
    catalog: str,
    schema: str,
) -> Dict[str, Dict[str, Any]]:
    """Chạy đo toàn bộ danh sách truy vấn cho một chế độ."""
    results: Dict[str, Dict[str, Any]] = {}
    print(f"[B-703] Đo chế độ '{MODE_LABELS[mode]}' (1 warm-up + {runs} lần đo)...")
    for code in query_codes:
        durations, median_s, row_count = measure_single_query(
            code=code,
            runs=runs,
            catalog=catalog,
            schema=schema,
        )
        results[code] = {
            "query_code": code,
            "mode": mode,
            "runs": durations,
            "median_s": median_s,
            "row_count": row_count,
        }
        print(
            f"  - {code} ({QUERY_SPECS[code].name}): runs={durations} | median={median_s:.4f}s | rows={row_count}"
        )
    return results


def _load_mode_csv(csv_path: str, expected_mode: str) -> Dict[str, Dict[str, Any]]:
    """Đọc kết quả đo một chế độ đã lưu trước đó để ghép thành Bảng kết quả đo tổng hợp."""
    loaded: Dict[str, Dict[str, Any]] = {}
    with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = row.get("query_code", "").strip()
            if code not in QUERY_SPECS:
                continue
            if row.get("mode_key", "").strip() not in ("", expected_mode):
                continue
            runs_str = row.get("runs_seconds", "").strip()
            runs = [float(x.strip()) for x in runs_str.split(";") if x.strip()]
            loaded[code] = {
                "query_code": code,
                "mode": expected_mode,
                "runs": runs,
                "median_s": float(row["median_seconds"]),
                "row_count": int(row.get("row_count", "0") or 0),
            }
    return loaded


def build_report_rows(
    query_codes: Sequence[str],
    without_res: Dict[str, Dict[str, Any]],
    with_res: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Tổng hợp Bảng kết quả đo theo đúng các trường ở Design Serving (Ghi nhận kết quả đo)
    và kiểm tra ngưỡng nghiệm thu (Requirement v3: Hiệu năng truy vấn).
    """
    rows: List[Dict[str, Any]] = []
    all_latency_ok = True
    speedup_pass_count = 0

    for code in query_codes:
        if code in without_res:
            w0 = without_res[code]
            ok0 = w0["median_s"] <= MAX_MEDIAN_WITHOUT_REFLECTION_S
            if not ok0:
                all_latency_ok = False
            rows.append(
                {
                    "query_code": code,
                    "mode_key": MODE_WITHOUT,
                    "mode_label": MODE_LABELS[MODE_WITHOUT],
                    "runs_seconds": "; ".join(f"{t:.4f}" for t in w0["runs"]),
                    "median_seconds": f"{w0['median_s']:.4f}",
                    "speedup_factor": "",
                    "result": "Đạt" if ok0 else "Không đạt",
                    "row_count": w0["row_count"],
                    "summary": "",
                }
            )

        if code in with_res:
            w1 = with_res[code]
            ok1 = w1["median_s"] <= MAX_MEDIAN_WITH_REFLECTION_S
            if not ok1:
                all_latency_ok = False
            speedup_str = ""
            if code in without_res and w1["median_s"] > 0:
                speedup = without_res[code]["median_s"] / w1["median_s"]
                speedup_str = f"{speedup:.2f}"
                if speedup >= MIN_SPEEDUP_FACTOR:
                    speedup_pass_count += 1
            rows.append(
                {
                    "query_code": code,
                    "mode_key": MODE_WITH,
                    "mode_label": MODE_LABELS[MODE_WITH],
                    "runs_seconds": "; ".join(f"{t:.4f}" for t in w1["runs"]),
                    "median_seconds": f"{w1['median_s']:.4f}",
                    "speedup_factor": speedup_str,
                    "result": "Đạt" if ok1 else "Không đạt",
                    "row_count": w1["row_count"],
                    "summary": "",
                }
            )

    has_both = bool(without_res) and bool(with_res)
    speedup_ok = (speedup_pass_count >= MIN_SPEEDUP_PASS_COUNT) if has_both else True
    overall_ok = all_latency_ok and speedup_ok

    summary_text = (
        f"{speedup_pass_count}/{len(query_codes)} truy vấn tăng tốc >= {MIN_SPEEDUP_FACTOR:.0f}x "
        f"(ngưỡng >= {MIN_SPEEDUP_PASS_COUNT}/{len(query_codes)})"
        if has_both
        else "Chế độ đo đơn lẻ (chưa gộp đủ 2 chế độ để tính tăng tốc)"
    )
    rows.append(
        {
            "query_code": "TỔNG HỢP",
            "mode_key": "summary",
            "mode_label": "Hai chế độ" if has_both else "Đơn chế độ",
            "runs_seconds": "",
            "median_seconds": "",
            "speedup_factor": f"{speedup_pass_count}/{len(query_codes)}" if has_both else "",
            "result": "Đạt" if overall_ok else "Không đạt",
            "row_count": "",
            "summary": summary_text,
        }
    )
    return rows, overall_ok


def write_performance_csv(rows: Sequence[Dict[str, Any]], output_path: str) -> None:
    """Ghi Bảng kết quả đo hiệu năng ra tệp CSV."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    fieldnames = [
        "query_code",
        "mode_key",
        "mode_label",
        "runs_seconds",
        "median_seconds",
        "speedup_factor",
        "result",
        "row_count",
        "summary",
    ]
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="B-703: Đo hiệu năng Q1..Q5 trên Dremio qua Arrow Flight SQL."
    )
    parser.add_argument(
        "--mode",
        choices=(MODE_WITHOUT, MODE_WITH, "merge"),
        default=MODE_WITHOUT,
        help=(
            "Chế độ đo: without_reflection (chưa bật Reflection), "
            "with_reflection (đã bật & làm mới Reflection), hoặc merge (gộp 2 tệp CSV)."
        ),
    )
    parser.add_argument(
        "--queries",
        nargs="+",
        default=list(STANDARD_QUERY_CODES),
        choices=list(STANDARD_QUERY_CODES),
        help="Danh sách truy vấn cần đo (mặc định: Q1 Q2 Q3 Q4 Q5).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Số lần chạy đo sau 1 lần làm nóng (mặc định: 5 theo Requirement v3).",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("DREMIO_CATALOG", "hive"),
        help="Tên catalog Dremio (mặc định: hive).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("DREMIO_GOLD_SCHEMA", "gold"),
        help="Tên schema Gold (mặc định: gold).",
    )
    parser.add_argument(
        "--refresh-dremio",
        action="store_true",
        help="Gọi airflow/dags/dremio_refresh.py (B-505) làm mới metadata và Reflection trước khi đo.",
    )
    parser.add_argument(
        "--without-csv",
        default="",
        help="Đường dẫn tệp CSV chế độ không có Reflection (dùng cho --mode merge).",
    )
    parser.add_argument(
        "--with-csv",
        default="",
        help="Đường dẫn tệp CSV chế độ có Reflection (dùng cho --mode merge).",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(CURRENT_DIR, "performance_report.csv"),
        help="Đường dẫn tệp CSV xuất Bảng kết quả đo.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    if args.runs < 1:
        raise ValueError("Tham số --runs phải >= 1.")

    if args.refresh_dremio and args.mode != "merge":
        from airflow.dags.dremio_refresh import refresh_gold_and_reflections

        print("[B-703] Đang gọi B-505 làm mới siêu dữ liệu Gold và Reflection trên Dremio...")
        ref_info = refresh_gold_and_reflections(catalog=args.catalog)
        print(f"[B-703] Làm mới Dremio hoàn tất: {ref_info}")

    without_res: Dict[str, Dict[str, Any]] = {}
    with_res: Dict[str, Dict[str, Any]] = {}

    if args.mode == "merge":
        if not args.without_csv or not args.with_csv:
            raise ValueError("Chế độ --mode merge yêu cầu truyền đủ --without-csv và --with-csv.")
        without_res = _load_mode_csv(args.without_csv, MODE_WITHOUT)
        with_res = _load_mode_csv(args.with_csv, MODE_WITH)
    elif args.mode == MODE_WITHOUT:
        without_res = run_benchmark_mode(
            MODE_WITHOUT, args.queries, args.runs, args.catalog, args.schema
        )
    else:
        with_res = run_benchmark_mode(
            MODE_WITH, args.queries, args.runs, args.catalog, args.schema
        )

    report_rows, overall_ok = build_report_rows(args.queries, without_res, with_res)
    write_performance_csv(report_rows, args.output)
    print(f"[B-703] Đã ghi Bảng kết quả đo tại: {args.output}")
    print(f"[B-703] KẾT LUẬN: {'ĐẠT' if overall_ok else 'KHÔNG ĐẠT'}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())

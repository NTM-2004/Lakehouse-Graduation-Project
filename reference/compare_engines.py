"""
reference/compare_engines.py - So khớp kết quả Q1 đến Q5 giữa Spark và Dremio (B-702)
Tác giả phụ trách: Minh | Reviewer: Kiên
Tham chiếu:
- Design Serving: Mục II (So khớp với Spark; Tiêu chí hòa của Q2)
- Design Build: B-702
- Requirement v3: F13

Quy tắc bắt buộc:
- Chạy cùng 5 truy vấn chuẩn Q1..Q5 (từ B-701) trên Spark và Dremio với cùng bảng Gold.
- Q1, Q3, Q4, Q5: so khớp theo khóa lát cắt (không phụ thuộc thứ tự dòng trả về), số dòng bằng nhau,
  từng giá trị bằng nhau, tỷ lệ/trung bình làm tròn đến 4 chữ số thập phân (RATE_DECIMALS = 4).
- Q2: so theo thứ tự xếp hạng Top 20 với tiêu chí hòa (doanh thu giảm dần, hòa thì seller_id tăng dần).
- Không viết cứng thông tin đăng nhập; đọc từ biến môi trường thông qua streamlit/dremio_client.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.reference_common import (
    RATE_DECIMALS,
    compare_keyed_maps,
    normalize_for_compare,
    sort_q2_rows,
)
from reference.standard_queries import (
    QUERY_SPECS,
    QuerySpec,
    STANDARD_QUERY_CODES,
    get_query_sql,
)


def _to_json_safe(val: Any) -> Any:
    """Chuyển kiểu dữ liệu Spark/Arrow/Pandas sang kiểu JSON cơ bản để so sánh nhất quán."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()[:10]
    if isinstance(val, Decimal):
        return float(val)
    if hasattr(val, "item") and callable(val.item):
        py_val = val.item()
        if isinstance(py_val, float) and math.isnan(py_val):
            return None
        return py_val
    return val


def _run_dremio_queries(
    query_codes: Sequence[str],
    catalog: str,
    schema: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Thực thi Q1..Q5 trên Dremio qua Arrow Flight SQL (streamlit/dremio_client.py)."""
    from streamlit.dremio_client import query_sql

    results: Dict[str, List[Dict[str, Any]]] = {}
    for code in query_codes:
        sql = get_query_sql(code, dialect="dremio", catalog=catalog, schema=schema)
        df = query_sql(sql)
        rows: List[Dict[str, Any]] = []
        for record in df.to_dict(orient="records"):
            rows.append({k: _to_json_safe(v) for k, v in record.items()})
        results[code] = rows
    return results


def _run_spark_queries_local(
    query_codes: Sequence[str],
    catalog: str,
    schema: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Chạy Q1..Q5 trực tiếp bằng PySpark cục bộ (khi chạy bên trong container Spark)."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("compare_engines_b702").getOrCreate()
    try:
        results: Dict[str, List[Dict[str, Any]]] = {}
        for code in query_codes:
            sql = get_query_sql(code, dialect="spark", catalog=catalog, schema=schema)
            df = spark.sql(sql)
            results[code] = [
                {k: _to_json_safe(v) for k, v in row.asDict(recursive=True).items()}
                for row in df.collect()
            ]
        return results
    finally:
        spark.stop()


def _run_spark_queries_docker(
    query_codes: Sequence[str],
    catalog: str,
    schema: str,
    spark_container: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Chạy Q1..Q5 bên trong container Spark đang hoạt động thông qua docker exec
    và trả về dữ liệu JSON chuẩn hóa.
    """
    sql_map = {
        code: get_query_sql(code, dialect="spark", catalog=catalog, schema=schema)
        for code in query_codes
    }
    inline_script = (
        "import json, math\n"
        "from datetime import date, datetime\n"
        "from decimal import Decimal\n"
        "from pyspark.sql import SparkSession\n"
        f"sql_map = {json.dumps(sql_map)}\n"
        "def safe(v):\n"
        "    if v is None: return None\n"
        "    if isinstance(v, float) and math.isnan(v): return None\n"
        "    if isinstance(v, (datetime, date)): return v.isoformat()[:10]\n"
        "    if isinstance(v, Decimal): return float(v)\n"
        "    return v\n"
        "spark = SparkSession.builder.appName('compare_engines_b702').getOrCreate()\n"
        "out = {}\n"
        "try:\n"
        "    for code, sql in sql_map.items():\n"
        "        rows = spark.sql(sql).collect()\n"
        "        out[code] = [{k: safe(v) for k, v in r.asDict(recursive=True).items()} for r in rows]\n"
        "finally:\n"
        "    spark.stop()\n"
        "print('__COMPARE_JSON_BEGIN__')\n"
        "print(json.dumps(out, ensure_ascii=False))\n"
        "print('__COMPARE_JSON_END__')\n"
    )
    proc = subprocess.run(
        ["docker", "exec", spark_container, "python3", "-c", inline_script],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Lỗi khi chạy Spark trong container '{spark_container}' (exit={proc.returncode}):\n"
            f"{proc.stderr}"
        )

    stdout = proc.stdout
    begin_marker = "__COMPARE_JSON_BEGIN__"
    end_marker = "__COMPARE_JSON_END__"
    if begin_marker not in stdout or end_marker not in stdout:
        raise RuntimeError("Không trích xuất được kết quả JSON từ container Spark.")

    json_payload = stdout.split(begin_marker, 1)[1].split(end_marker, 1)[0].strip()
    return json.loads(json_payload)


def _rows_to_keyed_map(
    rows: Sequence[Dict[str, Any]],
    spec: QuerySpec,
) -> Dict[Tuple[str, ...], Any]:
    """Biến đổi danh sách dòng sang map {(key..., col): value} để so khớp không phụ thuộc thứ tự."""
    keyed: Dict[Tuple[str, ...], Any] = {}
    for row in rows:
        slice_key = tuple(str(_to_json_safe(row.get(k))) for k in spec.key_columns)
        for col in spec.value_columns:
            keyed[slice_key + (col,)] = _to_json_safe(row.get(col))
    return keyed


def compare_query_results(
    spec: QuerySpec,
    spark_rows: Sequence[Dict[str, Any]],
    dremio_rows: Sequence[Dict[str, Any]],
    decimals: int = RATE_DECIMALS,
) -> Tuple[bool, List[str]]:
    """So khớp kết quả của một truy vấn chuẩn giữa Spark và Dremio."""
    diffs: List[str] = []
    if len(spark_rows) != len(dremio_rows):
        diffs.append(f"Lệch số dòng: Spark={len(spark_rows)}, Dremio={len(dremio_rows)}")

    if spec.is_ranked:
        spark_sorted = sort_q2_rows(spark_rows)
        dremio_sorted = sort_q2_rows(dremio_rows)
        limit = min(len(spark_sorted), len(dremio_sorted))
        for idx in range(limit):
            s_row = spark_sorted[idx]
            d_row = dremio_sorted[idx]
            for col in spec.key_columns + spec.value_columns:
                is_fl = col in spec.float_columns
                s_val = normalize_for_compare(s_row.get(col), is_float=is_fl, decimals=decimals)
                d_val = normalize_for_compare(d_row.get(col), is_float=is_fl, decimals=decimals)
                if s_val != d_val:
                    diffs.append(
                        f"Lệch hạng #{idx + 1} cột '{col}': Spark={s_val}, Dremio={d_val}"
                    )
        return len(diffs) == 0, diffs

    spark_map = _rows_to_keyed_map(spark_rows, spec)
    dremio_map = _rows_to_keyed_map(dremio_rows, spec)
    ok, map_diffs = compare_keyed_maps(
        expected=spark_map,
        actual=dremio_map,
        float_keys=lambda k: k[-1] in spec.float_columns,
        decimals=decimals,
    )
    diffs.extend(map_diffs)
    return ok and len(diffs) == 0, diffs


def write_comparison_report(
    report_rows: Sequence[Dict[str, Any]],
    output_path: str,
) -> None:
    """Ghi Bảng so khớp Spark và Dremio (F13) ra tệp CSV."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    fieldnames = [
        "query_code",
        "query_name",
        "spark_rows",
        "dremio_rows",
        "status",
        "diff_count",
        "sample_diffs",
    ]
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in report_rows:
            writer.writerow(row)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="B-702: So khớp truy vấn chuẩn Q1..Q5 giữa Spark và Dremio (F13)."
    )
    parser.add_argument(
        "--queries",
        nargs="+",
        default=list(STANDARD_QUERY_CODES),
        choices=list(STANDARD_QUERY_CODES),
        help="Danh sách mã truy vấn cần so khớp (mặc định: Q1 Q2 Q3 Q4 Q5).",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("DREMIO_CATALOG", "hive"),
        help="Tên catalog Iceberg/Hive chung (mặc định: hive).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("DREMIO_GOLD_SCHEMA", "gold"),
        help="Tên schema tầng Gold (mặc định: gold).",
    )
    parser.add_argument(
        "--spark-mode",
        choices=("auto", "local", "docker", "json"),
        default="auto",
        help="Cách lấy kết quả từ Spark: auto (PySpark -> docker), local, docker hoặc đọc tệp json.",
    )
    parser.add_argument(
        "--spark-container",
        default=os.getenv("SPARK_CONTAINER_NAME", "spark"),
        help="Tên container Spark khi dùng --spark-mode docker (mặc định: spark).",
    )
    parser.add_argument(
        "--spark-json",
        default="",
        help="Đường dẫn tệp JSON chứa kết quả Spark (khi dùng --spark-mode json).",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(CURRENT_DIR, "compare_engines_report.csv"),
        help="Đường dẫn tệp CSV ghi Bảng so khớp.",
    )
    return parser.parse_args(argv)


def _load_spark_results(args: argparse.Namespace) -> Dict[str, List[Dict[str, Any]]]:
    mode = args.spark_mode
    if mode == "json":
        if not args.spark_json or not os.path.isfile(args.spark_json):
            raise FileNotFoundError(f"Không tìm thấy tệp kết quả Spark JSON: '{args.spark_json}'")
        with open(args.spark_json, mode="r", encoding="utf-8") as f:
            return json.load(f)

    if mode == "auto":
        try:
            import pyspark  # noqa: F401
            mode = "local"
        except ImportError:
            mode = "docker"

    if mode == "local":
        return _run_spark_queries_local(args.queries, args.catalog, args.schema)
    return _run_spark_queries_docker(args.queries, args.catalog, args.schema, args.spark_container)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    print(f"[B-702] Bắt đầu so khớp {args.queries} giữa Spark và Dremio...")

    spark_results = _load_spark_results(args)
    dremio_results = _run_dremio_queries(args.queries, args.catalog, args.schema)

    report_rows: List[Dict[str, Any]] = []
    all_passed = True

    for code in args.queries:
        spec = QUERY_SPECS[code]
        s_rows = spark_results.get(code, [])
        d_rows = dremio_results.get(code, [])
        ok, diffs = compare_query_results(spec, s_rows, d_rows, decimals=RATE_DECIMALS)
        status = "ĐẠT" if ok else "KHÔNG ĐẠT"
        if not ok:
            all_passed = False

        print(
            f"  - {code} ({spec.name}): Spark={len(s_rows)} dòng | Dremio={len(d_rows)} dòng -> {status}"
        )
        for msg in diffs[:5]:
            print(f"      * {msg}")

        report_rows.append(
            {
                "query_code": code,
                "query_name": spec.name,
                "spark_rows": len(s_rows),
                "dremio_rows": len(d_rows),
                "status": status,
                "diff_count": len(diffs),
                "sample_diffs": " ; ".join(diffs[:5]),
            }
        )

    write_comparison_report(report_rows, args.output)
    print(f"[B-702] Đã ghi Bảng so khớp tại: {args.output}")
    print(f"[B-702] KẾT LUẬN TỔNG: {'ĐẠT' if all_passed else 'KHÔNG ĐẠT'}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

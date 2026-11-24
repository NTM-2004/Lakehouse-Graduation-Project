"""
reference/reference_uc04.py - Tính giá trị tham chiếu KPI UC-04 trực tiếp từ Raw Clickstream (B-435)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Mẫu triển khai: M-12
Tham chiếu:
- Requirement v3 (Quy ước Ngày của phiên; UC-04; F09)
- Design Gold v2 (fact_session, kpi_funnel_day, kpi_funnel_month, phễu chặt)
- Design Deployment (Mục III: Giá trị tham chiếu - độc lập với Bronze, Silver, Gold)
"""

import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.reference_common import BATCH_ORDER, safe_div

LOG_REGEX = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[INFO\] - "
    r"USER_ID: ([^,]+), SESSION_ID: ([^,]+), ORDER_ID: ([^,]+), "
    r"EVENT_TYPE: ([^,]+), PRODUCT_ID: ([^,]+), AMOUNT: ([^,]+), OUTCOME: (.+)$"
)


def compute_uc04_reference(raw_clickstream_root: str) -> List[Dict[str, Any]]:
    """
    Đọc trực tiếp 4 batch Raw Clickstream (access_log.txt), gộp dòng trùng nguyên văn (F09),
    gom phiên trên toàn bảng và tính giá trị tham chiếu cho kpi_funnel_day và kpi_funnel_month.
    """
    seen_raw_lines: Set[str] = set()
    # session_id -> {min_ts, max_ts, n_events, types: set}
    sessions: Dict[str, Dict[str, Any]] = {}

    for batch_id in BATCH_ORDER:
        path = os.path.join(raw_clickstream_root, batch_id, "access_log.txt")
        if not os.path.exists(path):
            continue
        with open(path, mode="r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\r\n")
                if not line or line in seen_raw_lines:
                    continue
                m = LOG_REGEX.match(line)
                if not m:
                    continue
                seen_raw_lines.add(line)

                ts_str, _user_id, session_id, _order_id, event_type, _prod_id, _amount, _outcome = m.groups()
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                sid = session_id.strip()
                etype = event_type.strip()

                st = sessions.get(sid)
                if st is None:
                    sessions[sid] = {
                        "min_ts": ts,
                        "max_ts": ts,
                        "n_events": 1,
                        "types": {etype},
                    }
                else:
                    if ts < st["min_ts"]:
                        st["min_ts"] = ts
                    if ts > st["max_ts"]:
                        st["max_ts"] = ts
                    st["n_events"] += 1
                    st["types"].add(etype)

    def _new_bucket() -> Dict[str, int]:
        return {
            "n_sessions": 0,
            "n_reach_home": 0,
            "n_reach_view": 0,
            "n_reach_cart": 0,
            "n_reach_purchase": 0,
            "n_purchase_sessions": 0,
            "n_cart_sessions": 0,
            "n_cart_abandoned_sessions": 0,
            "n_sessions_ge2": 0,
            "sum_duration_seconds": 0,
        }

    by_day: Dict[str, Dict[str, int]] = defaultdict(_new_bucket)
    by_month: Dict[str, Dict[str, int]] = defaultdict(_new_bucket)

    for st in sessions.values():
        first_ts: datetime = st["min_ts"]
        last_ts: datetime = st["max_ts"]
        day_key = first_ts.strftime("%Y-%m-%d")
        month_key = first_ts.strftime("%Y-%m")

        types: Set[str] = st["types"]
        has_home = "home" in types
        has_view = "product_view" in types
        has_cart = "add_to_cart" in types
        has_purchase = "purchase" in types
        duration_sec = int((last_ts - first_ts).total_seconds())
        n_events = st["n_events"]

        for b in (by_day[day_key], by_month[month_key]):
            b["n_sessions"] += 1
            if has_home:
                b["n_reach_home"] += 1
            if has_home and has_view:
                b["n_reach_view"] += 1
            if has_home and has_view and has_cart:
                b["n_reach_cart"] += 1
            if has_home and has_view and has_cart and has_purchase:
                b["n_reach_purchase"] += 1
            if has_purchase:
                b["n_purchase_sessions"] += 1
            if has_cart:
                b["n_cart_sessions"] += 1
                if not has_purchase:
                    b["n_cart_abandoned_sessions"] += 1
            if n_events >= 2:
                b["n_sessions_ge2"] += 1
                b["sum_duration_seconds"] += duration_sec

    out_rows: List[Dict[str, Any]] = []

    def _emit_table(table_name: str, buckets: Dict[str, Dict[str, int]]) -> None:
        for slice_key, b in sorted(buckets.items()):
            metrics = {
                "n_sessions": b["n_sessions"],
                "n_reach_home": b["n_reach_home"],
                "n_reach_view": b["n_reach_view"],
                "n_reach_cart": b["n_reach_cart"],
                "n_reach_purchase": b["n_reach_purchase"],
                "n_purchase_sessions": b["n_purchase_sessions"],
                "n_cart_sessions": b["n_cart_sessions"],
                "n_cart_abandoned_sessions": b["n_cart_abandoned_sessions"],
                "n_sessions_ge2": b["n_sessions_ge2"],
                "sum_duration_seconds": b["sum_duration_seconds"],
                "conversion_rate": safe_div(b["n_purchase_sessions"], b["n_sessions"]),
                "abandon_rate": safe_div(b["n_cart_abandoned_sessions"], b["n_cart_sessions"]),
                "avg_duration_seconds": safe_div(b["sum_duration_seconds"], b["n_sessions_ge2"]),
                "step_rate_home_view": safe_div(b["n_reach_view"], b["n_reach_home"]),
                "step_rate_view_cart": safe_div(b["n_reach_cart"], b["n_reach_view"]),
                "step_rate_cart_purchase": safe_div(b["n_reach_purchase"], b["n_reach_cart"]),
            }
            for kpi_name, val in metrics.items():
                out_rows.append(
                    {
                        "table_name": table_name,
                        "slice_key": slice_key,
                        "kpi_name": kpi_name,
                        "value": val,
                    }
                )

    _emit_table("kpi_funnel_day", by_day)
    _emit_table("kpi_funnel_month", by_month)
    return out_rows

"""
reference/reference_uc03.py - Tính giá trị tham chiếu KPI UC-03 trực tiếp từ Raw Marketing JSONL (B-434)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Mẫu triển khai: M-12
Tham chiếu:
- Requirement v3 (UC-03)
- Design Bronze, Silver v2 (silver.leads, cờ C1)
- Design Gold v2 (kpi_lead_month, kpi_lead_origin)
- Design Deployment (Mục III: Giá trị tham chiếu - độc lập với Bronze, Silver, Gold)
"""

import os
import sys
from collections import defaultdict
from typing import Any, Dict, List

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.reference_common import (
    calendar_days_diff,
    parse_date,
    read_raw_marketing_latest,
    safe_div,
)


def compute_uc03_reference(raw_marketing_root: str) -> List[Dict[str, Any]]:
    """
    Đọc trực tiếp 4 batch Raw Marketing JSONL (bỏ qua _excluded/),
    chọn bản chụp muộn nhất theo mql_id và tính giá trị tham chiếu cho
    kpi_lead_month và kpi_lead_origin.
    """
    leads = read_raw_marketing_latest(raw_marketing_root)

    def _new_bucket() -> Dict[str, int]:
        return {
            "n_leads": 0,
            "n_leads_with_deal": 0,
            "n_close_time_valid_deals": 0,
            "sum_days_to_close": 0,
            "n_leads_not_excluded": 0,
            "n_won_30d": 0,
        }

    by_month: Dict[str, Dict[str, int]] = defaultdict(_new_bucket)
    by_origin: Dict[str, Dict[str, int]] = defaultdict(_new_bucket)

    for rec in leads.values():
        fcd_str = rec.get("first_contact_date")
        fcd = parse_date(fcd_str)
        if fcd is None:
            continue
        ym = f"{fcd.year:04d}-{fcd.month:02d}"
        origin = (rec.get("origin") or "").strip().lower() or "unknown"

        deal = rec.get("deal")
        has_deal = isinstance(deal, dict)
        won_str = deal.get("won_date") if has_deal else None
        days_to_close = calendar_days_diff(won_str, fcd_str) if (has_deal and won_str) else None
        is_c1 = has_deal and days_to_close is not None and days_to_close < 0

        for b in (by_month[ym], by_origin[origin]):
            b["n_leads"] += 1
            if has_deal:
                b["n_leads_with_deal"] += 1
            if not is_c1:
                b["n_leads_not_excluded"] += 1
                if has_deal and days_to_close is not None:
                    b["n_close_time_valid_deals"] += 1
                    b["sum_days_to_close"] += days_to_close
                    if days_to_close <= 30:
                        b["n_won_30d"] += 1

    out_rows: List[Dict[str, Any]] = []

    def _emit_table(table_name: str, buckets: Dict[str, Dict[str, int]]) -> None:
        for slice_key, b in sorted(buckets.items()):
            metrics = {
                "n_leads": b["n_leads"],
                "n_leads_with_deal": b["n_leads_with_deal"],
                "n_close_time_valid_deals": b["n_close_time_valid_deals"],
                "sum_days_to_close": b["sum_days_to_close"],
                "n_leads_not_excluded": b["n_leads_not_excluded"],
                "n_won_30d": b["n_won_30d"],
                "lead_to_deal_rate": safe_div(b["n_leads_with_deal"], b["n_leads"]),
                "avg_days_to_close": safe_div(b["sum_days_to_close"], b["n_close_time_valid_deals"]),
                "won_30d_rate": safe_div(b["n_won_30d"], b["n_leads_not_excluded"]),
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

    _emit_table("kpi_lead_month", by_month)
    _emit_table("kpi_lead_origin", by_origin)
    return out_rows

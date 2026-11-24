"""
reference/reference_uc01_uc02.py - Tính giá trị tham chiếu KPI UC-01 và UC-02 trực tiếp từ Raw CSV (B-433)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Mẫu triển khai: M-12
Tham chiếu:
- Requirement v3 (Quy ước chung: Đơn hợp lệ, Đơn hủy, Đơn đã giao, Đơn quá hạn chưa giao, Đánh giá; UC-01, UC-02)
- Design Bronze, Silver v2 (Bảng tra cờ D1, D2, D4, R2)
- Design Gold v2 (kpi_sales_month, kpi_sales_state_month, kpi_delivery_month, kpi_delivery_state_month)
- Design Deployment (Mục III: Giá trị tham chiếu - độc lập với Bronze, Silver, Gold)
"""

import os
import sys
from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from reference.reference_common import (
    CUTOFF_DATE,
    SMALL_MONTH_ORDERS,
    all_dim_months,
    calendar_days_diff,
    parse_date,
    parse_timestamp,
    read_raw_csv_all,
    read_raw_csv_latest,
    safe_div,
)


def compute_uc01_uc02_reference(raw_csv_root: str) -> List[Dict[str, Any]]:
    """
    Đọc trực tiếp Raw CSV (4 batch), chọn bản chụp muộn nhất cho orders và order_reviews,
    và tính đầy đủ các chỉ số tham chiếu cho 4 bảng KPI UC-01 & UC-02.
    """
    orders = read_raw_csv_latest(
        raw_csv_root,
        "olist_orders_dataset.csv",
        key_fn=lambda r: (r["order_id"].strip(),),
    )
    customers = read_raw_csv_latest(
        raw_csv_root,
        "olist_customers_dataset.csv",
        key_fn=lambda r: (r["customer_id"].strip(),),
    )
    items_raw = read_raw_csv_all(raw_csv_root, "olist_order_items_dataset.csv")
    reviews = read_raw_csv_latest(
        raw_csv_root,
        "olist_order_reviews_dataset.csv",
        key_fn=lambda r: (r["review_id"].strip(), r["order_id"].strip()),
    )

    # Gộp dòng hàng theo order_id (loại dòng giá <= 0 theo quy ước Đơn hợp lệ)
    items_by_order: Dict[str, Tuple[int, Decimal]] = {}
    for it in items_raw:
        oid = it["order_id"].strip()
        try:
            price = Decimal(it["price"].strip())
        except Exception:
            continue
        if price <= 0:
            continue
        cnt, total = items_by_order.get(oid, (0, Decimal("0")))
        items_by_order[oid] = (cnt + 1, total + price)

    # Chọn 1 đánh giá hợp lệ mới nhất cho mỗi đơn (lọc trước, chọn sau theo Requirement Đánh giá)
    # Loại: chưa trả lời (review_score hoặc review_answer_timestamp trống) hoặc R2 (tạo trước ngày mua)
    reviews_by_order: Dict[str, List[Tuple[str, str, str, int]]] = defaultdict(list)
    for (_, oid), rev in reviews.items():
        if (oid,) not in orders:
            continue
        score_str = (rev.get("review_score") or "").strip()
        ans_str = (rev.get("review_answer_timestamp") or "").strip()
        cre_str = (rev.get("review_creation_date") or "").strip()
        if not score_str or not ans_str:
            continue
        o_purchase_dt = parse_date(orders[(oid,)].get("order_purchase_timestamp"))
        r_cre_dt = parse_date(cre_str)
        # Cờ R2: ngày tạo đánh giá sớm hơn ngày mua (so theo ngày)
        if o_purchase_dt is not None and r_cre_dt is not None and r_cre_dt < o_purchase_dt:
            continue
        try:
            score = int(float(score_str))
        except ValueError:
            continue
        # Thứ tự chọn giảm dần: review_answer_timestamp, review_creation_date, review_id
        reviews_by_order[oid].append((ans_str, cre_str[:10], rev["review_id"].strip(), score))

    chosen_score: Dict[str, int] = {}
    for oid, candidates in reviews_by_order.items():
        candidates.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        chosen_score[oid] = candidates[0][3]

    def _new_sales_bucket() -> Dict[str, Any]:
        return {
            "n_orders_total": 0,
            "n_canceled_orders": 0,
            "n_valid_orders": 0,
            "sum_revenue": Decimal("0"),
        }

    def _new_deliv_bucket() -> Dict[str, Any]:
        return {
            "n_valid_orders": 0,
            "n_delivered": 0,
            "sum_delivery_days": 0,
            "n_late": 0,
            "n_on_time": 0,
            "n_shipping_measured": 0,
            "sum_shipping_days": 0,
            "n_scored": 0,
            "sum_score": 0,
            "n_scored_on_time": 0,
            "sum_score_on_time": 0,
            "n_scored_late": 0,
            "sum_score_late": 0,
            "n_overdue_undelivered": 0,
        }

    sales_month = {ym: _new_sales_bucket() for ym in all_dim_months()}
    deliv_month = {ym: _new_deliv_bucket() for ym in all_dim_months()}
    sales_state_month: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(_new_sales_bucket)
    deliv_state_month: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(_new_deliv_bucket)

    for (oid,), o in orders.items():
        status = (o.get("order_status") or "").strip()
        p_ts = o.get("order_purchase_timestamp")
        c_ts = o.get("order_delivered_carrier_date")
        d_ts = o.get("order_delivered_customer_date")
        e_ts = o.get("order_estimated_delivery_date")

        p_date = parse_date(p_ts)
        if p_date is None:
            continue
        ym = f"{p_date.year:04d}-{p_date.month:02d}"
        cid = (o.get("customer_id") or "").strip()
        state = (customers.get((cid,), {}).get("customer_state") or "").strip()

        n_items, revenue = items_by_order.get(oid, (0, Decimal("0")))
        is_canceled = status in ("canceled", "unavailable")
        is_valid = (not is_canceled) and (n_items > 0)

        # Cờ D1, D2, D4 cho Đơn đã giao
        d_dt = parse_timestamp(d_ts)
        c_dt = parse_timestamp(c_ts)
        flag_d1 = status == "delivered" and d_dt is None
        flag_d2 = status != "delivered" and d_dt is not None
        flag_d4 = d_dt is not None and c_dt is not None and d_dt < c_dt
        is_delivered = status == "delivered" and d_dt is not None and not (flag_d1 or flag_d2 or flag_d4)

        deliv_days = calendar_days_diff(d_ts, p_ts) if is_delivered else None
        ship_days = calendar_days_diff(d_ts, c_ts) if (is_delivered and c_dt is not None) else None
        d_date = parse_date(d_ts)
        e_date = parse_date(e_ts)
        is_late = (d_date > e_date) if (is_delivered and d_date is not None and e_date is not None) else None
        is_overdue_undeliv = (
            status == "shipped"
            and d_dt is None
            and e_date is not None
            and e_date <= CUTOFF_DATE
        )
        score = chosen_score.get(oid)

        for bucket in (sales_month[ym], sales_state_month[(state, ym)]):
            bucket["n_orders_total"] += 1
            if is_canceled:
                bucket["n_canceled_orders"] += 1
            if is_valid:
                bucket["n_valid_orders"] += 1
                bucket["sum_revenue"] += revenue

        for bucket in (deliv_month[ym], deliv_state_month[(state, ym)]):
            if is_valid:
                bucket["n_valid_orders"] += 1
            if is_delivered:
                bucket["n_delivered"] += 1
                if deliv_days is not None:
                    bucket["sum_delivery_days"] += deliv_days
                if is_late:
                    bucket["n_late"] += 1
                else:
                    bucket["n_on_time"] += 1
                if ship_days is not None:
                    bucket["n_shipping_measured"] += 1
                    bucket["sum_shipping_days"] += ship_days
            if score is not None:
                bucket["n_scored"] += 1
                bucket["sum_score"] += score
                if is_delivered and is_late is not None:
                    if is_late:
                        bucket["n_scored_late"] += 1
                        bucket["sum_score_late"] += score
                    else:
                        bucket["n_scored_on_time"] += 1
                        bucket["sum_score_on_time"] += score
            if is_overdue_undeliv:
                bucket["n_overdue_undelivered"] += 1

    out_rows: List[Dict[str, Any]] = []

    def _emit(table_name: str, slice_key: str, metrics: Dict[str, Any]) -> None:
        for kpi_name, val in metrics.items():
            out_rows.append(
                {
                    "table_name": table_name,
                    "slice_key": slice_key,
                    "kpi_name": kpi_name,
                    "value": val,
                }
            )

    # 1. kpi_sales_month (24 tháng)
    prev_valid: Optional[int] = None
    for ym in all_dim_months():
        b = sales_month[ym]
        growth = (
            safe_div(b["n_valid_orders"] - prev_valid, prev_valid)
            if (prev_valid is not None and prev_valid >= SMALL_MONTH_ORDERS)
            else None
        )
        _emit(
            "kpi_sales_month",
            ym,
            {
                "n_orders_total": b["n_orders_total"],
                "n_canceled_orders": b["n_canceled_orders"],
                "n_valid_orders": b["n_valid_orders"],
                "sum_revenue": b["sum_revenue"],
                "avg_order_value": safe_div(float(b["sum_revenue"]), b["n_valid_orders"]),
                "cancel_rate": safe_div(b["n_canceled_orders"], b["n_orders_total"]),
                "order_growth": growth,
            },
        )
        prev_valid = b["n_valid_orders"]

    # 2. kpi_sales_state_month
    for (state, ym), b in sorted(sales_state_month.items()):
        _emit(
            "kpi_sales_state_month",
            f"{state}|{ym}",
            {
                "n_orders_total": b["n_orders_total"],
                "n_canceled_orders": b["n_canceled_orders"],
                "n_valid_orders": b["n_valid_orders"],
                "sum_revenue": b["sum_revenue"],
                "avg_order_value": safe_div(float(b["sum_revenue"]), b["n_valid_orders"]),
                "cancel_rate": safe_div(b["n_canceled_orders"], b["n_orders_total"]),
            },
        )

    def _deliv_metrics(b: Dict[str, Any], include_valid: bool) -> Dict[str, Any]:
        avg_on_time = safe_div(b["sum_score_on_time"], b["n_scored_on_time"])
        avg_late = safe_div(b["sum_score_late"], b["n_scored_late"])
        score_gap = (avg_on_time - avg_late) if (avg_on_time is not None and avg_late is not None) else None
        m: Dict[str, Any] = {}
        if include_valid:
            m["n_valid_orders"] = b["n_valid_orders"]
        m.update(
            {
                "n_delivered": b["n_delivered"],
                "sum_delivery_days": b["sum_delivery_days"],
                "n_late": b["n_late"],
                "n_on_time": b["n_on_time"],
                "n_shipping_measured": b["n_shipping_measured"],
                "sum_shipping_days": b["sum_shipping_days"],
                "n_scored": b["n_scored"],
                "sum_score": b["sum_score"],
                "n_scored_on_time": b["n_scored_on_time"],
                "sum_score_on_time": b["sum_score_on_time"],
                "n_scored_late": b["n_scored_late"],
                "sum_score_late": b["sum_score_late"],
                "n_overdue_undelivered": b["n_overdue_undelivered"],
                "avg_delivery_days": safe_div(b["sum_delivery_days"], b["n_delivered"]),
                "late_rate": safe_div(b["n_late"], b["n_delivered"]),
                "avg_score": safe_div(b["sum_score"], b["n_scored"]),
                "avg_shipping_days": safe_div(b["sum_shipping_days"], b["n_shipping_measured"]),
                "score_gap": score_gap,
            }
        )
        return m

    # 3. kpi_delivery_month (24 tháng)
    for ym in all_dim_months():
        _emit("kpi_delivery_month", ym, _deliv_metrics(deliv_month[ym], include_valid=True))

    # 4. kpi_delivery_state_month
    for (state, ym), b in sorted(deliv_state_month.items()):
        _emit("kpi_delivery_state_month", f"{state}|{ym}", _deliv_metrics(b, include_valid=False))

    return out_rows

"""
streamlit/display_rules.py - Quy tắc hiển thị KPI, ghi chú, cờ mẫu nhỏ và giá trị rỗng (B-603)
Tác giả phụ trách: Minh | Reviewer: Kiên
Tham chiếu:
- Design Serving: Mục III (Quy tắc hiển thị: Ghi chú giới hạn, Cờ mẫu nhỏ, Không xếp hạng, Giá trị rỗng)
- Design Build: B-603 (đọc B-602: streamlit/kpi_notes.csv)
- Requirement v3: F15, Mẫu nhỏ
"""

from __future__ import annotations

import csv
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
import streamlit as st

NULL_LABEL = "— (không đủ dữ liệu)"
SMALL_SAMPLE_BADGE = "⚠️ Mẫu nhỏ"


def _default_notes_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "kpi_notes.csv")


def load_kpi_notes(csv_path: Optional[str] = None) -> List[Dict[str, str]]:
    """Đọc toàn bộ danh sách ghi chú giới hạn KPI từ streamlit/kpi_notes.csv."""
    path = csv_path or _default_notes_path()
    if not os.path.exists(path):
        return []

    rows: List[Dict[str, str]] = []
    with open(path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "uc": (row.get("uc") or "").strip(),
                    "kpi_name": (row.get("kpi_name") or "").strip(),
                    "column_name": (row.get("column_name") or "").strip(),
                    "note": (row.get("note") or "").strip(),
                }
            )
    return rows


def get_kpi_note(kpi_name: str, csv_path: Optional[str] = None) -> str:
    """Tra cứu ghi chú giới hạn theo tên KPI; trả về chuỗi rỗng nếu ô Lưu ý trống."""
    target = kpi_name.strip()
    for row in load_kpi_notes(csv_path):
        if row["kpi_name"] == target:
            return row["note"]
    return ""


def get_notes_by_uc(uc_list: Iterable[str], csv_path: Optional[str] = None) -> List[Dict[str, str]]:
    """Lọc danh sách ghi chú KPI theo một hoặc nhiều mã UC (ví dụ ['UC-04'])."""
    wanted = {u.strip().upper() for u in uc_list}
    return [row for row in load_kpi_notes(csv_path) if row["uc"].upper() in wanted]


def is_missing(value: Any) -> bool:
    """Kiểm tra giá trị NULL / NaN / None."""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def format_kpi_value(
    value: Any,
    kind: str = "number",
    decimals: int = 2,
    is_small: bool = False,
) -> str:
    """
    Định dạng giá trị KPI tuân thủ Quy tắc hiển thị (Design Serving):
    - Tỷ lệ / trung bình (rate, number, currency, seconds) bị NULL -> '— (không đủ dữ liệu)', không hiển thị 0.
    - Cột đếm / tổng (count, sum) bằng 0 -> hiển thị 0.
    - Dòng có cờ mẫu nhỏ bật -> gắn nhãn cảnh báo '⚠️ Mẫu nhỏ' cạnh giá trị.
    """
    if kind in ("count", "sum"):
        if is_missing(value):
            formatted = "0" if kind == "count" else NULL_LABEL
        else:
            formatted = f"{int(round(float(value))):,}" if kind == "count" else f"{float(value):,.{decimals}f}"
    else:
        if is_missing(value):
            formatted = NULL_LABEL
        elif kind == "rate":
            formatted = f"{float(value) * 100:.{decimals}f}%"
        elif kind == "currency":
            formatted = f"{float(value):,.{decimals}f}"
        elif kind == "seconds":
            formatted = f"{float(value):,.{decimals}f}s"
        else:
            formatted = f"{float(value):,.{decimals}f}"

    if is_small:
        return f"{formatted} {SMALL_SAMPLE_BADGE}"
    return formatted


def should_flag_small_sample(row: pd.Series, column_name: str = "") -> bool:
    """
    Xác định có hiển thị nhãn mẫu nhỏ cho KPI trên một dòng hay không (Design Serving R51):
    - Mọi KPI bật nhãn khi is_small_sample = True.
    - Riêng KPI 'score_gap' (Chênh lệch điểm do giao trễ) bật nhãn cả khi is_small_gap_group = True.
    """
    base_flag = bool(row.get("is_small_sample", False)) if not is_missing(row.get("is_small_sample")) else False
    if column_name == "score_gap":
        gap_flag = bool(row.get("is_small_gap_group", False)) if not is_missing(row.get("is_small_gap_group")) else False
        return base_flag or gap_flag
    return base_flag


def format_display_dataframe(
    df: pd.DataFrame,
    slice_cols: Sequence[str],
    kpi_columns: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    """
    Tạo DataFrame hiển thị theo đúng Quy tắc hiển thị:
    - Sắp xếp cố định theo khóa lát cắt (slice_cols), KHÔNG xếp hạng theo giá trị KPI.
    - Gắn nhãn '⚠️ Mẫu nhỏ' cạnh mọi KPI của dòng có cờ mẫu nhỏ bật.
    - Giá trị tỷ lệ/trung bình NULL hiển thị '— (không đủ dữ liệu)'.

    kpi_columns: {
        'col_name': {'label': 'Tên hiển thị', 'kind': 'rate'|'number'|'count'|'currency'|'seconds', 'decimals': 2}
    }
    """
    if df.empty:
        return pd.DataFrame()

    sorted_df = df.sort_values(by=list(slice_cols), ascending=True).reset_index(drop=True)
    out = pd.DataFrame()

    for col in slice_cols:
        out[col] = sorted_df[col].astype(str)

    if "n_obs" in sorted_df.columns:
        out["Số quan sát (n_obs)"] = sorted_df["n_obs"].apply(
            lambda v: "0" if is_missing(v) else f"{int(v):,}"
        )

    for col_name, spec in kpi_columns.items():
        if col_name not in sorted_df.columns:
            continue
        label = spec.get("label", col_name)
        kind = spec.get("kind", "number")
        decimals = int(spec.get("decimals", 2))
        out[label] = sorted_df.apply(
            lambda r: format_kpi_value(
                r[col_name],
                kind=kind,
                decimals=decimals,
                is_small=should_flag_small_sample(r, column_name=col_name),
            ),
            axis=1,
        )

    if "is_small_sample" in sorted_df.columns:
        out["Cảnh báo mẫu nhỏ"] = sorted_df["is_small_sample"].apply(
            lambda flag: SMALL_SAMPLE_BADGE if bool(flag) else ""
        )

    return out


def render_kpi_notes_panel(uc_list: Iterable[str], title: str = "Ghi chú giới hạn các KPI") -> None:
    """
    Hiển thị bảng ghi chú giới hạn từ Requirement v3 (cột Lưu ý) ngay trên trang dashboard.
    Theo Design Serving R50: KPI có ô Lưu ý trống thì không hiển thị ghi chú.
    """
    notes = [item for item in get_notes_by_uc(uc_list) if item["note"]]
    if not notes:
        return

    with st.expander(title, expanded=True):
        for item in notes:
            st.markdown(f"- **{item['kpi_name']}**: {item['note']}")

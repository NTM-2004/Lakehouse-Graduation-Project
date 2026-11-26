"""
streamlit/executive_summary.py - Sinh Executive Summary từ bảng KPI đang hiển thị qua Gemini API (B-604)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Design Serving: Mục III (Executive Summary), Mục VI (Executive Summary: đầu vào, Gọi Gemini lỗi)
- Design Build: B-604
- Requirement v3: Tech stack (8. GenAI Integration), Quản lý bí mật

Quy tắc bắt buộc:
- Chỉ sinh khi người dùng bấm nút.
- Đầu vào: CHỈ các dòng bảng KPI đang hiển thị trong phạm vi bộ lọc, kèm ghi chú giới hạn
  và cờ mẫu nhỏ (is_small_sample / is_small_gap_group). Không gửi dữ liệu đơn/phiên/lead hay định danh.
- Tên model (GEMINI_MODEL) và khóa API (GEMINI_API_KEY) đọc qua biến môi trường.
- Kết quả hiển thị kèm nhãn do AI tạo.
- Khi lỗi: hiển thị thông báo lỗi, không thử lại tự động; dashboard KPI vẫn dùng bình thường.
"""

from __future__ import annotations

import json
import os
from typing import Dict, Iterable, List, Optional

import google.generativeai as genai
import pandas as pd
import streamlit as st

import display_rules

GEMINI_TIMEOUT_SECONDS = float(os.getenv("GEMINI_TIMEOUT_SECONDS", "60"))


def _serialize_kpi_table_for_prompt(df: pd.DataFrame) -> Dict[str, object]:
    """
    Chuyển toàn bộ các dòng KPI đang hiển thị trong bộ lọc sang định dạng bảng cột gọn
    {"columns": [...], "rows": [[...]]} để giảm ~4-5 lần kích thước JSON mà vẫn giữ đủ 100% dòng.
    """
    if df is None or df.empty:
        return {"columns": [], "rows": []}

    columns = list(df.columns)
    rows: List[list] = []
    for _, row in df.iterrows():
        clean_vals = []
        for val in row:
            if display_rules.is_missing(val):
                clean_vals.append(None)
            elif hasattr(val, "isoformat"):
                clean_vals.append(val.isoformat())
            elif isinstance(val, bool):
                clean_vals.append(val)
            elif isinstance(val, float):
                clean_vals.append(round(val, 4))
            elif isinstance(val, (int, str)):
                clean_vals.append(val)
            else:
                clean_vals.append(str(val))
        rows.append(clean_vals)
    return {"columns": columns, "rows": rows}


def build_summary_prompt(
    page_title: str,
    filter_description: str,
    kpi_tables: Dict[str, pd.DataFrame],
    uc_list: Iterable[str],
) -> str:
    """Tạo prompt chuẩn gửi Gemini API tuân thủ Design Serving R70-R73 (gửi đủ toàn bộ dòng đang hiển thị)."""
    notes = display_rules.get_notes_by_uc(uc_list)
    notes_payload = [
        {"kpi": n["kpi_name"], "column": n["column_name"], "limit_note": n["note"]}
        for n in notes
        if n["note"]
    ]

    tables_payload = {
        tname: _serialize_kpi_table_for_prompt(df)
        for tname, df in kpi_tables.items()
    }

    instructions = (
        f"Bạn là chuyên gia phân tích dữ liệu thương mại điện tử. Hãy viết một bản tóm tắt điều hành "
        f"(Executive Summary) ngắn gọn, súc tích bằng tiếng Việt cho trang '{page_title}'.\n"
        f"Phạm vi bộ lọc đang hiển thị: {filter_description}.\n\n"
        "QUY TẮC BẮT BUỘC:\n"
        "1. CHỈ sử dụng đúng các con số có trong dữ liệu JSON được cung cấp bên dưới. Không bịa đặt hay suy diễn số ngoài.\n"
        "   Lưu ý cấu trúc dữ liệu: mỗi bảng trong kpi_tables có dạng {\"columns\":[...],\"rows\":[[...]]}; "
        "mỗi phần tử trong từng hàng của rows tương ứng với columns theo đúng thứ tự vị trí.\n"
        "2. Khi đề cập đến bất kỳ dòng/chỉ số nào có cờ mẫu nhỏ (is_small_sample = true hoặc is_small_gap_group = true), "
        "PHẢI nêu rõ cảnh báo mẫu nhỏ.\n"
        "3. Khi diễn giải bất kỳ KPI nào có ghi chú giới hạn trong mục kpi_notes, PHẢI nêu rõ giới hạn đó.\n"
        "4. Không tự xếp hạng nhóm có cờ mẫu nhỏ. Nếu một giá trị là null, hiểu là 'không đủ dữ liệu' (không phải 0).\n"
        "5. Trình bày gồm 3 mục ngắn: (a) Điểm nổi bật chính, (b) Xu hướng / điểm cần chú ý, (c) Lưu ý giới hạn dữ liệu & mẫu nhỏ.\n"
    )

    payload = {
        "kpi_notes": notes_payload,
        "kpi_tables": tables_payload,
    }
    return f"{instructions}\nDỮ LIỆU KPI ĐANG HIỂN THỊ (JSON):\n{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"


def generate_executive_summary(
    page_title: str,
    filter_description: str,
    kpi_tables: Dict[str, pd.DataFrame],
    uc_list: Iterable[str],
) -> str:
    """
    Gọi Google Gemini API để sinh Executive Summary (không tự động thử lại khi lỗi).
    Đọc bắt buộc GEMINI_API_KEY và GEMINI_MODEL từ biến môi trường (không viết cứng tên model).
    """
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    model_name = os.getenv("GEMINI_MODEL", "").strip()

    if not api_key or api_key.startswith("CHANGE_ME") or api_key == "your_gemini_api_key_here":
        raise RuntimeError(
            "Chưa cấu hình biến môi trường GEMINI_API_KEY hợp lệ. "
            "Vui lòng cập nhật GEMINI_API_KEY trong tệp .env để sử dụng Executive Summary."
        )
    if not model_name:
        raise RuntimeError(
            "Chưa cấu hình biến môi trường GEMINI_MODEL (theo Requirement v3 không viết cứng tên model trong mã)."
        )

    prompt = build_summary_prompt(
        page_title=page_title,
        filter_description=filter_description,
        kpi_tables=kpi_tables,
        uc_list=uc_list,
    )

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)
    response = model.generate_content(prompt, request_options={"timeout": GEMINI_TIMEOUT_SECONDS})

    text = getattr(response, "text", None)
    if not text or not text.strip():
        raise RuntimeError("Gemini API không trả về nội dung tóm tắt.")
    return text.strip()


def render_executive_summary_section(
    page_key: str,
    page_title: str,
    filter_description: str,
    kpi_tables: Dict[str, pd.DataFrame],
    uc_list: Iterable[str],
) -> None:
    """
    Hiển thị khối Executive Summary trên trang Streamlit:
    - Chỉ gọi API khi người dùng bấm nút.
    - Gắn khóa lưu theo cả trang và bộ lọc để không hiện bản tóm tắt cũ khi đổi khoảng thời gian.
    - Gắn nhãn rõ là nội dung do AI tạo.
    - Khi lỗi: hiện thông báo lỗi, không làm gián đoạn phần KPI của dashboard.
    """
    st.divider()
    st.subheader("Executive Summary (AI)")
    st.caption(
        "Chỉ tổng hợp từ toàn bộ các dòng bảng KPI đang hiển thị trong phạm vi bộ lọc hiện tại, "
        "kèm ghi chú giới hạn và cờ mẫu nhỏ."
    )

    state_key = f"exec_summary_{page_key}__{filter_description}"
    if st.button("Tạo Executive Summary (AI)", key=f"btn_summary_{page_key}"):
        has_data = any(df is not None and not df.empty for df in kpi_tables.values())
        if not has_data:
            st.warning("Không có dòng KPI nào trong phạm vi bộ lọc để tóm tắt.")
            return

        with st.spinner("Đang gọi Google Gemini API để sinh tóm tắt..."):
            try:
                summary_text = generate_executive_summary(
                    page_title=page_title,
                    filter_description=filter_description,
                    kpi_tables=kpi_tables,
                    uc_list=uc_list,
                )
                st.session_state[state_key] = summary_text
            except Exception as exc:
                st.error(
                    f"Không thể sinh Executive Summary từ Gemini API: {exc}. "
                    "Dashboard KPI vẫn hoạt động bình thường; bạn có thể bấm nút để thử lại."
                )

    saved_summary: Optional[str] = st.session_state.get(state_key)
    if saved_summary:
        st.info("🤖 **Nội dung do AI tạo (Google Gemini)** — chỉ mang tính tổng hợp từ các chỉ số đang hiển thị.")
        st.markdown(saved_summary)

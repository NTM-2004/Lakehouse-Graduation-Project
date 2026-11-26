"""
streamlit/app.py - Khung ứng dụng Streamlit ba trang cho E-commerce Lakehouse BI (B-605)
Tác giả phụ trách: Minh | Reviewer: Kiên
Tham chiếu:
- Design Serving: Mục III (Dashboard ba trang), Mục VI (Không lưu đệm, Dữ liệu giữa các batch)
- Requirement v3: F14, F15
"""

from __future__ import annotations

import os

import streamlit as st

import dremio_client

st.set_page_config(
    page_title="E-commerce Lakehouse BI",
    page_icon="📊",
    layout="wide",
)

st.title("E-commerce Data Lakehouse Platform")
st.markdown(
    """
Hệ thống phân tích dữ liệu thương mại điện tử trên kiến trúc **Apache Iceberg + Hive Metastore + Dremio (Arrow Flight SQL)**.
Dữ liệu hiển thị được đọc trực tiếp từ **8 bảng KPI tầng Gold** (không lưu đệm kết quả truy vấn).

Vui lòng chọn dashboard ở thanh điều hướng bên trái:
- **Kinh doanh** (`kinh_doanh`): Giám đốc Kinh doanh — Doanh thu, số đơn, tăng trưởng (UC-01) và Giao hàng, đánh giá (UC-02) theo tháng & theo bang.
- **Marketing** (`marketing`): Trưởng Marketing — Phễu thu hút người bán Lead → Deal (UC-03) theo tháng liên hệ đầu & theo nguồn lead.
- **Hành vi** (`hanh_vi`): Chuyên viên phân tích hành vi — Phễu chuyển đổi duyệt web Clickstream (UC-04) theo ngày.
"""
)

st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**Nguồn truy vấn**")
    st.code(f"Dremio Flight SQL: {os.getenv('DREMIO_HOST', 'dremio')}:{os.getenv('DREMIO_FLIGHT_PORT', '32010')}")
with col2:
    st.markdown("**Mô hình GenAI**")
    st.code(os.getenv("GEMINI_MODEL") or "(chưa cấu hình GEMINI_MODEL)")
with col3:
    st.markdown("**Trạng thái kết nối Dremio**")
    try:
        if dremio_client.check_connection():
            st.success("Đã kết nối Dremio (Arrow Flight SQL)")
        else:
            st.warning("Kết nối Dremio trả về rỗng")
    except Exception as exc:
        st.error(f"Chưa kết nối được Dremio: {exc}")

st.caption(
    "Quy ước hiển thị (F15): Mọi dòng có cờ mẫu nhỏ (is_small_sample) đều được gắn nhãn cảnh báo "
    "và không xếp hạng; các tỷ lệ/trung bình có mẫu số bằng 0 được hiển thị là 'không đủ dữ liệu'."
)

"""
streamlit/pages/hanh_vi.py - Trang Chuyên viên phân tích hành vi: KPI UC-04 (B-608)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Mẫu triển khai: M-11
Tham chiếu:
- Design Serving: Mục III (Dashboard Hành vi, Quy tắc hiển thị, Nguồn của KPI trên dashboard, Executive Summary)
- Design Serving: Mục VI (Trang Hành vi: chỉ dùng kpi_funnel_day, không dùng kpi_funnel_month; Không lưu đệm)
- Requirement v3: UC-04, F14, F15
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

# Đảm bảo import được các module dùng chung ở thư mục gốc streamlit/
STREAMLIT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if STREAMLIT_ROOT not in sys.path:
    sys.path.insert(0, STREAMLIT_ROOT)

import display_rules
import dremio_client
import executive_summary

st.set_page_config(
    page_title="Hành vi duyệt web (UC-04) - Lakehouse BI",
    page_icon="🧭",
    layout="wide",
)

st.title("Phân tích Hành vi duyệt web (UC-04)")
st.caption("Đối tượng: Chuyên viên phân tích hành vi | Nguồn: `gold.kpi_funnel_day` (truy vấn trực tiếp qua Dremio Arrow Flight SQL, không lưu đệm)")

# 1. Đọc trực tiếp bảng kpi_funnel_day từ Dremio (không cache, sắp theo session_date)
try:
    df_day = dremio_client.load_kpi_table("kpi_funnel_day", order_by=["session_date"])
except Exception as exc:
    st.error(f"Không thể đọc bảng `gold.kpi_funnel_day` từ Dremio: {exc}")
    st.stop()

if df_day.empty:
    st.warning("Bảng `gold.kpi_funnel_day` hiện chưa có dữ liệu.")
    st.stop()

df_day["session_date"] = pd.to_datetime(df_day["session_date"]).dt.date

# 2. Bộ lọc thời gian: khoảng ngày (mặc định toàn bộ)
min_date = df_day["session_date"].min()
max_date = df_day["session_date"].max()

with st.sidebar:
    st.header("Bộ lọc thời gian")
    date_range = st.date_input(
        "Khoảng ngày (session_date)",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start_date, end_date = date_range[0], date_range[1]
else:
    start_date, end_date = min_date, max_date

filtered_df = df_day[
    (df_day["session_date"] >= start_date) & (df_day["session_date"] <= end_date)
].sort_values("session_date", ascending=True).reset_index(drop=True)

if filtered_df.empty:
    st.warning(f"Không có phiên nào trong khoảng ngày {start_date} đến {end_date}.")
    st.stop()

# 3. Ghi chú giới hạn KPI UC-04 (Requirement v3, cột Lưu ý)
display_rules.render_kpi_notes_panel(["UC-04"], title="Ghi chú giới hạn các KPI Hành vi (UC-04)")

# 4. Tổng quan nhanh về phạm vi lọc (chỉ hiển thị thông số đếm và số ngày có cờ mẫu nhỏ — không cộng gộp tỷ lệ qua ngày)
n_days = len(filtered_df)
n_small_days = int(filtered_df["is_small_sample"].fillna(False).astype(bool).sum())
total_sessions = int(filtered_df["n_sessions"].fillna(0).sum())

c1, c2, c3, c4 = st.columns(4)
c1.metric("Số ngày có phiên", f"{n_days:,}")
c2.metric("Tổng số phiên trong khoảng lọc", f"{total_sessions:,}")
c3.metric("Ngày có cờ mẫu nhỏ", f"{n_small_days:,}")
c4.metric("Khoảng ngày hiển thị", f"{start_date} → {end_date}")

if n_small_days > 0:
    small_dates = filtered_df.loc[
        filtered_df["is_small_sample"].fillna(False).astype(bool), "session_date"
    ].astype(str).tolist()
    preview = ", ".join(small_dates[:10])
    suffix = f" ... (và {len(small_dates) - 10} ngày khác)" if len(small_dates) > 10 else ""
    st.warning(
        f"⚠️ Có **{n_small_days}** ngày mang cờ mẫu nhỏ (`is_small_sample = true`) trong khoảng hiển thị "
        f"(cần lưu ý khi đọc biểu đồ xu hướng): {preview}{suffix}."
    )

# 5. Xu hướng KPI theo ngày
st.subheader("Xu hướng KPI theo ngày")
tab_rate, tab_dur, tab_funnel = st.tabs(
    [
        "Tỷ lệ chuyển đổi & Bỏ giỏ",
        "Thời lượng phiên trung bình",
        "Phễu 4 bước & Tỷ lệ chuyển bước",
    ]
)

chart_base = filtered_df.set_index("session_date")

with tab_rate:
    note_conv = display_rules.get_kpi_note("Tỷ lệ chuyển đổi phiên")
    if note_conv:
        st.caption(f"**Tỷ lệ chuyển đổi phiên**: {note_conv}")
    st.line_chart(
        chart_base[["conversion_rate", "abandon_rate"]].rename(
            columns={
                "conversion_rate": "Tỷ lệ chuyển đổi phiên",
                "abandon_rate": "Tỷ lệ bỏ giỏ",
            }
        )
    )

with tab_dur:
    st.caption(f"**Thời lượng phiên trung bình**: {display_rules.get_kpi_note('Thời lượng phiên trung bình')}")
    st.line_chart(
        chart_base[["avg_duration_seconds"]].rename(
            columns={"avg_duration_seconds": "Thời lượng phiên trung bình (giây)"}
        )
    )

with tab_funnel:
    st.caption(f"**Tỷ lệ chuyển bước phễu**: {display_rules.get_kpi_note('Tỷ lệ chuyển bước phễu')}")
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        st.markdown("**Số phiên đạt từng bước phễu theo ngày (phễu chặt)**")
        st.line_chart(
            chart_base[["n_reach_home", "n_reach_view", "n_reach_cart", "n_reach_purchase"]].rename(
                columns={
                    "n_reach_home": "1. Trang chủ (home)",
                    "n_reach_view": "2. Xem SP (view)",
                    "n_reach_cart": "3. Thêm giỏ (cart)",
                    "n_reach_purchase": "4. Mua (purchase)",
                }
            )
        )
    with col_f2:
        st.markdown("**Tỷ lệ chuyển bước phễu theo ngày**")
        st.line_chart(
            chart_base[["step_rate_home_view", "step_rate_view_cart", "step_rate_cart_purchase"]].rename(
                columns={
                    "step_rate_home_view": "Trang chủ → Xem SP",
                    "step_rate_view_cart": "Xem SP → Thêm giỏ",
                    "step_rate_cart_purchase": "Thêm giỏ → Mua",
                }
            )
        )

# 6. Bảng chi tiết theo ngày (kèm nhãn mẫu nhỏ và giá trị rỗng chuẩn Quy tắc hiển thị)
st.subheader("Bảng KPI chi tiết theo ngày (sắp theo session_date, không xếp hạng)")
kpi_columns = {
    "n_sessions": {"label": "Số phiên", "kind": "count"},
    "n_reach_home": {"label": "Đạt Trang chủ", "kind": "count"},
    "n_reach_view": {"label": "Đạt Xem SP", "kind": "count"},
    "n_reach_cart": {"label": "Đạt Thêm giỏ", "kind": "count"},
    "n_reach_purchase": {"label": "Đạt Mua", "kind": "count"},
    "conversion_rate": {"label": "Tỷ lệ chuyển đổi phiên", "kind": "rate", "decimals": 2},
    "abandon_rate": {"label": "Tỷ lệ bỏ giỏ", "kind": "rate", "decimals": 2},
    "avg_duration_seconds": {"label": "Thời lượng phiên TB", "kind": "seconds", "decimals": 2},
    "step_rate_home_view": {"label": "Bước: Home → View", "kind": "rate", "decimals": 2},
    "step_rate_view_cart": {"label": "Bước: View → Cart", "kind": "rate", "decimals": 2},
    "step_rate_cart_purchase": {"label": "Bước: Cart → Purchase", "kind": "rate", "decimals": 2},
}
display_df = display_rules.format_display_dataframe(
    filtered_df,
    slice_cols=["session_date"],
    kpi_columns=kpi_columns,
)
st.dataframe(display_df, use_container_width=True, hide_index=True)

# 7. Executive Summary (AI - Google Gemini)
executive_summary.render_executive_summary_section(
    page_key="hanh_vi",
    page_title="Hành vi duyệt web (UC-04)",
    filter_description=f"Khoảng ngày từ {start_date} đến {end_date} ({n_days} ngày, bảng gold.kpi_funnel_day)",
    kpi_tables={"kpi_funnel_day": filtered_df},
    uc_list=["UC-04"],
)

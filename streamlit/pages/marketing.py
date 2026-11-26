"""
streamlit/pages/marketing.py - Trang Trưởng Marketing: KPI UC-03 (B-607)
Tác giả phụ trách: Hoàng | Reviewer: Minh
Mẫu triển khai: M-11
Tham chiếu:
- Design Serving: Mục III (Dashboard Marketing, Quy tắc hiển thị, Nguồn đọc, Executive Summary)
- Design Serving: Mục VI (So sánh theo nguồn lead toàn kỳ không bị lọc theo khoảng tháng; Không lưu đệm)
- Requirement v3: UC-03, F14, F15
"""

from __future__ import annotations

import os
import sys

import streamlit as st

# Đảm bảo import được các module dùng chung ở thư mục gốc streamlit/
STREAMLIT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if STREAMLIT_ROOT not in sys.path:
    sys.path.insert(0, STREAMLIT_ROOT)

import display_rules
import dremio_client
import executive_summary

st.set_page_config(
    page_title="Phễu Marketing Lead → Deal (UC-03) - Lakehouse BI",
    page_icon="📣",
    layout="wide",
)

st.title("Phân tích Phễu Marketing Lead → Deal (UC-03)")
st.caption(
    "Đối tượng: Trưởng Marketing | Nguồn: `gold.kpi_lead_month` & `gold.kpi_lead_origin` "
    "(truy vấn trực tiếp qua Dremio Arrow Flight SQL, không lưu đệm)"
)

# 1. Đọc trực tiếp 2 bảng KPI Gold của UC-03 từ Dremio (không cache, sắp theo khóa lát cắt — không xếp hạng)
try:
    df_month = dremio_client.load_kpi_table("kpi_lead_month", order_by=["year_month"])
    df_origin = dremio_client.load_kpi_table("kpi_lead_origin", order_by=["origin"])
except Exception as exc:
    st.error(f"Không thể đọc bảng KPI UC-03 (`gold.kpi_lead_month` / `gold.kpi_lead_origin`) từ Dremio: {exc}")
    st.stop()

if df_month.empty and df_origin.empty:
    st.warning("Cả hai bảng `gold.kpi_lead_month` và `gold.kpi_lead_origin` hiện chưa có dữ liệu.")
    st.stop()

if not df_month.empty:
    df_month["year_month"] = df_month["year_month"].astype(str)
    df_month = df_month.sort_values("year_month", ascending=True).reset_index(drop=True)

if not df_origin.empty:
    df_origin["origin"] = df_origin["origin"].astype(str)
    df_origin = df_origin.sort_values("origin", ascending=True).reset_index(drop=True)

# 2. Bộ lọc thời gian: khoảng tháng liên hệ đầu (chỉ áp dụng cho phần xu hướng theo tháng)
available_months = df_month["year_month"].tolist() if not df_month.empty else []

with st.sidebar:
    st.header("Bộ lọc thời gian")
    if len(available_months) >= 2:
        start_month, end_month = st.select_slider(
            "Khoảng tháng liên hệ đầu (year_month)",
            options=available_months,
            value=(available_months[0], available_months[-1]),
        )
    elif len(available_months) == 1:
        start_month = end_month = available_months[0]
        st.info(f"Hiện chỉ có 1 tháng dữ liệu: `{start_month}`")
    else:
        start_month = end_month = ""
    st.caption(
        "Lưu ý: Bộ lọc khoảng tháng chỉ áp dụng cho phần **Xu hướng theo tháng** (`kpi_lead_month`). "
        "Phần **So sánh theo nguồn lead** (`kpi_lead_origin`) luôn hiển thị **toàn kỳ**."
    )

if not df_month.empty and start_month and end_month:
    filtered_month_df = df_month[
        (df_month["year_month"] >= start_month) & (df_month["year_month"] <= end_month)
    ].sort_values("year_month", ascending=True).reset_index(drop=True)
else:
    filtered_month_df = df_month.copy()

# 3. Ghi chú giới hạn KPI UC-03 (Requirement v3, cột Lưu ý — đọc từ kpi_notes.csv)
display_rules.render_kpi_notes_panel(["UC-03"], title="Ghi chú giới hạn các KPI Marketing (UC-03)")

# Cấu hình cột hiển thị dùng chung cho cả 2 lát cắt UC-03
UC03_KPI_COLUMNS = {
    "n_leads": {"label": "Số lead", "kind": "count"},
    "n_leads_with_deal": {"label": "Số lead chốt deal", "kind": "count"},
    "lead_to_deal_rate": {"label": "Tỷ lệ lead → deal", "kind": "rate", "decimals": 2},
    "avg_days_to_close": {"label": "Thời gian chốt TB (ngày)", "kind": "number", "decimals": 2},
    "won_30d_rate": {"label": "Tỷ lệ chốt trong 30 ngày", "kind": "rate", "decimals": 2},
}

# ===========================================================================
# PHẦN A: XU HƯỚNG THEO THÁNG LIÊN HỆ ĐẦU (kpi_lead_month — chịu bộ lọc tháng)
# ===========================================================================
st.header("1. Xu hướng theo tháng liên hệ đầu (`kpi_lead_month`)")

if filtered_month_df.empty:
    st.warning(f"Không có dữ liệu lead trong khoảng tháng `{start_month}` đến `{end_month}`.")
else:
    n_months = len(filtered_month_df)
    total_leads_m = int(filtered_month_df["n_leads"].fillna(0).sum())
    total_deals_m = int(filtered_month_df["n_leads_with_deal"].fillna(0).sum())
    n_small_months = int(filtered_month_df["is_small_sample"].fillna(False).astype(bool).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Số tháng hiển thị", f"{n_months:,}")
    c2.metric("Tổng số lead (trong khoảng lọc)", f"{total_leads_m:,}")
    c3.metric("Tổng số lead chốt deal", f"{total_deals_m:,}")
    c4.metric("Tháng có cờ mẫu nhỏ (< 30 deal)", f"{n_small_months:,}")

    if n_small_months > 0:
        small_months = filtered_month_df.loc[
            filtered_month_df["is_small_sample"].fillna(False).astype(bool), "year_month"
        ].tolist()
        st.warning(
            f"⚠️ Có **{n_small_months}** tháng mang cờ mẫu nhỏ (`is_small_sample = true`, dưới 30 deal) "
            f"trong khoảng hiển thị: {', '.join(small_months)}."
        )

    tab_rates, tab_close_days, tab_volume = st.tabs(
        [
            "Tỷ lệ lead → deal & Chốt trong 30 ngày",
            "Thời gian chốt trung bình (ngày)",
            "Số lượng lead & deal theo tháng",
        ]
    )

    month_chart_base = filtered_month_df.set_index("year_month")

    with tab_rates:
        note_l2d = display_rules.get_kpi_note("Tỷ lệ lead → deal")
        note_w30 = display_rules.get_kpi_note("Tỷ lệ chốt trong 30 ngày")
        if note_l2d:
            st.caption(f"**Tỷ lệ lead → deal**: {note_l2d}")
        if note_w30:
            st.caption(f"**Tỷ lệ chốt trong 30 ngày**: {note_w30}")
        st.line_chart(
            month_chart_base[["lead_to_deal_rate", "won_30d_rate"]].rename(
                columns={
                    "lead_to_deal_rate": "Tỷ lệ lead → deal",
                    "won_30d_rate": "Tỷ lệ chốt trong 30 ngày",
                }
            )
        )

    with tab_close_days:
        note_days = display_rules.get_kpi_note("Thời gian chốt trung bình")
        if note_days:
            st.caption(f"**Thời gian chốt trung bình**: {note_days}")
        st.line_chart(
            month_chart_base[["avg_days_to_close"]].rename(
                columns={"avg_days_to_close": "Thời gian chốt trung bình (ngày)"}
            )
        )

    with tab_volume:
        st.bar_chart(
            month_chart_base[["n_leads", "n_leads_with_deal"]].rename(
                columns={
                    "n_leads": "Số lead",
                    "n_leads_with_deal": "Số lead chốt deal",
                }
            )
        )

    st.subheader("Bảng KPI chi tiết theo tháng (sắp theo year_month, không xếp hạng)")
    display_month_df = display_rules.format_display_dataframe(
        filtered_month_df,
        slice_cols=["year_month"],
        kpi_columns=UC03_KPI_COLUMNS,
    )
    st.dataframe(display_month_df, use_container_width=True, hide_index=True)

# ===========================================================================
# PHẦN B: SO SÁNH THEO NGUỒN LEAD TOÀN KỲ (kpi_lead_origin — không lọc tháng)
# ===========================================================================
st.divider()
st.header("2. So sánh theo nguồn lead — Toàn kỳ (`kpi_lead_origin`)")
st.caption("Hiển thị toàn bộ các nguồn lead trong toàn kỳ dữ liệu (không chịu tác động của bộ lọc khoảng tháng; sắp theo tên nguồn, không xếp hạng).")

if df_origin.empty:
    st.warning("Bảng `gold.kpi_lead_origin` hiện chưa có dữ liệu.")
else:
    n_origins = len(df_origin)
    total_leads_o = int(df_origin["n_leads"].fillna(0).sum())
    total_deals_o = int(df_origin["n_leads_with_deal"].fillna(0).sum())
    n_small_origins = int(df_origin["is_small_sample"].fillna(False).astype(bool).sum())

    o1, o2, o3, o4 = st.columns(4)
    o1.metric("Số nguồn lead", f"{n_origins:,}")
    o2.metric("Tổng số lead (toàn kỳ)", f"{total_leads_o:,}")
    o3.metric("Tổng số lead chốt deal (toàn kỳ)", f"{total_deals_o:,}")
    o4.metric("Nguồn có cờ mẫu nhỏ (< 30 deal)", f"{n_small_origins:,}")

    if n_small_origins > 0:
        small_origins = df_origin.loc[
            df_origin["is_small_sample"].fillna(False).astype(bool), "origin"
        ].tolist()
        st.warning(
            f"⚠️ Có **{n_small_origins}** nguồn mang cờ mẫu nhỏ (`is_small_sample = true`, dưới 30 deal — không xếp hạng): "
            f"{', '.join(small_origins)}."
        )

    origin_chart_base = df_origin.set_index("origin")
    col_o1, col_o2 = st.columns(2)
    with col_o1:
        st.markdown("**Tỷ lệ lead → deal & Chốt trong 30 ngày theo nguồn (sắp theo tên nguồn)**")
        st.bar_chart(
            origin_chart_base[["lead_to_deal_rate", "won_30d_rate"]].rename(
                columns={
                    "lead_to_deal_rate": "Tỷ lệ lead → deal",
                    "won_30d_rate": "Tỷ lệ chốt trong 30 ngày",
                }
            )
        )
    with col_o2:
        st.markdown("**Thời gian chốt trung bình (ngày) theo nguồn (sắp theo tên nguồn)**")
        st.bar_chart(
            origin_chart_base[["avg_days_to_close"]].rename(
                columns={"avg_days_to_close": "Thời gian chốt trung bình (ngày)"}
            )
        )

    st.subheader("Bảng KPI chi tiết theo nguồn lead (toàn kỳ, sắp theo origin, không xếp hạng)")
    display_origin_df = display_rules.format_display_dataframe(
        df_origin,
        slice_cols=["origin"],
        kpi_columns=UC03_KPI_COLUMNS,
    )
    st.dataframe(display_origin_df, use_container_width=True, hide_index=True)

# ===========================================================================
# PHẦN C: EXECUTIVE SUMMARY (AI - Google Gemini)
# ===========================================================================
filter_desc = (
    f"Xu hướng theo tháng (kpi_lead_month): từ {start_month} đến {end_month} ({len(filtered_month_df)} tháng); "
    f"So sánh theo nguồn (kpi_lead_origin): toàn kỳ ({len(df_origin)} nguồn)"
)
executive_summary.render_executive_summary_section(
    page_key="marketing",
    page_title="Phễu Marketing Lead → Deal (UC-03)",
    filter_description=filter_desc,
    kpi_tables={
        "kpi_lead_month": filtered_month_df,
        "kpi_lead_origin": df_origin,
    },
    uc_list=["UC-03"],
)

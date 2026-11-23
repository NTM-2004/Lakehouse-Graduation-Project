import streamlit as st

st.set_page_config(
    page_title="E-commerce Lakehouse BI",
    page_icon="📊",
    layout="wide"
)

st.title("E-commerce Lakehouse Platform")
st.markdown("""
Chào mừng đến với hệ thống báo cáo và phân tích dữ liệu E-commerce Lakehouse.
Vui lòng chọn trang phân tích ở thanh điều hướng bên trái:
- **Kinh doanh**: Doanh thu, giao hàng, hiệu suất theo bang và danh mục.
- **Marketing**: Kênh lead, chuyển đổi closed deals, phễu bán hàng.
- **Hành vi**: Phân tích hành vi khách hàng và luồng sự kiện clickstream.
""")

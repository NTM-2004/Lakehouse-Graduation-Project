"""
reference/standard_queries.py - Thư viện truy vấn chuẩn Q1 đến Q5 (B-701)
Tác giả phụ trách: Minh | Reviewer: Kiên
Tham chiếu:
- Requirement v3: Hiệu năng truy vấn (Q1 đến Q5, ngưỡng nghiệm thu)
- Design Gold v2: Truy vấn chuẩn Q1 đến Q5; Quyết định Truy vấn Q2
- Design Serving: So khớp với Spark; Reflection; Tiêu chí hòa của Q2
- Design Build: B-701
- Hợp đồng bảng & cột: dbt/schema_contract.yml

Chức năng:
- Định nghĩa đúng 5 truy vấn chuẩn Q1..Q5 dùng chung cho Spark và Dremio trên cùng tầng Gold.
- Đảm bảo Q2 đọc fact_order_item, fact_order (is_valid_order = TRUE), dim_seller và áp dụng
  tiêu chí hòa: doanh thu giảm dần, hòa thì seller_id tăng dần, lấy top 20.
- Cung cấp siêu dữ liệu khóa lát cắt, cột số thực cần làm tròn 4 chữ số thập phân (So khớp với Spark)
  và danh sách 7 bảng Gold cần tạo Raw Reflection (Design Serving: Reflection).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

STANDARD_QUERY_CODES: Tuple[str, ...] = ("Q1", "Q2", "Q3", "Q4", "Q5")

# Danh sách chính xác các bảng Gold xuất hiện ở cột "Chạy trên" của Q1..Q5 (Design Serving: Reflection)
REFLECTION_GOLD_TABLES: Tuple[str, ...] = (
    "kpi_sales_state_month",
    "fact_order_item",
    "fact_order",
    "dim_seller",
    "kpi_delivery_state_month",
    "kpi_funnel_day",
    "kpi_lead_origin",
)


@dataclass(frozen=True)
class QuerySpec:
    """Mô tả cấu trúc một truy vấn chuẩn để chạy và đối soát giữa Spark và Dremio."""

    code: str
    name: str
    tables: Tuple[str, ...]
    key_columns: Tuple[str, ...]
    value_columns: Tuple[str, ...]
    float_columns: Tuple[str, ...]
    is_ranked: bool = False


QUERY_SPECS: Dict[str, QuerySpec] = {
    "Q1": QuerySpec(
        code="Q1",
        name="Doanh thu theo tháng và theo bang",
        tables=("kpi_sales_state_month",),
        key_columns=("year_month", "customer_state"),
        value_columns=("sum_revenue",),
        float_columns=("sum_revenue",),
    ),
    "Q2": QuerySpec(
        code="Q2",
        name="20 người bán có doanh thu cao nhất",
        tables=("fact_order_item", "fact_order", "dim_seller"),
        key_columns=("seller_id",),
        value_columns=("seller_city", "seller_state", "revenue"),
        float_columns=("revenue",),
        is_ranked=True,
    ),
    "Q3": QuerySpec(
        code="Q3",
        name="Tỷ lệ giao trễ theo bang",
        tables=("kpi_delivery_state_month",),
        key_columns=("customer_state", "year_month"),
        value_columns=("n_delivered", "n_late", "late_rate"),
        float_columns=("late_rate",),
    ),
    "Q4": QuerySpec(
        code="Q4",
        name="Phễu chuyển đổi theo ngày",
        tables=("kpi_funnel_day",),
        key_columns=("session_date",),
        value_columns=(
            "n_sessions",
            "n_reach_home",
            "n_reach_view",
            "n_reach_cart",
            "n_reach_purchase",
            "conversion_rate",
            "abandon_rate",
            "avg_duration_seconds",
        ),
        float_columns=("conversion_rate", "abandon_rate", "avg_duration_seconds"),
    ),
    "Q5": QuerySpec(
        code="Q5",
        name="Tỷ lệ lead sang closed deal theo nguồn lead",
        tables=("kpi_lead_origin",),
        key_columns=("origin",),
        value_columns=("n_leads", "n_leads_with_deal", "lead_to_deal_rate"),
        float_columns=("lead_to_deal_rate",),
    ),
}


def _qualify_table(table: str, dialect: str, catalog: str, schema: str) -> str:
    """Định dạng tên bảng đầy đủ theo cú pháp Spark hoặc Dremio."""
    norm_dialect = dialect.strip().lower()
    if norm_dialect == "dremio":
        return f'"{catalog}"."{schema}"."{table}"'
    if norm_dialect == "spark":
        return f"{catalog}.{schema}.{table}"
    raise ValueError(f"Dialect không hợp lệ: '{dialect}'. Chỉ hỗ trợ 'spark' hoặc 'dremio'.")


def get_query_sql(
    code: str,
    dialect: str = "dremio",
    catalog: str = "hive",
    schema: str = "gold",
) -> str:
    """
    Trả về câu lệnh SQL chuẩn cho mã truy vấn Q1..Q5 tương thích hoàn toàn
    giữa Spark SQL và Dremio SQL.
    """
    qcode = code.strip().upper()
    if qcode not in QUERY_SPECS:
        raise ValueError(f"Mã truy vấn không hợp lệ: '{code}'. Hợp lệ: {STANDARD_QUERY_CODES}")

    def tbl(name: str) -> str:
        return _qualify_table(name, dialect=dialect, catalog=catalog, schema=schema)

    if qcode == "Q1":
        return (
            "SELECT\n"
            "    year_month,\n"
            "    customer_state,\n"
            "    sum_revenue\n"
            f"FROM {tbl('kpi_sales_state_month')}\n"
            "ORDER BY year_month ASC, customer_state ASC"
        )

    if qcode == "Q2":
        return (
            "SELECT\n"
            "    s.seller_id,\n"
            "    s.seller_city,\n"
            "    s.seller_state,\n"
            "    SUM(foi.price) AS revenue\n"
            f"FROM {tbl('fact_order_item')} AS foi\n"
            f"INNER JOIN {tbl('fact_order')} AS fo\n"
            "    ON foi.order_id = fo.order_id\n"
            f"INNER JOIN {tbl('dim_seller')} AS s\n"
            "    ON foi.seller_id = s.seller_id\n"
            "WHERE fo.is_valid_order = TRUE\n"
            "GROUP BY s.seller_id, s.seller_city, s.seller_state\n"
            "ORDER BY revenue DESC, s.seller_id ASC\n"
            "LIMIT 20"
        )

    if qcode == "Q3":
        return (
            "SELECT\n"
            "    customer_state,\n"
            "    year_month,\n"
            "    n_delivered,\n"
            "    n_late,\n"
            "    late_rate\n"
            f"FROM {tbl('kpi_delivery_state_month')}\n"
            "ORDER BY customer_state ASC, year_month ASC"
        )

    if qcode == "Q4":
        return (
            "SELECT\n"
            "    session_date,\n"
            "    n_sessions,\n"
            "    n_reach_home,\n"
            "    n_reach_view,\n"
            "    n_reach_cart,\n"
            "    n_reach_purchase,\n"
            "    conversion_rate,\n"
            "    abandon_rate,\n"
            "    avg_duration_seconds\n"
            f"FROM {tbl('kpi_funnel_day')}\n"
            "ORDER BY session_date ASC"
        )

    # Q5
    return (
        "SELECT\n"
        "    origin,\n"
        "    n_leads,\n"
        "    n_leads_with_deal,\n"
        "    lead_to_deal_rate\n"
        f"FROM {tbl('kpi_lead_origin')}\n"
        "ORDER BY origin ASC"
    )


def get_all_queries(
    dialect: str = "dremio",
    catalog: str = "hive",
    schema: str = "gold",
) -> Dict[str, str]:
    """Trả về từ điển {Q1..Q5: sql} cho dialect chỉ định."""
    return {
        code: get_query_sql(code, dialect=dialect, catalog=catalog, schema=schema)
        for code in STANDARD_QUERY_CODES
    }

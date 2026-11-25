"""
streamlit/dremio_client.py - Thư viện kết nối Dremio qua Arrow Flight SQL (B-601)
Tác giả phụ trách: Minh | Reviewer: Kiên
Tham chiếu:
- Design Serving: Mục II (Kết nối F14), Mục III (Quy tắc Nguồn đọc), Mục VI (Không lưu đệm)
- Design Build: B-601
- Requirement v3: F14, Quản lý bí mật

Quy tắc bắt buộc:
- Truy vấn Dremio qua cổng Arrow Flight SQL (mặc định 32010).
- Thông tin đăng nhập đọc từ biến môi trường (DREMIO_HOST, DREMIO_FLIGHT_PORT,
  DREMIO_ADMIN_USER, DREMIO_ADMIN_PASSWORD).
- KHÔNG lưu đệm (cache) kết quả truy vấn để thấy batch mới ngay sau Làm mới Dremio.
- Dashboard chỉ được phép đọc 8 bảng KPI ở tầng Gold; không đọc Silver/Bronze/Raw/fact/dim.
"""

from __future__ import annotations

import os
from typing import Optional, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

DREMIO_CATALOG = os.getenv("DREMIO_CATALOG", "hive")
GOLD_SCHEMA = os.getenv("DREMIO_GOLD_SCHEMA", "gold")

ALLOWED_KPI_TABLES = frozenset(
    {
        "kpi_sales_month",
        "kpi_sales_state_month",
        "kpi_delivery_month",
        "kpi_delivery_state_month",
        "kpi_lead_month",
        "kpi_lead_origin",
        "kpi_funnel_day",
        "kpi_funnel_month",
    }
)


FLIGHT_TIMEOUT_SECONDS = float(os.getenv("DREMIO_FLIGHT_TIMEOUT", "30"))


def _get_flight_config() -> dict:
    user = os.getenv("DREMIO_ADMIN_USER", "").strip()
    password = os.getenv("DREMIO_ADMIN_PASSWORD", "").strip()
    if not user or not password:
        raise RuntimeError(
            "Thiếu biến môi trường DREMIO_ADMIN_USER hoặc DREMIO_ADMIN_PASSWORD. "
            "Vui lòng cấu hình trong tệp .env trước khi kết nối Dremio."
        )
    return {
        "host": os.getenv("DREMIO_HOST", "dremio"),
        "port": int(os.getenv("DREMIO_FLIGHT_PORT", "32010")),
        "user": user,
        "password": password,
    }


def _create_authenticated_client():
    cfg = _get_flight_config()
    location = f"grpc://{cfg['host']}:{cfg['port']}"
    client = flight.FlightClient(location)
    bearer_token = client.authenticate_basic_token(cfg["user"], cfg["password"])
    options = flight.FlightCallOptions(headers=[bearer_token], timeout=FLIGHT_TIMEOUT_SECONDS)
    return client, options


def query_sql(sql: str) -> pd.DataFrame:
    """
    Thực thi câu lệnh SELECT trên Dremio qua Arrow Flight SQL và trả về pandas.DataFrame.
    Tuyệt đối không lưu đệm (no cache) theo Design Serving.
    """
    normalized = sql.strip().upper()
    if not (normalized.startswith("SELECT") or normalized.startswith("WITH")):
        raise ValueError("Chỉ cho phép câu lệnh truy vấn đọc (SELECT / WITH).")

    client, options = _create_authenticated_client()
    try:
        descriptor = flight.FlightDescriptor.for_command(sql.encode("utf-8"))
        flight_info = client.get_flight_info(descriptor, options)

        tables = []
        for endpoint in flight_info.endpoints:
            reader = client.do_get(endpoint.ticket, options)
            tables.append(reader.read_all())

        if not tables:
            return pd.DataFrame()

        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        return combined.to_pandas()
    finally:
        client.close()


def load_kpi_table(
    table_name: str,
    order_by: Optional[Sequence[str]] = None,
    catalog: str = DREMIO_CATALOG,
    schema: str = GOLD_SCHEMA,
) -> pd.DataFrame:
    """
    Đọc một bảng KPI tầng Gold từ Dremio qua Arrow Flight SQL.
    - Kiểm tra table_name thuộc 8 bảng KPI được phép (Design Serving: Nguồn đọc).
    - Sắp xếp theo khóa lát cắt (order_by), không sắp xếp theo giá trị KPI (Không xếp hạng).
    - Không lưu đệm kết quả.
    """
    if table_name not in ALLOWED_KPI_TABLES:
        raise ValueError(
            f"Bảng '{table_name}' không nằm trong danh sách 8 bảng KPI Gold được phép đọc: "
            f"{sorted(ALLOWED_KPI_TABLES)}"
        )

    sql = f'SELECT * FROM "{catalog}"."{schema}"."{table_name}"'
    if order_by:
        order_clause = ", ".join(f'"{col}" ASC' for col in order_by)
        sql += f" ORDER BY {order_clause}"

    return query_sql(sql)


def check_connection() -> bool:
    """Kiểm tra nhanh kết nối Arrow Flight SQL tới Dremio."""
    df = query_sql("SELECT 1 AS ok")
    return not df.empty and int(df.iloc[0]["ok"]) == 1

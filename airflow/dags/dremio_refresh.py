"""
airflow/dags/dremio_refresh.py - Làm mới siêu dữ liệu Gold và Reflection trên Dremio (B-505)
Tác giả phụ trách: Minh | Reviewer: Kiên
Trạng thái giao diện: Tạm (T-04: Dremio thấy dữ liệu mới, T-05: Reflection sau khi dbt dựng lại bảng)
Tham chiếu:
- Design Serving: Mục II (Nội dung Làm mới Dremio, Reflection)
- Design Orchestration: Bước Làm mới Dremio (chỉ chạy khi cả Silver và Gold đạt)
- Requirement v3: F13
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List

import requests

DREMIO_CATALOG = os.getenv("DREMIO_CATALOG", "hive")

GOLD_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_date",
    "fact_order",
    "fact_order_item",
    "fact_lead",
    "fact_session",
    "kpi_sales_month",
    "kpi_sales_state_month",
    "kpi_delivery_month",
    "kpi_delivery_state_month",
    "kpi_lead_month",
    "kpi_lead_origin",
    "kpi_funnel_day",
    "kpi_funnel_month",
]


def _dremio_base_url() -> str:
    host = os.getenv("DREMIO_HOST", "dremio")
    port = os.getenv("DREMIO_PORT", "9047")
    return f"http://{host}:{port}"


def _login_dremio() -> requests.Session:
    """Đăng nhập Dremio REST API bằng tài khoản quản trị từ biến môi trường (không viết cứng mật khẩu)."""
    base_url = _dremio_base_url()
    user = os.getenv("DREMIO_ADMIN_USER", "").strip()
    password = os.getenv("DREMIO_ADMIN_PASSWORD", "").strip()
    if not user or not password:
        raise RuntimeError("Thiếu biến môi trường DREMIO_ADMIN_USER hoặc DREMIO_ADMIN_PASSWORD.")

    session = requests.Session()
    resp = session.post(
        f"{base_url}/apiv2/login",
        json={"userName": user, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["token"]
    session.headers.update({"Authorization": f"_dremio{token}"})
    return session


def _run_dremio_sql(session: requests.Session, sql: str, poll_timeout: int = 180) -> Dict[str, Any]:
    """Gửi câu lệnh SQL lên Dremio REST API và chờ hoàn tất."""
    base_url = _dremio_base_url()
    resp = session.post(f"{base_url}/api/v3/sql", json={"sql": sql}, timeout=60)
    resp.raise_for_status()
    job_id = resp.json()["id"]

    deadline = time.time() + poll_timeout
    while time.time() < deadline:
        status_resp = session.get(f"{base_url}/api/v3/job/{job_id}", timeout=30)
        status_resp.raise_for_status()
        payload = status_resp.json()
        state = payload.get("jobState")
        if state == "COMPLETED":
            return {"job_id": job_id, "state": state}
        if state in ("FAILED", "CANCELED", "CANCELLED"):
            raise RuntimeError(
                f"[DREMIO SQL LỖI] Job {job_id} ({state}) cho lệnh: {sql}\n"
                f"Chi tiết: {payload.get('errorMessage', status_resp.text)}"
            )
        time.sleep(1.5)

    raise TimeoutError(f"[DREMIO TIMEOUT] Quá {poll_timeout}s chờ lệnh SQL: {sql}")


def refresh_gold_metadata(
    session: requests.Session,
    tables: List[str] = GOLD_TABLES,
    catalog: str = DREMIO_CATALOG,
) -> List[str]:
    """
    Việc thứ nhất (Design Serving): Làm mới siêu dữ liệu các bảng Gold
    để Dremio thấy snapshot Iceberg mới. Không làm mới Silver, Bronze.
    """
    refreshed: List[str] = []
    for table in tables:
        sql = f'ALTER PDS "{catalog}"."gold"."{table}" REFRESH METADATA FORCE UPDATE'
        _run_dremio_sql(session, sql)
        refreshed.append(f"{catalog}.gold.{table}")
    return refreshed


def refresh_gold_reflections(
    session: requests.Session,
    tables: List[str] = GOLD_TABLES,
    catalog: str = DREMIO_CATALOG,
) -> List[str]:
    """
    Việc thứ hai (Design Serving): Làm mới các Reflection phụ thuộc bảng Gold
    sau khi siêu dữ liệu bảng Gold đã được cập nhật.
    """
    base_url = _dremio_base_url()
    refreshed_datasets: List[str] = []

    # Lấy danh sách Reflection hiện có để biết những bảng Gold nào đang có Reflection
    refl_resp = session.get(f"{base_url}/api/v3/reflection", timeout=30)
    refl_resp.raise_for_status()
    reflections = refl_resp.json().get("data", [])

    # Ánh xạ datasetId của các reflection đang bật
    active_dataset_ids = {
        r.get("datasetId")
        for r in reflections
        if r.get("enabled", True) and r.get("datasetId")
    }

    if not active_dataset_ids:
        return refreshed_datasets

    for table in tables:
        catalog_path = f"{catalog}/gold/{table}"
        ds_resp = session.get(f"{base_url}/api/v3/catalog/by-path/{catalog_path}", timeout=30)
        if ds_resp.status_code != 200:
            continue
        ds_id = ds_resp.json().get("id")
        if ds_id in active_dataset_ids:
            sql = f'ALTER DATASET "{catalog}"."gold"."{table}" REFRESH REFLECTIONS'
            _run_dremio_sql(session, sql)
            refreshed_datasets.append(f"{catalog}.gold.{table}")

    return refreshed_datasets


def refresh_gold_and_reflections(
    tables: List[str] = GOLD_TABLES,
    catalog: str = DREMIO_CATALOG,
) -> Dict[str, Any]:
    """
    Hàm công khai gọi bởi Airflow DAG (B-506):
    1. Làm mới metadata toàn bộ bảng Gold.
    2. Làm mới các Reflection liên quan tới bảng Gold.
    """
    session = _login_dremio()
    metadata_refreshed = refresh_gold_metadata(session, tables=tables, catalog=catalog)
    reflections_refreshed = refresh_gold_reflections(session, tables=tables, catalog=catalog)
    return {
        "metadata_refreshed_count": len(metadata_refreshed),
        "reflections_refreshed_count": len(reflections_refreshed),
        "reflections_refreshed": reflections_refreshed,
    }

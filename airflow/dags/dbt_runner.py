"""
airflow/dags/dbt_runner.py - Điều phối chạy dbt build cho Silver và Gold (B-504)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Trạng thái giao diện: Tạm (T-03: dbt-dremio ghi Iceberg vào nguồn Hive)
Tham chiếu:
- Design Orchestration: Bước Silver, Bước Gold, Mục IV (failed_model, rows_out), Độ tin cậy Mức 1
- Design Build: B-504, Mục IV (Luồng dựng Gold: dimension -> fact -> KPI + GT-01..GT-15)
- Requirement v3: F05, F08, F10, F11, F12
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Dict, List, Optional

import requests

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_BIN = os.getenv("DBT_BIN", "dbt")

# Thứ tự dựng Gold theo Design Build (R241-R243)
GOLD_DIMENSIONS = ["dim_customer", "dim_product", "dim_seller", "dim_date"]
GOLD_FACTS = ["fact_order", "fact_order_item", "fact_lead", "fact_session"]
GOLD_KPIS = [
    "kpi_sales_month",
    "kpi_sales_state_month",
    "kpi_delivery_month",
    "kpi_delivery_state_month",
    "kpi_lead_month",
    "kpi_lead_origin",
    "kpi_funnel_day",
    "kpi_funnel_month",
]

SILVER_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "products",
    "sellers",
    "product_category_translation",
    "geolocation",
    "leads",
    "clickstream_events",
]
GOLD_TABLES = GOLD_DIMENSIONS + GOLD_FACTS + GOLD_KPIS


class DbtStepFailedError(RuntimeError):
    """Lỗi khi chạy dbt build, mang theo tên model dbt gây lỗi để ghi ops_batch_run."""

    def __init__(self, message: str, failed_model: Optional[str] = None):
        super().__init__(message)
        self.failed_model = failed_model


def _extract_failed_model(project_dir: str = DBT_PROJECT_DIR) -> Optional[str]:
    """
    Đọc dbt/target/run_results.json để trích xuất tên model dbt đầu tiên gây lỗi (error/fail).
    """
    results_path = os.path.join(project_dir, "target", "run_results.json")
    manifest_path = os.path.join(project_dir, "target", "manifest.json")
    if not os.path.exists(results_path):
        return None

    try:
        with open(results_path, "r", encoding="utf-8") as f:
            run_results = json.load(f)
    except (OSError, ValueError):
        return None

    manifest_nodes: Dict[str, Any] = {}
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest_nodes = json.load(f).get("nodes", {})
        except (OSError, ValueError):
            manifest_nodes = {}

    for item in run_results.get("results", []):
        status = str(item.get("status", "")).lower()
        if status in ("error", "fail", "runtime error"):
            unique_id = item.get("unique_id", "")
            node = manifest_nodes.get(unique_id, {})
            resource_type = node.get("resource_type")

            if resource_type == "model":
                return node.get("name") or unique_id.split(".")[-1]

            if resource_type == "test":
                attached = node.get("attached_node")
                if attached and attached in manifest_nodes:
                    return manifest_nodes[attached].get("name") or attached.split(".")[-1]
                return node.get("name") or unique_id.split(".")[-1]

            if unique_id:
                return unique_id.split(".")[-1]
    return None


def _run_dbt_cli(
    subcommand: List[str],
    batch_id: str,
    project_dir: str = DBT_PROJECT_DIR,
    profiles_dir: str = DBT_PROFILES_DIR,
) -> subprocess.CompletedProcess:
    """Chạy lệnh dbt với tham số batch_id và cờ --fail-fast (dừng ngay ở model lỗi)."""
    vars_json = json.dumps({"batch_id": batch_id})
    cmd = [
        DBT_BIN,
        *subcommand,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--vars",
        vars_json,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        failed_model = _extract_failed_model(project_dir=project_dir)
        err_tail = (result.stdout or result.stderr or "").strip()[-2000:]
        raise DbtStepFailedError(
            f"[DBT LỖI] Lệnh '{' '.join(subcommand)}' (batch={batch_id}) thất bại "
            f"(model lỗi: {failed_model}).\n--- Chi tiết ---\n{err_tail}",
            failed_model=failed_model,
        )
    return result


def run_dbt_silver(
    batch_id: str,
    simulate_fail_model: Optional[str] = None,
    project_dir: str = DBT_PROJECT_DIR,
    profiles_dir: str = DBT_PROFILES_DIR,
) -> Dict[str, Any]:
    """
    Bước Silver (B-301 đến B-323):
    - Hỗ trợ công tắc gây lỗi cố ý cho kịch bản kiểm thử F11.
    - Nạp seed cần thiết (B-014..B-017).
    - Chạy dbt build --select path:models/silver --fail-fast, truyền batch_id.
    """
    sim_model = (simulate_fail_model or os.getenv("LAKEHOUSE_SIMULATE_DBT_FAILURE", "")).strip()
    if sim_model and sim_model.startswith("silver."):
        target = sim_model.split(".", 1)[1]
        raise DbtStepFailedError(
            f"[F11 SIMULATE FAILURE] Gây lỗi cố ý tại model Silver '{target}' (batch={batch_id}).",
            failed_model=target,
        )

    _run_dbt_cli(["seed"], batch_id=batch_id, project_dir=project_dir, profiles_dir=profiles_dir)
    _run_dbt_cli(
        ["build", "--select", "path:models/silver", "--fail-fast"],
        batch_id=batch_id,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    return {"stage": "silver", "batch_id": batch_id, "status": "ok"}


def _query_dremio_row_count(
    session: requests.Session,
    base_url: str,
    schema: str,
    table: str,
    catalog: str = "hive",
) -> int:
    """Truy vấn COUNT(*) của một bảng qua Dremio REST API SQL."""
    sql = f'SELECT COUNT(*) AS cnt FROM "{catalog}"."{schema}"."{table}"'
    resp = session.post(f"{base_url}/api/v3/sql", json={"sql": sql}, timeout=60)
    resp.raise_for_status()
    job_id = resp.json()["id"]

    for _ in range(120):
        status_resp = session.get(f"{base_url}/api/v3/job/{job_id}", timeout=30)
        status_resp.raise_for_status()
        state = status_resp.json().get("jobState")
        if state == "COMPLETED":
            res_resp = session.get(f"{base_url}/api/v3/job/{job_id}/results?offset=0&limit=1", timeout=30)
            res_resp.raise_for_status()
            rows = res_resp.json().get("rows", [])
            return int(rows[0]["cnt"]) if rows else 0
        if state in ("FAILED", "CANCELED", "CANCELLED"):
            raise RuntimeError(f"Dremio job {job_id} thất bại khi đếm {schema}.{table}: {status_resp.text}")
        import time
        time.sleep(1)

    raise TimeoutError(f"Quá thời gian chờ Dremio đếm dòng bảng {schema}.{table}")


def compute_rows_out() -> int:
    """
    Tính rows_out theo Design Orchestration & Design Build (B-504):
    Tổng số dòng của tất cả bảng Silver (11 bảng) và bảng Gold (16 bảng).
    """
    host = os.getenv("DREMIO_HOST", "dremio")
    port = os.getenv("DREMIO_PORT", "9047")
    user = os.getenv("DREMIO_ADMIN_USER", "admin")
    password = os.getenv("DREMIO_ADMIN_PASSWORD", "admin12345")
    base_url = f"http://{host}:{port}"

    session = requests.Session()
    login_resp = session.post(
        f"{base_url}/apiv2/login",
        json={"userName": user, "password": password},
        timeout=30,
    )
    login_resp.raise_for_status()
    token = login_resp.json()["token"]
    session.headers.update({"Authorization": f"_dremio{token}"})

    total = 0
    for tbl in SILVER_TABLES:
        total += _query_dremio_row_count(session, base_url, "silver", tbl)
    for tbl in GOLD_TABLES:
        total += _query_dremio_row_count(session, base_url, "gold", tbl)
    return total


def run_dbt_gold(
    batch_id: str,
    simulate_fail_model: Optional[str] = None,
    project_dir: str = DBT_PROJECT_DIR,
    profiles_dir: str = DBT_PROFILES_DIR,
) -> Dict[str, Any]:
    """
    Bước Gold (B-401 đến B-431) theo đúng 3 chặng trong Design Build (R241-R243):
    1. Dimension (B-401..B-404) kèm kiểm thử trực tiếp
    2. Fact (B-405..B-408) kèm kiểm thử trực tiếp
    3. 8 bảng KPI (B-409..B-416) và kiểm thử liên bảng GT-01..GT-15 (B-417..B-431)
    Sau đó tính tổng dòng rows_out (Silver + Gold). Nếu lỗi trước khi dựng xong -> rows_out là None.
    """
    sim_model = (simulate_fail_model or os.getenv("LAKEHOUSE_SIMULATE_DBT_FAILURE", "")).strip()
    if sim_model and sim_model.startswith("gold."):
        target = sim_model.split(".", 1)[1]
        raise DbtStepFailedError(
            f"[F11 SIMULATE FAILURE] Gây lỗi cố ý tại model Gold '{target}' (batch={batch_id}).",
            failed_model=target,
        )

    # Chặng 1: Dimensions
    _run_dbt_cli(
        ["build", "--select", *GOLD_DIMENSIONS, "--fail-fast"],
        batch_id=batch_id,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    # Chặng 2: Facts
    _run_dbt_cli(
        ["build", "--select", *GOLD_FACTS, "--fail-fast"],
        batch_id=batch_id,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    # Chặng 3: KPIs + Kiểm thử liên bảng Gold (GT-01..GT-15)
    _run_dbt_cli(
        ["build", "--select", *GOLD_KPIS, "--fail-fast"],
        batch_id=batch_id,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    _run_dbt_cli(
        ["test", "--select", "path:tests/gold_gt*", "--fail-fast"],
        batch_id=batch_id,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    rows_out = compute_rows_out()
    return {"stage": "gold", "batch_id": batch_id, "status": "ok", "rows_out": rows_out}

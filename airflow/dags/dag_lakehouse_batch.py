"""
airflow/dags/dag_lakehouse_batch.py - DAG tổng điều phối Lakehouse theo batch (B-506)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Trạng thái giao diện: Tạm (T-01, T-02, T-04)
Tham chiếu:
- Design Orchestration: Mục II (DAG và tham số, Các bước của một lượt chạy, Quy tắc điều phối)
- Design Build: B-506, Mục IV (Luồng chạy một batch)
- Requirement v3: F04, F05, F11, F12

Quy tắc bắt buộc:
- Tên DAG: dag_lakehouse_batch
- Kích hoạt thủ công (schedule=None), không lập lịch.
- max_active_runs=1: tại một thời điểm chỉ một lượt chạy, lượt thêm xếp hàng chờ.
- Tham số 'batch': một tên batch hợp lệ hoặc 'tất cả' (mở thành 4 lượt nối tiếp theo batch_seq,
  dừng ở lượt lỗi đầu tiên, không chạy lượt sau).
- Thử lại: 2 lần, cách nhau 1 phút cho Raw->Bronze, Gatekeeper, Silver, Gold, Làm mới Dremio.
  Không thử lại cho: Kiểm tra thứ tự, Chờ _SUCCESS, Ghi nhận vận hành.
- Chờ _SUCCESS: không đặt hạn chờ, kiểm tra đủ 3 nguồn (csv, marketing_funnel, clickstream),
  bỏ qua thư mục _excluded/.
- Ghi nhận vận hành: luôn chạy (TriggerRule.ALL_DONE) cho mọi lượt được thực thi,
  kể cả khi lỗi ở bước Kiểm tra thứ tự.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

import batch_order
import dbt_runner
import dremio_refresh
import ops_log
import spark_runner

ORDERED_BATCHES: List[str] = batch_order.get_ordered_batch_ids()
RAW_SOURCES = ["csv", "marketing_funnel", "clickstream"]
RAW_BASE_DIR = os.getenv("RAW_BASE_DIR", "/opt/airflow/raw")
SUCCESS_POLL_SECONDS = int(os.getenv("SUCCESS_POLL_SECONDS", "10"))


def _safe_group_id(batch_id: str) -> str:
    return f"batch_{batch_id.replace('-', '_')}"


def _is_batch_selected(batch_id: str, dag_run_conf: dict, params: dict) -> bool:
    raw_param = (dag_run_conf or {}).get("batch") or (params or {}).get("batch") or "tất cả"
    selected = batch_order.resolve_batches(raw_param)
    return batch_id in selected


def _task_gate_and_start(batch_id: str, **context) -> str:
    """
    Kiểm tra xem batch_id có nằm trong danh sách cần chạy của lượt kích hoạt này không.
    Nếu không được chọn -> skip toàn bộ TaskGroup của batch này (không ghi ops_log).
    Nếu được chọn -> ghi nhận mốc started_at (F12, tính cả thời gian chờ _SUCCESS).
    """
    dag_run = context["dag_run"]
    if not _is_batch_selected(batch_id, dag_run.conf, context.get("params", {})):
        raise AirflowSkipException(f"Bỏ qua batch '{batch_id}' theo tham số kích hoạt.")

    started_at = datetime.now(timezone.utc).isoformat()
    context["ti"].xcom_push(key="started_at", value=started_at)
    return started_at


def _task_check_order(batch_id: str, **context) -> dict:
    """Bước 1: Kiểm tra thứ tự batch (F11) — không thử lại."""
    return batch_order.check_batch_order(batch_id)


def _check_all_success_markers(batch_id: str) -> bool:
    """
    Kiểm tra tín hiệu _SUCCESS cho cả 3 nguồn (csv, marketing_funnel, clickstream)
    của batch_id. Luôn bỏ qua thư mục _excluded/.
    - Ưu tiên 1: kiểm tra trực tiếp trên thư mục mount cục bộ RAW_BASE_DIR (nếu có).
    - Ưu tiên 2: kiểm tra qua container Spark (/home/raw/<source>/<batch_id>/_SUCCESS,
      đã mount từ ../raw trong infra/docker-compose.yml).
    """
    if os.path.isdir(RAW_BASE_DIR):
        for source in RAW_SOURCES:
            marker_path = os.path.join(RAW_BASE_DIR, source, batch_id, "_SUCCESS")
            if not os.path.isfile(marker_path):
                return False
        return True

    spark_raw_dir = os.getenv("SPARK_RAW_DIR", "/home/raw").rstrip("/")
    check_expr = " && ".join(
        f"test -f '{spark_raw_dir}/{source}/{batch_id}/_SUCCESS'" for source in RAW_SOURCES
    )
    res = spark_runner._exec_in_spark_container(["bash", "-lc", check_expr])
    return res.returncode == 0


def _task_wait_success(batch_id: str, **context) -> bool:
    """
    Bước 2: Chờ tín hiệu _SUCCESS của batch trong Raw — không đặt hạn chờ (Design Orchestration R121).
    """
    while not _check_all_success_markers(batch_id):
        time.sleep(SUCCESS_POLL_SECONDS)
    return True


def _task_raw_to_bronze(batch_id: str, **context) -> list:
    """Bước 3: Raw -> Bronze (Spark B-202, B-203, B-204)."""
    return spark_runner.run_bronze_ingestion(batch_id=batch_id)


def _task_gatekeeper(batch_id: str, **context) -> int:
    """Bước 4: Gatekeeper (Spark B-205) — trả về rows_in."""
    rows_in = spark_runner.run_gatekeeper(batch_id=batch_id)
    context["ti"].xcom_push(key="rows_in", value=rows_in)
    return rows_in


def _task_dbt_silver(batch_id: str, **context) -> dict:
    """Bước 5: Silver (dbt build --fail-fast, truyền tham số batch_id)."""
    dag_run = context["dag_run"]
    sim_fail = (dag_run.conf or {}).get("simulate_dbt_failure_model") or context.get("params", {}).get("simulate_dbt_failure_model") or ""
    try:
        return dbt_runner.run_dbt_silver(batch_id=batch_id, simulate_fail_model=sim_fail)
    except dbt_runner.DbtStepFailedError as exc:
        context["ti"].xcom_push(key="failed_model", value=exc.failed_model)
        raise


def _task_dbt_gold(batch_id: str, **context) -> dict:
    """Bước 6: Gold (dbt build dimension -> fact -> KPI + GT-01..GT-15) và tính rows_out."""
    dag_run = context["dag_run"]
    sim_fail = (dag_run.conf or {}).get("simulate_dbt_failure_model") or context.get("params", {}).get("simulate_dbt_failure_model") or ""
    try:
        result = dbt_runner.run_dbt_gold(batch_id=batch_id, simulate_fail_model=sim_fail)
        context["ti"].xcom_push(key="rows_out", value=result.get("rows_out"))
        return result
    except dbt_runner.DbtStepFailedError as exc:
        context["ti"].xcom_push(key="failed_model", value=exc.failed_model)
        raise


def _task_refresh_dremio(batch_id: str, **context) -> dict:
    """Bước 7: Làm mới Dremio (metadata Gold + Reflection) — chỉ chạy khi Silver & Gold đạt."""
    return dremio_refresh.refresh_gold_and_reflections()


def _task_record_ops_log(batch_id: str, group_id: str, **context) -> dict:
    """
    Bước 8: Ghi nhận vận hành (F12) — luôn chạy kể cả khi lỗi (TriggerRule.ALL_DONE),
    nhưng bỏ qua nếu chính batch này không được chọn trong lượt kích hoạt.
    Nếu lượt chạy của batch này thất bại, ném RuntimeError SAU KHI đã ghi ops_batch_run
    để:
    - Đánh dấu DAG Run ở trạng thái failed (F11)
    - Chặn mọi batch sau không được chạy khi chạy chế độ 'tất cả'.
    """
    dag_run = context["dag_run"]
    ti = context["ti"]

    if not _is_batch_selected(batch_id, dag_run.conf, context.get("params", {})):
        raise AirflowSkipException(f"Batch '{batch_id}' không được chọn; không ghi ops_batch_run.")

    # Nếu batch trước trong chế độ 'tất cả' đã thất bại khiến gate bị upstream_failed -> không chạy & không ghi
    gate_ti = dag_run.get_task_instance(f"{group_id}.gate_start")
    if gate_ti and gate_ti.state in (State.UPSTREAM_FAILED, State.SKIPPED):
        raise AirflowSkipException(
            f"Batch '{batch_id}' chưa từng khởi chạy do trạng thái gate_start={gate_ti.state}."
        )

    started_at = ti.xcom_pull(task_ids=f"{group_id}.gate_start", key="started_at")
    rows_in = ti.xcom_pull(task_ids=f"{group_id}.gatekeeper", key="rows_in")
    rows_out = ti.xcom_pull(task_ids=f"{group_id}.dbt_gold", key="rows_out")
    failed_model = (
        ti.xcom_pull(task_ids=f"{group_id}.dbt_silver", key="failed_model")
        or ti.xcom_pull(task_ids=f"{group_id}.dbt_gold", key="failed_model")
    )

    step_ids = [
        f"{group_id}.check_order",
        f"{group_id}.wait_success_marker",
        f"{group_id}.raw_to_bronze",
        f"{group_id}.gatekeeper",
        f"{group_id}.dbt_silver",
        f"{group_id}.dbt_gold",
        f"{group_id}.refresh_dremio",
    ]

    all_succeeded = True
    failed_steps: List[str] = []
    for step_id in step_ids:
        step_ti = dag_run.get_task_instance(step_id)
        if not step_ti or step_ti.state != State.SUCCESS:
            all_succeeded = False
            failed_steps.append(f"{step_id}({step_ti.state if step_ti else 'none'})")

    status = ops_log.STATUS_SUCCESS if all_succeeded else ops_log.STATUS_FAILED
    if not all_succeeded:
        # Theo Design Orchestration: rows_out là NULL nếu lỗi trước khi dựng xong
        gold_ti = dag_run.get_task_instance(f"{group_id}.dbt_gold")
        if not gold_ti or gold_ti.state != State.SUCCESS:
            rows_out = None

    run_record_id = f"{dag_run.run_id}__{batch_id}"
    record = ops_log.record_batch_run(
        run_id=run_record_id,
        batch_id=batch_id,
        status=status,
        failed_model=failed_model,
        rows_in=rows_in,
        rows_out=rows_out,
        started_at=started_at,
        ended_at=datetime.now(timezone.utc),
    )

    if not all_succeeded:
        raise RuntimeError(
            f"[LƯỢT CHẠY LỖI] Batch '{batch_id}' thất bại tại: {', '.join(failed_steps)}. "
            f"Đã ghi Nhật ký vận hành (run_id={run_record_id}, failed_model={failed_model})."
        )

    return record


with DAG(
    dag_id="dag_lakehouse_batch",
    description="DAG tổng điều phối E-commerce Data Lakehouse theo batch (F11, F12)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "batch", "f11", "f12"],
    params={
        "batch": Param(
            default="tất cả",
            type="string",
            enum=["tất cả", *ORDERED_BATCHES],
            description="Tên batch cần chạy hoặc 'tất cả' để chạy 4 lượt nối tiếp theo batch_seq.",
        ),
        "simulate_dbt_failure_model": Param(
            default="",
            type="string",
            description="Công tắc gây lỗi cố ý cho F11 (ví dụ: 'silver.orders' hoặc 'gold.fact_order'). Để trống khi chạy thường.",
        ),
    },
) as dag:
    previous_group_end = None

    for b_id in ORDERED_BATCHES:
        gid = _safe_group_id(b_id)
        with TaskGroup(group_id=gid, tooltip=f"Lượt chạy batch {b_id}") as tg:
            t_gate = PythonOperator(
                task_id="gate_start",
                python_callable=_task_gate_and_start,
                op_kwargs={"batch_id": b_id},
                retries=0,
                trigger_rule=TriggerRule.NONE_FAILED,
            )

            t_check_order = PythonOperator(
                task_id="check_order",
                python_callable=_task_check_order,
                op_kwargs={"batch_id": b_id},
                retries=0,
            )

            t_wait_success = PythonOperator(
                task_id="wait_success_marker",
                python_callable=_task_wait_success,
                op_kwargs={"batch_id": b_id},
                retries=0,
            )

            t_bronze = PythonOperator(
                task_id="raw_to_bronze",
                python_callable=_task_raw_to_bronze,
                op_kwargs={"batch_id": b_id},
                retries=2,
                retry_delay=timedelta(minutes=1),
            )

            t_gatekeeper = PythonOperator(
                task_id="gatekeeper",
                python_callable=_task_gatekeeper,
                op_kwargs={"batch_id": b_id},
                retries=2,
                retry_delay=timedelta(minutes=1),
            )

            t_silver = PythonOperator(
                task_id="dbt_silver",
                python_callable=_task_dbt_silver,
                op_kwargs={"batch_id": b_id},
                retries=2,
                retry_delay=timedelta(minutes=1),
            )

            t_gold = PythonOperator(
                task_id="dbt_gold",
                python_callable=_task_dbt_gold,
                op_kwargs={"batch_id": b_id},
                retries=2,
                retry_delay=timedelta(minutes=1),
            )

            t_dremio = PythonOperator(
                task_id="refresh_dremio",
                python_callable=_task_refresh_dremio,
                op_kwargs={"batch_id": b_id},
                retries=2,
                retry_delay=timedelta(minutes=1),
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            t_ops_log = PythonOperator(
                task_id="record_ops_log",
                python_callable=_task_record_ops_log,
                op_kwargs={"batch_id": b_id, "group_id": gid},
                retries=0,
                trigger_rule=TriggerRule.ALL_DONE,
            )

            (
                t_gate
                >> t_check_order
                >> t_wait_success
                >> t_bronze
                >> t_gatekeeper
                >> t_silver
                >> t_gold
                >> t_dremio
                >> t_ops_log
            )

        if previous_group_end is not None:
            previous_group_end >> t_gate
        previous_group_end = t_ops_log

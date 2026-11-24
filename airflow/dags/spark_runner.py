"""
airflow/dags/spark_runner.py - Điều phối chạy tác vụ Spark trong container Spark (B-503)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Trạng thái giao diện: Tạm (T-01: Airflow gọi Spark, T-02: Tiến trình Spark còn sót)
Tham chiếu:
- Design Orchestration: Bước Raw -> Bronze, Bước Gatekeeper
- Design Build: B-503 (gọi B-202, B-203, B-204, B-205)
- Requirement v3: F03, F04, F05, F11
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from typing import Dict, List, Optional

SPARK_CONTAINER_NAME = os.getenv("SPARK_CONTAINER_NAME", "spark")
SPARK_SCRIPTS_DIR = os.getenv("SPARK_SCRIPTS_DIR", "/home/spark")
SPARK_SUBMIT_BIN = os.getenv("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")

BRONZE_SCRIPTS = [
    "bronze_csv.py",         # B-202 (Olist 9 bảng CSV)
    "bronze_marketing.py",   # B-203 (Marketing JSONL)
    "bronze_clickstream.py", # B-204 (Clickstream access_log.txt)
]
GATEKEEPER_SCRIPT = "gatekeeper.py"  # B-205


def _exec_in_spark_container(command: List[str], container_name: str = SPARK_CONTAINER_NAME) -> subprocess.CompletedProcess:
    """
    Thực thi lệnh bên trong container Spark thông qua Docker CLI (T-01).
    Airflow container được mount /var/run/docker.sock và cài sẵn docker.io.
    """
    full_cmd = ["docker", "exec", container_name] + command
    return subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        check=False,
    )


def kill_stale_spark_processes(
    container_name: str = SPARK_CONTAINER_NAME,
    wait_seconds: float = 2.0,
) -> None:
    """
    Dừng mọi tiến trình Spark còn sót trước khi chạy hoặc thử lại (T-02).
    Giúp tránh xung đột bộ nhớ (giới hạn 2GB của container spark) và khóa tệp.
    """
    kill_cmd = [
        "bash",
        "-lc",
        "pkill -9 -f 'org.apache.spark.deploy.SparkSubmit|bronze_csv.py|bronze_marketing.py|bronze_clickstream.py|gatekeeper.py' || true",
    ]
    _exec_in_spark_container(kill_cmd, container_name=container_name)
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def run_spark_script(
    script_name: str,
    batch_id: str,
    extra_args: Optional[List[str]] = None,
    container_name: str = SPARK_CONTAINER_NAME,
) -> Dict[str, object]:
    """
    Dừng tiến trình Spark còn sót (T-02), sau đó chạy spark-submit cho script_name với --batch_id.
    Ném RuntimeError nếu mã thoát khác 0 để Airflow kích hoạt cơ chế thử lại.
    """
    kill_stale_spark_processes(container_name=container_name)

    script_path = f"{SPARK_SCRIPTS_DIR.rstrip('/')}/{script_name}"
    cmd = [SPARK_SUBMIT_BIN, script_path, "--batch_id", batch_id]
    if extra_args:
        cmd.extend(extra_args)

    result = _exec_in_spark_container(cmd, container_name=container_name)
    if result.returncode != 0:
        err_tail = (result.stderr or result.stdout or "").strip()[-2000:]
        raise RuntimeError(
            f"[SPARK LỖI] Tác vụ '{script_name}' (batch={batch_id}) thất bại "
            f"với mã thoát {result.returncode}.\n--- Chi tiết ---\n{err_tail}"
        )

    return {
        "script": script_name,
        "batch_id": batch_id,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def run_bronze_ingestion(
    batch_id: str,
    container_name: str = SPARK_CONTAINER_NAME,
) -> List[Dict[str, object]]:
    """
    Bước Raw -> Bronze: chạy lần lượt 3 tác vụ Spark nạp CSV, Marketing và Clickstream (B-202..B-204).
    """
    results: List[Dict[str, object]] = []
    for script in BRONZE_SCRIPTS:
        res = run_spark_script(script, batch_id=batch_id, container_name=container_name)
        results.append(
            {
                "script": script,
                "batch_id": batch_id,
                "returncode": res["returncode"],
            }
        )
    return results


def _parse_rows_in_from_output(output: str) -> Optional[int]:
    """
    Trích xuất số dòng Bronze của batch (rows_in) từ stdout của gatekeeper.py (B-205).
    Hỗ trợ các định dạng đầu ra chuẩn:
    - JSON line chứa khóa 'rows_in'
    - Mẫu 'ROWS_IN=12345' hoặc 'rows_in: 12345'
    """
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                data = json.loads(stripped)
                if "rows_in" in data and data["rows_in"] is not None:
                    return int(data["rows_in"])
            except (ValueError, TypeError):
                pass

        match = re.search(r"(?:ROWS_IN|rows_in)\s*[:=]\s*([0-9,]+)", stripped)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


def run_gatekeeper(
    batch_id: str,
    container_name: str = SPARK_CONTAINER_NAME,
) -> int:
    """
    Bước Gatekeeper (B-205): so khớp số dòng Bronze với Raw của batch.
    Trả về rows_in (tổng số dòng Bronze của batch) để ghi vào ops_batch_run (F12).
    """
    res = run_spark_script(GATEKEEPER_SCRIPT, batch_id=batch_id, container_name=container_name)
    combined_out = f"{res.get('stdout', '')}\n{res.get('stderr', '')}"
    rows_in = _parse_rows_in_from_output(combined_out)
    if rows_in is None:
        raise RuntimeError(
            f"[GATEKEEPER] Không đọc được giá trị rows_in từ đầu ra của {GATEKEEPER_SCRIPT} "
            f"(batch={batch_id}). Cần in 'ROWS_IN=<số>' hoặc JSON chứa khóa 'rows_in'."
        )
    return rows_in

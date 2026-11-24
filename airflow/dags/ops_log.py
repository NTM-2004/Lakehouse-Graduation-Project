"""
airflow/dags/ops_log.py - Thư viện ghi và đọc Nhật ký vận hành (B-501)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Design Orchestration: Mục IV (Nhật ký vận hành - bảng ops_batch_run), Mục VI (Quyết định thiết kế)
- Design Build: B-501
- Requirement v3: F12

Bảng ops_batch_run lưu trong Airflow Metadata DB (PostgreSQL):
  run_id        VARCHAR(255) PRIMARY KEY
  batch_id      VARCHAR(64)  NOT NULL
  status        VARCHAR(32)  NOT NULL ('thành công' hoặc 'lỗi')
  failed_model  VARCHAR(255) NULL
  rows_in       BIGINT       NULL
  rows_out      BIGINT       NULL
  started_at    TIMESTAMPTZ  NOT NULL
  ended_at      TIMESTAMPTZ  NOT NULL
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor

STATUS_SUCCESS = "thành công"
STATUS_FAILED = "lỗi"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops_batch_run (
    run_id       VARCHAR(255) PRIMARY KEY,
    batch_id     VARCHAR(64)  NOT NULL,
    status       VARCHAR(32)  NOT NULL,
    failed_model VARCHAR(255) NULL,
    rows_in      BIGINT       NULL,
    rows_out     BIGINT       NULL,
    started_at   TIMESTAMPTZ  NOT NULL,
    ended_at     TIMESTAMPTZ  NOT NULL
);
"""

UPSERT_RUN_SQL = """
INSERT INTO ops_batch_run (
    run_id, batch_id, status, failed_model, rows_in, rows_out, started_at, ended_at
) VALUES (
    %(run_id)s, %(batch_id)s, %(status)s, %(failed_model)s,
    %(rows_in)s, %(rows_out)s, %(started_at)s, %(ended_at)s
)
ON CONFLICT (run_id) DO UPDATE SET
    batch_id     = EXCLUDED.batch_id,
    status       = EXCLUDED.status,
    failed_model = EXCLUDED.failed_model,
    rows_in      = EXCLUDED.rows_in,
    rows_out     = EXCLUDED.rows_out,
    started_at   = EXCLUDED.started_at,
    ended_at     = EXCLUDED.ended_at;
"""


def _get_db_connection_params() -> Dict[str, Any]:
    """Đọc cấu hình kết nối Airflow Metadata DB từ biến môi trường."""
    conn_uri = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if conn_uri:
        normalized = conn_uri.replace("postgresql+psycopg2://", "postgresql://")
        parsed = urlparse(normalized)
        return {
            "host": parsed.hostname or "airflow-postgres",
            "port": parsed.port or 5432,
            "user": parsed.username or os.getenv("AIRFLOW_DB_USER", "airflow"),
            "password": parsed.password or os.getenv("AIRFLOW_DB_PASSWORD", "airflow"),
            "dbname": (parsed.path or "/airflow").lstrip("/") or "airflow",
        }
    return {
        "host": os.getenv("AIRFLOW_DB_HOST", "airflow-postgres"),
        "port": int(os.getenv("AIRFLOW_DB_PORT", "5432")),
        "user": os.getenv("AIRFLOW_DB_USER", "airflow"),
        "password": os.getenv("AIRFLOW_DB_PASSWORD", "airflow"),
        "dbname": os.getenv("AIRFLOW_DB_NAME", "airflow"),
    }


def _connect():
    return psycopg2.connect(**_get_db_connection_params())


def ensure_ops_table() -> None:
    """Đảm bảo bảng ops_batch_run đã tồn tại trong Airflow Metadata DB."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()


def _to_utc_datetime(value: Optional[Any]) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise TypeError(f"Không thể chuyển đổi giá trị thời gian: {value!r}")


def record_batch_run(
    run_id: str,
    batch_id: str,
    status: str,
    failed_model: Optional[str] = None,
    rows_in: Optional[int] = None,
    rows_out: Optional[int] = None,
    started_at: Optional[Any] = None,
    ended_at: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Ghi một dòng Nhật ký vận hành (F12) vào bảng ops_batch_run.
    """
    if status not in (STATUS_SUCCESS, STATUS_FAILED):
        raise ValueError(
            f"Trạng thái không hợp lệ: {status!r}. Chỉ chấp nhận '{STATUS_SUCCESS}' hoặc '{STATUS_FAILED}'."
        )

    ensure_ops_table()
    payload = {
        "run_id": str(run_id),
        "batch_id": str(batch_id),
        "status": status,
        "failed_model": failed_model,
        "rows_in": int(rows_in) if rows_in is not None else None,
        "rows_out": int(rows_out) if rows_out is not None else None,
        "started_at": _to_utc_datetime(started_at),
        "ended_at": _to_utc_datetime(ended_at),
    }

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_RUN_SQL, payload)
        conn.commit()

    return payload


def get_latest_batch_status(batch_id: str) -> Optional[Dict[str, Any]]:
    """
    Lấy bản ghi lượt chạy gần nhất của batch_id trong ops_batch_run.
    Trả về None nếu batch chưa từng được chạy.
    """
    ensure_ops_table()
    query = """
        SELECT run_id, batch_id, status, failed_model, rows_in, rows_out, started_at, ended_at
        FROM ops_batch_run
        WHERE batch_id = %s
        ORDER BY ended_at DESC, started_at DESC
        LIMIT 1;
    """
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (batch_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def list_batch_runs(
    batch_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Liệt kê lịch sử lượt chạy từ ops_batch_run theo thời gian mới nhất."""
    ensure_ops_table()
    with _connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if batch_id:
                cur.execute(
                    """
                    SELECT run_id, batch_id, status, failed_model, rows_in, rows_out, started_at, ended_at
                    FROM ops_batch_run
                    WHERE batch_id = %s
                    ORDER BY ended_at DESC, started_at DESC
                    LIMIT %s;
                    """,
                    (batch_id, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT run_id, batch_id, status, failed_model, rows_in, rows_out, started_at, ended_at
                    FROM ops_batch_run
                    ORDER BY ended_at DESC, started_at DESC
                    LIMIT %s;
                    """,
                    (limit,),
                )
            return [dict(r) for r in cur.fetchall()]

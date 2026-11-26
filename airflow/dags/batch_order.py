"""
airflow/dags/batch_order.py - Quản lý thứ tự batch và Kiểm tra thứ tự (B-502)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Design Orchestration: Mục II (Tham số, Kiểm tra thứ tự, Quy tắc Thứ tự batch), Mục VI (Batch cuối)
- Design Build: B-502 (đọc seed dbt/seeds/batch_seq.csv và ops_batch_run)
- Requirement v3: F11
"""

from __future__ import annotations

import csv
import os
from typing import Dict, List, Optional, Tuple

import ops_log

ALL_BATCHES_ALIASES = {"tất cả", "tat ca", "all"}

DEFAULT_BATCH_SEQUENCE: List[Tuple[str, int]] = [
    ("bulk_2016-09_2018-05", 0),
    ("2018-06", 1),
    ("2018-07", 2),
    ("2018-08", 3),
]


def _default_seed_path() -> str:
    env_path = os.getenv("BATCH_SEQ_SEED_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    candidates = [
        "/opt/airflow/dbt/seeds/batch_seq.csv",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "dbt",
            "seeds",
            "batch_seq.csv",
        ),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return candidates[0]


def load_batch_sequence(seed_path: Optional[str] = None) -> List[Tuple[str, int]]:
    """
    Đọc danh sách (batch_id, batch_seq) đã sắp xếp tăng dần theo batch_seq từ seed B-014.
    """
    path = seed_path or _default_seed_path()
    if not os.path.exists(path):
        return list(DEFAULT_BATCH_SEQUENCE)

    rows: List[Tuple[str, int]] = []
    with open(path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            b_id = row["batch_id"].strip()
            b_seq = int(row["batch_seq"])
            rows.append((b_id, b_seq))

    rows.sort(key=lambda item: item[1])
    return rows


def get_ordered_batch_ids(seed_path: Optional[str] = None) -> List[str]:
    """Trả về danh sách batch_id theo đúng thứ tự batch_seq tăng dần."""
    return [b_id for b_id, _ in load_batch_sequence(seed_path)]


def get_final_batch_id(seed_path: Optional[str] = None) -> str:
    """Xác định Batch cuối (batch có batch_seq lớn nhất)."""
    seq = load_batch_sequence(seed_path)
    return seq[-1][0]


def is_final_batch(batch_id: str, seed_path: Optional[str] = None) -> bool:
    """Kiểm tra batch_id có phải Batch cuối hay không."""
    return batch_id.strip() == get_final_batch_id(seed_path)


def get_previous_batch_id(batch_id: str, seed_path: Optional[str] = None) -> Optional[str]:
    """Trả về batch liền trước theo batch_seq, hoặc None nếu là batch đầu tiên (bulk)."""
    ordered = get_ordered_batch_ids(seed_path)
    if batch_id not in ordered:
        raise ValueError(f"Batch không hợp lệ: {batch_id!r}. Danh sách hợp lệ: {ordered}")
    idx = ordered.index(batch_id)
    return ordered[idx - 1] if idx > 0 else None


def resolve_batches(batch_param: str, seed_path: Optional[str] = None) -> List[str]:
    """
    Giải tham số batch:
    - 'tất cả' (hoặc 'all'): mở thành 4 lượt nối tiếp theo batch_seq.
    - Một tên batch hợp lệ: trả về danh sách gồm đúng 1 batch đó.
    """
    if not batch_param or not str(batch_param).strip():
        raise ValueError("Tham số 'batch' không được để trống.")

    cleaned = str(batch_param).strip()
    ordered = get_ordered_batch_ids(seed_path)

    if cleaned.lower() in ALL_BATCHES_ALIASES:
        return ordered

    if cleaned in ordered:
        return [cleaned]

    raise ValueError(
        f"Tham số batch='{cleaned}' không hợp lệ. "
        f"Chỉ chấp nhận 'tất cả' hoặc một trong {ordered}."
    )


def check_batch_order(batch_id: str, seed_path: Optional[str] = None) -> Dict[str, object]:
    """
    Bước Kiểm tra thứ tự (F11):
    - Batch đầu (bulk_2016-09_2018-05): luôn được phép chạy.
    - Batch sau: lượt chạy gần nhất của batch liền trước trong ops_batch_run phải có
      status == 'thành công'. Nếu không, ném RuntimeError để chặn lượt chạy.
    """
    prev_batch = get_previous_batch_id(batch_id, seed_path)
    if prev_batch is None:
        return {
            "batch_id": batch_id,
            "previous_batch": None,
            "is_final_batch": is_final_batch(batch_id, seed_path),
            "order_valid": True,
        }

    latest_prev = ops_log.get_latest_batch_status(prev_batch)
    if not latest_prev:
        raise RuntimeError(
            f"[CHẶN THỨ TỰ F11] Không thể chạy batch '{batch_id}': "
            f"batch liền trước '{prev_batch}' chưa có lượt chạy nào trong ops_batch_run."
        )

    if latest_prev.get("status") != ops_log.STATUS_SUCCESS:
        raise RuntimeError(
            f"[CHẶN THỨ TỰ F11] Không thể chạy batch '{batch_id}': "
            f"lượt gần nhất của batch liền trước '{prev_batch}' có trạng thái "
            f"'{latest_prev.get('status')}' (run_id={latest_prev.get('run_id')}), "
            f"yêu cầu '{ops_log.STATUS_SUCCESS}'."
        )

    return {
        "batch_id": batch_id,
        "previous_batch": prev_batch,
        "previous_run_id": latest_prev.get("run_id"),
        "is_final_batch": is_final_batch(batch_id, seed_path),
        "order_valid": True,
    }

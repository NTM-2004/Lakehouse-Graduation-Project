"""
spark/f06_snapshots.py - Kịch bản nghiệm thu F06: Iceberg Time Travel & Snapshot Rollback (B-206)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Requirement v3: F06 (Time Travel - 5 bước nghiệm thu)
- Design Bronze, Silver v2: Mục V (F06: bảng có dữ liệu ở bulk và 2018-06, chạy trên bảng thử ngoài DAG)
- Design Build: B-206

5 bước nghiệm thu F06:
1. Nạp bulk vào bảng thử; ghi snapshot S1 và số dòng.
2. Nạp 2018-06 vào bảng thử; ghi snapshot S2; xác nhận số dòng tăng đúng bằng số dòng batch 2018-06.
3. Truy vấn tại S1 theo mã snapshot (VERSION AS OF) và theo thời điểm (TIMESTAMP AS OF);
   xác nhận số dòng bằng bước 1; liệt kê lịch sử snapshot.
4. Khôi phục bảng thử về S1 (rollback_to_snapshot); xác nhận số dòng bằng bước 1;
   nạp lại 2018-06 để trở về trạng thái bước 2.
5. Làm mới metadata bảng thử trên Dremio REST API và xác nhận Dremio đọc đúng số dòng hiện hành.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import timedelta
from urllib import request as urllib_request

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from spark.bronze_common import BATCH_2018_06, BATCH_BULK, get_spark_session


def _http_json(url: str, method: str = "GET", payload: dict | None = None, headers: dict | None = None, timeout: int = 30) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib_request.Request(url, data=data, headers=req_headers, method=method)
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _dremio_refresh_and_count(table_name: str, catalog: str = "hive", schema: str = "bronze") -> int:
    """Bước 5 F06: làm mới metadata bảng thử trên Dremio và đọc COUNT(*) (dùng urllib chuẩn, không cần gói ngoài)."""
    host = os.getenv("DREMIO_HOST", "dremio")
    port = os.getenv("DREMIO_PORT", "9047")
    user = os.getenv("DREMIO_ADMIN_USER", "").strip()
    password = os.getenv("DREMIO_ADMIN_PASSWORD", "").strip()
    if not user or not password:
        raise RuntimeError(
            "Thiếu biến môi trường DREMIO_ADMIN_USER hoặc DREMIO_ADMIN_PASSWORD để xác minh bước 5 F06."
        )

    base_url = f"http://{host}:{port}"
    login_data = _http_json(
        f"{base_url}/apiv2/login",
        method="POST",
        payload={"userName": user, "password": password},
    )
    auth_headers = {"Authorization": f"_dremio{login_data['token']}"}

    def _run_sql(sql: str):
        job_data = _http_json(
            f"{base_url}/api/v3/sql",
            method="POST",
            payload={"sql": sql},
            headers=auth_headers,
            timeout=60,
        )
        job_id = job_data["id"]
        for _ in range(120):
            st = _http_json(f"{base_url}/api/v3/job/{job_id}", headers=auth_headers)
            state = st.get("jobState")
            if state == "COMPLETED":
                res = _http_json(
                    f"{base_url}/api/v3/job/{job_id}/results?offset=0&limit=1",
                    headers=auth_headers,
                )
                return res.get("rows", [])
            if state in ("FAILED", "CANCELED", "CANCELLED"):
                raise RuntimeError(f"Dremio job {job_id} thất bại cho SQL '{sql}': {st}")
            time.sleep(1.5)
        raise TimeoutError(f"Timeout chờ Dremio cho SQL: {sql}")

    _run_sql(f'ALTER PDS "{catalog}"."{schema}"."{table_name}" REFRESH METADATA FORCE UPDATE')
    rows = _run_sql(f'SELECT COUNT(*) AS cnt FROM "{catalog}"."{schema}"."{table_name}"')
    return int(rows[0]["cnt"]) if rows else 0


def run_f06(
    source_table: str = "clickstream_events",
    test_table: str = "clickstream_events_f06_test",
    skip_dremio: bool = False,
    cleanup: bool = False,
) -> None:
    spark = get_spark_session(app_name="F06_TimeTravel_Verification")
    src_full = f"hive.bronze.{source_table}"
    dst_full = f"hive.bronze.{test_table}"

    try:
        if not spark.catalog.tableExists(src_full):
            raise RuntimeError(f"Bảng nguồn {src_full} chưa tồn tại. Cần nạp Bronze trước khi chạy F06.")

        warehouse_base = os.getenv("WAREHOUSE_PATH", "s3a://lakehouse/").rstrip("/")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS hive.bronze LOCATION '{warehouse_base}/bronze'")
        spark.sql(f"DROP TABLE IF EXISTS {dst_full}")

        # Lấy đúng 2 batch bulk và 2018-06 từ bảng nguồn (không sửa đổi dữ liệu thật)
        bulk_df = spark.sql(f"SELECT * FROM {src_full} WHERE batch_id = '{BATCH_BULK}'")
        b06_df = spark.sql(f"SELECT * FROM {src_full} WHERE batch_id = '{BATCH_2018_06}'")
        expected_bulk = bulk_df.count()
        expected_b06 = b06_df.count()
        if expected_bulk == 0 or expected_b06 == 0:
            raise RuntimeError(
                f"Bảng nguồn {src_full} phải có dữ liệu ở cả {BATCH_BULK} ({expected_bulk}) "
                f"và {BATCH_2018_06} ({expected_b06})."
            )

        # ---------------- BƯỚC 1: Nạp bulk -> Snapshot S1 ----------------
        print(f"=== F06 BƯỚC 1: Nạp {BATCH_BULK} vào bảng thử {dst_full} ===")
        (
            bulk_df.writeTo(dst_full)
            .partitionedBy("batch_id")
            .tableProperty("write.format.default", "parquet")
            .create()
        )
        snaps_1 = spark.sql(
            f"SELECT snapshot_id, committed_at FROM {dst_full}.snapshots ORDER BY committed_at DESC"
        ).collect()
        s1_id = snaps_1[0]["snapshot_id"]
        s1_ts = snaps_1[0]["committed_at"]
        count_s1 = spark.sql(f"SELECT COUNT(*) AS c FROM {dst_full}").collect()[0]["c"]
        assert count_s1 == expected_bulk, f"Bước 1 sai số dòng: {count_s1} != {expected_bulk}"
        print(f"  [OK] S1={s1_id} | committed_at={s1_ts} | rows={count_s1:,}")

        # Chờ 2 giây để timestamp giữa S1 và S2 tách biệt rõ ràng cho truy vấn TIMESTAMP AS OF
        time.sleep(2)

        # ---------------- BƯỚC 2: Nạp 2018-06 -> Snapshot S2 ----------------
        print(f"=== F06 BƯỚC 2: Nạp thêm {BATCH_2018_06} vào {dst_full} ===")
        b06_df.writeTo(dst_full).overwritePartitions()
        snaps_2 = spark.sql(
            f"SELECT snapshot_id, committed_at FROM {dst_full}.snapshots ORDER BY committed_at DESC"
        ).collect()
        s2_id = snaps_2[0]["snapshot_id"]
        s2_ts = snaps_2[0]["committed_at"]
        count_s2 = spark.sql(f"SELECT COUNT(*) AS c FROM {dst_full}").collect()[0]["c"]
        assert count_s2 == count_s1 + expected_b06, (
            f"Bước 2 số dòng tăng không khớp: {count_s2} != {count_s1} + {expected_b06}"
        )
        print(f"  [OK] S2={s2_id} | committed_at={s2_ts} | rows={count_s2:,} (+{expected_b06:,})")

        # ---------------- BƯỚC 3: Time Travel tại S1 theo mã & theo thời điểm ----------------
        print("=== F06 BƯỚC 3: Truy vấn Time Travel tại S1 & liệt kê lịch sử snapshot ===")
        count_by_version = spark.sql(
            f"SELECT COUNT(*) AS c FROM {dst_full} VERSION AS OF {s1_id}"
        ).collect()[0]["c"]
        # Cộng 500ms vào s1_ts (vẫn nhỏ hơn s2_ts do đã sleep 2s) để tránh sai số làm tròn millisecond
        s1_query_ts = (s1_ts + timedelta(milliseconds=500)).strftime("%Y-%m-%d %H:%M:%S.%f")
        count_by_ts = spark.sql(
            f"SELECT COUNT(*) AS c FROM {dst_full} TIMESTAMP AS OF '{s1_query_ts}'"
        ).collect()[0]["c"]
        assert count_by_version == count_s1, f"VERSION AS OF sai: {count_by_version} != {count_s1}"
        assert count_by_ts == count_s1, f"TIMESTAMP AS OF sai: {count_by_ts} != {count_s1}"
        print(f"  [OK] VERSION AS OF {s1_id}: {count_by_version:,} dòng")
        print(f"  [OK] TIMESTAMP AS OF '{s1_query_ts}': {count_by_ts:,} dòng")
        spark.sql(f"SELECT snapshot_id, parent_id, operation, committed_at FROM {dst_full}.snapshots").show(
            truncate=False
        )

        # ---------------- BƯỚC 4: Rollback về S1 rồi nạp lại 2018-06 ----------------
        print("=== F06 BƯỚC 4: Khôi phục bảng về S1 rồi nạp lại 2018-06 ===")
        spark.sql(f"CALL hive.system.rollback_to_snapshot('bronze.{test_table}', {s1_id})")
        count_after_rollback = spark.sql(f"SELECT COUNT(*) AS c FROM {dst_full}").collect()[0]["c"]
        assert count_after_rollback == count_s1, (
            f"Sau rollback số dòng sai: {count_after_rollback} != {count_s1}"
        )
        print(f"  [OK] Sau rollback về S1: {count_after_rollback:,} dòng")

        b06_df.writeTo(dst_full).overwritePartitions()
        count_after_replay = spark.sql(f"SELECT COUNT(*) AS c FROM {dst_full}").collect()[0]["c"]
        assert count_after_replay == count_s2, (
            f"Sau nạp lại 2018-06 số dòng sai: {count_after_replay} != {count_s2}"
        )
        print(f"  [OK] Sau nạp lại {BATCH_2018_06}: {count_after_replay:,} dòng (khớp bước 2)")

        # ---------------- BƯỚC 5: Xác nhận Dremio đọc đúng sau làm mới metadata ----------------
        if skip_dremio:
            print("=== F06 BƯỚC 5: Bỏ qua kiểm tra Dremio (--skip_dremio) ===")
        else:
            print("=== F06 BƯỚC 5: Làm mới metadata và kiểm tra số dòng trên Dremio ===")
            dremio_count = _dremio_refresh_and_count(test_table)
            assert dremio_count == count_s2, (
                f"Dremio đọc sai số dòng sau làm mới metadata: {dremio_count} != {count_s2}"
            )
            print(f"  [OK] Dremio đọc đúng trạng thái hiện hành: {dremio_count:,} dòng")

        print("=== [THÀNH CÔNG] KỊCH BẢN F06 ĐẠT TOÀN BỘ 5 BƯỚC ===")
    finally:
        if cleanup:
            spark.sql(f"DROP TABLE IF EXISTS {dst_full}")
            print(f"[CLEANUP] Đã xóa bảng thử {dst_full}")
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy kịch bản nghiệm thu F06 (Iceberg Time Travel).")
    parser.add_argument("--source_table", default="clickstream_events", help="Bảng Bronze nguồn có bulk và 2018-06.")
    parser.add_argument("--test_table", default="clickstream_events_f06_test", help="Tên bảng thử trong schema bronze.")
    parser.add_argument("--skip_dremio", action="store_true", help="Bỏ qua bước 5 kiểm tra Dremio (khi test cục bộ).")
    parser.add_argument("--cleanup", action="store_true", help="Xóa bảng thử sau khi hoàn tất.")
    args = parser.parse_args()
    run_f06(
        source_table=args.source_table,
        test_table=args.test_table,
        skip_dremio=args.skip_dremio,
        cleanup=args.cleanup,
    )


if __name__ == "__main__":
    main()

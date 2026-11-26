"""
spark/f07_schema_change.py - Kịch bản nghiệm thu F07: Iceberg Schema Evolution (B-207)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu:
- Requirement v3: F07 (Schema Evolution - 4 bước nghiệm thu)
- Design Bronze, Silver v2: Mục V (F07: thử trên bản sao bảng Bronze, không đổi batch_id)
- Design Build: B-207

4 bước nghiệm thu F07:
1. Tạo bản sao của một bảng Bronze (không đổi dữ liệu thật); ghi snapshot S0,
   danh sách tệp dữ liệu Parquet, số dòng và giá trị tổng hợp.
2. Đổi tên một cột (không phải batch_id) và thêm một cột mới.
3. Kiểm tra:
   - Không ghi lại tệp dữ liệu Parquet (danh sách file_path trong <table>.files không đổi).
   - Truy vấn hiện tại thấy tên cột mới và cột mới; số dòng và giá trị tổng hợp nguyên vẹn;
     cột mới rỗng (NULL) ở toàn bộ các dòng cũ.
   - Truy vấn tại snapshot cũ S0 vẫn chạy được với tên cột cũ.
4. Hỗ trợ chạy lại kiểm thử dbt mức error sau khi cập nhật tham chiếu (--dbt_select).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from spark.bronze_common import get_spark_session


def run_f07(
    source_table: str = "clickstream_events",
    test_table: str = "clickstream_events_f07_test",
    old_col: str = "outcome",
    new_col: str = "event_outcome",
    added_col: str = "schema_eval_note",
    agg_col: str = "amount",
    dbt_select: str = "",
    cleanup: bool = False,
) -> None:
    if old_col == "batch_id" or new_col == "batch_id":
        raise ValueError("Theo Design Bronze, Silver v2 (F07): tuyệt đối không đổi tên cột partition batch_id.")

    spark = get_spark_session(app_name="F07_SchemaEvolution_Verification")
    src_full = f"hive.bronze.{source_table}"
    dst_full = f"hive.bronze.{test_table}"

    try:
        if not spark.catalog.tableExists(src_full):
            raise RuntimeError(f"Bảng nguồn {src_full} chưa tồn tại. Cần nạp Bronze trước khi chạy F07.")

        warehouse_base = os.getenv("WAREHOUSE_PATH", "s3a://lakehouse/").rstrip("/")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS hive.bronze LOCATION '{warehouse_base}/bronze'")
        spark.sql(f"DROP TABLE IF EXISTS {dst_full}")

        # ---------------- BƯỚC 1: Tạo bản sao, ghi snapshot S0, file, số dòng, tổng hợp ----------------
        print(f"=== F07 BƯỚC 1: Tạo bản sao {dst_full} từ {src_full} ===")
        src_df = spark.table(src_full)
        (
            src_df.writeTo(dst_full)
            .partitionedBy("batch_id")
            .tableProperty("write.format.default", "parquet")
            .create()
        )

        s0_id = spark.sql(
            f"SELECT snapshot_id FROM {dst_full}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()[0]["snapshot_id"]
        files_before = sorted(
            row["file_path"] for row in spark.sql(f"SELECT file_path FROM {dst_full}.files").collect()
        )
        metrics_before = spark.sql(
            f"SELECT COUNT(*) AS cnt, COALESCE(SUM(CAST({agg_col} AS DOUBLE)), 0.0) AS agg_val FROM {dst_full}"
        ).collect()[0]
        count_before = int(metrics_before["cnt"])
        agg_before = float(metrics_before["agg_val"])
        print(
            f"  [OK] S0={s0_id} | files={len(files_before)} | rows={count_before:,} | "
            f"SUM({agg_col})={agg_before:,.2f}"
        )

        # ---------------- BƯỚC 2: Đổi tên một cột và thêm một cột mới ----------------
        print(f"=== F07 BƯỚC 2: RENAME COLUMN {old_col} -> {new_col} & ADD COLUMN {added_col} ===")
        spark.sql(f"ALTER TABLE {dst_full} RENAME COLUMN {old_col} TO {new_col}")
        spark.sql(f"ALTER TABLE {dst_full} ADD COLUMN {added_col} STRING")

        # ---------------- BƯỚC 3: Kiểm tra không ghi lại tệp & đọc được cả mới lẫn S0 ----------------
        print("=== F07 BƯỚC 3: Kiểm tra metadata-only evolution & tương thích snapshot cũ ===")
        files_after = sorted(
            row["file_path"] for row in spark.sql(f"SELECT file_path FROM {dst_full}.files").collect()
        )
        assert files_after == files_before, (
            f"Tệp dữ liệu Parquet đã bị thay đổi sau Schema Evolution! "
            f"before={len(files_before)}, after={len(files_after)}"
        )
        print(f"  [OK] Không ghi lại tệp dữ liệu ({len(files_after)} tệp Parquet giữ nguyên 100%).")

        cols_after = spark.table(dst_full).columns
        assert new_col in cols_after and old_col not in cols_after and added_col in cols_after, (
            f"Schema mới không khớp kỳ vọng: {cols_after}"
        )

        metrics_after = spark.sql(
            f"""
            SELECT
                COUNT(*) AS cnt,
                COALESCE(SUM(CAST({agg_col} AS DOUBLE)), 0.0) AS agg_val,
                COUNT({added_col}) AS non_null_added,
                COUNT({new_col}) AS non_null_renamed
            FROM {dst_full}
            """
        ).collect()[0]
        count_after = int(metrics_after["cnt"])
        agg_after = float(metrics_after["agg_val"])
        non_null_added = int(metrics_after["non_null_added"])

        assert count_after == count_before, f"Số dòng bị thay đổi: {count_after} != {count_before}"
        assert abs(agg_after - agg_before) <= 0.01, f"Giá trị tổng hợp bị lệch: {agg_after} != {agg_before}"
        assert non_null_added == 0, f"Cột mới thêm {added_col} phải NULL ở mọi dòng cũ, nhưng có {non_null_added} dòng khác NULL."
        print(
            f"  [OK] Dữ liệu hiện tại nguyên vẹn: rows={count_after:,}, "
            f"SUM({agg_col})={agg_after:,.2f}, {added_col} non-null={non_null_added}."
        )

        # Kiểm tra truy vấn tại snapshot cũ S0 vẫn chạy được với tên cột cũ
        old_snap_row = spark.sql(
            f"SELECT COUNT(*) AS cnt, COUNT({old_col}) AS non_null_old FROM {dst_full} VERSION AS OF {s0_id}"
        ).collect()[0]
        assert int(old_snap_row["cnt"]) == count_before
        assert int(old_snap_row["non_null_old"]) == int(metrics_after["non_null_renamed"])
        print(
            f"  [OK] Truy vấn snapshot cũ S0={s0_id} thành công với cột cũ '{old_col}' "
            f"(rows={int(old_snap_row['cnt']):,}, non-null={int(old_snap_row['non_null_old']):,})."
        )

        # ---------------- BƯỚC 4: Kiểm thử dbt mức error (khi truyền --dbt_select) ----------------
        if dbt_select:
            import shutil

            dbt_bin = os.getenv("DBT_BIN", "dbt")
            if shutil.which(dbt_bin) is None:
                raise RuntimeError(
                    f"Không tìm thấy lệnh '{dbt_bin}' trong container hiện tại (container spark không cài dbt). "
                    f"Hãy chạy bước 4 từ container airflow: "
                    f"docker exec airflow dbt test --select {dbt_select} --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
                )
            dbt_dir = os.getenv("DBT_PROJECT_DIR", os.path.join(PROJECT_ROOT, "dbt"))
            print(f"=== F07 BƯỚC 4: Chạy lại kiểm thử dbt mức error (--select {dbt_select}) ===")
            cmd = [
                dbt_bin,
                "test",
                "--select",
                dbt_select,
                "--project-dir",
                dbt_dir,
                "--profiles-dir",
                dbt_dir,
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if res.returncode != 0:
                print(res.stdout, file=sys.stderr)
                print(res.stderr, file=sys.stderr)
                raise RuntimeError(f"Bước 4 F07 thất bại: dbt test trả về mã thoát {res.returncode}")
            print("  [OK] Kiểm thử dbt mức error sau cập nhật tham chiếu đều ĐẠT.")
        else:
            print(
                "=== F07 BƯỚC 4: Chưa truyền --dbt_select. Khi nghiệm thu bước 4, "
                "cập nhật tham chiếu cột trong mô hình dbt và chạy với --dbt_select <model> ==="
            )

        print("=== [THÀNH CÔNG] KỊCH BẢN F07 HOÀN TẤT ===")
    finally:
        if cleanup:
            spark.sql(f"DROP TABLE IF EXISTS {dst_full}")
            print(f"[CLEANUP] Đã xóa bảng thử {dst_full}")
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy kịch bản nghiệm thu F07 (Iceberg Schema Evolution).")
    parser.add_argument("--source_table", default="clickstream_events", help="Bảng Bronze nguồn để nhân bản.")
    parser.add_argument("--test_table", default="clickstream_events_f07_test", help="Tên bảng thử trong schema bronze.")
    parser.add_argument("--old_col", default="outcome", help="Tên cột cần đổi (không được là batch_id).")
    parser.add_argument("--new_col", default="event_outcome", help="Tên cột sau khi đổi.")
    parser.add_argument("--added_col", default="schema_eval_note", help="Tên cột mới thêm.")
    parser.add_argument("--agg_col", default="amount", help="Tên cột số dùng kiểm tra giá trị tổng hợp.")
    parser.add_argument("--dbt_select", default="", help="Selector dbt để chạy kiểm thử bước 4 F07.")
    parser.add_argument("--cleanup", action="store_true", help="Xóa bảng thử sau khi hoàn tất.")
    args = parser.parse_args()
    run_f07(
        source_table=args.source_table,
        test_table=args.test_table,
        old_col=args.old_col,
        new_col=args.new_col,
        added_col=args.added_col,
        agg_col=args.agg_col,
        dbt_select=args.dbt_select,
        cleanup=args.cleanup,
    )


if __name__ == "__main__":
    main()

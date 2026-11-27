<!--
README.md - Hướng dẫn triển khai, vận hành và nghiệm thu từ máy trống đến dashboard (B-707)
Tác giả phụ trách: Minh | Reviewer: Hoàng
Tham chiếu: Design Deployment (Bảng README, Kịch bản nghiệm thu tổng, Ma trận nghiệm thu), Design Build v1.2 (B-707)
-->

# E-commerce Data Lakehouse

Hệ thống Data Lakehouse thương mại điện tử đa nguồn (Olist E-commerce, Marketing Funnel, Clickstream) xây dựng theo kiến trúc Medallion (**Raw → Bronze → Silver → Gold**) với **MinIO**, **Apache Iceberg**, **Hive Metastore**, **Apache Spark**, **dbt-core / dbt-dremio**, **Dremio OSS**, **Apache Airflow** và **Streamlit**.

---

## 1. Chuẩn bị

### 1.1. Yêu cầu máy tham chiếu & bộ nhớ
- Đã cài **Docker** và **Docker Compose** (v2+).
- Trên Windows (WSL2), cấp tối thiểu **12 GB RAM** cho WSL2 trong tệp `%UserProfile%\.wslconfig` (khuyến nghị `memory=12GB`, `swap=2GB`) để chứa toàn bộ stack không bị OOM-kill và giữ swap gần như không dùng khi đo hiệu năng.
- Tổng trần bộ nhớ cấu hình trong `infra/docker-compose.yml`:
  | Dịch vụ | Container | Cổng | Trần bộ nhớ |
  |---|---|---|---|
  | MinIO | `minio` | `9000` (S3), `9001` (Console) | `1024M` |
  | Hive Metastore DB | `hive-metastore-db` | nội bộ `5432` | `512M` |
  | Hive Metastore | `hive-metastore` | `9083` | `1024M` |
  | Apache Spark | `spark` | `8888`, `4040` | `2048M` |
  | Dremio OSS | `dremio` | `9047` (UI), `31010`, `32010` (Flight SQL) | `4096M` (Heap `1536M`, Direct `2048M`) |
  | Airflow DB | `airflow-postgres` | nội bộ `5432` | `512M` |
  | Apache Airflow | `airflow` | `8080` | `2048M` |
  | Streamlit | `streamlit` | `8501` | `1024M` |

### 1.2. Cấu hình biến môi trường (`.env`)
Sao chép tệp mẫu `.env.example` thành `.env` và điền đầy đủ giá trị bí mật (tuyệt đối không commit `.env` lên git):

```bash
cp .env.example .env
```

- Sinh khóa Fernet cho `AIRFLOW_FERNET_KEY`:
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```
- Điền `GEMINI_API_KEY` và `GEMINI_MODEL` trong `.env` để dùng tính năng Executive Summary trên Streamlit.

### 1.3. Tải các gói JAR tương thích (`B-028`)
Thư mục `infra/jars/` được loại khỏi git (`.gitignore`). Trước khi khởi động stack trên máy trống (`F16`), chạy kịch bản tải đủ 5 gói JAR tương thích cho Hive Metastore 3.1.3 và Spark 3.5.0:

```bash
bash infra/fetch_jars.sh
```

---

## 2. Khởi động

Khởi động toàn bộ hệ thống bằng **một lệnh** và chờ mọi dịch vụ đạt trạng thái `healthy` (riêng `minio-init` chạy một lần và thoát mã `0`):

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d --build
docker compose --env-file .env -f infra/docker-compose.yml ps
```

---

## 3. Dữ liệu Raw (`F01`, `F02`)

Đặt dữ liệu gốc Olist / Marketing Funnel / Clickstream vào thư mục nguồn cục bộ, sau đó chạy các chương trình trong `raw_prep/` để chia 4 batch (`bulk_2016-09_2018-05`, `2018-06`, `2018-07`, `2018-08`), chuyển Marketing sang JSONL (kèm `_excluded/`), tạo tệp cờ `_SUCCESS`, đối soát với `raw_prep/batch_row_counts.csv` (`--verify`) và đồng bộ lên MinIO (`s3a://lakehouse/raw/`):

```bash
# 1. Chia batch và kiểm tra đối soát (--verify)
python raw_prep/olist_csv_to_raw.py --verify
python raw_prep/marketing_to_raw.py --verify
python raw_prep/clickstream_to_raw.py --verify

# 2. Đẩy tầng Raw vào MinIO (nếu khởi động lại từ máy trống/volume mới)
docker compose --env-file .env -f infra/docker-compose.yml up minio-init
```

---

## 4. Cấu hình Dremio

Thực hiện thủ công sau khi container `dremio` đạt trạng thái `healthy`:

1. Mở Dremio Web UI tại `http://localhost:9047` và tạo tài khoản quản trị trùng với `DREMIO_ADMIN_USER` / `DREMIO_ADMIN_PASSWORD` trong `.env`.
2. Tạo nguồn dữ liệu Hive (**Add Source → Hive 3.x**):
   - **Tên nguồn chính xác:** `hive` *(bắt buộc khớp `DREMIO_CATALOG=hive` trong `.env`, `dbt/profiles.yml` và `streamlit/dremio_client.py`)*.
   - **Hive Metastore Host:** `hive-metastore`, **Port:** `9083`.
   - **Connection Properties (Advanced Options — tương thích MinIO trên Dremio 25.0, T-03):**
     - `fs.s3a.endpoint` = `minio:9000` *(không có tiền tố `http://`)*
     - `fs.s3a.access.key` = `<giá trị S3_ACCESS_KEY trong .env>`
     - `fs.s3a.secret.key` = `<giá trị S3_SECRET_KEY trong .env>`
     - `fs.s3a.path.style.access` = `true`
     - `fs.s3a.connection.ssl.enabled` = `false`
     - `fs.s3a.aws.credentials.provider` = `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`
     - `dremio.s3.compat` = `true`
3. **Danh sách Raw Reflection cần tạo** *(chỉ tạo sau khi tầng Gold đã có dữ liệu — Design Serving: Reflection)*:
   Chỉ tạo **Raw Reflection** (bản sao tính sẵn của bảng, giữ toàn bộ cột) cho đúng **7 bảng Gold** xuất hiện ở truy vấn chuẩn Q1–Q5, không tạo cho bảng khác:
   | Bảng Gold (`hive.gold.*`) | Tên Reflection quy ước | Phục vụ truy vấn |
   |---|---|---|
   | `hive.gold.kpi_sales_state_month` | `raw_refl_kpi_sales_state_month` | Q1 |
   | `hive.gold.fact_order_item` | `raw_refl_fact_order_item` | Q2 |
   | `hive.gold.fact_order` | `raw_refl_fact_order` | Q2 |
   | `hive.gold.dim_seller` | `raw_refl_dim_seller` | Q2 |
   | `hive.gold.kpi_delivery_state_month` | `raw_refl_kpi_delivery_state_month` | Q3 |
   | `hive.gold.kpi_funnel_day` | `raw_refl_kpi_funnel_day` | Q4 |
   | `hive.gold.kpi_lead_origin` | `raw_refl_kpi_lead_origin` | Q5 |

---

## 5. Chạy và xem

1. Mở Airflow Web UI tại `http://localhost:8080`, đăng nhập bằng `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.
2. Bật và kích hoạt thủ công (**Trigger DAG w/ config**) DAG `dag_lakehouse_batch` với tham số:
   ```json
   {"batch": "tất cả"}
   ```
   DAG sẽ chạy tuần tự 4 batch theo `batch_seq` (`bulk_2016-09_2018-05` → `2018-06` → `2018-07` → `2018-08`), tự động thực hiện Raw → Bronze, Gatekeeper, dbt Silver, dbt Gold, Làm mới Dremio và Ghi nhận vận hành (`ops_log`).
3. Sau khi Gold đã có dữ liệu, tạo 7 Raw Reflection theo mục **4.3** (và chờ Reflection hoàn tất).
4. Mở ứng dụng Streamlit tại `http://localhost:8501`, kiểm tra đủ 3 trang (**Kinh doanh**, **Marketing**, **Hành vi**) và bấm tạo **Executive Summary**.

---

## 6. Kiểm tra và dọn dẹp

### 6.1. Kịch bản nghiệm thu tổng (theo thứ tự từ trên xuống)
1. **Máy trống và một lệnh (`F16`):** Xóa sạch container/volume (mục 6.3), chạy lại từ mục 1.3 → 2 → 3.
2. **Cấu hình Dremio:** Làm theo mục 4 (chưa tạo Reflection vội nếu cần đo chế độ không có Reflection trước).
3. **Chạy toàn bộ & lấy mẫu bộ nhớ:**
   - Mở terminal riêng chạy lấy mẫu bộ nhớ trong suốt quá trình chạy 4 batch:
     ```bash
     bash infra/sample_memory.sh memory_usage_report.csv
     ```
   - Chạy `dag_lakehouse_batch` với `{"batch": "tất cả"}`. Xác nhận không container nào bị OOM-kill (`ExitCode != 137`, `RestartCount == 0`):
     ```bash
     docker inspect --format '{{.Name}}: Exit={{.State.ExitCode}}, Restarts={{.RestartCount}}, OOM={{.State.OOMKilled}}' $(docker compose --env-file .env -f infra/docker-compose.yml ps -q)
     ```
4. **Kiểm thử thường:** Chạy các kịch bản không gây lỗi (`F03`, `F04`/`GT-16`, `F06`, `F07`, `F08`–`F10`, `F13`).
5. **So giá trị tham chiếu:**
   ```bash
   python reference/reference_main.py
   python reference/compare_reference.py
   python reference/compare_dashboard.py
   ```
6. **Đo hiệu năng Q1–Q5 (`B-703`):**
   - Chế độ **Không có Reflection** (trước khi tạo/bật 7 Reflection):
     ```bash
     python reference/measure_performance.py --mode without_reflection --output reference/perf_without.csv
     ```
   - Tạo và làm mới 7 Raw Reflection theo mục 4.3, sau đó đo chế độ **Có Reflection**:
     ```bash
     python reference/measure_performance.py --mode with_reflection --refresh-dremio --output reference/perf_with.csv
     ```
   - Gộp 2 chế độ thành Bảng kết quả đo tổng hợp và kiểm tra ngưỡng (`<= 10s`, `<= 3s`, tăng tốc `>= 2x` ở ít nhất `3/5` truy vấn):
     ```bash
     python reference/measure_performance.py --mode merge --without-csv reference/perf_without.csv --with-csv reference/perf_with.csv --output reference/performance_report.csv
     ```
7. **Quét bí mật lịch sử git:** Làm theo mục 6.2.
8. **Kiểm thử gây lỗi (để cuối cùng):** Chạy `F05` (Ngắt nạp Bronze & Dừng dbt giữa chừng) và `F11` (Lỗi cố ý), sau mỗi kịch bản chạy lại cùng batch để khôi phục Silver và Gold về trạng thái sau batch cuối.

### 6.2. Quy trình nghiệm thu thủ công chi tiết
- **`F03` — Truy vết Bronze:**
  Kiểm tra trên Spark SQL rằng `batch_id` và `source_file` không rỗng ở cả 11 bảng `hive.bronze.*`, đồng thời `hive.bronze.marketing_leads` và `hive.bronze.clickstream_events` còn giữ nguyên văn dòng gốc (`raw_json` / `raw_line`):
  ```bash
  docker exec spark python3 -c "
  from pyspark.sql import SparkSession
  s = SparkSession.builder.getOrCreate()
  for t in ['orders','order_items','order_payments','order_reviews','customers','products','sellers','product_category_translation','geolocation','marketing_leads','clickstream_events']:
      n = s.sql(f'SELECT COUNT(*) c FROM hive.bronze.{t} WHERE batch_id IS NULL OR source_file IS NULL').collect()[0]['c']
      print(t, 'null_trace=', n)
  s.stop()
  "
  ```
- **`F04` & `GT-16` — Chạy lại cùng batch (`B-705`):**
  ```bash
  python reference/check_rerun.py snapshot --batch 2018-06 --output reference/rerun_before.json
  # Kích hoạt lại dag_lakehouse_batch với {"batch": "2018-06"} trên Airflow UI và chờ thành công
  python reference/check_rerun.py snapshot --batch 2018-06 --output reference/rerun_after.json
  python reference/check_rerun.py compare --batch 2018-06 --before reference/rerun_before.json --after reference/rerun_after.json
  ```
- **`F05` — Ngắt nạp Bronze giữa chừng & Dừng dbt giữa chừng (gây lỗi):**
  1. *Ngắt nạp Bronze:* Kích hoạt nạp một batch (ví dụ `2018-08`), khi tác vụ `raw_to_bronze` đang chạy, dừng tiến trình Spark bên trong container:
     ```bash
     docker exec spark pkill -f bronze_
     ```
     Xác nhận bảng Bronze không có dòng ghi dở và vẫn đọc được từ cả Spark lẫn Dremio; sau đó chạy lại cùng batch và xác nhận đạt Gatekeeper.
  2. *Dừng dbt giữa chừng:* Hủy tác vụ `dbt_silver` hoặc `dbt_gold` khi đang chạy trên Airflow UI; kiểm tra không làm mới Dremio; sau đó chạy lại cùng batch để khôi phục số dòng bằng lần chạy sạch.
- **`F06` & `F07` — Time Travel và Schema Evolution (chạy ngoài DAG):**
  ```bash
  docker exec spark spark-submit /home/spark/f06_snapshots.py
  docker exec spark spark-submit /home/spark/f07_schema_change.py
  ```
- **`F11` — Lỗi cố ý (gây lỗi):**
  Kích hoạt `dag_lakehouse_batch` với cấu hình gây lỗi một model cụ thể:
  ```json
  {"batch": "2018-08", "simulate_dbt_failure_model": "clickstream_events"}
  ```
  Xác nhận:
  - Tác vụ thử lại đúng 2 lần (cách nhau 1 phút) rồi chuyển trạng thái `failed`.
  - Bước `Làm mới Dremio` không chạy.
  - Bước `Ghi nhận vận hành` (`ops_log`) vẫn chạy và ghi trạng thái lỗi kèm `failed_model`.
  - Kích hoạt batch kế tiếp (nếu test trên `2018-07` rồi gọi `2018-08`) bị bước `Kiểm tra thứ tự` chặn ngay lập tức.
  - Sau khi hoàn tất kiểm chứng, chạy lại đúng batch đó không có `simulate_dbt_failure_model` để khôi phục.
- **`F12` — Đọc nhật ký vận hành:**
  Kiểm tra bảng `ops_batch_run` do `airflow/dags/ops_log.py` ghi trong Airflow Metadata DB (`airflow-postgres`), xác nhận đủ các cột (`run_id`, `batch_id`, `status`, `failed_model`, `rows_in`, `rows_out`, `started_at`, `ended_at`):
  ```bash
  docker exec airflow-postgres psql -U airflow -d airflow -c "SELECT run_id, batch_id, status, failed_model, rows_in, rows_out, started_at, ended_at FROM ops_batch_run ORDER BY started_at;"
  ```
- **`F13` — Dremio thấy dữ liệu mới & So khớp Q1–Q5 với Spark (`B-702`):**
  1. Sau Raw → Bronze của một batch, đối chiếu số dòng đọc qua Spark và qua Dremio trên bảng Bronze vừa nạp.
  2. Chạy so khớp 5 truy vấn chuẩn Q1–Q5 giữa Spark và Dremio:
     ```bash
     python reference/compare_engines.py --output reference/compare_engines_report.csv
     ```
- **Ghim phiên bản:**
  Kiểm tra mọi image trong `infra/` đều có tag phiên bản cụ thể khớp bảng Tech stack (không có `:latest`; riêng `minio` và `mc` được ghim cố định bằng SHA256 digest):
  ```bash
  grep -rn "image:" infra/
  ```
- **Quét bí mật lịch sử git:**
  Chạy pre-commit hook và quét toàn bộ lịch sử git bằng `gitleaks` (yêu cầu **0 phát hiện**):
  ```bash
  pre-commit run --all-files
  gitleaks detect --source . --verbose
  ```

### 6.3. Dọn dẹp (trở về máy trống)
Dừng và xóa toàn bộ container cùng volume dữ liệu (giữ lại mã nguồn, `.env` và dữ liệu gốc):

```bash
docker compose --env-file .env -f infra/docker-compose.yml down -v
```

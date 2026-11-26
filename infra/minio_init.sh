#!/bin/sh
set -e

# ===========================================================================
# B-008: minio_init.sh
# Tạo bucket lakehouse và các thư mục 4 tầng (raw, bronze, silver, gold)
# và các thư mục Raw: raw/csv, raw/marketing_funnel, raw/clickstream
# ===========================================================================

echo "Configuring MinIO client alias..."
mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

echo "Creating lakehouse bucket and directory structure..."
mc mb -p local/lakehouse
mc mb -p local/lakehouse/raw/csv
mc mb -p local/lakehouse/raw/marketing_funnel
mc mb -p local/lakehouse/raw/clickstream
mc mb -p local/lakehouse/bronze
mc mb -p local/lakehouse/silver
mc mb -p local/lakehouse/gold

# Đồng bộ dữ liệu Raw cục bộ (nếu đã chuẩn bị sẵn ở ../raw) lên MinIO (F03, F16)
# Chỉ đồng bộ 3 nguồn theo thiết kế chính thức; không dùng --remove để giữ tính bất biến của Raw
for src in csv marketing_funnel clickstream; do
  if [ -d "/raw_data/${src}" ] && [ -n "$(ls -A "/raw_data/${src}" 2>/dev/null)" ]; then
    echo "Syncing raw/${src} to local/lakehouse/raw/${src}..."
    mc mirror --overwrite --exclude "*.gitkeep" "/raw_data/${src}" "local/lakehouse/raw/${src}"
  fi
done

echo "MinIO buckets and paths ready!"

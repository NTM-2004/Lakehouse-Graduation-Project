#!/bin/sh
# Upload the WHOLE raw tree (olist / clickstream / marketing_funnel) to MinIO.
# _SUCCESS markers are copied in a second pass, so a half-uploaded batch is never marked ready.
# Usage (repo root):  ./upload_raw_to_minio.sh ./raw
# SRC="${1:-./raw}"
# docker run --rm --network host -v "$(cd "$SRC" && pwd)":/src quay.io/minio/mc:latest sh -c '
#   mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null &&
#   mc mirror --overwrite --exclude "_SUCCESS" --exclude ".gitkeep" /src local/lakehouse/raw &&
#   find /src -name _SUCCESS | while read f; do
#     rel=${f#/src/}; mc cp "$f" "local/lakehouse/raw/$rel" >/dev/null
#   done &&
#   echo "--- markers in MinIO ---" && mc find local/lakehouse/raw --name _SUCCESS'
SRC="${1:-./raw}"

# Lấy đường dẫn tuyệt đối dạng Windows (ví dụ D:/College/Final Project/...)
HOST_PATH=$(cd "$SRC" 2>/dev/null && pwd -W 2>/dev/null || pwd)

# MSYS_NO_PATHCONV=1 ngăn Git Bash tự biến đổi đường dẫn /src
MSYS_NO_PATHCONV=1 docker run --rm --network host --entrypoint sh \
  -v "$HOST_PATH":/src \
  quay.io/minio/mc:latest -c '
  mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null &&
  mc mirror --overwrite --exclude "_SUCCESS" --exclude ".gitkeep" /src local/lakehouse/raw &&
  find /src -name _SUCCESS | while read f; do
    rel=${f#/src/}; mc cp "$f" "local/lakehouse/raw/$rel" >/dev/null
  done &&
  echo "--- markers in MinIO ---" && mc find local/lakehouse/raw --name _SUCCESS'
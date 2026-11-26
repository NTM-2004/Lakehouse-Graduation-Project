#!/usr/bin/env bash
# ===========================================================================
# B-706: sample_memory.sh
# Lấy mẫu bộ nhớ từng dịch vụ khi chạy toàn bộ và ghi mã thoát, số lần khởi động lại
# ===========================================================================

OUTPUT_FILE="${1:-memory_usage_report.csv}"
echo "timestamp,container_name,cpu_percent,mem_usage,mem_limit,mem_percent" > "${OUTPUT_FILE}"

echo "Sampling memory every 10 seconds. Output file: ${OUTPUT_FILE}. Press Ctrl+C to stop."

while true; do
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" | while read -r line; do
        if [ -n "$line" ]; then
            echo "${TIMESTAMP},${line}" >> "${OUTPUT_FILE}"
        fi
    done
    sleep 10
done

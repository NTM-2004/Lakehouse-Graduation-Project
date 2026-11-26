#!/usr/bin/env bash
set -euo pipefail

# ===========================================================================
# B-028: fetch_jars.sh - Tải các gói JAR tương thích vào infra/jars/
# HMS: hadoop-aws 3.2.0, aws-java-sdk-bundle 1.11.563
# Spark: hadoop-aws 3.3.4, aws-java-sdk-bundle 1.12.261
# ===========================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="${SCRIPT_DIR}/jars"
mkdir -p "${JAR_DIR}"

MAVEN_CENTRAL="https://repo1.maven.org/maven2"

download_if_missing() {
    local url="$1"
    local dest="$2"
    if [ ! -f "${dest}" ]; then
        echo "Downloading $(basename "${dest}")..."
        curl -fSL "${url}" -o "${dest}"
    else
        echo "Found $(basename "${dest}"), skipping download."
    fi
}

echo "=== Ensuring Hive Metastore 3.1.3 jars ==="
download_if_missing \
    "${MAVEN_CENTRAL}/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar" \
    "${JAR_DIR}/hadoop-aws-3.2.0.jar"

download_if_missing \
    "${MAVEN_CENTRAL}/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar" \
    "${JAR_DIR}/aws-java-sdk-bundle-1.11.563.jar"

# PostgreSQL JDBC driver (thay thế driver cổ postgresql-9.4.1208 trong Hive 3.1.3 để tương thích Postgres 13+)
download_if_missing \
    "${MAVEN_CENTRAL}/org/postgresql/postgresql/42.5.4/postgresql-42.5.4.jar" \
    "${JAR_DIR}/postgresql-42.5.4.jar"

echo "=== Ensuring Spark 3.5.0 jars ==="
download_if_missing \
    "${MAVEN_CENTRAL}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    "${JAR_DIR}/hadoop-aws-3.3.4.jar"

download_if_missing \
    "${MAVEN_CENTRAL}/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar" \
    "${JAR_DIR}/aws-java-sdk-bundle-1.12.261.jar"

echo "All compatible jars verified in ${JAR_DIR}"

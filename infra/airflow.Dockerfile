FROM apache/airflow:2.9.3

# B-006: Dựng image Airflow tích hợp dbt-core, dbt-dremio và Docker CLI
USER root

# Install Docker CLI, curl, git
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        docker.io \
        git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages (dbt-core, dbt-dremio, etc.)
COPY airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

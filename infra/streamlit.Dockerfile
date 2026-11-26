FROM python:3.11-slim

# B-007: Dựng image Streamlit từ image Python nền ghim tag
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY streamlit/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY streamlit/ /app/

EXPOSE 8501

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

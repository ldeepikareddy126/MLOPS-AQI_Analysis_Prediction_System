FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=1000 --retries 10 -r /app/requirements.txt

COPY . /app

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=5 \
  CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]

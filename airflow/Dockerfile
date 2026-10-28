# =============================================================================
# Airflow Dockerfile
# Base: apache/airflow:2.10.5-python3.11 (official, pinned)
# Constraints: https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
# =============================================================================
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt

COPY airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir --default-timeout=1000 --retries 10 \
    -r /opt/airflow/requirements.txt \
    --constraint "${CONSTRAINTS_URL}"

COPY --chown=airflow:root pyproject.toml /opt/airflow_src/pyproject.toml
COPY --chown=airflow:root src/ /opt/airflow_src/src/
RUN pip install --no-cache-dir -e /opt/airflow_src/

COPY --chown=airflow:root mlops/pyproject.toml /opt/airflow_src/mlops/pyproject.toml
COPY --chown=airflow:root mlops/src/ /opt/airflow_src/mlops/src/
RUN pip install --no-cache-dir -e /opt/airflow_src/mlops/

ENV PYTHONPATH="/opt/airflow_src/src:/opt/airflow_src/mlops/src"

WORKDIR /opt/airflow

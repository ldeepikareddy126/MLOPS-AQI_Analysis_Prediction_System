"""Triggerable Kaggle manual-file ingestion DAG."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from airflow.decorators import dag, task


@dag("aqi_ingest_kaggle", start_date=datetime(2026, 1, 1), schedule=None, catchup=False, tags=["aqi", "ingest"])
def aqi_ingest_kaggle():
    @task
    def ingest():
        from aqi_pipeline.collectors.kaggle_loader import collect

        return collect(str(uuid4()))

    ingest()


aqi_ingest_kaggle()

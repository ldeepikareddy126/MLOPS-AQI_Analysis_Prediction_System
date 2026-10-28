"""Triggerable CPCB manual-file ingestion DAG."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from airflow.decorators import dag, task


@dag("aqi_ingest_cpcb", start_date=datetime(2026, 1, 1), schedule=None, catchup=False, tags=["aqi", "ingest"])
def aqi_ingest_cpcb():
    @task
    def ingest():
        from aqi_pipeline.collectors.cpcb_loader import collect

        return collect(str(uuid4()))

    ingest()


aqi_ingest_cpcb()

"""Triggerable Open-Meteo ingestion DAG."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from uuid import uuid4

from airflow.decorators import dag, task


@dag("aqi_ingest_open_meteo", start_date=datetime(2026, 1, 1), schedule=None, catchup=False, tags=["aqi", "ingest"])
def aqi_ingest_open_meteo():
    @task
    def ingest(**context):
        from aqi_pipeline.collectors.open_meteo import collect

        params = context.get("params") or {}
        default_end = date.today()
        default_start = default_end - timedelta(days=1)
        return collect(
            str(uuid4()),
            params.get("start_date") or default_start.isoformat(),
            params.get("end_date") or default_end.isoformat(),
        )

    ingest()


aqi_ingest_open_meteo()

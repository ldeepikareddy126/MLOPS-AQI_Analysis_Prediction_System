"""Master Stage A AQI ETL DAG."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID, uuid4

from airflow.decorators import dag, task


@dag(
    dag_id="aqi_etl_pipeline",
    description="Stage A master pipeline: ingest -> stage -> clean -> analytics -> coverage",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["aqi", "stage-a", "etl"],
    doc_md="""
    # AQI ETL Pipeline

    Runs the Stage A data engineering flow. OpenAQ and Kaggle may be logged as
    skipped until credentials/files are provided.

    To override the manual-run window, pass `{"start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD"}` in DAG run configuration; both dates are inclusive.
    To replay transformation and verification from an existing raw ingestion,
    trigger with `{"pipeline_run_id": "<existing UUID>", "ingest": false}`.
    """,
)
def aqi_etl_pipeline():
    @task
    def make_run_id(**context) -> str:
        conf = context["dag_run"].conf or {}
        requested_run_id = conf.get("pipeline_run_id")
        return str(UUID(str(requested_run_id))) if requested_run_id else str(uuid4())

    @task
    def run_pipeline(pipeline_run_id: str, **context) -> dict:
        from aqi_pipeline.transform.pipeline import run_stage_a

        params = context.get("params") or {}
        conf = context["dag_run"].conf or {}
        ingest = conf.get("ingest", True)
        if not isinstance(ingest, bool):
            raise ValueError("DAG run conf 'ingest' must be a boolean")
        interval_start = context["data_interval_start"].date().isoformat()
        interval_end = (context["data_interval_end"] - timedelta(days=1)).date().isoformat()
        start_date = conf.get("start_date") or params.get("start_date") or interval_start
        end_date = conf.get("end_date") or params.get("end_date") or interval_end
        if not isinstance(start_date, str) or not isinstance(end_date, str):
            raise ValueError("DAG run dates must be ISO strings in YYYY-MM-DD format")
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date().isoformat()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise ValueError("DAG run dates must be ISO strings in YYYY-MM-DD format") from exc
        if start_date > end_date:
            raise ValueError("DAG run start_date must not be after end_date")
        return run_stage_a(
            run_id=pipeline_run_id,
            start_date=start_date,
            end_date=end_date,
            ingest=ingest,
            dag_id=context["dag"].dag_id,
            dag_run_id=context["dag_run"].run_id,
        )

    @task
    def verify_source_coverage(pipeline_run_id: str) -> None:
        import importlib.util
        import sys
        import os

        # scripts/ is mounted at /opt/airflow/scripts/ inside Airflow containers.
        # Load by file path so PYTHONPATH inclusion of scripts/ is not required.
        scripts_path = os.environ.get(
            "VERIFY_SOURCES_PATH",
            "/opt/airflow/scripts/verify_sources.py",
        )
        spec = importlib.util.spec_from_file_location("verify_sources", scripts_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        old_argv = sys.argv
        try:
            sys.argv = ["verify_sources.py", "--run-id", pipeline_run_id, "--strict"]
            code = module.main()
        finally:
            sys.argv = old_argv
        if code:
            raise RuntimeError("Source coverage verification failed")

    rid = make_run_id()
    run_pipeline(rid) >> verify_source_coverage(rid)


aqi_etl_pipeline()

"""Integration tests for Stage A ETL pipeline — require a live database.

These tests run inside Docker (make test) where the AQI warehouse is available.
Skip them when running unit tests locally without DB access.
"""

from __future__ import annotations

import os
import pytest

# Mark entire module as integration — skip if no DB connection available
pytestmark = pytest.mark.skipif(
    not os.environ.get("POSTGRES_HOST"),
    reason="POSTGRES_HOST not set — skip integration tests",
)


@pytest.fixture(scope="module")
def engine():
    """Return a SQLAlchemy engine pointed at the test database."""
    from aqi_pipeline.utils.db import get_engine

    return get_engine()


@pytest.fixture(scope="module")
def run_id():
    from uuid import uuid4

    return str(uuid4())


@pytest.fixture(scope="module", autouse=True)
def cleanup_test_metadata(engine, run_id):
    """Keep integration-test operational rows out of the user's warehouse views."""
    yield
    from sqlalchemy import text

    with engine.begin() as conn:
        for table in ("ops.dq_results", "ops.ingestion_log", "ops.rejected_records", "ops.source_coverage", "ops.pipeline_run"):
            conn.execute(text(f"DELETE FROM {table} WHERE run_id = :run_id"), {"run_id": run_id})


class TestDatabaseConnectivity:
    """Basic connectivity tests — ensure DB is reachable and schemas exist."""

    def test_can_connect(self, engine):
        from sqlalchemy import text

        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
        assert result == 1

    def test_required_schemas_exist(self, engine):
        from sqlalchemy import text

        schemas = ["raw", "staging", "clean", "analytics", "ops", "monitoring"]
        with engine.connect() as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    text("SELECT schema_name FROM information_schema.schemata")
                )
            }
        for schema in schemas:
            assert schema in existing, f"Schema '{schema}' missing"

    def test_dim_tables_seeded(self, engine):
        from sqlalchemy import text

        with engine.connect() as conn:
            city_count = conn.execute(text("SELECT COUNT(*) FROM analytics.dim_city")).scalar()
            poll_count = conn.execute(text("SELECT COUNT(*) FROM analytics.dim_pollutant")).scalar()
            date_count = conn.execute(text("SELECT COUNT(*) FROM analytics.dim_date")).scalar()

        assert city_count > 0, "dim_city should have at least one city"
        assert poll_count > 0, "dim_pollutant should have at least one pollutant"
        assert date_count > 365, "dim_date should span multiple years"


class TestOpsMetadata:
    """Test operational metadata tables."""

    def test_source_registry_loaded(self, engine):
        from sqlalchemy import text

        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM ops.source_registry")).scalar()
        assert count >= 4, "source_registry should have at least 4 sources"

    def test_pipeline_run_can_be_created(self, engine, run_id):
        from aqi_pipeline.quality.ops import start_pipeline_run, finish_pipeline_run
        from sqlalchemy import text

        start_pipeline_run(run_id, "test_dag", "test_run", {"test": True})

        with engine.connect() as conn:
            status = conn.execute(
                text("SELECT status FROM ops.pipeline_run WHERE run_id = :rid"),
                {"rid": run_id},
            ).scalar()
        assert status == "RUNNING"

        finish_pipeline_run(run_id, "SUCCESS", {"rows": 0})
        with engine.connect() as conn:
            status = conn.execute(
                text("SELECT status FROM ops.pipeline_run WHERE run_id = :rid"),
                {"rid": run_id},
            ).scalar()
        assert status == "SUCCESS"

    def test_ingestion_log_can_be_written(self, engine, run_id):
        from aqi_pipeline.quality.ops import log_ingestion
        from sqlalchemy import text

        log_ingestion(
            run_id=run_id,
            source_id="openaq",
            endpoint_or_file="test://endpoint",
            status="SUCCESS",
            rows_extracted=42,
        )

        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM ops.ingestion_log WHERE run_id = :rid"),
                {"rid": run_id},
            ).scalar()
        assert count >= 1

    def test_dq_result_can_be_logged(self, engine, run_id):
        from aqi_pipeline.quality.ops import log_dq_result
        from sqlalchemy import text

        log_dq_result(
            run_id=run_id,
            check_name="integration_test_check",
            layer="staging",
            source_id="openaq",
            passed_count=100,
            failed_count=5,
            threshold=0.20,
            status="PASS",
        )

        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM ops.dq_results WHERE run_id = :rid"),
                {"rid": run_id},
            ).scalar()
        assert count >= 1


class TestOpenMeteoCollector:
    """Test Open-Meteo collection (no API key required — public API)."""

    def test_open_meteo_collect_returns_integer(self, run_id):
        from aqi_pipeline.collectors.open_meteo import collect

        # Use a very narrow date range to avoid excessive API calls in tests
        result = collect(run_id, "2024-01-01", "2024-01-02")
        assert isinstance(result, int)
        assert result >= 0


class TestDataTransforms:
    """Integration tests for transform functions against the database."""

    def test_match_city_returns_valid_id(self):
        from aqi_pipeline.transform.pipeline import _match_city
        from aqi_pipeline.utils.config import load_cities

        valid_ids = {c["id"] for c in load_cities()}
        result = _match_city("Delhi", None, None)
        assert result in valid_ids

    def test_match_city_by_coordinates(self):
        from aqi_pipeline.transform.pipeline import _match_city
        from aqi_pipeline.utils.config import load_cities

        valid_ids = {c["id"] for c in load_cities()}
        # Delhi coordinates
        result = _match_city(None, 28.6139, 77.2090)
        assert result in valid_ids

    def test_stage_raw_returns_integer(self, run_id, engine):
        """stage_raw should not throw even with empty raw tables."""
        from aqi_pipeline.transform.pipeline import stage_raw

        result = stage_raw(run_id)
        assert isinstance(result, int)
        assert result >= 0

    def test_validate_and_clean_returns_integer(self, run_id, engine):
        """validate_and_clean should handle empty staging gracefully."""
        from aqi_pipeline.transform.pipeline import validate_and_clean

        result = validate_and_clean(run_id)
        assert isinstance(result, int)
        assert result >= 0

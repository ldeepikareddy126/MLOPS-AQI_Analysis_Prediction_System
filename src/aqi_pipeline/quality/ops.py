"""Operational metadata writes for ingestion, DQ and pipeline runs."""

from __future__ import annotations

import json
import math
from numbers import Real
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psycopg2.extras

from aqi_pipeline.utils.db import get_raw_connection


def _json_safe(value: Any) -> Any:
    if isinstance(value, Real) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return value


def json_dumps(value: Any) -> str:
    return json.dumps(_json_safe(value), default=str, ensure_ascii=False, allow_nan=False)


def log_ingestion(
    *,
    run_id: UUID | str,
    source_id: str,
    endpoint_or_file: str,
    status: str,
    rows_extracted: int = 0,
    request_params: dict[str, Any] | None = None,
    http_status: int | None = None,
    error_message: str | None = None,
    duration_s: float | None = None,
    file_path: str | None = None,
    file_checksum: str | None = None,
    dag_id: str | None = None,
    task_id: str | None = None,
) -> None:
    sql = """
        INSERT INTO ops.ingestion_log (
            run_id, source_id, dag_id, task_id, endpoint_or_file, request_params,
            status, http_status, rows_extracted, error_message, duration_s,
            file_path, file_checksum
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            (
                str(run_id),
                source_id,
                dag_id,
                task_id,
                endpoint_or_file,
                json_dumps(request_params or {}),
                status,
                http_status,
                rows_extracted,
                error_message,
                duration_s,
                file_path,
                file_checksum,
            ),
        )
        conn.commit()


def start_pipeline_run(run_id: UUID | str, dag_id: str | None, dag_run_id: str | None, config: dict[str, Any]) -> None:
    sql = """
        INSERT INTO ops.pipeline_run (run_id, dag_id, dag_run_id, started_at, status, config_json)
        VALUES (%s, %s, %s, %s, 'RUNNING', %s::jsonb)
        ON CONFLICT (run_id) DO UPDATE SET
            dag_id = EXCLUDED.dag_id,
            dag_run_id = EXCLUDED.dag_run_id,
            started_at = EXCLUDED.started_at,
            finished_at = NULL,
            status = 'RUNNING',
            config_json = EXCLUDED.config_json,
            summary_json = '{}'::jsonb
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (str(run_id), dag_id, dag_run_id, datetime.now(timezone.utc), json_dumps(config)))
        conn.commit()


def finish_pipeline_run(run_id: UUID | str, status: str, summary: dict[str, Any] | None = None) -> None:
    sql = """
        UPDATE ops.pipeline_run
        SET finished_at = %s, status = %s, summary_json = %s::jsonb
        WHERE run_id = %s
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (datetime.now(timezone.utc), status, json_dumps(summary or {}), str(run_id)))
        conn.commit()


def insert_rejections(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO ops.rejected_records (
            run_id, source_id, layer, original_row, rule_violated,
            rejection_reason, pollutant_code, station_id, timestamp_utc
        )
        VALUES %s
    """
    values = [
        (
            str(r["run_id"]),
            r["source_id"],
            r.get("layer", "staging"),
            json_dumps(r["original_row"]),
            r["rule_violated"],
            r["rejection_reason"],
            r.get("pollutant_code"),
            r.get("station_id"),
            r.get("timestamp_utc"),
        )
        for r in rows
    ]
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
        conn.commit()
    return len(rows)


def log_dq_result(
    *,
    run_id: UUID | str,
    check_name: str,
    layer: str,
    source_id: str | None,
    passed_count: int,
    failed_count: int,
    threshold: float | None,
    status: str,
    notes: str | None = None,
) -> None:
    sql = """
        INSERT INTO ops.dq_results (
            run_id, check_name, layer, source_id, passed_count, failed_count,
            total_count, threshold, status, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            (
                str(run_id),
                check_name,
                layer,
                source_id,
                passed_count,
                failed_count,
                passed_count + failed_count,
                threshold,
                status,
                notes,
            ),
        )
        conn.commit()

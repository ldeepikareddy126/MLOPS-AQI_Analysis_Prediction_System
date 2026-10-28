"""Stage A ETL runner.

This module intentionally keeps the first runnable pipeline transparent:
raw tables -> staging -> clean -> analytics. More specialised modules can
replace pieces later without changing the Airflow task contract.
"""

from __future__ import annotations

import os
from functools import lru_cache
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pandas as pd
import psycopg2.extras
from sqlalchemy import text

from aqi_pipeline.collectors import cpcb_loader, kaggle_loader, open_meteo, openaq
from aqi_pipeline.quality.ops import (
    finish_pipeline_run,
    log_dq_result,
    start_pipeline_run,
)
from aqi_pipeline.transform.aqi import category, final_aqi, sub_index
from aqi_pipeline.transform.units import canonical_pollutant, convert_value
from aqi_pipeline.utils.config import load_cities, load_pollutants, load_source_priority
from aqi_pipeline.utils.db import get_engine, get_raw_connection
from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def new_run_id() -> str:
    return str(uuid4())


def run_stage_a(
    run_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    ingest: bool = True,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or new_run_id()
    start_date = start_date or os.environ.get("PIPELINE_BACKFILL_START", "2022-01-01")
    end_date = end_date or os.environ.get("PIPELINE_BACKFILL_END") or date.today().isoformat()
    summary: dict[str, Any] = {"run_id": run_id, "start_date": start_date, "end_date": end_date}
    start_pipeline_run(run_id, dag_id, dag_run_id, {"start_date": start_date, "end_date": end_date, "ingest": ingest})
    try:
        if ingest:
            summary["ingested"] = ingest_sources(run_id, start_date, end_date)
        summary["staged_rows"] = stage_raw(run_id)
        summary["clean_rows"] = validate_and_clean(run_id)
        summary["daily_rows"] = build_daily_averages(run_id)
        summary["analytics"] = build_analytics()
        finish_pipeline_run(run_id, "SUCCESS", summary)
        return summary
    except Exception as exc:
        finish_pipeline_run(run_id, "FAILED", {"error": str(exc), **summary})
        raise


def ingest_sources(run_id: str, start_date: str, end_date: str) -> dict[str, int]:
    return {
        "openaq": openaq.collect(run_id, start_date, end_date),
        "open_meteo": open_meteo.collect(run_id, start_date, end_date),
        "cpcb": cpcb_loader.collect(run_id),
        "kaggle": kaggle_loader.collect(run_id),
    }


def stage_raw(run_id: str) -> int:
    with get_engine().begin() as conn:
        conn.execute(text("DELETE FROM staging.measurements WHERE run_id = :run_id"), {"run_id": run_id})
        conn.execute(text("DELETE FROM staging.weather WHERE run_id = :run_id"), {"run_id": run_id})

    _seed_stations()
    staged_count = _stage_openaq(run_id)
    measurements = []
    measurements.extend(_stage_kaggle(run_id))
    measurements.extend(_stage_cpcb(run_id))
    _insert_staging_measurements(measurements)
    _stage_weather(run_id)
    return staged_count + len(measurements)


def _seed_stations() -> None:
    cities = load_cities()
    rows = [
        (
            f"city_{city['id']}",
            f"{city['name']} City Aggregate",
            city["id"],
            city.get("state"),
            city.get("country", "IN"),
            city.get("lat"),
            city.get("lon"),
            "config",
            None,
        )
        for city in cities
    ]
    sql = """
        INSERT INTO staging.station_dim (
            station_id_canonical, station_name_canonical, city_id, state, country,
            lat, lon, source_system, openaq_location_id
        )
        VALUES %s
        ON CONFLICT (station_id_canonical) DO UPDATE SET
            station_name_canonical = EXCLUDED.station_name_canonical,
            city_id = EXCLUDED.city_id,
            state = EXCLUDED.state,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            updated_at = NOW()
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
        conn.commit()


def _stage_openaq(run_id: str, batch_size: int = 25_000) -> int:
    rows_staged = 0
    chunks = pd.read_sql(
        """
        SELECT id, run_id, location_id, location_name, parameter, value, unit,
               date_utc, coordinates_lat, coordinates_lon, city
        FROM raw.openaq_measurements
        WHERE run_id = %(run_id)s
        """,
        get_engine(),
        params={"run_id": run_id},
        chunksize=batch_size,
    )
    for df in chunks:
        station_rows = []
        staged = []
        for row in df.to_dict("records"):
            pollutant = canonical_pollutant(row["parameter"])
            if not pollutant:
                continue
            value, unit = convert_value(row["value"], pollutant, row["unit"])
            ts = pd.to_datetime(row["date_utc"], utc=True, errors="coerce")
            if pd.isna(ts):
                continue
            city_id = _match_city(row.get("city"), row.get("coordinates_lat"), row.get("coordinates_lon"))
            station_id = f"openaq_{int(row['location_id'])}" if row.get("location_id") else f"city_{city_id}"
            station_rows.append(
                (
                    station_id,
                    row.get("location_name") or station_id,
                    city_id,
                    "IN",
                    row.get("coordinates_lat"),
                    row.get("coordinates_lon"),
                    "openaq",
                    row.get("location_id"),
                )
            )
            staged.append(_measurement_record(run_id, "openaq", station_id, row.get("location_name"), city_id, pollutant, value, unit, row.get("unit"), ts, row["id"]))
        _upsert_station_rows(station_rows)
        _insert_staging_measurements(staged)
        rows_staged += len(staged)
    return rows_staged


def _stage_kaggle(run_id: str) -> list[dict[str, Any]]:
    df = pd.read_sql(
        """
        SELECT id, city_raw, station_raw, date_raw, parameter_raw, value_raw
        FROM raw.kaggle_measurements
        WHERE run_id = %(run_id)s
        """,
        get_engine(),
        params={"run_id": run_id},
    )
    staged = []
    for row in df.to_dict("records"):
        pollutant = canonical_pollutant(row.get("parameter_raw"))
        if not pollutant:
            continue
        city_id = _match_city(row.get("city_raw"), None, None)
        station_name = row.get("station_raw") or f"{city_id} City Aggregate"
        station_id = _station_id("kaggle", city_id, station_name)
        value, unit = convert_value(row.get("value_raw"), pollutant, None)
        ts = pd.to_datetime(row.get("date_raw"), utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        staged.append(_measurement_record(run_id, "kaggle", station_id, station_name, city_id, pollutant, value, unit, None, ts, row["id"]))
    _upsert_generic_stations(staged, "kaggle")
    return staged


def _stage_cpcb(run_id: str) -> list[dict[str, Any]]:
    df = pd.read_sql(
        """
        SELECT id, station_name_raw, city_raw, date_raw, time_raw, parameter_raw, value_raw, unit_raw
        FROM raw.cpcb_measurements
        WHERE run_id = %(run_id)s
        """,
        get_engine(),
        params={"run_id": run_id},
    )
    staged = []
    for row in df.to_dict("records"):
        pollutant = canonical_pollutant(row.get("parameter_raw"))
        if not pollutant:
            continue
        city_id = _match_city(row.get("city_raw"), None, None)
        station_name = row.get("station_name_raw") or f"{city_id} City Aggregate"
        station_id = _station_id("cpcb", city_id, station_name)
        value, unit = convert_value(row.get("value_raw"), pollutant, row.get("unit_raw"))
        raw_ts = " ".join(str(v) for v in [row.get("date_raw"), row.get("time_raw")] if v not in (None, "nan", "NaT"))
        ts = pd.to_datetime(raw_ts, utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        staged.append(_measurement_record(run_id, "cpcb", station_id, station_name, city_id, pollutant, value, unit, row.get("unit_raw"), ts, row["id"]))
    _upsert_generic_stations(staged, "cpcb")
    return staged


def _stage_weather(run_id: str) -> int:
    sql = """
        INSERT INTO staging.weather (
            run_id, city_id, lat, lon, timestamp_utc, temperature_2m,
            relative_humidity_2m, precipitation, wind_speed_10m,
            wind_direction_10m, surface_pressure, cloud_cover, visibility
        )
        SELECT run_id, city_id, lat, lon, timestamp_utc, temperature_2m,
               relative_humidity_2m, precipitation, wind_speed_10m,
               wind_direction_10m, surface_pressure, cloud_cover, visibility
        FROM raw.open_meteo_weather
        WHERE run_id = :run_id
        ON CONFLICT (city_id, timestamp_utc) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            precipitation = EXCLUDED.precipitation,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            wind_direction_10m = EXCLUDED.wind_direction_10m,
            surface_pressure = EXCLUDED.surface_pressure,
            cloud_cover = EXCLUDED.cloud_cover,
            visibility = EXCLUDED.visibility
    """
    with get_engine().begin() as conn:
        result = conn.execute(text(sql), {"run_id": run_id})
        return int(result.rowcount or 0)


def _insert_staging_measurements(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    values = [
        (
            r["run_id"],
            r["source_system"],
            r["station_id_canonical"],
            r["station_name_raw"],
            r["city_id"],
            r["pollutant_code"],
            r["value_normalised"],
            r["unit_standard"],
            r["unit_original"],
            r["timestamp_utc"],
            r["timestamp_ist"],
            r["hour_utc"],
            r["date_utc"],
            r["source_row_id"],
        )
        for r in rows
    ]
    sql = """
        INSERT INTO staging.measurements (
            run_id, source_system, station_id_canonical, station_name_raw, city_id,
            pollutant_code, value_normalised, unit_standard, unit_original,
            timestamp_utc, timestamp_ist, hour_utc, date_utc, source_row_id
        )
        VALUES %s
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        conn.commit()


def validate_and_clean(run_id: str) -> int:
    with get_engine().begin() as conn:
        conn.execute(
            text("""
                DELETE FROM ops.rejected_records
                WHERE run_id = :run_id
                  AND layer = 'staging'
                  AND rule_violated = 'pollutant_range_validation'
            """),
            {"run_id": run_id},
        )
        conn.execute(
            text("""
                DELETE FROM ops.dq_results
                WHERE run_id = :run_id
                  AND layer = 'staging'
                  AND check_name = 'pollutant_range_validation'
            """),
            {"run_id": run_id},
        )

    value_predicate, value_params, rejection_reason = _staging_validation_sql("m")
    valid_predicate = (
        f"{value_predicate} AND m.station_id_canonical IS NOT NULL "
        "AND m.city_id IS NOT NULL AND m.hour_utc IS NOT NULL"
    )
    query_params = {"run_id": run_id, **value_params}
    with get_engine().connect() as conn:
        counts = conn.execute(
            text(f"""
                SELECT COUNT(*) AS total_count,
                       COUNT(*) FILTER (WHERE {valid_predicate}) AS passed_count
                FROM staging.measurements AS m
                WHERE m.run_id = :run_id
            """),
            query_params,
        ).mappings().one()
    total_count = int(counts["total_count"])
    passed_count = int(counts["passed_count"])
    failed_count = total_count - passed_count

    if total_count == 0:
        log_dq_result(
            run_id=run_id,
            check_name="pollutant_range_validation",
            layer="staging",
            source_id=None,
            passed_count=0,
            failed_count=0,
            threshold=float(os.environ.get("DQ_REJECTION_THRESHOLD", "0.20")),
            status="SKIP",
            notes="No staging measurements for this run",
        )
        _load_clean_weather()
        return 0

    with get_engine().begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO ops.rejected_records (
                    run_id, source_id, layer, original_row, rule_violated,
                    rejection_reason, pollutant_code, station_id, timestamp_utc
                )
                SELECT m.run_id, m.source_system, 'staging', to_jsonb(m),
                       'pollutant_range_validation', {rejection_reason},
                       m.pollutant_code, m.station_id_canonical, m.timestamp_utc
                FROM staging.measurements AS m
                WHERE m.run_id = :run_id
                  AND NOT ({valid_predicate})
            """),
            query_params,
        )

    threshold = float(os.environ.get("DQ_REJECTION_THRESHOLD", "0.20"))
    status = "FAIL" if failed_count and failed_count / total_count > threshold else "PASS"
    log_dq_result(
        run_id=run_id,
        check_name="pollutant_range_validation",
        layer="staging",
        source_id=None,
        passed_count=passed_count,
        failed_count=failed_count,
        threshold=threshold,
        status=status,
    )
    if status == "FAIL":
        raise RuntimeError(
            f"DQ gate failed: {failed_count} rejected of {total_count} staging measurement rows "
            f"({failed_count / total_count:.2%}) exceeds threshold {threshold:.2%}"
        )
    clean_rows = _dedupe_clean_from_staging(run_id, valid_predicate, query_params)
    _load_clean_weather()
    _build_completeness()
    return clean_rows


def _staging_validation_sql(alias: str) -> tuple[str, dict[str, Any], str]:
    """Build parameterized SQL that mirrors the configured pollutant range rules."""
    params: dict[str, Any] = {}
    valid_branches = []
    reason_cases = [
        f"WHEN {alias}.value_normalised IS NULL "
        f"OR {alias}.value_normalised::text IN ('NaN', 'Infinity', '-Infinity') "
        "THEN 'missing or non-numeric value'"
    ]
    for index, (pollutant, spec) in enumerate(load_pollutants().items()):
        code_param = f"pollutant_{index}"
        params[code_param] = pollutant
        terms = [
            f"{alias}.pollutant_code = :{code_param}",
            f"{alias}.value_normalised IS NOT NULL",
            f"{alias}.value_normalised::text NOT IN ('NaN', 'Infinity', '-Infinity')",
        ]
        sentinel_values = [float(value) for value in spec.get("sentinel_values", [])]
        if sentinel_values:
            sentinel_param = f"sentinels_{index}"
            params[sentinel_param] = sentinel_values
            terms.append(f"{alias}.value_normalised != ALL(:{sentinel_param})")
            reason_cases.append(
                f"WHEN {alias}.pollutant_code = :{code_param} "
                f"AND {alias}.value_normalised = ANY(:{sentinel_param}) THEN 'sentinel value'"
            )
        minimum = spec.get("physical_min")
        if minimum is not None:
            min_param = f"minimum_{index}"
            params[min_param] = float(minimum)
            terms.append(f"{alias}.value_normalised >= :{min_param}")
            reason_cases.append(
                f"WHEN {alias}.pollutant_code = :{code_param} "
                f"AND {alias}.value_normalised < :{min_param} "
                f"THEN 'value below physical minimum {float(minimum):g}'"
            )
        maximum = spec.get("physical_max")
        if maximum is not None:
            max_param = f"maximum_{index}"
            params[max_param] = float(maximum)
            terms.append(f"{alias}.value_normalised <= :{max_param}")
            reason_cases.append(
                f"WHEN {alias}.pollutant_code = :{code_param} "
                f"AND {alias}.value_normalised > :{max_param} "
                f"THEN 'value above physical maximum {float(maximum):g}'"
            )
        valid_branches.append("(" + " AND ".join(terms) + ")")

    value_predicate = "(" + " OR ".join(valid_branches) + ")"
    reason_cases.append(
        f"WHEN {alias}.station_id_canonical IS NULL OR {alias}.city_id IS NULL "
        "THEN 'missing station or city'"
    )
    reason_cases.append(
        f"WHEN {alias}.hour_utc IS NULL THEN 'missing normalized hour'"
    )
    reason_cases.append("ELSE 'unknown pollutant or outside configured range'")
    reason_sql = "CASE " + " ".join(reason_cases) + " END"
    return value_predicate, params, reason_sql


def _dedupe_clean_from_staging(
    run_id: str,
    valid_predicate: str,
    params: dict[str, Any],
) -> int:
    priority = load_source_priority()
    priority_params = {}
    priority_cases = []
    for index, (source, rank) in enumerate(priority.items()):
        source_param = f"source_{index}"
        rank_param = f"priority_{index}"
        priority_params[source_param] = source
        priority_params[rank_param] = rank
        priority_cases.append(f"WHEN :{source_param} THEN :{rank_param}")
    priority_sql = "CASE m.source_system " + " ".join(priority_cases) + " ELSE 99 END"
    sql = text(f"""
        INSERT INTO clean.measurements_hourly (
            run_id, station_id_canonical, city_id, pollutant_code, hour_utc,
            date_utc, hour_of_day, value, unit, source_system, source_priority
        )
        SELECT run_id, station_id_canonical, city_id, pollutant_code, hour_utc,
               date_utc, EXTRACT(HOUR FROM hour_utc)::smallint,
               value_normalised, unit_standard, source_system, source_priority
        FROM (
            SELECT DISTINCT ON (m.station_id_canonical, m.pollutant_code, m.hour_utc)
                   m.run_id, m.station_id_canonical, m.city_id, m.pollutant_code,
                   m.hour_utc, m.date_utc, m.value_normalised, m.unit_standard,
                   m.source_system, ({priority_sql})::smallint AS source_priority
            FROM staging.measurements AS m
            WHERE m.run_id = :run_id
              AND {valid_predicate}
            ORDER BY m.station_id_canonical, m.pollutant_code, m.hour_utc,
                     source_priority, m.ingested_at, m.id
        ) AS winners
        ON CONFLICT (station_id_canonical, pollutant_code, hour_utc) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            value = EXCLUDED.value,
            unit = EXCLUDED.unit,
            source_system = EXCLUDED.source_system,
            source_priority = EXCLUDED.source_priority
    """)
    with get_engine().begin() as conn:
        result = conn.execute(sql, {**params, **priority_params})
    return int(result.rowcount or 0)


def _load_clean_weather() -> None:
    sql = """
        INSERT INTO clean.weather_hourly (
            city_id, hour_utc, date_utc, temperature_2m, relative_humidity_2m,
            precipitation, wind_speed_10m, wind_direction_10m, surface_pressure,
            cloud_cover, visibility
        )
        SELECT city_id, timestamp_utc, timestamp_utc::date, temperature_2m,
               relative_humidity_2m, precipitation, wind_speed_10m,
               wind_direction_10m, surface_pressure, cloud_cover, visibility
        FROM staging.weather
        ON CONFLICT (city_id, hour_utc) DO UPDATE SET
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            precipitation = EXCLUDED.precipitation,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            wind_direction_10m = EXCLUDED.wind_direction_10m,
            surface_pressure = EXCLUDED.surface_pressure,
            cloud_cover = EXCLUDED.cloud_cover,
            visibility = EXCLUDED.visibility
    """
    with get_engine().begin() as conn:
        conn.execute(text(sql))


def build_daily_averages(run_id: str) -> int:
    sync_dim_station()
    daily = pd.read_sql(
        """
        WITH rolling AS (
            SELECT m.station_id_canonical, m.city_id, m.pollutant_code,
                   m.hour_utc, m.date_utc, m.value, m.unit,
                   p.aqi_averaging,
                   AVG(m.value) OVER (
                       PARTITION BY m.station_id_canonical, m.pollutant_code
                       ORDER BY m.hour_utc
                       RANGE BETWEEN INTERVAL '7 hours' PRECEDING AND CURRENT ROW
                   ) AS rolling_mean_8h,
                   COUNT(*) OVER (
                       PARTITION BY m.station_id_canonical, m.pollutant_code
                       ORDER BY m.hour_utc
                       RANGE BETWEEN INTERVAL '7 hours' PRECEDING AND CURRENT ROW
                   ) AS rolling_count_8h
            FROM clean.measurements_hourly AS m
            JOIN analytics.dim_pollutant AS p USING (pollutant_code)
            WHERE p.aqi_averaging IN ('24h_mean', '8h_max')
        )
        SELECT station_id_canonical, city_id, pollutant_code, date_utc,
               aqi_averaging,
               AVG(value) FILTER (WHERE aqi_averaging = '24h_mean') AS daily_mean,
               MAX(rolling_mean_8h) FILTER (
                   WHERE aqi_averaging = '8h_max' AND rolling_count_8h = 8
               ) AS eight_hour_max,
               COUNT(value)::integer AS obs_count,
               MIN(unit) AS unit
        FROM rolling
        GROUP BY station_id_canonical, city_id, pollutant_code, date_utc, aqi_averaging
        """,
        get_engine(),
    )
    if daily.empty:
        return 0
    rows = []
    for row in daily.to_dict("records"):
        concentration = (
            row["eight_hour_max"]
            if row["aqi_averaging"] == "8h_max"
            else row["daily_mean"]
        )
        rows.append(
            (
                row["station_id_canonical"],
                row["city_id"],
                row["pollutant_code"],
                row["date_utc"],
                row["daily_mean"],
                row["eight_hour_max"],
                sub_index(row["pollutant_code"], concentration),
                int(row["obs_count"]),
                row["unit"],
            )
        )
    sql = """
        INSERT INTO clean.daily_averages (
            station_id_canonical, city_id, pollutant_code, date_utc, daily_mean,
            eight_hour_max, sub_index, obs_count, unit
        )
        VALUES %s
        ON CONFLICT (station_id_canonical, pollutant_code, date_utc) DO UPDATE SET
            daily_mean = EXCLUDED.daily_mean,
            eight_hour_max = EXCLUDED.eight_hour_max,
            sub_index = EXCLUDED.sub_index,
            obs_count = EXCLUDED.obs_count,
            unit = EXCLUDED.unit,
            computed_at = NOW()
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
    _build_station_daily_aqi()
    return len(rows)


def build_analytics() -> dict[str, int]:
    statements = [_sql_fact_hourly(), _sql_city_daily(), _sql_pollutant_daily(), _sql_pollutant_contribution(), _sql_hourly_pattern(), _sql_monthly_pattern(), _sql_weather_impact(), _sql_data_quality()]
    counts = {}
    with get_engine().begin() as conn:
        for name, sql in statements:
            conn.execute(text(sql))
            counts[name] = conn.execute(text(f"SELECT COUNT(*) FROM analytics.{name}")).scalar() or 0
    return counts


def sync_dim_station() -> None:
    with get_engine().begin() as conn:
        conn.execute(text(_sql_sync_dim_station()[1]))


def _measurement_record(run_id: str, source: str, station_id: str, station_name: str | None, city_id: str, pollutant: str, value: float | None, unit: str, original_unit: str | None, ts: pd.Timestamp, source_row_id: int) -> dict[str, Any]:
    hour = ts.floor("h")
    return {
        "run_id": run_id,
        "source_system": source,
        "station_id_canonical": station_id,
        "station_name_raw": station_name,
        "city_id": city_id,
        "pollutant_code": pollutant,
        "value_normalised": value,
        "unit_standard": unit,
        "unit_original": original_unit,
        "timestamp_utc": ts.to_pydatetime(),
        "timestamp_ist": ts.tz_convert("Asia/Kolkata").to_pydatetime(),
        "hour_utc": hour.to_pydatetime(),
        "date_utc": hour.date(),
        "source_row_id": source_row_id,
    }


@lru_cache(maxsize=1)
def _configured_cities() -> tuple[dict[str, Any], ...]:
    return tuple(load_cities())


def _match_city(name: object, lat: object, lon: object) -> str:
    text_name = str(name or "").strip().lower()
    cities = _configured_cities()
    for city in cities:
        if city["id"] in text_name or city["name"].lower() in text_name:
            return city["id"]
    if lat is not None and lon is not None:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            return min(cities, key=lambda c: (float(c["lat"]) - lat_f) ** 2 + (float(c["lon"]) - lon_f) ** 2)["id"]
        except (TypeError, ValueError):
            pass
    return next((c["id"] for c in cities if c.get("primary")), cities[0]["id"])


def _station_id(source: str, city_id: str, station_name: object) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(station_name or city_id)).strip("_")
    while "__" in text:
        text = text.replace("__", "_")
    return f"{source}_{city_id}_{text or 'city'}"


def _upsert_station_rows(rows: list[tuple]) -> None:
    if not rows:
        return
    unique_rows: dict[str, tuple] = {}
    for row in rows:
        unique_rows.setdefault(row[0], row)
    rows = list(unique_rows.values())
    sql = """
        INSERT INTO staging.station_dim (
            station_id_canonical, station_name_canonical, city_id, country,
            lat, lon, source_system, openaq_location_id
        )
        VALUES %s
        ON CONFLICT (station_id_canonical) DO UPDATE SET
            station_name_canonical = EXCLUDED.station_name_canonical,
            city_id = EXCLUDED.city_id,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            source_system = EXCLUDED.source_system,
            openaq_location_id = EXCLUDED.openaq_location_id,
            updated_at = NOW()
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


def _upsert_generic_stations(rows: list[dict[str, Any]], source: str) -> None:
    station_rows = []
    city_lookup = {c["id"]: c for c in load_cities()}
    seen = set()
    for row in rows:
        station_id = row["station_id_canonical"]
        if station_id in seen:
            continue
        seen.add(station_id)
        city = city_lookup[row["city_id"]]
        station_rows.append(
            (
                station_id,
                row.get("station_name_raw") or station_id,
                row["city_id"],
                city.get("state"),
                city.get("country", "IN"),
                city.get("lat"),
                city.get("lon"),
                source,
                None,
            )
        )
    sql = """
        INSERT INTO staging.station_dim (
            station_id_canonical, station_name_canonical, city_id, state, country,
            lat, lon, source_system, openaq_location_id
        )
        VALUES %s
        ON CONFLICT (station_id_canonical) DO UPDATE SET
            station_name_canonical = EXCLUDED.station_name_canonical,
            city_id = EXCLUDED.city_id,
            state = EXCLUDED.state,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            source_system = EXCLUDED.source_system,
            updated_at = NOW()
    """
    if station_rows:
        with get_raw_connection() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, station_rows)
            conn.commit()


def _build_completeness() -> None:
    sql = """
        INSERT INTO clean.station_hourly_completeness (
            station_id_canonical, city_id, pollutant_code, period_start,
            period_end, expected_hours, present_hours
        )
        SELECT
            station_id_canonical,
            city_id,
            pollutant_code,
            MIN(date_utc),
            MAX(date_utc),
            GREATEST(EXTRACT(EPOCH FROM (MAX(hour_utc) - MIN(hour_utc)))::int / 3600 + 1, 1),
            COUNT(DISTINCT hour_utc)::int
        FROM clean.measurements_hourly
        GROUP BY station_id_canonical, city_id, pollutant_code
        ON CONFLICT (station_id_canonical, pollutant_code, period_start, period_end)
        DO UPDATE SET present_hours = EXCLUDED.present_hours, computed_at = NOW()
    """
    with get_engine().begin() as conn:
        conn.execute(text(sql))


def _build_station_daily_aqi() -> None:
    daily = pd.read_sql("SELECT * FROM clean.daily_averages", get_engine())
    if daily.empty:
        return
    rows = []
    for (station, city, dt), group in daily.groupby(["station_id_canonical", "city_id", "date_utc"]):
        subs = {r["pollutant_code"]: r["sub_index"] for r in group.to_dict("records")}
        aqi_value, dominant = final_aqi(subs)
        cat, cat_num = category(aqi_value)
        values = {r["pollutant_code"]: r for r in group.to_dict("records")}
        rows.append(
            (
                station,
                city,
                dt,
                aqi_value,
                cat,
                cat_num,
                dominant,
                _get_metric(values, "PM2.5", "daily_mean"),
                _get_metric(values, "PM10", "daily_mean"),
                _get_metric(values, "NO2", "daily_mean"),
                _get_metric(values, "SO2", "daily_mean"),
                _get_metric(values, "CO", "eight_hour_max"),
                _get_metric(values, "O3", "eight_hour_max"),
                _get_metric(values, "NH3", "daily_mean"),
                int(group["obs_count"].max() or 0),
            )
        )
    sql = """
        INSERT INTO analytics.mart_station_daily_aqi (
            station_id_canonical, city_id, date_utc, aqi, aqi_category,
            aqi_category_num, dominant_pollutant, pm25_24h, pm10_24h,
            no2_24h, so2_24h, co_8h_max, o3_8h_max, nh3_24h, obs_hours
        )
        VALUES %s
        ON CONFLICT (station_id_canonical, date_utc) DO UPDATE SET
            aqi = EXCLUDED.aqi,
            aqi_category = EXCLUDED.aqi_category,
            aqi_category_num = EXCLUDED.aqi_category_num,
            dominant_pollutant = EXCLUDED.dominant_pollutant,
            pm25_24h = EXCLUDED.pm25_24h,
            pm10_24h = EXCLUDED.pm10_24h,
            no2_24h = EXCLUDED.no2_24h,
            so2_24h = EXCLUDED.so2_24h,
            co_8h_max = EXCLUDED.co_8h_max,
            o3_8h_max = EXCLUDED.o3_8h_max,
            nh3_24h = EXCLUDED.nh3_24h,
            obs_hours = EXCLUDED.obs_hours,
            computed_at = NOW()
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


def _get_metric(values: dict[str, dict], pollutant: str, field: str) -> float | None:
    row = values.get(pollutant)
    return None if not row else row.get(field)


def _sql_sync_dim_station() -> tuple[str, str]:
    return (
        "dim_station",
        """
        INSERT INTO analytics.dim_station (
            station_id_canonical, station_name, city_id, state, lat, lon,
            source_system, openaq_location_id
        )
        SELECT station_id_canonical, station_name_canonical, city_id, state, lat, lon,
               source_system, openaq_location_id
        FROM staging.station_dim
        ON CONFLICT (station_id_canonical) DO UPDATE SET
            station_name = EXCLUDED.station_name,
            city_id = EXCLUDED.city_id,
            state = EXCLUDED.state,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            source_system = EXCLUDED.source_system,
            openaq_location_id = EXCLUDED.openaq_location_id
        """,
    )


def _sql_fact_hourly() -> tuple[str, str]:
    return (
        "fact_station_hourly",
        """
        INSERT INTO analytics.fact_station_hourly (
            station_id_canonical, city_id, hour_utc, date_utc, hour_of_day,
            pm25, pm10, no2, so2, co, o3, nh3, nox,
            temperature_2m, relative_humidity, precipitation, wind_speed,
            wind_direction, surface_pressure, has_weather, pollutant_count,
            source_systems
        )
        SELECT
            m.station_id_canonical,
            m.city_id,
            m.hour_utc,
            m.date_utc,
            m.hour_of_day,
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'PM2.5'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'PM10'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'NO2'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'SO2'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'CO'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'O3'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'NH3'),
            MAX(m.value) FILTER (WHERE m.pollutant_code = 'NOx'),
            MAX(w.temperature_2m),
            MAX(w.relative_humidity_2m),
            MAX(w.precipitation),
            MAX(w.wind_speed_10m),
            MAX(w.wind_direction_10m),
            MAX(w.surface_pressure),
            BOOL_OR(w.city_id IS NOT NULL),
            COUNT(DISTINCT m.pollutant_code)::smallint,
            ARRAY_AGG(DISTINCT m.source_system)
        FROM clean.measurements_hourly m
        LEFT JOIN clean.weather_hourly w ON w.city_id = m.city_id AND w.hour_utc = m.hour_utc
        GROUP BY m.station_id_canonical, m.city_id, m.hour_utc, m.date_utc, m.hour_of_day
        ON CONFLICT (station_id_canonical, hour_utc) DO UPDATE SET
            pm25 = EXCLUDED.pm25,
            pm10 = EXCLUDED.pm10,
            no2 = EXCLUDED.no2,
            so2 = EXCLUDED.so2,
            co = EXCLUDED.co,
            o3 = EXCLUDED.o3,
            nh3 = EXCLUDED.nh3,
            nox = EXCLUDED.nox,
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity = EXCLUDED.relative_humidity,
            precipitation = EXCLUDED.precipitation,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            surface_pressure = EXCLUDED.surface_pressure,
            has_weather = EXCLUDED.has_weather,
            pollutant_count = EXCLUDED.pollutant_count,
            source_systems = EXCLUDED.source_systems
        """,
    )


def _sql_city_daily() -> tuple[str, str]:
    return (
        "mart_city_daily_aqi",
        """
        INSERT INTO analytics.mart_city_daily_aqi (
            city_id, date_utc, aqi_mean, aqi_max, aqi_min, aqi_category,
            aqi_category_num, dominant_pollutant, station_count
        )
        SELECT
            city_id,
            date_utc,
            AVG(aqi),
            MAX(aqi),
            MIN(aqi),
            CASE
                WHEN AVG(aqi) <= 50 THEN 'Good'
                WHEN AVG(aqi) <= 100 THEN 'Satisfactory'
                WHEN AVG(aqi) <= 200 THEN 'Moderate'
                WHEN AVG(aqi) <= 300 THEN 'Poor'
                WHEN AVG(aqi) <= 400 THEN 'Very Poor'
                ELSE 'Severe'
            END,
            CASE
                WHEN AVG(aqi) <= 50 THEN 1
                WHEN AVG(aqi) <= 100 THEN 2
                WHEN AVG(aqi) <= 200 THEN 3
                WHEN AVG(aqi) <= 300 THEN 4
                WHEN AVG(aqi) <= 400 THEN 5
                ELSE 6
            END,
            MODE() WITHIN GROUP (ORDER BY dominant_pollutant),
            COUNT(DISTINCT station_id_canonical)::smallint
        FROM analytics.mart_station_daily_aqi
        WHERE aqi IS NOT NULL
        GROUP BY city_id, date_utc
        ON CONFLICT (city_id, date_utc) DO UPDATE SET
            aqi_mean = EXCLUDED.aqi_mean,
            aqi_max = EXCLUDED.aqi_max,
            aqi_min = EXCLUDED.aqi_min,
            aqi_category = EXCLUDED.aqi_category,
            aqi_category_num = EXCLUDED.aqi_category_num,
            dominant_pollutant = EXCLUDED.dominant_pollutant,
            station_count = EXCLUDED.station_count,
            computed_at = NOW()
        """,
    )


def _sql_pollutant_daily() -> tuple[str, str]:
    return (
        "mart_pollutant_daily",
        """
        INSERT INTO analytics.mart_pollutant_daily (
            city_id, pollutant_code, date_utc, daily_mean, daily_max,
            daily_min, daily_std, sub_index_mean, station_count
        )
        SELECT city_id, pollutant_code, date_utc, AVG(COALESCE(daily_mean, eight_hour_max)),
               MAX(COALESCE(daily_mean, eight_hour_max)), MIN(COALESCE(daily_mean, eight_hour_max)),
               STDDEV(COALESCE(daily_mean, eight_hour_max)), AVG(sub_index),
               COUNT(DISTINCT station_id_canonical)::smallint
        FROM clean.daily_averages
        GROUP BY city_id, pollutant_code, date_utc
        ON CONFLICT (city_id, pollutant_code, date_utc) DO UPDATE SET
            daily_mean = EXCLUDED.daily_mean,
            daily_max = EXCLUDED.daily_max,
            daily_min = EXCLUDED.daily_min,
            daily_std = EXCLUDED.daily_std,
            sub_index_mean = EXCLUDED.sub_index_mean,
            station_count = EXCLUDED.station_count,
            computed_at = NOW()
        """,
    )


def _sql_pollutant_contribution() -> tuple[str, str]:
    return (
        "mart_pollutant_contribution",
        """
        INSERT INTO analytics.mart_pollutant_contribution (
            city_id, year, month, pollutant_code, dominant_days, total_days, contribution_pct
        )
        SELECT city_id, EXTRACT(YEAR FROM date_utc)::smallint, EXTRACT(MONTH FROM date_utc)::smallint,
               dominant_pollutant, COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY city_id, EXTRACT(YEAR FROM date_utc), EXTRACT(MONTH FROM date_utc)),
               ROUND((100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY city_id, EXTRACT(YEAR FROM date_utc), EXTRACT(MONTH FROM date_utc)), 0))::numeric, 2)
        FROM analytics.mart_station_daily_aqi
        WHERE dominant_pollutant IS NOT NULL
        GROUP BY city_id, EXTRACT(YEAR FROM date_utc), EXTRACT(MONTH FROM date_utc), dominant_pollutant
        ON CONFLICT (city_id, year, month, pollutant_code) DO UPDATE SET
            dominant_days = EXCLUDED.dominant_days,
            total_days = EXCLUDED.total_days,
            contribution_pct = EXCLUDED.contribution_pct,
            computed_at = NOW()
        """,
    )


def _sql_hourly_pattern() -> tuple[str, str]:
    return (
        "mart_hourly_pattern",
        """
        INSERT INTO analytics.mart_hourly_pattern (city_id, hour_of_day, day_of_week, aqi_mean, pm25_mean, obs_count)
        SELECT f.city_id, f.hour_of_day, d.day_of_week, AVG(s.aqi), AVG(f.pm25), COUNT(*)
        FROM analytics.fact_station_hourly f
        JOIN analytics.dim_date d ON d.date_id = f.date_utc
        LEFT JOIN analytics.mart_station_daily_aqi s ON s.station_id_canonical = f.station_id_canonical AND s.date_utc = f.date_utc
        GROUP BY f.city_id, f.hour_of_day, d.day_of_week
        ON CONFLICT (city_id, hour_of_day, day_of_week) DO UPDATE SET
            aqi_mean = EXCLUDED.aqi_mean,
            pm25_mean = EXCLUDED.pm25_mean,
            obs_count = EXCLUDED.obs_count,
            computed_at = NOW()
        """,
    )


def _sql_monthly_pattern() -> tuple[str, str]:
    return (
        "mart_monthly_pattern",
        """
        INSERT INTO analytics.mart_monthly_pattern (
            city_id, year, month, aqi_mean, aqi_p75, pm25_mean, pm10_mean,
            good_days, satisfactory_days, moderate_days, poor_days, very_poor_days,
            severe_days, total_days
        )
        SELECT city_id, EXTRACT(YEAR FROM date_utc)::smallint, EXTRACT(MONTH FROM date_utc)::smallint,
               AVG(aqi), PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY aqi),
               AVG(pm25_24h), AVG(pm10_24h),
               COUNT(*) FILTER (WHERE aqi_category = 'Good'),
               COUNT(*) FILTER (WHERE aqi_category = 'Satisfactory'),
               COUNT(*) FILTER (WHERE aqi_category = 'Moderate'),
               COUNT(*) FILTER (WHERE aqi_category = 'Poor'),
               COUNT(*) FILTER (WHERE aqi_category = 'Very Poor'),
               COUNT(*) FILTER (WHERE aqi_category = 'Severe'),
               COUNT(*)
        FROM analytics.mart_station_daily_aqi
        GROUP BY city_id, EXTRACT(YEAR FROM date_utc), EXTRACT(MONTH FROM date_utc)
        ON CONFLICT (city_id, year, month) DO UPDATE SET
            aqi_mean = EXCLUDED.aqi_mean,
            aqi_p75 = EXCLUDED.aqi_p75,
            pm25_mean = EXCLUDED.pm25_mean,
            pm10_mean = EXCLUDED.pm10_mean,
            good_days = EXCLUDED.good_days,
            satisfactory_days = EXCLUDED.satisfactory_days,
            moderate_days = EXCLUDED.moderate_days,
            poor_days = EXCLUDED.poor_days,
            very_poor_days = EXCLUDED.very_poor_days,
            severe_days = EXCLUDED.severe_days,
            total_days = EXCLUDED.total_days,
            computed_at = NOW()
        """,
    )


def _sql_weather_impact() -> tuple[str, str]:
    return (
        "mart_weather_impact",
        """
        INSERT INTO analytics.mart_weather_impact (
            city_id, weather_variable, bin_label, bin_low, bin_high, aqi_mean, aqi_median, obs_count, pearson_r
        )
        SELECT f.city_id, 'temperature', 'all', MIN(f.temperature_2m), MAX(f.temperature_2m),
               AVG(s.aqi), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.aqi), COUNT(*),
               CORR(f.temperature_2m, s.aqi)
        FROM analytics.fact_station_hourly f
        JOIN analytics.mart_station_daily_aqi s ON s.station_id_canonical = f.station_id_canonical AND s.date_utc = f.date_utc
        WHERE f.temperature_2m IS NOT NULL AND s.aqi IS NOT NULL
        GROUP BY f.city_id
        ON CONFLICT (city_id, weather_variable, bin_label) DO UPDATE SET
            bin_low = EXCLUDED.bin_low,
            bin_high = EXCLUDED.bin_high,
            aqi_mean = EXCLUDED.aqi_mean,
            aqi_median = EXCLUDED.aqi_median,
            obs_count = EXCLUDED.obs_count,
            pearson_r = EXCLUDED.pearson_r,
            computed_at = NOW()
        """,
    )


def _sql_data_quality() -> tuple[str, str]:
    return (
        "mart_data_quality",
        """
        INSERT INTO analytics.mart_data_quality (
            station_id_canonical, city_id, year, month, pollutant_code,
            completeness_pct, rejected_count, total_expected
        )
        SELECT c.station_id_canonical, c.city_id, EXTRACT(YEAR FROM c.period_start)::smallint,
               EXTRACT(MONTH FROM c.period_start)::smallint, c.pollutant_code,
               SUM(c.present_hours)::numeric * 100 / NULLIF(SUM(c.expected_hours), 0),
               0, SUM(c.expected_hours)
        FROM clean.station_hourly_completeness c
        GROUP BY c.station_id_canonical, c.city_id, EXTRACT(YEAR FROM c.period_start),
                 EXTRACT(MONTH FROM c.period_start), c.pollutant_code
        ON CONFLICT (station_id_canonical, year, month, pollutant_code) DO UPDATE SET
            completeness_pct = EXCLUDED.completeness_pct,
            total_expected = EXCLUDED.total_expected,
            computed_at = NOW()
        """,
    )

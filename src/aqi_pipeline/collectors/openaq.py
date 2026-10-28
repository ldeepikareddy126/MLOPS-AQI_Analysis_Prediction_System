"""OpenAQ v3 collector.

Uses the current v3 API with the X-API-Key header. Relevant documented
resources are /v3/locations, /v3/locations/{id}/sensors and
/v3/sensors/{sensors_id}/measurements.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from uuid import UUID
from collections.abc import Iterator

import psycopg2.extras
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from aqi_pipeline.landing.raw_files import land_json
from aqi_pipeline.quality.ops import json_dumps, log_ingestion
from aqi_pipeline.utils.config import load_cities
from aqi_pipeline.utils.db import get_raw_connection
from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)

BASE_URL = "https://api.openaq.org/v3"
LOCATION_RADIUS_METERS = 25_000
LOCATION_PAGE_SIZE = 100
MEASUREMENT_PAGE_SIZE = 1_000
PLACEHOLDER_VALUES = {"", "your_openaq_api_key_here", "changeme"}


class OpenAQError(RuntimeError):
    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def _retry_wait(retry_state) -> float:
    error = retry_state.outcome.exception()
    if isinstance(error, OpenAQError) and error.retry_after is not None:
        return error.retry_after
    return min(5 * (2 ** (retry_state.attempt_number - 1)), 120)


def _resume_cursor(completed_pages: dict[int, int]) -> tuple[int, bool]:
    """Return the first page to fetch and whether the sensor is already complete."""
    page = 1
    while page in completed_pages:
        if completed_pages[page] < MEASUREMENT_PAGE_SIZE:
            return page, True
        page += 1
    return page, False


def _completed_measurement_pages(
    run_id: UUID | str,
    start_date: str,
    end_date: str,
) -> dict[str, dict[int, int]]:
    """Load successful pages so Airflow retries can resume partial source ingestion."""
    sql = """
        SELECT endpoint_or_file, (request_params->>'page')::INTEGER, rows_extracted
        FROM ops.ingestion_log
        WHERE run_id = %s
          AND source_id = 'openaq'
          AND status = 'SUCCESS'
          AND request_params->>'start_date' = %s
          AND request_params->>'end_date' = %s
          AND request_params ? 'page'
    """
    completed: dict[str, dict[int, int]] = {}
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (str(run_id), start_date, end_date))
        for endpoint, page, rows in cur.fetchall():
            completed.setdefault(endpoint, {})[page] = rows
    return completed


def _api_key() -> str | None:
    key = os.environ.get("OPENAQ_API_KEY", "").strip()
    return None if key in PLACEHOLDER_VALUES else key


@retry(
    retry=retry_if_exception_type(
        (
            requests.Timeout,
            requests.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            OpenAQError,
        )
    ),
    wait=_retry_wait,
    stop=stop_after_attempt(6),
    reraise=True,
)
def _get(session: requests.Session, endpoint: str, params: dict) -> tuple[dict, int, float]:
    started = time.perf_counter()
    response = session.get(f"{BASE_URL}{endpoint}", params=params, timeout=60)
    duration = time.perf_counter() - started
    if response.status_code == 429:
        try:
            retry_after = float(response.headers.get("Retry-After"))
        except (TypeError, ValueError):
            retry_after = None
        raise OpenAQError(
            f"Transient OpenAQ HTTP 429 for {endpoint}",
            retry_after=retry_after,
        )
    if 500 <= response.status_code < 600:
        raise OpenAQError(f"Transient OpenAQ HTTP {response.status_code} for {endpoint}")
    response.raise_for_status()
    return response.json(), response.status_code, duration


def _get_pages(
    session: requests.Session,
    endpoint: str,
    params: dict,
    start_page: int = 1,
) -> Iterator[tuple[dict, int, float, int]]:
    """Yield all result pages, stopping at the first short or empty page."""
    page = start_page
    limit = int(params["limit"])
    while True:
        payload, status_code, duration = _get(
            session, endpoint, {**params, "page": page}
        )
        results = payload.get("results")
        if not isinstance(results, list):
            raise OpenAQError(f"OpenAQ response for {endpoint} has no results list")
        yield payload, status_code, duration, page
        if len(results) < limit:
            break
        page += 1


def collect(run_id: UUID | str, start_date: str, end_date: str) -> int:
    key = _api_key()
    if not key:
        log_ingestion(
            run_id=run_id,
            source_id="openaq",
            endpoint_or_file=BASE_URL,
            status="SKIPPED",
            rows_extracted=0,
            error_message="OPENAQ_API_KEY is not configured yet",
        )
        return 0

    session = requests.Session()
    session.headers.update({"X-API-Key": key, "User-Agent": "aqi-mlops-course-project/0.1"})
    total = 0
    failures = []
    completed_pages = _completed_measurement_pages(run_id, start_date, end_date)
    for city in load_cities():
        try:
            for locations, status_code, duration, page in _get_pages(
                session,
                "/locations",
                {
                    "coordinates": f"{city['lat']},{city['lon']}",
                    "radius": LOCATION_RADIUS_METERS,
                    "limit": LOCATION_PAGE_SIZE,
                },
            ):
                path, checksum = land_json(
                    "openaq", str(run_id), f"{city['id']}_locations_page_{page}", locations
                )
                loc_rows = _insert_locations(run_id, city["id"], locations)
                log_ingestion(
                    run_id=run_id,
                    source_id="openaq",
                    endpoint_or_file=f"{BASE_URL}/locations",
                    request_params={
                        "city": city["id"],
                        "radius": LOCATION_RADIUS_METERS,
                        "page": page,
                    },
                    status="SUCCESS",
                    http_status=status_code,
                    rows_extracted=loc_rows,
                    duration_s=duration,
                    file_path=str(path),
                    file_checksum=checksum,
                )
                for location in locations["results"]:
                    total += _collect_location_measurements(
                        session,
                        run_id,
                        city["id"],
                        location,
                        start_date,
                        end_date,
                        completed_pages,
                    )
        except Exception as exc:
            log.exception("OpenAQ failed for city %s", city["id"])
            failures.append(city["id"])
            log_ingestion(
                run_id=run_id,
                source_id="openaq",
                endpoint_or_file=BASE_URL,
                request_params={"city": city["id"]},
                status="FAILED",
                rows_extracted=0,
                error_message=str(exc),
            )
    if failures:
        raise OpenAQError(
            "OpenAQ collection failed for city/cities: " + ", ".join(failures)
        )
    return total


def _collect_location_measurements(
    session: requests.Session,
    run_id: UUID | str,
    city_id: str,
    location: dict,
    start_date: str,
    end_date: str,
    completed_pages: dict[str, dict[int, int]] | None = None,
) -> int:
    sensors = location.get("sensors") or []
    rows = 0
    for sensor in sensors:
        sensor_id = sensor.get("id")
        if not sensor_id:
            continue
        endpoint = f"{BASE_URL}/sensors/{sensor_id}/measurements"
        first_page, complete = _resume_cursor(
            (completed_pages or {}).get(endpoint, {})
        )
        if complete:
            continue
        for payload, status_code, duration, page in _get_pages(
            session,
            f"/sensors/{sensor_id}/measurements",
            {
                "datetime_from": f"{start_date}T00:00:00Z",
                "datetime_to": f"{end_date}T23:59:59Z",
                "limit": MEASUREMENT_PAGE_SIZE,
            },
            start_page=first_page,
        ):
            path, checksum = land_json(
                "openaq",
                str(run_id),
                f"{city_id}_sensor_{sensor_id}_measurements_page_{page}",
                payload,
            )
            inserted = _insert_measurements(run_id, location, sensor, payload, str(path))
            log_ingestion(
                run_id=run_id,
                source_id="openaq",
                endpoint_or_file=f"{BASE_URL}/sensors/{sensor_id}/measurements",
                request_params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "page": page,
                },
                status="SUCCESS",
                http_status=status_code,
                rows_extracted=inserted,
                duration_s=duration,
                file_path=str(path),
                file_checksum=checksum,
            )
            rows += inserted
    return rows


def _insert_locations(run_id: UUID | str, city_id: str, payload: dict) -> int:
    rows = []
    for item in payload.get("results", []):
        coords = item.get("coordinates") or {}
        rows.append(
            (
                str(run_id),
                item.get("id"),
                item.get("name"),
                city_id,
                (item.get("country") or {}).get("code") or item.get("country"),
                coords.get("latitude"),
                coords.get("longitude"),
                json_dumps(item.get("sensors") or []),
                json_dumps(item),
            )
        )
    if not rows:
        return 0
    sql = """
        INSERT INTO raw.openaq_locations (
            run_id, location_id, location_name, city, country, lat, lon,
            sensors_json, raw_json
        )
        VALUES %s
        ON CONFLICT (location_id) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            location_name = EXCLUDED.location_name,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            sensors_json = EXCLUDED.sensors_json,
            raw_json = EXCLUDED.raw_json
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
        conn.commit()
    return len(rows)


def _insert_measurements(run_id: UUID | str, location: dict, sensor: dict, payload: dict, source_file: str) -> int:
    coords = location.get("coordinates") or {}
    parameter = sensor.get("parameter") or {}
    rows = []
    for item in payload.get("results", []):
        period = item.get("period") or {}
        dt = item.get("datetime") or {}
        rows.append(
            (
                str(run_id),
                location.get("id"),
                location.get("name"),
                sensor.get("id"),
                parameter.get("name") or parameter.get("displayName") or item.get("parameter"),
                item.get("value"),
                item.get("unit"),
                dt.get("utc") or period.get("datetimeFrom", {}).get("utc"),
                dt.get("local") or period.get("datetimeFrom", {}).get("local"),
                coords.get("latitude"),
                coords.get("longitude"),
                (location.get("country") or {}).get("code") or location.get("country"),
                location.get("city"),
                location.get("owner", {}).get("name") if isinstance(location.get("owner"), dict) else None,
                sensor.get("sensorType"),
                json_dumps(item),
                source_file,
            )
        )
    if not rows:
        return 0
    sql = """
        INSERT INTO raw.openaq_measurements (
            run_id, location_id, location_name, sensor_id, parameter, value,
            unit, date_utc, date_local, coordinates_lat, coordinates_lon,
            country, city, entity, sensor_type, raw_json, source_file
        )
        VALUES %s
        ON CONFLICT (sensor_id, parameter, date_utc) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            value = EXCLUDED.value,
            unit = EXCLUDED.unit,
            raw_json = EXCLUDED.raw_json,
            source_file = EXCLUDED.source_file
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
    return len(rows)

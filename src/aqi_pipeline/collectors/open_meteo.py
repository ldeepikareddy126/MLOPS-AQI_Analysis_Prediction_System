"""Open-Meteo historical weather collector."""

from __future__ import annotations

import time
from datetime import date
from uuid import UUID

import psycopg2.extras
import requests

from aqi_pipeline.landing.raw_files import land_json
from aqi_pipeline.quality.ops import json_dumps, log_ingestion
from aqi_pipeline.utils.config import load_cities
from aqi_pipeline.utils.db import get_raw_connection
from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
    "cloud_cover",
    "visibility",
]


def _request(params: dict) -> tuple[dict, int, float]:
    started = time.perf_counter()
    response = requests.get(ARCHIVE_URL, params=params, timeout=60)
    duration = time.perf_counter() - started
    response.raise_for_status()
    return response.json(), response.status_code, duration


def collect(run_id: UUID | str, start_date: str, end_date: str) -> int:
    total = 0
    for city in load_cities():
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(HOURLY),
            "timezone": "UTC",
        }
        try:
            payload, status_code, duration = _request(params)
            path, checksum = land_json("open_meteo", str(run_id), f"{city['id']}_weather", payload)
            rows = _insert_weather(run_id, city["id"], city["lat"], city["lon"], payload, str(path))
            log_ingestion(
                run_id=run_id,
                source_id="open_meteo",
                endpoint_or_file=ARCHIVE_URL,
                request_params=params,
                status="SUCCESS",
                http_status=status_code,
                rows_extracted=rows,
                duration_s=duration,
                file_path=str(path),
                file_checksum=checksum,
            )
            total += rows
        except Exception as exc:
            log.exception("Open-Meteo collection failed for %s", city["id"])
            log_ingestion(
                run_id=run_id,
                source_id="open_meteo",
                endpoint_or_file=ARCHIVE_URL,
                request_params=params,
                status="FAILED",
                rows_extracted=0,
                error_message=str(exc),
            )
    return total


def _insert_weather(run_id: UUID | str, city_id: str, lat: float, lon: float, payload: dict, source_file: str) -> int:
    hourly = payload.get("hourly", {})
    timestamps = hourly.get("time", [])
    values = []
    for idx, ts in enumerate(timestamps):
        timestamp_utc = f"{ts}:00+00:00" if len(str(ts)) == 16 else str(ts).replace("Z", "+00:00")
        values.append(
            (
                str(run_id),
                city_id,
                lat,
                lon,
                timestamp_utc,
                hourly.get("temperature_2m", [None] * len(timestamps))[idx],
                hourly.get("relative_humidity_2m", [None] * len(timestamps))[idx],
                hourly.get("precipitation", [None] * len(timestamps))[idx],
                hourly.get("wind_speed_10m", [None] * len(timestamps))[idx],
                hourly.get("wind_direction_10m", [None] * len(timestamps))[idx],
                hourly.get("surface_pressure", [None] * len(timestamps))[idx],
                hourly.get("cloud_cover", [None] * len(timestamps))[idx],
                hourly.get("visibility", [None] * len(timestamps))[idx],
                json_dumps(payload),
                source_file,
            )
        )
    if not values:
        return 0
    sql = """
        INSERT INTO raw.open_meteo_weather (
            run_id, city_id, lat, lon, timestamp_utc, temperature_2m,
            relative_humidity_2m, precipitation, wind_speed_10m,
            wind_direction_10m, surface_pressure, cloud_cover, visibility,
            raw_json, source_file
        )
        VALUES %s
        ON CONFLICT (city_id, timestamp_utc) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            precipitation = EXCLUDED.precipitation,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            wind_direction_10m = EXCLUDED.wind_direction_10m,
            surface_pressure = EXCLUDED.surface_pressure,
            cloud_cover = EXCLUDED.cloud_cover,
            visibility = EXCLUDED.visibility,
            raw_json = EXCLUDED.raw_json,
            source_file = EXCLUDED.source_file
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        conn.commit()
    return len(values)

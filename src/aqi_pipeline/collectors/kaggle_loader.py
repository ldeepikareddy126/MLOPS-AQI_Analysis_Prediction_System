"""Kaggle AQI dataset loader.

Supports manual CSV drops in data/external/kaggle. If Kaggle credentials are
present, the project can later add download automation without changing the raw
or staging contracts.
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import pandas as pd
import psycopg2.extras

from aqi_pipeline.landing.raw_files import land_file
from aqi_pipeline.quality.ops import json_dumps, log_ingestion
from aqi_pipeline.transform.units import canonical_pollutant
from aqi_pipeline.utils.db import get_raw_connection
from aqi_pipeline.utils.paths import DATA_DIR

KAGGLE_DIR = DATA_DIR / "external" / "kaggle"
KAGGLE_DATASET = "rohanrao/air-quality-data-in-india"
PLACEHOLDERS = {"", "your_kaggle_username_here", "your_kaggle_api_key_here"}


def collect(run_id: UUID | str) -> int:
    files = sorted(p for p in KAGGLE_DIR.glob("*.csv") if p.is_file())
    username = os.environ.get("KAGGLE_USERNAME", "").strip()
    key = os.environ.get("KAGGLE_KEY", "").strip()
    credentials_available = username not in PLACEHOLDERS and key not in PLACEHOLDERS

    if not files:
        if credentials_available:
            try:
                from kaggle.api.kaggle_api_extended import KaggleApi

                KAGGLE_DIR.mkdir(parents=True, exist_ok=True)
                api = KaggleApi()
                api.authenticate()
                api.dataset_download_files(
                    KAGGLE_DATASET,
                    path=str(KAGGLE_DIR),
                    unzip=True,
                    quiet=True,
                )
                files = sorted(KAGGLE_DIR.glob("*.csv"))
                if not files:
                    raise RuntimeError(
                        f"Kaggle dataset {KAGGLE_DATASET} downloaded without CSV files"
                    )
                return _ingest_files(run_id, files)
            except Exception as exc:
                log_ingestion(
                    run_id=run_id,
                    source_id="kaggle",
                    endpoint_or_file=KAGGLE_DATASET,
                    status="FAILED",
                    rows_extracted=0,
                    error_message=str(exc),
                )
                raise

        log_ingestion(
            run_id=run_id,
            source_id="kaggle",
            endpoint_or_file=str(KAGGLE_DIR),
            status="SKIPPED",
            rows_extracted=0,
            error_message="No Kaggle CSV files found in data/external/kaggle",
        )
        return 0

    return _ingest_files(run_id, files)


def _ingest_files(run_id: UUID | str, files: list[Path]) -> int:
    total = 0
    for path in files:
        try:
            landed, checksum = land_file("kaggle", str(run_id), path)
            df = pd.read_csv(path)
            rows = _insert(run_id, path, df)
            log_ingestion(
                run_id=run_id,
                source_id="kaggle",
                endpoint_or_file=str(path),
                status="SUCCESS",
                rows_extracted=rows,
                file_path=str(landed),
                file_checksum=checksum,
            )
            total += rows
        except Exception as exc:
            log_ingestion(
                run_id=run_id,
                source_id="kaggle",
                endpoint_or_file=str(path),
                status="FAILED",
                rows_extracted=0,
                error_message=str(exc),
            )
    return total


def _insert(run_id: UUID | str, path: Path, df: pd.DataFrame) -> int:
    dataset = path.stem.lower()
    values = []
    pollutant_columns = [c for c in df.columns if canonical_pollutant(c)]
    for idx, row in enumerate(df.to_dict("records"), start=1):
        if pollutant_columns:
            for pollutant_col in pollutant_columns:
                values.append(_value_tuple(run_id, dataset, path, idx, row, pollutant_col, row.get(pollutant_col)))
        else:
            values.append(_value_tuple(run_id, dataset, path, idx, row, row.get("Parameter"), row.get("Value")))
    if not values:
        return 0
    sql = """
        INSERT INTO raw.kaggle_measurements (
            run_id, dataset_name, source_file, row_number, city_raw, station_raw,
            date_raw, parameter_raw, value_raw, aqi_raw, aqi_bucket_raw, raw_json
        )
        VALUES %s
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.kaggle_measurements WHERE dataset_name = %s",
            (dataset,),
        )
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        conn.commit()
    return len(values)


def _value_tuple(run_id: UUID | str, dataset: str, path: Path, idx: int, row: dict, parameter: object, value: object) -> tuple:
    return (
        str(run_id),
        dataset,
        str(path),
        idx,
        row.get("City") or row.get("city"),
        row.get("StationId") or row.get("Station") or row.get("station"),
        row.get("Date") or row.get("date"),
        parameter,
        value,
        row.get("AQI") or row.get("aqi"),
        row.get("AQI_Bucket") or row.get("AQI Bucket") or row.get("aqi_bucket"),
        json_dumps(row),
    )

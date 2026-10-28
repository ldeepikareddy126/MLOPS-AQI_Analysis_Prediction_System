"""Manual CPCB CSV/Excel loader.

The CPCB portal may require forms/captchas, so this loader only reads files the
user has legally downloaded into data/external/cpcb.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pandas as pd
import psycopg2.extras

from aqi_pipeline.landing.raw_files import land_file
from aqi_pipeline.quality.ops import json_dumps, log_ingestion
from aqi_pipeline.utils.db import get_raw_connection
from aqi_pipeline.utils.paths import DATA_DIR

CPCB_DIR = DATA_DIR / "external" / "cpcb"


def _read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def _pick(row: dict, names: list[str]) -> object | None:
    normalised = {str(k).strip().lower(): v for k, v in row.items()}
    for name in names:
        if name.lower() in normalised:
            return normalised[name.lower()]
    return None


def collect(run_id: UUID | str) -> int:
    files = [
        p
        for p in CPCB_DIR.glob("*")
        if p.is_file() and p.suffix.lower() in {".csv", ".xlsx", ".xls"}
    ]
    if not files:
        log_ingestion(
            run_id=run_id,
            source_id="cpcb",
            endpoint_or_file=str(CPCB_DIR),
            status="SKIPPED",
            rows_extracted=0,
            error_message="No CPCB CSV/Excel files found in data/external/cpcb",
        )
        return 0

    total = 0
    for path in files:
        try:
            landed, checksum = land_file("cpcb", str(run_id), path)
            df = _read_file(path)
            rows = _insert(run_id, path, df)
            log_ingestion(
                run_id=run_id,
                source_id="cpcb",
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
                source_id="cpcb",
                endpoint_or_file=str(path),
                status="FAILED",
                rows_extracted=0,
                error_message=str(exc),
            )
    return total


def _insert(run_id: UUID | str, path: Path, df: pd.DataFrame) -> int:
    values = []
    for idx, record in enumerate(df.to_dict("records"), start=1):
        values.append(
            (
                str(run_id),
                str(path),
                idx,
                _pick(record, ["station", "station name", "monitoring station", "site"]),
                _pick(record, ["city", "city/town/village/area", "district"]),
                _pick(record, ["date", "from date", "sampling date"]),
                _pick(record, ["time", "from time", "timestamp"]),
                _pick(record, ["parameter", "pollutant", "pollutant id"]),
                _pick(record, ["value", "concentration", "avg", "average"]),
                _pick(record, ["unit", "units"]),
                json_dumps(record),
            )
        )
    if not values:
        return 0
    sql = """
        INSERT INTO raw.cpcb_measurements (
            run_id, source_file, row_number, station_name_raw, city_raw,
            date_raw, time_raw, parameter_raw, value_raw, unit_raw, raw_json
        )
        VALUES %s
    """
    with get_raw_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.cpcb_measurements WHERE source_file = %s",
            (str(path),),
        )
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        conn.commit()
    return len(values)

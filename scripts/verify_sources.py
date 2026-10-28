#!/usr/bin/env python3
"""Build the source coverage matrix for Stage A."""

from __future__ import annotations

import argparse
import os
from uuid import uuid4

from sqlalchemy import text

from aqi_pipeline.utils.db import get_engine


LAYERS = {
    "openaq": {
        "raw": "SELECT COUNT(*) row_count, COUNT(DISTINCT location_id) station_count, MIN(date_utc)::date date_min, MAX(date_utc)::date date_max FROM raw.openaq_measurements WHERE run_id=:run_id",
        "staging": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_id_canonical) station_count, MIN(date_utc) date_min, MAX(date_utc) date_max FROM staging.measurements WHERE run_id=:run_id AND source_system='openaq'",
        "clean": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_id_canonical) station_count, MIN(date_utc) date_min, MAX(date_utc) date_max FROM clean.measurements_hourly WHERE run_id=:run_id AND source_system='openaq'",
        "analytics": "SELECT COUNT(DISTINCT fact.id) row_count, COUNT(DISTINCT fact.station_id_canonical) station_count, MIN(fact.date_utc) date_min, MAX(fact.date_utc) date_max FROM analytics.fact_station_hourly AS fact JOIN clean.measurements_hourly AS measurement ON measurement.station_id_canonical=fact.station_id_canonical AND measurement.hour_utc=fact.hour_utc WHERE measurement.run_id=:run_id AND measurement.source_system='openaq' AND 'openaq'=ANY(fact.source_systems)",
    },
    "cpcb": {
        "raw": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_name_raw) station_count, NULL::date date_min, NULL::date date_max FROM raw.cpcb_measurements WHERE run_id=:run_id",
        "staging": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_id_canonical) station_count, MIN(date_utc) date_min, MAX(date_utc) date_max FROM staging.measurements WHERE run_id=:run_id AND source_system='cpcb'",
        "clean": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_id_canonical) station_count, MIN(date_utc) date_min, MAX(date_utc) date_max FROM clean.measurements_hourly WHERE run_id=:run_id AND source_system='cpcb'",
        "analytics": "SELECT COUNT(DISTINCT fact.id) row_count, COUNT(DISTINCT fact.station_id_canonical) station_count, MIN(fact.date_utc) date_min, MAX(fact.date_utc) date_max FROM analytics.fact_station_hourly AS fact JOIN clean.measurements_hourly AS measurement ON measurement.station_id_canonical=fact.station_id_canonical AND measurement.hour_utc=fact.hour_utc WHERE measurement.run_id=:run_id AND measurement.source_system='cpcb' AND 'cpcb'=ANY(fact.source_systems)",
    },
    "kaggle": {
        "raw": "SELECT COUNT(*) row_count, COUNT(DISTINCT COALESCE(station_raw, city_raw)) station_count, NULL::date date_min, NULL::date date_max FROM raw.kaggle_measurements WHERE run_id=:run_id",
        "staging": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_id_canonical) station_count, MIN(date_utc) date_min, MAX(date_utc) date_max FROM staging.measurements WHERE run_id=:run_id AND source_system='kaggle'",
        "clean": "SELECT COUNT(*) row_count, COUNT(DISTINCT station_id_canonical) station_count, MIN(date_utc) date_min, MAX(date_utc) date_max FROM clean.measurements_hourly WHERE run_id=:run_id AND source_system='kaggle'",
        "analytics": "SELECT COUNT(DISTINCT fact.id) row_count, COUNT(DISTINCT fact.station_id_canonical) station_count, MIN(fact.date_utc) date_min, MAX(fact.date_utc) date_max FROM analytics.fact_station_hourly AS fact JOIN clean.measurements_hourly AS measurement ON measurement.station_id_canonical=fact.station_id_canonical AND measurement.hour_utc=fact.hour_utc WHERE measurement.run_id=:run_id AND measurement.source_system='kaggle' AND 'kaggle'=ANY(fact.source_systems)",
    },
    "open_meteo": {
        "raw": "SELECT COUNT(*) row_count, COUNT(DISTINCT city_id) station_count, MIN(timestamp_utc)::date date_min, MAX(timestamp_utc)::date date_max FROM raw.open_meteo_weather WHERE run_id=:run_id",
        "staging": "SELECT COUNT(*) row_count, COUNT(DISTINCT city_id) station_count, MIN(timestamp_utc)::date date_min, MAX(timestamp_utc)::date date_max FROM staging.weather WHERE run_id=:run_id",
        "clean": "SELECT COUNT(*) row_count, COUNT(DISTINCT clean.city_id) station_count, MIN(clean.date_utc) date_min, MAX(clean.date_utc) date_max FROM clean.weather_hourly AS clean JOIN staging.weather AS staged ON staged.city_id=clean.city_id AND staged.timestamp_utc=clean.hour_utc WHERE staged.run_id=:run_id",
        "analytics": "SELECT COUNT(DISTINCT fact.id) row_count, COUNT(DISTINCT fact.city_id) station_count, MIN(fact.date_utc) date_min, MAX(fact.date_utc) date_max FROM analytics.fact_station_hourly AS fact WHERE fact.has_weather AND EXISTS (SELECT 1 FROM staging.weather AS staged WHERE staged.run_id=:run_id AND staged.city_id=fact.city_id AND staged.timestamp_utc=fact.hour_utc)",
    },
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=os.environ.get("PIPELINE_RUN_ID") or str(uuid4()))
    parser.add_argument("--strict", action="store_true", help="fail if required downstream coverage is zero")
    args = parser.parse_args()

    engine = get_engine()
    failures: list[str] = []
    rows: list[dict] = []
    with engine.begin() as conn:
        required = {
            r.source_id: bool(r.required)
            for r in conn.execute(text("SELECT source_id, required FROM ops.source_registry")).fetchall()
        }
        for source, layer_queries in LAYERS.items():
            latest_status = conn.execute(
                text(
                    """
                    SELECT status FROM ops.ingestion_log
                    WHERE source_id = :source AND run_id = :run_id
                    ORDER BY extraction_ts DESC
                    LIMIT 1
                    """
                ),
                {"source": source, "run_id": args.run_id},
            ).scalar()
            for layer, query in layer_queries.items():
                record = conn.execute(
                    text(query), {"run_id": args.run_id}
                ).mappings().one()
                row_count = int(record["row_count"] or 0)
                status = "OK" if row_count else "EMPTY"
                if row_count == 0 and latest_status == "SKIPPED":
                    status = "NOT_PROVIDED"
                note = None
                if (
                    args.strict
                    and required.get(source, False)
                    and layer in {"staging", "clean", "analytics"}
                    and row_count == 0
                ):
                    failures.append(f"{source}:{layer} has zero required rows")
                    status = "FAILED"
                    note = "Required source has no downstream rows"
                conn.execute(
                    text(
                        """
                        INSERT INTO ops.source_coverage (
                            run_id, source_id, layer, row_count, station_count,
                            date_min, date_max, status, notes
                        )
                        VALUES (:run_id, :source_id, :layer, :row_count, :station_count,
                                :date_min, :date_max, :status, :notes)
                        ON CONFLICT (run_id, source_id, layer) DO UPDATE SET
                            row_count = EXCLUDED.row_count,
                            station_count = EXCLUDED.station_count,
                            date_min = EXCLUDED.date_min,
                            date_max = EXCLUDED.date_max,
                            status = EXCLUDED.status,
                            notes = EXCLUDED.notes,
                            checked_at = NOW()
                        """
                    ),
                    {
                        "run_id": args.run_id,
                        "source_id": source,
                        "layer": layer,
                        "row_count": row_count,
                        "station_count": record["station_count"],
                        "date_min": record["date_min"],
                        "date_max": record["date_max"],
                        "status": status,
                        "notes": note,
                    },
                )
                rows.append({"source": source, "layer": layer, "rows": row_count, "status": status})

    print("Source coverage matrix")
    for row in rows:
        print(f"{row['source']:<11} {row['layer']:<9} {row['rows']:>10} {row['status']}")
    if failures:
        print("\nFailures:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

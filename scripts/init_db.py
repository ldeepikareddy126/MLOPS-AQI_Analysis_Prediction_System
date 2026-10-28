#!/usr/bin/env python3
"""
scripts/init_db.py
==================
Database initialisation script — runs on Airflow init service startup.

Responsibilities:
1. Wait for PostgreSQL to be ready.
2. Create all schemas (01_schemas.sql).
3. Create all tables in correct dependency order (02–06).
4. Populate dim_date, dim_city, dim_pollutant seed data.
5. Idempotent: safe to re-run; uses CREATE IF NOT EXISTS throughout.
"""

import os
import sys
import time
import logging
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [init_db] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent.parent / "sql" / "ddl"
CONFIG_DIR = Path(__file__).parent.parent / "config"

# DDL files in execution order
DDL_FILES = [
    "01_schemas.sql",
    "02_ops_tables.sql",
    "03_raw_tables.sql",
    "04_staging_tables.sql",
    "05_clean_tables.sql",
    "06_analytics_tables.sql",
    "07_monitoring_tables.sql",
]


def get_connection_params() -> dict:
    return {
        "host":     os.environ["POSTGRES_HOST"],
        "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "dbname":   os.environ["AQI_DB_NAME"],
    }


def wait_for_postgres(params: dict, max_retries: int = 30, delay: int = 5) -> None:
    """Retry until PostgreSQL is accepting connections."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            log.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError as exc:
            log.warning(f"Attempt {attempt}/{max_retries}: DB not ready — {exc}")
            time.sleep(delay)
    raise RuntimeError("PostgreSQL did not become ready in time.")


def run_ddl_file(conn, filepath: Path) -> None:
    """Execute a SQL DDL file inside a single transaction."""
    log.info(f"Running DDL: {filepath.name}")
    sql = filepath.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info(f"  ✅  {filepath.name} complete")


def seed_dim_date(conn) -> None:
    """Populate analytics.dim_date from 2015-01-01 to today+365 days."""
    log.info("Seeding analytics.dim_date ...")
    sql = """
    INSERT INTO analytics.dim_date (
        date_id, year, month, day, day_of_week, week_of_year,
        quarter, is_weekend, season, is_stubble_season, day_name, month_name
    )
    SELECT
        d::DATE                                          AS date_id,
        EXTRACT(YEAR  FROM d)::SMALLINT                 AS year,
        EXTRACT(MONTH FROM d)::SMALLINT                 AS month,
        EXTRACT(DAY   FROM d)::SMALLINT                 AS day,
        EXTRACT(ISODOW FROM d)::SMALLINT - 1            AS day_of_week,  -- 0=Mon
        EXTRACT(WEEK  FROM d)::SMALLINT                 AS week_of_year,
        EXTRACT(QUARTER FROM d)::SMALLINT               AS quarter,
        EXTRACT(ISODOW FROM d) IN (6, 7)                AS is_weekend,
        CASE
            WHEN EXTRACT(MONTH FROM d) IN (12, 1, 2)  THEN 'Winter'
            WHEN EXTRACT(MONTH FROM d) IN (3, 4, 5)   THEN 'Summer'
            WHEN EXTRACT(MONTH FROM d) IN (6, 7, 8, 9) THEN 'Monsoon'
            ELSE 'Post-Monsoon'
        END                                             AS season,
        -- Stubble-burning season: Oct–Nov in Punjab/Haryana
        EXTRACT(MONTH FROM d) IN (10, 11)               AS is_stubble_season,
        TO_CHAR(d, 'Day')                               AS day_name,
        TO_CHAR(d, 'Month')                             AS month_name
    FROM generate_series(
        '2015-01-01'::DATE,
        (CURRENT_DATE + INTERVAL '365 days')::DATE,
        '1 day'::INTERVAL
    ) AS t(d)
    ON CONFLICT (date_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("  ✅  dim_date seeded")


def seed_dim_city(conn) -> None:
    """Populate analytics.dim_city from cities.yaml."""
    import yaml
    cities_file = CONFIG_DIR / "cities.yaml"
    if not cities_file.exists():
        log.warning("cities.yaml not found; skipping dim_city seed")
        return
    with open(cities_file) as f:
        cfg = yaml.safe_load(f)
    cities = cfg.get("cities", [])
    sql = """
    INSERT INTO analytics.dim_city
        (city_id, city_name, state, country, lat, lon, is_primary, timezone)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (city_id) DO UPDATE SET
        city_name  = EXCLUDED.city_name,
        lat        = EXCLUDED.lat,
        lon        = EXCLUDED.lon,
        is_primary = EXCLUDED.is_primary;
    """
    with conn.cursor() as cur:
        for c in cities:
            cur.execute(sql, (
                c["id"], c["name"], c.get("state"), c.get("country", "IN"),
                c.get("lat"), c.get("lon"), c.get("primary", False),
                c.get("timezone", "Asia/Kolkata"),
            ))
    conn.commit()
    log.info(f"  ✅  dim_city seeded ({len(cities)} cities)")


def seed_dim_pollutant(conn) -> None:
    """Populate analytics.dim_pollutant from pollutants.yaml."""
    import yaml
    poll_file = CONFIG_DIR / "pollutants.yaml"
    if not poll_file.exists():
        log.warning("pollutants.yaml not found; skipping dim_pollutant seed")
        return
    with open(poll_file) as f:
        cfg = yaml.safe_load(f)
    pollutants = cfg.get("pollutants", {})
    aqi_codes = set(cfg.get("aqi_pollutants", []))
    sql = """
    INSERT INTO analytics.dim_pollutant
        (pollutant_code, display_name, standard_unit, aqi_averaging,
         is_aqi_component, physical_min, physical_max)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (pollutant_code) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        standard_unit = EXCLUDED.standard_unit;
    """
    with conn.cursor() as cur:
        for code, p in pollutants.items():
            cur.execute(sql, (
                code,
                p["display_name"],
                p["standard_unit"],
                p.get("aqi_averaging"),
                code in aqi_codes,
                p.get("physical_min"),
                p.get("physical_max"),
            ))
    conn.commit()
    log.info(f"  ✅  dim_pollutant seeded ({len(pollutants)} pollutants)")


def main() -> None:
    params = get_connection_params()
    wait_for_postgres(params)

    conn = psycopg2.connect(**params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        # Run DDL files
        for fname in DDL_FILES:
            fpath = SQL_DIR / fname
            if fpath.exists():
                # Need a fresh autocommit connection for DDL with GENERATED ALWAYS
                run_ddl_file(conn, fpath)
            else:
                log.warning(f"DDL file not found (skipped): {fpath}")

        # Seed reference data
        conn.set_isolation_level(0)  # back to default transaction
        seed_dim_date(conn)
        seed_dim_city(conn)
        seed_dim_pollutant(conn)

        log.info("🎉  Database initialisation complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

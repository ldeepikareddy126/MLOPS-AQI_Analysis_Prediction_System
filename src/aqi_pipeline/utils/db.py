"""
src/aqi_pipeline/utils/db.py
=============================
Database connection and query utilities.

Design decisions:
- Uses SQLAlchemy 1.4.x engine (required by Airflow 2.x; NOT 2.x).
- Connection params come exclusively from environment variables.
- Provides a context manager for safe session handling.
- Upsert helper (INSERT … ON CONFLICT DO UPDATE) for idempotent loads.
"""

import os
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)

# Cache engines by DSN to avoid re-creating on every call
_engines: dict[str, Engine] = {}


def get_dsn(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    dbname: Optional[str] = None,
) -> str:
    """Build a PostgreSQL DSN from explicit params or environment variables."""
    host     = host     or os.environ["POSTGRES_HOST"]
    port     = port     or int(os.environ.get("POSTGRES_PORT", 5432))
    user     = user     or os.environ["POSTGRES_USER"]
    password = password or os.environ["POSTGRES_PASSWORD"]
    dbname   = dbname   or os.environ["AQI_DB_NAME"]
    return URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    ).render_as_string(hide_password=False)


def get_engine(dsn: Optional[str] = None, **kwargs) -> Engine:
    """
    Return a cached SQLAlchemy engine for the given DSN.

    Keyword args are passed to create_engine (e.g. pool_size, pool_timeout).
    """
    dsn = dsn or get_dsn()
    if dsn not in _engines:
        engine = create_engine(
            dsn,
            pool_pre_ping=True,          # validate connection before use
            pool_size=kwargs.get("pool_size", 5),
            max_overflow=kwargs.get("max_overflow", 10),
            pool_timeout=kwargs.get("pool_timeout", 30),
            pool_recycle=kwargs.get("pool_recycle", 1800),
        )
        _engines[dsn] = engine
        log.info(f"Created SQLAlchemy engine for {dsn.split('@')[-1]}")
    return _engines[dsn]


@contextmanager
def get_session(dsn: Optional[str] = None) -> Generator[Session, None, None]:
    """
    Context manager that yields a SQLAlchemy Session.

    Usage:
        with get_session() as session:
            session.execute(text("SELECT 1"))
    """
    engine = get_engine(dsn)
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_raw_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    dbname: Optional[str] = None,
    dsn: Optional[str] = None,
):
    """Return a raw psycopg2 connection (for bulk COPY operations)."""
    if dsn:
        url = make_url(dsn)
        return psycopg2.connect(
            host=url.host,
            port=url.port or 5432,
            user=url.username,
            password=url.password,
            dbname=url.database,
        )
    return psycopg2.connect(
        host     = host     or os.environ["POSTGRES_HOST"],
        port     = port     or int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = user     or os.environ["POSTGRES_USER"],
        password = password or os.environ["POSTGRES_PASSWORD"],
        dbname   = dbname   or os.environ["AQI_DB_NAME"],
    )


def execute_sql_file(filepath: str, dsn: Optional[str] = None) -> None:
    """Execute a SQL file (DDL or DML) against the warehouse."""
    from pathlib import Path
    sql = Path(filepath).read_text(encoding="utf-8")
    engine = get_engine(dsn)
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    log.info(f"Executed SQL file: {filepath}")


def bulk_upsert(
    table: str,
    rows: list[dict],
    conflict_cols: list[str],
    update_cols: list[str],
    dsn: Optional[str] = None,
) -> int:
    """
    Bulk upsert rows into a table using INSERT … ON CONFLICT DO UPDATE.

    Args:
        table:          Fully qualified table name (e.g. 'raw.openaq_measurements').
        rows:           List of dicts; keys must match column names.
        conflict_cols:  Columns forming the unique constraint (conflict target).
        update_cols:    Columns to update on conflict (excluding conflict_cols).
        dsn:            Optional DSN override.

    Returns:
        Number of rows processed.

    Design: uses psycopg2 execute_values for efficient batch inserts.
    """
    if not rows:
        return 0

    columns      = list(rows[0].keys())
    col_list     = ", ".join(columns)
    placeholder  = ", ".join(["%s"] * len(columns))
    conflict_str = ", ".join(conflict_cols)
    update_str   = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES %s "
        f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
    )

    values = [tuple(row[c] for c in columns) for row in rows]
    conn = get_raw_connection(dsn=dsn) if dsn else get_raw_connection()

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values, page_size=500)
        conn.commit()
        log.debug(f"Upserted {len(rows)} rows into {table}")
        return len(rows)
    except Exception as exc:
        conn.rollback()
        log.error(f"Upsert failed for {table}: {exc}")
        raise
    finally:
        conn.close()

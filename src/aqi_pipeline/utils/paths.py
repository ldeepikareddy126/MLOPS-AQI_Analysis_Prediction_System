"""Path discovery that works both locally and inside Docker containers."""

from __future__ import annotations

import os
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
LOCAL_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


PROJECT_ROOT = Path(os.environ.get("AQI_PROJECT_ROOT", LOCAL_PROJECT_ROOT))
CONFIG_DIR = first_existing(
    Path(os.environ.get("AQI_CONFIG_DIR", "")) if os.environ.get("AQI_CONFIG_DIR") else PROJECT_ROOT / "config",
    Path("/opt/airflow/config"),       # Airflow containers
    Path("/app/config"),               # FastAPI container (Dockerfile.api copies here)
    LOCAL_PROJECT_ROOT / "config",
)
DATA_DIR = first_existing(
    Path(os.environ.get("AQI_DATA_DIR", "")) if os.environ.get("AQI_DATA_DIR") else PROJECT_ROOT / "data",
    Path("/opt/airflow/data"),
    LOCAL_PROJECT_ROOT / "data",
)
SQL_DIR = first_existing(
    Path(os.environ.get("AQI_SQL_DIR", "")) if os.environ.get("AQI_SQL_DIR") else PROJECT_ROOT / "sql",
    Path("/opt/airflow/sql"),
    LOCAL_PROJECT_ROOT / "sql",
)

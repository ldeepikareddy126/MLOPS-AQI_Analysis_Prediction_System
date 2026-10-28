"""
tests/conftest.py
==================
Shared pytest fixtures for AQI MLOps test suite (Stage A + Stage B).

Fixtures here are automatically discovered by pytest for both
tests/unit/ and tests/integration/ sub-packages.
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Environment guards
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests that require a live PostgreSQL connection"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests that are slow (e.g. full pipeline runs)"
    )


# ---------------------------------------------------------------------------
# Minimal DB env vars (in-memory / mock unless integration markers present)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def set_test_env():
    """
    Inject minimal environment variables for unit tests so that
    imports of aqi_pipeline.utils.db don't fail immediately.

    Integration tests override these by running against a live Postgres.
    """
    defaults = {
        "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5439"),
        "POSTGRES_USER": os.environ.get("POSTGRES_USER", "aqi_user"),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "changeme_strong_password"),
        "AQI_DB_NAME": os.environ.get("AQI_DB_NAME", "aqi_warehouse"),
        "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"),
        "AQI_CONFIG_DIR": os.environ.get(
            "AQI_CONFIG_DIR", str(Path(__file__).resolve().parent / "config")
        ),
    }
    for k, v in defaults.items():
        os.environ.setdefault(k, v)
    yield


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_openaq_record():
    """A single valid OpenAQ v3 measurement record."""
    return {
        "locationId": 123456,
        "location": "Delhi - IGI Airport T3",
        "city": "Delhi",
        "country": "IN",
        "coordinates": {"latitude": 28.5665, "longitude": 77.1031},
        "parameter": "pm25",
        "value": 87.4,
        "unit": "µg/m³",
        "date": {"utc": "2024-01-15T12:00:00Z", "local": "2024-01-15T17:30:00+05:30"},
        "isMobile": False,
        "isAnalysis": False,
    }


@pytest.fixture
def sample_cpcb_row():
    """A single CPCB-style measurement dict (post-normalisation)."""
    return {
        "station_id": "delhi_igi",
        "city_id": "delhi",
        "datetime_utc": "2024-01-15 12:00:00",
        "pm25": 87.4,
        "pm10": 145.2,
        "no2": 32.1,
        "so2": 8.5,
        "co": 1.2,
        "o3": 45.0,
        "aqi": 142,
        "aqi_category": "Moderate",
    }


@pytest.fixture
def sample_feature_df():
    """Small deterministic feature dataset shared by Stage B training tests."""
    np.random.seed(42)
    n = 60
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "city_id": "delhi",
            "date_utc": dates,
            "aqi": np.random.uniform(50, 300, n),
            "aqi_lag_1d": np.random.uniform(50, 300, n),
            "aqi_lag_2d": np.random.uniform(50, 300, n),
            "aqi_lag_3d": np.random.uniform(50, 300, n),
            "aqi_lag_7d": np.random.uniform(50, 300, n),
            "aqi_lag_14d": np.random.uniform(50, 300, n),
            "aqi_roll_mean_3d": np.random.uniform(50, 300, n),
            "aqi_roll_std_3d": np.random.uniform(5, 50, n),
            "aqi_roll_mean_7d": np.random.uniform(50, 300, n),
            "aqi_roll_std_7d": np.random.uniform(5, 50, n),
            "temperature_2m": np.random.uniform(5, 40, n),
            "relative_humidity_2m": np.random.uniform(20, 90, n),
            "precipitation": np.random.uniform(0, 10, n),
            "wind_speed_10m": np.random.uniform(0, 20, n),
            "wind_direction_10m": np.random.uniform(0, 360, n),
            "surface_pressure": np.random.uniform(990, 1020, n),
            "cloud_cover": np.random.uniform(0, 100, n),
            "day_of_week": np.arange(n) % 7,
            "month": (np.arange(n) % 12) + 1,
            "is_weekend": ((np.arange(n) % 7) >= 5).astype(int),
            "day_of_year": (np.arange(n) % 365) + 1,
            "week_of_year": (np.arange(n) // 7) + 1,
            "season_code": np.random.choice([0, 1, 2, 3], n),
            "is_stubble_season": np.random.choice([0, 1], n),
            "target_aqi": np.random.uniform(50, 300, n),
            "target_category": np.random.choice(
                ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"], n
            ),
            "aqi_category": np.random.choice(
                ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"], n
            ),
        }
    )

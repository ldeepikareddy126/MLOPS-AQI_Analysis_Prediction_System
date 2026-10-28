"""Feature engineering and store modules for AQI prediction."""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import text

from aqi_pipeline.utils.config import load_cities
from aqi_pipeline.utils.db import get_engine
from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)

FEATURE_VERSION = "v1"

# Lag window sizes (days) used as features
LAG_DAYS = [1, 2, 3, 7, 14]
ROLLING_WINDOWS = [3, 7]  # days for rolling mean/std

# Pollutant columns available in mart_station_daily_aqi
POLLUTANT_COLS = ["pm25_24h", "pm10_24h", "no2_24h", "so2_24h", "co_8h_max", "o3_8h_max", "nh3_24h"]

# Weather columns available in clean.weather_hourly
WEATHER_COLS = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m",
                "wind_direction_10m", "surface_pressure", "cloud_cover"]


def build_features_for_city(city_id: str, window_days: int = 90) -> pd.DataFrame:
    """
    Build a feature DataFrame for a single city.

    Features include:
    - Lag features for AQI and pollutants (t-1, t-2, t-3, t-7, t-14)
    - Rolling statistics (3-day, 7-day mean/std)
    - Daily weather averages
    - Calendar features (day_of_week, month, is_weekend, season)
    - Target: next-day AQI (t+1)

    Returns one row per date with all features + target_aqi + target_category.
    """
    engine = get_engine()
    end_date = date.today()
    start_date = end_date - timedelta(days=window_days + max(LAG_DAYS) + 5)

    aqi_sql = """
        SELECT city.date_utc,
               city.aqi_mean AS aqi,
               city.aqi_category,
               city.aqi_max,
               city.aqi_min,
               city.dominant_pollutant,
               city.station_count,
               AVG(station.pm25_24h) AS pm25_24h,
               AVG(station.pm10_24h) AS pm10_24h,
               AVG(station.no2_24h) AS no2_24h,
               AVG(station.so2_24h) AS so2_24h,
               AVG(station.co_8h_max) AS co_8h_max,
               AVG(station.o3_8h_max) AS o3_8h_max,
               AVG(station.nh3_24h) AS nh3_24h
        FROM analytics.mart_city_daily_aqi AS city
        LEFT JOIN analytics.mart_station_daily_aqi AS station
          ON station.city_id = city.city_id
         AND station.date_utc = city.date_utc
        WHERE city.city_id = :city_id
          AND city.date_utc BETWEEN :start_date AND :end_date
        GROUP BY city.date_utc, city.aqi_mean, city.aqi_category, city.aqi_max,
                 city.aqi_min, city.dominant_pollutant, city.station_count
        ORDER BY city.date_utc
    """
    weather_sql = """
        SELECT date_utc,
               AVG(temperature_2m)       AS temperature_2m,
               AVG(relative_humidity_2m) AS relative_humidity_2m,
               SUM(precipitation)        AS precipitation,
               AVG(wind_speed_10m)       AS wind_speed_10m,
               AVG(wind_direction_10m)   AS wind_direction_10m,
               AVG(surface_pressure)     AS surface_pressure,
               AVG(cloud_cover)          AS cloud_cover
        FROM clean.weather_hourly
        WHERE city_id = :city_id
          AND date_utc BETWEEN :start_date AND :end_date
        GROUP BY date_utc
        ORDER BY date_utc
    """
    params = {"city_id": city_id, "start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        aqi_df = pd.read_sql(text(aqi_sql), conn, params=params)
        weather_df = pd.read_sql(text(weather_sql), conn, params=params)

    if aqi_df.empty:
        log.warning("No AQI data found for city %s; skipping feature build", city_id)
        return pd.DataFrame()

    df = aqi_df

    df["date_utc"] = pd.to_datetime(df["date_utc"])
    df = df.sort_values("date_utc").set_index("date_utc")
    df = df.reindex(
        pd.date_range(start=start_date, end=end_date, freq="D", name="date_utc")
    )

    # Merge weather
    if not weather_df.empty:
        weather_df["date_utc"] = pd.to_datetime(weather_df["date_utc"])
        weather_df = weather_df.set_index("date_utc")
        df = df.join(weather_df, how="left")

    # --- Lag features ---
    for lag in LAG_DAYS:
        df[f"aqi_lag_{lag}d"] = df["aqi"].shift(lag)

    # --- Rolling statistics ---
    for win in ROLLING_WINDOWS:
        df[f"aqi_roll_mean_{win}d"] = df["aqi"].shift(1).rolling(win).mean()
        df[f"aqi_roll_std_{win}d"] = df["aqi"].shift(1).rolling(win).std()

    # --- Calendar features ---
    idx = df.index  # DatetimeIndex
    df["day_of_week"] = idx.dayofweek
    df["month"] = idx.month
    df["is_weekend"] = (idx.dayofweek >= 5).astype(int)
    df["day_of_year"] = idx.dayofyear
    df["week_of_year"] = idx.isocalendar().week.astype(int)
    df["season_code"] = df["month"].map(
        {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2, 9: 2, 10: 3, 11: 3}
    )
    df["is_stubble_season"] = (df["month"].isin([10, 11])).astype(int)

    # --- Target (next-day AQI) ---
    df = _add_next_day_targets(df)

    df["city_id"] = city_id

    return df.reset_index()


def _add_next_day_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Label each calendar date with the immediately following day's observed AQI."""
    result = df.copy()
    result["target_aqi"] = result["aqi"].shift(-1)
    result["target_category"] = result["aqi_category"].shift(-1)
    return result


def build_all_city_features(window_days: int = 365) -> pd.DataFrame:
    """Build and concatenate feature DataFrames for all configured cities."""
    frames = []
    for city in load_cities():
        df = build_features_for_city(city["id"], window_days=window_days)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def store_features(df: pd.DataFrame, feature_version: str = FEATURE_VERSION) -> int:
    """
    Persist a feature DataFrame to analytics.features_daily.

    Returns the number of rows written.
    """
    if df.empty:
        log.warning("No features to store")
        return 0

    engine = get_engine()
    required = {"city_id", "date_utc", "target_aqi"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Feature DataFrame missing columns: {missing}")

    rows = []
    for _, row in df.iterrows():
        feature_cols = [c for c in df.columns if c not in
                        {"city_id", "date_utc", "target_aqi", "target_category", "aqi_category"}]
        features_dict: dict[str, Any] = {}
        for c in feature_cols:
            val = row.get(c)
            if pd.isna(val):
                features_dict[c] = None
            elif hasattr(val, "item"):
                features_dict[c] = val.item()
            else:
                features_dict[c] = val

        rows.append(
            {
                "city_id": row["city_id"],
                "date_utc": row["date_utc"].date() if hasattr(row["date_utc"], "date") else row["date_utc"],
                "feature_version": feature_version,
                "features_json": json.dumps(features_dict, default=str),
                "target_aqi": None if pd.isna(row["target_aqi"]) else float(row["target_aqi"]),
                "target_category": row.get("target_category"),
                "data_window_start": df["date_utc"].min().date() if hasattr(df["date_utc"].min(), "date") else df["date_utc"].min(),
                "data_window_end": df["date_utc"].max().date() if hasattr(df["date_utc"].max(), "date") else df["date_utc"].max(),
            }
        )

    sql = text("""
        INSERT INTO analytics.features_daily
            (city_id, date_utc, feature_version, features_json,
             target_aqi, target_category, data_window_start, data_window_end)
        VALUES
            (:city_id, :date_utc, :feature_version, CAST(:features_json AS JSONB),
             :target_aqi, :target_category, :data_window_start, :data_window_end)
        ON CONFLICT (city_id, date_utc, feature_version) DO UPDATE SET
            features_json      = EXCLUDED.features_json,
            target_aqi         = EXCLUDED.target_aqi,
            target_category    = EXCLUDED.target_category,
            data_window_start  = EXCLUDED.data_window_start,
            data_window_end    = EXCLUDED.data_window_end,
            built_at           = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    log.info("Stored %d feature rows (version=%s)", len(rows), feature_version)
    return len(rows)


def load_features(
    feature_version: str = FEATURE_VERSION,
    min_date: str | None = None,
    max_date: str | None = None,
    city_id: str | None = None,
) -> pd.DataFrame:
    """
    Load features from analytics.features_daily and expand features_json columns.
    """
    engine = get_engine()
    conditions = ["feature_version = :version"]
    params: dict[str, Any] = {"version": feature_version}

    if min_date:
        conditions.append("date_utc >= :min_date")
        params["min_date"] = min_date
    if max_date:
        conditions.append("date_utc <= :max_date")
        params["max_date"] = max_date
    if city_id:
        conditions.append("city_id = :city_id")
        params["city_id"] = city_id

    where = " AND ".join(conditions)
    sql = text(f"""
        SELECT city_id, date_utc, feature_version, features_json,
               target_aqi, target_category
        FROM analytics.features_daily
        WHERE {where}
        ORDER BY city_id, date_utc
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df

    # Expand features_json
    expanded = pd.json_normalize(df["features_json"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else x
    ))
    result = pd.concat(
        [df[["city_id", "date_utc", "target_aqi", "target_category"]].reset_index(drop=True),
         expanded.reset_index(drop=True)],
        axis=1,
    )
    return result


def get_feature_columns() -> list[str]:
    """Return the list of feature column names (excluding meta/target columns)."""
    engine = get_engine()
    sql = text("""
        SELECT features_json
        FROM analytics.features_daily
        ORDER BY built_at DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()

    if not row:
        return []
    data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    return [k for k in data.keys()]

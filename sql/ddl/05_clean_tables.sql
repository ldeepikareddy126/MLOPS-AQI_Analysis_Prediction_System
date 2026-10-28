-- =============================================================================
-- 05_clean_tables.sql — Clean layer (validated, deduped, weather-joined)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- clean.measurements_hourly — validated, deduped, hourly granularity
-- One row per (station_id_canonical, pollutant_code, hour_utc)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.measurements_hourly (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    station_id_canonical TEXT        NOT NULL,
    city_id             VARCHAR(50)  NOT NULL,
    pollutant_code      VARCHAR(20)  NOT NULL,
    hour_utc            TIMESTAMPTZ  NOT NULL,
    date_utc            DATE         NOT NULL,
    hour_of_day         SMALLINT     NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    value               DOUBLE PRECISION NOT NULL,
    unit                VARCHAR(20)  NOT NULL,
    source_system       VARCHAR(50)  NOT NULL,    -- winning source after priority dedup
    source_priority     SMALLINT,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (station_id_canonical, pollutant_code, hour_utc)    -- idempotent upsert
);

-- Partitioning-friendly composite index
CREATE INDEX IF NOT EXISTS idx_clean_hourly_station_ts ON clean.measurements_hourly
    (station_id_canonical, hour_utc DESC);
CREATE INDEX IF NOT EXISTS idx_clean_hourly_city_ts    ON clean.measurements_hourly
    (city_id, hour_utc DESC);
CREATE INDEX IF NOT EXISTS idx_clean_hourly_date       ON clean.measurements_hourly(date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_clean_hourly_pollutant  ON clean.measurements_hourly(pollutant_code);

COMMENT ON TABLE clean.measurements_hourly IS
    'Validated, deduped hourly measurements; one row per station+pollutant+hour; '
    'source_system = winner after cross-source priority dedup';

-- ---------------------------------------------------------------------------
-- clean.weather_hourly — validated weather joined to city level
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.weather_hourly (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    hour_utc            TIMESTAMPTZ  NOT NULL,
    date_utc            DATE         NOT NULL,
    temperature_2m      DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    precipitation       DOUBLE PRECISION,
    wind_speed_10m      DOUBLE PRECISION,
    wind_direction_10m  DOUBLE PRECISION,
    surface_pressure    DOUBLE PRECISION,
    cloud_cover         DOUBLE PRECISION,
    visibility          DOUBLE PRECISION,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, hour_utc)
);

CREATE INDEX IF NOT EXISTS idx_clean_weather_city_ts ON clean.weather_hourly(city_id, hour_utc DESC);

COMMENT ON TABLE clean.weather_hourly IS 'Validated Open-Meteo hourly weather at city level';

-- ---------------------------------------------------------------------------
-- clean.station_hourly_completeness — gap tracking per station+pollutant
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.station_hourly_completeness (
    id                  BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL,
    city_id             VARCHAR(50),
    pollutant_code      VARCHAR(20)  NOT NULL,
    period_start        DATE         NOT NULL,
    period_end          DATE         NOT NULL,
    expected_hours      INTEGER      NOT NULL,
    present_hours       INTEGER      NOT NULL DEFAULT 0,
    missing_hours       INTEGER      GENERATED ALWAYS AS (expected_hours - present_hours) STORED,
    completeness_pct    NUMERIC(5,2) GENERATED ALWAYS AS (
                            CASE WHEN expected_hours > 0
                                 THEN 100.0 * present_hours / expected_hours
                                 ELSE 0.0 END
                        ) STORED,
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (station_id_canonical, pollutant_code, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS idx_completeness_station ON clean.station_hourly_completeness(station_id_canonical);
CREATE INDEX IF NOT EXISTS idx_completeness_pct     ON clean.station_hourly_completeness(completeness_pct);

COMMENT ON TABLE clean.station_hourly_completeness IS
    'Data completeness and gap analysis per station × pollutant × period';

-- ---------------------------------------------------------------------------
-- clean.daily_averages — pre-computed daily aggregations for AQI
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.daily_averages (
    id                  BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL,
    city_id             VARCHAR(50)  NOT NULL,
    pollutant_code      VARCHAR(20)  NOT NULL,
    date_utc            DATE         NOT NULL,
    -- 24-hour mean (PM2.5, PM10, NO2, SO2, NH3, Pb)
    daily_mean          DOUBLE PRECISION,
    -- 8-hour rolling maximum (CO, O3)
    eight_hour_max      DOUBLE PRECISION,
    -- AQI sub-index for this pollutant on this date
    sub_index           DOUBLE PRECISION,
    -- Number of valid hourly observations used
    obs_count           SMALLINT,
    unit                VARCHAR(20),
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (station_id_canonical, pollutant_code, date_utc)
);

CREATE INDEX IF NOT EXISTS idx_daily_avg_station_date ON clean.daily_averages
    (station_id_canonical, date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_daily_avg_city_date    ON clean.daily_averages(city_id, date_utc DESC);

COMMENT ON TABLE clean.daily_averages IS
    'Daily pollutant averages and AQI sub-indices computed per CPCB averaging rules';

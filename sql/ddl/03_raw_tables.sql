-- =============================================================================
-- 03_raw_tables.sql — Raw landing zone tables
-- One row per API response record / CSV row before any transformation.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- raw.openaq_measurements — raw OpenAQ v3 measurement records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.openaq_measurements (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    location_id         BIGINT,
    location_name       TEXT,
    sensor_id           BIGINT,
    parameter           TEXT,
    value               DOUBLE PRECISION,
    unit                TEXT,
    date_utc            TIMESTAMPTZ,
    date_local          TEXT,
    coordinates_lat     DOUBLE PRECISION,
    coordinates_lon     DOUBLE PRECISION,
    country             TEXT,
    city                TEXT,
    entity              TEXT,
    sensor_type         TEXT,
    raw_json            JSONB        NOT NULL,    -- full original API record
    source_file         TEXT,
    UNIQUE (sensor_id, parameter, date_utc)       -- idempotent upsert key
);

CREATE INDEX IF NOT EXISTS idx_raw_openaq_date    ON raw.openaq_measurements(date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_raw_openaq_loc     ON raw.openaq_measurements(location_id);
CREATE INDEX IF NOT EXISTS idx_raw_openaq_run     ON raw.openaq_measurements(run_id);

COMMENT ON TABLE raw.openaq_measurements IS 'Raw OpenAQ v3 API measurement records (unmodified)';

-- ---------------------------------------------------------------------------
-- raw.openaq_locations — OpenAQ location/station metadata
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.openaq_locations (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    location_id         BIGINT       NOT NULL,
    location_name       TEXT,
    city                TEXT,
    country             TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    sensors_json        JSONB,
    raw_json            JSONB        NOT NULL,
    UNIQUE (location_id)
);

COMMENT ON TABLE raw.openaq_locations IS 'Raw OpenAQ v3 location (station) metadata';

-- ---------------------------------------------------------------------------
-- raw.open_meteo_weather — raw Open-Meteo hourly weather records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.open_meteo_weather (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    city_id             VARCHAR(50)  NOT NULL,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    timestamp_utc       TIMESTAMPTZ  NOT NULL,
    temperature_2m      DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    precipitation       DOUBLE PRECISION,
    wind_speed_10m      DOUBLE PRECISION,
    wind_direction_10m  DOUBLE PRECISION,
    surface_pressure    DOUBLE PRECISION,
    cloud_cover         DOUBLE PRECISION,
    visibility          DOUBLE PRECISION,
    raw_json            JSONB,
    source_file         TEXT,
    UNIQUE (city_id, timestamp_utc)               -- idempotent
);

CREATE INDEX IF NOT EXISTS idx_raw_weather_ts     ON raw.open_meteo_weather(timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_raw_weather_city   ON raw.open_meteo_weather(city_id);

COMMENT ON TABLE raw.open_meteo_weather IS 'Raw Open-Meteo hourly weather records';

-- ---------------------------------------------------------------------------
-- raw.cpcb_measurements — raw CPCB CSV/Excel file records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.cpcb_measurements (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_file         TEXT         NOT NULL,
    row_number          INTEGER,
    station_name_raw    TEXT,
    city_raw            TEXT,
    date_raw            TEXT,
    time_raw            TEXT,
    parameter_raw       TEXT,
    value_raw           TEXT,
    unit_raw            TEXT,
    raw_json            JSONB        NOT NULL    -- full original row as dict
);

CREATE INDEX IF NOT EXISTS idx_raw_cpcb_file      ON raw.cpcb_measurements(source_file);
CREATE INDEX IF NOT EXISTS idx_raw_cpcb_run       ON raw.cpcb_measurements(run_id);

COMMENT ON TABLE raw.cpcb_measurements IS 'Raw CPCB CSV/Excel file records (unmodified; allows varying column layouts)';

-- ---------------------------------------------------------------------------
-- raw.kaggle_measurements — raw Kaggle AQI dataset records
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.kaggle_measurements (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    dataset_name        TEXT         NOT NULL,   -- e.g. 'city_day', 'station_day'
    source_file         TEXT         NOT NULL,
    row_number          INTEGER,
    city_raw            TEXT,
    station_raw         TEXT,
    date_raw            TEXT,
    parameter_raw       TEXT,
    value_raw           TEXT,
    aqi_raw             TEXT,
    aqi_bucket_raw      TEXT,
    raw_json            JSONB        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_kaggle_file    ON raw.kaggle_measurements(source_file);
CREATE INDEX IF NOT EXISTS idx_raw_kaggle_dataset ON raw.kaggle_measurements(dataset_name);
CREATE INDEX IF NOT EXISTS idx_raw_kaggle_run     ON raw.kaggle_measurements(run_id);

COMMENT ON TABLE raw.kaggle_measurements IS 'Raw Kaggle AQI dataset records (unmodified)';

-- =============================================================================
-- 04_staging_tables.sql — Staging layer (typed, normalised, pre-validation)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- staging.station_dim — canonical station registry / crosswalk
-- Built by normalise.py from all sources; unresolved aliases logged as warnings
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.station_dim (
    station_id          BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL UNIQUE,   -- e.g. 'openaq_12345' or 'cpcb_delhi_ito'
    station_name_canonical TEXT      NOT NULL,
    city_id             VARCHAR(50)  NOT NULL,
    state               TEXT,
    country             VARCHAR(5)   NOT NULL DEFAULT 'IN',
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    source_system       VARCHAR(50)  NOT NULL,           -- which source first registered this station
    openaq_location_id  BIGINT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.station_alias (
    alias_id            BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL REFERENCES staging.station_dim(station_id_canonical),
    source_system       VARCHAR(50)  NOT NULL,
    alias_name          TEXT         NOT NULL,
    alias_id_in_source  TEXT,
    match_method        VARCHAR(50)  NOT NULL CHECK (match_method IN ('exact','normalised','coordinate','manual')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, alias_name)
);

COMMENT ON TABLE staging.station_dim   IS 'Canonical station registry; all sources resolve to one row here';
COMMENT ON TABLE staging.station_alias IS 'Station name aliases across sources → canonical station mapping';

-- ---------------------------------------------------------------------------
-- staging.measurements — typed, normalised, pre-validation measurements
-- All four sources funnel here after name/unit/timestamp normalisation
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.measurements (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    source_system       VARCHAR(50)  NOT NULL,           -- 'openaq' | 'cpcb' | 'kaggle'
    station_id_canonical TEXT,                           -- NULL if not yet resolved
    station_name_raw    TEXT,
    city_id             VARCHAR(50),
    pollutant_code      VARCHAR(20)  NOT NULL,
    value_normalised    DOUBLE PRECISION,                -- in standard unit (µg/m³ or mg/m³ for CO)
    unit_standard       VARCHAR(20)  NOT NULL,
    unit_original       VARCHAR(30),
    timestamp_utc       TIMESTAMPTZ  NOT NULL,
    timestamp_ist       TIMESTAMPTZ,
    hour_utc            TIMESTAMPTZ,                     -- floor to hour
    date_utc            DATE,
    source_row_id       BIGINT,                          -- FK to raw.* id
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Composite index for dedup and join performance
CREATE INDEX IF NOT EXISTS idx_staging_meas_key ON staging.measurements
    (station_id_canonical, pollutant_code, timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_staging_meas_src ON staging.measurements(source_system);
CREATE INDEX IF NOT EXISTS idx_staging_meas_run ON staging.measurements(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_meas_ts  ON staging.measurements(timestamp_utc DESC);

COMMENT ON TABLE staging.measurements IS 'All measurements after normalisation but before DQ validation';

-- ---------------------------------------------------------------------------
-- staging.weather — normalised weather records pre-join
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.weather (
    id                  BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    city_id             VARCHAR(50)  NOT NULL,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    timestamp_utc       TIMESTAMPTZ  NOT NULL,
    temperature_2m      DOUBLE PRECISION,      -- °C
    relative_humidity_2m DOUBLE PRECISION,     -- %
    precipitation       DOUBLE PRECISION,      -- mm/hr
    wind_speed_10m      DOUBLE PRECISION,      -- km/h
    wind_direction_10m  DOUBLE PRECISION,      -- degrees
    surface_pressure    DOUBLE PRECISION,      -- hPa
    cloud_cover         DOUBLE PRECISION,      -- %
    visibility          DOUBLE PRECISION,      -- metres
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, timestamp_utc)
);

CREATE INDEX IF NOT EXISTS idx_staging_weather_ts   ON staging.weather(timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_staging_weather_city ON staging.weather(city_id);

COMMENT ON TABLE staging.weather IS 'Normalised Open-Meteo weather records before joining with measurements';

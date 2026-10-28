-- =============================================================================
-- 06_analytics_tables.sql — Gold layer: dimension tables + fact + marts
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Dimension tables
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_city (
    city_id             VARCHAR(50)  PRIMARY KEY,
    city_name           TEXT         NOT NULL,
    state               TEXT,
    country             VARCHAR(5)   NOT NULL DEFAULT 'IN',
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    is_primary          BOOLEAN      NOT NULL DEFAULT false,
    timezone            VARCHAR(50)  NOT NULL DEFAULT 'Asia/Kolkata',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.dim_station (
    station_id_canonical TEXT        PRIMARY KEY,
    station_name        TEXT         NOT NULL,
    city_id             VARCHAR(50)  NOT NULL REFERENCES analytics.dim_city(city_id),
    state               TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    source_system       VARCHAR(50),
    openaq_location_id  BIGINT,
    is_active           BOOLEAN      NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.station_alias (
    alias_id            BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL REFERENCES analytics.dim_station(station_id_canonical),
    source_system       VARCHAR(50)  NOT NULL,
    alias_name          TEXT         NOT NULL,
    alias_id_in_source  TEXT,
    match_method        VARCHAR(50)  NOT NULL CHECK (match_method IN ('exact','normalised','coordinate','manual')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, alias_name)
);

COMMENT ON TABLE analytics.station_alias IS 'Cross-source station aliases mirrored from staging.station_alias';

CREATE TABLE IF NOT EXISTS analytics.dim_pollutant (
    pollutant_code      VARCHAR(20)  PRIMARY KEY,
    display_name        TEXT         NOT NULL,
    standard_unit       VARCHAR(20)  NOT NULL,
    aqi_averaging       VARCHAR(20),           -- '24h_mean' or '8h_max'
    is_aqi_component    BOOLEAN      NOT NULL DEFAULT true,
    physical_min        DOUBLE PRECISION,
    physical_max        DOUBLE PRECISION,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id             DATE         PRIMARY KEY,
    year                SMALLINT     NOT NULL,
    month               SMALLINT     NOT NULL,
    day                 SMALLINT     NOT NULL,
    day_of_week         SMALLINT     NOT NULL,   -- 0=Monday, 6=Sunday (ISO)
    week_of_year        SMALLINT,
    quarter             SMALLINT     NOT NULL,
    is_weekend          BOOLEAN      NOT NULL,
    season              VARCHAR(20),             -- Winter/Spring/Summer/Monsoon/Autumn
    is_stubble_season   BOOLEAN      NOT NULL DEFAULT false,  -- Oct–Nov (Punjab/Haryana burning)
    day_name            TEXT,
    month_name          TEXT
);

COMMENT ON TABLE analytics.dim_city      IS 'City dimension table';
COMMENT ON TABLE analytics.dim_station   IS 'Station dimension table with canonical IDs';
COMMENT ON TABLE analytics.dim_pollutant IS 'Pollutant dimension table';
COMMENT ON TABLE analytics.dim_date      IS 'Date dimension with calendar attributes and India-specific season flags';

-- ---------------------------------------------------------------------------
-- Fact table: station-hour level (pollutants + weather + AQI)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_station_hourly (
    id                  BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL REFERENCES analytics.dim_station(station_id_canonical),
    city_id             VARCHAR(50)  NOT NULL REFERENCES analytics.dim_city(city_id),
    hour_utc            TIMESTAMPTZ  NOT NULL,
    date_utc            DATE         NOT NULL REFERENCES analytics.dim_date(date_id),
    hour_of_day         SMALLINT     NOT NULL,
    -- Pollutants (µg/m³ except CO in mg/m³)
    pm25                DOUBLE PRECISION,
    pm10                DOUBLE PRECISION,
    no2                 DOUBLE PRECISION,
    so2                 DOUBLE PRECISION,
    co                  DOUBLE PRECISION,
    o3                  DOUBLE PRECISION,
    nh3                 DOUBLE PRECISION,
    nox                 DOUBLE PRECISION,
    -- Weather
    temperature_2m      DOUBLE PRECISION,
    relative_humidity   DOUBLE PRECISION,
    precipitation       DOUBLE PRECISION,
    wind_speed          DOUBLE PRECISION,
    wind_direction      DOUBLE PRECISION,
    surface_pressure    DOUBLE PRECISION,
    -- Data quality flags
    has_weather         BOOLEAN      NOT NULL DEFAULT false,
    pollutant_count     SMALLINT     NOT NULL DEFAULT 0,
    -- Lineage
    source_systems      TEXT[],                  -- array of contributing sources
    UNIQUE (station_id_canonical, hour_utc)
);

CREATE INDEX IF NOT EXISTS idx_fact_hourly_station_ts ON analytics.fact_station_hourly
    (station_id_canonical, hour_utc DESC);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_city_ts    ON analytics.fact_station_hourly
    (city_id, hour_utc DESC);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_date       ON analytics.fact_station_hourly(date_utc DESC);

COMMENT ON TABLE analytics.fact_station_hourly IS
    'Central fact table: hourly measurements + weather per station; source of all mart rollups';

-- ---------------------------------------------------------------------------
-- Mart: station daily AQI
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_station_daily_aqi (
    id                  BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL REFERENCES analytics.dim_station(station_id_canonical),
    city_id             VARCHAR(50)  NOT NULL,
    date_utc            DATE         NOT NULL REFERENCES analytics.dim_date(date_id),
    aqi                 DOUBLE PRECISION,
    aqi_category        VARCHAR(30),             -- Good/Satisfactory/Moderate/Poor/Very Poor/Severe
    aqi_category_num    SMALLINT,                -- 1-6 ordinal
    dominant_pollutant  VARCHAR(20),
    pm25_24h            DOUBLE PRECISION,
    pm10_24h            DOUBLE PRECISION,
    no2_24h             DOUBLE PRECISION,
    so2_24h             DOUBLE PRECISION,
    co_8h_max           DOUBLE PRECISION,
    o3_8h_max           DOUBLE PRECISION,
    nh3_24h             DOUBLE PRECISION,
    obs_hours           SMALLINT,                -- valid hours contributing
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (station_id_canonical, date_utc)
);

CREATE INDEX IF NOT EXISTS idx_mart_station_aqi_date ON analytics.mart_station_daily_aqi
    (station_id_canonical, date_utc DESC);
CREATE INDEX IF NOT EXISTS idx_mart_station_aqi_city ON analytics.mart_station_daily_aqi
    (city_id, date_utc DESC);

-- ---------------------------------------------------------------------------
-- Mart: city daily AQI (average across stations)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_city_daily_aqi (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL REFERENCES analytics.dim_city(city_id),
    date_utc            DATE         NOT NULL REFERENCES analytics.dim_date(date_id),
    aqi_mean            DOUBLE PRECISION,
    aqi_max             DOUBLE PRECISION,
    aqi_min             DOUBLE PRECISION,
    aqi_category        VARCHAR(30),
    aqi_category_num    SMALLINT,
    dominant_pollutant  VARCHAR(20),
    station_count       SMALLINT,               -- stations contributing
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, date_utc)
);

CREATE INDEX IF NOT EXISTS idx_mart_city_aqi_date ON analytics.mart_city_daily_aqi
    (city_id, date_utc DESC);

-- ---------------------------------------------------------------------------
-- Mart: pollutant daily averages by city
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_pollutant_daily (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    pollutant_code      VARCHAR(20)  NOT NULL,
    date_utc            DATE         NOT NULL,
    daily_mean          DOUBLE PRECISION,
    daily_max           DOUBLE PRECISION,
    daily_min           DOUBLE PRECISION,
    daily_std           DOUBLE PRECISION,
    sub_index_mean      DOUBLE PRECISION,
    station_count       SMALLINT,
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, pollutant_code, date_utc)
);

-- ---------------------------------------------------------------------------
-- Mart: dominant pollutant contribution share by city+month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_pollutant_contribution (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    year                SMALLINT     NOT NULL,
    month               SMALLINT     NOT NULL,
    pollutant_code      VARCHAR(20)  NOT NULL,
    dominant_days       INTEGER      NOT NULL DEFAULT 0,
    total_days          INTEGER      NOT NULL DEFAULT 0,
    contribution_pct    NUMERIC(5,2),
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, year, month, pollutant_code)
);

-- ---------------------------------------------------------------------------
-- Mart: hourly patterns (hour × weekday × city — average AQI)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_hourly_pattern (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    hour_of_day         SMALLINT     NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week         SMALLINT     NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    aqi_mean            DOUBLE PRECISION,
    pm25_mean           DOUBLE PRECISION,
    obs_count           INTEGER,
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, hour_of_day, day_of_week)
);

-- ---------------------------------------------------------------------------
-- Mart: monthly seasonality patterns
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_monthly_pattern (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    year                SMALLINT     NOT NULL,
    month               SMALLINT     NOT NULL CHECK (month BETWEEN 1 AND 12),
    aqi_mean            DOUBLE PRECISION,
    aqi_p75             DOUBLE PRECISION,
    pm25_mean           DOUBLE PRECISION,
    pm10_mean           DOUBLE PRECISION,
    good_days           INTEGER,
    satisfactory_days   INTEGER,
    moderate_days       INTEGER,
    poor_days           INTEGER,
    very_poor_days      INTEGER,
    severe_days         INTEGER,
    total_days          INTEGER,
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, year, month)
);

-- ---------------------------------------------------------------------------
-- Mart: weather impact on AQI
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_weather_impact (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    weather_variable    VARCHAR(50)  NOT NULL,   -- 'temperature' | 'humidity' | 'wind_speed' | 'precipitation'
    bin_label           TEXT         NOT NULL,   -- e.g. '10-15°C' or 'Rainy'
    bin_low             DOUBLE PRECISION,
    bin_high            DOUBLE PRECISION,
    aqi_mean            DOUBLE PRECISION,
    aqi_median          DOUBLE PRECISION,
    obs_count           INTEGER,
    pearson_r           DOUBLE PRECISION,        -- correlation with AQI (overall, not per bin)
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, weather_variable, bin_label)
);

-- ---------------------------------------------------------------------------
-- Mart: data quality summary by station × month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_data_quality (
    id                  BIGSERIAL    PRIMARY KEY,
    station_id_canonical TEXT        NOT NULL,
    city_id             VARCHAR(50)  NOT NULL,
    year                SMALLINT     NOT NULL,
    month               SMALLINT     NOT NULL,
    pollutant_code      VARCHAR(20)  NOT NULL,
    completeness_pct    NUMERIC(5,2),
    rejected_count      INTEGER      NOT NULL DEFAULT 0,
    total_expected      INTEGER,
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (station_id_canonical, year, month, pollutant_code)
);

-- ---------------------------------------------------------------------------
-- Stage B: analytics.features_daily — feature store for ML
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.features_daily (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    date_utc            DATE         NOT NULL,
    feature_version     VARCHAR(50)  NOT NULL,
    built_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    features_json       JSONB        NOT NULL,   -- full feature vector as JSON
    target_aqi          DOUBLE PRECISION,        -- t+1 day AQI (regression target)
    target_category     VARCHAR(30),             -- t+1 day AQI category (classification target)
    data_window_start   DATE,
    data_window_end     DATE,
    UNIQUE (city_id, date_utc, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_features_city_date ON analytics.features_daily
    (city_id, date_utc DESC);

-- ---------------------------------------------------------------------------
-- Stage B: analytics.predictions — batch/API predictions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.predictions (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    as_of_date          DATE         NOT NULL,   -- date t (features available up to t)
    prediction_date     DATE         NOT NULL,   -- date t+1 (what we predict)
    predicted_aqi       DOUBLE PRECISION,
    predicted_category  VARCHAR(30),
    actual_aqi          DOUBLE PRECISION,        -- filled in when actuals arrive
    actual_category     VARCHAR(30),
    model_name          TEXT,
    model_version       TEXT,
    feature_version     VARCHAR(50),
    prediction_type     VARCHAR(20),             -- 'batch' or 'api'
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, as_of_date, model_name, prediction_type)
);

-- =============================================================================
-- 02_ops_tables.sql — Operational metadata tables
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ops.source_registry — catalogue of all four data sources
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.source_registry (
    source_id          VARCHAR(50)  PRIMARY KEY,
    source_name        TEXT         NOT NULL,
    source_url         TEXT         NOT NULL,
    access_type        VARCHAR(30)  NOT NULL CHECK (access_type IN ('api','file','manual_download','api_or_file')),
    licence            TEXT,
    terms_url          TEXT,
    expected_role      TEXT,
    required           BOOLEAN      NOT NULL DEFAULT true,
    api_version        VARCHAR(20),
    notes              TEXT,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ops.source_registry IS 'Catalogue of all data sources used by the pipeline';

-- Insert the four mandatory sources
INSERT INTO ops.source_registry
    (source_id, source_name, source_url, access_type, licence, terms_url, expected_role, required, api_version, notes)
VALUES
    ('openaq',     'OpenAQ API',
     'https://api.openaq.org/v3',
     'api',
     'CC BY 4.0 (data from member agencies; check individual station terms)',
     'https://openaq.org/developers/api/',
     'Primary API source: real-time pollutant measurements for Indian cities',
     true, 'v3',
     'Requires X-API-Key header; free tier: 60 req/min, 1000/day'),

    ('cpcb',       'CPCB CAAQMS Portal',
     'https://cpcb.nic.in/CAAQMS/',
     'manual_download',
     'Government of India Open Data; non-commercial use',
     'https://cpcb.nic.in/',
     'India-specific official measurements; historical backfill',
     false, NULL,
     'Manual download via CPCB portal (captcha form). Drop CSV/Excel in data/external/cpcb/. See docs/DATA_SOURCES.md.'),

    ('open_meteo', 'Open-Meteo Weather API',
     'https://api.open-meteo.com/v1/archive',
     'api',
     'CC BY 4.0; non-commercial use required for free tier',
     'https://open-meteo.com/en/terms',
     'Weather enrichment: temperature, humidity, wind, precipitation per station',
     true, 'v1',
     'No API key required. Historical archive + forecast endpoints.'),

    ('kaggle',     'Kaggle AQI Datasets',
     'https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india',
     'api_or_file',
     'CC0 1.0 Public Domain Dedication',
     'https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india',
     'Historical backfill / baseline: India AQI 2015-2020',
     false, NULL,
     'Supports Kaggle API (KAGGLE_USERNAME + KAGGLE_KEY) or manual file drop in data/external/kaggle/.')

ON CONFLICT (source_id) DO UPDATE SET
    required = EXCLUDED.required,
    updated_at = NOW();

-- ---------------------------------------------------------------------------
-- ops.ingestion_log — one row per extraction attempt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.ingestion_log (
    log_id              BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    source_id           VARCHAR(50)  NOT NULL REFERENCES ops.source_registry(source_id),
    dag_id              VARCHAR(200),
    task_id             VARCHAR(200),
    endpoint_or_file    TEXT,
    extraction_ts       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    request_params      JSONB,
    status              VARCHAR(20)  NOT NULL CHECK (status IN ('SUCCESS','PARTIAL','FAILED','SKIPPED')),
    http_status         INTEGER,
    rows_extracted      INTEGER,
    error_message       TEXT,
    duration_s          NUMERIC(10,3),
    file_path           TEXT,
    file_checksum       VARCHAR(64),   -- xxhash64 hex
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_run_id    ON ops.ingestion_log(run_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_source    ON ops.ingestion_log(source_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_ts        ON ops.ingestion_log(extraction_ts DESC);

COMMENT ON TABLE ops.ingestion_log IS 'Per-extraction-attempt log; one row per API call or file load';

-- ---------------------------------------------------------------------------
-- ops.rejected_records — individual rows that failed validation
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.rejected_records (
    reject_id           BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    source_id           VARCHAR(50)  NOT NULL,
    layer               VARCHAR(20)  NOT NULL CHECK (layer IN ('raw','staging','clean')),
    original_row        JSONB        NOT NULL,
    rule_violated       VARCHAR(200) NOT NULL,
    rejection_reason    TEXT         NOT NULL,
    pollutant_code      VARCHAR(20),
    station_id          TEXT,
    timestamp_utc       TIMESTAMPTZ,
    rejected_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rejected_run_id    ON ops.rejected_records(run_id);
CREATE INDEX IF NOT EXISTS idx_rejected_source    ON ops.rejected_records(source_id);
CREATE INDEX IF NOT EXISTS idx_rejected_rule      ON ops.rejected_records(rule_violated);
CREATE INDEX IF NOT EXISTS idx_rejected_ts        ON ops.rejected_records(rejected_at DESC);

COMMENT ON TABLE ops.rejected_records IS 'Each row that failed a validation rule, with the full original row for debugging';

-- ---------------------------------------------------------------------------
-- ops.dq_results — per-run / per-check data quality summary
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.dq_results (
    result_id           BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    check_name          VARCHAR(200) NOT NULL,
    layer               VARCHAR(20)  NOT NULL,
    source_id           VARCHAR(50),
    passed_count        INTEGER      NOT NULL DEFAULT 0,
    failed_count        INTEGER      NOT NULL DEFAULT 0,
    total_count         INTEGER      NOT NULL DEFAULT 0,
    failure_rate        NUMERIC(6,4) GENERATED ALWAYS AS (
                            CASE WHEN total_count > 0
                                 THEN failed_count::NUMERIC / total_count
                                 ELSE 0 END
                        ) STORED,
    threshold           NUMERIC(6,4),
    status              VARCHAR(20)  NOT NULL CHECK (status IN ('PASS','FAIL','WARN','SKIP')),
    notes               TEXT,
    checked_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_results_run_id ON ops.dq_results(run_id);
CREATE INDEX IF NOT EXISTS idx_dq_results_status ON ops.dq_results(status);

COMMENT ON TABLE ops.dq_results IS 'Per-run data quality gate summary; aggregated from rejected_records';

-- ---------------------------------------------------------------------------
-- ops.source_coverage — cross-layer source coverage matrix
-- Populated by verify_sources.py / verify_source_coverage Airflow task
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.source_coverage (
    coverage_id         BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL,
    source_id           VARCHAR(50)  NOT NULL REFERENCES ops.source_registry(source_id),
    layer               VARCHAR(20)  NOT NULL CHECK (layer IN ('raw','staging','clean','analytics')),
    row_count           BIGINT       NOT NULL DEFAULT 0,
    station_count       INTEGER,
    date_min            DATE,
    date_max            DATE,
    status              VARCHAR(30)  NOT NULL
                            CHECK (status IN ('OK','NOT_PROVIDED','EMPTY','FAILED','PARTIAL')),
    notes               TEXT,
    checked_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_id, layer)
);

CREATE INDEX IF NOT EXISTS idx_source_coverage_run  ON ops.source_coverage(run_id);
CREATE INDEX IF NOT EXISTS idx_source_coverage_src  ON ops.source_coverage(source_id);

COMMENT ON TABLE ops.source_coverage IS 'Source × layer coverage matrix; built at end of every pipeline run';

-- ---------------------------------------------------------------------------
-- ops.pipeline_run — master run metadata
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id              UUID         PRIMARY KEY,
    dag_id              VARCHAR(200),
    dag_run_id          VARCHAR(500),
    started_at          TIMESTAMPTZ  NOT NULL,
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(20)  CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),
    config_json         JSONB,
    summary_json        JSONB,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ops.pipeline_run IS 'Master table tracking each end-to-end pipeline run';

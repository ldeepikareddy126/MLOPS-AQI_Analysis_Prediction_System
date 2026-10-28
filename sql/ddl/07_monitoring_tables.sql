-- =============================================================================
-- 07_monitoring_tables.sql — Stage B: Monitoring, Drift, Alerts, Prediction Logs
-- =============================================================================

CREATE TABLE IF NOT EXISTS monitoring.prediction_log (
    id                  BIGSERIAL    PRIMARY KEY,
    prediction_id       UUID         NOT NULL,
    timestamp           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    model_name          TEXT         NOT NULL,
    model_version       TEXT         NOT NULL,
    city_id             VARCHAR(50)  NOT NULL,
    as_of_date          DATE         NOT NULL,
    predicted_aqi       DOUBLE PRECISION,
    predicted_category  VARCHAR(30),
    actual_aqi          DOUBLE PRECISION,
    actual_category     VARCHAR(30),
    latency_ms          DOUBLE PRECISION,
    input_features      JSONB,
    client_ip           TEXT
);

CREATE INDEX IF NOT EXISTS idx_pred_log_ts ON monitoring.prediction_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pred_log_city_date ON monitoring.prediction_log(city_id, as_of_date DESC);

CREATE TABLE IF NOT EXISTS monitoring.api_request_log (
    request_id          UUID         PRIMARY KEY,
    timestamp           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    endpoint            TEXT         NOT NULL,
    method              VARCHAR(10)  NOT NULL,
    status_code         SMALLINT     NOT NULL,
    latency_ms          DOUBLE PRECISION NOT NULL,
    error_type          TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_request_ts ON monitoring.api_request_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_api_request_status_ts ON monitoring.api_request_log(status_code, timestamp DESC);

CREATE TABLE IF NOT EXISTS monitoring.drift_metrics (
    id                  BIGSERIAL    PRIMARY KEY,
    evaluated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    feature_name        VARCHAR(100) NOT NULL,
    baseline_start      DATE,
    baseline_end        DATE,
    current_start       DATE,
    current_end         DATE,
    psi                 DOUBLE PRECISION,
    ks_statistic        DOUBLE PRECISION,
    ks_pvalue           DOUBLE PRECISION,
    drift_status        VARCHAR(20)  NOT NULL,  -- 'STABLE', 'MODERATE', 'SIGNIFICANT'
    details             JSONB
);

CREATE INDEX IF NOT EXISTS idx_drift_eval ON monitoring.drift_metrics(evaluated_at DESC, feature_name);

CREATE TABLE IF NOT EXISTS monitoring.alerts (
    id                  BIGSERIAL    PRIMARY KEY,
    alert_id            UUID         NOT NULL,
    raised_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    alert_type          VARCHAR(50)  NOT NULL,  -- 'DATA_DRIFT', 'PERFORMANCE_DROP', 'QUALITY_DEGRADATION', 'HIGH_LATENCY'
    severity            VARCHAR(20)  NOT NULL,  -- 'INFO', 'WARNING', 'CRITICAL'
    description         TEXT         NOT NULL,
    metric_value        DOUBLE PRECISION,
    threshold_value     DOUBLE PRECISION,
    is_resolved         BOOLEAN      NOT NULL DEFAULT false,
    resolved_at         TIMESTAMPTZ,
    metadata            JSONB
);

CREATE INDEX IF NOT EXISTS idx_alerts_raised ON monitoring.alerts(raised_at DESC, severity);

CREATE TABLE IF NOT EXISTS monitoring.model_performance_daily (
    id                  BIGSERIAL    PRIMARY KEY,
    date_evaluated      DATE         NOT NULL,
    model_name          TEXT         NOT NULL,
    model_version       TEXT         NOT NULL,
    mae                 DOUBLE PRECISION,
    rmse                DOUBLE PRECISION,
    r2                  DOUBLE PRECISION,
    f1_macro            DOUBLE PRECISION,
    accuracy            DOUBLE PRECISION,
    sample_count        INTEGER,
    evaluated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (date_evaluated, model_name, model_version)
);

-- Monitoring demo schema for drift demonstration
CREATE TABLE IF NOT EXISTS monitoring_demo.drifted_features (
    id                  BIGSERIAL    PRIMARY KEY,
    city_id             VARCHAR(50)  NOT NULL,
    date_utc            DATE         NOT NULL,
    features_json       JSONB        NOT NULL,
    demo_only           BOOLEAN      NOT NULL DEFAULT true,
    perturbation_desc   TEXT         NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

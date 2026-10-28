-- =============================================================================
-- 01_schemas.sql — Create all schemas
-- =============================================================================

-- Raw landing zone: unmodified API responses / file data
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging: typed, normalised, not yet validated
CREATE SCHEMA IF NOT EXISTS staging;

-- Clean: validated, deduped, weather-joined
CREATE SCHEMA IF NOT EXISTS clean;

-- Analytics / Gold: mart tables for dashboard and ML
CREATE SCHEMA IF NOT EXISTS analytics;

-- Operational metadata: ingestion logs, DQ results, coverage
CREATE SCHEMA IF NOT EXISTS ops;

-- Monitoring (Stage B): prediction logs, drift, alerts
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Monitoring demo only (perturbed data for drift demonstration)
CREATE SCHEMA IF NOT EXISTS monitoring_demo;

COMMENT ON SCHEMA raw        IS 'Raw landing zone: unmodified source data';
COMMENT ON SCHEMA staging    IS 'Staging: typed and normalised, pre-validation';
COMMENT ON SCHEMA clean      IS 'Clean: validated, deduped, weather-enriched';
COMMENT ON SCHEMA analytics  IS 'Gold layer: mart tables for dashboard and ML';
COMMENT ON SCHEMA ops        IS 'Operational metadata: logs, DQ results, coverage';
COMMENT ON SCHEMA monitoring IS 'Stage B: prediction logs, drift metrics, alerts';
COMMENT ON SCHEMA monitoring_demo IS 'Stage B demo only: perturbed data for drift demo, NEVER used in real metrics';

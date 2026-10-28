-- =============================================================================
-- init_databases.sql — PostgreSQL Initialisation Script
-- Runs automatically via docker-entrypoint-initdb.d on first container start.
-- This file is executed via psql by the Postgres docker entrypoint, so
-- \gexec IS available here (it's a psql meta-command executed by psql).
-- Creates the Airflow metadata DB and MLflow backend DB alongside the
-- default aqi_warehouse DB (already created by POSTGRES_DB env var).
-- =============================================================================

-- Airflow metadata database
SELECT 'CREATE DATABASE airflow_metadata OWNER ' || current_user
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_metadata')\gexec

-- MLflow backend database (Stage B)
SELECT 'CREATE DATABASE mlflow_backend OWNER ' || current_user
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow_backend')\gexec

-- Grant all privileges on all databases to the owner
GRANT ALL PRIVILEGES ON DATABASE aqi_warehouse TO CURRENT_USER;
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO CURRENT_USER;
GRANT ALL PRIVILEGES ON DATABASE mlflow_backend TO CURRENT_USER;

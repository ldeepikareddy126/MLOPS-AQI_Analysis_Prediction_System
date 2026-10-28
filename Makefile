# =============================================================================
# Makefile — AQI MLOps Project
# =============================================================================

.PHONY: help up up-all down reset logs test e2e verify train serve monitor demo-all \
        ps shell-airflow shell-postgres fernet-key

COMPOSE      = docker compose
STAGE_A      = --profile stagea
STAGE_LITE   = --profile lite
STAGE_ALL    = --profile stagea --profile mlops
AIRFLOW_SVC  = airflow-webserver
AIRFLOW_WEBSERVER_PORT ?= 8082

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Stack management
# ---------------------------------------------------------------------------
up-lite:  ## Alias of Stage A (same services as --profile lite)
	$(COMPOSE) $(STAGE_LITE) up -d --build

up:  ## Start Stage A stack (Postgres, Airflow, Dashboard, pgAdmin)
	$(COMPOSE) $(STAGE_A) up -d --build
	@echo ""
	@echo "  ✅  Stack starting..."
	@echo "  🌐  Airflow UI   → http://localhost:$(AIRFLOW_WEBSERVER_PORT)  (admin / see .env)"
	@echo "  📊  Dashboard    → http://localhost:8501"
	@echo "  🐘  pgAdmin      → http://localhost:5050"

up-all:  ## Start full stack including Stage B (MLflow, API)
	$(COMPOSE) $(STAGE_ALL) up -d --build
	@echo ""
	@echo "  ✅  Full stack starting..."
	@echo "  🌐  Airflow UI   → http://localhost:$(AIRFLOW_WEBSERVER_PORT)"
	@echo "  📊  Dashboard    → http://localhost:8501"
	@echo "  🔬  MLflow UI    → http://localhost:5000"
	@echo "  🚀  FastAPI      → http://localhost:8000/docs"
	@echo "  🐘  pgAdmin      → http://localhost:5050"

down:  ## Stop all services (data preserved)
	$(COMPOSE) $(STAGE_ALL) down

reset:  ## ⚠️  Full reset: stop all, remove volumes, rebuild (destroys ALL data)
	$(COMPOSE) $(STAGE_ALL) down -v --remove-orphans
	docker volume prune -f
	@echo "  ⚠️   All volumes removed. Run 'make up' to rebuild from scratch."

logs:  ## Tail logs for all services
	$(COMPOSE) $(STAGE_A) logs -f

logs-airflow:  ## Tail Airflow webserver logs
	$(COMPOSE) logs -f airflow-webserver

logs-scheduler:  ## Tail Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

ps:  ## Show running service status
	$(COMPOSE) $(STAGE_ALL) ps

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test:  ## Run Stage A unit + integration tests
	$(COMPOSE) $(STAGE_A) run --rm airflow-webserver \
	  python -m pytest /opt/airflow_src/tests/ -v --tb=short 2>&1

test-all:  ## Run full test suite (Stage A + Stage B)
	$(COMPOSE) $(STAGE_ALL) run --rm airflow-webserver \
	  python -m pytest /opt/airflow_src/tests/ /opt/airflow_src/mlops/tests/ -v --tb=short 2>&1

# ---------------------------------------------------------------------------
# End-to-end demo
# ---------------------------------------------------------------------------
e2e:  ## Run Stage A end-to-end demo (ingest → raw → staging → clean → gold → coverage check)
	@echo "  🔄  Triggering Stage A end-to-end pipeline..."
	$(COMPOSE) $(STAGE_A) run --rm airflow-webserver \
	  bash /opt/airflow/scripts/run_e2e_demo.sh

verify:  ## Run source coverage verification (scripts/verify_sources.py)
	$(COMPOSE) $(STAGE_A) run --rm airflow-webserver \
	  python /opt/airflow/scripts/verify_sources.py

# ---------------------------------------------------------------------------
# Stage B operations
# ---------------------------------------------------------------------------
train:  ## Trigger training pipeline (regression + classification)
	$(COMPOSE) $(STAGE_ALL) run --rm airflow-webserver \
	  airflow dags trigger aqi_training_pipeline

serve:  ## Check API health and print a sample prediction
	@curl -s http://localhost:8000/health | python -m json.tool
	@echo ""
	@curl -s -X POST http://localhost:8000/predict \
	  -H "Content-Type: application/json" \
	  -d '{"city_id": "delhi", "as_of_date": "$(shell date -I)"}' | python -m json.tool

monitor:  ## Trigger monitoring DAG
	$(COMPOSE) $(STAGE_ALL) run --rm airflow-webserver \
	  airflow dags trigger aqi_monitoring

demo-all:  ## Full Stage B demo: reset → up → e2e → train → serve → monitor
	make reset
	make up-all
	@echo "  ⏳  Waiting 90s for services to stabilise..."
	sleep 90
	make e2e
	make train
	@echo "  ⏳  Waiting 60s for training to complete..."
	sleep 60
	make serve
	make monitor

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
fernet-key:  ## Generate a new Airflow Fernet key
	@docker run --rm python:3.11-slim \
	  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

shell-airflow:  ## Open a shell in the Airflow webserver container
	$(COMPOSE) exec airflow-webserver bash

shell-postgres:  ## Open psql in the Postgres container
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER} -d $${AQI_DB_NAME}

export-samples:  ## Export sample tables to docs/samples/
	$(COMPOSE) $(STAGE_A) run --rm airflow-webserver \
	  python /opt/airflow/scripts/export_sample_tables.py

# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------
backfill:  ## Run a date-range backfill: make backfill START=2022-01-01 END=2022-12-31
	$(COMPOSE) $(STAGE_A) run --rm airflow-webserver \
	  python /opt/airflow/scripts/trigger_dag.py aqi_etl_pipeline \
	    --start-date "$(START)" --end-date "$(END)" \
	    --extra-conf '{"backfill": true}'

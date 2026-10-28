# Assignment requirements crosscheck

Status key: **Verified** = code and current runtime evidence support it; **Implemented** = code exists but current live evidence is absent; **Partial** = material limitation remains; **Not provided** = deliverable still must be supplied.

## Rubric-to-code map

| Rubric area | Primary implementation and evidence |
|---|---|
| Problem understanding and design | `README.md`, `docs/ARCHITECTURE.md`, and the Stage A/Stage B workflow descriptions in `airflow/dags/`. |
| Data sources and ingestion | `src/aqi_pipeline/collectors/`, `src/aqi_pipeline/landing/raw_files.py`, `src/aqi_pipeline/quality/ops.py`, `airflow/dags/aqi_ingest_*.py`, and `docs/DATA_SOURCES.md`. |
| ETL, validation, and storage | `src/aqi_pipeline/transform/`, `sql/ddl/01_schemas.sql` through `07_monitoring_tables.sql`, `docs/DATA_DICTIONARY.md`, and `tests/unit/`. |
| Visualization and interpretation | `dashboard/app.py`, `dashboard/pages/`, `docs/screenshots/`, and the dashboard evidence in `docs/EXECUTION_EVIDENCE.md`. |
| Documentation | `README.md`, `docs/ARCHITECTURE.md`, `docs/DATA_SOURCES.md`, `docs/DATA_DICTIONARY.md`, this crosscheck, and `docs/EXECUTION_EVIDENCE.md`. The separate academic report is outside the project folder. |
| Model development and evaluation | `mlops/src/aqi_mlops/features/`, `mlops/src/aqi_mlops/training/`, `airflow/dags/aqi_training_pipeline.py`, and `mlops/tests/test_training.py`. |
| MLflow, registry, Docker, and serving | `mlops/src/aqi_mlops/registry/`, `mlops/src/aqi_mlops/serving/`, `docker-compose.yml`, `mlops/Dockerfile.api`, `mlops/Dockerfile.mlflow`, and `docs/EXECUTION_EVIDENCE.md`. These surfaces are inventoried here; no deployment implementation changes are part of this review. |

## Part 1 — data engineering

| Requirement | Status | Evidence / remaining work |
|---|---|---|
| Collect pollution from a free public source | Verified | OpenAQ v3 collector, run-scoped extraction logs/raw payloads, successful 30-day run. |
| Collect weather enrichment | Verified | Open-Meteo collector; 4,320 raw/staging hourly rows for six cities in the verified run. |
| Use CPCB/Kaggle as optional additional data | Partial (optional sources) | The CPCB fixture has 3 CSV rows; the Kaggle fixture has 3 city-day rows that expand to 21 pollutant measurements. Both validate parsing only, not historical coverage. OpenAQ and Open-Meteo provide the demonstrated primary sources; use approved historical CPCB/Kaggle data only if claiming those datasets in the final analysis. |
| Preserve raw API/file data before transformation | Verified with limitation | Raw files/payloads and relational raw tables exist. Relational natural-key upserts are not a complete immutable extraction archive; use raw file landing for retained payload history. |
| Record extraction date, source, status and row count | Verified | `ops.ingestion_log` includes extraction timestamp, source, endpoint/file, status and row count. |
| Handle extraction errors and log failures | Implemented | Collector retries/logs API failures and extraction metadata. Verify failure behavior with an intentionally invalid key only in a disposable/local test. |
| Repeatable/scheduled Airflow ingestion | Verified | All seven project DAGs are registered/unpaused with no import errors. Scheduler and metadata DB are healthy; ETL and monitoring are daily, training is weekly. Latest ETL run passed all three tasks and strict source verification; run UUID matches the warehouse and 16 coverage rows. Earlier failed attempts are retained and explained in execution evidence. |
| Raw, staging, clean and analytics layers | Verified | PostgreSQL schemas and coverage matrix; verified source counts across layers. |
| Normalize timestamps, stations and pollutant units | Verified | Canonical station/pollutant/unit transformations; UTC/IST staging timestamps. |
| Deduplicate, validate, reject and detect missing periods | Partial | Range checks/rejections and source priority exist. Completeness is based on the observed first-to-last interval and does not synthesize wholly absent periods. |
| Join weather by station/city and timestamp | Verified | City/hour Open-Meteo values join into station-hour facts by city and UTC hour. |
| Calculate averages and AQI categories | Verified | The refreshed run populated 24,568 daily rows and gold marts using the corrected rule: at least three valid pollutant sub-indices including PM2.5 or PM10; 8-hour maxima require a complete window. |
| Gold city/station/pollutant/date analysis | Verified | Dimension, fact, daily AQI, pollutant, hourly/monthly, weather-impact and DQ marts exist. |
| Dashboard with five meaningful views/indicators | Verified in live app | Eight main views now cover overview/AQI scale, city and station AQI trends, city comparison, pollutant concentrations, hourly/weekday patterns, monthly seasonality, dominant-pollutant days, weather association, source coverage and MLOps; dedicated health/prediction pages add more. All views passed live Streamlit smoke tests, including CO's correct 8-hour statistic. Seven live screenshots are bundled under `docs/screenshots/` and embedded in the report. |
| Suggested hourly/monthly and pollutant-contribution visuals | Verified in live app | `Patterns & Contributions` visualizes hourly/weekday PM2.5 concentration, monthly mean and 75th-percentile AQI, and dominant-pollutant share of valid station-days. Labels clarify UTC hours, distinguish concentration from AQI, and note that dominance is not an emissions inventory. |
| Architecture diagram and data dictionary | Implemented | `docs/ARCHITECTURE.md` and `docs/DATA_DICTIONARY.md`. |
| Source access instructions and project README | Implemented | `docs/DATA_SOURCES.md` and root `README.md`. |
| Approximately 8–10 page project report | Partial | The latest report package is documented as a 20-page PDF stored outside the project folder. It contains the required topics, but exceeds the assignment's approximate length target and should be condensed to 8–10 pages unless the instructor approves the longer format. |
| Demonstrate successful end-to-end execution | Verified | Stage A refresh, corrected-data training, and monitoring DAG runs succeeded; evidence and run IDs are recorded in `docs/EXECUTION_EVIDENCE.md`. Fresh screenshots are optional because run/test/database evidence is documented. |

## Part 2 — MLOps extension

| Requirement | Status | Evidence / remaining work |
|---|---|---|
| Next-day AQI regression and category classification | Verified | Reusable next-day feature targets; both training tasks and API response fields implemented. |
| Reusable lag/rolling feature pipeline with a true next-calendar-day target | Verified | Corrected features were rebuilt and trained; feature store contains 2,310 rows, including 182 matured labels and unlabeled dates for inference. |
| Chronological evaluation split | Verified | The corrected training DAG succeeded with global date ordering and training-only imputation/scaling. |
| Track RMSE/MAE and F1/accuracy with MLflow | Verified | Latest run logged regression MAE 23.30, RMSE 43.68, R² -0.025 and classifier macro-F1 0.361, accuracy 0.703. The small/mixed-coverage sample limits interpretation. |
| Register/version models | Verified | Both model families are registered. Regressor v1 remains Production; weaker corrected-data regressor v2 was archived. Classifier v2 remains unpromoted. |
| FastAPI inference in Docker | Verified | Live `/ready` and `/health` succeeded with both model families loaded; a post-rebuild Delhi `/predict` returned AQI 142.82. A legacy Production model's missing-input failure was fixed using its training-fitted scaler medians; invalid input returns 422. |
| Integrate predictions into dashboard | Verified in code/runtime | MLOps dashboard page and API test form; batch/API prediction tables are shown. |
| Monitor input quality, drift, forecast error, latency and failures | Partial with live evidence | Monitoring DAG succeeded and raised drift (29/33 PSI features), latency (p95 4,254 ms), and API failure alerts. The rolling API log includes seven synthetic test 503s and one pre-fix inference 500; telemetry is now isolated in tests, the live API is healthy, and the old alert must be reassessed after its rolling window expires. No delayed forecast-performance rows are available until actuals mature. |
| Define retraining criteria and model lifecycle | Partial | Weekly training and registry promotion exist. Report documents operational triggers, but retraining is not automatically triggered by monitor alerts; operator approval/runbook remains the control. |

## Presentation readiness notes

- The 20-page PDF and editable/source files are stored in Downloads outside the project folder. Seven runtime screenshots remain under `docs/screenshots/`; execution evidence and a timed demo guide are in this project.
- A 20-slide editable PowerPoint with project-specific dashboard evidence and date-free workflow diagrams is delivered separately outside the project folder. No presentation deck or preview PDF is stored in `docs/`. The requested source template was not available in the workspace/attachments.
- Reassess the API failure/latency alert after the 24-hour window containing pre-fix and synthetic test requests expires; the live API was healthy at the last check and test telemetry is isolated.
- Treat CPCB/Kaggle data as parser fixtures unless real approved historical files are added; neither optional source is required for the primary OpenAQ/Open-Meteo demonstration.
- State model limitations clearly: the holdout regression R² is negative, classifier macro-F1 is modest, only 182 labels matured, and delayed forecast-performance rows are not yet available.

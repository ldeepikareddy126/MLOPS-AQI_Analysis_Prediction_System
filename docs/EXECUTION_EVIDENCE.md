# Execution evidence

This file records the most recent observed end-to-end evidence available in the project workspace. It supports the assignment's screenshots-or-execution-evidence submission requirement.

## Verified environment and run

- Docker Compose stack was running when checked: PostgreSQL, Airflow webserver/scheduler, Streamlit dashboard, MLflow 3.2.0, and FastAPI containers reported healthy. pgAdmin was running. FastAPI `/ready` returned `ready`; `/health` returned `ok` with both model families loaded.
- Airflow integration check: all seven project DAGs were registered and unpaused, `airflow dags list-import-errors` returned no errors, the metadata database and scheduler heartbeat were healthy, and the active executor was `LocalExecutor`. The scheduler metadata showed the next ETL/monitoring interval for 2026-10-16 00:00 UTC and the next weekly training interval for 2026-10-18 00:00 UTC.
- The latest complete ETL Airflow run `manual__2026-10-15T13:45:08+00:00` succeeded in all three tasks (`make_run_id`, `run_pipeline`, `verify_source_coverage`). Its pipeline UUID `e463631a-a16a-499a-868a-b4ba73973167` matches the successful `ops.pipeline_run` row and has 16 source/layer coverage records. All five tasks in the corrected-data training DAG and all five tasks in the post-serving-fix monitoring DAG also succeeded.
- Several earlier ETL and training attempts remain visible as failed historical Airflow runs during debugging. The current ETL uses `pipeline_run_id` and bounded database-side transformations; the latest end-to-end ETL, training, and monitoring runs succeeded. Historical failures remain in Airflow for audit rather than being hidden.
- Successful Stage A run: run UUID `e463631a-a16a-499a-868a-b4ba73973167`, DAG `aqi_etl_pipeline`, 2026-10-15, requested window 2026-09-15 through 2026-10-14.
- The optimized, bounded refresh completed successfully for that run: 526,782 clean rows, 24,568 daily rows, and populated hourly/city/pollutant/monthly/weather/data-quality analytics marts. It reused landed data rather than making another multi-million-row API extraction.
- Source coverage recorded for that run: OpenAQ raw 3,250,417, staging 2,005,721, clean 526,758, analytics facts 85,503; Open-Meteo 4,320 hourly rows; CPCB sample 3 rows; Kaggle sample 21 pollutant measurements.
- The 24-row difference between aggregate and OpenAQ-only staging is the CPCB and Kaggle fixtures (3 + 21 measurements). The Kaggle input itself has 3 city-day CSV rows that expand to 21 pollutant measurements; both files are parser fixtures, not historical datasets. The aggregate clean count likewise includes those 24 rows.
- OpenAQ raw-to-staging difference is exactly 1,244,696. A raw parameter count showed these records are parameters outside the canonical AQI pollutant scope (notably weather variables and other measures), which remain raw and are intentionally not transformed into AQI pollutant rows.
- The final ETL quality check rejected 472 of 2,005,745 checked rows (below the configured 20% threshold): 2,005,273 passed, 472 failed, status `PASS`. Rejected rows were persisted, and the retry-safe check now has one result row for the run.
- Corrected-data training DAG run `audit-training-corrected-data` succeeded. The feature store has 2,310 rows across 2025-09-26–2026-10-15, of which 182 have matured next-day labels. Regression v2 logged MAE 23.30, RMSE 43.68, and R² -0.025; it was archived rather than replacing Production regressor v1. Classifier v2 logged macro-F1 0.361 and accuracy 0.703 and remains unpromoted. These metrics demonstrate the workflow, not reliable generalization.
- Six city batch predictions were stored. After rebuilding the API, a live Delhi request for 2026-10-14 succeeded with next-day AQI 142.82 (regressor v1), and `/health` confirmed both model families loaded. A pre-fix request for the same date had exposed missing-pollutant NaNs in the legacy Production model; inference now fills them from the model's own training-fitted scaler medians. Invalid input remains a 422.
- Monitoring DAG run `audit-monitoring-after-serving-fix` succeeded. It stored drift results and raised a warning that 29 of 33 evaluated features had significant PSI drift. No delayed model-performance rows exist yet because predictions do not have a sufficient set of matured actual outcomes.
- The 24-hour API sample at the final monitor run had 19 requests, 7 server errors, and p95 latency 4,254 ms, producing API failure and latency alerts. Seven 503s now present in the rolling request log came from pre-isolation API unit-test requests written to the shared warehouse; one 500 was the live missing-value inference failure fixed above. API test fixtures now stub telemetry except in the dedicated logging test. The final full suite left the server-error row count unchanged at 8, confirming test traffic is isolated. The alert reflects the pre-fix rolling window and should be reassessed after it expires.
- Final full Docker test suite after the fixes: 111 passed, 0 failed. One Starlette/httpx deprecation warning remains; it did not affect test outcomes.

## Presentation readiness recheck (2026-10-15)

- The full Compose stack was rechecked: PostgreSQL, Airflow webserver/scheduler, Streamlit, MLflow, and FastAPI were running; services with health checks reported healthy. Airflow `/health` reported a healthy metadata database and scheduler heartbeat. FastAPI `/ready` and `/health` passed and reported both model families loaded; Streamlit health returned `ok`; the MLflow UI responded.
- All seven project DAGs were listed as unpaused and Airflow reported no DAG import errors. The latest ETL run remains `manual__2026-10-15T13:45:08+00:00` (success); the latest training (`audit-training-corrected-data`) and monitoring (`audit-monitoring-after-serving-fix`) runs also succeeded.
- The full documented Docker test command was rerun: **111 passed, 0 failed**, with one non-failing Starlette/httpx deprecation warning.
- The dashboard now includes the existing hourly/weekday, monthly AQI, and dominant-pollutant marts as visualizations, and uses the correct 8-hour measure for CO/O3. All eight main dashboard views passed live Streamlit smoke tests; the CO chart rendered with the CO selection.
- At the 2026-10-15 readiness check, the report package was an 8-page PDF. On 2026-10-19 it was replaced with a 20-page academic report, stored outside the project folder in the user's Downloads. The editable DOCX, Markdown source, report builder and four required screenshots are kept with that local report package. Its run/model values remain dated evidence from 2026-10-15, not a fresh runtime check. The current project folder intentionally does not contain the generated report. A separate 20-slide PowerPoint deck uses project-specific dashboard evidence and date-free workflow diagrams; neither the deck nor a presentation PDF preview is stored in the project folder. `docs/PRESENTATION_GUIDE.md` gives the recommended demo sequence and cautions. The original presentation template was not available in the workspace/attachments.

## Subsequent runtime check (2026-10-16)

- The full Compose stack was started without triggering ETL, training, or monitoring. Before starting the Airflow scheduler, all seven DAGs were paused to prevent scheduled work; no new DAG run was created. The ETL run history remained unchanged.
- At that check, PostgreSQL, Airflow webserver/scheduler, Streamlit, MLflow, FastAPI, and pgAdmin were running. The application health endpoints returned HTTP 200 and the dashboard loaded warehouse data. FastAPI `/ready` passed with the Production regressor loaded; the classifier was not loaded because no classifier version was in Production.
- A later check on the same day found Docker Desktop unavailable and the Airflow/MLflow endpoints unreachable. Therefore, the October 15/16 health results are historical snapshots and must not be presented as proof that services are running now. Check Docker and each health endpoint before a live demo.

## Reproduction and evidence collection

For a live demonstration, use the existing successful Stage A run rather than triggering a fresh multi-million-row extraction. The screenshots already bundled in `docs/screenshots/` capture:

1. Airflow `aqi_etl_pipeline` successful run and its task graph.
2. Streamlit Overview and at least four analytical views.
3. Source coverage matrix and DQ/rejection view.
4. MLflow experiment metrics/registered model version.
5. FastAPI `/ready` and a successful `/predict` response.
6. Monitoring DAG output and monitoring dashboard.

The live Streamlit verification exercised all eight main dashboard views, city/station AQI trend modes, the CO pollutant selection, and both multipage views against the populated warehouse; all rendered without Streamlit exceptions or database-query warnings. This exposed and fixed invalid operational-table column references, incorrect weather-chart semantics, stale all-history drift counts, and the dashboard image's NumPy/PyArrow incompatibility. The updated dashboard image uses NumPy 1.26.4 with PyArrow 17.0.0, and UUID fields are rendered as text.

Suggested read-only SQL checks (replace the run UUID as appropriate):

```sql
SELECT run_id, dag_id, status, started_at, finished_at, summary_json
FROM ops.pipeline_run
WHERE run_id = 'e463631a-a16a-499a-868a-b4ba73973167';

SELECT source_id, layer, row_count, station_count, date_min, date_max, status
FROM ops.source_coverage
WHERE run_id = 'e463631a-a16a-499a-868a-b4ba73973167'
ORDER BY source_id, layer;

SELECT check_name, layer, source_id, passed_count, failed_count, threshold, status
FROM ops.dq_results
WHERE run_id = 'e463631a-a16a-499a-868a-b4ba73973167';

SELECT COUNT(*) AS successful_predictions FROM monitoring.prediction_log;
SELECT COUNT(*) AS prediction_requests FROM monitoring.api_request_log;
```

## Remaining runtime caveats

The report, editable submission copy, screenshot evidence, and execution evidence are present. At the last observed monitoring run, the rolling API failure/latency alert still included pre-fix and synthetic test requests; reassess after that 24-hour window expires rather than presenting it as a clean current failure-rate estimate. The live API health checks passed after the inference fix. Drift remains elevated, and delayed forecast metrics are unavailable until actual outcomes mature. CPCB/Kaggle files remain parser fixtures, and the current model metrics do not support a claim of reliable generalization.

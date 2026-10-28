# AQI Project Presentation Guide

The editable 20-slide project presentation is delivered as a separate download outside the project folder; no presentation deck or preview PDF is stored in this project. The PowerPoint template mentioned in the request was not present in the available workspace or attachments, so the deck uses a project-specific design. The slides explain the data layers, AQI method, time-series evaluation, monitoring approach, and assignment evidence, alongside verified project results.

## Before presenting

- Start Docker Desktop and confirm the existing Compose services are healthy. If the stack is stopped, from the project root run `docker compose --profile stagea --profile mlops up -d` and wait for health checks.
- Open Streamlit at `http://localhost:8501`, Airflow at `http://localhost:8082`, MLflow at `http://localhost:5000`, and FastAPI readiness at `http://localhost:8000/ready`.
- Use the locally configured Airflow account from `.env`; do not show or share the password.
- Do not run `docker compose down -v`; it removes persisted data. Avoid triggering a new ETL during the presentation unless a fresh extraction is explicitly required and there is time to wait.
- Use the evidence already collected: the successful ETL run is `manual__2026-10-15T13:45:08+00:00`, pipeline UUID `e463631a-a16a-499a-868a-b4ba73973167`. Its data window is 15 September–14 October 2026.

## Suggested six-minute walkthrough

| Time | Screen | Talk track |
|---|---|---|
| 0:00–0:45 | Architecture / dashboard Overview | “This project collects public OpenAQ pollution and Open-Meteo weather, preserves raw data, transforms it into PostgreSQL analytical layers, and exposes the results in Streamlit. The same warehouse supports next-day AQI regression and category classification.” |
| 0:45–1:45 | Airflow `aqi_etl_pipeline` | Select the successful 15 October run and open Graph. Explain `make_run_id → run_pipeline → verify_source_coverage`. Point out all three tasks succeeded. Older failed development attempts remain visible for audit and are not being hidden. |
| 1:45–3:00 | Streamlit Overview, AQI Trends, Pollutants | Explain the AQI scale and city/station comparison. Select CO and note that the chart uses the complete rolling 8-hour maximum in mg/m³; it does not use a 24-hour CO mean. Missing observations are not zero pollution. |
| 3:00–4:00 | Patterns & Contributions, Pipeline Health | Show hourly/weekday PM2.5, monthly AQI, dominant-pollutant share, source coverage, and DQ results. Hourly values are UTC; dominant pollutant means the highest valid AQI sub-index, not an emissions inventory. |
| 4:00–5:15 | MLOps dashboard, MLflow, FastAPI | Show recorded predictions, model registry, and API readiness. The serving workflow runs, but the regression holdout R² is -0.025, classifier macro-F1 is 0.361, and only 182 labels matured. These prove workflow execution, not reliable forecast generalization. |
| 5:15–6:00 | Monitoring and close | Explain 29 of 33 features showed significant PSI drift and delayed forecast-performance rows are not yet available. The latest monitor's rolling API alert included pre-fix/synthetic requests; live readiness passed after the fix. Retraining and model promotion remain operator-reviewed. |

## Accurate facts to emphasize

- The demonstrated live sources are OpenAQ v3 and Open-Meteo. CPCB and Kaggle files in the repository are small parser fixtures, not historical datasets.
- The successful Stage A run processed 526,782 clean rows and 24,568 daily rows; 472 of 2,005,745 checked records were rejected, below the configured 20% failure threshold.
- The latest corrected-data training run logged regression MAE 23.30, RMSE 43.68, R² -0.025 and classifier accuracy 0.703, macro-F1 0.361. Regressor v1 remains in Production; weaker regressor v2 was archived and classifier v2 was not promoted.
- AQI calculation is educational and CPCB-style, not an official regulator-certified result. Weather relationships are descriptive, not causal. Coverage and station counts matter when interpreting city averages.
- Historical failed Airflow runs and monitoring alerts are retained. Present the latest successful DAG run while acknowledging those records and their context.

## If asked to run the pipeline

Explain that a new ETL performs external API extraction, writes new raw landing data, and then transforms/loads warehouse layers; it is not a dashboard-only refresh. It can take time, depends on public API availability/rate limits, and can change data powering the dashboard. For a short presentation, use the already successful run and its traceable run ID instead.

## Evidence files

- The 20-page PDF report, editable DOCX, Markdown source and rebuild assets are in the user's Downloads, outside this project folder.
- The separately delivered editable PowerPoint — 20 slides with project-specific dashboard evidence and date-free workflow diagrams.
- `EXECUTION_EVIDENCE.md` and `REQUIREMENTS_CROSSCHECK.md` — detailed run IDs, tests, counts, and requirement status.
- `screenshots/` — seven captured views from the running dashboard and Airflow.

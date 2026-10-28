"""Stage B: AQI Monitoring DAG.

Daily monitoring pipeline:
  1. Evaluate yesterday's predictions vs actuals (MAE, RMSE, R², accuracy)
  2. Detect feature drift (baseline window vs recent window)
  3. Monitor API latency/failures and ETL source/data quality
  4. Raise alerts for performance drops or significant drift
  5. Persist metrics to monitoring tables
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="aqi_monitoring",
    description="Stage B: Evaluate prediction accuracy, detect drift, raise alerts",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["aqi", "stage-b", "monitoring", "mlops"],
    doc_md="""
    # AQI Monitoring DAG

    Runs daily. Performs:
    1. Prediction accuracy evaluation vs actuals (back-fills monitoring.prediction_log)
    2. Feature PSI/KS drift detection (baseline=last 90 days vs current=last 14 days)
    3. Prediction API p95 latency and server-error rate
    4. Required-source freshness and recent ETL data-quality failures
    5. Alerts for performance drops (MAE > threshold) or significant drift (PSI ≥ 0.2)
    """,
)
def aqi_monitoring():

    @task
    def evaluate_performance(**context) -> dict:
        """Compare yesterday's predictions against actuals."""
        from aqi_mlops.monitoring import evaluate_prediction_accuracy, store_daily_performance
        from aqi_mlops.training import REGRESSOR_NAME, CLASSIFIER_NAME
        import os
        import mlflow
        from mlflow.tracking import MlflowClient

        eval_date = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"))
        client = MlflowClient()
        versions = client.get_latest_versions(REGRESSOR_NAME, stages=["Production"])
        if not versions:
            return {"sample_count": 0, "status": "NO_PRODUCTION_MODEL"}
        reg_model_version = str(max(versions, key=lambda version: int(version.version)).version)

        perf = evaluate_prediction_accuracy(REGRESSOR_NAME, reg_model_version, eval_date)
        store_daily_performance(perf)
        return perf

    @task
    def detect_drift(**context) -> dict:
        """Compute PSI and KS statistics for feature distributions."""
        from aqi_mlops.monitoring import detect_feature_drift, store_drift_metrics
        from datetime import date

        today = date.today()
        baseline_days = 90
        current_days = 14
        baseline_end = (today - timedelta(days=current_days + 1)).isoformat()
        baseline_start = (today - timedelta(days=current_days + baseline_days + 1)).isoformat()
        current_start = (today - timedelta(days=current_days)).isoformat()
        current_end = today.isoformat()

        results = detect_feature_drift(
            baseline_start=baseline_start,
            baseline_end=baseline_end,
            current_start=current_start,
            current_end=current_end,
        )
        stored = store_drift_metrics(results)

        significant = [r for r in results if r["drift_status"] == "SIGNIFICANT"]
        return {
            "features_evaluated": len(results),
            "significant_drift_count": len(significant),
            "drifted_features": [r["feature_name"] for r in significant],
            "stored": stored,
        }

    @task
    def evaluate_service_health() -> dict:
        from aqi_mlops.monitoring import evaluate_api_health, evaluate_pipeline_quality

        return {
            "api": evaluate_api_health(),
            "pipeline": evaluate_pipeline_quality(),
        }

    @task
    def raise_alerts(perf: dict, drift: dict, health: dict, **context) -> dict:
        """Raise alerts if thresholds are breached."""
        from aqi_mlops.monitoring import raise_alert

        alerts_raised = []

        # Performance alert
        mae = perf.get("mae")
        mae_threshold = 30.0
        if mae and mae > mae_threshold:
            alert_id = raise_alert(
                alert_type="PERFORMANCE_DROP",
                severity="WARNING",
                description=f"Regressor MAE={mae:.1f} exceeds threshold={mae_threshold}",
                metric_value=mae,
                threshold_value=mae_threshold,
                metadata=perf,
            )
            alerts_raised.append({"type": "PERFORMANCE_DROP", "alert_id": alert_id})

        # R² alert
        r2 = perf.get("r2")
        if r2 is not None and r2 < 0.5:
            alert_id = raise_alert(
                alert_type="PERFORMANCE_DROP",
                severity="CRITICAL",
                description=f"Regressor R²={r2:.4f} is critically low (< 0.5)",
                metric_value=r2,
                threshold_value=0.5,
                metadata=perf,
            )
            alerts_raised.append({"type": "PERFORMANCE_DROP_R2", "alert_id": alert_id})

        # Drift alert
        n_drifted = drift.get("significant_drift_count", 0)
        if n_drifted > 0:
            alert_id = raise_alert(
                alert_type="DATA_DRIFT",
                severity="WARNING",
                description=f"{n_drifted} features with significant PSI drift detected",
                metric_value=float(n_drifted),
                threshold_value=0.0,
                metadata=drift,
            )
            alerts_raised.append({"type": "DATA_DRIFT", "alert_id": alert_id})

        return {
            "alerts_raised": len(alerts_raised),
            "details": alerts_raised,
            "service_health": health,
        }

    @task
    def generate_monitoring_report(perf: dict, drift: dict, health: dict, alerts: dict) -> dict:
        """Log a monitoring summary to Airflow task logs."""
        import logging

        log = logging.getLogger("aqi_monitoring")
        log.info("=" * 60)
        log.info("DAILY MONITORING REPORT")
        log.info("=" * 60)
        log.info("Performance: MAE=%.2f  RMSE=%.2f  R²=%.4f  Accuracy=%.4f  Samples=%d",
                 perf.get("mae", 0), perf.get("rmse", 0),
                 perf.get("r2", 0), perf.get("accuracy", 0),
                 perf.get("sample_count", 0))
        log.info("Drift: %d features evaluated, %d with significant drift",
                 drift.get("features_evaluated", 0), drift.get("significant_drift_count", 0))
        if drift.get("drifted_features"):
            log.info("Drifted features: %s", ", ".join(drift["drifted_features"]))
        log.info("Alerts raised: %d", alerts.get("alerts_raised", 0))
        log.info("API health: %s", health.get("api", {}))
        log.info("Pipeline quality: %s", health.get("pipeline", {}))
        log.info("=" * 60)

        return {
            "status": "SUCCESS",
            "performance": perf,
            "drift_summary": drift,
            "service_health": health,
            "alerts": alerts,
        }

    perf = evaluate_performance()
    drift = detect_drift()
    health = evaluate_service_health()
    alerts = raise_alerts(perf, drift, health)
    generate_monitoring_report(perf, drift, health, alerts)


aqi_monitoring()

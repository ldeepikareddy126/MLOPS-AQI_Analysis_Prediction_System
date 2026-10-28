"""Monitoring, drift detection, and alerting modules."""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from aqi_pipeline.utils.db import get_engine
from aqi_pipeline.utils.logging import get_logger
from sqlalchemy import text

log = get_logger(__name__)

# PSI bin count for Population Stability Index
PSI_BINS = 10
PSI_MODERATE_THRESHOLD = 0.1
PSI_SIGNIFICANT_THRESHOLD = 0.2


# ---------------------------------------------------------------------------
# Population Stability Index (PSI)
# ---------------------------------------------------------------------------

def compute_psi(expected: np.ndarray, actual: np.ndarray, bins: int = PSI_BINS) -> float:
    """
    Compute PSI between two distributions (expected=baseline, actual=current).

    PSI < 0.1  → Stable
    PSI < 0.2  → Moderate drift
    PSI >= 0.2 → Significant drift
    """
    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]

    if len(expected) == 0 or len(actual) == 0:
        return float("nan")

    # Bin edges from expected distribution
    breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))
    breakpoints = np.unique(breakpoints)
    if len(breakpoints) < 2:
        center = float(breakpoints[0])
        tolerance = max(abs(center) * 1e-6, 1e-6)
        bin_edges = np.array([-np.inf, center - tolerance, center + tolerance, np.inf])
    else:
        bin_edges = np.concatenate(([-np.inf], breakpoints[1:-1], [np.inf]))

    expected_percents = np.histogram(expected, bins=bin_edges)[0] / len(expected)
    actual_percents = np.histogram(actual, bins=bin_edges)[0] / len(actual)

    # Clip to avoid log(0)
    expected_percents = np.where(expected_percents == 0, 1e-4, expected_percents)
    actual_percents = np.where(actual_percents == 0, 1e-4, actual_percents)

    psi = np.sum((actual_percents - expected_percents) * np.log(actual_percents / expected_percents))
    return float(psi)


# ---------------------------------------------------------------------------
# KS test wrapper
# ---------------------------------------------------------------------------

def compute_ks(expected: np.ndarray, actual: np.ndarray) -> tuple[float, float]:
    """Return (ks_statistic, p_value) between two samples."""
    from scipy.stats import ks_2samp

    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]
    if len(expected) == 0 or len(actual) == 0:
        return float("nan"), float("nan")
    stat, pval = ks_2samp(expected, actual)
    return float(stat), float(pval)


# ---------------------------------------------------------------------------
# Feature drift detection
# ---------------------------------------------------------------------------

def detect_feature_drift(
    baseline_start: str,
    baseline_end: str,
    current_start: str,
    current_end: str,
    feature_version: str = "v1",
) -> list[dict[str, Any]]:
    """
    Compare feature distributions between baseline and current windows.

    For each numeric feature, computes PSI and KS-test.
    Returns a list of drift result dicts, one per feature.
    """
    engine = get_engine()

    def _load_window(start: str, end: str) -> pd.DataFrame:
        sql = text("""
            SELECT features_json FROM analytics.features_daily
            WHERE feature_version = :ver
              AND date_utc BETWEEN :start AND :end
        """)
        with engine.connect() as conn:
            rows = conn.execute(sql, {"ver": feature_version, "start": start, "end": end}).fetchall()
        if not rows:
            return pd.DataFrame()
        records = [json.loads(r[0]) if isinstance(r[0], str) else r[0] for r in rows]
        return pd.DataFrame(records)

    baseline_df = _load_window(baseline_start, baseline_end)
    current_df = _load_window(current_start, current_end)

    if baseline_df.empty or current_df.empty:
        log.warning("Drift detection skipped: empty baseline or current window")
        return []

    results = []
    for col in baseline_df.select_dtypes(include=[np.number]).columns:
        if col not in current_df.columns:
            continue
        exp = baseline_df[col].dropna().values
        act = current_df[col].dropna().values
        psi = compute_psi(exp, act)
        ks_stat, ks_pval = compute_ks(exp, act)

        if np.isnan(psi):
            status = "UNKNOWN"
        elif psi < PSI_MODERATE_THRESHOLD:
            status = "STABLE"
        elif psi < PSI_SIGNIFICANT_THRESHOLD:
            status = "MODERATE"
        else:
            status = "SIGNIFICANT"

        results.append(
            {
                "feature_name": col,
                "baseline_start": baseline_start,
                "baseline_end": baseline_end,
                "current_start": current_start,
                "current_end": current_end,
                "psi": psi,
                "ks_statistic": ks_stat,
                "ks_pvalue": ks_pval,
                "drift_status": status,
            }
        )

    return results


def store_drift_metrics(drift_results: list[dict[str, Any]]) -> int:
    """Persist drift detection results to monitoring.drift_metrics."""
    if not drift_results:
        return 0
    engine = get_engine()
    sql = text("""
        INSERT INTO monitoring.drift_metrics
            (feature_name, baseline_start, baseline_end, current_start, current_end,
             psi, ks_statistic, ks_pvalue, drift_status, details)
        VALUES
            (:feature_name, :baseline_start, :baseline_end, :current_start, :current_end,
             :psi, :ks_statistic, :ks_pvalue, :drift_status, :details)
    """)
    rows = [
        {**r, "details": json.dumps({k: v for k, v in r.items() if k not in
            {"feature_name", "baseline_start", "baseline_end",
             "current_start", "current_end", "psi", "ks_statistic", "ks_pvalue", "drift_status"}})}
        for r in drift_results
    ]
    with engine.begin() as conn:
        conn.execute(sql, rows)
    log.info("Stored %d drift metric rows", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Model performance monitoring
# ---------------------------------------------------------------------------

def evaluate_prediction_accuracy(
    model_name: str,
    model_version: str,
    evaluation_date: str | None = None,
) -> dict[str, float | int]:
    """
    Compare stored predictions against actuals for the given model/date.

    Actuals are read from analytics.mart_city_daily_aqi for prediction_date.
    """
    eval_date = evaluation_date or (date.today() - timedelta(days=1)).isoformat()
    engine = get_engine()

    sql = text("""
        SELECT p.city_id, p.prediction_date, p.predicted_aqi, p.predicted_category,
               m.aqi_mean AS actual_aqi, m.aqi_category AS actual_category
        FROM analytics.predictions p
        JOIN analytics.mart_city_daily_aqi m
          ON m.city_id = p.city_id AND m.date_utc = p.prediction_date
        WHERE p.model_name = :model_name
          AND p.model_version = :model_version
          AND p.prediction_date = :eval_date
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"model_name": model_name,
                                             "model_version": model_version,
                                             "eval_date": eval_date})

    if df.empty:
        return {"sample_count": 0}

    df = df.dropna(subset=["predicted_aqi", "actual_aqi"])
    if df.empty:
        return {"sample_count": 0}

    mae = float((df["predicted_aqi"] - df["actual_aqi"]).abs().mean())
    rmse = float(np.sqrt(((df["predicted_aqi"] - df["actual_aqi"]) ** 2).mean()))
    r2 = 1 - ((df["predicted_aqi"] - df["actual_aqi"]) ** 2).sum() / \
        ((df["actual_aqi"] - df["actual_aqi"].mean()) ** 2).sum()

    cat_map = {"Good": 1, "Satisfactory": 2, "Moderate": 3, "Poor": 4, "Very Poor": 5, "Severe": 6}
    df["pred_cat_num"] = df["predicted_category"].map(cat_map).fillna(0).astype(int)
    df["actual_cat_num"] = df["actual_category"].map(cat_map).fillna(0).astype(int)

    cat_mask = (df["pred_cat_num"] > 0) & (df["actual_cat_num"] > 0)
    accuracy = float((df.loc[cat_mask, "pred_cat_num"] == df.loc[cat_mask, "actual_cat_num"]).mean()) if cat_mask.any() else 0.0

    # Update actuals in predictions table
    _update_prediction_actuals(df, engine)

    return {
        "model_name": model_name,
        "model_version": model_version,
        "date": eval_date,
        "mae": mae,
        "rmse": rmse,
        "r2": float(r2),
        "accuracy": accuracy,
        "sample_count": len(df),
    }


def _update_prediction_actuals(df: pd.DataFrame, engine: Any) -> None:
    """Back-fill actual_aqi and actual_category in analytics.predictions."""
    sql = text("""
        UPDATE analytics.predictions SET
            actual_aqi      = :actual_aqi,
            actual_category = :actual_category
        WHERE city_id = :city_id AND prediction_date = :prediction_date
    """)
    rows = [
        {
            "city_id": r["city_id"],
            "prediction_date": r["prediction_date"],
            "actual_aqi": float(r["actual_aqi"]) if pd.notna(r["actual_aqi"]) else None,
            "actual_category": r.get("actual_category"),
        }
        for _, r in df.iterrows()
    ]
    if rows:
        with engine.begin() as conn:
            conn.execute(sql, rows)


def store_daily_performance(metrics: dict[str, float | int]) -> None:
    """Persist daily performance metrics to monitoring.model_performance_daily."""
    if not metrics.get("sample_count"):
        return
    engine = get_engine()
    sql = text("""
        INSERT INTO monitoring.model_performance_daily
            (date_evaluated, model_name, model_version, mae, rmse, r2, accuracy, sample_count)
        VALUES
            (:date, :model_name, :model_version, :mae, :rmse, :r2, :accuracy, :sample_count)
        ON CONFLICT (date_evaluated, model_name, model_version) DO UPDATE SET
            mae          = EXCLUDED.mae,
            rmse         = EXCLUDED.rmse,
            r2           = EXCLUDED.r2,
            accuracy     = EXCLUDED.accuracy,
            sample_count = EXCLUDED.sample_count,
            evaluated_at = NOW()
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "date": metrics.get("date", date.today().isoformat()),
            "model_name": metrics.get("model_name", "unknown"),
            "model_version": str(metrics.get("model_version", "unknown")),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "r2": metrics.get("r2"),
            "accuracy": metrics.get("accuracy"),
            "sample_count": int(metrics.get("sample_count", 0)),
        })


def evaluate_api_health(
    lookback_hours: int = 24,
    min_samples: int = 10,
    latency_threshold_ms: float | None = None,
    failure_rate_threshold: float | None = None,
) -> dict[str, Any]:
    """Measure recent prediction endpoint p95 latency and server-error rate."""
    latency_threshold = latency_threshold_ms or float(os.environ.get("API_LATENCY_P95_MS", "2000"))
    failure_threshold = failure_rate_threshold or float(os.environ.get("API_FAILURE_RATE_THRESHOLD", "0.05"))
    sql = text("""
        SELECT COUNT(*) AS sample_count,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
               COUNT(*) FILTER (WHERE status_code >= 500)::double precision
                   / NULLIF(COUNT(*), 0) AS failure_rate
        FROM monitoring.api_request_log
        WHERE timestamp >= NOW() - (:lookback_hours * INTERVAL '1 hour')
    """)
    with get_engine().connect() as conn:
        row = conn.execute(sql, {"lookback_hours": lookback_hours}).mappings().one()

    result = {
        "sample_count": int(row["sample_count"] or 0),
        "p95_latency_ms": float(row["p95_latency_ms"]) if row["p95_latency_ms"] is not None else None,
        "failure_rate": float(row["failure_rate"]) if row["failure_rate"] is not None else None,
        "lookback_hours": lookback_hours,
    }
    if result["sample_count"] < min_samples:
        result["status"] = "INSUFFICIENT_TRAFFIC"
        return result

    result["status"] = "EVALUATED"
    if result["p95_latency_ms"] is not None and result["p95_latency_ms"] > latency_threshold:
        raise_alert(
            alert_type="HIGH_LATENCY",
            severity="WARNING",
            description=f"Prediction API p95 latency={result['p95_latency_ms']:.1f}ms exceeds {latency_threshold:.1f}ms",
            metric_value=result["p95_latency_ms"],
            threshold_value=latency_threshold,
            metadata=result,
        )
    if result["failure_rate"] is not None and result["failure_rate"] > failure_threshold:
        raise_alert(
            alert_type="API_FAILURE",
            severity="CRITICAL",
            description=f"Prediction API server-error rate={result['failure_rate']:.1%} exceeds {failure_threshold:.1%}",
            metric_value=result["failure_rate"],
            threshold_value=failure_threshold,
            metadata=result,
        )
    return result


def evaluate_pipeline_quality(
    lookback_hours: int = 30,
    failure_rate_threshold: float | None = None,
) -> dict[str, Any]:
    """Alert when required sources are stale or recent ETL quality checks fail."""
    threshold = failure_rate_threshold or float(os.environ.get("DQ_FAILURE_RATE_THRESHOLD", "0.20"))
    engine = get_engine()
    source_sql = text("""
        SELECT registry.source_id
        FROM ops.source_registry AS registry
        WHERE registry.required
          AND NOT EXISTS (
              SELECT 1
              FROM ops.ingestion_log AS ingestion
              WHERE ingestion.source_id = registry.source_id
                AND ingestion.status IN ('SUCCESS', 'PARTIAL')
                AND ingestion.extraction_ts >= NOW() - (:lookback_hours * INTERVAL '1 hour')
          )
        ORDER BY registry.source_id
    """)
    dq_sql = text("""
        SELECT COUNT(*) AS checks,
               COUNT(*) FILTER (WHERE status = 'FAIL') AS failed_checks,
               COALESCE(MAX(failure_rate), 0) AS max_failure_rate
        FROM ops.dq_results
        WHERE checked_at >= NOW() - (:lookback_hours * INTERVAL '1 hour')
    """)
    with engine.connect() as conn:
        missing_sources = [
            row[0] for row in conn.execute(source_sql, {"lookback_hours": lookback_hours})
        ]
        dq = conn.execute(dq_sql, {"lookback_hours": lookback_hours}).mappings().one()

    result = {
        "missing_required_sources": missing_sources,
        "dq_checks": int(dq["checks"] or 0),
        "failed_dq_checks": int(dq["failed_checks"] or 0),
        "max_dq_failure_rate": float(dq["max_failure_rate"] or 0),
        "lookback_hours": lookback_hours,
    }
    if missing_sources:
        raise_alert(
            alert_type="QUALITY_DEGRADATION",
            severity="CRITICAL",
            description=f"Required sources have no recent successful ingestion: {', '.join(missing_sources)}",
            metadata=result,
        )
    if result["failed_dq_checks"] or result["max_dq_failure_rate"] > threshold:
        raise_alert(
            alert_type="QUALITY_DEGRADATION",
            severity="WARNING",
            description="Recent ETL quality checks failed or exceeded the configured rejection threshold",
            metric_value=result["max_dq_failure_rate"],
            threshold_value=threshold,
            metadata=result,
        )
    return result


# ---------------------------------------------------------------------------
# Alert management
# ---------------------------------------------------------------------------

def raise_alert(
    alert_type: str,
    severity: str,
    description: str,
    metric_value: float | None = None,
    threshold_value: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    """
    Insert an alert into monitoring.alerts.

    alert_type: 'DATA_DRIFT' | 'PERFORMANCE_DROP' | 'QUALITY_DEGRADATION' | 'HIGH_LATENCY'
    severity:   'INFO' | 'WARNING' | 'CRITICAL'
    Returns the alert_id (UUID string).
    """
    alert_id = str(uuid4())
    engine = get_engine()
    sql = text("""
        INSERT INTO monitoring.alerts
            (alert_id, alert_type, severity, description, metric_value, threshold_value, metadata)
        VALUES
            (:alert_id, :alert_type, :severity, :description,
             :metric_value, :threshold_value, :metadata)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "alert_id": alert_id,
            "alert_type": alert_type,
            "severity": severity,
            "description": description,
            "metric_value": metric_value,
            "threshold_value": threshold_value,
            "metadata": json.dumps(metadata or {}),
        })
    log.warning("Alert raised [%s/%s]: %s", severity, alert_type, description)
    return alert_id


# ---------------------------------------------------------------------------
# Full monitoring pipeline
# ---------------------------------------------------------------------------

def run_monitoring_pipeline(
    model_name: str = "aqi_regressor",
    model_version: str | None = None,
    baseline_days: int = 90,
    current_days: int = 14,
    feature_version: str = "v1",
    mae_alert_threshold: float = 30.0,
    psi_alert_threshold: float = PSI_SIGNIFICANT_THRESHOLD,
) -> dict[str, Any]:
    """
    Full monitoring pipeline:
    1. Evaluate yesterday's predictions vs actuals
    2. Detect feature drift (baseline vs recent window)
    3. Raise alerts if thresholds exceeded

    Returns a summary dict.
    """
    summary: dict[str, Any] = {}

    # --- Step 1: Performance evaluation ---
    log.info("Evaluating prediction accuracy for model '%s'", model_name)
    eval_date = (date.today() - timedelta(days=1)).isoformat()
    perf = evaluate_prediction_accuracy(model_name, model_version or "1", eval_date)
    store_daily_performance(perf)
    summary["performance"] = perf

    if perf.get("mae") and perf["mae"] > mae_alert_threshold:
        raise_alert(
            alert_type="PERFORMANCE_DROP",
            severity="WARNING",
            description=f"Model '{model_name}' MAE={perf['mae']:.1f} exceeds threshold={mae_alert_threshold}",
            metric_value=perf["mae"],
            threshold_value=mae_alert_threshold,
            metadata=perf,
        )

    # --- Step 2: Feature drift ---
    log.info("Detecting feature drift")
    today = date.today()
    baseline_end = (today - timedelta(days=current_days + 1)).isoformat()
    baseline_start = (today - timedelta(days=current_days + baseline_days + 1)).isoformat()
    current_start = (today - timedelta(days=current_days)).isoformat()
    current_end = today.isoformat()

    drift_results = detect_feature_drift(
        baseline_start=baseline_start,
        baseline_end=baseline_end,
        current_start=current_start,
        current_end=current_end,
        feature_version=feature_version,
    )
    n_stored = store_drift_metrics(drift_results)
    summary["drift_features_evaluated"] = len(drift_results)
    summary["drift_metrics_stored"] = n_stored

    drifted = [r for r in drift_results if r["drift_status"] == "SIGNIFICANT"]
    if drifted:
        raise_alert(
            alert_type="DATA_DRIFT",
            severity="WARNING",
            description=f"{len(drifted)} features with significant drift detected",
            metric_value=max(r["psi"] for r in drifted),
            threshold_value=psi_alert_threshold,
            metadata={"drifted_features": [r["feature_name"] for r in drifted]},
        )

    summary["status"] = "SUCCESS"
    return summary

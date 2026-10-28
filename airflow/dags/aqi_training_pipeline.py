"""Stage B: AQI Training Pipeline DAG.

Runs the full ML training pipeline:
  1. Build / refresh features for all configured cities
  2. Train regression model (GBM → next-day AQI value)
  3. Train classification model (GBM → AQI category)
  4. Auto-promote winning models to MLflow Production stage
  5. Trigger a batch prediction run for all cities
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

from airflow.decorators import dag, task


@dag(
    dag_id="aqi_training_pipeline",
    description="Stage B: Feature engineering → model training → MLflow registration → batch predictions",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",           # retrain every Sunday; can be triggered manually too
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["aqi", "stage-b", "training", "mlops"],
    doc_md="""
    # AQI Training Pipeline

    Triggered weekly (or manually). Performs:
    1. Feature build from analytics marts (lag, rolling, calendar, weather features)
    2. GBM regression model training with MLflow tracking
    3. GBM classification model training with MLflow tracking
    4. Auto-promotion: candidate replaces Production if MAE improves
    5. Batch predictions for yesterday across all configured cities
    """,
)
def aqi_training_pipeline():

    @task
    def build_and_store_features(**context) -> dict:
        from airflow.exceptions import AirflowException
        from aqi_mlops.features import build_all_city_features, store_features

        params = context.get("params") or {}
        window_days = int(params.get("window_days", 365))
        feature_version = params.get("feature_version", "v1")

        df = build_all_city_features(window_days=window_days)
        if df.empty:
            raise AirflowException("No AQI features were built; check ingestion and analytics marts")

        stored = store_features(df, feature_version=feature_version)
        return {
            "features_stored": stored,
            "cities": df["city_id"].nunique(),
            "date_min": str(df["date_utc"].min()),
            "date_max": str(df["date_utc"].max()),
            "status": "SUCCESS",
        }

    @task
    def train_regressor(feature_summary: dict, **context) -> dict:
        if feature_summary.get("status") == "SKIPPED":
            return {"status": "SKIPPED", "reason": "no_features"}

        from airflow.exceptions import AirflowFailException
        from aqi_mlops.features import load_features
        from aqi_mlops.training import train_regressor as _train

        params = context.get("params") or {}
        feature_version = params.get("feature_version", "v1")
        df = load_features(feature_version=feature_version)
        if df.empty:
            raise RuntimeError("No stored features are available for regression training")

        result = _train(df)
        if result.get("status") == "SKIPPED":
            raise AirflowFailException(
                f"Regressor training skipped: {result.get('reason', 'unknown reason')}"
            )
        return result

    @task
    def train_classifier(feature_summary: dict, **context) -> dict:
        if feature_summary.get("status") == "SKIPPED":
            return {"status": "SKIPPED", "reason": "no_features"}

        from airflow.exceptions import AirflowFailException
        from aqi_mlops.features import load_features
        from aqi_mlops.training import train_classifier as _train

        params = context.get("params") or {}
        feature_version = params.get("feature_version", "v1")
        df = load_features(feature_version=feature_version)
        if df.empty:
            raise RuntimeError("No stored features are available for classification training")

        result = _train(df)
        if result.get("status") == "SKIPPED":
            raise AirflowFailException(
                f"Classifier training skipped: {result.get('reason', 'unknown reason')}"
            )
        return result

    @task
    def promote_regressor(reg_result: dict) -> dict:
        """Auto-promote regressor if it beats current Production on MAE."""
        run_id = reg_result.get("run_id")
        if not run_id:
            return {"promoted": False, "reason": "no_run_id"}

        import mlflow
        from aqi_mlops.registry import compare_and_promote
        from aqi_mlops.training import REGRESSOR_NAME
        from mlflow.tracking import MlflowClient
        import os

        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient()

        versions = client.get_latest_versions(REGRESSOR_NAME, stages=["None"])
        if not versions:
            return {"promoted": False, "reason": "no_new_version_found"}

        version = sorted(versions, key=lambda v: int(v.version))[-1].version
        promoted = compare_and_promote(REGRESSOR_NAME, version, metric_key="mae", lower_is_better=True)
        return {"promoted": promoted, "version": version}

    @task
    def batch_predict(**context) -> dict:
        """Run batch predictions for all cities for today's date."""
        from datetime import date
        from aqi_pipeline.utils.config import load_cities
        from aqi_pipeline.utils.db import get_engine
        from aqi_mlops.registry import (
            get_latest_model_version,
            get_model_feature_names,
            load_production_model,
            select_model_features,
        )
        from aqi_mlops.features import build_features_for_city
        from aqi_mlops.training import REGRESSOR_NAME, CLASSIFIER_NAME
        from sqlalchemy import text
        import pandas as pd

        engine = get_engine()
        as_of = date.today()
        cities = load_cities()

        reg_result = load_production_model(REGRESSOR_NAME)
        clf_result = load_production_model(CLASSIFIER_NAME)
        if reg_result is None:
            raise RuntimeError("No production regressor is available for batch prediction")

        reg_version_uri = get_latest_model_version(REGRESSOR_NAME, "Production")
        reg_version = reg_version_uri.split("/")[-1] if reg_version_uri else "N/A"
        clf_version_uri = get_latest_model_version(CLASSIFIER_NAME, "Production")
        clf_version = clf_version_uri.split("/")[-1] if clf_version_uri else "N/A"
        reg_feature_names = get_model_feature_names(reg_version_uri) if reg_version_uri else []
        clf_feature_names = get_model_feature_names(clf_version_uri) if clf_version_uri else []

        category_labels = {1: "Good", 2: "Satisfactory", 3: "Moderate", 4: "Poor", 5: "Very Poor", 6: "Severe"}
        predictions = []

        for city in cities:
            try:
                df = build_features_for_city(city["id"], window_days=30)
                if df.empty:
                    continue

                df["date_utc"] = pd.to_datetime(df["date_utc"])
                match = df[df["date_utc"].dt.date <= as_of]
                if match.empty:
                    continue

                reg_features = select_model_features(match, reg_feature_names)
                X = reg_features.fillna(reg_features.median()).iloc[[-1]]

                predicted_aqi = float(reg_result.predict(X)[0]) if reg_result else None
                predicted_category = None
                if clf_result and predicted_aqi is not None:
                    clf_features = select_model_features(match, clf_feature_names)
                    clf_input = clf_features.fillna(clf_features.median()).iloc[[-1]]
                    cat_code = int(clf_result.predict(clf_input)[0])
                    predicted_category = category_labels.get(cat_code)

                if predicted_aqi is not None and predicted_category is None:
                    if predicted_aqi <= 50:
                        predicted_category = "Good"
                    elif predicted_aqi <= 100:
                        predicted_category = "Satisfactory"
                    elif predicted_aqi <= 200:
                        predicted_category = "Moderate"
                    elif predicted_aqi <= 300:
                        predicted_category = "Poor"
                    elif predicted_aqi <= 400:
                        predicted_category = "Very Poor"
                    else:
                        predicted_category = "Severe"

                predictions.append({
                    "city_id": city["id"],
                    "as_of": as_of.isoformat(),
                    "predicted_aqi": predicted_aqi,
                    "predicted_category": predicted_category,
                })
            except Exception as exc:
                import logging
                logging.getLogger(__name__).exception("Batch predict failed for %s: %s", city["id"], exc)

        # Persist to analytics.predictions
        from datetime import timedelta
        if predictions:
            sql = text("""
                INSERT INTO analytics.predictions
                    (city_id, as_of_date, prediction_date, predicted_aqi,
                     predicted_category, model_name, model_version, prediction_type)
                VALUES
                    (:city_id, :as_of_date, :prediction_date, :predicted_aqi,
                     :predicted_category, :model_name, :model_version, 'batch')
                ON CONFLICT (city_id, as_of_date, model_name, prediction_type) DO UPDATE SET
                    predicted_aqi      = EXCLUDED.predicted_aqi,
                    predicted_category = EXCLUDED.predicted_category,
                    model_version      = EXCLUDED.model_version,
                    created_at         = NOW()
            """)
            rows = [
                {
                    "city_id": p["city_id"],
                    "as_of_date": p["as_of"],
                    "prediction_date": (as_of + timedelta(days=1)).isoformat(),
                    "predicted_aqi": p["predicted_aqi"],
                    "predicted_category": p["predicted_category"],
                    "model_name": REGRESSOR_NAME,
                    "model_version": reg_version,
                }
                for p in predictions
            ]
            with engine.begin() as conn:
                conn.execute(sql, rows)
        else:
            raise RuntimeError("No city predictions were generated; check feature availability")

        return {"cities_predicted": len(predictions), "as_of_date": as_of.isoformat()}

    # DAG wiring
    features = build_and_store_features()
    reg = train_regressor(features)
    clf = train_classifier(features)
    promote = promote_regressor(reg)
    batch = batch_predict()

    # Batch prediction runs after both models are trained and regressor is promoted
    [promote, clf] >> batch


aqi_training_pipeline()

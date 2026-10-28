"""Model training pipeline — regression (AQI value) + classification (AQI category)."""

from __future__ import annotations

import os
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from aqi_pipeline.utils.logging import get_logger

try:
    import mlflow
    import mlflow.sklearn
except ImportError:
    mlflow = None  # type: ignore

log = get_logger(__name__)

REGRESSOR_NAME = "aqi_regressor"
CLASSIFIER_NAME = "aqi_classifier"
FEATURE_VERSION = os.environ.get("FEATURE_VERSION", "v1")
TEST_SIZE = float(os.environ.get("TRAIN_TEST_SPLIT", "0.2"))
RANDOM_STATE = 42

# Columns that are NOT features (meta + targets)
NON_FEATURE_COLS = {
    "city_id", "date_utc", "target_aqi", "target_category",
    "aqi_category", "dominant_pollutant",
}


def _get_feature_cols(df: pd.DataFrame) -> list[str]:
    """Return numeric feature column names, excluding meta/target columns."""
    return [
        c for c in df.columns
        if c not in NON_FEATURE_COLS and pd.api.types.is_numeric_dtype(df[c])
    ]


def _prepare_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Split a feature DataFrame into X, y_reg, y_clf.

    Returns:
        X: feature matrix (numeric only, NaN-filled)
        y_reg: regression target (target_aqi)
        y_clf: classification target (target_category ordinal int 1-6)
    """
    feature_cols = _get_feature_cols(df)
    X = df[feature_cols]
    y_reg = df["target_aqi"]

    category_map = {
        "Good": 1,
        "Satisfactory": 2,
        "Moderate": 3,
        "Poor": 4,
        "Very Poor": 5,
        "Severe": 6,
    }
    y_clf = df["target_category"].map(category_map).fillna(0).astype(int)
    return X, y_reg, y_clf


def _chronological_order(df: pd.DataFrame) -> pd.DataFrame:
    """Order every city's observations by time before splitting train and test."""
    sort_cols = [column for column in ("date_utc", "city_id") if column in df.columns]
    if not sort_cols:
        raise ValueError("Training data must include date_utc for a chronological split")
    return df.sort_values(sort_cols, kind="stable").reset_index(drop=True)


def train_regressor(
    df: pd.DataFrame,
    mlflow_tracking_uri: str | None = None,
) -> dict[str, Any]:
    """
    Train a GradientBoostingRegressor on AQI regression.

    Uses time-based train/test split to respect temporal ordering.
    Logs experiment to MLflow and registers the best model.

    Returns a summary dict with run_id, metrics, feature importances, etc.
    """
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import RobustScaler
    from sklearn.pipeline import Pipeline
    global mlflow
    if mlflow is None:
        import mlflow  # type: ignore
        import mlflow.sklearn  # type: ignore

    tracking_uri = mlflow_tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(REGRESSOR_NAME)

    df = _chronological_order(df.dropna(subset=["target_aqi"]))
    X, y_reg, _ = _prepare_data(df)

    # Time-based split
    split_idx = int(len(df) * (1 - TEST_SIZE))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y_reg.iloc[:split_idx], y_reg.iloc[split_idx:]

    if len(X_train) < 30:
        log.warning("Insufficient training data (%d rows); training aborted", len(X_train))
        return {"status": "SKIPPED", "reason": "insufficient data"}

    params = {
        "n_estimators": int(os.environ.get("REG_N_ESTIMATORS", "200")),
        "max_depth": int(os.environ.get("REG_MAX_DEPTH", "5")),
        "learning_rate": float(os.environ.get("REG_LEARNING_RATE", "0.05")),
        "min_samples_split": 5,
        "random_state": RANDOM_STATE,
    }

    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", RobustScaler()),
        ("gbr", GradientBoostingRegressor(**params)),
    ])

    with mlflow.start_run(run_name=f"regressor_{date.today()}") as run:
        mlflow.log_params(params)
        mlflow.log_param("feature_version", FEATURE_VERSION)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("n_features", len(X.columns))

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mae = mean_absolute_error(y_test, y_pred)
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        r2 = r2_score(y_test, y_pred)

        mlflow.log_metrics({"mae": mae, "rmse": rmse, "r2": r2})

        # Feature importances (from the underlying GBR)
        importances = dict(zip(
            X.columns,
            model.named_steps["gbr"].feature_importances_,
        ))
        # Log top-10 as metrics
        top10 = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:10]
        mlflow.log_metrics({f"fi_{k}": float(v) for k, v in top10})

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=REGRESSOR_NAME,
        )

        run_id = run.info.run_id

    log.info("Regressor training complete — MAE=%.2f, RMSE=%.2f, R2=%.4f", mae, rmse, r2)
    return {
        "run_id": run_id,
        "model_name": REGRESSOR_NAME,
        "metrics": {"mae": mae, "rmse": rmse, "r2": r2},
        "feature_importances": dict(top10),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
    }


def train_classifier(
    df: pd.DataFrame,
    mlflow_tracking_uri: str | None = None,
) -> dict[str, Any]:
    """
    Train a GradientBoostingClassifier for AQI category prediction.

    Returns summary dict.
    """
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import classification_report, f1_score, accuracy_score
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import RobustScaler
    from sklearn.pipeline import Pipeline
    global mlflow
    if mlflow is None:
        import mlflow  # type: ignore
        import mlflow.sklearn  # type: ignore

    tracking_uri = mlflow_tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(CLASSIFIER_NAME)

    df = _chronological_order(df.dropna(subset=["target_aqi"]))
    X, _, y_clf = _prepare_data(df)

    # Filter out rows where label is 0 (unknown category)
    mask = y_clf > 0
    X = X[mask]
    y_clf = y_clf[mask]

    split_idx = int(len(X) * (1 - TEST_SIZE))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y_clf.iloc[:split_idx], y_clf.iloc[split_idx:]

    if len(X_train) < 30:
        log.warning("Insufficient classifier training data (%d rows); skipping", len(X_train))
        return {"status": "SKIPPED", "reason": "insufficient data"}

    params = {
        "n_estimators": int(os.environ.get("CLF_N_ESTIMATORS", "150")),
        "max_depth": int(os.environ.get("CLF_MAX_DEPTH", "4")),
        "learning_rate": float(os.environ.get("CLF_LEARNING_RATE", "0.05")),
        "random_state": RANDOM_STATE,
    }

    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", RobustScaler()),
        ("gbc", GradientBoostingClassifier(**params)),
    ])

    with mlflow.start_run(run_name=f"classifier_{date.today()}") as run:
        mlflow.log_params(params)
        mlflow.log_param("feature_version", FEATURE_VERSION)
        mlflow.log_param("train_size", len(X_train))

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        f1 = f1_score(y_test, y_pred, average="macro", zero_division=0)
        acc = accuracy_score(y_test, y_pred)
        mlflow.log_metrics({"f1_macro": f1, "accuracy": acc})

        report = classification_report(y_test, y_pred, zero_division=0)
        mlflow.log_text(report, "classification_report.txt")

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=CLASSIFIER_NAME,
        )

        run_id = run.info.run_id

    log.info("Classifier training complete — F1=%.4f, Accuracy=%.4f", f1, acc)
    return {
        "run_id": run_id,
        "model_name": CLASSIFIER_NAME,
        "metrics": {"f1_macro": f1, "accuracy": acc},
        "train_rows": len(X_train),
        "test_rows": len(X_test),
    }


def run_training_pipeline(
    window_days: int = 365,
    feature_version: str = FEATURE_VERSION,
    auto_promote: bool = True,
    mlflow_tracking_uri: str | None = None,
) -> dict[str, Any]:
    """
    End-to-end training pipeline:
    1. Build / refresh features for all cities
    2. Store features to DB
    3. Train regression model
    4. Train classification model
    5. Auto-promote models that outperform Production

    Returns a summary dictionary.
    """
    from aqi_mlops.features import build_all_city_features, store_features, load_features
    from aqi_mlops.registry import compare_and_promote

    summary: dict[str, Any] = {}

    log.info("Step 1/4: Building features (window=%d days)", window_days)
    feature_df = build_all_city_features(window_days=window_days)
    if feature_df.empty:
        log.warning("No features available; aborting training pipeline")
        return {"status": "SKIPPED", "reason": "no_features"}

    log.info("Step 2/4: Storing %d feature rows to DB", len(feature_df))
    stored = store_features(feature_df, feature_version=feature_version)
    summary["features_stored"] = stored

    log.info("Step 3/4: Training regressor")
    reg_result = train_regressor(feature_df, mlflow_tracking_uri=mlflow_tracking_uri)
    summary["regressor"] = reg_result

    log.info("Step 4/4: Training classifier")
    clf_result = train_classifier(feature_df, mlflow_tracking_uri=mlflow_tracking_uri)
    summary["classifier"] = clf_result

    if auto_promote and reg_result.get("run_id"):
        try:
            # Get the version just registered (latest None-stage)
            tracking_uri = mlflow_tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
            import mlflow
            mlflow.set_tracking_uri(tracking_uri)
            from mlflow.tracking import MlflowClient
            client = MlflowClient()
            reg_versions = client.get_latest_versions(REGRESSOR_NAME, stages=["None"])
            if reg_versions:
                v = sorted(reg_versions, key=lambda x: int(x.version))[-1].version
                promoted = compare_and_promote(REGRESSOR_NAME, v, metric_key="mae", lower_is_better=True)
                summary["regressor_promoted"] = promoted
        except Exception as exc:
            log.exception("Auto-promote failed for regressor: %s", exc)
            summary["regressor_promoted"] = False

    summary["status"] = "SUCCESS"
    return summary

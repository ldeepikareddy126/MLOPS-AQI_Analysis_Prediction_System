"""Model registry helpers — MLflow-backed model lifecycle management."""

from __future__ import annotations

import os
from typing import Any

from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")


def _mlflow_client():
    """Return a configured MlflowClient."""
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    return MlflowClient()


def register_model(
    run_id: str,
    artifact_path: str,
    model_name: str,
    tags: dict[str, str] | None = None,
) -> str:
    """
    Register an MLflow model artifact as a new version in the model registry.

    Returns the registered model version string (e.g. '3').
    """
    import mlflow
    from mlflow.exceptions import MlflowException

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"runs:/{run_id}/{artifact_path}"

    try:
        mv = mlflow.register_model(model_uri, model_name, tags=tags)
        log.info(
            "Registered model '%s' version %s from run %s",
            model_name, mv.version, run_id,
        )
        return mv.version
    except MlflowException as exc:
        log.exception("Model registration failed: %s", exc)
        raise


def get_latest_model_version(
    model_name: str,
    stage: str = "Production",
) -> str | None:
    """
    Return the latest model version URI at the given stage, or None.

    Example: 'models:/aqi_regressor/3'
    """
    client = _mlflow_client()
    try:
        versions = client.get_latest_versions(model_name, stages=[stage])
        if not versions:
            versions = client.get_latest_versions(model_name, stages=["None"])
        if not versions:
            return None
        mv = sorted(versions, key=lambda v: int(v.version))[-1]
        return f"models:/{model_name}/{mv.version}"
    except Exception as exc:
        log.warning("Could not load model '%s' at stage '%s': %s", model_name, stage, exc)
        return None


def transition_model_stage(
    model_name: str,
    version: str,
    new_stage: str,
    archive_existing: bool = True,
) -> None:
    """
    Transition a registered model version to a new stage.

    Stages: 'Staging', 'Production', 'Archived', 'None'
    """
    client = _mlflow_client()
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=new_stage,
        archive_existing_versions=archive_existing,
    )
    log.info("Model '%s' v%s transitioned to stage '%s'", model_name, version, new_stage)


def load_production_model(model_name: str, stage: str = "Production") -> Any:
    """
    Load the latest model at the given stage using mlflow.pyfunc.

    Returns an mlflow.pyfunc.PyFuncModel or None if not available.
    """
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    uri = get_latest_model_version(model_name, stage=stage)
    if not uri:
        log.warning("No model found for '%s' at stage '%s'", model_name, stage)
        return None
    try:
        model = mlflow.pyfunc.load_model(uri)
        log.info("Loaded model from %s", uri)
        return model
    except Exception as exc:
        log.exception("Failed to load model from %s: %s", uri, exc)
        return None


def get_model_feature_names(model_uri: str) -> list[str]:
    """Return the input feature order recorded by a registered scikit-learn model."""
    import mlflow
    import mlflow.sklearn

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model = mlflow.sklearn.load_model(model_uri)
    feature_names = getattr(model, "feature_names_in_", None)
    if feature_names is None:
        raise ValueError(f"Model {model_uri!r} does not expose feature names")
    return [str(name) for name in feature_names]


def select_model_features(features: Any, feature_names: list[str]) -> Any:
    """Select model inputs in their training order and report missing columns."""
    missing = [name for name in feature_names if name not in features.columns]
    if missing:
        raise ValueError(f"Model input features are missing: {missing}")
    return features.loc[:, feature_names]


def log_model_to_mlflow(
    model: Any,
    model_name: str,
    artifact_path: str,
    metrics: dict[str, float],
    params: dict[str, Any],
    run_name: str | None = None,
) -> tuple[str, str]:
    """
    Log a scikit-learn model to MLflow with metrics and params.

    Returns (run_id, version) after registration.
    """
    import mlflow
    import mlflow.sklearn

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    experiment = mlflow.set_experiment(model_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name) as run:
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(
            model,
            artifact_path=artifact_path,
            registered_model_name=model_name,
        )
        run_id = run.info.run_id

    # Get the just-registered version
    client = _mlflow_client()
    versions = client.get_latest_versions(model_name, stages=["None"])
    version = sorted(versions, key=lambda v: int(v.version))[-1].version if versions else "1"
    log.info("Logged model '%s' v%s (run_id=%s)", model_name, version, run_id)
    return run_id, version


def compare_and_promote(
    model_name: str,
    candidate_version: str,
    metric_key: str = "mae",
    lower_is_better: bool = True,
) -> bool:
    """
    Compare a candidate model version against current Production.

    If candidate improves on `metric_key`, promote it to Production and archive
    the old Production version. Returns True if promotion happened.
    """
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = _mlflow_client()

    # Get candidate metric
    candidate_mv = client.get_model_version(model_name, candidate_version)
    cand_run = client.get_run(candidate_mv.run_id)
    cand_metric = cand_run.data.metrics.get(metric_key)
    if cand_metric is None:
        log.warning("Candidate version %s has no metric '%s'; skipping promotion", candidate_version, metric_key)
        return False

    # Get current Production
    prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    if not prod_versions:
        # No current Production — just promote candidate
        transition_model_stage(model_name, candidate_version, "Production")
        log.info("No existing Production model; promoted v%s", candidate_version)
        return True

    prod_mv = prod_versions[0]
    prod_run = client.get_run(prod_mv.run_id)
    prod_metric = prod_run.data.metrics.get(metric_key)

    if prod_metric is None:
        # Can't compare — promote candidate
        transition_model_stage(model_name, candidate_version, "Production", archive_existing=True)
        return True

    improved = cand_metric < prod_metric if lower_is_better else cand_metric > prod_metric
    if improved:
        log.info(
            "Candidate v%s (%s=%.4f) beats Production v%s (%s=%.4f); promoting",
            candidate_version, metric_key, cand_metric,
            prod_mv.version, metric_key, prod_metric,
        )
        transition_model_stage(model_name, candidate_version, "Production", archive_existing=True)
        return True
    else:
        log.info(
            "Candidate v%s (%s=%.4f) does NOT beat Production v%s (%s=%.4f); keeping existing",
            candidate_version, metric_key, cand_metric,
            prod_mv.version, metric_key, prod_metric,
        )
        transition_model_stage(model_name, candidate_version, "Archived", archive_existing=False)
        return False

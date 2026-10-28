"""FastAPI inference server for AQI prediction."""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from typing import Any
from uuid import uuid4

import pandas as pd
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from aqi_pipeline.utils.logging import get_logger

log = get_logger(__name__)

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
API_RELOAD_TOKEN = os.environ.get("API_RELOAD_TOKEN", "changeme")
API_PORT = int(os.environ.get("API_PORT", "8000"))

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AQI Prediction API",
    description="Real-time AQI prediction for Indian cities using trained MLflow models.",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-process model cache
_model_cache: dict[str, tuple[Any, str, list[str]]] = {}


def _log_api_request(
    request_id: str,
    endpoint: str,
    method: str,
    status_code: int,
    latency_ms: float,
    error_type: str | None,
) -> None:
    """Persist /predict request health without storing request payloads."""
    from aqi_pipeline.utils.db import get_engine
    from sqlalchemy import text

    try:
        with get_engine().begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO monitoring.api_request_log
                        (request_id, endpoint, method, status_code, latency_ms, error_type)
                    VALUES (:request_id, :endpoint, :method, :status_code, :latency_ms, :error_type)
                """),
                {
                    "request_id": request_id,
                    "endpoint": endpoint,
                    "method": method,
                    "status_code": status_code,
                    "latency_ms": latency_ms,
                    "error_type": error_type,
                },
            )
    except SQLAlchemyError:
        log.exception("Failed to persist API request health record")


@app.middleware("http")
async def monitor_prediction_requests(request: Request, call_next):
    if request.method != "POST" or request.url.path != "/predict":
        return await call_next(request)

    started = time.perf_counter()
    status_code = 500
    error_type = None
    try:
        response = await call_next(request)
        status_code = response.status_code
        if status_code >= 500:
            error_type = "HTTP_5XX"
        elif status_code >= 400:
            error_type = "HTTP_4XX"
        return response
    except Exception as exc:
        error_type = type(exc).__name__
        raise
    finally:
        latency_ms = (time.perf_counter() - started) * 1000
        await run_in_threadpool(
            _log_api_request,
            str(uuid4()),
            request.url.path,
            request.method,
            status_code,
            latency_ms,
            error_type,
        )


def _get_model(model_name: str):
    """Load (and cache) an MLflow model."""
    if model_name in _model_cache:
        return _model_cache[model_name]
    try:
        import mlflow
        import mlflow.sklearn

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        from mlflow.tracking import MlflowClient

        client = MlflowClient()
        versions = client.get_latest_versions(model_name, stages=["Production"])
        if not versions:
            versions = client.get_latest_versions(model_name, stages=["None"])
        if not versions:
            return None
        mv = sorted(versions, key=lambda v: int(v.version))[-1]
        uri = f"models:/{model_name}/{mv.version}"
        model = mlflow.sklearn.load_model(uri)
        feature_names = getattr(model, "feature_names_in_", None)
        if feature_names is None:
            raise ValueError(f"Model {uri!r} does not expose feature names")
        feature_names = [str(name) for name in feature_names]
        _model_cache[model_name] = (model, mv.version, feature_names)
        log.info("Loaded model '%s' v%s", model_name, mv.version)
        return _model_cache[model_name]
    except Exception as exc:
        if getattr(exc, "error_code", None) == "RESOURCE_DOES_NOT_EXIST":
            log.info("Model '%s' is not registered yet", model_name)
        else:
            log.exception("Failed to load model '%s': %s", model_name, exc)
        return None


def _prepare_model_input(model: Any, features: pd.DataFrame) -> pd.DataFrame:
    """Use training-fitted preprocessing, with a median fallback for legacy models."""
    if not features.isna().any().any() or "imputer" in getattr(model, "named_steps", {}):
            return features

    scaler = getattr(model, "named_steps", {}).get("scaler")
    centers = getattr(scaler, "center_", None)
    model_features = getattr(model, "feature_names_in_", None)
    if centers is None or model_features is None or len(centers) != len(model_features):
            raise ValueError("Model has no training-derived imputation values for missing features")

    medians = pd.Series(centers, index=[str(name) for name in model_features])
    missing_columns = features.columns[features.isna().any()]
    unusable = [name for name in missing_columns if name not in medians or pd.isna(medians[name])]
    if unusable:
            raise ValueError(f"Model has no training-derived imputation value for: {unusable}")
    return features.fillna(medians)


def _build_feature_row(city_id: str, as_of_date: date) -> pd.DataFrame | None:
    """
    Build a single feature row for the given city and date.
    Uses stored features from analytics.features_daily when available;
    falls back to on-the-fly computation.
    """
    from aqi_pipeline.utils.db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    sql = text("""
        SELECT features_json
        FROM analytics.features_daily
        WHERE city_id = :city_id AND date_utc = :date_utc
        ORDER BY built_at DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"city_id": city_id, "date_utc": as_of_date.isoformat()}).fetchone()

    if row:
        import json

        data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        return pd.DataFrame([data])

    # Fallback: compute on the fly
    try:
        from aqi_mlops.features import build_features_for_city

        df = build_features_for_city(city_id, window_days=30)
        if df.empty:
            return None
        # Pick the row closest to as_of_date
        df["date_utc"] = pd.to_datetime(df["date_utc"])
        match = df[df["date_utc"].dt.date <= as_of_date]
        if match.empty:
            return None
        row_data = match.sort_values("date_utc").iloc[[-1]]
        from aqi_mlops.training import _get_feature_cols, NON_FEATURE_COLS

        feat_cols = _get_feature_cols(row_data)
        return row_data[feat_cols].fillna(row_data[feat_cols].median())
    except Exception as exc:
        log.exception("Fallback feature build failed for %s/%s: %s", city_id, as_of_date, exc)
        return None


def _log_prediction(
    prediction_id: str,
    city_id: str,
    as_of_date: date,
    predicted_aqi: float | None,
    predicted_category: str | None,
    model_name: str,
    model_version: str,
    latency_ms: float,
    input_features: dict | None = None,
    client_ip: str | None = None,
) -> None:
    """Write prediction to monitoring.prediction_log and analytics.predictions."""
    from aqi_pipeline.utils.db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    import json

    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO monitoring.prediction_log
                        (prediction_id, model_name, model_version, city_id, as_of_date,
                         predicted_aqi, predicted_category, latency_ms, input_features, client_ip)
                    VALUES (:pid, :mname, :mver, :city, :date,
                            :aqi, :cat, :lat, :feat, :ip)
                """),
                {
                    "pid": prediction_id,
                    "mname": model_name,
                    "mver": model_version,
                    "city": city_id,
                    "date": as_of_date.isoformat(),
                    "aqi": predicted_aqi,
                    "cat": predicted_category,
                    "lat": latency_ms,
                    "feat": json.dumps(input_features or {}),
                    "ip": client_ip,
                },
            )
            conn.execute(
                text("""
                    INSERT INTO analytics.predictions
                        (city_id, as_of_date, prediction_date, predicted_aqi,
                         predicted_category, model_name, model_version, prediction_type)
                    VALUES (:city, :as_of, :pred_date, :aqi, :cat, :mname, :mver, 'api')
                    ON CONFLICT (city_id, as_of_date, model_name, prediction_type) DO UPDATE SET
                        predicted_aqi      = EXCLUDED.predicted_aqi,
                        predicted_category = EXCLUDED.predicted_category,
                        model_version      = EXCLUDED.model_version,
                        created_at         = NOW()
                """),
                {
                    "city": city_id,
                    "as_of": as_of_date.isoformat(),
                    "pred_date": (as_of_date + timedelta(days=1)).isoformat(),
                    "aqi": predicted_aqi,
                    "cat": predicted_category,
                    "mname": model_name,
                    "mver": model_version,
                },
            )
    except Exception as exc:
        log.exception("Failed to log prediction: %s", exc)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    city_id: str = Field(..., description="City identifier (e.g. 'delhi', 'mumbai')")
    as_of_date: str = Field(
        default_factory=lambda: date.today().isoformat(),
        description="ISO date string (YYYY-MM-DD); features available up to this date",
    )

    model_config = {"json_schema_extra": {"example": {"city_id": "delhi", "as_of_date": "2025-01-15"}}}


class PredictResponse(BaseModel):
    model_config = {"protected_namespaces": ()}

    prediction_id: str
    city_id: str
    as_of_date: str
    prediction_date: str
    predicted_aqi: float | None
    predicted_category: str | None
    model_name: str
    model_version: str
    latency_ms: float


class HealthResponse(BaseModel):
    status: str
    regressor_loaded: bool
    classifier_loaded: bool
    timestamp: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["meta"])
def health_check():
    """Service liveness probe."""
    return HealthResponse(
        status="ok",
        regressor_loaded="aqi_regressor" in _model_cache,
        classifier_loaded="aqi_classifier" in _model_cache,
        timestamp=pd.Timestamp.utcnow().isoformat(),
    )


@app.get("/ready", tags=["meta"])
def readiness_check():
    """Service readiness probe; requires a registered AQI regressor."""
    if _get_model("aqi_regressor") is None:
        raise HTTPException(status_code=503, detail="Regressor model not available")
    return {"status": "ready"}


@app.post("/predict", response_model=PredictResponse, tags=["prediction"])
def predict(req: PredictRequest, x_forwarded_for: str | None = Header(default=None)):
    """
    Predict next-day AQI for a city given features available as of `as_of_date`.

    Returns predicted_aqi (regression) and predicted_category (classification).
    """
    t0 = time.perf_counter()

    try:
        as_of = date.fromisoformat(req.as_of_date)
    except ValueError:
        raise HTTPException(status_code=422, detail="as_of_date must be ISO format YYYY-MM-DD")

    # Load models
    reg_result = _get_model("aqi_regressor")
    clf_result = _get_model("aqi_classifier")

    if not reg_result:
        raise HTTPException(status_code=503, detail="Regressor model not available")

    reg_model, reg_version, reg_feature_names = reg_result
    clf_model = clf_result[0] if clf_result else None
    clf_version = clf_result[1] if clf_result else "N/A"
    clf_feature_names = clf_result[2] if clf_result else []

    # Build feature row
    X = _build_feature_row(req.city_id, as_of)
    if X is None or X.empty:
        raise HTTPException(
            status_code=422,
            detail=f"No features available for city '{req.city_id}' as of {req.as_of_date}",
        )
    from aqi_mlops.registry import select_model_features

    raw_features = X
    try:
        reg_X = select_model_features(raw_features, reg_feature_names)
        reg_X = _prepare_model_input(reg_model, reg_X)
        clf_X = select_model_features(raw_features, clf_feature_names) if clf_model else None
        if clf_model is not None:
            clf_X = _prepare_model_input(clf_model, clf_X)
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=f"Model feature mismatch: {exc}") from exc

    # Regression prediction
    try:
        predicted_aqi = float(reg_model.predict(reg_X)[0])
    except Exception as exc:
        log.exception("Regressor prediction failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Prediction error: {exc}")

    # Classification prediction
    predicted_category = None
    category_labels = {1: "Good", 2: "Satisfactory", 3: "Moderate", 4: "Poor", 5: "Very Poor", 6: "Severe"}
    if clf_model:
        try:
            cat_code = int(clf_model.predict(clf_X)[0])
            predicted_category = category_labels.get(cat_code)
        except Exception as exc:
            log.warning("Classifier prediction failed (non-fatal): %s", exc)

    # Derive category from AQI value if classifier unavailable
    if predicted_category is None:
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

    latency_ms = (time.perf_counter() - t0) * 1000
    prediction_id = str(uuid4())

    _log_prediction(
        prediction_id=prediction_id,
        city_id=req.city_id,
        as_of_date=as_of,
        predicted_aqi=predicted_aqi,
        predicted_category=predicted_category,
        model_name="aqi_regressor",
        model_version=reg_version,
        latency_ms=latency_ms,
        input_features=raw_features.iloc[0].to_dict() if not raw_features.empty else {},
        client_ip=x_forwarded_for,
    )

    return PredictResponse(
        prediction_id=prediction_id,
        city_id=req.city_id,
        as_of_date=req.as_of_date,
        prediction_date=(as_of + timedelta(days=1)).isoformat(),
        predicted_aqi=round(predicted_aqi, 2),
        predicted_category=predicted_category,
        model_name="aqi_regressor",
        model_version=reg_version,
        latency_ms=round(latency_ms, 2),
    )


@app.get("/cities", tags=["meta"])
def list_cities():
    """List available cities for prediction."""
    from aqi_pipeline.utils.config import load_cities

    return {"cities": [{"id": c["id"], "name": c["name"]} for c in load_cities()]}


@app.post("/admin/reload-model", tags=["admin"])
@app.post("/reload", tags=["admin"])
def reload_models(x_reload_token: str = Header(...)):
    """Force reload of cached models (requires API_RELOAD_TOKEN header)."""
    if x_reload_token != API_RELOAD_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid reload token")
    _model_cache.clear()
    return {"status": "cache_cleared", "timestamp": pd.Timestamp.utcnow().isoformat()}


@app.get("/predictions/{city_id}", tags=["prediction"])
def get_predictions(city_id: str, days: int = 7):
    """Return recent predictions for a city."""
    from aqi_pipeline.utils.db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    sql = text("""
        SELECT city_id, as_of_date, prediction_date, predicted_aqi,
               predicted_category, actual_aqi, actual_category,
               model_name, model_version, created_at
        FROM analytics.predictions
        WHERE city_id = :city_id
          AND as_of_date >= :since
        ORDER BY as_of_date DESC
    """)
    since = (date.today() - timedelta(days=days)).isoformat()
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"city_id": city_id, "since": since})

    return {"city_id": city_id, "predictions": df.to_dict("records")}


if __name__ == "__main__":
    uvicorn.run("aqi_mlops.serving:app", host="0.0.0.0", port=API_PORT, reload=False)

"""
mlops/tests/test_serving.py
============================
Unit tests for the Stage B FastAPI serving module.

Uses FastAPI's TestClient so no live server is needed.
MLflow and DB calls are mocked.
"""
from __future__ import annotations

import os
import pytest


@pytest.fixture(autouse=True)
def isolate_request_telemetry(monkeypatch):
    """Keep API unit tests from writing synthetic traffic to the live warehouse."""
    from aqi_mlops import serving

    monkeypatch.setattr(serving, "_log_api_request", lambda *args, **kwargs: None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_app():
    """Import the FastAPI app with mocked external dependencies."""
    import unittest.mock as mock
    import importlib
    import sys

    # Patch get_engine before importing serving so no real DB call is made
    fake_engine = mock.MagicMock()
    sys.modules["aqi_pipeline.utils.db"] = mock.MagicMock(get_engine=mock.Mock(return_value=fake_engine))

    # Re-import to pick up the patches
    if "aqi_mlops.serving" in sys.modules:
        del sys.modules["aqi_mlops.serving"]

    from aqi_mlops.serving import app
    return app


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_returns_200(self):
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_response_schema(self):
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        client = TestClient(app)
        data = client.get("/health").json()
        assert "status" in data
        assert data["status"] == "ok"
        assert "regressor_loaded" in data
        assert "classifier_loaded" in data
        assert "timestamp" in data

    def test_health_models_not_loaded_by_default(self):
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app, _model_cache

        # Clear cache to ensure clean state
        _model_cache.clear()
        client = TestClient(app)
        data = client.get("/health").json()
        assert data["regressor_loaded"] is False
        assert data["classifier_loaded"] is False


class TestCitiesEndpoint:
    """Tests for GET /cities."""

    def test_cities_returns_200(self, monkeypatch):
        import unittest.mock as mock
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        monkeypatch.setattr(
            "aqi_mlops.serving.app.dependency_overrides",
            {},
        )

        # Mock load_cities
        mock_cities = [
            {"id": "delhi", "name": "Delhi"},
            {"id": "mumbai", "name": "Mumbai"},
        ]
        monkeypatch.setattr(
            "aqi_pipeline.utils.config.load_cities",
            mock.Mock(return_value=mock_cities),
        )

        client = TestClient(app)
        resp = client.get("/cities")
        assert resp.status_code == 200

    def test_cities_response_has_list(self, monkeypatch):
        import unittest.mock as mock
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        mock_cities = [{"id": "delhi", "name": "Delhi"}]
        monkeypatch.setattr(
            "aqi_pipeline.utils.config.load_cities",
            mock.Mock(return_value=mock_cities),
        )

        client = TestClient(app)
        data = client.get("/cities").json()
        assert "cities" in data
        assert isinstance(data["cities"], list)


class TestPredictEndpoint:
    """Tests for POST /predict."""

    def test_legacy_model_uses_scaler_training_medians_for_missing_features(self):
        import numpy as np
        import pandas as pd
        from aqi_mlops.serving import _prepare_model_input

        model = type("LegacyModel", (), {
            "named_steps": {"scaler": type("Scaler", (), {"center_": np.array([12.0, 4.0])})()},
            "feature_names_in_": np.array(["pm25_24h", "temperature_2m"]),
        })()
        features = pd.DataFrame({"pm25_24h": [np.nan], "temperature_2m": [25.0]})

        prepared = _prepare_model_input(model, features)

        assert prepared.loc[0, "pm25_24h"] == 12.0
        assert prepared.loc[0, "temperature_2m"] == 25.0

    def test_model_without_training_median_fails_on_missing_feature(self):
        import numpy as np
        import pandas as pd
        from aqi_mlops.serving import _prepare_model_input

        model = type("LegacyModel", (), {
            "named_steps": {"scaler": type("Scaler", (), {"center_": np.array([np.nan])})()},
            "feature_names_in_": np.array(["pm25_24h"]),
        })()
        features = pd.DataFrame({"pm25_24h": [np.nan]})

        with pytest.raises(ValueError, match="no training-derived imputation value"):
            _prepare_model_input(model, features)

    def test_predict_without_model_returns_503(self, monkeypatch):
        """When no model is loaded, /predict should return 503."""
        import unittest.mock as mock
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app, _model_cache

        _model_cache.clear()
        # Mock _get_model to return None (no model)
        monkeypatch.setattr("aqi_mlops.serving._get_model", mock.Mock(return_value=None))

        client = TestClient(app)
        resp = client.post(
            "/predict",
            json={"city_id": "delhi", "as_of_date": "2024-01-15"},
        )
        assert resp.status_code == 503

    def test_predict_invalid_date_format_returns_422(self):
        """Invalid ISO date should return 422."""
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        client = TestClient(app)
        resp = client.post(
            "/predict",
            json={"city_id": "delhi", "as_of_date": "not-a-date"},
        )
        assert resp.status_code == 422

    def test_predict_missing_city_returns_422(self):
        """Missing required field city_id → 422."""
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        client = TestClient(app)
        resp = client.post("/predict", json={"as_of_date": "2024-01-15"})
        assert resp.status_code == 422

    def test_prediction_request_health_is_logged(self, monkeypatch):
        import unittest.mock as mock
        from fastapi.testclient import TestClient
        from aqi_mlops import serving

        logged = mock.Mock()
        monkeypatch.setattr(serving, "_log_api_request", logged)
        client = TestClient(serving.app)
        response = client.post(
            "/predict",
            json={"city_id": "delhi", "as_of_date": "not-a-date"},
        )

        assert response.status_code == 422
        logged.assert_called_once()
        assert logged.call_args.args[3] == 422
        assert logged.call_args.args[5] == "HTTP_4XX"


class TestReloadEndpoint:
    """Tests for POST /reload."""

    def test_reload_requires_token(self):
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        client = TestClient(app)
        resp = client.post("/reload")  # No header
        assert resp.status_code == 422  # Missing required header

    def test_reload_wrong_token_returns_403(self):
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app

        client = TestClient(app)
        resp = client.post("/reload", headers={"x-reload-token": "wrong_token"})
        assert resp.status_code == 403

    def test_reload_valid_token_clears_cache(self, monkeypatch):
        import os
        import unittest.mock as mock
        from fastapi.testclient import TestClient
        from aqi_mlops.serving import app, _model_cache

        token = "test_token_123"
        monkeypatch.setenv("API_RELOAD_TOKEN", token)
        # Patch the module-level constant
        monkeypatch.setattr("aqi_mlops.serving.API_RELOAD_TOKEN", token)

        _model_cache["aqi_regressor"] = mock.MagicMock()
        client = TestClient(app)
        resp = client.post("/reload", headers={"x-reload-token": token})
        assert resp.status_code == 200
        assert len(_model_cache) == 0

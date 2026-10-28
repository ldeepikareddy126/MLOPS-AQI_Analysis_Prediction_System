"""MLOps unit tests — feature engineering, training, monitoring."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Feature module tests
# ---------------------------------------------------------------------------

class TestFeatureEngineering:
    """Tests for aqi_mlops.features."""

    def _make_sample_df(self, n: int = 50) -> pd.DataFrame:
        """Return a minimal feature DataFrame."""
        dates = pd.date_range("2024-01-01", periods=n, freq="D")
        return pd.DataFrame(
            {
                "city_id": "delhi",
                "date_utc": dates,
                "aqi": np.random.uniform(50, 300, n),
                "aqi_category": np.random.choice(
                    ["Good", "Moderate", "Poor", "Very Poor"], n
                ),
                "temperature_2m": np.random.uniform(10, 40, n),
                "relative_humidity_2m": np.random.uniform(30, 90, n),
                "target_aqi": np.random.uniform(50, 300, n),
                "target_category": np.random.choice(
                    ["Good", "Moderate", "Poor", "Very Poor"], n
                ),
            }
        )

    def test_store_and_load_features_roundtrip(self):
        """store_features should produce JSON that load_features can parse."""
        from aqi_mlops.features import store_features

        df = self._make_sample_df()
        # Monkeypatch DB call to avoid real connection
        with patch("aqi_mlops.features.get_engine") as mock_get_engine:
            mock_conn = MagicMock()
            mock_get_engine.return_value.begin.return_value.__enter__.return_value = mock_conn
            result = store_features(df, feature_version="test_v0")
        assert result == len(df)
        assert "CAST(:features_json AS JSONB)" in str(mock_conn.execute.call_args.args[0])

    def test_build_all_city_features_returns_dataframe_type(self):
        """build_all_city_features returns DataFrame (may be empty without DB)."""
        from aqi_mlops.features import build_all_city_features

        with patch("aqi_mlops.features.get_engine") as mock_eng, \
             patch("aqi_mlops.features.load_cities") as mock_cities:
            mock_cities.return_value = []
            result = build_all_city_features(window_days=30)
        assert isinstance(result, pd.DataFrame)

    def test_build_features_aggregates_station_data_without_ambiguous_columns(self):
        from aqi_mlops.features import build_features_for_city
        from datetime import date

        aqi_df = pd.DataFrame(
            {
                "date_utc": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "aqi": [80.0, 90.0],
                "aqi_category": ["Satisfactory", "Satisfactory"],
            }
        )
        with patch("aqi_mlops.features.date") as mock_date, \
             patch("aqi_mlops.features.get_engine") as mock_engine, \
             patch(
                 "aqi_mlops.features.pd.read_sql",
                 side_effect=[aqi_df, pd.DataFrame()],
             ) as mock_read_sql:
            mock_date.today.return_value = date(2025, 1, 2)
            mock_engine.return_value.connect.return_value.__enter__.return_value = MagicMock()
            result = build_features_for_city("delhi", window_days=30)

        latest_observed = result[result["date_utc"] == pd.Timestamp("2025-01-01")]
        assert len(latest_observed) == 1
        assert latest_observed.iloc[0]["target_aqi"] == 90.0
        assert pd.isna(result.loc[result["date_utc"] == pd.Timestamp("2025-01-02"), "target_aqi"]).all()
        query = str(mock_read_sql.call_args_list[0].args[0])
        assert "AVG(station.pm25_24h)" in query
        assert "GROUP BY city.date_utc" in query

    def test_next_day_targets_do_not_bridge_missing_calendar_dates(self):
        from aqi_mlops.features import _add_next_day_targets

        observations = pd.DataFrame(
            {
                "aqi": [100.0, 300.0],
                "aqi_category": ["Moderate", "Poor"],
            },
            index=pd.to_datetime(["2025-01-01", "2025-01-03"]),
        )
        observations = observations.reindex(
            pd.date_range("2025-01-01", "2025-01-03", freq="D")
        )

        labelled = _add_next_day_targets(observations)

        assert pd.isna(labelled.loc[pd.Timestamp("2025-01-01"), "target_aqi"])
        assert pd.isna(labelled.loc[pd.Timestamp("2025-01-03"), "target_aqi"])

    def test_build_all_city_features_does_not_suppress_query_errors(self):
        from aqi_mlops.features import build_all_city_features

        with patch(
            "aqi_mlops.features.load_cities",
            return_value=[{"id": "delhi"}],
        ), patch(
            "aqi_mlops.features.build_features_for_city",
            side_effect=RuntimeError("database query failed"),
        ):
            with pytest.raises(RuntimeError, match="database query failed"):
                build_all_city_features()

    def test_store_features_raises_on_missing_columns(self):
        """store_features should raise ValueError if required columns are missing."""
        from aqi_mlops.features import store_features

        bad_df = pd.DataFrame({"x": [1, 2, 3]})
        with patch("aqi_mlops.features.get_engine"):
            with pytest.raises(ValueError, match="missing columns"):
                store_features(bad_df)


# ---------------------------------------------------------------------------
# Monitoring / drift tests
# ---------------------------------------------------------------------------

class TestDriftDetection:
    """Tests for PSI and KS drift metrics."""

    def test_psi_identical_distributions_is_zero(self):
        from aqi_mlops.monitoring import compute_psi

        arr = np.random.normal(0, 1, 500)
        psi = compute_psi(arr, arr.copy())
        assert psi < 0.01  # essentially zero

    def test_psi_very_different_distributions_is_high(self):
        from aqi_mlops.monitoring import compute_psi

        expected = np.random.normal(0, 1, 1000)
        actual = np.random.normal(10, 1, 1000)
        psi = compute_psi(expected, actual)
        assert psi > 0.5  # clearly significant

    def test_psi_moderate_shift(self):
        from aqi_mlops.monitoring import compute_psi

        np.random.seed(42)
        expected = np.random.normal(100, 20, 1000)
        actual = np.random.normal(115, 22, 1000)  # small shift
        psi = compute_psi(expected, actual)
        assert 0.0 <= psi <= 2.0  # valid range

    def test_ks_identical_distributions(self):
        from aqi_mlops.monitoring import compute_ks

        arr = np.random.normal(0, 1, 500)
        stat, pval = compute_ks(arr, arr.copy())
        assert stat < 0.1
        assert pval > 0.01

    def test_ks_different_distributions(self):
        from aqi_mlops.monitoring import compute_ks

        expected = np.random.normal(0, 1, 1000)
        actual = np.random.normal(5, 1, 1000)
        stat, pval = compute_ks(expected, actual)
        assert stat > 0.3
        assert pval < 0.001

    def test_psi_empty_arrays_returns_nan(self):
        from aqi_mlops.monitoring import compute_psi

        psi = compute_psi(np.array([]), np.array([1.0, 2.0]))
        assert np.isnan(psi)

    def test_drift_status_thresholds(self):
        from aqi_mlops.monitoring import (
            PSI_MODERATE_THRESHOLD,
            PSI_SIGNIFICANT_THRESHOLD,
        )

        assert PSI_MODERATE_THRESHOLD < PSI_SIGNIFICANT_THRESHOLD
        assert PSI_MODERATE_THRESHOLD == pytest.approx(0.1)
        assert PSI_SIGNIFICANT_THRESHOLD == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# Training module tests
# ---------------------------------------------------------------------------

class TestTrainingHelpers:
    """Tests for training helper functions."""

    def _make_feature_df(self, n: int = 100) -> pd.DataFrame:
        np.random.seed(42)
        dates = pd.date_range("2023-01-01", periods=n, freq="D")
        return pd.DataFrame(
            {
                "city_id": "delhi",
                "date_utc": dates,
                "aqi_lag_1d": np.random.uniform(50, 300, n),
                "aqi_lag_7d": np.random.uniform(50, 300, n),
                "aqi_roll_mean_7d": np.random.uniform(50, 300, n),
                "aqi_roll_std_7d": np.random.uniform(5, 50, n),
                "temperature_2m": np.random.uniform(10, 40, n),
                "relative_humidity_2m": np.random.uniform(20, 90, n),
                "day_of_week": np.random.randint(0, 7, n),
                "month": np.random.randint(1, 13, n),
                "is_weekend": np.random.randint(0, 2, n),
                "season_code": np.random.randint(0, 4, n),
                "target_aqi": np.random.uniform(50, 300, n),
                "target_category": np.random.choice(
                    ["Good", "Moderate", "Poor", "Severe"], n
                ),
            }
        )

    def test_get_feature_cols_excludes_meta_cols(self):
        from aqi_mlops.training import _get_feature_cols, NON_FEATURE_COLS

        df = self._make_feature_df()
        cols = _get_feature_cols(df)
        for meta in NON_FEATURE_COLS:
            assert meta not in cols

    def test_prepare_data_returns_correct_shapes(self):
        from aqi_mlops.training import _prepare_data

        df = self._make_feature_df(60)
        X, y_reg, y_clf = _prepare_data(df)
        assert len(X) == len(y_reg) == len(y_clf) == 60
        assert all(y_reg.between(0, 500))

    def test_prepare_data_maps_categories_to_ordinals(self):
        from aqi_mlops.training import _prepare_data

        df = self._make_feature_df(40)
        df["target_category"] = "Good"  # all Good → should map to 1
        _, _, y_clf = _prepare_data(df)
        assert (y_clf == 1).all()

    def test_train_regressor_insufficient_data_skips(self):
        from aqi_mlops.training import train_regressor

        df = self._make_feature_df(n=20)  # only 20 rows → < 30 threshold
        with patch("aqi_mlops.training.mlflow") as mock_mlflow:
            result = train_regressor(df)
        assert result.get("status") == "SKIPPED"


# ---------------------------------------------------------------------------
# Registry module tests
# ---------------------------------------------------------------------------

class TestRegistryHelpers:
    """Tests for model registry helper functions."""

    def test_get_latest_model_version_returns_none_when_no_model(self):
        from aqi_mlops.registry import get_latest_model_version

        with patch("aqi_mlops.registry._mlflow_client") as mock_client:
            mock_client.return_value.get_latest_versions.return_value = []
            result = get_latest_model_version("nonexistent_model", stage="Production")
        assert result is None

    def test_get_latest_model_version_returns_uri(self):
        from aqi_mlops.registry import get_latest_model_version

        mock_mv = MagicMock()
        mock_mv.version = "3"
        with patch("aqi_mlops.registry._mlflow_client") as mock_client:
            mock_client.return_value.get_latest_versions.return_value = [mock_mv]
            result = get_latest_model_version("aqi_regressor", stage="Production")
        assert result == "models:/aqi_regressor/3"


# ---------------------------------------------------------------------------
# Serving API tests
# ---------------------------------------------------------------------------

class TestServingAPI:
    """Tests for FastAPI inference endpoints."""

    @pytest.fixture(autouse=True)
    def setup_client(self, monkeypatch):
        from aqi_mlops import serving
        from aqi_mlops.serving import app
        from fastapi.testclient import TestClient

        monkeypatch.setattr(serving, "_log_api_request", lambda *args, **kwargs: None)
        self.client = TestClient(app)

    def test_health_endpoint(self):
        res = self.client.get("/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "ok"
        assert "regressor_loaded" in data
        assert "classifier_loaded" in data

    def test_ready_endpoint_requires_regressor(self):
        with patch("aqi_mlops.serving._get_model", return_value=None):
            res = self.client.get("/ready")
        assert res.status_code == 503

    def test_predict_returns_503_when_model_unavailable(self):
        with patch("aqi_mlops.serving._get_model", return_value=None):
            res = self.client.post("/predict", json={"city_id": "delhi"})
        assert res.status_code == 503
        assert "not available" in res.json()["detail"].lower()

    def test_get_model_returns_unavailable_when_mlflow_cannot_import(self):
        from aqi_mlops import serving

        with patch.dict("sys.modules", {"mlflow": None}):
            assert serving._get_model("missing_mlflow_dependency") is None

    def test_predict_success_with_mocked_model(self):
        mock_reg = MagicMock()
        mock_reg.predict.return_value = np.array([142.5])

        mock_clf = MagicMock()
        mock_clf.predict.return_value = np.array([3])  # Moderate

        dummy_features = pd.DataFrame([{"aqi": 140.0, "temperature_2m": 25.0}])

        with patch("aqi_mlops.serving._get_model") as mock_get_model, \
             patch("aqi_mlops.serving._build_feature_row", return_value=dummy_features), \
             patch("aqi_mlops.serving._log_prediction") as mock_log:

            def model_side_effect(name):
                if name == "aqi_regressor":
                    return (mock_reg, "1", ["aqi", "temperature_2m"])
                if name == "aqi_classifier":
                    return (mock_clf, "1", ["aqi", "temperature_2m"])
                return None

            mock_get_model.side_effect = model_side_effect

            res = self.client.post(
                "/predict",
                json={"city_id": "delhi", "as_of_date": "2026-01-01"},
            )

        assert res.status_code == 200
        data = res.json()
        assert data["city_id"] == "delhi"
        assert data["predicted_aqi"] == 142.5
        assert data["predicted_category"] == "Moderate"
        assert data["model_name"] == "aqi_regressor"
        assert mock_log.called

    def test_reload_models_unauthorized(self):
        res = self.client.post("/reload", headers={"x-reload-token": "wrong-token"})
        assert res.status_code == 403

    def test_reload_models_authorized(self):
        from aqi_mlops.serving import API_RELOAD_TOKEN

        res = self.client.post("/reload", headers={"x-reload-token": API_RELOAD_TOKEN})
        assert res.status_code == 200
        assert res.json()["status"] == "cache_cleared"

        res = self.client.post(
            "/admin/reload-model",
            headers={"x-reload-token": API_RELOAD_TOKEN},
        )
        assert res.status_code == 200

    def test_list_cities(self):
        with patch("aqi_pipeline.utils.config.load_cities", return_value=[{"id": "delhi", "name": "Delhi"}]):
            res = self.client.get("/cities")
        assert res.status_code == 200
        assert res.json() == {"cities": [{"id": "delhi", "name": "Delhi"}]}

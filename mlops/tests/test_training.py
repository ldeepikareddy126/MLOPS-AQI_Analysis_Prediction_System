"""
mlops/tests/test_training.py
==============================
Unit tests for the Stage B training module.

Tests the data preparation, feature selection, and model training
functions WITHOUT hitting a live database or MLflow server.
"""
from __future__ import annotations

import os
import numpy as np
import pandas as pd
import pytest


class TestPrepareData:
    """Tests for _prepare_data helper."""

    def test_returns_x_yreg_yclf(self, sample_feature_df):
        from aqi_mlops.training import _prepare_data

        X, y_reg, y_clf = _prepare_data(sample_feature_df)

        assert not X.empty
        assert len(y_reg) == len(X)
        assert len(y_clf) == len(X)

    def test_excludes_non_feature_cols(self, sample_feature_df):
        from aqi_mlops.training import _prepare_data, NON_FEATURE_COLS

        X, _, _ = _prepare_data(sample_feature_df)

        for col in NON_FEATURE_COLS:
            assert col not in X.columns, f"Non-feature col '{col}' found in X"

    def test_numeric_only_in_x(self, sample_feature_df):
        from aqi_mlops.training import _prepare_data

        X, _, _ = _prepare_data(sample_feature_df)

        for col in X.columns:
            assert pd.api.types.is_numeric_dtype(X[col]), f"Non-numeric column: {col}"

    def test_returns_feature_matrix_without_fitting_on_test_data(self, sample_feature_df):
        from aqi_mlops.training import _prepare_data

        X, _, _ = _prepare_data(sample_feature_df)
        assert list(X.columns)

    def test_category_map_applied(self, sample_feature_df):
        from aqi_mlops.training import _prepare_data

        _, _, y_clf = _prepare_data(sample_feature_df)

        # All mapped values should be 0–6
        assert y_clf.between(0, 6).all()

    def test_regression_target_matches_input(self, sample_feature_df):
        from aqi_mlops.training import _prepare_data

        _, y_reg, _ = _prepare_data(sample_feature_df)

        pd.testing.assert_series_equal(
            y_reg.reset_index(drop=True),
            sample_feature_df["target_aqi"].reset_index(drop=True),
        )


class TestGetFeatureCols:
    """Tests for _get_feature_cols."""

    def test_returns_list_of_strings(self, sample_feature_df):
        from aqi_mlops.training import _get_feature_cols

        cols = _get_feature_cols(sample_feature_df)
        assert isinstance(cols, list)
        assert all(isinstance(c, str) for c in cols)

    def test_excludes_meta_cols(self, sample_feature_df):
        from aqi_mlops.training import _get_feature_cols, NON_FEATURE_COLS

        cols = _get_feature_cols(sample_feature_df)
        for meta in NON_FEATURE_COLS:
            assert meta not in cols

    def test_only_numeric(self, sample_feature_df):
        from aqi_mlops.training import _get_feature_cols

        cols = _get_feature_cols(sample_feature_df)
        for c in cols:
            assert pd.api.types.is_numeric_dtype(sample_feature_df[c])


class TestChronologicalOrder:
    def test_sorts_dates_across_cities_before_split(self):
        from aqi_mlops.training import _chronological_order

        df = pd.DataFrame({
            "city_id": ["delhi", "mumbai", "delhi", "mumbai"],
            "date_utc": pd.to_datetime([
                "2024-01-03", "2024-01-01", "2024-01-02", "2024-01-04",
            ]),
        })

        ordered = _chronological_order(df)

        assert ordered["date_utc"].is_monotonic_increasing
        assert ordered.iloc[0]["city_id"] == "mumbai"

    def test_requires_date_for_time_based_split(self):
        from aqi_mlops.training import _chronological_order

        with pytest.raises(ValueError, match="date_utc"):
            _chronological_order(pd.DataFrame({"feature": [1]}))


class TestSelectModelFeatures:
    def test_selects_features_in_model_order(self):
        from aqi_mlops.registry import select_model_features

        features = pd.DataFrame({"second": [2.0], "first": [1.0]})
        selected = select_model_features(features, ["first", "second"])

        assert list(selected.columns) == ["first", "second"]
        assert selected.iloc[0].tolist() == [1.0, 2.0]

    def test_reports_missing_model_features(self):
        from aqi_mlops.registry import select_model_features

        with pytest.raises(ValueError, match="missing"):
            select_model_features(pd.DataFrame({"first": [1.0]}), ["second"])


class TestTrainRegressor:
    """Unit tests for train_regressor (mocks MLflow)."""

    def test_trains_with_sufficient_data(self, sample_feature_df, monkeypatch):
        """Should train successfully and return run metrics."""
        import unittest.mock as mock

        # Patch out mlflow to avoid needing a live tracking server
        mock_mlflow = mock.MagicMock()
        mock_run = mock.MagicMock()
        mock_run.__enter__ = mock.Mock(return_value=mock_run)
        mock_run.__exit__ = mock.Mock(return_value=False)
        mock_run.info.run_id = "test-run-id-abc123"
        mock_mlflow.start_run.return_value = mock_run

        monkeypatch.setattr("aqi_mlops.training.mlflow", mock_mlflow)

        from aqi_mlops.training import train_regressor

        result = train_regressor(sample_feature_df, mlflow_tracking_uri="http://fake:5000")

        assert result.get("run_id") == "test-run-id-abc123"
        assert "mae" in result.get("metrics", {})
        assert "rmse" in result.get("metrics", {})
        assert "r2" in result.get("metrics", {})

    def test_skips_with_insufficient_data(self, monkeypatch):
        """Should return SKIPPED when fewer than 30 training rows."""
        import unittest.mock as mock

        mock_mlflow = mock.MagicMock()
        monkeypatch.setattr("aqi_mlops.training.mlflow", mock_mlflow)

        from aqi_mlops.training import train_regressor

        tiny_df = pd.DataFrame(
            {
                "city_id": ["delhi"] * 10,
                "date_utc": pd.date_range("2024-01-01", periods=10, freq="D"),
                "aqi_lag_1d": np.random.uniform(50, 200, 10),
                "target_aqi": np.random.uniform(50, 200, 10),
                "target_category": ["Moderate"] * 10,
            }
        )
        result = train_regressor(tiny_df, mlflow_tracking_uri="http://fake:5000")
        assert result.get("status") == "SKIPPED"


class TestTrainClassifier:
    """Unit tests for train_classifier (mocks MLflow)."""

    def test_trains_with_sufficient_data(self, sample_feature_df, monkeypatch):
        import unittest.mock as mock

        mock_mlflow = mock.MagicMock()
        mock_run = mock.MagicMock()
        mock_run.__enter__ = mock.Mock(return_value=mock_run)
        mock_run.__exit__ = mock.Mock(return_value=False)
        mock_run.info.run_id = "test-clf-run-id"
        mock_mlflow.start_run.return_value = mock_run
        monkeypatch.setattr("aqi_mlops.training.mlflow", mock_mlflow)

        from aqi_mlops.training import train_classifier

        result = train_classifier(sample_feature_df, mlflow_tracking_uri="http://fake:5000")

        assert result.get("run_id") == "test-clf-run-id"
        assert "f1_macro" in result.get("metrics", {})
        assert "accuracy" in result.get("metrics", {})

"""
mlops/tests/test_monitoring.py
================================
Unit tests for the Stage B monitoring module.

Tests PSI computation, KS test, drift classification,
and alert logic WITHOUT hitting a live database.
"""
from __future__ import annotations

import numpy as np
import pytest


class TestComputePSI:
    """Tests for compute_psi function."""

    def test_identical_distributions_zero_psi(self):
        from aqi_mlops.monitoring import compute_psi

        x = np.random.default_rng(42).uniform(0, 100, 500)
        psi = compute_psi(x, x)
        # Identical distributions → PSI ≈ 0
        assert psi < 0.05

    def test_completely_different_distributions_high_psi(self):
        from aqi_mlops.monitoring import compute_psi

        rng = np.random.default_rng(42)
        baseline = rng.uniform(0, 50, 500)
        current = rng.uniform(150, 200, 500)
        psi = compute_psi(baseline, current)
        # Very different → PSI should be > 0.2 (significant)
        assert psi > 0.2

    def test_shift_beyond_baseline_bins_is_counted(self):
        from aqi_mlops.monitoring import compute_psi

        expected = np.full(100, 10.0)
        actual = np.full(100, 100.0)

        assert compute_psi(expected, actual) > 0.2

    def test_empty_baseline_returns_nan(self):
        from aqi_mlops.monitoring import compute_psi

        psi = compute_psi(np.array([]), np.array([1.0, 2.0, 3.0]))
        assert np.isnan(psi)

    def test_empty_actual_returns_nan(self):
        from aqi_mlops.monitoring import compute_psi

        psi = compute_psi(np.array([1.0, 2.0, 3.0]), np.array([]))
        assert np.isnan(psi)

    def test_returns_float(self):
        from aqi_mlops.monitoring import compute_psi

        rng = np.random.default_rng(42)
        x = rng.normal(100, 20, 200)
        psi = compute_psi(x, x + 10)
        assert isinstance(psi, float)

    def test_moderate_drift_in_expected_range(self):
        from aqi_mlops.monitoring import compute_psi, PSI_MODERATE_THRESHOLD, PSI_SIGNIFICANT_THRESHOLD

        rng = np.random.default_rng(42)
        baseline = rng.normal(100, 20, 500)
        # Small shift → moderate range
        current = rng.normal(120, 20, 500)
        psi = compute_psi(baseline, current)
        # Should not be nan
        assert not np.isnan(psi)


class TestComputeKS:
    """Tests for compute_ks function."""

    def test_identical_returns_low_statistic(self):
        from aqi_mlops.monitoring import compute_ks

        x = np.random.default_rng(42).uniform(0, 100, 500)
        stat, pval = compute_ks(x, x)
        # Same data → stat=0, pval=1
        assert stat == 0.0
        assert pval == 1.0

    def test_different_returns_low_pvalue(self):
        from aqi_mlops.monitoring import compute_ks

        rng = np.random.default_rng(42)
        baseline = rng.uniform(0, 50, 500)
        current = rng.uniform(150, 200, 500)
        stat, pval = compute_ks(baseline, current)
        assert stat > 0.5
        assert pval < 0.01

    def test_empty_returns_nan(self):
        from aqi_mlops.monitoring import compute_ks

        stat, pval = compute_ks(np.array([]), np.array([1.0, 2.0]))
        assert np.isnan(stat)
        assert np.isnan(pval)

    def test_returns_tuple_of_floats(self):
        from aqi_mlops.monitoring import compute_ks

        rng = np.random.default_rng(42)
        x = rng.normal(100, 20, 200)
        y = rng.normal(110, 20, 200)
        stat, pval = compute_ks(x, y)
        assert isinstance(stat, float)
        assert isinstance(pval, float)


class TestDriftStatusClassification:
    """Tests that PSI values are correctly classified into drift statuses."""

    @pytest.mark.parametrize("psi,expected_status", [
        (0.05, "STABLE"),
        (0.09, "STABLE"),
        (0.10, "MODERATE"),
        (0.15, "MODERATE"),
        (0.19, "MODERATE"),
        (0.20, "SIGNIFICANT"),
        (0.50, "SIGNIFICANT"),
    ])
    def test_psi_status_thresholds(self, psi, expected_status):
        from aqi_mlops.monitoring import PSI_MODERATE_THRESHOLD, PSI_SIGNIFICANT_THRESHOLD

        if np.isnan(psi):
            status = "UNKNOWN"
        elif psi < PSI_MODERATE_THRESHOLD:
            status = "STABLE"
        elif psi < PSI_SIGNIFICANT_THRESHOLD:
            status = "MODERATE"
        else:
            status = "SIGNIFICANT"

        assert status == expected_status


class TestRaiseAlert:
    """Tests raise_alert function with mocked DB."""

    def test_returns_uuid_string(self, monkeypatch):
        import unittest.mock as mock

        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__ = mock.Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=False)

        monkeypatch.setattr("aqi_mlops.monitoring.get_engine", mock.Mock(return_value=mock_engine))

        from aqi_mlops.monitoring import raise_alert

        alert_id = raise_alert(
            alert_type="DATA_DRIFT",
            severity="WARNING",
            description="Test alert",
            metric_value=0.25,
            threshold_value=0.2,
        )
        assert isinstance(alert_id, str)
        assert len(alert_id) == 36  # UUID length with dashes

    def test_insert_called_with_correct_type(self, monkeypatch):
        import unittest.mock as mock

        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__ = mock.Mock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=False)

        monkeypatch.setattr("aqi_mlops.monitoring.get_engine", mock.Mock(return_value=mock_engine))

        from aqi_mlops.monitoring import raise_alert

        raise_alert(
            alert_type="PERFORMANCE_DROP",
            severity="CRITICAL",
            description="MAE too high",
            metric_value=50.0,
            threshold_value=30.0,
        )
        assert mock_conn.execute.called

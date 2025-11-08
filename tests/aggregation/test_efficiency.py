"""Tests for efficiency metrics calculation."""

from __future__ import annotations

import pytest

from roastcoffea.aggregation.efficiency import calculate_efficiency_metrics


class TestCalculateEfficiencyMetrics:
    """Test efficiency metrics calculation from worker and workflow data."""

    def test_calculate_core_efficiency(self):
        """Calculates core efficiency from CPU time and available cores."""
        workflow_metrics = {
            "wall_time": 100.0,  # 100 seconds
            "total_cpu_time": 400.0,  # 400 seconds of CPU work
        }

        worker_metrics = {
            "avg_workers": 2.0,
            "total_cores": 8.0,  # 2 workers * 4 cores each
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        assert "core_efficiency" in efficiency
        assert "speedup_factor" in efficiency

        # Core efficiency = CPU time / (cores * wall time)
        # = 400 / (8 * 100) = 400 / 800 = 0.5 = 50%
        assert efficiency["core_efficiency"] == pytest.approx(0.5)

    def test_calculate_speedup_factor(self):
        """Calculates speedup factor (parallelization effectiveness)."""
        workflow_metrics = {
            "wall_time": 50.0,
            "total_cpu_time": 200.0,
        }

        worker_metrics = {
            "avg_workers": 4.0,
            "total_cores": 16.0,
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        # Speedup = CPU time / wall time
        # = 200 / 50 = 4x
        assert efficiency["speedup_factor"] == pytest.approx(4.0)

    def test_calculate_event_rate_per_core(self):
        """Calculates per-core event processing rate."""
        workflow_metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 400.0,
            "total_events": 1_000_000,
        }

        worker_metrics = {
            "avg_workers": 2.0,
            "total_cores": 8.0,
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        assert "event_rate_core_hz" in efficiency

        # Events per core = total events / (cores * wall time)
        # = 1_000_000 / (8 * 100) = 1_000_000 / 800 = 1250 Hz
        assert efficiency["event_rate_core_hz"] == pytest.approx(1250.0)

    def test_handles_missing_total_cores(self):
        """Handles missing total_cores gracefully."""
        workflow_metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 400.0,
            "total_events": 1_000_000,
        }

        worker_metrics = {
            "avg_workers": 2.0,
            "total_cores": None,  # Missing
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        # Core-dependent metrics should be None
        assert efficiency["core_efficiency"] is None
        assert efficiency["event_rate_core_hz"] is None

        # Speedup should still work (doesn't need cores)
        assert efficiency["speedup_factor"] == pytest.approx(4.0)

    def test_handles_zero_wall_time(self):
        """Handles zero wall time gracefully."""
        workflow_metrics = {
            "wall_time": 0.0,
            "total_cpu_time": 100.0,
            "total_events": 1000,
        }

        worker_metrics = {
            "avg_workers": 2.0,
            "total_cores": 8.0,
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        # Should not crash, return 0 or None for rates
        assert efficiency["core_efficiency"] == 0.0
        assert efficiency["event_rate_core_hz"] == 0.0
        assert efficiency["speedup_factor"] == 0.0

    def test_handles_zero_cpu_time(self):
        """Handles zero CPU time gracefully."""
        workflow_metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 0.0,
            "total_events": 0,
        }

        worker_metrics = {
            "avg_workers": 2.0,
            "total_cores": 8.0,
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        # Core efficiency should be 0
        assert efficiency["core_efficiency"] == 0.0
        assert efficiency["speedup_factor"] == 0.0

    def test_realistic_high_efficiency_scenario(self):
        """Test realistic high-efficiency scenario."""
        # CPU-bound workload with good parallelization
        workflow_metrics = {
            "wall_time": 60.0,
            "total_cpu_time": 450.0,  # 450 / 60 = 7.5x speedup
            "total_events": 5_000_000,
        }

        worker_metrics = {
            "avg_workers": 2.0,
            "total_cores": 8.0,
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        # 450 / (8 * 60) = 450 / 480 = 93.75% efficiency
        assert efficiency["core_efficiency"] == pytest.approx(0.9375)
        assert efficiency["speedup_factor"] == pytest.approx(7.5)

        # 5M events / (8 cores * 60 sec) = ~10.4k Hz per core
        assert efficiency["event_rate_core_hz"] == pytest.approx(10416.67, rel=1e-2)

    def test_realistic_low_efficiency_scenario(self):
        """Test realistic low-efficiency scenario (I/O bound)."""
        # I/O-bound workload with poor parallelization
        workflow_metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 150.0,  # 150 / 100 = 1.5x speedup (I/O bound)
            "total_events": 1_000_000,
        }

        worker_metrics = {
            "avg_workers": 4.0,
            "total_cores": 16.0,
        }

        efficiency = calculate_efficiency_metrics(workflow_metrics, worker_metrics)

        # 150 / (16 * 100) = 150 / 1600 = 9.375% efficiency (very I/O bound)
        assert efficiency["core_efficiency"] == pytest.approx(0.09375)
        assert efficiency["speedup_factor"] == pytest.approx(1.5)

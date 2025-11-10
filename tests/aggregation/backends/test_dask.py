"""Tests for Dask-specific aggregation parsers."""

from __future__ import annotations

import datetime
from typing import Any

import pytest

from roastcoffea.aggregation.backends.dask import (
    DaskTrackingDataParser,
    calculate_average_memory_per_worker,
    calculate_peak_memory,
    calculate_time_averaged_workers,
)


class TestParseTrackingData:
    """Test parsing of Dask scheduler tracking data."""

    @pytest.fixture
    def sample_tracking_data(self) -> dict[str, Any]:
        """Sample tracking data from DaskMetricsBackend."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 2)

        return {
            "worker_counts": {
                t0: 2,
                t1: 2,
                t2: 2,
            },
            "worker_memory": {
                "worker1": [
                    (t0, 1_000_000_000),  # 1 GB
                    (t1, 1_500_000_000),  # 1.5 GB
                    (t2, 2_000_000_000),  # 2 GB
                ],
                "worker2": [
                    (t0, 800_000_000),  # 0.8 GB
                    (t1, 1_200_000_000),  # 1.2 GB
                    (t2, 1_600_000_000),  # 1.6 GB
                ],
            },
            "worker_memory_limit": {
                "worker1": [
                    (t0, 4_000_000_000),  # 4 GB
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],
                "worker2": [
                    (t0, 4_000_000_000),
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],
            },
            "worker_active_tasks": {
                "worker1": [
                    (t0, 1),
                    (t1, 2),
                    (t2, 1),
                ],
                "worker2": [
                    (t0, 2),
                    (t1, 1),
                    (t2, 2),
                ],
            },
            "worker_cores": {
                "worker1": [
                    (t0, 4),
                    (t1, 4),
                    (t2, 4),
                ],
                "worker2": [
                    (t0, 4),
                    (t1, 4),
                    (t2, 4),
                ],
            },
        }

    def test_parse_tracking_data_returns_dict(self, sample_tracking_data):
        """parse_tracking_data returns aggregated metrics dictionary."""
        parser = DaskTrackingDataParser()
        metrics = parser.parse_tracking_data(sample_tracking_data)

        assert isinstance(metrics, dict)
        assert "avg_workers" in metrics
        assert "peak_workers" in metrics
        assert "total_cores" in metrics
        assert "peak_memory_bytes" in metrics
        assert "avg_memory_per_worker_bytes" in metrics

    def test_parse_tracking_data_calculates_worker_metrics(self, sample_tracking_data):
        """parse_tracking_data calculates correct worker statistics."""
        parser = DaskTrackingDataParser()
        metrics = parser.parse_tracking_data(sample_tracking_data)

        # All samples have 2 workers
        assert metrics["avg_workers"] == pytest.approx(2.0)
        assert metrics["peak_workers"] == 2

        # 2 workers * 4 cores each = 8 total cores
        assert metrics["total_cores"] == pytest.approx(8.0)
        # Average cores per worker = 4.0
        assert metrics["cores_per_worker"] == pytest.approx(4.0)

    def test_parse_tracking_data_calculates_memory_metrics(self, sample_tracking_data):
        """parse_tracking_data calculates memory statistics."""
        parser = DaskTrackingDataParser()
        metrics = parser.parse_tracking_data(sample_tracking_data)

        # Peak memory should be worker1 at t2: 2 GB
        assert metrics["peak_memory_bytes"] == 2_000_000_000

        # Average memory should be time-weighted average across both workers
        assert metrics["avg_memory_per_worker_bytes"] > 0

    def test_parse_tracking_data_handles_missing_cores_per_worker(self):
        """parse_tracking_data handles missing worker_cores gracefully."""
        tracking_data = {
            "worker_counts": {
                datetime.datetime(2025, 1, 1, 12, 0, 0): 2,
            },
            "worker_memory": {},
            "worker_memory_limit": {},
            "worker_active_tasks": {},
            "worker_cores": {},
        }

        parser = DaskTrackingDataParser()
        metrics = parser.parse_tracking_data(tracking_data)

        # Should still calculate worker metrics
        assert metrics["avg_workers"] == 2.0
        assert metrics["peak_workers"] == 2

        # cores-related metrics should be None
        assert metrics["total_cores"] is None
        assert metrics["cores_per_worker"] is None

    def test_parse_tracking_data_heterogeneous_cores(self):
        """parse_tracking_data handles heterogeneous cluster (different cores per worker)."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)

        tracking_data = {
            "worker_counts": {t0: 3},
            "worker_memory": {},
            "worker_memory_limit": {},
            "worker_active_tasks": {},
            "worker_cores": {
                "worker1": [(t0, 4)],  # 4 cores
                "worker2": [(t0, 8)],  # 8 cores
                "worker3": [(t0, 2)],  # 2 cores
            },
        }

        parser = DaskTrackingDataParser()
        metrics = parser.parse_tracking_data(tracking_data)

        # Total cores should be sum: 4 + 8 + 2 = 14
        assert metrics["total_cores"] == pytest.approx(14.0)
        # Average cores per worker: (4 + 8 + 2) / 3 = 4.67
        assert metrics["cores_per_worker"] == pytest.approx(14.0 / 3)

    def test_parse_tracking_data_handles_empty_data(self):
        """parse_tracking_data handles completely empty tracking data."""
        tracking_data: dict[str, dict] = {
            "worker_counts": {},
            "worker_memory": {},
            "worker_memory_limit": {},
            "worker_active_tasks": {},
            "worker_cores": {},
        }

        parser = DaskTrackingDataParser()
        metrics = parser.parse_tracking_data(tracking_data)

        # Should return zero/None values without crashing
        assert metrics["avg_workers"] == 0.0
        assert metrics["peak_workers"] == 0
        assert metrics["peak_memory_bytes"] == 0.0


class TestCalculateTimeAveragedWorkers:
    """Test time-weighted worker averaging calculation."""

    def test_calculate_with_constant_workers(self):
        """Time-averaged calculation with constant worker count."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 2)

        worker_counts = {t0: 4, t1: 4, t2: 4}

        avg = calculate_time_averaged_workers(worker_counts)
        assert avg == pytest.approx(4.0)

    def test_calculate_with_varying_workers(self):
        """Time-averaged calculation with varying worker count."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)  # +1 second
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 2)  # +1 second

        # 2 workers for first second, 4 workers for second second
        # Average = (2*1 + 4*1) / 2 = 3.0
        worker_counts = {t0: 2, t1: 4, t2: 4}

        avg = calculate_time_averaged_workers(worker_counts)
        # Trapezoidal: ((2+4)/2 * 1 + (4+4)/2 * 1) / 2 = (3 + 4) / 2 = 3.5
        assert avg == pytest.approx(3.5)

    def test_calculate_with_single_sample(self):
        """Single sample returns that value."""
        worker_counts = {datetime.datetime(2025, 1, 1, 12, 0, 0): 5}
        avg = calculate_time_averaged_workers(worker_counts)
        assert avg == 5.0

    def test_calculate_with_empty_data(self):
        """Empty data returns 0."""
        avg = calculate_time_averaged_workers({})
        assert avg == 0.0


class TestCalculatePeakMemory:
    """Test peak memory calculation across workers."""

    def test_calculate_peak_across_workers(self):
        """Peak memory is maximum across all workers and time."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)

        worker_memory = {
            "worker1": [(t0, 1_000_000_000), (t1, 2_000_000_000)],
            "worker2": [(t0, 800_000_000), (t1, 3_500_000_000)],  # Peak here
        }

        peak = calculate_peak_memory(worker_memory)
        assert peak == 3_500_000_000

    def test_calculate_peak_with_single_worker(self):
        """Peak memory with single worker timeline."""
        worker_memory = {
            "worker1": [
                (datetime.datetime(2025, 1, 1, 12, 0, 0), 500_000_000),
                (datetime.datetime(2025, 1, 1, 12, 0, 1), 1_200_000_000),
            ],
        }

        peak = calculate_peak_memory(worker_memory)
        assert peak == 1_200_000_000

    def test_calculate_peak_with_empty_data(self):
        """Empty data returns 0."""
        peak = calculate_peak_memory({})
        assert peak == 0.0


class TestCalculateAverageMemoryPerWorker:
    """Test time-weighted average memory per worker calculation."""

    def test_calculate_average_per_worker(self):
        """Average memory is time-weighted for each worker, then averaged."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 2)

        # Worker 1: constant 1 GB
        # Worker 2: constant 2 GB
        # Average should be 1.5 GB
        worker_memory = {
            "worker1": [
                (t0, 1_000_000_000),
                (t1, 1_000_000_000),
                (t2, 1_000_000_000),
            ],
            "worker2": [
                (t0, 2_000_000_000),
                (t1, 2_000_000_000),
                (t2, 2_000_000_000),
            ],
        }

        avg = calculate_average_memory_per_worker(worker_memory)
        assert avg == pytest.approx(1_500_000_000)

    def test_calculate_average_with_varying_memory(self):
        """Average accounts for time-weighted changes."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)

        # Single worker: 1 GB -> 2 GB over 1 second
        # Trapezoidal average: (1 + 2) / 2 = 1.5 GB
        worker_memory = {
            "worker1": [(t0, 1_000_000_000), (t1, 2_000_000_000)],
        }

        avg = calculate_average_memory_per_worker(worker_memory)
        assert avg == pytest.approx(1_500_000_000)

    def test_calculate_average_with_empty_data(self):
        """Empty data returns 0."""
        avg = calculate_average_memory_per_worker({})
        assert avg == 0.0

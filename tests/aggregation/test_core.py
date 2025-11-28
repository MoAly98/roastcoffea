"""Tests for core metrics aggregator."""

from __future__ import annotations

import datetime
from typing import Any

import pytest

from roastcoffea.aggregation.core import MetricsAggregator


class TestMetricsAggregator:
    """Test core MetricsAggregator that combines all aggregation modules."""

    @pytest.fixture
    def sample_coffea_report(self) -> dict[str, Any]:
        """Sample Coffea report."""
        return {
            "bytesread": 5_000_000_000,  # 5 GB
            "entries": 500_000,
            "processtime": 50.0,
            "chunks": 25,
        }

    @pytest.fixture
    def sample_tracking_data(self) -> dict[str, Any]:
        """Sample Dask tracking data."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)

        return {
            "worker_counts": {t0: 2, t1: 2},
            "worker_memory": {
                "worker1": [(t0, 1_000_000_000), (t1, 1_500_000_000)],
                "worker2": [(t0, 800_000_000), (t1, 1_200_000_000)],
            },
            "worker_memory_limit": {
                "worker1": [(t0, 4_000_000_000), (t1, 4_000_000_000)],
                "worker2": [(t0, 4_000_000_000), (t1, 4_000_000_000)],
            },
            "worker_active_tasks": {
                "worker1": [(t0, 2), (t1, 1)],
                "worker2": [(t0, 1), (t1, 2)],
            },
            "worker_cores": {
                "worker1": [(t0, 4), (t1, 4)],
                "worker2": [(t0, 4), (t1, 4)],
            },
        }

    def test_aggregate_combines_all_metrics(
        self, sample_coffea_report, sample_tracking_data
    ):
        """MetricsAggregator.aggregate combines workflow, worker, and efficiency metrics."""
        aggregator = MetricsAggregator(backend="dask")

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
        )

        # Should have workflow metrics
        assert "wall_time" in metrics
        assert "total_cpu_time" in metrics
        assert "overall_rate_gbps" in metrics
        assert "event_rate_wall_khz" in metrics

        # Should have worker metrics
        assert "avg_workers" in metrics
        assert "peak_workers" in metrics
        assert "total_cores" in metrics
        assert "peak_memory_bytes" in metrics

        # Should have efficiency metrics
        assert "core_efficiency" in metrics
        assert "speedup_factor" in metrics
        assert "event_rate_core_hz" in metrics

    def test_aggregate_with_dask_backend(
        self, sample_coffea_report, sample_tracking_data
    ):
        """Aggregator correctly uses Dask backend parser."""
        aggregator = MetricsAggregator(backend="dask")

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
        )

        # Verify Dask-specific parsing worked
        assert metrics["avg_workers"] == pytest.approx(2.0)
        assert metrics["total_cores"] == pytest.approx(8.0)

        # Verify raw tracking_data is preserved for visualization
        assert "tracking_data" in metrics
        assert metrics["tracking_data"] == sample_tracking_data

    def test_aggregate_without_tracking_data(self, sample_coffea_report):
        """Aggregator works without tracking data (workflow metrics only)."""
        aggregator = MetricsAggregator(backend="dask")

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=None,
            t_start=0.0,
            t_end=25.0,
        )

        # Should have workflow metrics
        assert "wall_time" in metrics
        assert "overall_rate_gbps" in metrics

        # tracking_data should be None
        assert metrics["tracking_data"] is None

        # Worker metrics should be None or absent
        assert metrics.get("avg_workers") is None
        assert metrics.get("total_cores") is None

        # Efficiency metrics that depend on workers should be None
        assert metrics.get("core_efficiency") is None

    def test_aggregate_with_custom_metrics(self, sample_tracking_data):
        """Aggregator handles custom per-dataset metrics."""
        coffea_report = {
            "bytesread": 5_000_000_000,
            "entries": 500_000,
            "processtime": 50.0,
            "chunks": 25,
        }

        custom_metrics = {
            "TTbar": {
                "entries": 300_000,
                "duration": 30.0,
                "performance_counters": {"num_requested_bytes": 3_000_000_000},
            },
            "WJets": {
                "entries": 200_000,
                "duration": 20.0,
                "performance_counters": {"num_requested_bytes": 2_000_000_000},
            },
        }

        aggregator = MetricsAggregator(backend="dask")

        metrics = aggregator.aggregate(
            coffea_report=coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
            custom_metrics=custom_metrics,
        )

        # Should aggregate across all datasets
        assert metrics["total_events"] == 500_000

    def test_instantiate_with_unsupported_backend_raises(self):
        """MetricsAggregator raises error for unsupported backend."""
        with pytest.raises(ValueError, match="Unsupported backend"):
            MetricsAggregator(backend="unknown_backend")

    def test_aggregate_returns_immutable_structure(
        self, sample_coffea_report, sample_tracking_data
    ):
        """Aggregated metrics are returned as new dict (not mutating inputs)."""
        aggregator = MetricsAggregator(backend="dask")

        original_report = sample_coffea_report.copy()
        original_tracking = sample_tracking_data.copy()

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
        )

        # Original data should be unchanged
        assert sample_coffea_report == original_report
        assert sample_tracking_data == original_tracking

        # Metrics should be a new dict
        assert metrics is not sample_coffea_report
        assert metrics is not sample_tracking_data

    def test_aggregate_with_span_metrics(self, sample_coffea_report, sample_tracking_data):
        """Aggregator processes span_metrics when provided."""
        aggregator = MetricsAggregator(backend="dask")

        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.5,
            ("execute", "task-123", "thread-noncpu"): 2.3,
            ("execute", "task-456", "disk-read"): 1024,
        }

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
            span_metrics=span_metrics,
        )

        # Should have processed span_metrics (exact keys depend on parse_fine_metrics)
        # Just verify that aggregation succeeded
        assert "wall_time" in metrics

    def test_aggregate_with_chunk_metrics(self, sample_coffea_report, sample_tracking_data):
        """Aggregator processes chunk_metrics when provided."""
        aggregator = MetricsAggregator(backend="dask")

        chunk_metrics = [
            {
                "t_start": 0.0,
                "t_end": 10.0,
                "duration": 10.0,
                "num_events": 1000,
                "dataset": "TTbar",
            },
            {
                "t_start": 10.0,
                "t_end": 20.0,
                "duration": 10.0,
                "num_events": 1500,
                "dataset": "WJets",
            },
        ]

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
            chunk_metrics=chunk_metrics,
        )

        # Should have chunk aggregation metrics (exact keys depend on aggregate_chunk_metrics)
        # Verify chunk_duration_max was calculated
        assert "chunk_duration_max" in metrics
        assert metrics["chunk_duration_max"] == 10.0

        # Should preserve raw chunk metrics
        assert "raw_chunk_metrics" in metrics
        assert metrics["raw_chunk_metrics"] == chunk_metrics

    def test_aggregate_with_section_metrics(self, sample_coffea_report, sample_tracking_data):
        """Aggregator preserves section_metrics when provided."""
        aggregator = MetricsAggregator(backend="dask")

        chunk_metrics = [
            {"t_start": 0.0, "t_end": 10.0, "duration": 10.0, "num_events": 1000}
        ]

        section_metrics = [
            {"section": "jet_selection", "duration": 5.0},
            {"section": "histogram_fill", "duration": 3.0},
        ]

        metrics = aggregator.aggregate(
            coffea_report=sample_coffea_report,
            tracking_data=sample_tracking_data,
            t_start=0.0,
            t_end=25.0,
            chunk_metrics=chunk_metrics,
            section_metrics=section_metrics,
        )

        # Should preserve raw section metrics
        assert "raw_section_metrics" in metrics
        assert metrics["raw_section_metrics"] == section_metrics

"""Tests for workflow-level metrics aggregation."""

from __future__ import annotations

from typing import Any

import pytest

from roastcoffea.aggregation.workflow import aggregate_workflow_metrics


class TestAggregateWorkflowMetrics:
    """Test workflow-level metrics aggregation from Coffea reports."""

    @pytest.fixture
    def sample_coffea_report(self) -> dict[str, Any]:
        """Sample Coffea report from Runner."""
        return {
            "bytesread": 10_000_000_000,  # 10 GB compressed
            "entries": 1_000_000,  # 1M events
            "processtime": 100.0,  # 100 seconds aggregated CPU time
            "chunks": 50,  # 50 chunks processed
        }

    def test_aggregate_basic_metrics(self, sample_coffea_report):
        """Aggregates basic metrics from Coffea report."""
        t_start = 0.0
        t_end = 50.0  # 50 seconds wall time

        metrics = aggregate_workflow_metrics(
            coffea_report=sample_coffea_report,
            t_start=t_start,
            t_end=t_end,
        )

        assert isinstance(metrics, dict)
        assert "wall_time" in metrics
        assert "total_cpu_time" in metrics
        assert "num_chunks" in metrics
        assert "total_events" in metrics
        assert "total_bytes_compressed" in metrics

        assert metrics["wall_time"] == 50.0
        assert metrics["total_cpu_time"] == 100.0
        assert metrics["num_chunks"] == 50
        assert metrics["total_events"] == 1_000_000
        assert metrics["total_bytes_compressed"] == 10_000_000_000

    def test_aggregate_throughput_metrics(self, sample_coffea_report):
        """Calculates throughput metrics."""
        metrics = aggregate_workflow_metrics(
            coffea_report=sample_coffea_report,
            t_start=0.0,
            t_end=50.0,
        )

        assert "overall_rate_gbps" in metrics
        assert "overall_rate_mbps" in metrics

        # 10 GB in 50 seconds = 0.2 GB/s = 1.6 Gbps
        assert metrics["overall_rate_gbps"] == pytest.approx(1.6)
        # 10 GB = 10000 MB, 10000 / 50 = 200 MB/s
        assert metrics["overall_rate_mbps"] == pytest.approx(200.0)

    def test_aggregate_event_rate_metrics(self, sample_coffea_report):
        """Calculates event rate metrics."""
        metrics = aggregate_workflow_metrics(
            coffea_report=sample_coffea_report,
            t_start=0.0,
            t_end=50.0,
        )

        assert "event_rate_wall_khz" in metrics
        assert "event_rate_agg_khz" in metrics

        # 1M events / 50 sec = 20k Hz = 20 kHz
        assert metrics["event_rate_wall_khz"] == pytest.approx(20.0)

        # 1M events / 100 sec CPU = 10k Hz = 10 kHz
        assert metrics["event_rate_agg_khz"] == pytest.approx(10.0)

    def test_aggregate_chunk_metrics(self, sample_coffea_report):
        """Calculates chunk-level metrics."""
        metrics = aggregate_workflow_metrics(
            coffea_report=sample_coffea_report,
            t_start=0.0,
            t_end=50.0,
        )

        assert "avg_cpu_time_per_chunk" in metrics

        # 100 sec CPU / 50 chunks = 2 sec per chunk
        assert metrics["avg_cpu_time_per_chunk"] == pytest.approx(2.0)

    def test_aggregate_with_custom_metrics(self):
        """Aggregates with per-dataset custom metrics."""
        coffea_report = {
            "bytesread": 10_000_000_000,
            "entries": 1_000_000,
            "processtime": 100.0,
            "chunks": 50,
        }

        custom_metrics = {
            "TTbar": {
                "entries": 500_000,
                "duration": 60.0,
                "performance_counters": {
                    "num_requested_bytes": 6_000_000_000,
                },
            },
            "WJets": {
                "entries": 500_000,
                "duration": 40.0,
                "performance_counters": {
                    "num_requested_bytes": 4_000_000_000,
                },
            },
        }

        metrics = aggregate_workflow_metrics(
            coffea_report=coffea_report,
            t_start=0.0,
            t_end=50.0,
            custom_metrics=custom_metrics,
        )

        # Should aggregate across all datasets
        assert metrics["total_events"] == 1_000_000
        assert metrics["total_bytes_compressed"] == 10_000_000_000

    def test_aggregate_estimates_compression_ratio(self, sample_coffea_report):
        """Estimates compression ratio for uncompressed bytes."""
        metrics = aggregate_workflow_metrics(
            coffea_report=sample_coffea_report,
            t_start=0.0,
            t_end=50.0,
        )

        assert "compression_ratio" in metrics
        assert "total_bytes_uncompressed" in metrics

        # Should estimate ~2.5x compression for NanoAOD
        assert metrics["compression_ratio"] == pytest.approx(2.5)
        assert metrics["total_bytes_uncompressed"] == pytest.approx(
            10_000_000_000 * 2.5
        )

    def test_aggregate_handles_zero_wall_time(self):
        """Handles edge case of zero wall time gracefully."""
        coffea_report = {
            "bytesread": 1000,
            "entries": 100,
            "processtime": 10.0,
            "chunks": 5,
        }

        metrics = aggregate_workflow_metrics(
            coffea_report=coffea_report,
            t_start=0.0,
            t_end=0.0,  # Zero wall time
        )

        # Rate metrics should be 0 (not error)
        assert metrics["overall_rate_gbps"] == 0.0
        assert metrics["event_rate_wall_khz"] == 0.0

    def test_aggregate_handles_zero_cpu_time(self):
        """Handles edge case of zero CPU time gracefully."""
        coffea_report = {
            "bytesread": 1000,
            "entries": 100,
            "processtime": 0.0,  # Zero CPU time
            "chunks": 5,
        }

        metrics = aggregate_workflow_metrics(
            coffea_report=coffea_report,
            t_start=0.0,
            t_end=10.0,
        )

        # CPU-based rate should be 0
        assert metrics["event_rate_agg_khz"] == 0.0

    def test_aggregate_handles_missing_chunks(self):
        """Handles missing chunks field gracefully."""
        coffea_report = {
            "bytesread": 1000,
            "entries": 100,
            "processtime": 10.0,
            # No "chunks" field
        }

        metrics = aggregate_workflow_metrics(
            coffea_report=coffea_report,
            t_start=0.0,
            t_end=10.0,
        )

        assert metrics["num_chunks"] == 0
        assert metrics["avg_cpu_time_per_chunk"] == 0.0

"""Tests for Dask Spans fine metrics parsing."""

from __future__ import annotations

import pytest

from roastcoffea.aggregation.fine_metrics import (
    calculate_compression_from_spans,
    parse_fine_metrics,
)


class TestParseFineMetrics:
    """Test parsing of Dask Spans cumulative_worker_metrics."""

    @pytest.fixture
    def sample_spans_data(self):
        """Sample cumulative_worker_metrics from Dask Span with tuple keys."""
        return {
            ("execute", "process-abc", "thread-cpu", "seconds"): 100.0,
            ("execute", "process-abc", "thread-noncpu", "seconds"): 50.0,
            ("execute", "process-abc", "disk-read", "bytes"): 10_000_000_000,
            ("execute", "process-abc", "disk-write", "bytes"): 500_000_000,
            ("execute", "process-abc", "decompress", "seconds"): 5.0,
            ("execute", "process-abc", "compress", "seconds"): 1.0,
            ("execute", "process-abc", "deserialize", "seconds"): 3.0,
            ("execute", "process-abc", "serialize", "seconds"): 2.0,
        }

    def test_parse_returns_dict(self, sample_spans_data):
        """parse_fine_metrics returns dictionary."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert isinstance(metrics, dict)

    def test_parse_extracts_cpu_time(self, sample_spans_data):
        """Extracts CPU time from thread-cpu."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert metrics["cpu_time_seconds"] == 100.0

    def test_parse_extracts_io_time(self, sample_spans_data):
        """Extracts I/O time from thread-noncpu."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert metrics["io_time_seconds"] == 50.0

    def test_parse_calculates_percentages(self, sample_spans_data):
        """Calculates CPU and I/O percentages."""
        metrics = parse_fine_metrics(sample_spans_data)

        # 100 / (100 + 50) = 66.67%
        assert metrics["cpu_percentage"] == pytest.approx(66.67, rel=0.01)
        # 50 / (100 + 50) = 33.33%
        assert metrics["io_percentage"] == pytest.approx(33.33, rel=0.01)

    def test_parse_extracts_disk_io(self, sample_spans_data):
        """Extracts disk I/O bytes."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert metrics["disk_read_bytes"] == 10_000_000_000
        assert metrics["disk_write_bytes"] == 500_000_000

    def test_parse_extracts_compression_overhead(self, sample_spans_data):
        """Extracts compression/decompression times."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert metrics["compression_time_seconds"] == 1.0
        assert metrics["decompression_time_seconds"] == 5.0
        assert metrics["total_compression_overhead_seconds"] == 6.0

    def test_parse_extracts_serialization_overhead(self, sample_spans_data):
        """Extracts serialization/deserialization times."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert metrics["serialization_time_seconds"] == 2.0
        assert metrics["deserialization_time_seconds"] == 3.0
        assert metrics["total_serialization_overhead_seconds"] == 5.0

    def test_parse_handles_missing_metrics(self):
        """Handles missing metrics gracefully (returns 0)."""
        metrics = parse_fine_metrics({})

        assert metrics["cpu_time_seconds"] == 0.0
        assert metrics["io_time_seconds"] == 0.0
        assert metrics["cpu_percentage"] == 0.0
        assert metrics["io_percentage"] == 0.0
        assert metrics["disk_read_bytes"] == 0
        assert metrics["disk_write_bytes"] == 0

    def test_parse_handles_zero_total_time(self):
        """Handles zero total time without division by zero."""
        metrics = parse_fine_metrics({
            ("execute", "task", "thread-cpu", "seconds"): 0.0,
            ("execute", "task", "thread-noncpu", "seconds"): 0.0,
        })

        assert metrics["cpu_percentage"] == 0.0
        assert metrics["io_percentage"] == 0.0


class TestCalculateCompressionFromSpans:
    """Test compression calculation from Spans data."""

    def test_calculates_compression_ratio(self):
        """Calculates compression ratio from compressed and uncompressed bytes."""
        spans_data = {("execute", "task", "disk-read", "bytes"): 10_000_000_000}
        compressed = 4_000_000_000  # 4 GB compressed

        ratio, uncompressed = calculate_compression_from_spans(compressed, spans_data)

        assert ratio == pytest.approx(2.5)  # 10 / 4 = 2.5x
        assert uncompressed == 10_000_000_000

    def test_returns_none_if_no_disk_read(self):
        """Returns None if disk-read not available in Spans."""
        ratio, uncompressed = calculate_compression_from_spans(
            4_000_000_000, {}  # Missing disk-read
        )

        assert ratio is None
        assert uncompressed is None

    def test_returns_none_if_zero_disk_read(self):
        """Returns None if disk-read is zero."""
        spans_data = {("execute", "task", "disk-read", "bytes"): 0}

        ratio, uncompressed = calculate_compression_from_spans(
            4_000_000_000, spans_data
        )

        assert ratio is None
        assert uncompressed is None

    def test_returns_none_if_zero_compressed(self):
        """Returns None if compressed bytes is zero."""
        spans_data = {("execute", "task", "disk-read", "bytes"): 10_000_000_000}

        ratio, uncompressed = calculate_compression_from_spans(0, spans_data)

        assert ratio is None
        assert uncompressed is None

    def test_handles_no_compression(self):
        """Handles case where files are not compressed (ratio = 1.0)."""
        spans_data = {("execute", "task", "disk-read", "bytes"): 5_000_000_000}
        compressed = 5_000_000_000  # Same size

        ratio, uncompressed = calculate_compression_from_spans(compressed, spans_data)

        assert ratio == pytest.approx(1.0)
        assert uncompressed == 5_000_000_000

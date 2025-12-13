"""Tests for Dask Spans fine metrics parsing."""

from __future__ import annotations

import pytest

from roastcoffea.aggregation.fine_metrics import parse_fine_metrics


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

        assert metrics["processor_cpu_time_seconds"] == 100.0

    def test_parse_extracts_noncpu_time(self, sample_spans_data):
        """Extracts I/O wait time from thread-noncpu."""
        metrics = parse_fine_metrics(sample_spans_data)

        assert metrics["processor_io_wait_time_seconds"] == 50.0

    def test_parse_calculates_percentages(self, sample_spans_data):
        """Calculates CPU and I/O wait percentages."""
        metrics = parse_fine_metrics(sample_spans_data)

        # 100 / (100 + 50) = 66.67%
        assert metrics["processor_cpu_percent"] == pytest.approx(66.67, rel=0.01)
        # 50 / (100 + 50) = 33.33%
        assert metrics["processor_io_wait_percent"] == pytest.approx(33.33, rel=0.01)

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

        assert metrics["processor_cpu_time_seconds"] == 0.0
        assert metrics["processor_io_wait_time_seconds"] == 0.0
        assert metrics["processor_cpu_percent"] == 0.0
        assert metrics["processor_io_wait_percent"] == 0.0
        assert metrics["disk_read_bytes"] == 0
        assert metrics["disk_write_bytes"] == 0

    def test_parse_handles_zero_total_time(self):
        """Handles zero total time without division by zero."""
        metrics = parse_fine_metrics({
            ("execute", "task", "thread-cpu", "seconds"): 0.0,
            ("execute", "task", "thread-noncpu", "seconds"): 0.0,
        })

        assert metrics["processor_cpu_percent"] == 0.0
        assert metrics["processor_io_wait_percent"] == 0.0

    def test_separates_processor_from_overhead(self):
        """Separates processor metrics from Dask overhead when processor_name given."""
        spans_data = {
            # Processor work
            ("execute", "MyProcessor", "thread-cpu", "seconds"): 100.0,
            ("execute", "MyProcessor", "thread-noncpu", "seconds"): 20.0,
            # Dask overhead
            ("execute", "lambda", "thread-cpu", "seconds"): 5.0,
            ("execute", "lambda", "thread-noncpu", "seconds"): 2.0,
        }

        metrics = parse_fine_metrics(spans_data, processor_name="MyProcessor")

        # Processor metrics
        assert metrics["processor_cpu_time_seconds"] == 100.0
        assert metrics["processor_io_wait_time_seconds"] == 20.0
        # Overhead metrics
        assert metrics["overhead_cpu_time_seconds"] == 5.0
        assert metrics["overhead_io_wait_time_seconds"] == 2.0
        # Percentages only for processor
        total = 100.0 + 20.0
        assert metrics["processor_cpu_percent"] == pytest.approx(100.0 / total * 100)
        assert metrics["processor_io_wait_percent"] == pytest.approx(20.0 / total * 100)

    def test_parse_skips_invalid_keys(self):
        """parse_fine_metrics skips invalid keys (line 64)."""
        spans_data = {
            ("execute", "process", "thread-cpu", "seconds"): 100.0,
            "invalid_string_key": 50.0,  # Not a tuple
            ("short",): 25.0,  # Tuple too short (len < 3)
            ("a", "b"): 10.0,  # Tuple with len = 2 (< 3)
        }

        metrics = parse_fine_metrics(spans_data)

        # Should only process the valid key
        assert metrics["processor_cpu_time_seconds"] == 100.0

    def test_parse_handles_memory_read_activity(self):
        """parse_fine_metrics handles memory-read activity (line 90)."""
        spans_data = {
            ("execute", "process", "memory-read", "bytes"): 5_000_000_000,
            ("execute", "process", "thread-cpu", "seconds"): 10.0,
        }

        metrics = parse_fine_metrics(spans_data)

        # Should capture memory-read bytes
        assert metrics["total_bytes_memory_read"] == 5_000_000_000
        assert metrics["processor_cpu_time_seconds"] == 10.0

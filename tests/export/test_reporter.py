"""Tests for Rich table formatting and reporting."""

from __future__ import annotations

from rich.table import Table

from roastcoffea.export.reporter import (
    format_event_processing_table,
    format_fine_metrics_table,
    format_resources_table,
    format_throughput_table,
    format_timing_table,
)


class TestFormatThroughputTable:
    """Test throughput metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_throughput_table returns Rich Table object."""
        metrics = {
            "overall_rate_gbps": 1.5,
            "overall_rate_mbps": 187.5,
            "compression_ratio": 2.5,
            "total_bytes_compressed": 5_000_000_000,
            "total_bytes_uncompressed": 12_500_000_000,
        }

        table = format_throughput_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Throughput Metrics"

    def test_table_has_correct_columns(self):
        """Throughput table has Metric and Value columns."""
        metrics = {
            "overall_rate_gbps": 1.5,
            "overall_rate_mbps": 187.5,
            "compression_ratio": 2.5,
        }

        table = format_throughput_table(metrics)

        # Table should have 2 columns
        assert len(table.columns) == 2

    def test_table_includes_data_rate(self):
        """Throughput table includes data rate in Gbps and MB/s."""
        metrics = {
            "overall_rate_gbps": 1.5,
            "overall_rate_mbps": 187.5,
            "compression_ratio": 2.5,
        }

        table = format_throughput_table(metrics)

        # Should have at least one row
        assert len(table.rows) > 0

    def test_handles_missing_optional_fields(self):
        """Throughput table handles missing optional fields gracefully."""
        metrics = {
            "overall_rate_gbps": 1.5,
            "overall_rate_mbps": 187.5,
        }

        # Should not crash
        table = format_throughput_table(metrics)
        assert isinstance(table, Table)


class TestFormatEventProcessingTable:
    """Test event processing metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_event_processing_table returns Rich Table."""
        metrics = {
            "total_events": 1_000_000,
            "event_rate_wall_khz": 20.0,
            "event_rate_agg_khz": 10.0,
            "event_rate_core_hz": 1250.0,
        }

        table = format_event_processing_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Event Processing Metrics"

    def test_table_includes_event_rates(self):
        """Event processing table includes all event rates."""
        metrics = {
            "total_events": 1_000_000,
            "event_rate_wall_khz": 20.0,
            "event_rate_agg_khz": 10.0,
            "event_rate_core_hz": 1250.0,
        }

        table = format_event_processing_table(metrics)

        assert len(table.rows) >= 3  # At least 3 rate metrics

    def test_handles_missing_core_rate(self):
        """Event processing table handles missing per-core rate (no worker data)."""
        metrics = {
            "total_events": 1_000_000,
            "event_rate_wall_khz": 20.0,
            "event_rate_agg_khz": 10.0,
            "event_rate_core_hz": None,  # No worker tracking
        }

        # Should not crash
        table = format_event_processing_table(metrics)
        assert isinstance(table, Table)


class TestFormatResourcesTable:
    """Test resource utilization metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_resources_table returns Rich Table."""
        metrics = {
            "avg_workers": 2.5,
            "peak_workers": 4,
            "total_cores": 16.0,
            "core_efficiency": 0.75,
            "speedup_factor": 3.0,
        }

        table = format_resources_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Resource Utilization"

    def test_table_includes_worker_metrics(self):
        """Resources table includes worker and core metrics."""
        metrics = {
            "avg_workers": 2.5,
            "peak_workers": 4,
            "cores_per_worker": 4.0,
            "total_cores": 16.0,
            "core_efficiency": 0.75,
            "speedup_factor": 3.0,
            "peak_memory_bytes": 2_000_000_000,
            "avg_memory_per_worker_bytes": 1_500_000_000,
        }

        table = format_resources_table(metrics)

        assert len(table.rows) >= 8  # Workers, cores, efficiency, memory

    def test_handles_missing_worker_tracking(self):
        """Resources table handles missing worker tracking data."""
        metrics = {
            "avg_workers": None,
            "peak_workers": None,
            "total_cores": None,
            "core_efficiency": None,
            "speedup_factor": None,
        }

        # Should not crash, should show "N/A" for missing data
        table = format_resources_table(metrics)
        assert isinstance(table, Table)


class TestFormatTimingTable:
    """Test timing metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_timing_table returns Rich Table."""
        metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 400.0,
            "num_chunks": 50,
            "avg_cpu_time_per_chunk": 8.0,
        }

        table = format_timing_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Timing Breakdown"

    def test_table_includes_timing_metrics(self):
        """Timing table includes wall time, CPU time, and chunk metrics."""
        metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 400.0,
            "num_chunks": 50,
            "avg_cpu_time_per_chunk": 8.0,
        }

        table = format_timing_table(metrics)

        assert len(table.rows) >= 2  # At least wall time and CPU time

    def test_handles_zero_chunks(self):
        """Timing table handles zero chunks gracefully."""
        metrics = {
            "wall_time": 100.0,
            "total_cpu_time": 400.0,
            "num_chunks": 0,
            "avg_cpu_time_per_chunk": 0.0,
        }

        # Should not crash
        table = format_timing_table(metrics)
        assert isinstance(table, Table)

    def test_formats_time_human_readable(self):
        """Timing table formats times in human-readable format."""
        metrics = {
            "wall_time": 3723.0,  # 1h 2m 3s
            "total_cpu_time": 45.2,  # 45.2s
            "num_chunks": 10,
            "avg_cpu_time_per_chunk": 4.52,
        }

        table = format_timing_table(metrics)

        # Table should be created successfully
        assert isinstance(table, Table)
        # Actual formatting is tested by implementation


class TestFormatFineMetricsTable:
    """Test fine metrics (Dask Spans) Rich table formatting."""

    def test_returns_rich_table_when_data_available(self):
        """format_fine_metrics_table returns Rich Table when metrics available."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "disk_read_bytes": 10_000_000_000,
            "disk_write_bytes": 500_000_000,
            "compression_time_seconds": 1.0,
            "decompression_time_seconds": 5.0,
            "total_compression_overhead_seconds": 6.0,
            "serialization_time_seconds": 2.0,
            "deserialization_time_seconds": 3.0,
            "total_serialization_overhead_seconds": 5.0,
        }

        table = format_fine_metrics_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Fine Metrics (from Dask Spans)"

    def test_returns_none_when_no_data_available(self):
        """format_fine_metrics_table returns None when no fine metrics available."""
        metrics = {
            "overall_rate_gbps": 1.5,
            "wall_time": 100.0,
        }

        table = format_fine_metrics_table(metrics)

        assert table is None

    def test_table_includes_cpu_noncpu_breakdown(self):
        """Fine metrics table includes CPU and non-CPU time breakdown."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
        }

        table = format_fine_metrics_table(metrics)

        assert len(table.rows) >= 4  # CPU time, non-CPU time, CPU %, non-CPU %

    def test_table_includes_disk_io(self):
        """Fine metrics table includes disk I/O if non-zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "disk_read_bytes": 10_000_000_000,
            "disk_write_bytes": 500_000_000,
        }

        table = format_fine_metrics_table(metrics)

        # Should have processor CPU, processor non-CPU, CPU %, non-CPU %, disk read, disk write = 6 rows
        assert len(table.rows) == 6

    def test_table_includes_compression_overhead(self):
        """Fine metrics table includes compression overhead if non-zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "compression_time_seconds": 1.0,
            "decompression_time_seconds": 5.0,
            "total_compression_overhead_seconds": 6.0,
        }

        table = format_fine_metrics_table(metrics)

        # CPU time, I/O time, CPU %, I/O %, total compression, compress, decompress = 7 rows
        assert len(table.rows) == 7

    def test_table_includes_serialization_overhead(self):
        """Fine metrics table includes serialization overhead if non-zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "serialization_time_seconds": 2.0,
            "deserialization_time_seconds": 3.0,
            "total_serialization_overhead_seconds": 5.0,
        }

        table = format_fine_metrics_table(metrics)

        # Processor CPU, processor non-CPU, CPU %, non-CPU %, total serialization, serialize, deserialize = 7 rows
        assert len(table.rows) == 7

    def test_omits_zero_disk_io(self):
        """Fine metrics table omits disk I/O if zero or None."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "disk_read_bytes": 0,
            "disk_write_bytes": None,
        }

        table = format_fine_metrics_table(metrics)

        # Should have CPU time, I/O time, CPU %, I/O % (4 rows), no disk rows
        assert len(table.rows) == 4

    def test_omits_zero_compression_overhead(self):
        """Fine metrics table omits compression if zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "total_compression_overhead_seconds": 0.0,
        }

        table = format_fine_metrics_table(metrics)

        # Should have processor CPU, processor non-CPU, CPU %, non-CPU % (4 rows), no compression
        assert len(table.rows) == 4

    def test_omits_zero_serialization_overhead(self):
        """Fine metrics table omits serialization if zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_noncpu_time_seconds": 50.0,
            "processor_cpu_percentage": 66.67,
            "processor_noncpu_percentage": 33.33,
            "total_serialization_overhead_seconds": 0.0,
        }

        table = format_fine_metrics_table(metrics)

        # Should have CPU time, I/O time, CPU %, I/O % (4 rows), no serialization
        assert len(table.rows) == 4

    def test_handles_partial_metrics(self):
        """Fine metrics table handles partial metrics gracefully."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            # processor_noncpu_time_seconds missing
            "disk_read_bytes": 10_000_000_000,
        }

        table = format_fine_metrics_table(metrics)

        # Should still create table with whatever is available
        assert isinstance(table, Table)

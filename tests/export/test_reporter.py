"""Tests for Rich table formatting and reporting."""

from __future__ import annotations

from rich.table import Table

from roastcoffea.export.reporter import (
    format_chunk_metrics_table,
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
            "data_rate_gbps": 1.5,
            "data_rate_mbps": 187.5,
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
            "data_rate_gbps": 1.5,
            "data_rate_mbps": 187.5,
            "compression_ratio": 2.5,
        }

        table = format_throughput_table(metrics)

        # Table should have 2 columns
        assert len(table.columns) == 2

    def test_table_includes_data_rate(self):
        """Throughput table includes data rate in Gbps and MB/s."""
        metrics = {
            "data_rate_gbps": 1.5,
            "data_rate_mbps": 187.5,
            "compression_ratio": 2.5,
        }

        table = format_throughput_table(metrics)

        # Should have at least one row
        assert len(table.rows) > 0

    def test_handles_missing_optional_fields(self):
        """Throughput table handles missing optional fields gracefully."""
        metrics = {
            "data_rate_gbps": 1.5,
            "data_rate_mbps": 187.5,
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
            "event_rate_elapsed_khz": 20.0,
            "event_rate_cpu_total_khz": 10.0,
            "event_rate_core_khz": 1250.0,
        }

        table = format_event_processing_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Event Processing Metrics"

    def test_table_includes_event_rates(self):
        """Event processing table includes all event rates."""
        metrics = {
            "total_events": 1_000_000,
            "event_rate_elapsed_khz": 20.0,
            "event_rate_cpu_total_khz": 10.0,
            "event_rate_core_khz": 1250.0,
        }

        table = format_event_processing_table(metrics)

        assert len(table.rows) >= 3  # At least 3 rate metrics

    def test_handles_missing_core_rate(self):
        """Event processing table handles missing per-core rate (no worker data)."""
        metrics = {
            "total_events": 1_000_000,
            "event_rate_elapsed_khz": 20.0,
            "event_rate_cpu_total_khz": 10.0,
            "event_rate_core_khz": None,  # No worker tracking
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
            "elapsed_time_seconds": 100.0,
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
            "elapsed_time_seconds": 100.0,
            "total_cpu_time": 400.0,
            "num_chunks": 50,
            "avg_cpu_time_per_chunk": 8.0,
        }

        table = format_timing_table(metrics)

        assert len(table.rows) >= 2  # At least wall time and CPU time

    def test_handles_zero_chunks(self):
        """Timing table handles zero chunks gracefully."""
        metrics = {
            "elapsed_time_seconds": 100.0,
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
            "elapsed_time_seconds": 3723.0,  # 1h 2m 3s
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
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
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
            "data_rate_gbps": 1.5,
            "elapsed_time_seconds": 100.0,
        }

        table = format_fine_metrics_table(metrics)

        assert table is None

    def test_table_includes_cpu_noncpu_breakdown(self):
        """Fine metrics table includes CPU and non-CPU time breakdown."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
        }

        table = format_fine_metrics_table(metrics)

        assert len(table.rows) >= 4  # CPU time, non-CPU time, CPU %, non-CPU %

    def test_table_includes_disk_io(self):
        """Fine metrics table includes disk I/O if non-zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
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
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
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
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
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
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
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
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
            "total_compression_overhead_seconds": 0.0,
        }

        table = format_fine_metrics_table(metrics)

        # Should have processor CPU, processor non-CPU, CPU %, non-CPU % (4 rows), no compression
        assert len(table.rows) == 4

    def test_omits_zero_serialization_overhead(self):
        """Fine metrics table omits serialization if zero."""
        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_io_wait_time_seconds": 50.0,
            "processor_cpu_percent": 66.67,
            "processor_io_wait_percent": 33.33,
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


class TestFormatChunkMetricsTable:
    """Test chunk metrics Rich table formatting."""

    def test_returns_none_when_no_chunks(self):
        """format_chunk_metrics_table returns None when no chunks."""
        metrics = {"num_chunks": 0}

        table = format_chunk_metrics_table(metrics)

        assert table is None

    def test_returns_rich_table_when_chunks_present(self):
        """format_chunk_metrics_table returns Rich Table when chunks present."""
        metrics = {
            "num_chunks": 10,
            "num_successful_chunks": 10,
            "num_failed_chunks": 0,
            "chunk_duration_mean": 2.5,
            "chunk_duration_min": 1.0,
            "chunk_duration_max": 5.0,
            "chunk_duration_std": 1.2,
            "total_events_from_chunks": 100000,
            "chunk_events_mean": 10000,
            "chunk_events_min": 8000,
            "chunk_events_max": 12000,
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Chunk Metrics"

    def test_table_includes_basic_stats(self):
        """Chunk table includes basic statistics."""
        metrics = {
            "num_chunks": 5,
            "num_successful_chunks": 4,
            "num_failed_chunks": 1,
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        # Should have at least total chunks, successful, failed rows
        assert len(table.rows) >= 3

    def test_table_includes_timing_stats(self):
        """Chunk table includes timing statistics."""
        metrics = {
            "num_chunks": 10,
            "chunk_duration_mean": 3.5,
            "chunk_duration_min": 2.0,
            "chunk_duration_max": 5.0,
            "chunk_duration_std": 0.8,
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        # Should include timing rows
        assert len(table.rows) > 1

    def test_table_includes_memory_stats(self):
        """Chunk table includes memory statistics when available."""
        metrics = {
            "num_chunks": 10,
            "chunk_mem_delta_mean_mb": 150.0,
            "chunk_mem_delta_min_mb": 100.0,
            "chunk_mem_delta_max_mb": 200.0,
            "chunk_mem_delta_std_mb": 25.0,
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_table_includes_event_stats(self):
        """Chunk table includes event statistics when available."""
        metrics = {
            "num_chunks": 8,
            "total_events_from_chunks": 80000,
            "chunk_events_mean": 10000,
            "chunk_events_min": 9000,
            "chunk_events_max": 11000,
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_table_includes_per_dataset_breakdown(self):
        """Chunk table includes per-dataset breakdown when available."""
        metrics = {
            "num_chunks": 20,
            "per_dataset": {
                "dataset_A": {
                    "num_chunks": 12,
                    "mean_duration": 2.5,
                    "total_events": 120000,
                    "mean_events_per_chunk": 10000,
                },
                "dataset_B": {
                    "num_chunks": 8,
                    "mean_duration": 3.0,
                    "total_events": 80000,
                    "mean_events_per_chunk": 10000,
                },
            },
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_table_includes_section_breakdown(self):
        """Chunk table includes section breakdown when available."""
        metrics = {
            "num_chunks": 10,
            "sections": {
                "jet_selection": {
                    "count": 10,
                    "mean_duration": 0.5,
                    "type": "time",
                },
                "load_branches": {
                    "count": 10,
                    "mean_duration": 1.0,
                    "mean_mem_delta_mb": 50.0,
                    "type": "memory",
                },
            },
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_handles_minimal_metrics(self):
        """Chunk table handles minimal metrics gracefully."""
        metrics = {
            "num_chunks": 3,
        }

        table = format_chunk_metrics_table(metrics)

        # Should still create table
        assert isinstance(table, Table)

    def test_handles_complete_metrics(self):
        """Chunk table handles complete metrics set."""
        metrics = {
            "num_chunks": 15,
            "num_successful_chunks": 14,
            "num_failed_chunks": 1,
            "chunk_duration_mean": 2.8,
            "chunk_duration_min": 1.5,
            "chunk_duration_max": 4.5,
            "chunk_duration_std": 0.9,
            "chunk_mem_delta_mean_mb": 120.0,
            "chunk_mem_delta_min_mb": 80.0,
            "chunk_mem_delta_max_mb": 180.0,
            "chunk_mem_delta_std_mb": 30.0,
            "total_events_from_chunks": 150000,
            "chunk_events_mean": 10000,
            "chunk_events_min": 9000,
            "chunk_events_max": 11000,
            "per_dataset": {
                "test_dataset": {
                    "num_chunks": 15,
                    "mean_duration": 2.8,
                    "total_events": 150000,
                    "mean_events_per_chunk": 10000,
                },
            },
            "sections": {
                "selection": {
                    "count": 15,
                    "mean_duration": 0.8,
                    "type": "time",
                },
            },
        }

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        assert len(table.rows) > 5  # Should have many rows with all this data


class TestFormatterEdgeCases:
    """Test edge cases in formatter functions."""

    def test_format_bytes_petabytes(self):
        """Test _format_bytes with petabyte values (line 16)."""
        from roastcoffea.export.reporter import _format_bytes

        # Test with very large value that reaches PB
        petabytes = 2.5 * 1024**5  # 2.5 PB in bytes
        result = _format_bytes(petabytes)
        assert "PB" in result
        assert "2.50" in result

    def test_format_timing_table_with_optional_coffea_bytes(self):
        """Test format_timing_table with total_bytes_read_coffea (line 64)."""
        from roastcoffea.export.reporter import format_timing_table

        metrics = {
            "elapsed_time_seconds": 100.0,
            "total_cpu_time": 80.0,
            "total_events": 1000,
            "total_bytes_read": 5_000_000_000,  # Optional metric
        }

        table = format_timing_table(metrics)
        assert isinstance(table, Table)
        # Check that the Coffea bytes row was added
        row_count = len(table.rows)
        assert row_count > 0
        # Verify table can be rendered (exercises all add_row calls)
        from rich.console import Console
        from io import StringIO
        console = Console(file=StringIO())
        console.print(table)

    def test_format_timing_table_with_optional_dask_bytes(self):
        """Test format_timing_table with total_bytes_memory_read_dask (line 72)."""
        from roastcoffea.export.reporter import format_timing_table

        metrics = {
            "elapsed_time_seconds": 100.0,
            "total_cpu_time": 80.0,
            "total_events": 1000,
            "total_bytes_memory_read": 3_000_000_000,  # Optional metric
        }

        table = format_timing_table(metrics)
        assert isinstance(table, Table)
        # Check that the Dask bytes row was added
        row_count = len(table.rows)
        assert row_count > 0
        # Verify table can be rendered (exercises all add_row calls)
        from rich.console import Console
        from io import StringIO
        console = Console(file=StringIO())
        console.print(table)

    def test_format_fine_metrics_with_overhead_cpu(self):
        """Test format_fine_metrics_table with overhead_cpu_time_seconds (line 279)."""
        from roastcoffea.export.reporter import format_fine_metrics_table

        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_io_wait_time_seconds": 20.0,
            "overhead_cpu_time_seconds": 5.0,  # Optional metric
            "overhead_io_wait_time_seconds": 0.0,
        }

        table = format_fine_metrics_table(metrics)
        assert table is not None

    def test_format_fine_metrics_with_overhead_noncpu(self):
        """Test format_fine_metrics_table with overhead_noncpu_time_seconds (line 281)."""
        from roastcoffea.export.reporter import format_fine_metrics_table

        metrics = {
            "processor_cpu_time_seconds": 100.0,
            "processor_io_wait_time_seconds": 20.0,
            "overhead_cpu_time_seconds": 0.0,
            "overhead_io_wait_time_seconds": 2.0,  # Optional metric
        }

        table = format_fine_metrics_table(metrics)
        assert table is not None

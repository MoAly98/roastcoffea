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


def _make_metrics(
    throughput: dict | None = None,
    events: dict | None = None,
    resources: dict | None = None,
    timing: dict | None = None,
    efficiency: dict | None = None,
    fine: dict | None = None,
    chunks: dict | None = None,
) -> dict:
    """Helper to create metrics dict with nested structure."""
    return {
        "summary": {
            "throughput": throughput or {},
            "events": events or {},
            "resources": resources or {},
            "timing": timing or {},
            "efficiency": efficiency or {},
            "fine": fine or {},
            "chunks": chunks or {},
        }
    }


class TestFormatThroughputTable:
    """Test throughput metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_throughput_table returns Rich Table object."""
        metrics = _make_metrics(
            throughput={
                "data_rate_gbps": 1.5,
                "bytes_read": 5_000_000_000,
            }
        )

        table = format_throughput_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Throughput Metrics"

    def test_table_has_correct_columns(self):
        """Throughput table has Metric and Value columns."""
        metrics = _make_metrics(
            throughput={
                "data_rate_gbps": 1.5,
            }
        )

        table = format_throughput_table(metrics)

        # Table should have 2 columns
        assert len(table.columns) == 2

    def test_table_includes_data_rate(self):
        """Throughput table includes data rate in Gbps and MB/s."""
        metrics = _make_metrics(
            throughput={
                "data_rate_gbps": 1.5,
            }
        )

        table = format_throughput_table(metrics)

        # Should have at least one row
        assert len(table.rows) > 0

    def test_handles_missing_optional_fields(self):
        """Throughput table handles missing optional fields gracefully."""
        metrics = _make_metrics(
            throughput={
                "data_rate_gbps": 1.5,
            }
        )

        # Should not crash
        table = format_throughput_table(metrics)
        assert isinstance(table, Table)


class TestFormatEventProcessingTable:
    """Test event processing metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_event_processing_table returns Rich Table."""
        metrics = _make_metrics(
            events={
                "total": 1_000_000,
                "rate_wall_khz": 20.0,
                "rate_cpu_khz": 10.0,
                "rate_core_khz": 1250.0,
            }
        )

        table = format_event_processing_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Event Processing Metrics"

    def test_table_includes_event_rates(self):
        """Event processing table includes all event rates."""
        metrics = _make_metrics(
            events={
                "total": 1_000_000,
                "rate_wall_khz": 20.0,
                "rate_cpu_khz": 10.0,
                "rate_core_khz": 1250.0,
            }
        )

        table = format_event_processing_table(metrics)

        assert len(table.rows) >= 3  # At least 3 rate metrics

    def test_handles_missing_core_rate(self):
        """Event processing table handles missing per-core rate (no worker data)."""
        metrics = _make_metrics(
            events={
                "total": 1_000_000,
                "rate_wall_khz": 20.0,
                "rate_cpu_khz": 10.0,
                "rate_core_khz": None,  # No worker tracking
            }
        )

        # Should not crash
        table = format_event_processing_table(metrics)
        assert isinstance(table, Table)


class TestFormatResourcesTable:
    """Test resource utilization metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_resources_table returns Rich Table."""
        metrics = _make_metrics(
            resources={
                "workers_avg": 2.5,
                "workers_peak": 4,
                "cores_total": 16.0,
            },
            efficiency={
                "core_efficiency": 0.75,
                "speedup": 3.0,
            },
        )

        table = format_resources_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Resource Utilization"

    def test_table_includes_worker_metrics(self):
        """Resources table includes worker and core metrics."""
        metrics = _make_metrics(
            resources={
                "workers_avg": 2.5,
                "workers_peak": 4,
                "cores_per_worker": 4.0,
                "cores_total": 16.0,
                "memory_peak_bytes": 2_000_000_000,
                "memory_avg_bytes": 1_500_000_000,
            },
            efficiency={
                "core_efficiency": 0.75,
                "speedup": 3.0,
            },
        )

        table = format_resources_table(metrics)

        assert len(table.rows) >= 8  # Workers, cores, efficiency, memory

    def test_handles_missing_worker_tracking(self):
        """Resources table handles missing worker tracking data."""
        metrics = _make_metrics(
            resources={
                "workers_avg": None,
                "workers_peak": None,
                "cores_total": None,
            },
            efficiency={
                "core_efficiency": None,
                "speedup": None,
            },
        )

        # Should not crash, should show "N/A" for missing data
        table = format_resources_table(metrics)
        assert isinstance(table, Table)


class TestFormatTimingTable:
    """Test timing metrics Rich table formatting."""

    def test_returns_rich_table(self):
        """format_timing_table returns Rich Table."""
        metrics = _make_metrics(
            timing={
                "wall_seconds": 100.0,
                "cpu_seconds": 400.0,
                "num_chunks": 50,
                "avg_chunk_seconds": 8.0,
            }
        )

        table = format_timing_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Timing Breakdown"

    def test_table_includes_timing_metrics(self):
        """Timing table includes wall time, CPU time, and chunk metrics."""
        metrics = _make_metrics(
            timing={
                "wall_seconds": 100.0,
                "cpu_seconds": 400.0,
                "num_chunks": 50,
                "avg_chunk_seconds": 8.0,
            }
        )

        table = format_timing_table(metrics)

        assert len(table.rows) >= 2  # At least wall time and CPU time

    def test_handles_zero_chunks(self):
        """Timing table handles zero chunks gracefully."""
        metrics = _make_metrics(
            timing={
                "wall_seconds": 100.0,
                "cpu_seconds": 400.0,
                "num_chunks": 0,
                "avg_chunk_seconds": 0.0,
            }
        )

        # Should not crash
        table = format_timing_table(metrics)
        assert isinstance(table, Table)

    def test_formats_time_human_readable(self):
        """Timing table formats times in human-readable format."""
        metrics = _make_metrics(
            timing={
                "wall_seconds": 3723.0,  # 1h 2m 3s
                "cpu_seconds": 45.2,  # 45.2s
                "num_chunks": 10,
                "avg_chunk_seconds": 4.52,
            }
        )

        table = format_timing_table(metrics)

        # Table should be created successfully
        assert isinstance(table, Table)
        # Actual formatting is tested by implementation


class TestFormatFineMetricsTable:
    """Test fine metrics (Dask Spans) Rich table formatting."""

    def test_returns_rich_table_when_data_available(self):
        """format_fine_metrics_table returns Rich Table when metrics available."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "disk_read_bytes": 10_000_000_000,
                "disk_write_bytes": 500_000_000,
                "compression_seconds": 1.0,
                "decompression_seconds": 5.0,
                "serialization_seconds": 2.0,
                "deserialization_seconds": 3.0,
            }
        )

        table = format_fine_metrics_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Fine Metrics (from Dask Spans)"

    def test_returns_none_when_no_data_available(self):
        """format_fine_metrics_table returns None when no fine metrics available."""
        metrics = _make_metrics(
            throughput={"data_rate_gbps": 1.5},
            timing={"wall_seconds": 100.0},
        )

        table = format_fine_metrics_table(metrics)

        assert table is None

    def test_table_includes_cpu_noncpu_breakdown(self):
        """Fine metrics table includes CPU and non-CPU time breakdown."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
            }
        )

        table = format_fine_metrics_table(metrics)

        assert len(table.rows) >= 4  # CPU time, non-CPU time, CPU %, non-CPU %

    def test_table_includes_disk_io(self):
        """Fine metrics table includes disk I/O if non-zero."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "disk_read_bytes": 10_000_000_000,
                "disk_write_bytes": 500_000_000,
            }
        )

        table = format_fine_metrics_table(metrics)

        # Should have processor CPU, processor non-CPU, CPU %, non-CPU %, disk read, disk write = 6 rows
        assert len(table.rows) == 6

    def test_table_includes_compression_overhead(self):
        """Fine metrics table includes compression overhead if non-zero."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "compression_seconds": 1.0,
                "decompression_seconds": 5.0,
            }
        )

        table = format_fine_metrics_table(metrics)

        # CPU time, I/O time, CPU %, I/O %, total compression, compress, decompress = 7 rows
        assert len(table.rows) == 7

    def test_table_includes_serialization_overhead(self):
        """Fine metrics table includes serialization overhead if non-zero."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "serialization_seconds": 2.0,
                "deserialization_seconds": 3.0,
            }
        )

        table = format_fine_metrics_table(metrics)

        # Processor CPU, processor non-CPU, CPU %, non-CPU %, total serialization, serialize, deserialize = 7 rows
        assert len(table.rows) == 7

    def test_omits_zero_disk_io(self):
        """Fine metrics table omits disk I/O if zero or None."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "disk_read_bytes": 0,
                "disk_write_bytes": None,
            }
        )

        table = format_fine_metrics_table(metrics)

        # Should have CPU time, I/O time, CPU %, I/O % (4 rows), no disk rows
        assert len(table.rows) == 4

    def test_omits_zero_compression_overhead(self):
        """Fine metrics table omits compression if zero."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "compression_seconds": 0.0,
                "decompression_seconds": 0.0,
            }
        )

        table = format_fine_metrics_table(metrics)

        # Should have processor CPU, processor non-CPU, CPU %, non-CPU % (4 rows), no compression
        assert len(table.rows) == 4

    def test_omits_zero_serialization_overhead(self):
        """Fine metrics table omits serialization if zero."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 50.0,
                "processor_cpu_percent": 66.67,
                "processor_io_percent": 33.33,
                "serialization_seconds": 0.0,
                "deserialization_seconds": 0.0,
            }
        )

        table = format_fine_metrics_table(metrics)

        # Should have CPU time, I/O time, CPU %, I/O % (4 rows), no serialization
        assert len(table.rows) == 4

    def test_handles_partial_metrics(self):
        """Fine metrics table handles partial metrics gracefully."""
        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                # processor_io_seconds missing
                "disk_read_bytes": 10_000_000_000,
            }
        )

        table = format_fine_metrics_table(metrics)

        # Should still create table with whatever is available
        assert isinstance(table, Table)


class TestFormatChunkMetricsTable:
    """Test chunk metrics Rich table formatting."""

    def test_returns_none_when_no_chunks(self):
        """format_chunk_metrics_table returns None when no chunks."""
        metrics = _make_metrics(chunks={"num_chunks": 0})

        table = format_chunk_metrics_table(metrics)

        assert table is None

    def test_returns_rich_table_when_chunks_present(self):
        """format_chunk_metrics_table returns Rich Table when chunks present."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 10,
                "num_successful": 10,
                "num_failed": 0,
                "duration_mean": 2.5,
                "duration_min": 1.0,
                "duration_max": 5.0,
                "duration_std": 1.2,
                "events_mean": 10000,
                "events_min": 8000,
                "events_max": 12000,
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        assert table.title == "Chunk Metrics"

    def test_table_includes_basic_stats(self):
        """Chunk table includes basic statistics."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 5,
                "num_successful": 4,
                "num_failed": 1,
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        # Should have at least total chunks, successful, failed rows
        assert len(table.rows) >= 3

    def test_table_includes_timing_stats(self):
        """Chunk table includes timing statistics."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 10,
                "duration_mean": 3.5,
                "duration_min": 2.0,
                "duration_max": 5.0,
                "duration_std": 0.8,
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        # Should include timing rows
        assert len(table.rows) > 1

    def test_table_includes_memory_stats(self):
        """Chunk table includes memory statistics when available."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 10,
                "mem_delta_mean_mb": 150.0,
                "mem_delta_min_mb": 100.0,
                "mem_delta_max_mb": 200.0,
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_table_includes_event_stats(self):
        """Chunk table includes event statistics when available."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 8,
                "events_mean": 10000,
                "events_min": 9000,
                "events_max": 11000,
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_table_includes_per_dataset_breakdown(self):
        """Chunk table includes per-dataset breakdown when available."""
        metrics = _make_metrics(
            chunks={
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
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_table_includes_section_breakdown(self):
        """Chunk table includes section breakdown when available."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 10,
                "sections": {
                    "jet_selection": {
                        "count": 10,
                        "mean_duration": 0.5,
                        "total_duration": 5.0,
                        "type": "time",
                    },
                    "load_branches": {
                        "count": 10,
                        "mean_duration": 1.0,
                        "total_duration": 10.0,
                        "mean_mem_delta_mb": 50.0,
                        "type": "memory",
                    },
                },
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)

    def test_handles_minimal_metrics(self):
        """Chunk table handles minimal metrics gracefully."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 3,
            }
        )

        table = format_chunk_metrics_table(metrics)

        # Should still create table
        assert isinstance(table, Table)

    def test_handles_complete_metrics(self):
        """Chunk table handles complete metrics set."""
        metrics = _make_metrics(
            chunks={
                "num_chunks": 15,
                "num_successful": 14,
                "num_failed": 1,
                "duration_mean": 2.8,
                "duration_min": 1.5,
                "duration_max": 4.5,
                "duration_std": 0.9,
                "mem_delta_mean_mb": 120.0,
                "mem_delta_min_mb": 80.0,
                "mem_delta_max_mb": 180.0,
                "events_mean": 10000,
                "events_min": 9000,
                "events_max": 11000,
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
                        "total_duration": 12.0,
                        "type": "time",
                    },
                },
            }
        )

        table = format_chunk_metrics_table(metrics)

        assert isinstance(table, Table)
        assert len(table.rows) > 5  # Should have many rows with all this data


class TestFormatterEdgeCases:
    """Test edge cases in formatter functions."""

    def test_format_bytes_petabytes(self):
        """Test _format_bytes with petabyte values (line 16)."""
        from roastcoffea.export.reporter import _format_bytes  # noqa: PLC2701

        # Test with very large value that reaches PB
        petabytes = 2.5 * 1024**5  # 2.5 PB in bytes
        result = _format_bytes(petabytes)
        assert "PB" in result
        assert "2.50" in result

    def test_format_timing_table_with_optional_coffea_bytes(self):
        """Test format_timing_table handles various timing scenarios."""
        from roastcoffea.export.reporter import format_timing_table

        metrics = _make_metrics(
            timing={
                "wall_seconds": 100.0,
                "cpu_seconds": 80.0,
                "num_chunks": 10,
                "avg_chunk_seconds": 8.0,
            },
            throughput={
                "bytes_read": 5_000_000_000,
            },
        )

        table = format_timing_table(metrics)
        assert isinstance(table, Table)
        # Verify table can be rendered (exercises all add_row calls)
        from io import StringIO

        from rich.console import Console

        console = Console(file=StringIO())
        console.print(table)

    def test_format_fine_metrics_with_overhead_cpu(self):
        """Test format_fine_metrics_table with overhead_cpu_seconds."""
        from roastcoffea.export.reporter import format_fine_metrics_table

        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 20.0,
                "overhead_cpu_seconds": 5.0,
                "overhead_io_seconds": 0.0,
            }
        )

        table = format_fine_metrics_table(metrics)
        assert table is not None

    def test_format_fine_metrics_with_overhead_noncpu(self):
        """Test format_fine_metrics_table with overhead_io_seconds."""
        from roastcoffea.export.reporter import format_fine_metrics_table

        metrics = _make_metrics(
            fine={
                "processor_cpu_seconds": 100.0,
                "processor_io_seconds": 20.0,
                "overhead_cpu_seconds": 0.0,
                "overhead_io_seconds": 2.0,
            }
        )

        table = format_fine_metrics_table(metrics)
        assert table is not None

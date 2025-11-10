"""Rich table formatting for metrics reporting."""

from __future__ import annotations

from typing import Any

from rich.table import Table


def _format_bytes(num_bytes: float) -> str:
    """Format bytes in human-readable units."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"


def _format_time(seconds: float) -> str:
    """Format time in human-readable units."""
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = minutes // 60
    remaining_minutes = minutes % 60

    return f"{hours}h {remaining_minutes}m {remaining_seconds}s"


def format_throughput_table(metrics: dict[str, Any]) -> Table:
    """Format throughput metrics as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary

    Returns
    -------
    Table
        Rich table
    """
    table = Table(
        title="Throughput Metrics", show_header=True, header_style="bold cyan"
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Data rate
    table.add_row(
        "Data Rate",
        f"{metrics.get('overall_rate_gbps', 0):.2f} Gbps ({metrics.get('overall_rate_mbps', 0):.1f} MB/s)",
    )

    # Compression (only if available from Dask Spans)
    ratio = metrics.get("compression_ratio")
    if ratio is not None:
        table.add_row("Compression Ratio", f"{ratio:.2f}x")
    else:
        table.add_row("Compression Ratio", "[dim]N/A (requires Dask Spans)[/dim]")

    # Data volume
    compressed = metrics.get("total_bytes_compressed", 0)
    uncompressed = metrics.get("total_bytes_uncompressed")
    if compressed:
        if uncompressed is not None:
            table.add_row(
                "Total Data Read",
                f"{_format_bytes(compressed)} compressed, {_format_bytes(uncompressed)} uncompressed",
            )
        else:
            table.add_row(
                "Total Data Read",
                f"{_format_bytes(compressed)} compressed",
            )

    return table


def format_event_processing_table(metrics: dict[str, Any]) -> Table:
    """Format event processing metrics as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary

    Returns
    -------
    Table
        Rich table
    """
    table = Table(
        title="Event Processing Metrics", show_header=True, header_style="bold cyan"
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Total events
    total_events = metrics.get("total_events", 0)
    table.add_row("Total Events", f"{total_events:,}")

    # Event rates
    wall_khz = metrics.get("event_rate_wall_khz", 0)
    table.add_row("Event Rate (Wall Clock)", f"{wall_khz:.1f} kHz")

    agg_khz = metrics.get("event_rate_agg_khz", 0)
    table.add_row("Event Rate (Aggregated)", f"{agg_khz:.1f} kHz")

    # Core-averaged rate (may be None if no worker data)
    core_hz = metrics.get("event_rate_core_hz")
    if core_hz is not None:
        table.add_row("Event Rate (Core-Averaged)", f"{core_hz:.1f} Hz/core")
    else:
        table.add_row("Event Rate (Core-Averaged)", "[dim]N/A (no worker data)[/dim]")

    # Efficiency ratio
    if wall_khz and agg_khz:
        efficiency_ratio = wall_khz / agg_khz
        table.add_row("Efficiency Ratio", f"{efficiency_ratio:.1%}")

    return table


def format_resources_table(metrics: dict[str, Any]) -> Table:
    """Format resource utilization metrics as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary

    Returns
    -------
    Table
        Rich table
    """
    table = Table(
        title="Resource Utilization", show_header=True, header_style="bold cyan"
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Worker metrics
    avg_workers = metrics.get("avg_workers")
    if avg_workers is not None:
        table.add_row("Workers (Time-Averaged)", f"{avg_workers:.1f}")
    else:
        table.add_row("Workers (Time-Averaged)", "[dim]N/A (no worker tracking)[/dim]")

    peak_workers = metrics.get("peak_workers")
    if peak_workers is not None:
        table.add_row("Peak Workers", f"{peak_workers}")
    else:
        table.add_row("Peak Workers", "[dim]N/A (no worker tracking)[/dim]")

    # Core metrics
    cores_per_worker = metrics.get("cores_per_worker")
    if cores_per_worker is not None:
        table.add_row("Cores per Worker", f"{cores_per_worker:.1f}")
    else:
        table.add_row("Cores per Worker", "[dim]N/A (no worker tracking)[/dim]")

    total_cores = metrics.get("total_cores")
    if total_cores is not None:
        table.add_row("Total Cores", f"{total_cores:.0f}")
    else:
        table.add_row("Total Cores", "[dim]N/A (no worker tracking)[/dim]")

    # Efficiency
    core_efficiency = metrics.get("core_efficiency")
    if core_efficiency is not None:
        table.add_row("Core Efficiency", f"{core_efficiency:.1%}")
    else:
        table.add_row("Core Efficiency", "[dim]N/A (no worker tracking)[/dim]")

    # Speedup
    speedup = metrics.get("speedup_factor")
    if speedup is not None:
        table.add_row("Speedup Factor", f"{speedup:.1f}x")
    else:
        table.add_row("Speedup Factor", "[dim]N/A (no worker tracking)[/dim]")

    # Memory metrics
    peak_memory = metrics.get("peak_memory_bytes")
    if peak_memory is not None:
        table.add_row("Peak Memory (per worker)", _format_bytes(peak_memory))
    else:
        table.add_row("Peak Memory (per worker)", "[dim]N/A (no worker tracking)[/dim]")

    avg_memory = metrics.get("avg_memory_per_worker_bytes")
    if avg_memory is not None:
        table.add_row("Avg Memory (per worker)", _format_bytes(avg_memory))
    else:
        table.add_row("Avg Memory (per worker)", "[dim]N/A (no worker tracking)[/dim]")

    return table


def format_timing_table(metrics: dict[str, Any]) -> Table:
    """Format timing metrics as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary

    Returns
    -------
    Table
        Rich table
    """
    table = Table(title="Timing Breakdown", show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Wall time
    wall_time = metrics.get("wall_time", 0)
    table.add_row("Wall Time", _format_time(wall_time))

    # CPU time
    cpu_time = metrics.get("total_cpu_time", 0)
    table.add_row("Total CPU Time", _format_time(cpu_time))

    # Chunk metrics
    num_chunks = metrics.get("num_chunks", 0)
    if num_chunks > 0:
        table.add_row("Number of Chunks", f"{num_chunks:,}")
        avg_cpu_per_chunk = metrics.get("avg_cpu_time_per_chunk", 0)
        table.add_row("Avg CPU Time/Chunk", _format_time(avg_cpu_per_chunk))

    return table


def format_fine_metrics_table(metrics: dict[str, Any]) -> Table | None:
    """Format fine-grained metrics from Dask Spans as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary

    Returns
    -------
    Table or None
        Rich table if fine metrics available, None otherwise
    """
    # Check if any fine metrics are available
    cpu_time = metrics.get("cpu_time_seconds")
    io_time = metrics.get("io_time_seconds")

    if cpu_time is None and io_time is None:
        return None

    table = Table(
        title="Fine Metrics (from Dask Spans)",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # CPU vs I/O breakdown
    if cpu_time is not None:
        table.add_row("CPU Time", _format_time(cpu_time))
    if io_time is not None:
        table.add_row("I/O Time", _format_time(io_time))

    cpu_pct = metrics.get("cpu_percentage")
    io_pct = metrics.get("io_percentage")
    if cpu_pct is not None and io_pct is not None:
        table.add_row("CPU %", f"{cpu_pct:.1f}%")
        table.add_row("I/O %", f"{io_pct:.1f}%")

    # Disk I/O
    disk_read = metrics.get("disk_read_bytes")
    disk_write = metrics.get("disk_write_bytes")
    if disk_read is not None and disk_read > 0:
        table.add_row("Disk Read", _format_bytes(disk_read))
    if disk_write is not None and disk_write > 0:
        table.add_row("Disk Write", _format_bytes(disk_write))

    # Compression overhead
    compress_time = metrics.get("compression_time_seconds")
    decompress_time = metrics.get("decompression_time_seconds")
    total_compression = metrics.get("total_compression_overhead_seconds")

    if total_compression is not None and total_compression > 0:
        table.add_row("Compression Overhead", _format_time(total_compression))
        if compress_time is not None and compress_time > 0:
            table.add_row("  • Compress", _format_time(compress_time))
        if decompress_time is not None and decompress_time > 0:
            table.add_row("  • Decompress", _format_time(decompress_time))

    # Serialization overhead
    serialize_time = metrics.get("serialization_time_seconds")
    deserialize_time = metrics.get("deserialization_time_seconds")
    total_serialization = metrics.get("total_serialization_overhead_seconds")

    if total_serialization is not None and total_serialization > 0:
        table.add_row("Serialization Overhead", _format_time(total_serialization))
        if serialize_time is not None and serialize_time > 0:
            table.add_row("  • Serialize", _format_time(serialize_time))
        if deserialize_time is not None and deserialize_time > 0:
            table.add_row("  • Deserialize", _format_time(deserialize_time))

    return table

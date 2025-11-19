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

    # Data volume from Coffea
    bytes_coffea = metrics.get("total_bytes_read_coffea", 0)
    if bytes_coffea:
        table.add_row(
            "Total Bytes Read (Coffea)",
            f"{_format_bytes(bytes_coffea)}",
        )

    # Data volume from Dask Spans (if available)
    bytes_dask = metrics.get("total_bytes_memory_read_dask", 0)
    if bytes_dask:
        table.add_row(
            "Memory Read (Dask Spans)",
            f"{_format_bytes(bytes_dask)}",
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
    # Check if any fine metrics are available (new names)
    processor_cpu = metrics.get("processor_cpu_time_seconds")
    processor_noncpu = metrics.get("processor_noncpu_time_seconds")
    overhead_cpu = metrics.get("overhead_cpu_time_seconds")
    overhead_noncpu = metrics.get("overhead_noncpu_time_seconds")

    if processor_cpu is None and processor_noncpu is None:
        return None

    table = Table(
        title="Fine Metrics (from Dask Spans)",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Processor CPU vs non-CPU breakdown
    if processor_cpu is not None:
        table.add_row("Processor CPU Time", _format_time(processor_cpu))
    if processor_noncpu is not None:
        table.add_row("Processor Non-CPU Time", _format_time(processor_noncpu))

    processor_cpu_pct = metrics.get("processor_cpu_percentage")
    processor_noncpu_pct = metrics.get("processor_noncpu_percentage")
    if processor_cpu_pct is not None and processor_noncpu_pct is not None:
        table.add_row("  CPU %", f"{processor_cpu_pct:.1f}%")
        table.add_row("  Non-CPU %", f"{processor_noncpu_pct:.1f}%")

    # Dask overhead (if separated)
    if overhead_cpu is not None and overhead_cpu > 0:
        table.add_row("Dask Overhead CPU Time", _format_time(overhead_cpu))
    if overhead_noncpu is not None and overhead_noncpu > 0:
        table.add_row("Dask Overhead Non-CPU Time", _format_time(overhead_noncpu))

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


def format_chunk_metrics_table(metrics: dict[str, Any]) -> Table | None:
    """Format chunk-level metrics as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary

    Returns
    -------
    Table or None
        Rich table, or None if no chunk metrics available
    """
    num_chunks = metrics.get("num_chunks", 0)

    if num_chunks == 0:
        return None

    table = Table(
        title="Chunk Metrics",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Basic stats
    table.add_row("Total Chunks", str(num_chunks))

    num_successful = metrics.get("num_successful_chunks", num_chunks)
    num_failed = metrics.get("num_failed_chunks", 0)
    if num_failed > 0:
        table.add_row("  • Successful", str(num_successful))
        table.add_row("  • Failed", str(num_failed), style="red")

    # Timing statistics
    mean_duration = metrics.get("chunk_duration_mean")
    if mean_duration is not None:
        table.add_row("Mean Chunk Time", _format_time(mean_duration))

        min_duration = metrics.get("chunk_duration_min")
        max_duration = metrics.get("chunk_duration_max")
        std_duration = metrics.get("chunk_duration_std")

        if min_duration is not None:
            table.add_row("  • Min", _format_time(min_duration))
        if max_duration is not None:
            table.add_row("  • Max", _format_time(max_duration))
        if std_duration is not None and std_duration > 0:
            table.add_row("  • Std Dev", _format_time(std_duration))

    # Memory statistics
    mean_mem = metrics.get("chunk_mem_delta_mean_mb")
    if mean_mem is not None:
        table.add_row("Mean Memory Delta", f"{mean_mem:.1f} MB")

        max_mem = metrics.get("chunk_mem_delta_max_mb")
        min_mem = metrics.get("chunk_mem_delta_min_mb")

        if min_mem is not None:
            table.add_row("  • Min", f"{min_mem:.1f} MB")
        if max_mem is not None:
            table.add_row("  • Max", f"{max_mem:.1f} MB")

    # Event statistics
    chunk_events_mean = metrics.get("chunk_events_mean")
    if chunk_events_mean is not None:
        table.add_row("Mean Events/Chunk", f"{chunk_events_mean:.0f}")

        min_events = metrics.get("chunk_events_min")
        max_events = metrics.get("chunk_events_max")

        if min_events is not None:
            table.add_row("  • Min", f"{min_events:.0f}")
        if max_events is not None:
            table.add_row("  • Max", f"{max_events:.0f}")

    # Per-dataset breakdown
    per_dataset = metrics.get("per_dataset")
    if per_dataset and len(per_dataset) > 1:
        table.add_row("", "")  # Spacer
        table.add_row("Per-Dataset Breakdown", "", style="bold")

        for dataset, data in per_dataset.items():
            num_dataset_chunks = data.get("num_chunks", 0)
            mean_time = data.get("mean_duration", 0)
            table.add_row(f"  {dataset}", f"{num_dataset_chunks} chunks, {_format_time(mean_time)} avg")

    # Section timing breakdown
    sections = metrics.get("sections")
    if sections:
        table.add_row("", "")  # Spacer
        table.add_row("Section Timing", "", style="bold")

        # Sort by total duration (most expensive first)
        sorted_sections = sorted(
            sections.items(),
            key=lambda x: x[1].get("total_duration", 0),
            reverse=True,
        )

        for name, data in sorted_sections[:5]:  # Top 5 sections
            mean_time = data.get("mean_duration", 0)
            count = data.get("count", 0)
            section_type = data.get("type", "section")

            if section_type == "memory":
                mean_mem = data.get("mean_mem_delta_mb", 0)
                table.add_row(
                    f"  {name}",
                    f"{_format_time(mean_time)} ({count}x), {mean_mem:.1f} MB avg",
                )
            else:
                table.add_row(f"  {name}", f"{_format_time(mean_time)} ({count}x)")

    return table

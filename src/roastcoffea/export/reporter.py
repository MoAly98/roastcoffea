"""Rich table formatting for metrics reporting."""

from __future__ import annotations

from typing import Any

from rich.table import Table

from roastcoffea.utils import get_nested


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
        Metrics dictionary with 'summary' and 'raw' top-level keys

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

    throughput = get_nested(metrics, "summary", "throughput", default={})

    # Data rate
    data_rate_gbps = throughput.get("data_rate_gbps", 0) or 0
    data_rate_mbps = (data_rate_gbps * 1000) / 8  # Convert Gbps to MB/s
    table.add_row(
        "Data Rate",
        f"{data_rate_gbps:.2f} Gbps ({data_rate_mbps:.1f} MB/s)",
    )

    # Data volume from Coffea
    bytes_coffea = throughput.get("bytes_read", 0)
    if bytes_coffea:
        table.add_row(
            "Total Bytes Read (Coffea)",
            f"{_format_bytes(bytes_coffea)}",
        )

    # Data volume from Dask Spans (if available)
    bytes_dask = throughput.get("bytes_read_spans", 0)
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
        Metrics dictionary with 'summary' and 'raw' top-level keys

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

    events = get_nested(metrics, "summary", "events", default={})

    # Total events
    total_events = events.get("total", 0) or 0
    table.add_row("Total Events", f"{total_events:,}")

    # Event rates
    elapsed_khz = events.get("rate_wall_khz", 0) or 0
    table.add_row("Event Rate (Elapsed Time)", f"{elapsed_khz:.1f} kHz")

    cpu_total_khz = events.get("rate_cpu_khz", 0) or 0
    table.add_row("Event Rate (Total CPU)", f"{cpu_total_khz:.1f} kHz")

    # Core-averaged rate (may be None if no worker data)
    core_khz = events.get("rate_core_khz")
    if core_khz is not None:
        table.add_row("Event Rate (Core-Averaged)", f"{core_khz:.1f} kHz/core")
    else:
        table.add_row("Event Rate (Core-Averaged)", "[dim]N/A (no worker data)[/dim]")

    # Efficiency ratio
    if elapsed_khz and cpu_total_khz:
        efficiency_ratio = elapsed_khz / cpu_total_khz
        table.add_row("Efficiency Ratio", f"{efficiency_ratio:.1%}")

    return table


def format_resources_table(metrics: dict[str, Any]) -> Table:
    """Format resource utilization metrics as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary with 'summary' and 'raw' top-level keys

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

    resources = get_nested(metrics, "summary", "resources", default={})
    efficiency = get_nested(metrics, "summary", "efficiency", default={})

    # Worker metrics
    avg_workers = resources.get("workers_avg")
    if avg_workers is not None:
        table.add_row("Workers (Time-Averaged)", f"{avg_workers:.1f}")
    else:
        table.add_row("Workers (Time-Averaged)", "[dim]N/A (no worker tracking)[/dim]")

    peak_workers = resources.get("workers_peak")
    if peak_workers is not None:
        table.add_row("Peak Workers", f"{peak_workers}")
    else:
        table.add_row("Peak Workers", "[dim]N/A (no worker tracking)[/dim]")

    # Core metrics
    cores_per_worker = resources.get("cores_per_worker")
    if cores_per_worker is not None:
        table.add_row("Cores per Worker", f"{cores_per_worker:.1f}")
    else:
        table.add_row("Cores per Worker", "[dim]N/A (no worker tracking)[/dim]")

    total_cores = resources.get("cores_total")
    if total_cores is not None:
        table.add_row("Total Cores", f"{total_cores:.0f}")
    else:
        table.add_row("Total Cores", "[dim]N/A (no worker tracking)[/dim]")

    # Efficiency
    core_efficiency = efficiency.get("core_efficiency")
    if core_efficiency is not None:
        table.add_row("Core Efficiency", f"{core_efficiency:.1%}")
    else:
        table.add_row("Core Efficiency", "[dim]N/A (no worker tracking)[/dim]")

    # Speedup
    speedup = efficiency.get("speedup")
    if speedup is not None:
        table.add_row("Speedup Factor", f"{speedup:.1f}x")
    else:
        table.add_row("Speedup Factor", "[dim]N/A (no worker tracking)[/dim]")

    # Memory metrics
    peak_memory = resources.get("memory_peak_bytes")
    if peak_memory is not None:
        table.add_row("Peak Memory (per worker)", _format_bytes(peak_memory))
    else:
        table.add_row("Peak Memory (per worker)", "[dim]N/A (no worker tracking)[/dim]")

    avg_memory = resources.get("memory_avg_bytes")
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
        Metrics dictionary with 'summary' and 'raw' top-level keys

    Returns
    -------
    Table
        Rich table
    """
    table = Table(title="Timing Breakdown", show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    timing = get_nested(metrics, "summary", "timing", default={})

    # Elapsed time
    elapsed_time = timing.get("wall_seconds", 0) or 0
    table.add_row("Elapsed Time", _format_time(elapsed_time))

    # CPU time
    cpu_time = timing.get("cpu_seconds", 0) or 0
    table.add_row("Total CPU Time", _format_time(cpu_time))

    # Chunk metrics
    num_chunks = timing.get("num_chunks", 0) or 0
    if num_chunks > 0:
        table.add_row("Number of Chunks", f"{num_chunks:,}")
        avg_cpu_per_chunk = timing.get("avg_chunk_seconds", 0) or 0
        table.add_row("Avg CPU Time/Chunk", _format_time(avg_cpu_per_chunk))

    return table


def format_fine_metrics_table(metrics: dict[str, Any]) -> Table | None:
    """Format fine-grained metrics from Dask Spans as Rich table.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary with 'summary' and 'raw' top-level keys

    Returns
    -------
    Table or None
        Rich table if fine metrics available, None otherwise
    """
    fine = get_nested(metrics, "summary", "fine", default={})

    # Check if any fine metrics are available
    processor_cpu = fine.get("processor_cpu_seconds")
    processor_io = fine.get("processor_io_seconds")
    overhead_cpu = fine.get("overhead_cpu_seconds")
    overhead_io = fine.get("overhead_io_seconds")

    if processor_cpu is None and processor_io is None:
        return None

    table = Table(
        title="Fine Metrics (from Dask Spans)",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Processor CPU vs I/O wait breakdown
    if processor_cpu is not None:
        table.add_row("Processor CPU Time", _format_time(processor_cpu))
    if processor_io is not None:
        table.add_row("Processor I/O & Waiting Time", _format_time(processor_io))

    processor_cpu_pct = fine.get("processor_cpu_percent")
    processor_io_pct = fine.get("processor_io_percent")
    if processor_cpu_pct is not None and processor_io_pct is not None:
        table.add_row("  CPU %", f"{processor_cpu_pct:.1f}%")
        table.add_row("  I/O & Wait %", f"{processor_io_pct:.1f}%")

    # Dask overhead (if separated)
    if overhead_cpu is not None and overhead_cpu > 0:
        table.add_row("Dask Overhead CPU Time", _format_time(overhead_cpu))
    if overhead_io is not None and overhead_io > 0:
        table.add_row("Dask Overhead I/O & Waiting Time", _format_time(overhead_io))

    # Disk I/O
    disk_read = fine.get("disk_read_bytes")
    disk_write = fine.get("disk_write_bytes")
    if disk_read is not None and disk_read > 0:
        table.add_row("Disk Read", _format_bytes(disk_read))
    if disk_write is not None and disk_write > 0:
        table.add_row("Disk Write", _format_bytes(disk_write))

    # Compression overhead
    compress_time = fine.get("compression_seconds")
    decompress_time = fine.get("decompression_seconds")

    if compress_time or decompress_time:
        total_compression = (compress_time or 0) + (decompress_time or 0)
        if total_compression > 0:
            table.add_row("Compression Overhead", _format_time(total_compression))
            if compress_time is not None and compress_time > 0:
                table.add_row("  • Compress", _format_time(compress_time))
            if decompress_time is not None and decompress_time > 0:
                table.add_row("  • Decompress", _format_time(decompress_time))

    # Serialization overhead
    serialize_time = fine.get("serialization_seconds")
    deserialize_time = fine.get("deserialization_seconds")

    if serialize_time or deserialize_time:
        total_serialization = (serialize_time or 0) + (deserialize_time or 0)
        if total_serialization > 0:
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
        Metrics dictionary with 'summary' and 'raw' top-level keys

    Returns
    -------
    Table or None
        Rich table, or None if no chunk metrics available
    """
    chunks = get_nested(metrics, "summary", "chunks", default={})
    num_chunks = chunks.get("num_chunks", 0) or 0

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

    num_successful = chunks.get("num_successful", num_chunks)
    num_failed = chunks.get("num_failed", 0)
    if num_failed and num_failed > 0:
        table.add_row("  • Successful", str(num_successful))
        table.add_row("  • Failed", str(num_failed), style="red")

    # Timing statistics
    mean_duration = chunks.get("duration_mean")
    if mean_duration is not None:
        table.add_row("Mean Chunk Time", _format_time(mean_duration))

        min_duration = chunks.get("duration_min")
        max_duration = chunks.get("duration_max")
        std_duration = chunks.get("duration_std")

        if min_duration is not None:
            table.add_row("  • Min", _format_time(min_duration))
        if max_duration is not None:
            table.add_row("  • Max", _format_time(max_duration))
        if std_duration is not None and std_duration > 0:
            table.add_row("  • Std Dev", _format_time(std_duration))

    # Memory statistics
    mean_mem = chunks.get("mem_delta_mean_mb")
    if mean_mem is not None:
        table.add_row("Mean Memory Delta", f"{mean_mem:.1f} MB")

        max_mem = chunks.get("mem_delta_max_mb")
        min_mem = chunks.get("mem_delta_min_mb")

        if min_mem is not None:
            table.add_row("  • Min", f"{min_mem:.1f} MB")
        if max_mem is not None:
            table.add_row("  • Max", f"{max_mem:.1f} MB")

    # Event statistics
    chunk_events_mean = chunks.get("events_mean")
    if chunk_events_mean is not None:
        table.add_row("Mean Events/Chunk", f"{chunk_events_mean:.0f}")

        min_events = chunks.get("events_min")
        max_events = chunks.get("events_max")

        if min_events is not None:
            table.add_row("  • Min", f"{min_events:.0f}")
        if max_events is not None:
            table.add_row("  • Max", f"{max_events:.0f}")

    # Per-dataset breakdown
    per_dataset = chunks.get("per_dataset")
    if per_dataset and len(per_dataset) > 1:
        table.add_row("", "")  # Spacer
        table.add_row("Per-Dataset Breakdown", "", style="bold")

        for dataset, data in per_dataset.items():
            num_dataset_chunks = data.get("num_chunks", 0)
            mean_time = data.get("mean_duration", 0)
            table.add_row(
                f"  {dataset}",
                f"{num_dataset_chunks} chunks, {_format_time(mean_time)} avg",
            )

    # Section timing breakdown
    sections = chunks.get("sections")
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
                mem_delta = data.get("mean_mem_delta_mb", 0)
                table.add_row(
                    f"  {name}",
                    f"{_format_time(mean_time)} ({count}x), {mem_delta:.1f} MB avg",
                )
            else:
                table.add_row(f"  {name}", f"{_format_time(mean_time)} ({count}x)")

    return table

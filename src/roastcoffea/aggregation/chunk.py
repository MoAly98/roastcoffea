"""Chunk-level metrics aggregation."""

from __future__ import annotations

from typing import Any


def aggregate_chunk_metrics(
    chunk_metrics: list[dict[str, Any]] | None,
    section_metrics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Aggregate chunk-level metrics.

    Parameters
    ----------
    chunk_metrics : list of dict, optional
        List of per-chunk metrics from @track_metrics decorator
    section_metrics : list of dict, optional
        List of section metrics from track_section() and track_memory()

    Returns
    -------
    dict
        Aggregated chunk metrics including:
        - Number of chunks processed
        - Timing statistics (mean, min, max, std)
        - Memory statistics
        - Per-dataset breakdown
        - Section timing breakdown
    """
    result: dict[str, Any] = {}

    if not chunk_metrics:
        result["num_chunks"] = 0
        return result

    result["num_chunks"] = len(chunk_metrics)

    # Filter successful chunks (no errors)
    successful_chunks = [c for c in chunk_metrics if "error" not in c]
    failed_chunks = [c for c in chunk_metrics if "error" in c]

    result["num_successful_chunks"] = len(successful_chunks)
    result["num_failed_chunks"] = len(failed_chunks)

    if not successful_chunks:
        return result

    # Timing statistics
    durations = [c["duration"] for c in successful_chunks]
    result["chunk_duration_mean"] = sum(durations) / len(durations)
    result["chunk_duration_min"] = min(durations)
    result["chunk_duration_max"] = max(durations)

    if len(durations) > 1:
        mean = result["chunk_duration_mean"]
        variance = sum((d - mean) ** 2 for d in durations) / (len(durations) - 1)
        result["chunk_duration_std"] = variance**0.5
    else:
        result["chunk_duration_std"] = 0.0

    # Memory statistics (if available)
    mem_deltas = [c["mem_delta_mb"] for c in successful_chunks if "mem_delta_mb" in c]
    if mem_deltas:
        result["chunk_mem_delta_mean_mb"] = sum(mem_deltas) / len(mem_deltas)
        result["chunk_mem_delta_min_mb"] = min(mem_deltas)
        result["chunk_mem_delta_max_mb"] = max(mem_deltas)

        if len(mem_deltas) > 1:
            mean_mem = result["chunk_mem_delta_mean_mb"]
            variance_mem = sum((m - mean_mem) ** 2 for m in mem_deltas) / (len(mem_deltas) - 1)
            result["chunk_mem_delta_std_mb"] = variance_mem**0.5
        else:
            result["chunk_mem_delta_std_mb"] = 0.0

    # Event statistics (if available)
    event_counts = [c["num_events"] for c in successful_chunks if "num_events" in c]
    if event_counts:
        result["total_events_from_chunks"] = sum(event_counts)
        result["chunk_events_mean"] = sum(event_counts) / len(event_counts)
        result["chunk_events_min"] = min(event_counts)
        result["chunk_events_max"] = max(event_counts)

    # Per-dataset breakdown
    datasets = {}
    for chunk in successful_chunks:
        dataset = chunk.get("dataset", "unknown")
        if dataset not in datasets:
            datasets[dataset] = {
                "num_chunks": 0,
                "total_duration": 0.0,
                "total_events": 0,
            }

        datasets[dataset]["num_chunks"] += 1
        datasets[dataset]["total_duration"] += chunk["duration"]
        if "num_events" in chunk:
            datasets[dataset]["total_events"] += chunk["num_events"]

    # Calculate per-dataset averages
    for dataset, data in datasets.items():
        if data["num_chunks"] > 0:
            data["mean_duration"] = data["total_duration"] / data["num_chunks"]
        if data["total_events"] > 0:
            data["mean_events_per_chunk"] = data["total_events"] / data["num_chunks"]

    result["per_dataset"] = datasets

    # Section timing breakdown (if available)
    if section_metrics:
        sections = {}
        for section in section_metrics:
            name = section.get("name", "unknown")
            if name not in sections:
                sections[name] = {
                    "count": 0,
                    "total_duration": 0.0,
                    "type": section.get("type", "section"),
                }

            sections[name]["count"] += 1
            sections[name]["total_duration"] += section.get("duration", 0.0)

            # Add memory stats for memory tracking
            if section.get("type") == "memory" and "mem_delta_mb" in section:
                if "mem_deltas" not in sections[name]:
                    sections[name]["mem_deltas"] = []
                sections[name]["mem_deltas"].append(section["mem_delta_mb"])

        # Calculate averages
        for name, data in sections.items():
            if data["count"] > 0:
                data["mean_duration"] = data["total_duration"] / data["count"]

            # Memory averages
            if "mem_deltas" in data:
                mem_deltas_section = data["mem_deltas"]
                data["mean_mem_delta_mb"] = sum(mem_deltas_section) / len(mem_deltas_section)
                data["max_mem_delta_mb"] = max(mem_deltas_section)
                data["min_mem_delta_mb"] = min(mem_deltas_section)
                del data["mem_deltas"]  # Remove raw list

        result["sections"] = sections

    return result


def build_chunk_info(chunk_metrics: list[dict[str, Any]]) -> dict[tuple, tuple]:
    """Build chunk_info dict from chunk metrics for throughput plotting.

    Transforms chunk-level metrics collected by @track_metrics into the format
    expected by plot_throughput_timeline().

    Parameters
    ----------
    chunk_metrics : list of dict
        List of chunk metrics dicts from @track_metrics decorator.
        Each dict contains: file, entry_start, entry_stop,
        t_start, t_end, bytes_read

    Returns
    -------
    dict
        Dictionary mapping chunk keys to timing/bytes data:
        {(filename, entry_start, entry_stop): (t_start, t_end, bytes_read)}

    Examples
    --------
    >>> chunk_metrics = [
    ...     {"file": "data.root", "entry_start": 0, "entry_stop": 1000,
    ...      "t_start": 1.0, "t_end": 2.5, "bytes_read": 50000},
    ... ]
    >>> chunk_info = build_chunk_info(chunk_metrics)
    >>> chunk_info
    {('data.root', 0, 1000): (1.0, 2.5, 50000)}

    Notes
    -----
    - Chunks without file/entry metadata are skipped.
    - Chunks without bytes_read default to 0 bytes.
    """
    chunk_info = {}

    for chunk in chunk_metrics:
        # Extract required fields
        filename = chunk.get("file")
        entry_start = chunk.get("entry_start")
        entry_stop = chunk.get("entry_stop")
        t_start = chunk.get("t_start")
        t_end = chunk.get("t_end")
        bytes_read = chunk.get("bytes_read", 0)

        # Skip chunks without essential metadata
        if filename is None or entry_start is None or entry_stop is None:
            continue
        if t_start is None or t_end is None:
            continue

        # Build chunk key and value
        key = (filename, entry_start, entry_stop)
        value = (t_start, t_end, bytes_read)

        chunk_info[key] = value

    return chunk_info

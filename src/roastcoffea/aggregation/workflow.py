"""Workflow-level metrics aggregation.

Calculates throughput, data rates, event rates, compression ratios,
and overall timing metrics from coffea reports and tracking data.
"""

from __future__ import annotations

from typing import Any


def aggregate_workflow_metrics(
    coffea_report: dict[str, Any],
    t_start: float,
    t_end: float,
    custom_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Calculate workflow metrics from Coffea report.

    Parameters
    ----------
    coffea_report : dict
        Coffea report from Runner
    t_start : float
        Start time
    t_end : float
        End time
    custom_metrics : dict, optional
        Per-dataset custom metrics

    Returns
    -------
    dict
        Workflow metrics
    """
    # Calculate wall time
    wall_time = t_end - t_start

    # Extract number of chunks from coffea report
    num_chunks = coffea_report.get("chunks", 0)

    # Combine coffea report and custom metrics into unified structure
    # If custom_metrics provided, use those; otherwise use coffea report as "total"
    combined_report = {}

    if custom_metrics:
        # Custom metrics provide the detailed breakdown
        combined_report.update(custom_metrics)
    elif "bytesread" in coffea_report:
        # Convert coffea report to "total" dataset if no custom metrics
        combined_report["total"] = {
            "entries": coffea_report.get("entries", 0),
            "duration": coffea_report.get("processtime", wall_time),
            "performance_counters": {
                "num_requested_bytes": coffea_report.get("bytesread", 0)
            },
        }

    # Extract and aggregate metrics from combined report
    total_bytes_compressed = 0
    total_events = 0
    total_cpu_time = 0

    for _dataset_name, dataset_data in combined_report.items():
        # Skip non-dataset entries
        if not isinstance(dataset_data, dict):
            continue

        # Get performance counters
        perf_counters = dataset_data.get("performance_counters", {})
        total_bytes_compressed += perf_counters.get("num_requested_bytes", 0)

        # Get events and duration
        total_events += dataset_data.get("entries", 0)
        total_cpu_time += dataset_data.get("duration", 0)

    # Uncompressed bytes and compression ratio not available from Coffea report
    # Will be available when Dask Spans integration is added
    total_bytes_uncompressed = None
    compression_ratio = None

    # Calculate throughput metrics (based on compressed bytes)
    overall_rate_gbps = (
        (total_bytes_compressed * 8 / 1e9) / wall_time if wall_time > 0 else 0
    )
    overall_rate_mbps = (
        (total_bytes_compressed / 1e6) / wall_time if wall_time > 0 else 0
    )

    # Calculate event rate metrics
    event_rate_wall_khz = (total_events / wall_time) / 1000 if wall_time > 0 else 0
    event_rate_agg_khz = (
        (total_events / total_cpu_time) / 1000 if total_cpu_time > 0 else 0
    )

    # Calculate chunk-level metrics
    avg_cpu_time_per_chunk = total_cpu_time / num_chunks if num_chunks > 0 else 0

    # Build metrics dictionary
    return {
        # Throughput metrics
        "overall_rate_gbps": overall_rate_gbps,
        "overall_rate_mbps": overall_rate_mbps,
        "compression_ratio": compression_ratio,
        # Event processing metrics
        "total_events": total_events,
        "event_rate_wall_khz": event_rate_wall_khz,
        "event_rate_agg_khz": event_rate_agg_khz,
        # Timing metrics
        "wall_time": wall_time,
        "total_cpu_time": total_cpu_time,
        "num_chunks": num_chunks,
        "avg_cpu_time_per_chunk": avg_cpu_time_per_chunk,
        # Data volume metrics
        "total_bytes_compressed": total_bytes_compressed,
        "total_bytes_uncompressed": total_bytes_uncompressed,
    }

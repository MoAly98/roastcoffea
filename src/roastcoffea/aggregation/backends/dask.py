"""Dask-specific aggregation parsers.

Parses Dask scheduler tracking data and (v0.2+) fine metrics
into standardized worker metrics dictionaries.
"""

from __future__ import annotations

from typing import Any


def parse_tracking_data(tracking_data: dict[str, Any]) -> dict[str, Any]:
    """Parse Dask scheduler tracking data into aggregated metrics.

    Parameters
    ----------
    tracking_data : dict
        Raw tracking data from DaskMetricsBackend.stop_tracking()

    Returns
    -------
    dict
        Aggregated worker metrics
    """
    # Stub implementation
    return {
        "avg_workers": 0.0,
        "peak_workers": 0,
        "total_cores": None,
        "peak_memory_bytes": 0.0,
        "avg_memory_per_worker_bytes": 0.0,
    }


def calculate_time_averaged_workers(worker_counts: dict) -> float:
    """Calculate time-weighted average worker count.

    Parameters
    ----------
    worker_counts : dict
        Mapping from datetime to worker count

    Returns
    -------
    float
        Time-averaged worker count
    """
    # Stub implementation
    return 0.0


def calculate_peak_memory(worker_memory: dict) -> float:
    """Calculate peak memory usage across all workers.

    Parameters
    ----------
    worker_memory : dict
        Dictionary from tracking data: worker_id -> [(timestamp, memory_bytes), ...]

    Returns
    -------
    float
        Maximum memory usage observed
    """
    # Stub implementation
    return 0.0


def calculate_average_memory_per_worker(worker_memory: dict) -> float:
    """Calculate time-weighted average memory per worker.

    Parameters
    ----------
    worker_memory : dict
        Dictionary from tracking data: worker_id -> [(timestamp, memory_bytes), ...]

    Returns
    -------
    float
        Average memory per worker
    """
    # Stub implementation
    return 0.0

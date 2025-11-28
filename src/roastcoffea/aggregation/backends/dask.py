"""Dask-specific aggregation parsers.

Parses Dask scheduler tracking data and (v0.2+) fine metrics
into standardized worker metrics dictionaries.
"""

from __future__ import annotations

import datetime
from typing import Any

import numpy as np

from roastcoffea.aggregation.backends.base import AbstractTrackingDataParser


class DaskTrackingDataParser(AbstractTrackingDataParser):
    """Parser for Dask scheduler tracking data."""

    def parse_tracking_data(self, tracking_data: dict[str, Any]) -> dict[str, Any]:
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
        worker_counts = tracking_data.get("worker_counts", {})
        worker_memory = tracking_data.get("worker_memory", {})
        worker_cores = tracking_data.get("worker_cores", {})

        # Calculate worker metrics
        avg_workers = calculate_time_averaged_workers(worker_counts)
        peak_workers = max(worker_counts.values()) if worker_counts else 0

        # Calculate total cores and cores per worker from per-worker core tracking
        total_cores = None
        cores_per_worker = None
        if worker_cores:
            # Sum cores across all workers (use latest value for each worker)
            cores_sum = 0
            core_counts = []
            for _worker_id, timeline in worker_cores.items():
                if timeline:
                    # Use the latest (or any) core count for this worker
                    # Cores don't change over time, so any value is fine
                    worker_core_count = timeline[-1][1]
                    cores_sum += worker_core_count
                    core_counts.append(worker_core_count)

            if cores_sum > 0:
                total_cores = float(cores_sum)
                # Calculate average cores per worker
                cores_per_worker = float(np.mean(core_counts)) if core_counts else None

        # Calculate memory metrics
        peak_memory_bytes = calculate_peak_memory(worker_memory)
        avg_memory_per_worker_bytes = calculate_average_memory_per_worker(worker_memory)

        return {
            "avg_workers": avg_workers,
            "peak_workers": peak_workers,
            "total_cores": total_cores,
            "cores_per_worker": cores_per_worker,
            "peak_memory_bytes": peak_memory_bytes,
            "avg_memory_per_worker_bytes": avg_memory_per_worker_bytes,
        }


def calculate_time_averaged_workers(
    worker_counts: dict[datetime.datetime, int],
) -> float:
    """Calculate time-weighted average worker count.

    Uses trapezoidal integration to compute the average number of workers
    weighted by the time each count was active.

    Parameters
    ----------
    worker_counts : dict
        Mapping from datetime to worker count

    Returns
    -------
    float
        Time-averaged worker count
    """
    if not worker_counts:
        return 0.0

    if len(worker_counts) < 2:
        return float(next(iter(worker_counts.values())))

    # Sort by timestamp
    sorted_items = sorted(worker_counts.items())
    timestamps = [t for t, _ in sorted_items]
    counts = [c for _, c in sorted_items]

    # Convert to seconds since first sample
    t0 = timestamps[0]
    times = np.array([(t - t0).total_seconds() for t in timestamps])
    worker_array = np.array(counts, dtype=float)

    # Calculate time intervals
    delta_t = np.diff(times)

    # Trapezoidal integration: area = (y1 + y2) / 2 * delta_t
    workers_times_time = [
        (worker_array[i] + worker_array[i + 1]) / 2 * delta_t[i]
        for i in range(len(delta_t))
    ]

    # Time-weighted average
    total_time = times[-1] - times[0]
    return sum(workers_times_time) / total_time


def calculate_peak_memory(worker_memory: dict[str, list[tuple]]) -> float:
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
    if not worker_memory:
        return 0.0

    all_memory_values = []
    for _worker_id, timeline in worker_memory.items():
        for _timestamp, memory_bytes in timeline:
            all_memory_values.append(memory_bytes)

    return max(all_memory_values) if all_memory_values else 0.0


def calculate_average_memory_per_worker(
    worker_memory: dict[str, list[tuple]],
) -> float:
    """Calculate time-weighted average memory per worker.

    Computes time-weighted average for each worker, then averages across workers.

    Parameters
    ----------
    worker_memory : dict
        Dictionary from tracking data: worker_id -> [(timestamp, memory_bytes), ...]

    Returns
    -------
    float
        Average memory per worker
    """
    if not worker_memory:
        return 0.0

    worker_averages = []

    for _worker_id, timeline in worker_memory.items():
        if len(timeline) < 2:
            if timeline:
                worker_averages.append(timeline[0][1])
            continue

        # Sort by timestamp
        sorted_timeline = sorted(timeline, key=lambda x: x[0])

        # Extract timestamps and memory values
        timestamps = [t for t, m in sorted_timeline]
        memory_values = [m for t, m in sorted_timeline]

        # Convert to seconds since first sample
        t0 = timestamps[0]
        times = np.array([(t - t0).total_seconds() for t in timestamps])
        memory = np.array(memory_values, dtype=float)

        # Calculate time intervals
        delta_t = np.diff(times)

        # Trapezoidal integration
        memory_times_time = [
            (memory[i] + memory[i + 1]) / 2 * delta_t[i] for i in range(len(delta_t))
        ]

        # Time-weighted average for this worker
        total_time = times[-1] - times[0]
        worker_avg = sum(memory_times_time) / total_time
        worker_averages.append(worker_avg)

    # Average across all workers
    return float(np.mean(worker_averages)) if worker_averages else 0.0

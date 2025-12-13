"""Efficiency metrics calculation.

Calculates core efficiency, speedup factors, and per-core event rates
from workflow and worker metrics.
"""

from __future__ import annotations

from typing import Any


def calculate_efficiency_metrics(
    workflow_metrics: dict[str, Any],
    worker_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Calculate efficiency metrics from workflow and worker data.

    Parameters
    ----------
    workflow_metrics : dict
        Workflow metrics from aggregate_workflow_metrics()
    worker_metrics : dict
        Worker metrics from parse_tracking_data()

    Returns
    -------
    dict
        Efficiency metrics
    """
    elapsed_time_seconds = workflow_metrics.get("elapsed_time_seconds", 0)
    total_cpu_time = workflow_metrics.get("total_cpu_time", 0)
    total_events = workflow_metrics.get("total_events", 0)
    total_cores = worker_metrics.get("total_cores")

    # Calculate core efficiency
    core_efficiency = None
    if total_cores is not None:
        if elapsed_time_seconds > 0:
            total_available_time = total_cores * elapsed_time_seconds
            core_efficiency = (
                total_cpu_time / total_available_time
                if total_available_time > 0
                else 0.0
            )
        else:
            core_efficiency = 0.0

    # Calculate speedup factor
    speedup_factor = (
        total_cpu_time / elapsed_time_seconds if elapsed_time_seconds > 0 else 0.0
    )

    # Calculate event rate per core (in kHz for consistency)
    event_rate_core_khz = None
    if total_cores is not None:
        if elapsed_time_seconds > 0:
            event_rate_core_khz = (
                total_events / (elapsed_time_seconds * total_cores)
            ) / 1000
        else:
            event_rate_core_khz = 0.0

    return {
        "core_efficiency": core_efficiency,
        "speedup_factor": speedup_factor,
        "event_rate_core_khz": event_rate_core_khz,
    }

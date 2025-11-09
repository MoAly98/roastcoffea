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
    wall_time = workflow_metrics.get("wall_time", 0)
    total_cpu_time = workflow_metrics.get("total_cpu_time", 0)
    total_events = workflow_metrics.get("total_events", 0)
    total_cores = worker_metrics.get("total_cores")

    # Calculate core efficiency
    core_efficiency = None
    if total_cores is not None:
        if wall_time > 0:
            total_available_time = total_cores * wall_time
            core_efficiency = (
                total_cpu_time / total_available_time if total_available_time > 0 else 0.0
            )
        else:
            core_efficiency = 0.0

    # Calculate speedup factor
    speedup_factor = total_cpu_time / wall_time if wall_time > 0 else 0.0

    # Calculate event rate per core
    event_rate_core_hz = None
    if total_cores is not None:
        if wall_time > 0:
            event_rate_core_hz = total_events / (wall_time * total_cores)
        else:
            event_rate_core_hz = 0.0

    return {
        "core_efficiency": core_efficiency,
        "speedup_factor": speedup_factor,
        "event_rate_core_hz": event_rate_core_hz,
    }

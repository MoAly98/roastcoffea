"""Individual plot functions for metrics visualization.

Supports both static (matplotlib) and interactive (bokeh) outputs.
"""

from __future__ import annotations

from roastcoffea.visualization.plots.cpu import (
    plot_executing_tasks_timeline,
    plot_occupancy_timeline,
)
from roastcoffea.visualization.plots.memory import plot_memory_utilization_timeline
from roastcoffea.visualization.plots.scaling import (
    plot_efficiency_summary,
    plot_resource_utilization,
)
from roastcoffea.visualization.plots.throughput import (
    plot_total_active_tasks_timeline,
    plot_worker_activity_timeline,
)
from roastcoffea.visualization.plots.workers import plot_worker_count_timeline

__all__ = [
    "plot_efficiency_summary",
    "plot_executing_tasks_timeline",
    "plot_memory_utilization_timeline",
    "plot_occupancy_timeline",
    "plot_resource_utilization",
    "plot_total_active_tasks_timeline",
    "plot_worker_activity_timeline",
    "plot_worker_count_timeline",
]

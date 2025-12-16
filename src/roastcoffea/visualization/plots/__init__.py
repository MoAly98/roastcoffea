"""Individual plot functions for metrics visualization.

Supports both static (matplotlib) and interactive (bokeh) outputs.
"""

from __future__ import annotations

from roastcoffea.visualization.plots.chunks import (
    plot_runtime_distribution,
    plot_runtime_vs_events,
)
from roastcoffea.visualization.plots.cpu import (
    plot_cpu_utilization_mean_timeline,
    plot_cpu_utilization_per_worker_timeline,
    plot_executing_tasks_timeline,
    plot_occupancy_timeline,
)
from roastcoffea.visualization.plots.io import (
    plot_branch_access_per_chunk,
    plot_bytes_accessed_per_chunk,
    plot_compression_ratio_distribution,
    plot_data_access_percentage,
)
from roastcoffea.visualization.plots.memory import (
    plot_memory_utilization_mean_timeline,
    plot_memory_utilization_per_worker_timeline,
)
from roastcoffea.visualization.plots.per_task import (
    plot_per_task_bytes_read,
    plot_per_task_cpu_io,
    plot_per_task_overhead,
)
from roastcoffea.visualization.plots.scaling import (
    plot_efficiency_summary,
    plot_resource_utilization,
)
from roastcoffea.visualization.plots.throughput import (
    plot_throughput_timeline,
    plot_total_active_tasks_timeline,
    plot_worker_activity_timeline,
)
from roastcoffea.visualization.plots.workers import plot_worker_count_timeline

__all__ = [
    "plot_branch_access_per_chunk",
    "plot_bytes_accessed_per_chunk",
    "plot_compression_ratio_distribution",
    "plot_cpu_utilization_mean_timeline",
    "plot_cpu_utilization_per_worker_timeline",
    "plot_data_access_percentage",
    "plot_efficiency_summary",
    "plot_executing_tasks_timeline",
    "plot_memory_utilization_mean_timeline",
    "plot_memory_utilization_per_worker_timeline",
    "plot_occupancy_timeline",
    "plot_per_task_bytes_read",
    "plot_per_task_cpu_io",
    "plot_per_task_overhead",
    "plot_resource_utilization",
    "plot_runtime_distribution",
    "plot_runtime_vs_events",
    "plot_throughput_timeline",
    "plot_total_active_tasks_timeline",
    "plot_worker_activity_timeline",
    "plot_worker_count_timeline",
]

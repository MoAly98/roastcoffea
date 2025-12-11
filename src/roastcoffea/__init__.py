"""roastcoffea: Comprehensive performance monitoring for Coffea workflows.

This package provides tools for collecting, analyzing, and visualizing performance
metrics from Coffea-based High Energy Physics analysis workflows.

Main Features:
- Automatic worker and resource tracking
- Fine-grained performance metrics via Dask Spans
- Chunk-level instrumentation with decorators
- Rich visualization and reporting
- Multiple backend support (Dask, future: TaskVine)

Basic Usage:
    from roastcoffea import MetricsCollector
    from coffea.processor import Runner, DaskExecutor

    executor = DaskExecutor(client=dask_client)

    with MetricsCollector(executor, output_dir="benchmarks") as mc:
        runner = Runner(executor=executor)
        output, report = runner(fileset, processor_instance)
        mc.set_coffea_report(report)

    # Metrics auto-saved to benchmarks/<timestamp>/
    print(f"Throughput: {mc.metrics['overall_rate_gbps']:.2f} Gbps")
"""

from __future__ import annotations

from roastcoffea.collector import MetricsCollector
from roastcoffea.decorator import track_metrics
from roastcoffea.export.measurements import load_measurement
from roastcoffea.instrumentation import track_bytes, track_memory, track_time
from roastcoffea.visualization.plots.chunks import (
    plot_runtime_distribution,
    plot_runtime_vs_events,
)
from roastcoffea.visualization.plots.cpu import (
    plot_cpu_utilization_timeline,
    plot_executing_tasks_timeline,
    plot_occupancy_timeline,
)
from roastcoffea.visualization.plots.io import (
    plot_compression_ratio_distribution,
    plot_data_access_percentage,
)
from roastcoffea.visualization.plots.memory import plot_memory_utilization_timeline
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
    plot_total_active_tasks_timeline,
    plot_worker_activity_timeline,
)
from roastcoffea.visualization.plots.workers import plot_worker_count_timeline

__version__ = "0.1.0"

__all__ = [
    "MetricsCollector",
    "__version__",
    "load_measurement",
    # Instrumentation
    "track_metrics",
    "track_time",
    "track_memory",
    "track_bytes",
    # Worker timeline plots
    "plot_worker_count_timeline",
    "plot_memory_utilization_timeline",
    "plot_cpu_utilization_timeline",
    "plot_occupancy_timeline",
    "plot_executing_tasks_timeline",
    "plot_worker_activity_timeline",
    "plot_total_active_tasks_timeline",
    # Summary plots
    "plot_efficiency_summary",
    "plot_resource_utilization",
    # Per-task fine metrics plots
    "plot_per_task_cpu_io",
    "plot_per_task_bytes_read",
    "plot_per_task_overhead",
    # I/O analysis plots
    "plot_compression_ratio_distribution",
    "plot_data_access_percentage",
    # Chunk-level plots
    "plot_runtime_distribution",
    "plot_runtime_vs_events",
]

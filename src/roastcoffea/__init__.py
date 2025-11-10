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
from roastcoffea.export.measurements import load_measurement
from roastcoffea.visualization.plots import (
    plot_memory_utilization_timeline,
    plot_worker_count_timeline,
)

__version__ = "0.1.0"

__all__ = [
    "MetricsCollector",
    "__version__",
    "load_measurement",
    "plot_memory_utilization_timeline",
    "plot_worker_count_timeline",
]

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

    with MetricsCollector(client, processor_instance=processor) as collector:
        runner = Runner(executor=executor)
        output, report = runner(fileset, processor_instance=processor)
        collector.set_coffea_report(report)

    # Metrics auto-saved and available
    print(f"Throughput: {collector.metrics['data_rate_gbps']:.2f} Gbps")
"""

from __future__ import annotations

from roastcoffea.collector import MetricsCollector
from roastcoffea.decorator import track_metrics
from roastcoffea.export.measurements import load_measurement
from roastcoffea.instrumentation import track_bytes, track_memory, track_time

__version__ = "0.1.1"

__all__ = [
    "MetricsCollector",
    "__version__",
    "load_measurement",
    "track_bytes",
    "track_memory",
    "track_metrics",
    "track_time",
]

"""Metrics collector context manager.

Main entry point for comprehensive metrics collection.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from rich.console import Console

from roastcoffea.aggregation.core import MetricsAggregator
from roastcoffea.backends.dask import DaskMetricsBackend
from roastcoffea.export.measurements import save_measurement
from roastcoffea.export.reporter import (
    format_event_processing_table,
    format_resources_table,
    format_throughput_table,
    format_timing_table,
)


class MetricsCollector:
    """Context manager for collecting workflow metrics.

    This is the main user-facing API for metrics collection.

    Parameters
    ----------
    client : distributed.Client
        Dask distributed client
    backend : str, optional
        Backend name (default: "dask")
    track_workers : bool, optional
        Enable worker tracking (default: True)
    worker_tracking_interval : float, optional
        Sampling interval in seconds (default: 1.0)

    Examples
    --------
    >>> from dask.distributed import Client
    >>> from roastcoffea.collector import MetricsCollector
    >>>
    >>> client = Client()
    >>> with MetricsCollector(client) as collector:
    ...     output, report = runner(...)
    ...     collector.set_coffea_report(report)
    >>>
    >>> metrics = collector.metrics
    >>> print(f"Throughput: {metrics['overall_rate_gbps']:.2f} Gbps")
    """

    def __init__(
        self,
        client: Any,
        backend: str = "dask",
        track_workers: bool = True,
        worker_tracking_interval: float = 1.0,
    ) -> None:
        """Initialize MetricsCollector."""
        self.client = client
        self.backend = backend
        self.track_workers = track_workers
        self.worker_tracking_interval = worker_tracking_interval

        # Initialize backend
        if backend == "dask":
            self.metrics_backend = DaskMetricsBackend(client=client)
        else:
            msg = f"Unsupported backend: {backend}"
            raise ValueError(msg)

        # Initialize aggregator
        self.aggregator = MetricsAggregator(backend=backend)

        # State
        self.t_start: float | None = None
        self.t_end: float | None = None
        self.coffea_report: dict[str, Any] | None = None
        self.tracking_data: dict[str, Any] | None = None
        self.metrics: dict[str, Any] | None = None

    def __enter__(self) -> MetricsCollector:
        """Enter context manager - start tracking."""
        self.t_start = time.perf_counter()

        if self.track_workers:
            self.metrics_backend.start_tracking(interval=self.worker_tracking_interval)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - stop tracking and aggregate metrics."""
        self.t_end = time.perf_counter()

        if self.track_workers:
            self.tracking_data = self.metrics_backend.stop_tracking()

        # Auto-aggregate if we have a coffea report
        if self.coffea_report is not None:
            self._aggregate_metrics()

    def set_coffea_report(
        self, report: dict[str, Any], custom_metrics: dict[str, Any] | None = None
    ) -> None:
        """Set Coffea report and optionally aggregate immediately.

        Parameters
        ----------
        report : dict
            Coffea report from Runner
        custom_metrics : dict, optional
            Per-dataset custom metrics
        """
        self.coffea_report = report
        self.custom_metrics = custom_metrics

    def _aggregate_metrics(self) -> None:
        """Aggregate all metrics."""
        if self.t_start is None or self.t_end is None:
            msg = "Timing not available - use within context manager"
            raise RuntimeError(msg)

        if self.coffea_report is None:
            msg = "Coffea report not set - call set_coffea_report() first"
            raise RuntimeError(msg)

        self.metrics = self.aggregator.aggregate(
            coffea_report=self.coffea_report,
            tracking_data=self.tracking_data,
            t_start=self.t_start,
            t_end=self.t_end,
            custom_metrics=getattr(self, "custom_metrics", None),
        )

    def get_metrics(self) -> dict[str, Any]:
        """Get aggregated metrics.

        Returns
        -------
        dict
            Aggregated metrics dictionary
        """
        if self.metrics is None:
            self._aggregate_metrics()

        return self.metrics

    def save_measurement(
        self, output_dir: Path, measurement_name: str | None = None
    ) -> Path:
        """Save measurement to disk.

        Parameters
        ----------
        output_dir : Path
            Output directory
        measurement_name : str, optional
            Measurement name

        Returns
        -------
        Path
            Path to measurement directory
        """
        metrics = self.get_metrics()

        return save_measurement(
            metrics=metrics,
            t0=self.t_start,
            t1=self.t_end,
            output_dir=output_dir,
            measurement_name=measurement_name,
        )

    def print_summary(self) -> None:
        """Print Rich table summary of metrics."""
        console = Console()
        metrics = self.get_metrics()

        console.print()
        console.print(format_throughput_table(metrics))
        console.print()
        console.print(format_event_processing_table(metrics))
        console.print()
        console.print(format_resources_table(metrics))
        console.print()
        console.print(format_timing_table(metrics))
        console.print()

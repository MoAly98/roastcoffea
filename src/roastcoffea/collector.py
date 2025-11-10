"""Metrics collector context manager.

Main entry point for comprehensive metrics collection.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from rich.console import Console

from roastcoffea.aggregation.core import MetricsAggregator
from roastcoffea.backends.dask import DaskMetricsBackend
from roastcoffea.export.measurements import save_measurement
from roastcoffea.export.reporter import (
    format_event_processing_table,
    format_fine_metrics_table,
    format_resources_table,
    format_throughput_table,
    format_timing_table,
)

logger = logging.getLogger(__name__)


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
    processor_instance : ProcessorABC, optional
        Coffea processor instance. If provided, fine metrics will separate
        processor work from Dask overhead. Without this, all activities
        (including Dask internals) are aggregated together.

    Examples
    --------
    >>> from dask.distributed import Client
    >>> from roastcoffea.collector import MetricsCollector
    >>>
    >>> # Basic usage (aggregates all activities)
    >>> client = Client()
    >>> with MetricsCollector(client) as collector:
    ...     output, report = runner(...)
    ...     collector.set_coffea_report(report)
    >>>
    >>> # Recommended: separate processor from overhead
    >>> processor = MyProcessor()
    >>> with MetricsCollector(client, processor_instance=processor) as collector:
    ...     output, report = runner(fileset, processor_instance=processor)
    ...     collector.set_coffea_report(report)
    >>>
    >>> metrics = collector.metrics
    >>> print(f"Throughput: {metrics['overall_rate_gbps']:.2f} Gbps")
    >>> print(f"Processor CPU: {metrics['processor_cpu_time_seconds']:.2f}s")
    >>> print(f"Dask overhead: {metrics['overhead_cpu_time_seconds']:.2f}s")
    """

    def __init__(
        self,
        client: Any,
        backend: str = "dask",
        track_workers: bool = True,
        worker_tracking_interval: float = 1.0,
        processor_instance: Any = None,
    ) -> None:
        """Initialize MetricsCollector."""
        self.client = client
        self.backend = backend
        self.track_workers = track_workers
        self.worker_tracking_interval = worker_tracking_interval
        self.processor_instance = processor_instance

        # Get processor name for filtering metrics
        if processor_instance is not None:
            self.processor_name = processor_instance.__class__.__name__
        else:
            self.processor_name = None

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
        self.span_info: dict[str, Any] | None = None
        self.span_metrics: dict[str, Any] | None = None
        self.metrics: dict[str, Any] | None = None

    def __enter__(self) -> MetricsCollector:
        """Enter context manager - start tracking."""
        self.t_start = time.perf_counter()

        if self.track_workers:
            self.metrics_backend.start_tracking(interval=self.worker_tracking_interval)

        # Create Span for fine-grained metrics
        self.span_info = self.metrics_backend.create_span("coffea-processing")
        if self.span_info is not None:
            try:
                # Enter the span context and capture the span ID
                span_id = self.span_info["context"].__enter__()
                self.span_info["id"] = span_id
                logger.debug(f"Dask Span created successfully (ID: {span_id}) for fine metrics collection")
            except Exception as e:
                logger.warning(
                    f"Failed to enter Dask Span context: {e}. "
                    "Fine-grained metrics will not be collected."
                )
                self.span_info = None

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - stop tracking and aggregate metrics."""
        self.t_end = time.perf_counter()

        # Exit span context and extract metrics
        if self.span_info is not None:
            try:
                self.span_info["context"].__exit__(exc_type, exc_val, exc_tb)
                self.span_metrics = self.metrics_backend.get_span_metrics(self.span_info)
                if self.span_metrics:
                    logger.debug(f"Collected {len(self.span_metrics)} fine metrics from Dask Span")
                else:
                    logger.warning(
                        "Dask Span completed but no metrics were collected. "
                        "Fine-grained metrics may not be available."
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to extract metrics from Dask Span: {e}. "
                    "Fine-grained metrics will not be available."
                )
                self.span_metrics = None

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

        # Warn if span metrics collected without processor_name
        if self.span_metrics and self.processor_name is None:
            logger.warning(
                "Fine metrics will be aggregated from ALL task activities (including Dask overhead). "
                "To separate processor metrics from Dask overhead, pass processor_instance to MetricsCollector: "
                "MetricsCollector(client, processor_instance=my_processor)"
            )

        self.metrics = self.aggregator.aggregate(
            coffea_report=self.coffea_report,
            tracking_data=self.tracking_data,
            t_start=self.t_start,
            t_end=self.t_end,
            custom_metrics=getattr(self, "custom_metrics", None),
            span_metrics=self.span_metrics,
            processor_name=self.processor_name,
        )

    def get_metrics(self) -> dict[str, Any]:
        """Get aggregated metrics.

        Returns
        -------
        dict
            Aggregated metrics dictionary

        Raises
        ------
        RuntimeError
            If metrics aggregation failed
        """
        if self.metrics is None:
            self._aggregate_metrics()

        if self.metrics is None:
            msg = "Metrics aggregation failed - this should not happen"
            raise RuntimeError(msg)

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
        if self.t_start is None or self.t_end is None:
            msg = "Cannot save measurement before context manager completes"
            raise RuntimeError(msg)

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

        # Print fine metrics table if available
        fine_table = format_fine_metrics_table(metrics)
        if fine_table is not None:
            console.print()
            console.print(fine_table)

        console.print()

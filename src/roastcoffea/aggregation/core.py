"""Core metrics aggregator combining all aggregation modules."""

from __future__ import annotations

from typing import Any

from roastcoffea.aggregation.backends import get_parser
from roastcoffea.aggregation.chunk import aggregate_chunk_metrics
from roastcoffea.aggregation.chunks import build_chunk_info
from roastcoffea.aggregation.efficiency import calculate_efficiency_metrics
from roastcoffea.aggregation.fine_metrics import parse_fine_metrics
from roastcoffea.aggregation.workflow import aggregate_workflow_metrics


class MetricsAggregator:
    """Main aggregator combining workflow, worker, and efficiency metrics."""

    def __init__(self, backend: str) -> None:
        """Initialize aggregator for specific backend.

        Parameters
        ----------
        backend : str
            Backend name ("dask", "taskvine", etc.)

        Raises
        ------
        ValueError
            If backend is not supported
        """
        self.backend = backend
        self.parser = get_parser(backend)

    def aggregate(
        self,
        coffea_report: dict[str, Any],
        tracking_data: dict[str, Any] | None,
        t_start: float,
        t_end: float,
        custom_metrics: dict[str, Any] | None = None,
        span_metrics: dict[str, Any] | None = None,
        processor_name: str | None = None,
        chunk_metrics: list[dict[str, Any]] | None = None,
        section_metrics: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Aggregate all metrics from workflow run.

        Parameters
        ----------
        coffea_report : dict
            Coffea report
        tracking_data : dict, optional
            Backend tracking data
        t_start : float
            Start time
        t_end : float
            End time
        custom_metrics : dict, optional
            Per-dataset metrics
        span_metrics : dict, optional
            Dask Spans cumulative_worker_metrics
        processor_name : str, optional
            Name of processor class for filtering fine metrics
        chunk_metrics : list of dict, optional
            Per-chunk metrics from @track_metrics decorator
        section_metrics : list of dict, optional
            Section metrics from track_section() and track_memory()

        Returns
        -------
        dict
            Combined metrics
        """
        # Aggregate workflow metrics
        workflow_metrics = aggregate_workflow_metrics(
            coffea_report=coffea_report,
            t_start=t_start,
            t_end=t_end,
            custom_metrics=custom_metrics,
        )

        # Parse worker metrics if tracking data available
        worker_metrics = {}
        if tracking_data is not None:
            worker_metrics = self.parser.parse_tracking_data(tracking_data)

        # Parse fine metrics from Spans if available
        fine_metrics = {}
        if span_metrics:
            fine_metrics = parse_fine_metrics(span_metrics, processor_name=processor_name)

            # Update compression metrics with real data from Spans
            # Don't calculate compression ratio - the two metrics measure different things:
            # - Coffea bytesread: compressed bytes from file
            # - Dask memory-read: incomplete tracking of in-memory access
            # We don't have enough information to compute a valid compression ratio

        # Aggregate chunk metrics if available
        chunk_agg_metrics = {}
        if chunk_metrics:
            chunk_agg_metrics = aggregate_chunk_metrics(
                chunk_metrics=chunk_metrics,
                section_metrics=section_metrics,
            )

            # Build chunk_info for throughput plotting
            # This transforms chunk metrics into the format expected by plot_throughput_timeline()
            chunk_info = build_chunk_info(chunk_metrics)
            if chunk_info:
                # Inject into coffea_report so plotting functions can find it
                coffea_report["chunk_info"] = chunk_info

        # Calculate efficiency metrics
        efficiency_metrics = calculate_efficiency_metrics(
            workflow_metrics=workflow_metrics,
            worker_metrics=worker_metrics,
        )

        # Combine all metrics
        combined_metrics = {}
        combined_metrics.update(workflow_metrics)
        combined_metrics.update(worker_metrics)
        combined_metrics.update(efficiency_metrics)
        combined_metrics.update(fine_metrics)
        combined_metrics.update(chunk_agg_metrics)

        # Preserve raw tracking data for visualization
        combined_metrics["tracking_data"] = tracking_data

        # Preserve raw chunk metrics for detailed analysis
        if chunk_metrics:
            combined_metrics["raw_chunk_metrics"] = chunk_metrics
        if section_metrics:
            combined_metrics["raw_section_metrics"] = section_metrics

        return combined_metrics

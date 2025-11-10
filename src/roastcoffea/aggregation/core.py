"""Core metrics aggregator combining all aggregation modules."""

from __future__ import annotations

from typing import Any

from roastcoffea.aggregation.backends import get_parser
from roastcoffea.aggregation.efficiency import calculate_efficiency_metrics
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

        # Preserve raw tracking data for visualization
        combined_metrics["tracking_data"] = tracking_data

        return combined_metrics

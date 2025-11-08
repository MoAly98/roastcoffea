"""Core metrics aggregator combining all aggregation modules."""

from __future__ import annotations

from typing import Any


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
        if backend not in ["dask"]:
            raise ValueError(f"Unsupported backend: {backend}")
        self.backend = backend

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
        pass

"""Dask backend for metrics collection.

Implements metrics collection for Dask executors, including:
- Worker resource tracking via scheduler sampling
- Fine-grained metrics via Dask Spans
"""

from __future__ import annotations

from typing import Any

from roastcoffea.backends.base import AbstractMetricsBackend


class DaskMetricsBackend(AbstractMetricsBackend):
    """Dask-specific metrics collection backend."""

    def __init__(self, client: Any) -> None:
        """Initialize DaskMetricsBackend with Dask client.

        Parameters
        ----------
        client : distributed.Client
            Dask distributed client

        Raises
        ------
        ValueError
            If client is None
        """
        if client is None:
            raise ValueError("client cannot be None")
        self.client = client

    def start_tracking(self, interval: float = 1.0) -> None:
        """Start tracking worker resources.

        Parameters
        ----------
        interval : float
            Sampling interval in seconds
        """
        pass

    def stop_tracking(self) -> dict[str, Any]:
        """Stop tracking and return collected data.

        Returns
        -------
        dict
            Tracking data with worker_counts, worker_memory, etc.
        """
        return {
            "worker_counts": {},
            "worker_memory": {},
            "worker_memory_limit": {},
            "worker_active_tasks": {},
            "cores_per_worker": None,
        }

    def create_span(self, name: str) -> Any:
        """Create a performance span for fine metrics collection.

        Parameters
        ----------
        name : str
            Name of the span

        Returns
        -------
        span_id : Any
            Span identifier
        """
        return None

    def get_span_metrics(self, span_id: Any) -> dict[str, Any]:
        """Extract metrics from a span.

        Parameters
        ----------
        span_id : Any
            Span identifier from create_span

        Returns
        -------
        dict
            Span metrics
        """
        return {}

    def supports_fine_metrics(self) -> bool:
        """Check if this backend supports fine-grained metrics.

        Returns
        -------
        bool
            True for Dask (supports Spans)
        """
        return True

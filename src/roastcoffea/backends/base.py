"""Abstract base class for metrics backends.

This module defines the interface that all backend implementations must follow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AbstractMetricsBackend(ABC):
    """Abstract base class for metrics collection backends.

    Backends are responsible for interfacing with different executors
    (Dask, TaskVine, etc.) to collect resource usage and performance metrics.
    """

    @abstractmethod
    def start_tracking(self, interval: float) -> None:
        """Start tracking worker resources.

        Parameters
        ----------
        interval : float
            Sampling interval in seconds.
        """
        ...

    @abstractmethod
    def stop_tracking(self) -> dict[str, Any]:
        """Stop tracking and return collected data.

        Returns
        -------
        dict[str, Any]
            Tracking data including worker counts, memory, CPU, etc.
        """
        ...

    @abstractmethod
    def create_span(self, name: str) -> Any:
        """Create a performance span for fine metrics collection.

        Parameters
        ----------
        name : str
            Name of the span (e.g., "coffea-processing").

        Returns
        -------
        Any
            Span context manager.
        """
        ...

    @abstractmethod
    def get_span_metrics(self, span_id: Any) -> dict[tuple[str, ...], Any]:
        """Extract metrics from a span.

        Parameters
        ----------
        span_id : Any
            Span identifier.

        Returns
        -------
        dict[tuple[str, ...], Any]
            Span metrics (cumulative_worker_metrics format with tuple keys).
        """
        ...

    @abstractmethod
    def supports_fine_metrics(self) -> bool:
        """Check if this backend supports fine-grained metrics.

        Returns
        -------
        bool
            True if fine metrics (Dask Spans or equivalent) are available.
        """
        ...

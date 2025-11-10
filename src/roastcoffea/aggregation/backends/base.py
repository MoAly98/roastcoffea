"""Abstract base class for tracking data parsers.

This module defines the interface that all aggregation backend parsers must follow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AbstractTrackingDataParser(ABC):
    """Abstract base class for parsing backend tracking data.

    Parsers are responsible for converting raw tracking data from different
    backends (Dask, TaskVine, etc.) into a standardized metrics format.
    """

    @abstractmethod
    def parse_tracking_data(self, tracking_data: dict[str, Any]) -> dict[str, Any]:
        """Parse raw tracking data into aggregated metrics.

        Parameters
        ----------
        tracking_data : dict
            Raw tracking data from backend (e.g., worker_counts, worker_memory)

        Returns
        -------
        dict
            Aggregated worker metrics (avg_workers, peak_workers, total_cores, etc.)
        """
        ...

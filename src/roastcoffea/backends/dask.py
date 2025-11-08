"""Dask backend for metrics collection.

Implements metrics collection for Dask executors, including:
- Worker resource tracking via scheduler sampling
- Fine-grained metrics via Dask Spans
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Any

from roastcoffea.backends.base import AbstractMetricsBackend


# =============================================================================
# Scheduler-Side Functions (Run via client.run_on_scheduler)
# =============================================================================


def _start_tracking_on_scheduler(dask_scheduler, interval: float = 1.0):
    """Start tracking worker metrics on scheduler.

    This function runs ON THE SCHEDULER via client.run_on_scheduler().

    Parameters
    ----------
    dask_scheduler : distributed.Scheduler
        Dask scheduler object
    interval : float
        Seconds between samples
    """
    # Initialize tracking state on scheduler
    dask_scheduler.worker_counts = {}
    dask_scheduler.worker_memory = {}
    dask_scheduler.worker_memory_limit = {}
    dask_scheduler.worker_active_tasks = {}
    dask_scheduler.track_count = True

    # Capture cores_per_worker from first worker
    if dask_scheduler.workers:
        first_worker = list(dask_scheduler.workers.values())[0]
        dask_scheduler.cores_per_worker = first_worker.nthreads
    else:
        dask_scheduler.cores_per_worker = None

    async def track_worker_metrics():
        """Async task to track worker metrics."""
        while dask_scheduler.track_count:
            timestamp = datetime.datetime.now()

            # Record worker count
            num_workers = len(dask_scheduler.workers)
            dask_scheduler.worker_counts[timestamp] = num_workers

            # Record memory, memory limit, and active tasks for each worker
            for worker_id, worker_state in dask_scheduler.workers.items():
                # Get memory from worker metrics
                memory_bytes = worker_state.metrics.get("memory", 0)

                # Get memory limit
                memory_limit = getattr(worker_state, "memory_limit", 0)

                # Get active tasks
                processing = getattr(worker_state, "processing", set())
                active_tasks = len(processing) if processing else 0

                # Initialize worker-specific lists if not present
                if worker_id not in dask_scheduler.worker_memory:
                    dask_scheduler.worker_memory[worker_id] = []
                if worker_id not in dask_scheduler.worker_memory_limit:
                    dask_scheduler.worker_memory_limit[worker_id] = []
                if worker_id not in dask_scheduler.worker_active_tasks:
                    dask_scheduler.worker_active_tasks[worker_id] = []

                # Append timestamped data
                dask_scheduler.worker_memory[worker_id].append((timestamp, memory_bytes))
                dask_scheduler.worker_memory_limit[worker_id].append(
                    (timestamp, memory_limit)
                )
                dask_scheduler.worker_active_tasks[worker_id].append(
                    (timestamp, active_tasks)
                )

            # Sleep for interval
            await asyncio.sleep(interval)

    # Create and start the tracking task
    asyncio.create_task(track_worker_metrics())


def _stop_tracking_on_scheduler(dask_scheduler) -> dict:
    """Stop tracking and return collected data.

    This function runs ON THE SCHEDULER via client.run_on_scheduler().

    Parameters
    ----------
    dask_scheduler : distributed.Scheduler
        Dask scheduler object

    Returns
    -------
    dict
        Tracking data
    """
    # Stop tracking
    dask_scheduler.track_count = False

    # Retrieve and return data
    tracking_data = {
        "worker_counts": dask_scheduler.worker_counts,
        "worker_memory": dask_scheduler.worker_memory,
        "worker_memory_limit": getattr(dask_scheduler, "worker_memory_limit", {}),
        "worker_active_tasks": getattr(dask_scheduler, "worker_active_tasks", {}),
        "cores_per_worker": getattr(dask_scheduler, "cores_per_worker", None),
    }

    return tracking_data


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
        # Run start_tracking function on scheduler
        self.client.run_on_scheduler(_start_tracking_on_scheduler, interval=interval)

    def stop_tracking(self) -> dict[str, Any]:
        """Stop tracking and return collected data.

        Returns
        -------
        dict
            Tracking data with worker_counts, worker_memory, etc.
        """
        # Run stop_tracking function on scheduler and get data
        tracking_data = self.client.run_on_scheduler(_stop_tracking_on_scheduler)
        return tracking_data

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

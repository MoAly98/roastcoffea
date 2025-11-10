"""Dask backend for metrics collection.

Implements metrics collection for Dask executors, including:
- Worker resource tracking via scheduler sampling
- Fine-grained metrics via Dask Spans
"""

from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Any

from roastcoffea.backends.base import AbstractMetricsBackend

logger = logging.getLogger(__name__)

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
    dask_scheduler.worker_cores = {}
    dask_scheduler.worker_nbytes = {}
    dask_scheduler.worker_occupancy = {}
    dask_scheduler.worker_executing = {}
    dask_scheduler.worker_last_seen = {}
    dask_scheduler.track_count = True

    async def track_worker_metrics():
        """Async task to track worker metrics."""
        while dask_scheduler.track_count:
            timestamp = datetime.datetime.now()

            # Record worker count
            num_workers = len(dask_scheduler.workers)
            dask_scheduler.worker_counts[timestamp] = num_workers

            # Record metrics for each worker
            for worker_id, worker_state in dask_scheduler.workers.items():
                # Get memory from worker metrics
                memory_bytes = worker_state.metrics.get("memory", 0)

                # Get memory limit
                memory_limit = getattr(worker_state, "memory_limit", 0)

                # Get cores (nthreads)
                cores = worker_state.nthreads

                # Get active tasks (processing)
                processing: set = getattr(worker_state, "processing", set())
                active_tasks = len(processing) if processing else 0

                # Get data stored on worker (nbytes)
                nbytes = getattr(worker_state, "nbytes", 0)

                # Get worker occupancy (saturation metric)
                occupancy = getattr(worker_state, "occupancy", 0.0)

                # Get executing tasks (subset of processing that's actually running)
                executing: set = getattr(worker_state, "executing", set())
                executing_tasks = len(executing) if executing else 0

                # Get last_seen timestamp (for detecting dead workers)
                last_seen = getattr(worker_state, "last_seen", 0.0)

                # Initialize worker-specific lists if not present
                if worker_id not in dask_scheduler.worker_memory:
                    dask_scheduler.worker_memory[worker_id] = []
                if worker_id not in dask_scheduler.worker_memory_limit:
                    dask_scheduler.worker_memory_limit[worker_id] = []
                if worker_id not in dask_scheduler.worker_active_tasks:
                    dask_scheduler.worker_active_tasks[worker_id] = []
                if worker_id not in dask_scheduler.worker_cores:
                    dask_scheduler.worker_cores[worker_id] = []
                if worker_id not in dask_scheduler.worker_nbytes:
                    dask_scheduler.worker_nbytes[worker_id] = []
                if worker_id not in dask_scheduler.worker_occupancy:
                    dask_scheduler.worker_occupancy[worker_id] = []
                if worker_id not in dask_scheduler.worker_executing:
                    dask_scheduler.worker_executing[worker_id] = []
                if worker_id not in dask_scheduler.worker_last_seen:
                    dask_scheduler.worker_last_seen[worker_id] = []

                # Append timestamped data
                dask_scheduler.worker_memory[worker_id].append(
                    (timestamp, memory_bytes)
                )
                dask_scheduler.worker_memory_limit[worker_id].append(
                    (timestamp, memory_limit)
                )
                dask_scheduler.worker_active_tasks[worker_id].append(
                    (timestamp, active_tasks)
                )
                dask_scheduler.worker_cores[worker_id].append((timestamp, cores))
                dask_scheduler.worker_nbytes[worker_id].append((timestamp, nbytes))
                dask_scheduler.worker_occupancy[worker_id].append(
                    (timestamp, occupancy)
                )
                dask_scheduler.worker_executing[worker_id].append(
                    (timestamp, executing_tasks)
                )
                dask_scheduler.worker_last_seen[worker_id].append(
                    (timestamp, last_seen)
                )

            # Sleep for interval
            await asyncio.sleep(interval)

    # Create and start the tracking task
    dask_scheduler.tracking_task = asyncio.create_task(track_worker_metrics())


def _stop_tracking_on_scheduler(dask_scheduler) -> dict:
    """Stop tracking and return collected data.

    This function runs on the scheduler via client.run_on_scheduler().

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
    return {
        "worker_counts": dask_scheduler.worker_counts,
        "worker_memory": dask_scheduler.worker_memory,
        "worker_memory_limit": getattr(dask_scheduler, "worker_memory_limit", {}),
        "worker_active_tasks": getattr(dask_scheduler, "worker_active_tasks", {}),
        "worker_cores": getattr(dask_scheduler, "worker_cores", {}),
        "worker_nbytes": getattr(dask_scheduler, "worker_nbytes", {}),
        "worker_occupancy": getattr(dask_scheduler, "worker_occupancy", {}),
        "worker_executing": getattr(dask_scheduler, "worker_executing", {}),
        "worker_last_seen": getattr(dask_scheduler, "worker_last_seen", {}),
    }


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
            msg = "client cannot be None"
            raise ValueError(msg)
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
        return self.client.run_on_scheduler(_stop_tracking_on_scheduler)

    def create_span(self, name: str) -> Any:
        """Create a performance span for fine metrics collection.

        Parameters
        ----------
        name : str
            Name of the span

        Returns
        -------
        span_info : dict or None
            Dictionary with 'context' (context manager) and 'id' (span ID), or None if unavailable
        """
        try:
            from distributed import span

            span_cm = span(name)
            # The span context manager returns the span_id when entered
            # We need to return both the context manager and capture the ID
            return {"context": span_cm, "name": name}
        except ImportError:
            logger.warning(
                "Dask Spans not available (distributed.span import failed). "
                "Fine-grained metrics (CPU/I/O breakdown, compression ratio) will not be collected. "
                "To enable, ensure you have a recent version of dask.distributed installed."
            )
            return None
        except Exception as e:
            logger.warning(
                f"Failed to create Dask Span: {e}. "
                "Fine-grained metrics will not be collected."
            )
            return None

    def get_span_metrics(self, span_info: Any) -> dict[str, Any]:
        """Extract metrics from a span.

        Parameters
        ----------
        span_info : dict or Any
            Span info dict from create_span containing 'id' and 'name'

        Returns
        -------
        dict
            cumulative_worker_metrics from span, or empty dict if unavailable
        """
        if span_info is None:
            return {}

        try:
            # Get the span_id from the span_info
            span_id = span_info.get("id")
            if span_id is None:
                logger.debug("No span_id available, cannot extract metrics")
                return {}

            # Access the Span object through the scheduler's spans extension
            # Use run_on_scheduler to get the actual Span object
            def _get_span_metrics(dask_scheduler, span_id):
                """Get cumulative_worker_metrics from a span on the scheduler."""
                spans_ext = dask_scheduler.extensions.get("spans")
                if spans_ext is None:
                    return {}

                span_obj = spans_ext.spans.get(span_id)
                if span_obj is None:
                    return {}

                # Return the cumulative_worker_metrics property
                return span_obj.cumulative_worker_metrics

            metrics = self.client.run_on_scheduler(_get_span_metrics, span_id=span_id)
            return metrics if metrics else {}

        except Exception as e:
            logger.debug(f"Failed to extract span metrics: {e}")
            return {}

    def supports_fine_metrics(self) -> bool:
        """Check if this backend supports fine-grained metrics.

        Returns
        -------
        bool
            True for Dask (supports Spans)
        """
        return True

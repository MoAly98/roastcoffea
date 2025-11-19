"""Decorator for chunk-level metrics tracking.

Provides @track_metrics decorator for automatic chunk boundary detection
and metrics collection in processor.process() methods.
"""

from __future__ import annotations

import functools
import time
from typing import TYPE_CHECKING, Any, Callable

from roastcoffea.utils import get_process_memory

if TYPE_CHECKING:
    from roastcoffea.collector import MetricsCollector

# Global registry for active collector
_active_collector: MetricsCollector | None = None


def set_active_collector(collector: MetricsCollector | None) -> None:
    """Set the active metrics collector for decorator use.

    Args:
        collector: MetricsCollector instance or None to clear
    """
    global _active_collector
    _active_collector = collector


def get_active_collector() -> MetricsCollector | None:
    """Get the currently active metrics collector.

    Returns:
        Active MetricsCollector or None if no collector is active
    """
    return _active_collector


def track_metrics(func: Callable) -> Callable:
    """Decorator to track metrics for processor.process() method.

    Automatically captures chunk-level metrics including:
    - Wall time (start, end, duration)
    - Memory usage (before/after)
    - Chunk metadata (dataset, file, entry range if available)

    Usage:
        ```python
        from coffea import processor
        from roastcoffea import track_metrics

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                # Your processing code
                return results
        ```

    Note:
        The decorator requires an active MetricsCollector context.
        If no collector is active, the decorator is a no-op.

    Note:
        Per-chunk bytes read is not currently tracked - this requires
        investigation into combining multiple metric sources.
    """

    @functools.wraps(func)
    def wrapper(self, events, *args, **kwargs):
        collector = get_active_collector()

        if collector is None:
            # No active collector - just run the function normally
            return func(self, events, *args, **kwargs)

        # Capture start time and memory
        t_start = time.time()
        mem_before = get_process_memory()

        # Extract chunk metadata from events
        chunk_metadata = _extract_chunk_metadata(events)

        try:
            # Run the actual processor
            result = func(self, events, *args, **kwargs)

            # Capture end time and memory
            t_end = time.time()
            mem_after = get_process_memory()

            # Record chunk metrics
            chunk_metrics = {
                "t_start": t_start,
                "t_end": t_end,
                "duration": t_end - t_start,
                "mem_before_mb": mem_before,
                "mem_after_mb": mem_after,
                "mem_delta_mb": mem_after - mem_before,
                **chunk_metadata,
            }

            collector.record_chunk_metrics(chunk_metrics)

            return result

        except Exception as e:
            # Record failed chunk
            t_end = time.time()
            chunk_metrics = {
                "t_start": t_start,
                "t_end": t_end,
                "duration": t_end - t_start,
                "error": str(e),
                **chunk_metadata,
            }
            collector.record_chunk_metrics(chunk_metrics)
            raise

    return wrapper


def _extract_chunk_metadata(events: Any) -> dict[str, Any]:
    """Extract metadata from events object.

    Attempts to extract:
    - dataset name
    - file path
    - entry range (start, stop)
    - number of events

    Args:
        events: Events object (NanoEvents or similar)

    Returns:
        Dictionary with available metadata fields
    """
    metadata: dict[str, Any] = {}

    # Try to get number of events
    try:
        metadata["num_events"] = len(events)
    except Exception:
        pass

    # Try to get metadata from events object
    # NanoEvents has behavior.get("__events_factory__") which contains metadata
    if hasattr(events, "behavior"):
        behavior = events.behavior
        if hasattr(behavior, "get"):
            factory = behavior.get("__events_factory__")
            if factory is not None:
                # Extract metadata from factory
                if hasattr(factory, "_partition_key"):
                    partition_key = factory._partition_key
                    if isinstance(partition_key, dict):
                        # Extract dataset, file, entry range
                        metadata["dataset"] = partition_key.get("dataset")
                        metadata["file"] = partition_key.get("filename")

                        # Entry range
                        start = partition_key.get("entrysteps", [None, None])[0]
                        stop = partition_key.get("entrysteps", [None, None])[1]
                        if start is not None:
                            metadata["entry_start"] = start
                        if stop is not None:
                            metadata["entry_stop"] = stop

    # Try alternative method: check metadata attribute
    if hasattr(events, "metadata"):
        meta = events.metadata
        if isinstance(meta, dict):
            metadata.update({k: v for k, v in meta.items() if k not in metadata})

    return metadata

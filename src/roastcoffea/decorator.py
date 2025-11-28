"""Decorator for chunk-level metrics tracking.

Provides @track_metrics decorator for automatic chunk boundary detection
and metrics collection in processor.process() methods.
"""

from __future__ import annotations

import functools
import time
from typing import Any, Callable

from roastcoffea.utils import get_process_memory


def track_metrics(func: Callable) -> Callable:
    """Decorator to track metrics for processor.process() method.

    Automatically captures chunk-level metrics including:
    - Wall time (start, end, duration)
    - Memory usage (before/after)
    - Chunk metadata (dataset, file, entry range if available)
    - Fine-grained timing/memory sections from context managers

    The decorator works in distributed Dask mode by injecting metrics
    directly into the output dictionary as a list. Coffea's tree reduction
    naturally concatenates these lists across chunks.

    Usage::

        from coffea import processor
        from roastcoffea import track_metrics, track_time, track_memory

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_time(self, "jet_selection"):
                    jets = events.Jet[events.Jet.pt > 30]

                with track_memory(self, "histogram_filling"):
                    # ... fill histograms

                return {"sum": len(events)}

    Note:
        The decorator requires an active MetricsCollector context.
        The collector sets `_roastcoffea_collect_metrics = True` on the
        processor instance to enable collection.

    Note:
        Metrics are injected as: `output["__roastcoffea_metrics__"] = [chunk_metrics]`
        The list format allows natural concatenation during Coffea's tree reduction.
    """

    @functools.wraps(func)
    def wrapper(self, events, *args, **kwargs):
        # Check if collection is enabled on processor instance
        should_collect = getattr(self, "_roastcoffea_collect_metrics", False)

        if not should_collect:
            # No active collector - just run the function normally
            return func(self, events, *args, **kwargs)

        # Initialize metrics container for context managers to write to
        self._roastcoffea_current_chunk = {
            "timing": {},
            "memory": {},
        }

        # Capture start time and memory
        t_start = time.time()
        mem_before = get_process_memory()

        # Extract chunk metadata from events
        chunk_metadata = _extract_chunk_metadata(events)

        try:
            # Run the actual processor
            # Context managers will write to self._roastcoffea_current_chunk
            result = func(self, events, *args, **kwargs)

            # Capture end time and memory
            t_end = time.time()
            mem_after = get_process_memory()

            # Assemble complete chunk metrics
            chunk_metrics = {
                "t_start": t_start,
                "t_end": t_end,
                "duration": t_end - t_start,
                "mem_before_mb": mem_before,
                "mem_after_mb": mem_after,
                "mem_delta_mb": mem_after - mem_before,
                "timestamp": time.time(),
                **chunk_metadata,
                # Include fine-grained sections
                "timing": self._roastcoffea_current_chunk.get("timing", {}),
                "memory": self._roastcoffea_current_chunk.get("memory", {}),
            }

            # Clean up container
            delattr(self, "_roastcoffea_current_chunk")

            # Inject metrics as LIST into output
            # This is the key: lists concatenate naturally in Coffea's tree reduction
            if isinstance(result, dict):
                result["__roastcoffea_metrics__"] = [chunk_metrics]
            else:
                # Can't inject into non-dict output
                pass

            return result

        except Exception:
            # Clean up container if it exists
            if hasattr(self, "_roastcoffea_current_chunk"):
                delattr(self, "_roastcoffea_current_chunk")

            # Re-raise the exception
            raise

    return wrapper


def _extract_chunk_metadata(events: Any) -> dict[str, Any]:
    """Extract metadata from events object.

    Attempts to extract:
    - dataset name
    - file path
    - entry range (start, stop)
    - number of events
    - uuid

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

    # Try direct metadata attribute first (NanoEvents provides this)
    try:
        metadata_obj = events.metadata
        metadata["dataset"] = metadata_obj.get("dataset")
        metadata["file"] = metadata_obj.get("filename")
        metadata["uuid"] = metadata_obj.get("uuid")
        metadata["entry_start"] = metadata_obj.get("entrystart")
        metadata["entry_stop"] = metadata_obj.get("entrystop")
    except Exception:
        pass

    # Fallback: Try behavior-based extraction
    if not metadata.get("dataset") and hasattr(events, "behavior"):
        behavior = events.behavior
        if hasattr(behavior, "get"):
            factory = behavior.get("__events_factory__")
            if factory is not None:
                # Extract metadata from factory
                if hasattr(factory, "_partition_key"):
                    partition_key = factory._partition_key
                    if isinstance(partition_key, dict):
                        # Extract dataset, file, entry range
                        if "dataset" not in metadata:
                            metadata["dataset"] = partition_key.get("dataset")
                        if "file" not in metadata:
                            metadata["file"] = partition_key.get("filename")

                        # Entry range
                        if "entry_start" not in metadata:
                            start = partition_key.get("entrysteps", [None, None])[0]
                            if start is not None:
                                metadata["entry_start"] = start
                        if "entry_stop" not in metadata:
                            stop = partition_key.get("entrysteps", [None, None])[1]
                            if stop is not None:
                                metadata["entry_stop"] = stop

    return metadata

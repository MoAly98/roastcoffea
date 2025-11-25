"""Instrumentation context managers for fine-grained tracking.

Provides track_time() and track_memory() context managers for
detailed profiling within processor methods.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Generator

from roastcoffea.utils import get_process_memory


@contextmanager
def track_time(processor_self: Any, section_name: str) -> Generator[None, None, None]:
    """Context manager to track timing for a named operation.

    Measures wall time for a specific operation within processor.process().
    Useful for identifying bottlenecks and understanding where time is spent.

    Works in distributed Dask mode by writing directly to the processor
    instance's metrics container, which is then injected into the output
    by the @track_metrics decorator.

    Args:
        processor_self: The processor instance (self)
        section_name: Name of the operation (e.g., "jet_selection", "histogram_filling")

    Yields:
        None

    Usage::

        from roastcoffea import track_metrics, track_time

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_time(self, "jet_selection"):
                    jets = events.Jet[events.Jet.pt > 30]

                with track_time(self, "event_selection"):
                    selected = events[ak.num(jets) >= 2]

                return {"sum": len(events)}

    Note:
        Timing metrics are automatically attached to the current chunk
        if used within a @track_metrics decorated function. If no collection
        is active, this context manager is a no-op.
    """
    if processor_self and hasattr(processor_self, "_roastcoffea_current_chunk"):
        start_time = time.time()

        try:
            yield
        finally:
            duration = time.time() - start_time

            if "timing" not in processor_self._roastcoffea_current_chunk:
                processor_self._roastcoffea_current_chunk["timing"] = {}

            processor_self._roastcoffea_current_chunk["timing"][section_name] = duration
    else:
        # No collection active, just yield
        yield


@contextmanager
def track_memory(processor_self: Any, section_name: str) -> Generator[None, None, None]:
    """Context manager to track memory usage for a named operation.

    Measures memory delta (before/after) for a specific operation.
    Useful for identifying memory-intensive operations.

    Works in distributed Dask mode by writing directly to the processor
    instance's metrics container, which is then injected into the output
    by the @track_metrics decorator.

    Args:
        processor_self: The processor instance (self)
        section_name: Name of the operation (e.g., "load_jets", "apply_corrections")

    Yields:
        None

    Usage::

        from roastcoffea import track_metrics, track_memory

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_memory(self, "load_all_branches"):
                    jets = events.Jet
                    electrons = events.Electron
                    muons = events.Muon

                return {"sum": len(events)}

    Note:
        Requires psutil package. If not available, memory tracking
        will be skipped gracefully (returns 0.0 for measurements).

    Note:
        Memory metrics are automatically attached to the current chunk
        if used within a @track_metrics decorated function. If no collection
        is active, this context manager is a no-op.
    """
    if processor_self and hasattr(processor_self, "_roastcoffea_current_chunk"):
        try:
            import psutil

            process = psutil.Process()
            mem_before = process.memory_info().rss / 1024 / 1024  # MB
        except ImportError:
            mem_before = None

        try:
            yield
        finally:
            if mem_before is not None:
                try:
                    import psutil

                    process = psutil.Process()
                    mem_after = process.memory_info().rss / 1024 / 1024  # MB
                    delta_mb = mem_after - mem_before
                except Exception:
                    delta_mb = 0.0
            else:
                delta_mb = 0.0

            if "memory" not in processor_self._roastcoffea_current_chunk:
                processor_self._roastcoffea_current_chunk["memory"] = {}

            processor_self._roastcoffea_current_chunk["memory"][section_name] = delta_mb
    else:
        # No collection active, just yield
        yield

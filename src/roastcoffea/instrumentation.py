"""Instrumentation context managers for fine-grained tracking.

Provides track_section() and track_memory() context managers for
detailed profiling within processor methods.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator

from roastcoffea.utils import get_process_memory

if TYPE_CHECKING:
    from roastcoffea.collector import MetricsCollector


@contextmanager
def track_time(name: str, collector: MetricsCollector | None = None, metadata: dict[str, Any] | None = None) -> Generator[dict[str, Any], None, None]:
    """Context manager to track timing for a named operation.

    Measures wall time for a specific operation within processor.process().
    Useful for identifying bottlenecks and understanding where time is spent.

    Args:
        name: Name of the operation (e.g., "jet_selection", "histogram_filling")
        collector: MetricsCollector instance. If None, uses active collector from decorator
        metadata: Optional additional metadata to attach to this operation

    Yields:
        Dictionary that will be populated with timing metrics

    Usage:
        ```python
        from roastcoffea import track_time

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_time("jet_selection"):
                    jets = events.Jet[events.Jet.pt > 30]

                with track_time("event_selection"):
                    selected = events[ak.num(jets) >= 2]

                return results
        ```

    Note:
        Timing metrics are automatically attached to the current chunk
        if used within a @track_metrics decorated function.
    """
    from roastcoffea.decorator import get_active_collector

    if collector is None:
        collector = get_active_collector()

    time_metrics: dict[str, Any] = {
        "name": name,
        "type": "time",
    }

    if metadata:
        time_metrics.update(metadata)

    t_start = time.time()

    try:
        yield time_metrics
    finally:
        t_end = time.time()
        time_metrics["t_start"] = t_start
        time_metrics["t_end"] = t_end
        time_metrics["duration"] = t_end - t_start

        if collector is not None:
            collector.record_section_metrics(time_metrics)


@contextmanager
def track_memory(name: str, collector: MetricsCollector | None = None, metadata: dict[str, Any] | None = None) -> Generator[dict[str, Any], None, None]:
    """Context manager to track memory usage for a named operation.

    Measures memory delta (before/after) for a specific operation.
    Useful for identifying memory-intensive operations.

    Args:
        name: Name of the operation (e.g., "load_jets", "apply_corrections")
        collector: MetricsCollector instance. If None, uses active collector from decorator
        metadata: Optional additional metadata to attach to this operation

    Yields:
        Dictionary that will be populated with memory metrics

    Usage:
        ```python
        from roastcoffea import track_memory

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_memory("load_all_branches"):
                    jets = events.Jet
                    electrons = events.Electron
                    muons = events.Muon

                return results
        ```

    Note:
        Requires psutil package. If not available, memory tracking
        will be skipped gracefully.

    Note:
        Memory metrics are automatically attached to the current chunk
        if used within a @track_metrics decorated function.
    """
    from roastcoffea.decorator import get_active_collector

    if collector is None:
        collector = get_active_collector()

    memory_metrics: dict[str, Any] = {
        "name": name,
        "type": "memory",
    }

    if metadata:
        memory_metrics.update(metadata)

    mem_before = get_process_memory()
    t_start = time.time()

    try:
        yield memory_metrics
    finally:
        t_end = time.time()
        mem_after = get_process_memory()

        memory_metrics["t_start"] = t_start
        memory_metrics["t_end"] = t_end
        memory_metrics["duration"] = t_end - t_start
        memory_metrics["mem_before_mb"] = mem_before
        memory_metrics["mem_after_mb"] = mem_after
        memory_metrics["mem_delta_mb"] = mem_after - mem_before

        if collector is not None:
            collector.record_section_metrics(memory_metrics)

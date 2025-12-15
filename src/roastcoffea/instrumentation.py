"""Instrumentation context managers for fine-grained tracking.

Provides track_time(), track_memory(), and track_bytes() context managers for
detailed profiling within processor methods.
"""

from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager

import awkward as ak
from coffea.processor import ProcessorABC


@contextmanager
def track_time(
    processor_self: ProcessorABC, section_name: str
) -> Generator[None, None, None]:
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
def track_memory(
    processor_self: ProcessorABC, section_name: str
) -> Generator[None, None, None]:
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


@contextmanager
def track_bytes(
    processor_self: ProcessorABC,
    events: ak.Array,
    section_name: str,
) -> Generator[None, None, None]:
    """Context manager to track bytes read from filehandle for a named operation.

    Measures the number of bytes read from the file source during a specific
    operation. Useful for identifying I/O-intensive operations and understanding
    data access patterns.

    Works in distributed Dask mode by writing directly to the processor
    instance's metrics container, which is then injected into the output
    by the @track_metrics decorator.

    Args:
        processor_self: The processor instance (self)
        events: Events object with metadata containing filehandle
        section_name: Name of the operation (e.g., "load_jets", "read_systematics")

    Yields:
        None

    Usage::

        from roastcoffea import track_metrics, track_bytes

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_bytes(self, events, "jet_loading"):
                    jets = events.Jet  # Lazy loading triggers file reads

                with track_bytes(self, events, "muon_loading"):
                    muons = events.Muon

                return {"sum": len(events)}

    Note:
        Requires the filehandle to be available via events.attrs["@events_factory"].filehandle
        with access to filehandle.file.source.num_requested_bytes. This is
        available when using the modified coffea version with file handle exposure.

    Note:
        Byte metrics are automatically attached to the current chunk
        if used within a @track_metrics decorated function. If no collection
        is active or no filehandle is available, this context manager is a no-op.
    """
    if processor_self and hasattr(processor_self, "_roastcoffea_current_chunk"):
        # Check if filehandle is available for byte tracking (once)
        source = None
        try:
            factory = events.attrs.get("@events_factory")
            if factory and hasattr(factory, "filehandle"):
                filehandle = factory.filehandle
                if filehandle and hasattr(filehandle, "file"):
                    source = filehandle.file.source
                    if not hasattr(source, "num_requested_bytes"):
                        source = None
        except Exception:
            source = None

        # Capture bytes at start if filehandle available
        bytes_before = 0
        if source:
            try:
                bytes_before = source.num_requested_bytes
            except Exception:
                pass

        try:
            yield
        finally:
            # Capture bytes at end if filehandle available
            bytes_after = 0
            if source:
                try:
                    bytes_after = source.num_requested_bytes
                except Exception:
                    pass

            bytes_delta = bytes_after - bytes_before

            if "bytes" not in processor_self._roastcoffea_current_chunk:
                processor_self._roastcoffea_current_chunk["bytes"] = {}

            processor_self._roastcoffea_current_chunk["bytes"][section_name] = (
                bytes_delta
            )
    else:
        # No collection active, just yield
        yield

"""Decorator for chunk-level metrics tracking.

Provides @track_metrics decorator for automatic chunk boundary detection
and metrics collection in processor.process() methods.
"""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import Any

from roastcoffea.utils import get_process_memory

import warnings

def track_metrics(func: Callable) -> Callable:
    """Decorator to track metrics for processor.process() method.

    Automatically captures chunk-level metrics including:
    - Wall time (start, end, duration)
    - Memory usage (before/after)
    - Bytes read from file source (if available)
    - Chunk metadata (dataset, file, entry range if available)
    - Fine-grained timing/memory/bytes sections from context managers

    The decorator works in distributed Dask mode by injecting metrics
    directly into the output dictionary as a list. Coffea's tree reduction
    naturally concatenates these lists across chunks.

    Usage::

        from coffea import processor
        from roastcoffea import track_metrics, track_time, track_memory, track_bytes

        class MyProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                with track_time(self, "jet_selection"):
                    jets = events.Jet[events.Jet.pt > 30]

                with track_memory(self, "histogram_filling"):
                    # ... fill histograms

                with track_bytes(self, events, "muon_loading"):
                    muons = events.Muon

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
            "bytes": {},
        }

        # Capture start time and memory
        t_start = time.time()
        mem_before = get_process_memory()

        # Extract chunk metadata from events
        chunk_metadata = _extract_chunk_metadata(events)

        # Extract file-level metadata (only once per file per worker)
        file_metadata = _extract_file_metadata(self, events)

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
        bytes_start = 0
        if source:
            try:
                bytes_start = source.num_requested_bytes
            except Exception:
                pass

        try:
            # Run the actual processor
            # Context managers will write to self._roastcoffea_current_chunk
            result = func(self, events, *args, **kwargs)

            # Capture end time and memory
            t_end = time.time()
            mem_after = get_process_memory()

            # Capture bytes at end if filehandle available
            bytes_end = 0
            if source:
                try:
                    bytes_end = source.num_requested_bytes
                except Exception:
                    pass

            bytes_read = bytes_end - bytes_start

            # Assemble complete chunk metrics
            chunk_metrics = {
                "t_start": t_start,
                "t_end": t_end,
                "duration": t_end - t_start,
                "mem_before_mb": mem_before,
                "mem_after_mb": mem_after,
                "mem_delta_mb": mem_after - mem_before,
                "bytes_read": bytes_read,
                "timestamp": time.time(),
                **chunk_metadata,
                # Include fine-grained sections
                "timing": self._roastcoffea_current_chunk.get("timing", {}),
                "memory": self._roastcoffea_current_chunk.get("memory", {}),
                "bytes": self._roastcoffea_current_chunk.get("bytes", {}),
            }

            # Include file-level metadata if extracted
            if file_metadata:
                chunk_metrics["file_metadata"] = file_metadata

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

    # Extract from events.metadata (NanoEvents provides this)
    metadata_obj = events.metadata
    metadata["dataset"] = metadata_obj.get("dataset")
    metadata["file"] = metadata_obj.get("filename")
    metadata["uuid"] = metadata_obj.get("uuid")
    metadata["entry_start"] = metadata_obj.get("entrystart")
    metadata["entry_stop"] = metadata_obj.get("entrystop")

    return metadata


def _extract_file_metadata(processor_self: Any, events: Any) -> dict[str, Any] | None:
    """Extract file-level metadata (compression ratio, branch info).

    This function extracts metadata that is constant for an entire file,
    not chunk-specific. To avoid repeated extraction, it tracks which files
    have already been processed on this worker using a set stored on the
    processor instance.

    Args:
        processor_self: The processor instance (self)
        events: Events object with metadata containing filehandle

    Returns:
        Dictionary with file-level metadata, or None if already extracted
        or filehandle not available

    Metadata includes:
        - filename: Full path to the file
        - compression_ratio: compressed_bytes / uncompressed_bytes
        - total_branches: Number of branches in the tree
        - branch_bytes: Dict mapping branch_name -> compressed_bytes
        - total_tree_bytes: Total compressed bytes in tree
    """
    # Initialize tracking set on processor instance (persists across chunks)
    if not hasattr(processor_self, "_roastcoffea_processed_files"):
        processor_self._roastcoffea_processed_files = set()

    try:
        # Get filehandle from events factory and filename from metadata
        factory = events.attrs.get("@events_factory")
        filehandle = (
            factory.filehandle if factory and hasattr(factory, "filehandle") else None
        )
        metadata_obj = events.metadata
        filename = metadata_obj.get("filename")


        # Skip if no filehandle or filename
        if not filehandle or not filename:
            return None

        # Skip if already extracted for this file on this worker
        if filename in processor_self._roastcoffea_processed_files:
            return None

        # Get tree name (default to "Events")
        tree_name = metadata_obj.get("treename", "Events")

        # Access the tree
        tree = filehandle[tree_name]

        # Build per-branch byte mapping for data access analysis
        branch_bytes = {}
        for branch_name in tree.keys():
            try:
                branch_bytes[branch_name] = tree[branch_name].compressed_bytes
            except Exception as e:
                # Skip branches that don't have compressed_bytes attribute
                pass

        # Calculate compression ratio
        compressed = tree.compressed_bytes
        uncompressed = tree.uncompressed_bytes
        compression_ratio = compressed / uncompressed if uncompressed > 0 else 0.0

        # Assemble file metadata
        file_metadata = {
            "filename": filename,
            "compression_ratio": compression_ratio,
            "total_branches": len(tree.keys()),
            "branch_bytes": branch_bytes,
            "total_tree_bytes": compressed,
        }

        # Mark as processed on this worker
        processor_self._roastcoffea_processed_files.add(filename)

        return file_metadata

    except Exception:
        # If anything fails, just return None (file metadata is optional)
        return None

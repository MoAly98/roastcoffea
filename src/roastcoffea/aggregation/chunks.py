"""Chunk-level metrics aggregation.

Aggregates per-chunk timing, memory, and section metrics
collected via @track_metrics decorator.
"""

from __future__ import annotations

from typing import Any


def build_chunk_info(chunk_metrics: list[dict[str, Any]]) -> dict[tuple, tuple]:
    """Build chunk_info dict from chunk metrics for throughput plotting.

    Transforms chunk-level metrics collected by @track_metrics into the format
    expected by plot_throughput_timeline().

    Args:
        chunk_metrics: List of chunk metrics dicts from @track_metrics decorator.
                      Each dict contains: file, entry_start, entry_stop,
                      t_start, t_end, bytes_read

    Returns:
        Dictionary mapping chunk keys to timing/bytes data:
        {(filename, entry_start, entry_stop): (t_start, t_end, bytes_read)}

    Example:
        >>> chunk_metrics = [
        ...     {"file": "data.root", "entry_start": 0, "entry_stop": 1000,
        ...      "t_start": 1.0, "t_end": 2.5, "bytes_read": 50000},
        ... ]
        >>> chunk_info = build_chunk_info(chunk_metrics)
        >>> chunk_info
        {('data.root', 0, 1000): (1.0, 2.5, 50000)}

    Note:
        Chunks without file/entry metadata are skipped.
        Chunks without bytes_read default to 0 bytes.
    """
    chunk_info = {}

    for chunk in chunk_metrics:
        # Extract required fields
        filename = chunk.get("file")
        entry_start = chunk.get("entry_start")
        entry_stop = chunk.get("entry_stop")
        t_start = chunk.get("t_start")
        t_end = chunk.get("t_end")
        bytes_read = chunk.get("bytes_read", 0)

        # Skip chunks without essential metadata
        if filename is None or entry_start is None or entry_stop is None:
            continue
        if t_start is None or t_end is None:
            continue

        # Build chunk key and value
        key = (filename, entry_start, entry_stop)
        value = (t_start, t_end, bytes_read)

        chunk_info[key] = value

    return chunk_info

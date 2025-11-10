"""Parse Dask Spans fine-grained performance metrics.

Dask Spans provide detailed breakdown of task activity via cumulative_worker_metrics.
This module parses those metrics into a standardized format.
"""

from __future__ import annotations

from typing import Any


def parse_fine_metrics(cumulative_worker_metrics: dict[str, Any]) -> dict[str, Any]:
    """Parse Dask Spans cumulative_worker_metrics into fine metrics.

    Parameters
    ----------
    cumulative_worker_metrics : dict
        Raw metrics from span.cumulative_worker_metrics with tuple keys like:
        ('execute', task_prefix, activity, unit) -> value
        Activities include: thread-cpu, thread-noncpu, disk-read, disk-write,
        compress, decompress, serialize, deserialize

    Returns
    -------
    dict
        Parsed fine metrics with keys:
        - cpu_time_seconds: Pure CPU time
        - io_time_seconds: I/O + waiting time
        - cpu_percentage: CPU / (CPU + I/O) × 100
        - io_percentage: I/O / (CPU + I/O) × 100
        - disk_read_bytes: Bytes read from disk
        - disk_write_bytes: Bytes written to disk
        - decompression_time_seconds: Time spent decompressing
        - compression_time_seconds: Time spent compressing
        - deserialization_time_seconds: Time spent deserializing
        - serialization_time_seconds: Time spent serializing
        - total_serialization_overhead_seconds: Sum of serialize + deserialize
        - total_compression_overhead_seconds: Sum of compress + decompress
    """
    # Aggregate metrics by activity type across all task prefixes
    # Metrics have keys like: ('execute', task_prefix, activity, unit)
    cpu_time = 0.0
    io_time = 0.0
    disk_read = 0
    disk_write = 0
    decompress_time = 0.0
    compress_time = 0.0
    deserialize_time = 0.0
    serialize_time = 0.0

    for key, value in cumulative_worker_metrics.items():
        if not isinstance(key, tuple) or len(key) < 3:
            continue

        # Extract activity from tuple key
        # Format: (context, task_prefix, activity, unit) or similar
        activity = key[2] if len(key) > 2 else None

        if activity == "thread-cpu":
            cpu_time += value
        elif activity == "thread-noncpu":
            io_time += value
        elif activity == "disk-read":
            disk_read += value
        elif activity == "disk-write":
            disk_write += value
        elif activity == "decompress":
            decompress_time += value
        elif activity == "compress":
            compress_time += value
        elif activity == "deserialize":
            deserialize_time += value
        elif activity == "serialize":
            serialize_time += value

    # Calculate percentages
    total_time = cpu_time + io_time
    cpu_percentage = (cpu_time / total_time * 100) if total_time > 0 else 0.0
    io_percentage = (io_time / total_time * 100) if total_time > 0 else 0.0

    # Calculate overhead totals
    total_serialization_overhead = serialize_time + deserialize_time
    total_compression_overhead = compress_time + decompress_time

    return {
        # Time breakdown
        "cpu_time_seconds": cpu_time,
        "io_time_seconds": io_time,
        "cpu_percentage": cpu_percentage,
        "io_percentage": io_percentage,
        # Disk I/O
        "disk_read_bytes": disk_read,
        "disk_write_bytes": disk_write,
        # Compression overhead
        "decompression_time_seconds": decompress_time,
        "compression_time_seconds": compress_time,
        "total_compression_overhead_seconds": total_compression_overhead,
        # Serialization overhead
        "deserialization_time_seconds": deserialize_time,
        "serialization_time_seconds": serialize_time,
        "total_serialization_overhead_seconds": total_serialization_overhead,
    }


def calculate_compression_from_spans(
    compressed_bytes: float,
    cumulative_worker_metrics: dict[str, Any],
) -> tuple[float | None, float | None]:
    """Calculate compression ratio and uncompressed bytes from Spans data.

    Dask Spans track disk-read (actual disk I/O) or memory-read (in-memory access)
    which combined with compressed bytes from Coffea gives us real compression ratio.

    Parameters
    ----------
    compressed_bytes : float
        Compressed bytes from Coffea report (bytesread)
    cumulative_worker_metrics : dict
        Raw metrics from span.cumulative_worker_metrics with tuple keys

    Returns
    -------
    compression_ratio : float or None
        Uncompressed / compressed ratio, or None if data unavailable
    total_bytes_uncompressed : float or None
        Actual uncompressed bytes read, or None if data unavailable
    """
    # Aggregate disk-read and memory-read across all tasks
    # disk-read: actual disk I/O (local files, spills)
    # memory-read: in-memory data access (includes decompressed ROOT data)
    disk_read_bytes = 0
    memory_read_bytes = 0

    for key, value in cumulative_worker_metrics.items():
        if not isinstance(key, tuple) or len(key) < 3:
            continue

        activity = key[2]
        unit = key[3] if len(key) >= 4 else None

        if activity == "disk-read" and unit == "bytes":
            disk_read_bytes += value
        elif activity == "memory-read" and unit == "bytes":
            memory_read_bytes += value

    # Prefer disk-read if available (actual file I/O), otherwise use memory-read
    uncompressed_bytes = disk_read_bytes if disk_read_bytes > 0 else memory_read_bytes

    if uncompressed_bytes == 0:
        return None, None

    if compressed_bytes == 0:
        return None, None

    # Calculate real compression ratio
    compression_ratio = uncompressed_bytes / compressed_bytes

    return compression_ratio, float(uncompressed_bytes)

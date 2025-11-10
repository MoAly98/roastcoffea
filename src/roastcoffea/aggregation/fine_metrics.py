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
        Raw metrics from span.cumulative_worker_metrics with keys like:
        - thread-cpu: CPU time (seconds)
        - thread-noncpu: I/O + waiting time (seconds)
        - disk-read: Bytes read from disk
        - disk-write: Bytes written to disk
        - decompress: Decompression time (seconds)
        - compress: Compression time (seconds)
        - deserialize: Deserialization time (seconds)
        - serialize: Serialization time (seconds)

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
    # Extract raw metrics
    cpu_time = cumulative_worker_metrics.get("thread-cpu", 0.0)
    io_time = cumulative_worker_metrics.get("thread-noncpu", 0.0)
    disk_read = cumulative_worker_metrics.get("disk-read", 0)
    disk_write = cumulative_worker_metrics.get("disk-write", 0)
    decompress_time = cumulative_worker_metrics.get("decompress", 0.0)
    compress_time = cumulative_worker_metrics.get("compress", 0.0)
    deserialize_time = cumulative_worker_metrics.get("deserialize", 0.0)
    serialize_time = cumulative_worker_metrics.get("serialize", 0.0)

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

    Dask Spans track disk-read (uncompressed bytes actually read) which combined
    with compressed bytes from Coffea gives us real compression ratio.

    Parameters
    ----------
    compressed_bytes : float
        Compressed bytes from Coffea report (bytesread)
    cumulative_worker_metrics : dict
        Raw metrics from span.cumulative_worker_metrics

    Returns
    -------
    compression_ratio : float or None
        Uncompressed / compressed ratio, or None if data unavailable
    total_bytes_uncompressed : float or None
        Actual uncompressed bytes read, or None if data unavailable
    """
    # Get uncompressed bytes from Spans (disk-read)
    uncompressed_bytes = cumulative_worker_metrics.get("disk-read")

    if uncompressed_bytes is None or uncompressed_bytes == 0:
        return None, None

    if compressed_bytes == 0:
        return None, None

    # Calculate real compression ratio
    compression_ratio = uncompressed_bytes / compressed_bytes

    return compression_ratio, float(uncompressed_bytes)

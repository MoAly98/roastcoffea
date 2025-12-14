"""Parse Dask Spans fine-grained performance metrics.

Dask Spans provide detailed breakdown of task activity via cumulative_worker_metrics.
This module parses those metrics into a standardized format.
"""

from __future__ import annotations

from typing import Any


def parse_fine_metrics(
    cumulative_worker_metrics: dict[tuple[str, ...], Any],
    processor_name: str | None = None,
) -> dict[str, Any]:
    """Parse Dask Spans cumulative_worker_metrics into fine metrics.

    Parameters
    ----------
    cumulative_worker_metrics : dict
        Raw metrics from span.cumulative_worker_metrics with tuple keys like:
        ('execute', task_prefix, activity, unit) -> value
        Activities include: thread-cpu, thread-noncpu, disk-read, disk-write,
        compress, decompress, serialize, deserialize
    processor_name : str, optional
        Name of processor class to filter metrics for. If provided, only metrics
        from this processor are included in processor_* fields, and other metrics
        go into overhead_* fields.

    Returns
    -------
    dict
        Parsed fine metrics with keys:
        - processor_cpu_time_seconds: CPU time in processor
        - processor_io_wait_time_seconds: I/O and waiting time in processor (I/O, GIL, blocking)
        - processor_cpu_percent: CPU / (CPU + I/O wait) x 100 for processor
        - processor_io_wait_percent: I/O wait / (CPU + I/O wait) x 100 for processor
        - overhead_cpu_time_seconds: CPU time in Dask overhead (if processor_name given)
        - overhead_io_wait_time_seconds: I/O and waiting time in Dask overhead
        - disk_read_bytes: Bytes read from disk
        - disk_write_bytes: Bytes written to disk
        - decompression_time_seconds: Time spent decompressing
        - compression_time_seconds: Time spent compressing
        - deserialization_time_seconds: Time spent deserializing
        - serialization_time_seconds: Time spent serializing
        - total_serialization_overhead_seconds: Sum of serialize + deserialize
        - total_compression_overhead_seconds: Sum of compress + decompress
    """
    # Aggregate metrics by activity type
    # Metrics have keys like: ('execute', task_prefix, activity, unit)
    processor_cpu = 0.0
    processor_io_wait = 0.0
    overhead_cpu = 0.0
    overhead_io_wait = 0.0
    disk_read = 0
    disk_write = 0
    memory_read = 0
    decompress_time = 0.0
    compress_time = 0.0
    deserialize_time = 0.0
    serialize_time = 0.0

    for key, value in cumulative_worker_metrics.items():
        if len(key) < 3:
            continue

        # Extract components from tuple key
        # Format: (context, task_prefix, activity, unit)
        task_prefix = key[1]
        activity = key[2]
        unit = key[3] if len(key) > 3 else None

        # Determine if this is processor work or overhead
        is_processor = (processor_name is None) or (task_prefix == processor_name)

        if activity == "thread-cpu":
            if is_processor:
                processor_cpu += value
            else:
                overhead_cpu += value
        elif activity == "thread-noncpu":
            if is_processor:
                processor_io_wait += value
            else:
                overhead_io_wait += value
        elif activity == "disk-read" and unit == "bytes":
            disk_read += value
        elif activity == "disk-write" and unit == "bytes":
            disk_write += value
        elif activity == "memory-read" and unit == "bytes":
            memory_read += value
        elif activity == "decompress":
            decompress_time += value
        elif activity == "compress":
            compress_time += value
        elif activity == "deserialize":
            deserialize_time += value
        elif activity == "serialize":
            serialize_time += value

    # Calculate percentages for processor
    processor_total = processor_cpu + processor_io_wait
    processor_cpu_pct = (
        (processor_cpu / processor_total * 100) if processor_total > 0 else 0.0
    )
    processor_io_wait_pct = (
        (processor_io_wait / processor_total * 100) if processor_total > 0 else 0.0
    )

    # Calculate overhead totals
    total_serialization_overhead = serialize_time + deserialize_time
    total_compression_overhead = compress_time + decompress_time

    return {
        # Processor time breakdown
        "processor_cpu_time_seconds": processor_cpu,
        "processor_io_wait_time_seconds": processor_io_wait,
        "processor_cpu_percent": processor_cpu_pct,
        "processor_io_wait_percent": processor_io_wait_pct,
        # Dask overhead (only populated if processor_name given)
        "overhead_cpu_time_seconds": overhead_cpu,
        "overhead_io_wait_time_seconds": overhead_io_wait,
        # Data volume from Dask Spans
        "total_bytes_memory_read": memory_read,  # In-memory data access tracked by Dask
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

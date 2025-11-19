"""Common utility functions."""

from __future__ import annotations


def get_process_memory() -> float:
    """Get current process memory usage in MB.

    Returns
    -------
    float
        Memory usage in MB, or 0.0 if psutil not available
    """
    try:
        import psutil
        import os

        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024**2  # Convert to MB
    except ImportError:
        return 0.0

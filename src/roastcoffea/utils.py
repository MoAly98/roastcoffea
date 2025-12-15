"""Common utility functions."""

from __future__ import annotations

from typing import Any


def get_nested(d: dict, *keys: str, default: Any = None) -> Any:
    """Safely get a nested value from a dict.

    Parameters
    ----------
    d : dict
        The dictionary to traverse
    *keys : str
        Keys to traverse in order
    default : Any
        Value to return if any key is missing (default: None)

    Returns
    -------
    Any
        The nested value or default if not found

    Examples
    --------
    >>> metrics = {"summary": {"throughput": {"data_rate_gbps": 1.5}}}
    >>> get_nested(metrics, "summary", "throughput", "data_rate_gbps")
    1.5
    >>> get_nested(metrics, "summary", "missing", default={})
    {}
    """
    for key in keys:
        if d is None or not isinstance(d, dict):
            return default
        d = d.get(key)
    return d if d is not None else default


def get_process_memory() -> float:
    """Get current process memory usage in MB.

    Returns
    -------
    float
        Memory usage in MB, or 0.0 if psutil not available
    """
    try:
        import os

        import psutil

        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024**2  # Convert to MB
    except ImportError:
        return 0.0

"""Per-task visualization from Dask Spans fine metrics.

These plots show metrics broken down by individual tasks (task prefix).
"""

from __future__ import annotations

from typing import Any

import matplotlib.pyplot as plt
import numpy as np


def extract_per_task_metrics(span_metrics: dict[tuple, Any]) -> dict[str, dict[str, float]]:
    """Extract per-task metrics from span cumulative_worker_metrics.

    Parameters
    ----------
    span_metrics : dict
        cumulative_worker_metrics from Dask Span with tuple keys

    Returns
    -------
    dict
        Nested dict: {task_prefix: {activity: value}}
    """
    per_task = {}

    for key, value in span_metrics.items():
        if not isinstance(key, tuple) or len(key) < 3:
            continue

        context, task_prefix, activity, *_ = key

        if context != "execute":
            continue

        if task_prefix not in per_task:
            per_task[task_prefix] = {}

        per_task[task_prefix][activity] = value

    return per_task


def plot_per_task_cpu_io(
    span_metrics: dict[tuple, Any],
    output_path: str | None = None,
    title: str = "CPU vs I/O Time per Task",
    figsize: tuple[int, int] = (12, 6),
) -> tuple:
    """Plot CPU and I/O time for each task.

    Parameters
    ----------
    span_metrics : dict
        cumulative_worker_metrics from Dask Span
    output_path : str, optional
        Path to save figure
    title : str
        Plot title
    figsize : tuple
        Figure size (width, height)

    Returns
    -------
    fig, ax : matplotlib figure and axes
    """
    per_task = extract_per_task_metrics(span_metrics)

    # Extract data
    task_names = []
    cpu_times = []
    io_times = []

    for task_prefix, activities in sorted(per_task.items()):
        if task_prefix == "N/A":
            continue

        cpu = activities.get("thread-cpu", 0.0)
        io = activities.get("thread-noncpu", 0.0)

        if cpu > 0 or io > 0:
            task_names.append(task_prefix)
            cpu_times.append(cpu)
            io_times.append(io)

    if not task_names:
        msg = "No per-task CPU/IO metrics found in span_metrics"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    x = np.arange(len(task_names))
    width = 0.35

    ax.bar(x - width / 2, cpu_times, width, label="CPU Time", color="#2ecc71")
    ax.bar(x + width / 2, io_times, width, label="I/O Time", color="#e74c3c")

    ax.set_xlabel("Task")
    ax.set_ylabel("Time (seconds)")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(task_names, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_per_task_bytes_read(
    span_metrics: dict[tuple, Any],
    output_path: str | None = None,
    title: str = "Bytes Read per Task",
    figsize: tuple[int, int] = (12, 6),
) -> tuple:
    """Plot bytes read (disk-read) for each task.

    Parameters
    ----------
    span_metrics : dict
        cumulative_worker_metrics from Dask Span
    output_path : str, optional
        Path to save figure
    title : str
        Plot title
    figsize : tuple
        Figure size (width, height)

    Returns
    -------
    fig, ax : matplotlib figure and axes
    """
    per_task = extract_per_task_metrics(span_metrics)

    # Extract data
    task_names = []
    bytes_read = []

    for task_prefix, activities in sorted(per_task.items()):
        if task_prefix == "N/A":
            continue

        disk_read = activities.get("disk-read", 0)

        if disk_read > 0:
            task_names.append(task_prefix)
            bytes_read.append(disk_read / 1e9)  # Convert to GB

    if not task_names:
        msg = "No per-task disk-read metrics found in span_metrics"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    x = np.arange(len(task_names))

    ax.bar(x, bytes_read, color="#3498db")

    ax.set_xlabel("Task")
    ax.set_ylabel("Bytes Read (GB)")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(task_names, rotation=45, ha="right")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_per_task_overhead(
    span_metrics: dict[tuple, Any],
    output_path: str | None = None,
    title: str = "Compression & Serialization Overhead per Task",
    figsize: tuple[int, int] = (12, 6),
) -> tuple:
    """Plot compression and serialization overhead for each task.

    Parameters
    ----------
    span_metrics : dict
        cumulative_worker_metrics from Dask Span
    output_path : str, optional
        Path to save figure
    title : str
        Plot title
    figsize : tuple
        Figure size (width, height)

    Returns
    -------
    fig, ax : matplotlib figure and axes
    """
    per_task = extract_per_task_metrics(span_metrics)

    # Extract data
    task_names = []
    compress_times = []
    decompress_times = []
    serialize_times = []
    deserialize_times = []

    for task_prefix, activities in sorted(per_task.items()):
        if task_prefix == "N/A":
            continue

        compress = activities.get("compress", 0.0)
        decompress = activities.get("decompress", 0.0)
        serialize = activities.get("serialize", 0.0)
        deserialize = activities.get("deserialize", 0.0)

        if any([compress, decompress, serialize, deserialize]):
            task_names.append(task_prefix)
            compress_times.append(compress)
            decompress_times.append(decompress)
            serialize_times.append(serialize)
            deserialize_times.append(deserialize)

    if not task_names:
        msg = "No per-task overhead metrics found in span_metrics"
        raise ValueError(msg)

    # Create stacked bar plot
    fig, ax = plt.subplots(figsize=figsize)

    x = np.arange(len(task_names))
    width = 0.35

    # Stack compression
    p1 = ax.bar(x - width / 2, decompress_times, width, label="Decompress")
    p2 = ax.bar(
        x - width / 2,
        compress_times,
        width,
        bottom=decompress_times,
        label="Compress",
    )

    # Stack serialization
    p3 = ax.bar(x + width / 2, deserialize_times, width, label="Deserialize")
    p4 = ax.bar(
        x + width / 2,
        serialize_times,
        width,
        bottom=deserialize_times,
        label="Serialize",
    )

    ax.set_xlabel("Task")
    ax.set_ylabel("Time (seconds)")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(task_names, rotation=45, ha="right")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

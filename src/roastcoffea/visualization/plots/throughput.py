"""Throughput and data rate plots.

Visualizations for data processing rates and event throughput.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def plot_worker_activity_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Worker Activity Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot active tasks per worker over time.

    Shows the number of active (processing + queued) tasks per worker,
    which indicates overall workload distribution.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_active_tasks
    output_path : Path, optional
        Save path
    figsize : tuple
        Figure size
    title : str
        Plot title

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing active tasks data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_active_tasks = tracking_data.get("worker_active_tasks", {})

    if not worker_active_tasks:
        msg = "No worker active tasks data available"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot each worker's active task count timeline
    for worker_id, timeline in worker_active_tasks.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            task_counts = [val for _, val in timeline]
            ax.plot(timestamps, task_counts, label=worker_id, alpha=0.7, linewidth=2)

    ax.set_xlabel("Time")
    ax.set_ylabel("Number of Active Tasks")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8)

    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_total_active_tasks_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 5),
    title: str = "Total Active Tasks Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot total active tasks across all workers over time.

    Aggregates active tasks from all workers to show overall cluster activity.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_active_tasks
    output_path : Path, optional
        Save path
    figsize : tuple
        Figure size
    title : str
        Plot title

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing active tasks data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_active_tasks = tracking_data.get("worker_active_tasks", {})

    if not worker_active_tasks:
        msg = "No worker active tasks data available"
        raise ValueError(msg)

    # Aggregate across all workers at each timestamp
    timestamp_totals: dict = {}

    for _worker_id, timeline in worker_active_tasks.items():
        for timestamp, task_count in timeline:
            if timestamp not in timestamp_totals:
                timestamp_totals[timestamp] = 0
            timestamp_totals[timestamp] += task_count

    # Sort by timestamp
    sorted_items = sorted(timestamp_totals.items())
    timestamps = [t for t, _ in sorted_items]
    totals = [c for _, c in sorted_items]

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(timestamps, totals, linewidth=2, color="steelblue")
    ax.fill_between(timestamps, totals, alpha=0.3, color="steelblue")

    ax.set_xlabel("Time")
    ax.set_ylabel("Total Active Tasks")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

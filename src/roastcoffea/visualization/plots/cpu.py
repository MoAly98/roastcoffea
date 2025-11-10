"""CPU utilization plots.

Both static and interactive visualizations of CPU usage.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def plot_occupancy_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Worker Occupancy Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot worker occupancy (task saturation) over time.

    Occupancy is a metric from Dask scheduler indicating how saturated
    a worker is with tasks. 0.0 = idle, higher values = more saturated.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_occupancy
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
        If tracking_data is None or missing occupancy data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_occupancy = tracking_data.get("worker_occupancy", {})

    if not worker_occupancy:
        msg = "No worker occupancy data available"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot each worker's occupancy timeline
    for worker_id, timeline in worker_occupancy.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            occupancy_values = [val for _, val in timeline]
            ax.plot(timestamps, occupancy_values, label=worker_id, alpha=0.7, linewidth=2)

    ax.set_xlabel("Time")
    ax.set_ylabel("Occupancy (saturation)")
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


def plot_executing_tasks_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Executing Tasks Per Worker Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot number of executing tasks per worker over time.

    Executing tasks are tasks actually running (subset of active tasks).

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_executing
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
        If tracking_data is None or missing executing data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_executing = tracking_data.get("worker_executing", {})

    if not worker_executing:
        msg = "No worker executing tasks data available"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot each worker's executing task count timeline
    for worker_id, timeline in worker_executing.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            executing_counts = [val for _, val in timeline]
            ax.plot(
                timestamps, executing_counts, label=worker_id, alpha=0.7, linewidth=2
            )

    ax.set_xlabel("Time")
    ax.set_ylabel("Number of Executing Tasks")
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

"""CPU utilization plots.

Both static and interactive visualizations of CPU usage.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from roastcoffea.visualization.utils import add_hover_tooltips


def plot_occupancy_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Worker Occupancy Over Time",
    max_legend_entries: int = 5,
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
    max_legend_entries : int, optional
        Maximum number of workers to show in legend. If worker count exceeds
        this threshold, legend is hidden and hover tooltips are used instead.
        Default is 5.

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

    # Track lines and labels for hover tooltips
    lines = []
    labels = []

    # Plot each worker's occupancy timeline
    for worker_id, timeline in worker_occupancy.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            occupancy_values = [val for _, val in timeline]
            (line,) = ax.plot(timestamps, occupancy_values, label=worker_id, alpha=0.7, linewidth=2)
            lines.append(line)
            labels.append(worker_id)

    ax.set_xlabel("Time")
    ax.set_ylabel("Occupancy (saturation)")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    # Show legend only if worker count is below threshold
    num_workers = len(worker_occupancy)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        # Add text annotation showing worker count
        ax.text(
            0.02,
            0.98,
            f"Showing {num_workers} workers",
            transform=ax.transAxes,
            va="top",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.7),
        )

    # Add hover tooltips
    add_hover_tooltips(lines, labels)

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
    max_legend_entries: int = 5,
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
    max_legend_entries : int, optional
        Maximum number of workers to show in legend. If worker count exceeds
        this threshold, legend is hidden and hover tooltips are used instead.
        Default is 5.

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

    # Track lines and labels for hover tooltips
    lines = []
    labels = []

    # Plot each worker's executing task count timeline
    for worker_id, timeline in worker_executing.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            executing_counts = [val for _, val in timeline]
            (line,) = ax.plot(timestamps, executing_counts, label=worker_id, alpha=0.7, linewidth=2)
            lines.append(line)
            labels.append(worker_id)

    ax.set_xlabel("Time")
    ax.set_ylabel("Number of Executing Tasks")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    # Show legend only if worker count is below threshold
    num_workers = len(worker_executing)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        # Add text annotation showing worker count
        ax.text(
            0.02,
            0.98,
            f"Showing {num_workers} workers",
            transform=ax.transAxes,
            va="top",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.7),
        )

    # Add hover tooltips
    add_hover_tooltips(lines, labels)

    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

def plot_cpu_utilization_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "CPU Utilization Per Worker Over Time",
    max_legend_entries: int = 5,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot CPU utilization percentage per worker over time.

    Shows actual CPU usage (0-100%) for each worker, providing insight
    into compute resource utilization.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_cpu
    output_path : Path, optional
        Save path
    figsize : tuple
        Figure size
    title : str
        Plot title
    max_legend_entries : int, optional
        Maximum number of workers to show in legend. If worker count exceeds
        this threshold, legend is hidden and hover tooltips are used instead.
        Default is 5.

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing CPU data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_cpu = tracking_data.get("worker_cpu", {})

    if not worker_cpu:
        msg = "No worker CPU data available"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Track lines and labels for hover tooltips
    lines = []
    labels = []

    # Plot each worker's CPU utilization timeline
    for worker_id, timeline in worker_cpu.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            cpu_values = [val for _, val in timeline]
            (line,) = ax.plot(timestamps, cpu_values, label=worker_id, alpha=0.7, linewidth=2)
            lines.append(line)
            labels.append(worker_id)

    ax.set_xlabel("Time")
    ax.set_ylabel("CPU Utilization (%)")
    ax.set_ylim([0, 100])
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    # Show legend only if worker count is below threshold
    num_workers = len(worker_cpu)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        # Add text annotation showing worker count
        ax.text(
            0.02,
            0.98,
            f"Showing {num_workers} workers",
            transform=ax.transAxes,
            va="top",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.7),
        )

    # Add hover tooltips
    add_hover_tooltips(lines, labels)

    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

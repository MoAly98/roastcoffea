"""CPU utilization plots.

Visualizations for CPU usage and worker task metrics.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np

from roastcoffea.visualization.utils import (
    add_worker_count_annotation,
    finalize_timeline_plot,
    setup_timeline_axes,
    validate_tracking_data,
)


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
        Maximum number of workers to show in legend. Default is 5.

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing occupancy data
    """
    worker_occupancy = validate_tracking_data(
        tracking_data, "worker_occupancy", "No worker occupancy data available"
    )

    fig, ax = plt.subplots(figsize=figsize)

    for worker_id, timeline in worker_occupancy.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            values = [val for _, val in timeline]
            ax.plot(timestamps, values, label=worker_id, alpha=0.7, linewidth=2)

    setup_timeline_axes(ax, ylabel="Occupancy (saturation)", title=title)

    num_workers = len(worker_occupancy)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        add_worker_count_annotation(ax, num_workers)

    finalize_timeline_plot(fig, ax, output_path)
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
        Maximum number of workers to show in legend. Default is 5.

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing executing data
    """
    worker_executing = validate_tracking_data(
        tracking_data, "worker_executing", "No worker executing tasks data available"
    )

    fig, ax = plt.subplots(figsize=figsize)

    for worker_id, timeline in worker_executing.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            values = [val for _, val in timeline]
            ax.plot(timestamps, values, label=worker_id, alpha=0.7, linewidth=2)

    setup_timeline_axes(ax, ylabel="Number of Executing Tasks", title=title)

    num_workers = len(worker_executing)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        add_worker_count_annotation(ax, num_workers)

    finalize_timeline_plot(fig, ax, output_path)
    return fig, ax


def plot_cpu_utilization_per_worker_timeline(
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
        Maximum number of workers to show in legend. Default is 5.

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing CPU data
    """
    worker_cpu = validate_tracking_data(
        tracking_data, "worker_cpu", "No worker CPU data available"
    )

    fig, ax = plt.subplots(figsize=figsize)

    for worker_id, timeline in worker_cpu.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            values = [val for _, val in timeline]
            ax.plot(timestamps, values, label=worker_id, alpha=0.7, linewidth=2)

    setup_timeline_axes(ax, ylabel="CPU Utilization (%)", title=title, ylim=(0, 100))

    num_workers = len(worker_cpu)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        add_worker_count_annotation(ax, num_workers)

    finalize_timeline_plot(fig, ax, output_path)
    return fig, ax


def plot_cpu_utilization_mean_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 4),
    title: str = "CPU Utilization Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot mean CPU utilization percentage over time with min-max band.

    Shows aggregated CPU usage across all workers, with mean line and
    shaded min-max range.

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

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If tracking_data is None or missing CPU data
    """
    worker_cpu = validate_tracking_data(
        tracking_data, "worker_cpu", "No worker CPU data available"
    )

    # Collect all unique timestamps
    all_timestamps = set()
    for worker_id in worker_cpu:
        for timestamp, _ in worker_cpu[worker_id]:
            all_timestamps.add(timestamp)

    sorted_timestamps = sorted(all_timestamps)

    # Calculate CPU utilization stats at each timestamp
    cpu_mean = []
    cpu_min = []
    cpu_max = []

    for timestamp in sorted_timestamps:
        worker_values = []
        for worker_id in worker_cpu:
            for t, cpu_value in worker_cpu[worker_id]:
                if t == timestamp:
                    worker_values.append(cpu_value)
                    break

        if worker_values:
            cpu_mean.append(np.mean(worker_values))
            cpu_min.append(np.min(worker_values))
            cpu_max.append(np.max(worker_values))
        else:
            cpu_mean.append(0)
            cpu_min.append(0)
            cpu_max.append(0)

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(sorted_timestamps, cpu_mean, linewidth=2, label="Mean", color="C0")
    ax.fill_between(
        sorted_timestamps,
        cpu_min,
        cpu_max,
        alpha=0.3,
        label="Min-Max Range",
        color="C0",
    )

    setup_timeline_axes(ax, ylabel="CPU Utilization (%)", title=title, ylim=(0, 100))
    ax.legend()

    finalize_timeline_plot(fig, ax, output_path)
    return fig, ax

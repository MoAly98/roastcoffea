"""Throughput and data rate plots.

Visualizations for data processing rates and event throughput.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np

from roastcoffea.visualization.utils import add_hover_tooltips


def plot_worker_activity_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Worker Activity Over Time",
    max_legend_entries: int = 5,
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

    # Track lines and labels for hover tooltips
    lines = []
    labels = []

    # Plot each worker's active task count timeline
    for worker_id, timeline in worker_active_tasks.items():
        if timeline:
            timestamps = [t for t, _ in timeline]
            task_counts = [val for _, val in timeline]
            (line,) = ax.plot(timestamps, task_counts, label=worker_id, alpha=0.7, linewidth=2)
            lines.append(line)
            labels.append(worker_id)

    ax.set_xlabel("Time")
    ax.set_ylabel("Number of Active Tasks")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    # Show legend only if worker count is below threshold
    num_workers = len(worker_active_tasks)
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


def plot_throughput_timeline(
    chunk_info: dict[tuple[str, int, int], tuple[float, float, int]],
    tracking_data: dict[str, Any] | None = None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Data Throughput Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot instantaneous data throughput (Gbps) over time.

    Computes the instantaneous data rate at each sample point by finding
    all chunks that were being processed at that moment and summing their
    individual throughputs.

    Optionally overlays worker count on a secondary y-axis if tracking_data
    is provided.

    Parameters
    ----------
    chunk_info : dict
        Per-chunk timing data from metrics.
        Format: {(filename, start, stop): (t0, t1, bytesread)}
    tracking_data : dict, optional
        Worker tracking data with worker_counts for overlay plot
    output_path : Path, optional
        Path to save figure
    figsize : tuple
        Figure size (width, height)
    title : str
        Plot title

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes (returns primary axes)

    Raises
    ------
    ValueError
        If chunk_info is empty
    """
    if not chunk_info:
        msg = "No chunk_info provided. Pass chunk_info parameter from metrics."
        raise ValueError(msg)

    # Extract per-chunk timing: starts, ends, bytes, runtimes
    starts = []
    ends = []
    bytes_read = []
    runtimes = []

    for _key, (t0, t1, bytesread) in chunk_info.items():
        starts.append(t0)
        ends.append(t1)
        bytes_read.append(bytesread)
        runtimes.append(t1 - t0)

    # Convert to numpy arrays
    starts = np.array(starts)
    ends = np.array(ends)
    bytes_read = np.array(bytes_read)
    runtimes = np.array(runtimes)

    # Convert epoch times to datetime for plotting
    start_times_dt = [datetime.datetime.fromtimestamp(t) for t in starts]
    end_times_dt = [datetime.datetime.fromtimestamp(t) for t in ends]

    # Determine time range
    t_min = min(starts)
    t_max = max(ends)

    # Generate sample timestamps (100 points across the run)
    sample_times_epoch = np.linspace(t_min, t_max, num=100)
    sample_times_dt = [datetime.datetime.fromtimestamp(t) for t in sample_times_epoch]

    # Calculate instantaneous rate at each sample point
    instantaneous_rates = []
    for t in sample_times_epoch:
        # Find chunks active at this timestamp
        mask = (starts <= t) & (t <= ends)
        if mask.any():
            # Sum up throughput of all active chunks
            # Each chunk's instantaneous rate = bytes / runtime
            active_bytes = bytes_read[mask]
            active_runtimes = runtimes[mask]
            # Avoid division by zero
            valid = active_runtimes > 0
            if valid.any():
                rate_Gbps = np.sum(active_bytes[valid] * 8 / 1e9 / active_runtimes[valid])
            else:
                rate_Gbps = 0.0
        else:
            rate_Gbps = 0.0
        instantaneous_rates.append(rate_Gbps)

    # Create plot
    fig, ax1 = plt.subplots(figsize=figsize)

    # Plot throughput
    ax1.fill_between(sample_times_dt, instantaneous_rates, alpha=0.5, color="C1", edgecolor="C1", linewidth=0.5)
    ax1.set_xlabel("Time")
    ax1.set_ylabel("Data Rate (Gbps)", color="C1")
    ax1.tick_params(axis="y", labelcolor="C1")
    ax1.set_ylim([0, max(instantaneous_rates) * 1.1 if instantaneous_rates else 1])
    ax1.grid(True, alpha=0.3)

    # Overlay worker count if available
    if tracking_data and "worker_counts" in tracking_data:
        worker_counts = tracking_data["worker_counts"]
        if worker_counts:
            timestamps = [t for t, _ in worker_counts.items()]
            counts = [c for _, c in worker_counts.items()]

            ax2 = ax1.twinx()
            ax2.plot(timestamps, counts, linewidth=2, color="C0", label="Workers")
            ax2.set_ylabel("Number of Workers", color="C0")
            ax2.tick_params(axis="y", labelcolor="C0")
            ax2.set_ylim([0, max(counts) * 1.1])

    ax1.set_title(title)

    # Format x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax1

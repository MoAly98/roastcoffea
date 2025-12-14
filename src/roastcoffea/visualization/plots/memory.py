"""Memory utilization timeline plotting."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np

from roastcoffea.visualization.utils import (
    add_worker_count_annotation,
    finalize_timeline_plot,
    setup_timeline_axes,
)


def plot_memory_utilization_mean_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 4),
    title: str = "Memory Utilization Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot mean memory utilization percentage over time with min-max band.

    Shows aggregated memory usage across all workers, with mean line and
    shaded min-max range.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_memory and worker_memory_limit
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
        If tracking_data is None or missing memory data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_memory = tracking_data.get("worker_memory", {})
    worker_memory_limit = tracking_data.get("worker_memory_limit", {})

    if not worker_memory or not worker_memory_limit:
        msg = "Memory or memory limit data not available"
        raise ValueError(msg)

    # Collect all unique timestamps
    all_timestamps = set()
    for worker_id in worker_memory:
        for timestamp, _ in worker_memory[worker_id]:
            all_timestamps.add(timestamp)

    sorted_timestamps = sorted(all_timestamps)

    # Calculate memory utilization % at each timestamp
    utilization_pct = []
    utilization_min = []
    utilization_max = []

    for timestamp in sorted_timestamps:
        worker_utils = []

        for worker_id in worker_memory:
            mem_data = worker_memory[worker_id]
            limit_data = worker_memory_limit.get(worker_id, [])

            mem_value = None
            for t, m in mem_data:
                if t == timestamp:
                    mem_value = m
                    break

            limit_value = None
            for t, limit in limit_data:
                if t == timestamp:
                    limit_value = limit
                    break

            if mem_value is not None and limit_value is not None and limit_value > 0:
                util_pct = (mem_value / limit_value) * 100
                worker_utils.append(util_pct)

        if worker_utils:
            utilization_pct.append(np.mean(worker_utils))
            utilization_min.append(np.min(worker_utils))
            utilization_max.append(np.max(worker_utils))
        else:
            utilization_pct.append(0)
            utilization_min.append(0)
            utilization_max.append(0)

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(sorted_timestamps, utilization_pct, linewidth=2, label="Mean", color="C0")
    ax.fill_between(
        sorted_timestamps,
        utilization_min,
        utilization_max,
        alpha=0.3,
        label="Min-Max Range",
        color="C0",
    )

    setup_timeline_axes(ax, ylabel="Memory Utilization (%)", title=title, ylim=(0, 100))
    ax.legend()

    finalize_timeline_plot(fig, ax, output_path)
    return fig, ax


def plot_memory_utilization_per_worker_timeline(
    tracking_data: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Memory Utilization Per Worker Over Time",
    max_legend_entries: int = 5,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot memory utilization percentage per worker over time.

    Shows actual memory usage as percentage of limit (0-100%) for each worker,
    providing insight into memory resource utilization.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with worker_memory and worker_memory_limit
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
        If tracking_data is None or missing memory data
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    worker_memory = tracking_data.get("worker_memory", {})
    worker_memory_limit = tracking_data.get("worker_memory_limit", {})

    if not worker_memory or not worker_memory_limit:
        msg = "Memory or memory limit data not available"
        raise ValueError(msg)

    fig, ax = plt.subplots(figsize=figsize)

    for worker_id, timeline in worker_memory.items():
        if timeline:
            limit_data = worker_memory_limit.get(worker_id, [])
            if not limit_data:
                continue

            timestamps = []
            utilization_values = []

            for timestamp, mem_value in timeline:
                limit_value = None
                for t, limit in limit_data:
                    if t == timestamp:
                        limit_value = limit
                        break

                if limit_value is not None and limit_value > 0:
                    util_pct = (mem_value / limit_value) * 100
                    timestamps.append(timestamp)
                    utilization_values.append(util_pct)

            if timestamps:
                ax.plot(
                    timestamps,
                    utilization_values,
                    label=worker_id,
                    alpha=0.7,
                    linewidth=2,
                )

    setup_timeline_axes(ax, ylabel="Memory Utilization (%)", title=title, ylim=(0, 100))

    num_workers = len(worker_memory)
    if num_workers <= max_legend_entries:
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1), fontsize=8)
    else:
        add_worker_count_annotation(ax, num_workers)

    finalize_timeline_plot(fig, ax, output_path)
    return fig, ax

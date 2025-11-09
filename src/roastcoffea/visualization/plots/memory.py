"""Memory utilization timeline plotting."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np


def plot_memory_utilization_timeline(
    tracking_data: dict[str, Any],
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 4),
    title: str = "Memory Utilization Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot memory utilization percentage over time.

    Parameters
    ----------
    tracking_data : dict
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
    """
    worker_memory = tracking_data.get("worker_memory", {})
    worker_memory_limit = tracking_data.get("worker_memory_limit", {})

    if not worker_memory or not worker_memory_limit:
        raise ValueError("Memory or memory limit data not available")

    # Collect all unique timestamps
    all_timestamps = set()
    for worker_id in worker_memory.keys():
        for timestamp, _ in worker_memory[worker_id]:
            all_timestamps.add(timestamp)

    sorted_timestamps = sorted(all_timestamps)

    # Calculate memory utilization % at each timestamp
    utilization_pct = []
    utilization_min = []
    utilization_max = []

    for timestamp in sorted_timestamps:
        worker_utils = []

        for worker_id in worker_memory.keys():
            # Find memory usage at this timestamp
            mem_data = worker_memory[worker_id]
            limit_data = worker_memory_limit.get(worker_id, [])

            # Find closest timestamp (should be exact match)
            mem_value = None
            for t, m in mem_data:
                if t == timestamp:
                    mem_value = m
                    break

            limit_value = None
            for t, l in limit_data:
                if t == timestamp:
                    limit_value = l
                    break

            if (
                mem_value is not None
                and limit_value is not None
                and limit_value > 0
            ):
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

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot mean with confidence band
    ax.plot(sorted_timestamps, utilization_pct, linewidth=2, label="Mean", color="C0")
    ax.fill_between(
        sorted_timestamps,
        utilization_min,
        utilization_max,
        alpha=0.3,
        label="Min-Max Range",
        color="C0",
    )

    ax.set_xlabel("Time")
    ax.set_ylabel("Memory Utilization (%)")
    ax.set_title(title)
    ax.set_ylim(0, 100)
    ax.grid(True, alpha=0.3)
    ax.legend()

    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

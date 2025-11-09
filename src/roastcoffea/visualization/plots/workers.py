"""Worker count timeline plotting."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def plot_worker_count_timeline(
    tracking_data: dict[str, Any],
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 4),
    title: str = "Worker Count Over Time",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot worker count over time.

    Parameters
    ----------
    tracking_data : dict
        Tracking data with worker_counts
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
    worker_counts = tracking_data.get("worker_counts", {})

    if not worker_counts:
        msg = "No worker count data available"
        raise ValueError(msg)

    # Sort by timestamp
    sorted_items = sorted(worker_counts.items())
    timestamps = [t for t, _ in sorted_items]
    counts = [c for _, c in sorted_items]

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(timestamps, counts, marker="o", linestyle="-", linewidth=2, markersize=4)
    ax.set_xlabel("Time")
    ax.set_ylabel("Number of Workers")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

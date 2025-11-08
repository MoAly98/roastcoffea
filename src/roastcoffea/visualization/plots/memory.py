"""Memory utilization timeline plotting."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


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
    pass

"""Chunk-level performance plots.

Per-chunk timing, memory, and performance breakdowns.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np


def plot_runtime_distribution(
    chunk_metrics: list[dict[str, Any]] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 6),
    title: str = "Chunk Runtime Distribution",
    bins: int = 30,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot histogram of chunk processing runtimes.

    Shows the distribution of wall time per chunk, helping identify
    outliers and understand processing time consistency.

    Parameters
    ----------
    chunk_metrics : list of dict, optional
        Per-chunk metrics from @track_metrics decorator
    output_path : Path, optional
        Save path
    figsize : tuple
        Figure size
    title : str
        Plot title
    bins : int
        Number of histogram bins

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If chunk_metrics is None or empty
    """
    if not chunk_metrics:
        msg = "No chunk metrics available"
        raise ValueError(msg)

    # Extract runtimes
    runtimes = [chunk.get("duration", 0) for chunk in chunk_metrics]

    if not runtimes:
        msg = "No runtime data available in chunk metrics"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot histogram
    ax.hist(runtimes, bins=bins, alpha=0.7, color="steelblue", edgecolor="black")

    # Calculate statistics
    mean_runtime = np.mean(runtimes)
    median_runtime = np.median(runtimes)

    # Add vertical lines for mean and median
    ax.axvline(
        mean_runtime,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Mean: {mean_runtime:.2f}s",
    )
    ax.axvline(
        median_runtime,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"Median: {median_runtime:.2f}s",
    )

    ax.set_xlabel("Runtime (seconds)")
    ax.set_ylabel("Number of Chunks")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_runtime_vs_events(
    chunk_metrics: list[dict[str, Any]] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 6),
    title: str = "Chunk Runtime vs Number of Events",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot scatter plot of chunk runtime vs number of events.

    Shows the relationship between chunk size (events) and processing time,
    helping identify scaling behavior and inefficiencies.

    Parameters
    ----------
    chunk_metrics : list of dict, optional
        Per-chunk metrics from @track_metrics decorator
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
        If chunk_metrics is None or empty
    """
    if not chunk_metrics:
        msg = "No chunk metrics available"
        raise ValueError(msg)

    # Extract data
    events = []
    runtimes = []
    for chunk in chunk_metrics:
        num_events = chunk.get("num_events")
        duration = chunk.get("duration")
        if num_events is not None and duration is not None:
            events.append(num_events)
            runtimes.append(duration)

    if not events or not runtimes:
        msg = "No runtime vs events data available in chunk metrics"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Scatter plot
    ax.scatter(events, runtimes, alpha=0.6, s=50, color="steelblue", edgecolors="black")

    # Fit linear regression for trend line
    if len(events) > 1:
        coeffs = np.polyfit(events, runtimes, 1)
        poly = np.poly1d(coeffs)
        events_sorted = np.sort(events)
        ax.plot(
            events_sorted,
            poly(events_sorted),
            color="red",
            linestyle="--",
            linewidth=2,
            label=f"Trend: {coeffs[0]:.2e}*events + {coeffs[1]:.2f}",
        )

    ax.set_xlabel("Number of Events")
    ax.set_ylabel("Runtime (seconds)")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    if len(events) > 1:
        ax.legend()

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

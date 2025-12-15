"""I/O and data access plots.

Visualizations for compression ratios and data access patterns.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np

from roastcoffea.utils import get_nested


def plot_compression_ratio_distribution(
    metrics: dict[str, Any],
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 6),
    title: str = "Compression Ratio Distribution Across Files",
    bins: int = 20,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot histogram of compression ratios across all files.

    Shows the distribution of compression efficiency (compressed/uncompressed)
    for all ROOT files processed. Lower ratios indicate better compression.

    Parameters
    ----------
    metrics : dict
        Metrics dict containing compression_ratios list
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
        If no compression ratio data is available
    """
    data_access = get_nested(metrics, "summary", "data_access", default={})
    compression_ratios = data_access.get("compression_ratios", []) or []

    if not compression_ratios:
        msg = "No compression ratio data available"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot histogram
    ax.hist(
        compression_ratios, bins=bins, alpha=0.7, color="steelblue", edgecolor="black"
    )

    # Calculate statistics
    mean_ratio = np.mean(compression_ratios)
    median_ratio = np.median(compression_ratios)

    # Add vertical lines for mean and median
    ax.axvline(
        mean_ratio,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Mean: {mean_ratio:.3f}",
    )
    ax.axvline(
        median_ratio,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"Median: {median_ratio:.3f}",
    )

    ax.set_xlabel("Compression Ratio (compressed/uncompressed)")
    ax.set_ylabel("Number of Files")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_data_access_percentage(
    metrics: dict[str, Any],
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 6),
    title: str = "Bytes Read Percentage Distribution",
    bins: int = 20,
) -> tuple[plt.Figure, plt.Axes]:
    """Plot histogram of bytes read percentages across all files.

    Shows what percentage of each file's total bytes were read based on
    which branches were accessed. Helps identify if the analysis is reading
    only a small fraction of the available data.

    Parameters
    ----------
    metrics : dict
        Metrics dict containing bytes_read_percent_per_file list
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
        If no bytes read percentage data is available
    """
    data_access = get_nested(metrics, "summary", "data_access", default={})
    bytes_read_percentages = data_access.get("bytes_read_percent_per_file", []) or []

    if not bytes_read_percentages:
        msg = "No bytes read percentage data available"
        raise ValueError(msg)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot histogram
    ax.hist(
        bytes_read_percentages,
        bins=bins,
        alpha=0.7,
        color="forestgreen",
        edgecolor="black",
    )

    # Calculate statistics
    mean_pct = np.mean(bytes_read_percentages)
    median_pct = np.median(bytes_read_percentages)

    # Add vertical lines for mean and median
    ax.axvline(
        mean_pct,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Mean: {mean_pct:.1f}%",
    )
    ax.axvline(
        median_pct,
        color="blue",
        linestyle="--",
        linewidth=2,
        label=f"Median: {median_pct:.1f}%",
    )

    ax.set_xlabel("Bytes Read (%)")
    ax.set_ylabel("Number of Files")
    ax.set_title(title)
    ax.set_xlim((0, 100))
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

"""I/O and data access plots.

Visualizations for compression ratios and data access patterns.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np


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
    compression_ratios = metrics.get("compression_ratios", [])

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
    bytes_read_percentages = metrics.get("bytes_read_percent_per_file", [])

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
    # Dynamic x-axis range based on data
    max_pct = max(bytes_read_percentages)
    ax.set_xlim((0, min(100, max_pct * 1.1)))  # 10% padding, capped at 100
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_branch_access_per_chunk(
    chunk_metrics: list[dict[str, Any]],
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Branches Accessed Per Chunk",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot number of branches accessed per chunk.

    Shows variation in branch access patterns across chunks, useful for
    identifying chunks with different data access patterns.

    Parameters
    ----------
    chunk_metrics : list of dict
        Raw chunk metrics from @track_metrics decorator
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
        If no chunk metrics data is available
    """
    if not chunk_metrics:
        msg = "No chunk metrics data available"
        raise ValueError(msg)

    # Extract per-chunk branch counts
    branch_counts = [chunk.get("num_branches_accessed", 0) for chunk in chunk_metrics]

    if not any(branch_counts):
        msg = "No branch access data in chunk metrics"
        raise ValueError(msg)

    fig, ax = plt.subplots(figsize=figsize)

    chunk_indices = range(len(branch_counts))
    ax.bar(
        chunk_indices, branch_counts, alpha=0.7, color="steelblue", edgecolor="black"
    )

    # Add mean line
    mean_branches = np.mean(branch_counts)
    ax.axhline(
        mean_branches,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Mean: {mean_branches:.1f}",
    )

    ax.set_xlabel("Chunk Index")
    ax.set_ylabel("Branches Accessed")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_bytes_accessed_per_chunk(
    chunk_metrics: list[dict[str, Any]],
    output_path: Path | None = None,
    figsize: tuple[int, int] = (12, 6),
    title: str = "Bytes Accessed Per Chunk (Compressed vs Uncompressed)",
) -> tuple[plt.Figure, plt.Axes]:
    """Plot compressed and uncompressed bytes accessed per chunk.

    Shows data volume patterns across chunks, comparing on-disk (compressed)
    vs in-memory (uncompressed) sizes.

    Parameters
    ----------
    chunk_metrics : list of dict
        Raw chunk metrics from @track_metrics decorator
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
        If no chunk metrics data is available
    """
    if not chunk_metrics:
        msg = "No chunk metrics data available"
        raise ValueError(msg)

    # Extract per-chunk byte counts (in MB)
    compressed = [chunk.get("accessed_bytes", 0) / 1e6 for chunk in chunk_metrics]
    uncompressed = [
        chunk.get("accessed_uncompressed_bytes", 0) / 1e6 for chunk in chunk_metrics
    ]

    if not any(compressed) and not any(uncompressed):
        msg = "No bytes access data in chunk metrics"
        raise ValueError(msg)

    fig, ax = plt.subplots(figsize=figsize)

    chunk_indices = np.arange(len(compressed))
    width = 0.35

    ax.bar(
        chunk_indices - width / 2,
        compressed,
        width,
        label="Compressed (on-disk)",
        alpha=0.7,
        color="steelblue",
    )
    ax.bar(
        chunk_indices + width / 2,
        uncompressed,
        width,
        label="Uncompressed (in-memory)",
        alpha=0.7,
        color="coral",
    )

    ax.set_xlabel("Chunk Index")
    ax.set_ylabel("Bytes (MB)")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

"""Scaling efficiency plots.

Visualizations for parallel efficiency and scaling analysis.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


def plot_efficiency_summary(
    metrics: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 6),
) -> tuple[plt.Figure, plt.Axes]:
    """Plot efficiency metrics summary as bar chart.

    Shows core efficiency, speedup factor, and other efficiency metrics
    in a compact bar chart format.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary with efficiency metrics
    output_path : Path, optional
        Save path
    figsize : tuple
        Figure size

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If metrics is None or missing required data
    """
    if metrics is None:
        msg = "metrics cannot be None"
        raise ValueError(msg)

    # Extract efficiency metrics
    core_efficiency = metrics.get("core_efficiency")
    speedup_factor = metrics.get("speedup_factor")

    if core_efficiency is None and speedup_factor is None:
        msg = "No efficiency metrics available (core_efficiency or speedup_factor)"
        raise ValueError(msg)

    # Prepare data for plotting
    metric_names = []
    metric_values = []

    if core_efficiency is not None:
        metric_names.append("Core Efficiency")
        metric_values.append(core_efficiency * 100)  # Convert to percentage

    if speedup_factor is not None:
        metric_names.append("Speedup Factor")
        metric_values.append(speedup_factor)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    bars = ax.bar(
        metric_names, metric_values, color=["steelblue", "coral"][: len(metric_names)]
    )

    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{height:.1f}",
            ha="center",
            va="bottom",
            fontsize=12,
            fontweight="bold",
        )

    ax.set_ylabel("Value")
    ax.set_title("Efficiency Metrics Summary")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax


def plot_resource_utilization(
    metrics: dict[str, Any] | None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (10, 6),
) -> tuple[plt.Figure, plt.Axes]:
    """Plot resource utilization summary.

    Shows worker count, cores, and memory statistics in a grouped bar chart.

    Parameters
    ----------
    metrics : dict
        Metrics dictionary with worker and resource metrics
    output_path : Path, optional
        Save path
    figsize : tuple
        Figure size

    Returns
    -------
    fig, ax : Figure and Axes
        Matplotlib figure and axes

    Raises
    ------
    ValueError
        If metrics is None or missing required data
    """
    if metrics is None:
        msg = "metrics cannot be None"
        raise ValueError(msg)

    # Extract resource metrics
    avg_workers = metrics.get("avg_workers")
    total_cores = metrics.get("total_cores")
    peak_memory_bytes = metrics.get("peak_memory_bytes")
    peak_memory_gb = peak_memory_bytes / 1e9 if peak_memory_bytes else None

    if avg_workers is None and total_cores is None and peak_memory_gb is None:
        msg = "No resource metrics available (workers, cores, or memory)"
        raise ValueError(msg)

    # Prepare data for plotting
    resource_names = []
    resource_values = []

    if avg_workers is not None:
        resource_names.append("Avg Workers")
        resource_values.append(avg_workers)

    if total_cores is not None:
        resource_names.append("Total Cores")
        resource_values.append(total_cores)

    if peak_memory_gb is not None:
        resource_names.append("Peak Memory (GB)")
        resource_values.append(peak_memory_gb)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    colors = ["steelblue", "coral", "lightgreen"][: len(resource_names)]
    bars = ax.bar(resource_names, resource_values, color=colors)

    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{height:.1f}",
            ha="center",
            va="bottom",
            fontsize=12,
            fontweight="bold",
        )

    ax.set_ylabel("Value")
    ax.set_title("Resource Utilization Summary")
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    return fig, ax

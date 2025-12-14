"""Visualization utility functions.

Common helpers for plotting and interactive features.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def validate_tracking_data(
    tracking_data: dict[str, Any] | None,
    data_key: str,
    error_msg: str,
) -> dict[str, Any]:
    """Validate tracking_data and extract a data dictionary.

    Parameters
    ----------
    tracking_data : dict or None
        The tracking data dictionary
    data_key : str
        Key to extract from tracking_data
    error_msg : str
        Error message if data is missing

    Returns
    -------
    dict
        The extracted data dictionary

    Raises
    ------
    ValueError
        If tracking_data is None or data_key is missing/empty
    """
    if tracking_data is None:
        msg = "tracking_data cannot be None"
        raise ValueError(msg)

    data = tracking_data.get(data_key, {})
    if not data:
        raise ValueError(error_msg)

    return data


def setup_timeline_axes(
    ax: plt.Axes,
    xlabel: str = "Time",
    ylabel: str = "",
    title: str = "",
    ylim: tuple[float, float] | None = None,
    grid_alpha: float = 0.3,
) -> None:
    """Set up common axis properties for timeline plots.

    Parameters
    ----------
    ax : plt.Axes
        The matplotlib axes to configure
    xlabel : str
        X-axis label (default: "Time")
    ylabel : str
        Y-axis label
    title : str
        Plot title
    ylim : tuple, optional
        Y-axis limits as (min, max)
    grid_alpha : float
        Grid transparency (default: 0.3)
    """
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=grid_alpha)
    if ylim is not None:
        ax.set_ylim(ylim)


def finalize_timeline_plot(
    fig: plt.Figure,
    ax: plt.Axes,
    output_path: Path | None = None,
) -> None:
    """Finalize a timeline plot with common formatting.

    Formats x-axis for timestamps, applies tight layout, and optionally saves.

    Parameters
    ----------
    fig : plt.Figure
        The matplotlib figure
    ax : plt.Axes
        The matplotlib axes
    output_path : Path, optional
        If provided, save the figure to this path
    """
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.xticks(rotation=45)
    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")


def add_worker_count_annotation(
    ax: plt.Axes,
    num_workers: int,
) -> None:
    """Add a text annotation showing worker count.

    Used when there are too many workers to show in legend.

    Parameters
    ----------
    ax : plt.Axes
        The matplotlib axes
    num_workers : int
        Number of workers to display
    """
    ax.text(
        0.02,
        0.98,
        f"Showing {num_workers} workers",
        transform=ax.transAxes,
        va="top",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "white", "alpha": 0.7},
    )

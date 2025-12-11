"""Visualization utility functions.

Common helpers for plotting and interactive features.
"""

from __future__ import annotations

from typing import Any


def add_hover_tooltips(lines: list[Any], labels: list[str]) -> None:
    """Add interactive hover tooltips to plot lines.

    Uses mplcursors to add hover tooltips showing line labels and values.
    Gracefully handles cases where mplcursors is unavailable or the backend
    doesn't support interactivity.

    Parameters
    ----------
    lines : list
        List of matplotlib Line2D objects
    labels : list of str
        Labels corresponding to each line (e.g., worker IDs)

    Notes
    -----
    This function silently does nothing if:
    - mplcursors is not installed
    - The matplotlib backend doesn't support interactivity
    - Any other error occurs during tooltip setup

    Examples
    --------
    >>> fig, ax = plt.subplots()
    >>> lines = []
    >>> labels = []
    >>> for worker_id, data in worker_data.items():
    ...     line, = ax.plot(x, y, label=worker_id)
    ...     lines.append(line)
    ...     labels.append(worker_id)
    >>> add_hover_tooltips(lines, labels)
    """
    try:
        import mplcursors

        # Create mapping of line objects to labels
        line_labels = dict(zip(lines, labels))

        # Create cursor with hover activation
        cursor = mplcursors.cursor(lines, hover=True)

        # Configure tooltip content
        @cursor.connect("add")
        def on_add(sel):
            # Get the label for this line
            label = line_labels.get(sel.artist, "Unknown")

            # Format tooltip text: label + value
            sel.annotation.set_text(f"{label}\n{sel.target[1]:.2f}")

    except (ImportError, Exception):
        # mplcursors not installed, backend doesn't support it, or other error
        # Silently skip hover functionality
        pass

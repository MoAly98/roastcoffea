"""Workflow-level metrics aggregation.

Calculates throughput, data rates, event rates, compression ratios,
and overall timing metrics from coffea reports and tracking data.
"""

from __future__ import annotations

from typing import Any


def aggregate_workflow_metrics(
    coffea_report: dict[str, Any],
    t_start: float,
    t_end: float,
    custom_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Calculate workflow metrics from Coffea report.

    Parameters
    ----------
    coffea_report : dict
        Coffea report from Runner
    t_start : float
        Start time
    t_end : float
        End time
    custom_metrics : dict, optional
        Per-dataset custom metrics

    Returns
    -------
    dict
        Workflow metrics
    """
    pass

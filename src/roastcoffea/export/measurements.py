"""Save and load benchmark measurements for later reanalysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def save_measurement(
    metrics: dict[str, Any],
    t0: float,
    t1: float,
    output_dir: Path,
    measurement_name: str | None = None,
    config: dict[str, Any] | None = None,
) -> Path:
    """Save benchmark measurement to disk.

    Parameters
    ----------
    metrics : dict
        Performance metrics
    t0 : float
        Start timestamp
    t1 : float
        End timestamp
    output_dir : Path
        Output directory
    measurement_name : str, optional
        Measurement directory name
    config : dict, optional
        Configuration to save

    Returns
    -------
    Path
        Path to measurement directory
    """
    pass


def load_measurement(measurement_path: Path) -> tuple[dict[str, Any], float, float]:
    """Load saved measurement.

    Parameters
    ----------
    measurement_path : Path
        Measurement directory

    Returns
    -------
    metrics : dict
        Performance metrics
    t0 : float
        Start timestamp
    t1 : float
        End timestamp
    """
    pass

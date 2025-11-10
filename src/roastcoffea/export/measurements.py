"""Save and load benchmark measurements for later reanalysis."""

from __future__ import annotations

import json
from datetime import datetime
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
    # Create timestamped measurement directory name if not provided
    if measurement_name is None:
        measurement_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create measurement directory
    measurement_path = Path(output_dir) / measurement_name
    measurement_path.mkdir(parents=True, exist_ok=True)

    # Save metrics with timestamp
    metrics_file = measurement_path / "metrics.json"
    with Path(metrics_file).open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)

    # Save timing information
    with Path(measurement_path / "start_end_time.txt").open("w", encoding="utf-8") as f:
        f.write(f"{t0},{t1}\n")

    # Save config if provided
    if config is not None:
        with Path(measurement_path / "config.json").open("w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, default=str)

    # Save measurement metadata
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "wall_time": t1 - t0,
        "format": "roastcoffea_measurement_v1",
    }
    with Path(measurement_path / "metadata.json").open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return measurement_path


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
    measurement_path = Path(measurement_path)

    if not measurement_path.exists():
        msg = f"Measurement directory not found: {measurement_path}"
        raise FileNotFoundError(msg)

    # Load metrics
    metrics_file = measurement_path / "metrics.json"
    if not metrics_file.exists():
        msg = f"Metrics file not found: {metrics_file}"
        raise FileNotFoundError(msg)

    with Path(metrics_file).open(encoding="utf-8") as f:
        metrics = json.load(f)

    # Load timing
    timing_file = measurement_path / "start_end_time.txt"
    if not timing_file.exists():
        msg = f"Timing file not found: {timing_file}"
        raise FileNotFoundError(msg)

    with Path(timing_file).open(encoding="utf-8") as f:
        timing_line = f.readline().strip()
        try:
            t0_str, t1_str = timing_line.split(",")
            t0 = float(t0_str)
            t1 = float(t1_str)
        except (ValueError, AttributeError) as e:
            msg = f"Invalid timing format in {timing_file}: {e}"
            raise ValueError(msg) from e

    return metrics, t0, t1

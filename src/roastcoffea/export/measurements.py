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
    measurement_path = output_dir / measurement_name
    measurement_path.mkdir(parents=True, exist_ok=True)

    # Save metrics with timestamp
    metrics_file = measurement_path / "metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    # Save timing information
    with open(measurement_path / "start_end_time.txt", "w") as f:
        f.write(f"{t0},{t1}\n")

    # Save config if provided
    if config is not None:
        with open(measurement_path / "config.json", "w") as f:
            json.dump(config, f, indent=2, default=str)

    # Save measurement metadata
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "wall_time": t1 - t0,
        "format": "roastcoffea_measurement_v1",
    }
    with open(measurement_path / "metadata.json", "w") as f:
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
        raise FileNotFoundError(f"Measurement directory not found: {measurement_path}")

    # Load metrics
    metrics_file = measurement_path / "metrics.json"
    if not metrics_file.exists():
        raise FileNotFoundError(f"Metrics file not found: {metrics_file}")

    with open(metrics_file) as f:
        metrics = json.load(f)

    # Load timing
    timing_file = measurement_path / "start_end_time.txt"
    if not timing_file.exists():
        raise FileNotFoundError(f"Timing file not found: {timing_file}")

    with open(timing_file) as f:
        timing_line = f.readline().strip()
        try:
            t0_str, t1_str = timing_line.split(",")
            t0 = float(t0_str)
            t1 = float(t1_str)
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid timing format in {timing_file}: {e}") from e

    return metrics, t0, t1

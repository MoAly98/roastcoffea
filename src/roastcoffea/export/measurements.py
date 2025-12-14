"""Save and load benchmark measurements for later reanalysis."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _serialize_for_json(obj: Any) -> Any:
    """Recursively convert datetime objects and tuple keys to JSON-serializable format.

    Parameters
    ----------
    obj : Any
        Object to serialize

    Returns
    -------
    Any
        JSON-serializable object
    """
    if isinstance(obj, dict):
        return {_serialize_key(k): _serialize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize_for_json(item) for item in obj]
    if isinstance(obj, tuple):
        # Convert tuples to lists for JSON (tuples in values, not keys)
        return [_serialize_for_json(item) for item in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def _serialize_key(key: Any) -> str:
    """Convert dictionary keys to JSON-compatible strings.

    Parameters
    ----------
    key : Any
        Dictionary key (may be tuple, datetime, or primitive)

    Returns
    -------
    str
        String representation of key
    """
    if isinstance(key, datetime):
        return key.isoformat()
    if isinstance(key, tuple):
        # Convert tuple keys to string representation
        return str(key)
    if isinstance(key, str):
        return key
    # Convert int, float, bool, None, and anything else to string
    return str(key)


def _deserialize_tracking_data(
    tracking_data: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Convert ISO timestamp strings back to datetime objects in tracking_data.

    Parameters
    ----------
    tracking_data : dict or None
        Tracking data with ISO string timestamps

    Returns
    -------
    dict or None
        Tracking data with datetime objects
    """
    if tracking_data is None:
        return None

    result = {}

    # Convert worker_counts keys from ISO strings to datetime
    if "worker_counts" in tracking_data:
        result["worker_counts"] = {
            datetime.fromisoformat(k): v
            for k, v in tracking_data["worker_counts"].items()
        }

    # Convert worker_memory timestamps from ISO strings to datetime
    if "worker_memory" in tracking_data:
        result["worker_memory"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_memory"].items()
        }

    # Convert worker_memory_limit timestamps from ISO strings to datetime
    if "worker_memory_limit" in tracking_data:
        result["worker_memory_limit"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_memory_limit"].items()
        }

    # Convert worker_active_tasks timestamps from ISO strings to datetime
    if "worker_active_tasks" in tracking_data:
        result["worker_active_tasks"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_active_tasks"].items()
        }

    # Convert worker_cores timestamps from ISO strings to datetime
    if "worker_cores" in tracking_data:
        result["worker_cores"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_cores"].items()
        }

    # Convert worker_nbytes timestamps from ISO strings to datetime
    if "worker_nbytes" in tracking_data:
        result["worker_nbytes"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_nbytes"].items()
        }

    # Convert worker_occupancy timestamps from ISO strings to datetime
    if "worker_occupancy" in tracking_data:
        result["worker_occupancy"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_occupancy"].items()
        }

    # Convert worker_executing timestamps from ISO strings to datetime
    if "worker_executing" in tracking_data:
        result["worker_executing"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_executing"].items()
        }

    # Convert worker_last_seen timestamps from ISO strings to datetime
    if "worker_last_seen" in tracking_data:
        result["worker_last_seen"] = {
            worker_id: [(datetime.fromisoformat(ts), val) for ts, val in data]
            for worker_id, data in tracking_data["worker_last_seen"].items()
        }

    # Preserve legacy cores_per_worker if present (for backwards compatibility)
    if "cores_per_worker" in tracking_data:
        result["cores_per_worker"] = tracking_data["cores_per_worker"]

    return result


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

    output_dir = Path(output_dir)
    # Create measurement directory
    measurement_path = Path(output_dir) / measurement_name
    measurement_path.mkdir(parents=True, exist_ok=True)

    # Save metrics with timestamp (serialize datetime objects first)
    metrics_file = measurement_path / "metrics.json"
    serialized_metrics = _serialize_for_json(metrics)
    with Path(metrics_file).open("w", encoding="utf-8") as f:
        json.dump(serialized_metrics, f, indent=2)

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
        "elapsed_time_seconds": t1 - t0,
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

    # Deserialize tracking_data timestamps back to datetime objects
    if "tracking_data" in metrics:
        metrics["tracking_data"] = _deserialize_tracking_data(metrics["tracking_data"])

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

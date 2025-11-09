"""Tests for measurement save/load functionality."""

from __future__ import annotations

import json
import pathlib

import pytest

from roastcoffea.export.measurements import load_measurement, save_measurement


class TestSaveMeasurement:
    """Test saving benchmark measurements to disk."""

    def test_save_creates_directory(self, tmp_path):
        """save_measurement creates measurement directory."""
        metrics = {"wall_time": 100.0, "throughput": 50.0}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        assert measurement_path.exists()
        assert measurement_path.is_dir()
        assert measurement_path.name == "test_run"

    def test_save_creates_timestamped_directory_if_no_name(self, tmp_path):
        """save_measurement creates timestamped directory if name not provided."""
        metrics = {"wall_time": 100.0}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
        )

        assert measurement_path.exists()
        # Should have timestamp format YYYY-MM-DD_HH-MM-SS
        assert "_" in measurement_path.name
        assert "-" in measurement_path.name

    def test_save_creates_metrics_file(self, tmp_path):
        """save_measurement creates metrics JSON file."""
        metrics = {
            "wall_time": 100.0,
            "throughput_gbps": 1.5,
            "events_processed": 1_000_000,
        }

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        metrics_file = measurement_path / "metrics.json"
        assert metrics_file.exists()

        with pathlib.Path(metrics_file).open(encoding="utf-8") as f:
            saved_metrics = json.load(f)

        assert saved_metrics["wall_time"] == 100.0
        assert saved_metrics["throughput_gbps"] == 1.5
        assert saved_metrics["events_processed"] == 1_000_000

    def test_save_creates_timing_file(self, tmp_path):
        """save_measurement creates timing file with t0, t1."""
        metrics = {"wall_time": 50.0}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=10.5,
            t1=60.5,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        timing_file = measurement_path / "start_end_time.txt"
        assert timing_file.exists()

        with pathlib.Path(timing_file).open(encoding="utf-8") as f:
            content = f.read().strip()

        assert content == "10.5,60.5"

    def test_save_creates_metadata_file(self, tmp_path):
        """save_measurement creates metadata file."""
        metrics = {"wall_time": 100.0}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        metadata_file = measurement_path / "metadata.json"
        assert metadata_file.exists()

        with pathlib.Path(metadata_file).open(encoding="utf-8") as f:
            metadata = json.load(f)

        assert "timestamp" in metadata
        assert "wall_time" in metadata
        assert metadata["wall_time"] == 100.0
        assert "format" in metadata

    def test_save_with_config(self, tmp_path):
        """save_measurement optionally saves config."""
        metrics = {"wall_time": 100.0}
        config = {"dataset": "TTbar", "year": "2018", "analysis": "dihiggs"}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
            measurement_name="test_run",
            config=config,
        )

        config_file = measurement_path / "config.json"
        assert config_file.exists()

        with pathlib.Path(config_file).open(encoding="utf-8") as f:
            saved_config = json.load(f)

        assert saved_config == config

    def test_save_without_config(self, tmp_path):
        """save_measurement works without config."""
        metrics = {"wall_time": 100.0}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        config_file = measurement_path / "config.json"
        assert not config_file.exists()


class TestLoadMeasurement:
    """Test loading saved measurements."""

    def test_load_reads_metrics(self, tmp_path):
        """load_measurement reads saved metrics."""
        metrics = {"wall_time": 100.0, "throughput": 50.0}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=0.0,
            t1=100.0,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        loaded_metrics, t0, t1 = load_measurement(measurement_path)

        assert loaded_metrics["wall_time"] == 100.0
        assert loaded_metrics["throughput"] == 50.0
        assert t0 == 0.0
        assert t1 == 100.0

    def test_load_reads_timing(self, tmp_path):
        """load_measurement reads timing data."""
        metrics = {"events": 1000}

        measurement_path = save_measurement(
            metrics=metrics,
            t0=123.45,
            t1=678.90,
            output_dir=tmp_path,
            measurement_name="test_run",
        )

        _, t0, t1 = load_measurement(measurement_path)

        assert t0 == pytest.approx(123.45)
        assert t1 == pytest.approx(678.90)

    def test_load_raises_if_directory_missing(self, tmp_path):
        """load_measurement raises FileNotFoundError if directory doesn't exist."""
        nonexistent_path = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError):
            load_measurement(nonexistent_path)

    def test_load_raises_if_metrics_missing(self, tmp_path):
        """load_measurement raises FileNotFoundError if metrics file missing."""
        # Create directory but no metrics file
        measurement_path = tmp_path / "incomplete"
        measurement_path.mkdir()

        with pytest.raises(FileNotFoundError):
            load_measurement(measurement_path)

    def test_load_raises_if_timing_missing(self, tmp_path):
        """load_measurement raises FileNotFoundError if timing file missing."""
        measurement_path = tmp_path / "incomplete"
        measurement_path.mkdir()

        # Create metrics file but no timing file
        metrics_file = measurement_path / "metrics.json"
        with pathlib.Path(metrics_file).open("w", encoding="utf-8") as f:
            json.dump({"wall_time": 100.0}, f)

        with pytest.raises(FileNotFoundError):
            load_measurement(measurement_path)

    def test_save_and_load_roundtrip(self, tmp_path):
        """Roundtrip save and load preserves data."""
        original_metrics = {
            "wall_time": 100.0,
            "throughput_gbps": 1.5,
            "events_processed": 1_000_000,
            "core_efficiency": 0.85,
        }

        measurement_path = save_measurement(
            metrics=original_metrics,
            t0=10.0,
            t1=110.0,
            output_dir=tmp_path,
            measurement_name="roundtrip_test",
        )

        loaded_metrics, t0, t1 = load_measurement(measurement_path)

        assert loaded_metrics == original_metrics
        assert t0 == 10.0
        assert t1 == 110.0

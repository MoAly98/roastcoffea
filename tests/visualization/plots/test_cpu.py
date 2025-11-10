"""Tests for CPU and occupancy timeline plotting."""

from __future__ import annotations

import datetime

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.cpu import (
    plot_executing_tasks_timeline,
    plot_occupancy_timeline,
)


class TestPlotOccupancyTimeline:
    """Test occupancy timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data for plotting."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)

        return {
            "worker_occupancy": {
                "worker1": [
                    (t0, 0.5),
                    (t1, 0.8),
                    (t2, 0.6),
                ],
                "worker2": [
                    (t0, 0.3),
                    (t1, 0.9),
                    (t2, 0.7),
                ],
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_occupancy_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_occupancy_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_occupancy_per_worker(self, sample_tracking_data):
        """Occupancy timeline plots one line per worker."""
        fig, ax = plot_occupancy_timeline(sample_tracking_data)

        # Should have two lines (one per worker)
        lines = ax.get_lines()
        assert len(lines) == 2

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Occupancy plot has correct axis labels and title."""
        fig, ax = plot_occupancy_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Occupancy (saturation)"
        assert ax.get_title() == "Worker Occupancy Over Time"

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_occupancy_timeline(sample_tracking_data, title="Custom Title")

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "occupancy_timeline.png"

        fig, _ax = plot_occupancy_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_tracking_data(self):
        """Raises ValueError if tracking_data is None."""
        with pytest.raises(ValueError, match="tracking_data cannot be None"):
            plot_occupancy_timeline(None)

    def test_raises_on_missing_data(self):
        """Raises ValueError if occupancy data missing."""
        with pytest.raises(ValueError, match="No worker occupancy data available"):
            plot_occupancy_timeline({"worker_occupancy": {}})


class TestPlotExecutingTasksTimeline:
    """Test executing tasks timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data for plotting."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)

        return {
            "worker_executing": {
                "worker1": [
                    (t0, 2),
                    (t1, 4),
                    (t2, 3),
                ],
                "worker2": [
                    (t0, 1),
                    (t1, 3),
                    (t2, 2),
                ],
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_executing_tasks_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_executing_tasks_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_executing_per_worker(self, sample_tracking_data):
        """Executing tasks timeline plots one line per worker."""
        fig, ax = plot_executing_tasks_timeline(sample_tracking_data)

        # Should have two lines (one per worker)
        lines = ax.get_lines()
        assert len(lines) == 2

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Executing tasks plot has correct axis labels and title."""
        fig, ax = plot_executing_tasks_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Number of Executing Tasks"
        assert ax.get_title() == "Executing Tasks Per Worker Over Time"

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_executing_tasks_timeline(
            sample_tracking_data, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "executing_timeline.png"

        fig, _ax = plot_executing_tasks_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_tracking_data(self):
        """Raises ValueError if tracking_data is None."""
        with pytest.raises(ValueError, match="tracking_data cannot be None"):
            plot_executing_tasks_timeline(None)

    def test_raises_on_missing_data(self):
        """Raises ValueError if executing data missing."""
        with pytest.raises(
            ValueError, match="No worker executing tasks data available"
        ):
            plot_executing_tasks_timeline({"worker_executing": {}})

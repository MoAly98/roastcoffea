"""Tests for throughput and activity timeline plotting."""

from __future__ import annotations

import datetime

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.throughput import (
    plot_total_active_tasks_timeline,
    plot_worker_activity_timeline,
)


class TestPlotWorkerActivityTimeline:
    """Test worker activity timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data for plotting."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)

        return {
            "worker_active_tasks": {
                "worker1": [
                    (t0, 5),
                    (t1, 10),
                    (t2, 7),
                ],
                "worker2": [
                    (t0, 3),
                    (t1, 8),
                    (t2, 6),
                ],
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_worker_activity_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_worker_activity_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_activity_per_worker(self, sample_tracking_data):
        """Activity timeline plots one line per worker."""
        fig, ax = plot_worker_activity_timeline(sample_tracking_data)

        # Should have two lines (one per worker)
        lines = ax.get_lines()
        assert len(lines) == 2

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Activity plot has correct axis labels and title."""
        fig, ax = plot_worker_activity_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Number of Active Tasks"
        assert ax.get_title() == "Worker Activity Over Time"

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_worker_activity_timeline(
            sample_tracking_data, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "activity_timeline.png"

        fig, _ax = plot_worker_activity_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_tracking_data(self):
        """Raises ValueError if tracking_data is None."""
        with pytest.raises(ValueError, match="tracking_data cannot be None"):
            plot_worker_activity_timeline(None)

    def test_raises_on_missing_data(self):
        """Raises ValueError if active tasks data missing."""
        with pytest.raises(ValueError, match="No worker active tasks data available"):
            plot_worker_activity_timeline({"worker_active_tasks": {}})


class TestPlotTotalActiveTasksTimeline:
    """Test total active tasks timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data for plotting."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)

        return {
            "worker_active_tasks": {
                "worker1": [
                    (t0, 5),
                    (t1, 10),
                    (t2, 7),
                ],
                "worker2": [
                    (t0, 3),
                    (t1, 8),
                    (t2, 6),
                ],
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_total_active_tasks_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_total_active_tasks_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_total_activity(self, sample_tracking_data):
        """Total activity timeline plots aggregated data."""
        fig, ax = plot_total_active_tasks_timeline(sample_tracking_data)

        # Should have one line (aggregated)
        lines = ax.get_lines()
        assert len(lines) == 1

        # Should have 3 data points
        line = lines[0]
        xdata, ydata = line.get_data()
        assert len(xdata) == 3
        assert len(ydata) == 3

        # Y data should be sum across workers: [5+3, 10+8, 7+6] = [8, 18, 13]
        assert list(ydata) == [8, 18, 13]

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Total activity plot has correct axis labels and title."""
        fig, ax = plot_total_active_tasks_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Total Active Tasks"
        assert ax.get_title() == "Total Active Tasks Over Time"

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_total_active_tasks_timeline(
            sample_tracking_data, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "total_activity_timeline.png"

        fig, _ax = plot_total_active_tasks_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_tracking_data(self):
        """Raises ValueError if tracking_data is None."""
        with pytest.raises(ValueError, match="tracking_data cannot be None"):
            plot_total_active_tasks_timeline(None)

    def test_raises_on_missing_data(self):
        """Raises ValueError if active tasks data missing."""
        with pytest.raises(ValueError, match="No worker active tasks data available"):
            plot_total_active_tasks_timeline({"worker_active_tasks": {}})

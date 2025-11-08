"""Tests for worker count timeline plotting."""

from __future__ import annotations

import datetime

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.workers import plot_worker_count_timeline


class TestPlotWorkerCountTimeline:
    """Test worker count timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data for plotting."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)
        t3 = datetime.datetime(2025, 1, 1, 12, 0, 30)

        return {
            "worker_counts": {
                t0: 2,
                t1: 4,
                t2: 4,
                t3: 3,
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_worker_count_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_worker_count_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_worker_counts(self, sample_tracking_data):
        """Worker count timeline plots correct data."""
        fig, ax = plot_worker_count_timeline(sample_tracking_data)

        # Should have one line
        lines = ax.get_lines()
        assert len(lines) == 1

        # Line should have 4 data points
        line = lines[0]
        xdata, ydata = line.get_data()
        assert len(xdata) == 4
        assert len(ydata) == 4

        # Y data should match worker counts
        assert list(ydata) == [2, 4, 4, 3]

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Worker count plot has correct axis labels and title."""
        fig, ax = plot_worker_count_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Number of Workers"
        assert ax.get_title() == "Worker Count Over Time"

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_worker_count_timeline(
            sample_tracking_data, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_figsize(self, sample_tracking_data):
        """Can set custom figure size."""
        fig, ax = plot_worker_count_timeline(sample_tracking_data, figsize=(12, 6))

        # Check figure size (approximately, due to DPI)
        width, height = fig.get_size_inches()
        assert width == pytest.approx(12, rel=0.1)
        assert height == pytest.approx(6, rel=0.1)

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "worker_timeline.png"

        fig, ax = plot_worker_count_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_empty_data(self):
        """Raises ValueError on empty worker count data."""
        tracking_data = {"worker_counts": {}}

        with pytest.raises(ValueError, match="No worker count data"):
            plot_worker_count_timeline(tracking_data)

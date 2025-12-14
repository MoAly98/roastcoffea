"""Tests for memory utilization timeline plotting."""

from __future__ import annotations

import datetime

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.memory import (
    plot_memory_utilization_mean_timeline,
    plot_memory_utilization_per_worker_timeline,
)


class TestPlotMemoryUtilizationMeanTimeline:
    """Test memory utilization mean timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data with memory information."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)

        return {
            "worker_memory": {
                "worker1": [
                    (t0, 1_000_000_000),  # 1 GB / 4 GB = 25%
                    (t1, 2_000_000_000),  # 2 GB / 4 GB = 50%
                    (t2, 3_000_000_000),  # 3 GB / 4 GB = 75%
                ],
                "worker2": [
                    (t0, 800_000_000),  # 0.8 GB / 4 GB = 20%
                    (t1, 1_600_000_000),  # 1.6 GB / 4 GB = 40%
                    (t2, 2_400_000_000),  # 2.4 GB / 4 GB = 60%
                ],
            },
            "worker_memory_limit": {
                "worker1": [
                    (t0, 4_000_000_000),
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],
                "worker2": [
                    (t0, 4_000_000_000),
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_memory_utilization_mean_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_memory_utilization_mean_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_memory_utilization_percentage(self, sample_tracking_data):
        """Memory utilization is plotted as percentage."""
        fig, ax = plot_memory_utilization_mean_timeline(sample_tracking_data)

        # Y-axis should be limited to 0-100%
        ymin, ymax = ax.get_ylim()
        assert ymin == 0
        assert ymax == 100

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Memory utilization plot has correct labels."""
        fig, ax = plot_memory_utilization_mean_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Memory Utilization (%)"
        assert ax.get_title() == "Memory Utilization Over Time"

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_memory_utilization_mean_timeline(
            sample_tracking_data, title="Custom Memory Title"
        )

        assert ax.get_title() == "Custom Memory Title"

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save memory utilization plot to file."""
        output_file = tmp_path / "memory_util.png"

        fig, _ax = plot_memory_utilization_mean_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_missing_memory_data(self):
        """Raises ValueError if memory data missing."""
        tracking_data: dict[str, dict] = {
            "worker_memory": {},
            "worker_memory_limit": {},
        }

        with pytest.raises(ValueError, match=r"Memory.*not available"):
            plot_memory_utilization_mean_timeline(tracking_data)

    def test_raises_on_missing_memory_limit(self):
        """Raises ValueError if memory limit data missing."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)

        tracking_data = {
            "worker_memory": {
                "worker1": [(t0, 1_000_000_000)],
            },
            "worker_memory_limit": {},  # Missing
        }

        with pytest.raises(ValueError, match=r"Memory.*not available"):
            plot_memory_utilization_mean_timeline(tracking_data)

    def test_raises_on_none_tracking_data(self):
        """Raises ValueError when tracking_data is None."""
        with pytest.raises(ValueError, match=r"tracking_data cannot be None"):
            plot_memory_utilization_mean_timeline(None)

    def test_handles_empty_worker_utils_at_timestamp(self):
        """Handles timestamps where no workers have utilization data (lines 91-93)."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 1)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 2)

        # At t1, limit exists but no memory data for any worker
        tracking_data = {
            "worker_memory": {
                "worker1": [
                    (t0, 1_000_000_000),
                    (t2, 2_000_000_000),
                ],  # Data at t0 and t2, but NOT t1
            },
            "worker_memory_limit": {
                "worker1": [
                    (t0, 4_000_000_000),
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],  # Limit at all times including t1
            },
        }

        # Should handle empty worker_utils at t1 by appending zeros
        fig, ax = plot_memory_utilization_mean_timeline(tracking_data)
        assert fig is not None
        assert ax is not None


class TestPlotMemoryUtilizationPerWorkerTimeline:
    """Test memory utilization per worker timeline plotting."""

    @pytest.fixture
    def sample_tracking_data(self):
        """Sample tracking data with memory information."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        t1 = datetime.datetime(2025, 1, 1, 12, 0, 10)
        t2 = datetime.datetime(2025, 1, 1, 12, 0, 20)

        return {
            "worker_memory": {
                "worker1": [
                    (t0, 1_000_000_000),  # 1 GB / 4 GB = 25%
                    (t1, 2_000_000_000),  # 2 GB / 4 GB = 50%
                    (t2, 3_000_000_000),  # 3 GB / 4 GB = 75%
                ],
                "worker2": [
                    (t0, 800_000_000),  # 0.8 GB / 4 GB = 20%
                    (t1, 1_600_000_000),  # 1.6 GB / 4 GB = 40%
                    (t2, 2_400_000_000),  # 2.4 GB / 4 GB = 60%
                ],
            },
            "worker_memory_limit": {
                "worker1": [
                    (t0, 4_000_000_000),
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],
                "worker2": [
                    (t0, 4_000_000_000),
                    (t1, 4_000_000_000),
                    (t2, 4_000_000_000),
                ],
            },
        }

    def test_returns_figure_and_axes(self, sample_tracking_data):
        """plot_memory_utilization_per_worker_timeline returns matplotlib Figure and Axes."""
        fig, ax = plot_memory_utilization_per_worker_timeline(sample_tracking_data)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_memory_per_worker(self, sample_tracking_data):
        """Memory utilization per worker timeline plots one line per worker."""
        fig, ax = plot_memory_utilization_per_worker_timeline(sample_tracking_data)

        # Should have two lines (one per worker)
        lines = ax.get_lines()
        assert len(lines) == 2

        plt.close(fig)

    def test_has_correct_labels(self, sample_tracking_data):
        """Memory utilization per worker plot has correct axis labels and title."""
        fig, ax = plot_memory_utilization_per_worker_timeline(sample_tracking_data)

        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Memory Utilization (%)"
        assert ax.get_title() == "Memory Utilization Per Worker Over Time"

        plt.close(fig)

    def test_y_axis_limited_to_100_percent(self, sample_tracking_data):
        """Y-axis is limited to 0-100%."""
        fig, ax = plot_memory_utilization_per_worker_timeline(sample_tracking_data)

        ylim = ax.get_ylim()
        assert ylim[0] == 0
        assert ylim[1] == 100

        plt.close(fig)

    def test_custom_title(self, sample_tracking_data):
        """Can set custom title."""
        fig, ax = plot_memory_utilization_per_worker_timeline(
            sample_tracking_data, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_saves_to_file(self, sample_tracking_data, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "memory_per_worker_timeline.png"

        fig, _ax = plot_memory_utilization_per_worker_timeline(
            sample_tracking_data, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_tracking_data(self):
        """Raises ValueError if tracking_data is None."""
        with pytest.raises(ValueError, match="tracking_data cannot be None"):
            plot_memory_utilization_per_worker_timeline(None)

    def test_raises_on_missing_memory_data(self):
        """Raises ValueError if memory data missing."""
        with pytest.raises(ValueError, match=r"Memory.*not available"):
            plot_memory_utilization_per_worker_timeline(
                {"worker_memory": {}, "worker_memory_limit": {}}
            )

    def test_raises_on_missing_memory_limit(self):
        """Raises ValueError if memory limit data missing."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        with pytest.raises(ValueError, match=r"Memory.*not available"):
            plot_memory_utilization_per_worker_timeline(
                {
                    "worker_memory": {"worker1": [(t0, 1_000_000_000)]},
                    "worker_memory_limit": {},
                }
            )

    def test_handles_edge_case_zero_memory(self):
        """Handles workers with zero memory utilization."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        tracking_data = {
            "worker_memory": {
                "worker1": [(t0, 0)],
            },
            "worker_memory_limit": {
                "worker1": [(t0, 4_000_000_000)],
            },
        }

        fig, _ax = plot_memory_utilization_per_worker_timeline(tracking_data)
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

    def test_handles_edge_case_full_memory(self):
        """Handles workers with 100% memory utilization."""
        t0 = datetime.datetime(2025, 1, 1, 12, 0, 0)
        tracking_data = {
            "worker_memory": {
                "worker1": [(t0, 4_000_000_000)],
            },
            "worker_memory_limit": {
                "worker1": [(t0, 4_000_000_000)],
            },
        }

        fig, _ax = plot_memory_utilization_per_worker_timeline(tracking_data)
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

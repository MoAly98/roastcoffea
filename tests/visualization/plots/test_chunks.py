"""Tests for chunk-level performance plots."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.chunks import (
    plot_runtime_distribution,
    plot_runtime_vs_events,
)


class TestPlotRuntimeDistribution:
    """Test chunk runtime distribution histogram."""

    @pytest.fixture
    def sample_chunk_metrics(self):
        """Sample chunk metrics with duration data."""
        return [
            {"duration": 1.5, "num_events": 10000},
            {"duration": 2.0, "num_events": 12000},
            {"duration": 1.8, "num_events": 11000},
            {"duration": 2.2, "num_events": 13000},
            {"duration": 1.7, "num_events": 10500},
        ]

    def test_returns_figure_and_axes(self, sample_chunk_metrics):
        """plot_runtime_distribution returns matplotlib Figure and Axes."""
        fig, ax = plot_runtime_distribution(sample_chunk_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_has_correct_labels(self, sample_chunk_metrics):
        """Runtime distribution plot has correct axis labels and title."""
        fig, ax = plot_runtime_distribution(sample_chunk_metrics)

        assert ax.get_xlabel() == "Runtime (seconds)"
        assert ax.get_ylabel() == "Number of Chunks"
        assert ax.get_title() == "Chunk Runtime Distribution"

        plt.close(fig)

    def test_custom_title(self, sample_chunk_metrics):
        """Can set custom title."""
        fig, ax = plot_runtime_distribution(sample_chunk_metrics, title="Custom Title")

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_bins(self, sample_chunk_metrics):
        """Can set custom number of bins."""
        fig, ax = plot_runtime_distribution(sample_chunk_metrics, bins=10)

        # Check that histogram was created
        patches = ax.patches
        assert len(patches) > 0

        plt.close(fig)

    def test_custom_figsize(self, sample_chunk_metrics):
        """Can set custom figure size."""
        fig, _ax = plot_runtime_distribution(sample_chunk_metrics, figsize=(8, 4))

        assert fig.get_figwidth() == 8
        assert fig.get_figheight() == 4

        plt.close(fig)

    def test_saves_to_file(self, sample_chunk_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "runtime_dist.png"

        fig, _ax = plot_runtime_distribution(
            sample_chunk_metrics, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_chunk_metrics(self):
        """Raises ValueError if chunk_metrics is None."""
        with pytest.raises(ValueError, match="No chunk metrics available"):
            plot_runtime_distribution(None)

    def test_raises_on_empty_chunk_metrics(self):
        """Raises ValueError if chunk_metrics is empty."""
        with pytest.raises(ValueError, match="No chunk metrics available"):
            plot_runtime_distribution([])

    def test_handles_missing_duration_data(self):
        """Handles chunks with missing duration gracefully (uses default 0)."""
        chunk_metrics = [{"num_events": 1000}, {"num_events": 2000}]

        # Should not raise - missing duration defaults to 0
        fig, ax = plot_runtime_distribution(chunk_metrics)

        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_plots_mean_and_median_lines(self, sample_chunk_metrics):
        """Plots mean and median vertical lines."""
        fig, ax = plot_runtime_distribution(sample_chunk_metrics)

        # Check for vertical lines (at least 2: mean and median)
        vertical_lines = [
            line for line in ax.get_lines() if len(line.get_xdata()) == 2
        ]
        assert len(vertical_lines) >= 2

        plt.close(fig)


class TestPlotRuntimeVsEvents:
    """Test chunk runtime vs events scatter plot."""

    @pytest.fixture
    def sample_chunk_metrics(self):
        """Sample chunk metrics with duration and events data."""
        return [
            {"duration": 1.5, "num_events": 10000},
            {"duration": 3.0, "num_events": 20000},
            {"duration": 4.5, "num_events": 30000},
            {"duration": 6.0, "num_events": 40000},
            {"duration": 7.5, "num_events": 50000},
        ]

    def test_returns_figure_and_axes(self, sample_chunk_metrics):
        """plot_runtime_vs_events returns matplotlib Figure and Axes."""
        fig, ax = plot_runtime_vs_events(sample_chunk_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_has_correct_labels(self, sample_chunk_metrics):
        """Runtime vs events plot has correct axis labels and title."""
        fig, ax = plot_runtime_vs_events(sample_chunk_metrics)

        assert ax.get_xlabel() == "Number of Events"
        assert ax.get_ylabel() == "Runtime (seconds)"
        assert ax.get_title() == "Chunk Runtime vs Number of Events"

        plt.close(fig)

    def test_custom_title(self, sample_chunk_metrics):
        """Can set custom title."""
        fig, ax = plot_runtime_vs_events(sample_chunk_metrics, title="Custom Title")

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_figsize(self, sample_chunk_metrics):
        """Can set custom figure size."""
        fig, _ax = plot_runtime_vs_events(sample_chunk_metrics, figsize=(8, 4))

        assert fig.get_figwidth() == 8
        assert fig.get_figheight() == 4

        plt.close(fig)

    def test_saves_to_file(self, sample_chunk_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "runtime_vs_events.png"

        fig, _ax = plot_runtime_vs_events(sample_chunk_metrics, output_path=output_file)

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_none_chunk_metrics(self):
        """Raises ValueError if chunk_metrics is None."""
        with pytest.raises(ValueError, match="No chunk metrics available"):
            plot_runtime_vs_events(None)

    def test_raises_on_empty_chunk_metrics(self):
        """Raises ValueError if chunk_metrics is empty."""
        with pytest.raises(ValueError, match="No chunk metrics available"):
            plot_runtime_vs_events([])

    def test_raises_on_missing_data(self):
        """Raises ValueError if no runtime vs events data."""
        chunk_metrics = [{"duration": 1.5}, {"num_events": 1000}]

        with pytest.raises(
            ValueError, match="No runtime vs events data available"
        ):
            plot_runtime_vs_events(chunk_metrics)

    def test_plots_scatter_points(self, sample_chunk_metrics):
        """Plots scatter points for each chunk."""
        fig, ax = plot_runtime_vs_events(sample_chunk_metrics)

        # Check that scatter plot was created (collections exist)
        collections = ax.collections
        assert len(collections) > 0

        plt.close(fig)

    def test_plots_trend_line_with_sufficient_data(self, sample_chunk_metrics):
        """Plots trend line when there are multiple data points."""
        fig, ax = plot_runtime_vs_events(sample_chunk_metrics)

        # Should have at least one line (trend line)
        lines = ax.get_lines()
        assert len(lines) >= 1

        plt.close(fig)

    def test_no_trend_line_with_single_point(self):
        """Does not plot trend line with only one data point."""
        chunk_metrics = [{"duration": 1.5, "num_events": 10000}]

        fig, ax = plot_runtime_vs_events(chunk_metrics)

        # Should have no lines (no trend with 1 point)
        lines = ax.get_lines()
        assert len(lines) == 0

        plt.close(fig)

    def test_handles_zero_duration(self):
        """Handles chunks with zero duration."""
        chunk_metrics = [
            {"duration": 0.0, "num_events": 0},
            {"duration": 1.5, "num_events": 10000},
        ]

        fig, ax = plot_runtime_vs_events(chunk_metrics)

        # Should not raise, plot should be created
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

    def test_handles_varying_event_sizes(self):
        """Handles chunks with widely varying event counts."""
        chunk_metrics = [
            {"duration": 0.5, "num_events": 100},
            {"duration": 5.0, "num_events": 100000},
            {"duration": 2.5, "num_events": 50000},
        ]

        fig, ax = plot_runtime_vs_events(chunk_metrics)

        # Should not raise, plot should be created
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

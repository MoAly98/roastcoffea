"""Tests for I/O and data access plots."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.io import (
    plot_branch_access_per_chunk,
    plot_bytes_accessed_per_chunk,
    plot_compression_ratio_distribution,
    plot_data_access_percentage,
)


class TestPlotCompressionRatioDistribution:
    """Test compression ratio distribution histogram."""

    @pytest.fixture
    def sample_metrics(self):
        """Sample metrics with compression ratios."""
        return {
            "compression_ratios": [0.25, 0.30, 0.28, 0.32, 0.27, 0.29],
        }

    def test_returns_figure_and_axes(self, sample_metrics):
        """plot_compression_ratio_distribution returns matplotlib Figure and Axes."""
        fig, ax = plot_compression_ratio_distribution(sample_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_has_correct_labels(self, sample_metrics):
        """Compression ratio plot has correct axis labels and title."""
        fig, ax = plot_compression_ratio_distribution(sample_metrics)

        assert ax.get_xlabel() == "Compression Ratio (compressed/uncompressed)"
        assert ax.get_ylabel() == "Number of Files"
        assert ax.get_title() == "Compression Ratio Distribution Across Files"

        plt.close(fig)

    def test_custom_title(self, sample_metrics):
        """Can set custom title."""
        fig, ax = plot_compression_ratio_distribution(
            sample_metrics, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_bins(self, sample_metrics):
        """Can set custom number of bins."""
        fig, ax = plot_compression_ratio_distribution(sample_metrics, bins=10)

        # Check that histogram was created (patches exist)
        patches = ax.patches
        assert len(patches) > 0

        plt.close(fig)

    def test_custom_figsize(self, sample_metrics):
        """Can set custom figure size."""
        fig, _ax = plot_compression_ratio_distribution(sample_metrics, figsize=(8, 4))

        assert fig.get_figwidth() == 8
        assert fig.get_figheight() == 4

        plt.close(fig)

    def test_saves_to_file(self, sample_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "compression_ratio.png"

        fig, _ax = plot_compression_ratio_distribution(
            sample_metrics, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_empty_data(self):
        """Raises ValueError if no compression ratio data."""
        with pytest.raises(ValueError, match="No compression ratio data available"):
            plot_compression_ratio_distribution({"compression_ratios": []})

    def test_raises_on_missing_key(self):
        """Raises ValueError if compression_ratios key missing."""
        with pytest.raises(ValueError, match="No compression ratio data available"):
            plot_compression_ratio_distribution({})

    def test_plots_mean_and_median_lines(self, sample_metrics):
        """Plots mean and median vertical lines."""
        fig, ax = plot_compression_ratio_distribution(sample_metrics)

        # Check for vertical lines (at least 2: mean and median)
        vertical_lines = [line for line in ax.get_lines() if len(line.get_xdata()) == 2]
        assert len(vertical_lines) >= 2

        plt.close(fig)


class TestPlotDataAccessPercentage:
    """Test bytes read percentage distribution histogram."""

    @pytest.fixture
    def sample_metrics(self):
        """Sample metrics with bytes read percentages."""
        return {
            "bytes_read_percent_per_file": [15.5, 20.3, 18.7, 22.1, 17.9, 19.4],
        }

    def test_returns_figure_and_axes(self, sample_metrics):
        """plot_data_access_percentage returns matplotlib Figure and Axes."""
        fig, ax = plot_data_access_percentage(sample_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_has_correct_labels(self, sample_metrics):
        """Data access percentage plot has correct axis labels and title."""
        fig, ax = plot_data_access_percentage(sample_metrics)

        assert ax.get_xlabel() == "Bytes Read (%)"
        assert ax.get_ylabel() == "Number of Files"
        assert ax.get_title() == "Bytes Read Percentage Distribution"

        plt.close(fig)

    def test_x_axis_dynamic_range(self, sample_metrics):
        """X-axis dynamically scales to data with padding, capped at 100%."""
        fig, ax = plot_data_access_percentage(sample_metrics)

        xlim = ax.get_xlim()
        assert xlim[0] == 0
        # Dynamic range: max(data) * 1.1, capped at 100
        max_val = max(sample_metrics["bytes_read_percent_per_file"])
        expected_xlim = min(100, max_val * 1.1)
        assert xlim[1] == pytest.approx(expected_xlim)

        plt.close(fig)

    def test_custom_title(self, sample_metrics):
        """Can set custom title."""
        fig, ax = plot_data_access_percentage(sample_metrics, title="Custom Title")

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_bins(self, sample_metrics):
        """Can set custom number of bins."""
        fig, ax = plot_data_access_percentage(sample_metrics, bins=10)

        # Check that histogram was created
        patches = ax.patches
        assert len(patches) > 0

        plt.close(fig)

    def test_custom_figsize(self, sample_metrics):
        """Can set custom figure size."""
        fig, _ax = plot_data_access_percentage(sample_metrics, figsize=(8, 4))

        assert fig.get_figwidth() == 8
        assert fig.get_figheight() == 4

        plt.close(fig)

    def test_saves_to_file(self, sample_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "bytes_read_pct.png"

        fig, _ax = plot_data_access_percentage(sample_metrics, output_path=output_file)

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_empty_data(self):
        """Raises ValueError if no bytes read percentage data."""
        with pytest.raises(ValueError, match="No bytes read percentage data available"):
            plot_data_access_percentage({"bytes_read_percent_per_file": []})

    def test_raises_on_missing_key(self):
        """Raises ValueError if bytes_read_percent_per_file key missing."""
        with pytest.raises(ValueError, match="No bytes read percentage data available"):
            plot_data_access_percentage({})

    def test_plots_mean_and_median_lines(self, sample_metrics):
        """Plots mean and median vertical lines."""
        fig, ax = plot_data_access_percentage(sample_metrics)

        # Check for vertical lines (at least 2: mean and median)
        vertical_lines = [line for line in ax.get_lines() if len(line.get_xdata()) == 2]
        assert len(vertical_lines) >= 2

        plt.close(fig)

    def test_handles_edge_case_high_percentage(self):
        """Handles high bytes read percentages correctly."""
        metrics = {"bytes_read_percent_per_file": [95.0, 98.5, 97.2, 99.1]}

        fig, _ax = plot_data_access_percentage(metrics)

        # Should not raise, plot should be created
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

    def test_handles_edge_case_low_percentage(self):
        """Handles low bytes read percentages correctly."""
        metrics = {"bytes_read_percent_per_file": [1.5, 2.3, 0.8, 3.1]}

        fig, _ax = plot_data_access_percentage(metrics)

        # Should not raise, plot should be created
        assert isinstance(fig, plt.Figure)

        plt.close(fig)


class TestPlotBranchAccessPerChunk:
    """Test branches accessed per chunk bar chart."""

    @pytest.fixture
    def sample_chunk_metrics(self):
        """Sample chunk metrics with branch access data."""
        return [
            {"num_branches_accessed": 10},
            {"num_branches_accessed": 15},
            {"num_branches_accessed": 12},
            {"num_branches_accessed": 8},
        ]

    def test_returns_figure_and_axes(self, sample_chunk_metrics):
        """plot_branch_access_per_chunk returns matplotlib Figure and Axes."""
        fig, ax = plot_branch_access_per_chunk(sample_chunk_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_has_correct_labels(self, sample_chunk_metrics):
        """Branch access plot has correct axis labels and title."""
        fig, ax = plot_branch_access_per_chunk(sample_chunk_metrics)

        assert ax.get_xlabel() == "Chunk Index"
        assert ax.get_ylabel() == "Branches Accessed"
        assert ax.get_title() == "Branches Accessed Per Chunk"

        plt.close(fig)

    def test_custom_title(self, sample_chunk_metrics):
        """Can set custom title."""
        fig, ax = plot_branch_access_per_chunk(
            sample_chunk_metrics, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_figsize(self, sample_chunk_metrics):
        """Can set custom figure size."""
        fig, _ax = plot_branch_access_per_chunk(sample_chunk_metrics, figsize=(8, 4))

        assert fig.get_figwidth() == 8
        assert fig.get_figheight() == 4

        plt.close(fig)

    def test_saves_to_file(self, sample_chunk_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "branch_access.png"

        fig, _ax = plot_branch_access_per_chunk(
            sample_chunk_metrics, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_empty_data(self):
        """Raises ValueError if no chunk metrics data."""
        with pytest.raises(ValueError, match="No chunk metrics data available"):
            plot_branch_access_per_chunk([])

    def test_raises_on_zero_branch_counts(self):
        """Raises ValueError if all branch counts are zero."""
        with pytest.raises(ValueError, match="No branch access data in chunk metrics"):
            plot_branch_access_per_chunk([{"num_branches_accessed": 0}])

    def test_plots_mean_line(self, sample_chunk_metrics):
        """Plots mean horizontal line."""
        fig, ax = plot_branch_access_per_chunk(sample_chunk_metrics)

        # Check for horizontal line (mean)
        horizontal_lines = [
            line for line in ax.get_lines() if len(line.get_ydata()) == 2
        ]
        assert len(horizontal_lines) >= 1

        plt.close(fig)


class TestPlotBytesAccessedPerChunk:
    """Test bytes accessed per chunk grouped bar chart."""

    @pytest.fixture
    def sample_chunk_metrics(self):
        """Sample chunk metrics with compressed and uncompressed byte data."""
        return [
            {"accessed_bytes": 1_000_000, "accessed_uncompressed_bytes": 4_000_000},
            {"accessed_bytes": 1_500_000, "accessed_uncompressed_bytes": 5_500_000},
            {"accessed_bytes": 1_200_000, "accessed_uncompressed_bytes": 4_800_000},
        ]

    def test_returns_figure_and_axes(self, sample_chunk_metrics):
        """plot_bytes_accessed_per_chunk returns matplotlib Figure and Axes."""
        fig, ax = plot_bytes_accessed_per_chunk(sample_chunk_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_has_correct_labels(self, sample_chunk_metrics):
        """Bytes accessed plot has correct axis labels and title."""
        fig, ax = plot_bytes_accessed_per_chunk(sample_chunk_metrics)

        assert ax.get_xlabel() == "Chunk Index"
        assert ax.get_ylabel() == "Bytes (MB)"
        assert ax.get_title() == "Bytes Accessed Per Chunk (Compressed vs Uncompressed)"

        plt.close(fig)

    def test_custom_title(self, sample_chunk_metrics):
        """Can set custom title."""
        fig, ax = plot_bytes_accessed_per_chunk(
            sample_chunk_metrics, title="Custom Title"
        )

        assert ax.get_title() == "Custom Title"

        plt.close(fig)

    def test_custom_figsize(self, sample_chunk_metrics):
        """Can set custom figure size."""
        fig, _ax = plot_bytes_accessed_per_chunk(sample_chunk_metrics, figsize=(8, 4))

        assert fig.get_figwidth() == 8
        assert fig.get_figheight() == 4

        plt.close(fig)

    def test_saves_to_file(self, sample_chunk_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "bytes_accessed.png"

        fig, _ax = plot_bytes_accessed_per_chunk(
            sample_chunk_metrics, output_path=output_file
        )

        assert output_file.exists()

        plt.close(fig)

    def test_raises_on_empty_data(self):
        """Raises ValueError if no chunk metrics data."""
        with pytest.raises(ValueError, match="No chunk metrics data available"):
            plot_bytes_accessed_per_chunk([])

    def test_raises_on_zero_bytes(self):
        """Raises ValueError if all byte counts are zero."""
        with pytest.raises(ValueError, match="No bytes access data in chunk metrics"):
            plot_bytes_accessed_per_chunk(
                [{"accessed_bytes": 0, "accessed_uncompressed_bytes": 0}]
            )

    def test_has_legend(self, sample_chunk_metrics):
        """Has legend with compressed and uncompressed labels."""
        fig, ax = plot_bytes_accessed_per_chunk(sample_chunk_metrics)

        legend = ax.get_legend()
        assert legend is not None
        legend_texts = [t.get_text() for t in legend.get_texts()]
        assert "Compressed (on-disk)" in legend_texts
        assert "Uncompressed (in-memory)" in legend_texts

        plt.close(fig)

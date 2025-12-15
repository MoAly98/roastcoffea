"""Tests for scaling and efficiency plots."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.scaling import (
    plot_efficiency_summary,
    plot_resource_utilization,
)


def _make_metrics(
    efficiency: dict | None = None,
    resources: dict | None = None,
    timing: dict | None = None,
) -> dict:
    """Helper to create metrics dict with nested structure."""
    return {
        "summary": {
            "efficiency": efficiency or {},
            "resources": resources or {},
            "timing": timing or {},
        }
    }


class TestPlotEfficiencySummary:
    """Test efficiency summary plotting."""

    @pytest.fixture
    def sample_metrics(self):
        """Sample metrics for plotting."""
        return _make_metrics(
            efficiency={
                "core_efficiency": 0.75,
                "speedup": 3.5,
            },
            timing={
                "wall_seconds": 100.0,
                "cpu_seconds": 350.0,
            },
        )

    def test_returns_figure_and_axes(self, sample_metrics):
        """plot_efficiency_summary returns matplotlib Figure and Axes."""
        fig, ax = plot_efficiency_summary(sample_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_efficiency_metrics(self, sample_metrics):
        """Efficiency summary plots bars for each metric."""
        fig, ax = plot_efficiency_summary(sample_metrics)

        # Should have two bars (core_efficiency and speedup)
        bars = ax.patches
        assert len(bars) == 2

        plt.close(fig)

    def test_has_correct_labels(self, sample_metrics):
        """Efficiency plot has correct axis labels and title."""
        fig, ax = plot_efficiency_summary(sample_metrics)

        assert ax.get_ylabel() == "Value"
        assert ax.get_title() == "Efficiency Metrics Summary"

        plt.close(fig)

    def test_saves_to_file(self, sample_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "efficiency_summary.png"

        fig, _ax = plot_efficiency_summary(sample_metrics, output_path=output_file)

        assert output_file.exists()

        plt.close(fig)

    def test_handles_missing_speedup(self):
        """Works with only core_efficiency."""
        metrics = _make_metrics(efficiency={"core_efficiency": 0.75})

        fig, ax = plot_efficiency_summary(metrics)

        # Should have one bar
        bars = ax.patches
        assert len(bars) == 1

        plt.close(fig)

    def test_handles_missing_core_efficiency(self):
        """Works with only speedup."""
        metrics = _make_metrics(efficiency={"speedup": 3.5})

        fig, ax = plot_efficiency_summary(metrics)

        # Should have one bar
        bars = ax.patches
        assert len(bars) == 1

        plt.close(fig)

    def test_raises_on_none_metrics(self):
        """Raises ValueError if metrics is None."""
        with pytest.raises(ValueError, match="metrics cannot be None"):
            plot_efficiency_summary(None)

    def test_raises_on_missing_all_metrics(self):
        """Raises ValueError if no efficiency metrics available."""
        metrics = _make_metrics(timing={"wall_seconds": 100})
        with pytest.raises(
            ValueError,
            match="No efficiency metrics available",
        ):
            plot_efficiency_summary(metrics)


class TestPlotResourceUtilization:
    """Test resource utilization plotting."""

    @pytest.fixture
    def sample_metrics(self):
        """Sample metrics for plotting."""
        return _make_metrics(
            resources={
                "workers_avg": 4.0,
                "cores_total": 16.0,
                "memory_peak_bytes": 8_000_000_000,  # 8 GB
            }
        )

    def test_returns_figure_and_axes(self, sample_metrics):
        """plot_resource_utilization returns matplotlib Figure and Axes."""
        fig, ax = plot_resource_utilization(sample_metrics)

        assert isinstance(fig, plt.Figure)
        assert isinstance(ax, plt.Axes)

        plt.close(fig)

    def test_plots_resource_metrics(self, sample_metrics):
        """Resource utilization plots bars for each metric."""
        fig, ax = plot_resource_utilization(sample_metrics)

        # Should have three bars (workers, cores, memory)
        bars = ax.patches
        assert len(bars) == 3

        plt.close(fig)

    def test_has_correct_labels(self, sample_metrics):
        """Resource plot has correct axis labels and title."""
        fig, ax = plot_resource_utilization(sample_metrics)

        assert ax.get_ylabel() == "Value"
        assert ax.get_title() == "Resource Utilization Summary"

        plt.close(fig)

    def test_saves_to_file(self, sample_metrics, tmp_path):
        """Can save plot to file."""
        output_file = tmp_path / "resource_utilization.png"

        fig, _ax = plot_resource_utilization(sample_metrics, output_path=output_file)

        assert output_file.exists()

        plt.close(fig)

    def test_handles_partial_metrics(self):
        """Works with subset of metrics."""
        metrics = _make_metrics(
            resources={
                "workers_avg": 4.0,
                # Missing cores_total and memory_peak_bytes
            }
        )

        fig, ax = plot_resource_utilization(metrics)

        # Should have one bar
        bars = ax.patches
        assert len(bars) == 1

        plt.close(fig)

    def test_raises_on_none_metrics(self):
        """Raises ValueError if metrics is None."""
        with pytest.raises(ValueError, match="metrics cannot be None"):
            plot_resource_utilization(None)

    def test_raises_on_missing_all_metrics(self):
        """Raises ValueError if no resource metrics available."""
        metrics = _make_metrics(timing={"wall_seconds": 100})
        with pytest.raises(
            ValueError,
            match="No resource metrics available",
        ):
            plot_resource_utilization(metrics)

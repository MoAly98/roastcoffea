"""Tests for per-task visualization functions."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest

from roastcoffea.visualization.plots.per_task import (
    extract_per_task_metrics,
    plot_per_task_bytes_read,
    plot_per_task_cpu_io,
    plot_per_task_overhead,
)


class TestExtractPerTaskMetrics:
    """Test extract_per_task_metrics function."""

    def test_extract_basic(self):
        """Extract metrics from basic span data."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.5,
            ("execute", "task-123", "thread-noncpu"): 2.3,
            ("execute", "task-456", "thread-cpu"): 5.2,
            ("execute", "task-456", "disk-read"): 1024,
        }

        result = extract_per_task_metrics(span_metrics)

        assert "task-123" in result
        assert "task-456" in result
        assert result["task-123"]["thread-cpu"] == 10.5
        assert result["task-123"]["thread-noncpu"] == 2.3
        assert result["task-456"]["thread-cpu"] == 5.2
        assert result["task-456"]["disk-read"] == 1024

    def test_extract_filters_non_execute_context(self):
        """Ignore metrics not from 'execute' context."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.5,
            ("serialize", "task-456", "thread-cpu"): 2.0,  # Different context
            ("deserialize", "task-789", "thread-cpu"): 1.5,  # Different context
        }

        result = extract_per_task_metrics(span_metrics)

        # Only execute context should be included
        assert "task-123" in result
        assert "task-456" not in result
        assert "task-789" not in result

    def test_extract_handles_malformed_keys(self):
        """Handle keys that are not tuples or have wrong length."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.5,
            "invalid_key": 5.0,  # String, not tuple
            ("execute", "task-456"): 2.0,  # Tuple too short
            ("execute", "task-789", "disk-read"): 1024,  # Valid
        }

        result = extract_per_task_metrics(span_metrics)

        # Should only extract valid keys
        assert "task-123" in result
        assert "task-789" in result
        assert result["task-123"]["thread-cpu"] == 10.5
        assert result["task-789"]["disk-read"] == 1024

    def test_extract_empty_input(self):
        """Handle empty span_metrics."""
        span_metrics = {}

        result = extract_per_task_metrics(span_metrics)

        assert result == {}

    def test_extract_groups_activities_by_task(self):
        """Group multiple activities under same task."""
        span_metrics = {
            ("execute", "task-abc", "thread-cpu"): 10.0,
            ("execute", "task-abc", "thread-noncpu"): 2.0,
            ("execute", "task-abc", "disk-read"): 500,
            ("execute", "task-abc", "disk-write"): 300,
        }

        result = extract_per_task_metrics(span_metrics)

        assert "task-abc" in result
        assert len(result["task-abc"]) == 4
        assert result["task-abc"]["thread-cpu"] == 10.0
        assert result["task-abc"]["thread-noncpu"] == 2.0
        assert result["task-abc"]["disk-read"] == 500
        assert result["task-abc"]["disk-write"] == 300


class TestPlotPerTaskCpuIo:
    """Test plot_per_task_cpu_io function."""

    def test_plot_basic(self, tmp_path):
        """Create basic CPU vs I/O plot."""
        span_metrics = {
            ("execute", "task-abc123456", "thread-cpu"): 10.5,
            ("execute", "task-abc123456", "thread-noncpu"): 2.3,
            ("execute", "task-def789012", "thread-cpu"): 5.2,
            ("execute", "task-def789012", "thread-noncpu"): 1.1,
        }

        fig, ax = plot_per_task_cpu_io(span_metrics)

        # Verify plot was created
        assert fig is not None
        assert ax is not None

        # Check title and labels
        assert ax.get_title() == "CPU vs I/O Time per Task"
        assert ax.get_xlabel() == "Task"
        assert ax.get_ylabel() == "Time (seconds)"

        # Check legend
        legend_labels = [text.get_text() for text in ax.get_legend().get_texts()]
        assert "CPU Time" in legend_labels
        assert "I/O Time" in legend_labels

        plt.close(fig)

    def test_plot_saves_to_file(self, tmp_path):
        """Save plot to file."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.5,
            ("execute", "task-123", "thread-noncpu"): 2.3,
        }

        output_path = tmp_path / "cpu_io_plot.png"
        fig, ax = plot_per_task_cpu_io(span_metrics, output_path=str(output_path))

        # Verify file was created
        assert output_path.exists()

        plt.close(fig)

    def test_plot_with_custom_params(self):
        """Create plot with custom title and figsize."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.5,
            ("execute", "task-123", "thread-noncpu"): 2.3,
        }

        fig, ax = plot_per_task_cpu_io(
            span_metrics, title="Custom Title", figsize=(10, 5)
        )

        assert ax.get_title() == "Custom Title"
        assert fig.get_size_inches()[0] == 10
        assert fig.get_size_inches()[1] == 5

        plt.close(fig)

    def test_plot_skips_na_tasks(self):
        """Skip tasks with N/A prefix."""
        span_metrics = {
            ("execute", "N/A", "thread-cpu"): 100.0,  # Should be skipped
            ("execute", "task-123", "thread-cpu"): 10.5,
            ("execute", "task-123", "thread-noncpu"): 2.3,
        }

        fig, ax = plot_per_task_cpu_io(span_metrics)

        # Should only have 1 task (N/A filtered out)
        assert len(ax.get_xticklabels()) == 1

        plt.close(fig)

    def test_plot_raises_on_no_metrics(self):
        """Raise error when no valid metrics found."""
        span_metrics = {
            ("execute", "N/A", "thread-cpu"): 100.0,  # Only N/A task
        }

        with pytest.raises(ValueError, match="No per-task CPU/IO metrics found"):
            plot_per_task_cpu_io(span_metrics)

    def test_plot_handles_zero_values(self):
        """Handle tasks with zero CPU or I/O time."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 0.0,
            ("execute", "task-123", "thread-noncpu"): 2.3,
            ("execute", "task-456", "thread-cpu"): 10.5,
            ("execute", "task-456", "thread-noncpu"): 0.0,
        }

        fig, ax = plot_per_task_cpu_io(span_metrics)

        # Should include both tasks
        assert len(ax.get_xticklabels()) == 2

        plt.close(fig)

    def test_plot_truncates_long_task_names(self):
        """Truncate task names to last 8 characters."""
        span_metrics = {
            ("execute", "very-long-task-name-1234567890", "thread-cpu"): 10.5,
            ("execute", "very-long-task-name-1234567890", "thread-noncpu"): 2.3,
        }

        fig, ax = plot_per_task_cpu_io(span_metrics)

        # Check that task label is truncated
        labels = [label.get_text() for label in ax.get_xticklabels()]
        assert len(labels[0]) == 8
        assert labels[0] == "34567890"  # Last 8 chars

        plt.close(fig)


class TestPlotPerTaskBytesRead:
    """Test plot_per_task_bytes_read function."""

    def test_plot_basic(self):
        """Create basic bytes read plot."""
        span_metrics = {
            ("execute", "task-abc12345", "disk-read"): 1_000_000_000,  # 1 GB
            ("execute", "task-def67890", "disk-read"): 2_000_000_000,  # 2 GB
        }

        fig, ax = plot_per_task_bytes_read(span_metrics)

        # Verify plot was created
        assert fig is not None
        assert ax is not None

        # Check title and labels
        assert ax.get_title() == "Bytes Read per Task"
        assert ax.get_xlabel() == "Task"
        assert ax.get_ylabel() == "Bytes Read (GB)"

        plt.close(fig)

    def test_plot_converts_to_gb(self):
        """Convert bytes to gigabytes."""
        span_metrics = {
            ("execute", "task-123", "disk-read"): 5_000_000_000,  # 5 GB
        }

        fig, ax = plot_per_task_bytes_read(span_metrics)

        # Get bar heights
        bars = [patch for patch in ax.patches]
        assert len(bars) == 1
        assert bars[0].get_height() == pytest.approx(5.0)  # 5 GB

        plt.close(fig)

    def test_plot_saves_to_file(self, tmp_path):
        """Save plot to file."""
        span_metrics = {
            ("execute", "task-123", "disk-read"): 1_000_000_000,
        }

        output_path = tmp_path / "bytes_read.png"
        fig, ax = plot_per_task_bytes_read(span_metrics, output_path=str(output_path))

        assert output_path.exists()

        plt.close(fig)

    def test_plot_raises_on_no_metrics(self):
        """Raise error when no disk-read metrics found."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.0,  # No disk-read
        }

        with pytest.raises(ValueError, match="No per-task disk-read metrics found"):
            plot_per_task_bytes_read(span_metrics)

    def test_plot_skips_zero_disk_read(self):
        """Skip tasks with zero disk-read."""
        span_metrics = {
            ("execute", "task-123", "disk-read"): 0,  # Zero, should be skipped
            ("execute", "task-456", "disk-read"): 1_000_000_000,
        }

        fig, ax = plot_per_task_bytes_read(span_metrics)

        # Should only have 1 task
        assert len(ax.get_xticklabels()) == 1

        plt.close(fig)


class TestPlotPerTaskOverhead:
    """Test plot_per_task_overhead function."""

    def test_plot_basic(self):
        """Create basic overhead plot."""
        span_metrics = {
            ("execute", "task-abc12345", "compress"): 1.5,
            ("execute", "task-abc12345", "decompress"): 2.0,
            ("execute", "task-abc12345", "serialize"): 0.5,
            ("execute", "task-abc12345", "deserialize"): 0.8,
        }

        fig, ax = plot_per_task_overhead(span_metrics)

        # Verify plot was created
        assert fig is not None
        assert ax is not None

        # Check title and labels
        assert ax.get_title() == "Compression & Serialization Overhead per Task"
        assert ax.get_xlabel() == "Task"
        assert ax.get_ylabel() == "Time (seconds)"

        # Check legend for all 4 activities
        legend_labels = [text.get_text() for text in ax.get_legend().get_texts()]
        assert "Compress" in legend_labels
        assert "Decompress" in legend_labels
        assert "Serialize" in legend_labels
        assert "Deserialize" in legend_labels

        plt.close(fig)

    def test_plot_stacks_bars(self):
        """Verify bars are stacked correctly."""
        span_metrics = {
            ("execute", "task-123", "compress"): 1.0,
            ("execute", "task-123", "decompress"): 2.0,
        }

        fig, ax = plot_per_task_overhead(span_metrics)

        # Should have stacked bars
        bars = [patch for patch in ax.patches]
        assert len(bars) >= 2  # At least 2 bars (decompress + compress)

        plt.close(fig)

    def test_plot_saves_to_file(self, tmp_path):
        """Save plot to file."""
        span_metrics = {
            ("execute", "task-123", "compress"): 1.0,
        }

        output_path = tmp_path / "overhead.png"
        fig, ax = plot_per_task_overhead(span_metrics, output_path=str(output_path))

        assert output_path.exists()

        plt.close(fig)

    def test_plot_raises_on_no_metrics(self):
        """Raise error when no overhead metrics found."""
        span_metrics = {
            ("execute", "task-123", "thread-cpu"): 10.0,  # No overhead metrics
        }

        with pytest.raises(ValueError, match="No per-task overhead metrics found"):
            plot_per_task_overhead(span_metrics)

    def test_plot_handles_partial_overhead(self):
        """Handle tasks with only some overhead metrics."""
        span_metrics = {
            ("execute", "task-123", "compress"): 1.0,  # Only compress
            # No decompress, serialize, or deserialize
        }

        fig, ax = plot_per_task_overhead(span_metrics)

        # Should still create plot
        assert fig is not None
        assert ax is not None

        plt.close(fig)

    def test_plot_skips_na_tasks(self):
        """Skip tasks with N/A prefix."""
        span_metrics = {
            ("execute", "N/A", "compress"): 100.0,  # Should be skipped
            ("execute", "task-123", "compress"): 1.0,
        }

        fig, ax = plot_per_task_overhead(span_metrics)

        # Should only have 1 task
        assert len(ax.get_xticklabels()) == 1

        plt.close(fig)

    def test_plot_includes_task_with_any_overhead(self):
        """Include task if it has any overhead activity."""
        span_metrics = {
            ("execute", "task-123", "compress"): 0.0,
            ("execute", "task-123", "decompress"): 0.0,
            ("execute", "task-123", "serialize"): 1.0,  # Has serialize
            ("execute", "task-123", "deserialize"): 0.0,
        }

        fig, ax = plot_per_task_overhead(span_metrics)

        # Should include task-123
        assert len(ax.get_xticklabels()) == 1

        plt.close(fig)


class TestPlotPerTaskBytesReadEdgeCases:
    """Test edge cases for plot_per_task_bytes_read function."""

    def test_skips_na_task_prefix(self):
        """Skips tasks with N/A prefix (line 148)."""
        span_metrics = {
            ("execute", "task-123", "disk-read"): 1_000_000,
            ("execute", "N/A", "disk-read"): 500_000,  # Should be skipped
            ("execute", "task-456", "disk-read"): 2_000_000,
        }

        fig, ax = plot_per_task_bytes_read(span_metrics)

        # Should only have 2 tasks (123 and 456, not N/A)
        assert len(ax.patches) == 2

        plt.close(fig)

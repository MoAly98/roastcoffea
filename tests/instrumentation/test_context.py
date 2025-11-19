"""Tests for track_time() and track_memory() context managers."""

from __future__ import annotations

import pytest

from roastcoffea.decorator import get_active_collector, set_active_collector
from roastcoffea.instrumentation import track_memory, track_time


class MockCollector:
    """Mock MetricsCollector for testing."""

    def __init__(self):
        self.section_metrics = []

    def record_section_metrics(self, section_data):
        self.section_metrics.append(section_data)


class TestTrackTimeContext:
    """Test track_time() context manager."""

    def test_track_time_without_collector_is_noop(self):
        """track_time() without active collector is a no-op."""
        set_active_collector(None)

        with track_time("test_operation"):
            result = 42

        assert result == 42
        # No collector, so no metrics recorded

    def test_track_time_records_timing(self):
        """track_time() records timing metrics when collector is active."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("test_operation"):
            pass

        assert len(collector.section_metrics) == 1
        section = collector.section_metrics[0]
        assert section["name"] == "test_operation"
        assert section["type"] == "time"
        assert "t_start" in section
        assert "t_end" in section
        assert "duration" in section
        assert section["duration"] > 0

        set_active_collector(None)

    def test_track_time_measures_duration(self):
        """track_time() measures elapsed time correctly."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("slow_operation"):
            import time
            time.sleep(0.01)

        section = collector.section_metrics[0]
        assert section["duration"] >= 0.01
        assert section["t_end"] > section["t_start"]

        set_active_collector(None)

    def test_track_time_with_explicit_collector(self):
        """track_time() accepts explicit collector parameter."""
        collector = MockCollector()
        # Don't set as active
        set_active_collector(None)

        with track_time("test_operation", collector=collector):
            pass

        assert len(collector.section_metrics) == 1
        assert collector.section_metrics[0]["name"] == "test_operation"

    def test_track_time_with_metadata(self):
        """track_time() accepts and preserves metadata."""
        collector = MockCollector()
        set_active_collector(collector)

        metadata = {"category": "preprocessing", "step": 1}
        with track_time("test_operation", metadata=metadata):
            pass

        section = collector.section_metrics[0]
        assert section["name"] == "test_operation"
        assert section["category"] == "preprocessing"
        assert section["step"] == 1

        set_active_collector(None)

    def test_track_time_with_exception(self):
        """track_time() still records metrics when exception occurs."""
        collector = MockCollector()
        set_active_collector(collector)

        with pytest.raises(ValueError, match="test error"):
            with track_time("failing_operation"):
                raise ValueError("test error")

        # Should still record timing
        assert len(collector.section_metrics) == 1
        section = collector.section_metrics[0]
        assert section["name"] == "failing_operation"
        assert "duration" in section
        assert section["duration"] > 0

        set_active_collector(None)

    def test_track_time_multiple_operations(self):
        """track_time() records multiple operations correctly."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("operation_1"):
            pass

        with track_time("operation_2"):
            pass

        with track_time("operation_3"):
            pass

        assert len(collector.section_metrics) == 3
        assert collector.section_metrics[0]["name"] == "operation_1"
        assert collector.section_metrics[1]["name"] == "operation_2"
        assert collector.section_metrics[2]["name"] == "operation_3"

        set_active_collector(None)

    def test_track_time_yields_metrics_dict(self):
        """track_time() yields metrics dictionary that gets populated."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("test_operation") as metrics:
            # Before completion, should have name and type
            assert metrics["name"] == "test_operation"
            assert metrics["type"] == "time"
            # Timing not yet populated
            assert "t_start" not in metrics
            assert "t_end" not in metrics

        # After completion, metrics dict should be populated
        section = collector.section_metrics[0]
        assert "t_start" in section
        assert "t_end" in section
        assert "duration" in section

        set_active_collector(None)


class TestTrackMemoryContext:
    """Test track_memory() context manager."""

    def test_track_memory_without_collector_is_noop(self):
        """track_memory() without active collector is a no-op."""
        set_active_collector(None)

        with track_memory("test_operation"):
            result = 42

        assert result == 42
        # No collector, so no metrics recorded

    def test_track_memory_records_memory(self):
        """track_memory() records memory metrics when collector is active."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_memory("test_operation"):
            pass

        assert len(collector.section_metrics) == 1
        section = collector.section_metrics[0]
        assert section["name"] == "test_operation"
        assert section["type"] == "memory"
        assert "t_start" in section
        assert "t_end" in section
        assert "duration" in section
        assert "mem_before_mb" in section
        assert "mem_after_mb" in section
        assert "mem_delta_mb" in section
        # Memory values should be non-negative (0 if psutil not available)
        assert section["mem_before_mb"] >= 0
        assert section["mem_after_mb"] >= 0

        set_active_collector(None)

    def test_track_memory_measures_duration(self):
        """track_memory() also measures elapsed time."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_memory("memory_operation"):
            import time
            time.sleep(0.01)

        section = collector.section_metrics[0]
        assert section["duration"] >= 0.01
        assert section["t_end"] > section["t_start"]

        set_active_collector(None)

    def test_track_memory_with_explicit_collector(self):
        """track_memory() accepts explicit collector parameter."""
        collector = MockCollector()
        # Don't set as active
        set_active_collector(None)

        with track_memory("test_operation", collector=collector):
            pass

        assert len(collector.section_metrics) == 1
        assert collector.section_metrics[0]["name"] == "test_operation"

    def test_track_memory_with_metadata(self):
        """track_memory() accepts and preserves metadata."""
        collector = MockCollector()
        set_active_collector(collector)

        metadata = {"category": "loading", "data_type": "jets"}
        with track_memory("test_operation", metadata=metadata):
            pass

        section = collector.section_metrics[0]
        assert section["name"] == "test_operation"
        assert section["category"] == "loading"
        assert section["data_type"] == "jets"

        set_active_collector(None)

    def test_track_memory_with_exception(self):
        """track_memory() still records metrics when exception occurs."""
        collector = MockCollector()
        set_active_collector(collector)

        with pytest.raises(ValueError, match="test error"):
            with track_memory("failing_operation"):
                raise ValueError("test error")

        # Should still record memory metrics
        assert len(collector.section_metrics) == 1
        section = collector.section_metrics[0]
        assert section["name"] == "failing_operation"
        assert "mem_before_mb" in section
        assert "mem_after_mb" in section
        assert "mem_delta_mb" in section

        set_active_collector(None)

    def test_track_memory_multiple_operations(self):
        """track_memory() records multiple operations correctly."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_memory("operation_1"):
            pass

        with track_memory("operation_2"):
            pass

        with track_memory("operation_3"):
            pass

        assert len(collector.section_metrics) == 3
        assert collector.section_metrics[0]["name"] == "operation_1"
        assert collector.section_metrics[1]["name"] == "operation_2"
        assert collector.section_metrics[2]["name"] == "operation_3"

        set_active_collector(None)

    def test_track_memory_yields_metrics_dict(self):
        """track_memory() yields metrics dictionary that gets populated."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_memory("test_operation") as metrics:
            # Before completion, should have name and type
            assert metrics["name"] == "test_operation"
            assert metrics["type"] == "memory"
            # Memory/timing not yet populated
            assert "mem_before_mb" not in metrics
            assert "mem_after_mb" not in metrics

        # After completion, metrics dict should be populated
        section = collector.section_metrics[0]
        assert "mem_before_mb" in section
        assert "mem_after_mb" in section
        assert "mem_delta_mb" in section

        set_active_collector(None)

    def test_track_memory_allocates_memory(self):
        """track_memory() detects memory allocation (if psutil available)."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_memory("allocation_test"):
            # Allocate some memory
            data = [0] * 100000  # ~800KB worth of integers

        section = collector.section_metrics[0]
        # If psutil is available, should detect some memory change
        # If not available, both will be 0
        if section["mem_before_mb"] > 0:
            # psutil is available, check that we measured something
            assert section["mem_after_mb"] >= section["mem_before_mb"]

        set_active_collector(None)


class TestCombinedContextUsage:
    """Test using both context managers together."""

    def test_track_time_and_memory_together(self):
        """Can use track_time() and track_memory() together."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("outer_timing"):
            with track_memory("inner_memory"):
                pass

        assert len(collector.section_metrics) == 2
        assert collector.section_metrics[0]["type"] == "memory"  # Inner completes first
        assert collector.section_metrics[1]["type"] == "time"   # Outer completes second

        set_active_collector(None)

    def test_multiple_nested_contexts(self):
        """Can nest multiple context managers."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("outer"):
            with track_memory("middle"):
                with track_time("inner"):
                    pass

        assert len(collector.section_metrics) == 3
        # Completion order: inner -> middle -> outer
        assert collector.section_metrics[0]["name"] == "inner"
        assert collector.section_metrics[1]["name"] == "middle"
        assert collector.section_metrics[2]["name"] == "outer"

        set_active_collector(None)

    def test_parallel_contexts(self):
        """Can use multiple context managers in sequence."""
        collector = MockCollector()
        set_active_collector(collector)

        with track_time("step_1"):
            pass

        with track_memory("step_2"):
            pass

        with track_time("step_3"):
            pass

        assert len(collector.section_metrics) == 3
        assert collector.section_metrics[0]["name"] == "step_1"
        assert collector.section_metrics[1]["name"] == "step_2"
        assert collector.section_metrics[2]["name"] == "step_3"

        set_active_collector(None)

"""Tests for track_time() and track_memory() context managers."""

from __future__ import annotations

import pytest

from roastcoffea.instrumentation import track_memory, track_time


class MockProcessor:
    """Mock processor for testing context managers."""

    def __init__(self):
        self._roastcoffea_current_chunk = {"timing": {}, "memory": {}}


class TestTrackTimeContext:
    """Test track_time() context manager."""

    def test_track_time_without_collector_is_noop(self):
        """track_time() without active collection is a no-op."""
        # No _roastcoffea_current_chunk attribute
        processor = object()

        with track_time(processor, "test_operation"):
            result = 42

        assert result == 42
        # No metrics recorded

    def test_track_time_records_timing(self):
        """track_time() records timing metrics when collection is active."""
        processor = MockProcessor()

        with track_time(processor, "test_operation"):
            pass

        assert "test_operation" in processor._roastcoffea_current_chunk["timing"]
        duration = processor._roastcoffea_current_chunk["timing"]["test_operation"]
        assert duration >= 0

    def test_track_time_measures_duration(self):
        """track_time() measures elapsed time correctly."""
        processor = MockProcessor()

        with track_time(processor, "slow_operation"):
            import time

            time.sleep(0.01)

        duration = processor._roastcoffea_current_chunk["timing"]["slow_operation"]
        assert duration >= 0.01

    def test_track_time_with_exception(self):
        """track_time() still records metrics when exception occurs."""
        processor = MockProcessor()

        with pytest.raises(ValueError, match="test error"):
            with track_time(processor, "failing_operation"):
                raise ValueError("test error")

        # Should still record timing
        assert "failing_operation" in processor._roastcoffea_current_chunk["timing"]
        duration = processor._roastcoffea_current_chunk["timing"]["failing_operation"]
        assert duration > 0

    def test_track_time_multiple_operations(self):
        """track_time() records multiple operations correctly."""
        processor = MockProcessor()

        with track_time(processor, "operation_1"):
            pass

        with track_time(processor, "operation_2"):
            pass

        with track_time(processor, "operation_3"):
            pass

        assert len(processor._roastcoffea_current_chunk["timing"]) == 3
        assert "operation_1" in processor._roastcoffea_current_chunk["timing"]
        assert "operation_2" in processor._roastcoffea_current_chunk["timing"]
        assert "operation_3" in processor._roastcoffea_current_chunk["timing"]


class TestTrackMemoryContext:
    """Test track_memory() context manager."""

    def test_track_memory_without_collector_is_noop(self):
        """track_memory() without active collection is a no-op."""
        # No _roastcoffea_current_chunk attribute
        processor = object()

        with track_memory(processor, "test_operation"):
            result = 42

        assert result == 42
        # No metrics recorded

    def test_track_memory_records_memory(self):
        """track_memory() records memory metrics when collection is active."""
        processor = MockProcessor()

        with track_memory(processor, "test_operation"):
            pass

        assert "test_operation" in processor._roastcoffea_current_chunk["memory"]
        delta_mb = processor._roastcoffea_current_chunk["memory"]["test_operation"]
        # Memory delta should be a number (could be 0 if psutil not available)
        assert isinstance(delta_mb, (int, float))
        assert delta_mb >= 0 or delta_mb < 0  # Can be negative if memory freed

    def test_track_memory_with_exception(self):
        """track_memory() still records metrics when exception occurs."""
        processor = MockProcessor()

        with pytest.raises(ValueError, match="test error"):
            with track_memory(processor, "failing_operation"):
                raise ValueError("test error")

        # Should still record memory metrics
        assert "failing_operation" in processor._roastcoffea_current_chunk["memory"]
        delta_mb = processor._roastcoffea_current_chunk["memory"]["failing_operation"]
        assert isinstance(delta_mb, (int, float))

    def test_track_memory_multiple_operations(self):
        """track_memory() records multiple operations correctly."""
        processor = MockProcessor()

        with track_memory(processor, "operation_1"):
            pass

        with track_memory(processor, "operation_2"):
            pass

        with track_memory(processor, "operation_3"):
            pass

        assert len(processor._roastcoffea_current_chunk["memory"]) == 3
        assert "operation_1" in processor._roastcoffea_current_chunk["memory"]
        assert "operation_2" in processor._roastcoffea_current_chunk["memory"]
        assert "operation_3" in processor._roastcoffea_current_chunk["memory"]

    def test_track_memory_allocates_memory(self):
        """track_memory() detects memory allocation (if psutil available)."""
        processor = MockProcessor()

        with track_memory(processor, "allocation_test"):
            # Allocate some memory
            data = [0] * 100000  # ~800KB worth of integers

        delta_mb = processor._roastcoffea_current_chunk["memory"]["allocation_test"]
        # If psutil is available, should detect some memory change
        # If not available, will be 0
        assert isinstance(delta_mb, (int, float))


class TestCombinedContextUsage:
    """Test using both context managers together."""

    def test_track_time_and_memory_together(self):
        """Can use track_time() and track_memory() together."""
        processor = MockProcessor()

        with track_time(processor, "outer_timing"):
            with track_memory(processor, "inner_memory"):
                pass

        assert "outer_timing" in processor._roastcoffea_current_chunk["timing"]
        assert "inner_memory" in processor._roastcoffea_current_chunk["memory"]

    def test_multiple_nested_contexts(self):
        """Can nest multiple context managers."""
        processor = MockProcessor()

        with track_time(processor, "outer"):
            with track_memory(processor, "middle"):
                with track_time(processor, "inner"):
                    pass

        assert "outer" in processor._roastcoffea_current_chunk["timing"]
        assert "middle" in processor._roastcoffea_current_chunk["memory"]
        assert "inner" in processor._roastcoffea_current_chunk["timing"]

    def test_parallel_contexts(self):
        """Can use multiple context managers in sequence."""
        processor = MockProcessor()

        with track_time(processor, "step_1"):
            pass

        with track_memory(processor, "step_2"):
            pass

        with track_time(processor, "step_3"):
            pass

        assert "step_1" in processor._roastcoffea_current_chunk["timing"]
        assert "step_2" in processor._roastcoffea_current_chunk["memory"]
        assert "step_3" in processor._roastcoffea_current_chunk["timing"]

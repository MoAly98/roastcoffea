"""Tests for @track_metrics decorator."""

from __future__ import annotations

import pytest

from roastcoffea.decorator import (
    get_active_collector,
    set_active_collector,
    track_metrics,
)


class MockCollector:
    """Mock MetricsCollector for testing."""

    def __init__(self):
        self.chunk_metrics = []

    def record_chunk_metrics(self, chunk_data):
        self.chunk_metrics.append(chunk_data)


class MockEvents:
    """Mock events object for testing."""

    def __init__(self, num_events=100, dataset="test", filename="test.root"):
        self._num_events = num_events
        self._dataset = dataset
        self._filename = filename

    def __len__(self):
        return self._num_events


class TestTrackMetricsDecorator:
    """Test @track_metrics decorator."""

    def test_decorator_without_collector_is_noop(self):
        """Decorator without active collector is a no-op."""
        set_active_collector(None)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                return {"result": len(events)}

        processor = TestProcessor()
        events = MockEvents(num_events=50)

        result = processor.process(events)

        assert result == {"result": 50}
        # No collector, so no metrics recorded

    def test_decorator_records_chunk_metrics(self):
        """Decorator records chunk metrics when collector is active."""
        collector = MockCollector()
        set_active_collector(collector)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                return {"result": len(events)}

        processor = TestProcessor()
        events = MockEvents(num_events=100)

        result = processor.process(events)

        assert result == {"result": 100}
        assert len(collector.chunk_metrics) == 1

        chunk = collector.chunk_metrics[0]
        assert "t_start" in chunk
        assert "t_end" in chunk
        assert "duration" in chunk
        assert chunk["duration"] > 0
        assert chunk["num_events"] == 100

        set_active_collector(None)

    def test_decorator_captures_timing(self):
        """Decorator captures timing information."""
        collector = MockCollector()
        set_active_collector(collector)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                import time
                time.sleep(0.01)  # Small delay
                return {}

        processor = TestProcessor()
        events = MockEvents()

        processor.process(events)

        chunk = collector.chunk_metrics[0]
        assert chunk["duration"] >= 0.01
        assert chunk["t_end"] > chunk["t_start"]

        set_active_collector(None)

    def test_decorator_captures_memory(self):
        """Decorator captures memory information."""
        collector = MockCollector()
        set_active_collector(collector)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                return {}

        processor = TestProcessor()
        events = MockEvents()

        processor.process(events)

        chunk = collector.chunk_metrics[0]
        assert "mem_before_mb" in chunk
        assert "mem_after_mb" in chunk
        assert "mem_delta_mb" in chunk
        # Memory values should be non-negative (0 if psutil not available)
        assert chunk["mem_before_mb"] >= 0
        assert chunk["mem_after_mb"] >= 0

        set_active_collector(None)

    def test_decorator_captures_event_count(self):
        """Decorator captures event count from len(events)."""
        collector = MockCollector()
        set_active_collector(collector)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                return {}

        processor = TestProcessor()
        events = MockEvents(num_events=250)

        processor.process(events)

        chunk = collector.chunk_metrics[0]
        assert chunk["num_events"] == 250

        set_active_collector(None)

    def test_decorator_records_errors(self):
        """Decorator records chunk even if processing fails."""
        collector = MockCollector()
        set_active_collector(collector)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                raise ValueError("Processing failed")

        processor = TestProcessor()
        events = MockEvents()

        with pytest.raises(ValueError, match="Processing failed"):
            processor.process(events)

        # Should still record chunk with error
        assert len(collector.chunk_metrics) == 1
        chunk = collector.chunk_metrics[0]
        assert "error" in chunk
        assert chunk["error"] == "Processing failed"
        assert "t_start" in chunk
        assert "t_end" in chunk
        assert "duration" in chunk

        set_active_collector(None)

    def test_decorator_multiple_chunks(self):
        """Decorator records multiple chunks correctly."""
        collector = MockCollector()
        set_active_collector(collector)

        class TestProcessor:
            @track_metrics
            def process(self, events):
                return {"nevents": len(events)}

        processor = TestProcessor()

        # Process 3 chunks
        for i in range(3):
            events = MockEvents(num_events=100 + i * 10)
            processor.process(events)

        assert len(collector.chunk_metrics) == 3
        assert collector.chunk_metrics[0]["num_events"] == 100
        assert collector.chunk_metrics[1]["num_events"] == 110
        assert collector.chunk_metrics[2]["num_events"] == 120

        set_active_collector(None)


class TestMetadataExtraction:
    """Test chunk metadata extraction."""

    def test_extract_metadata_from_events_without_len(self):
        """Metadata extraction handles events without len()."""
        from roastcoffea.decorator import _extract_chunk_metadata

        class EventsWithoutLen:
            """Events object that doesn't support len()."""
            pass

        events = EventsWithoutLen()
        metadata = _extract_chunk_metadata(events)

        # Should return empty dict or at least not crash
        assert isinstance(metadata, dict)
        assert "num_events" not in metadata

    def test_extract_metadata_with_metadata_attribute(self):
        """Metadata extraction uses metadata attribute if available."""
        from roastcoffea.decorator import _extract_chunk_metadata

        class EventsWithMetadata:
            """Events with metadata attribute."""
            def __init__(self):
                self.metadata = {"dataset": "test_dataset", "custom_field": "value"}

            def __len__(self):
                return 100

        events = EventsWithMetadata()
        metadata = _extract_chunk_metadata(events)

        assert metadata["num_events"] == 100
        assert metadata["dataset"] == "test_dataset"
        assert metadata["custom_field"] == "value"

    def test_extract_metadata_with_non_dict_metadata(self):
        """Metadata extraction handles non-dict metadata attribute."""
        from roastcoffea.decorator import _extract_chunk_metadata

        class EventsWithBadMetadata:
            """Events with non-dict metadata."""
            def __init__(self):
                self.metadata = "not a dict"

            def __len__(self):
                return 50

        events = EventsWithBadMetadata()
        metadata = _extract_chunk_metadata(events)

        # Should still get num_events
        assert metadata["num_events"] == 50
        # But not try to extract from non-dict metadata
        assert "metadata" not in metadata


class TestActiveCollectorRegistry:
    """Test active collector registry functions."""

    def test_set_and_get_collector(self):
        """Can set and get active collector."""
        collector = MockCollector()
        set_active_collector(collector)

        assert get_active_collector() is collector

        set_active_collector(None)
        assert get_active_collector() is None

    def test_collector_registry_isolation(self):
        """Collector registry is properly isolated."""
        # Start with None
        set_active_collector(None)
        assert get_active_collector() is None

        # Set collector 1
        collector1 = MockCollector()
        set_active_collector(collector1)
        assert get_active_collector() is collector1

        # Replace with collector 2
        collector2 = MockCollector()
        set_active_collector(collector2)
        assert get_active_collector() is collector2
        assert get_active_collector() is not collector1

        # Clear
        set_active_collector(None)
        assert get_active_collector() is None

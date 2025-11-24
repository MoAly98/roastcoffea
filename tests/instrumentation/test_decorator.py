"""Tests for @track_metrics decorator."""

from __future__ import annotations

import pytest

from roastcoffea.decorator import track_metrics, _extract_chunk_metadata


class MockEvents:
    """Mock events object for testing."""

    def __init__(self, num_events=100, metadata=None):
        self._num_events = num_events
        self.metadata = metadata or {}

    def __len__(self):
        return self._num_events


class TestTrackMetricsDecorator:
    """Test @track_metrics decorator."""

    def test_decorator_without_collection_flag_is_noop(self):
        """Decorator without collection flag is a no-op."""

        class TestProcessor:
            @track_metrics
            def process(self, events):
                return {"result": len(events)}

        processor = TestProcessor()
        # No _roastcoffea_collect_metrics flag set
        events = MockEvents(num_events=50)

        result = processor.process(events)

        assert result == {"result": 50}
        # No metrics injected
        assert "__roastcoffea_metrics__" not in result

    def test_decorator_injects_metrics_as_list(self):
        """Decorator injects chunk metrics as list into output."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {"result": len(events)}

        processor = TestProcessor()
        events = MockEvents(num_events=100)

        result = processor.process(events)

        assert result["result"] == 100
        assert "__roastcoffea_metrics__" in result
        assert isinstance(result["__roastcoffea_metrics__"], list)
        assert len(result["__roastcoffea_metrics__"]) == 1

        chunk = result["__roastcoffea_metrics__"][0]
        assert "t_start" in chunk
        assert "t_end" in chunk
        assert "duration" in chunk
        assert chunk["duration"] > 0
        assert chunk["num_events"] == 100

    def test_decorator_captures_timing(self):
        """Decorator captures timing information."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                import time

                time.sleep(0.01)  # Small delay
                return {}

        processor = TestProcessor()
        events = MockEvents()

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert chunk["duration"] >= 0.01
        assert chunk["t_end"] > chunk["t_start"]

    def test_decorator_captures_memory(self):
        """Decorator captures memory information."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {}

        processor = TestProcessor()
        events = MockEvents()

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert "mem_before_mb" in chunk
        assert "mem_after_mb" in chunk
        assert "mem_delta_mb" in chunk
        # Memory values should be non-negative (0 if psutil not available)
        assert chunk["mem_before_mb"] >= 0
        assert chunk["mem_after_mb"] >= 0

    def test_decorator_captures_event_count(self):
        """Decorator captures event count from len(events)."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {}

        processor = TestProcessor()
        events = MockEvents(num_events=250)

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert chunk["num_events"] == 250

    def test_decorator_with_non_dict_output(self):
        """Decorator handles non-dict output gracefully."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return "not a dict"

        processor = TestProcessor()
        events = MockEvents()

        result = processor.process(events)

        # Should return original output unchanged
        assert result == "not a dict"

    def test_decorator_includes_timing_sections(self):
        """Decorator includes timing sections from track_time()."""
        from roastcoffea.instrumentation import track_time

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                with track_time(self, "test_section"):
                    import time

                    time.sleep(0.01)
                return {}

        processor = TestProcessor()
        events = MockEvents()

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert "timing" in chunk
        assert "test_section" in chunk["timing"]
        assert chunk["timing"]["test_section"] >= 0.01

    def test_decorator_includes_memory_sections(self):
        """Decorator includes memory sections from track_memory()."""
        from roastcoffea.instrumentation import track_memory

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                with track_memory(self, "test_memory"):
                    data = [0] * 1000
                return {}

        processor = TestProcessor()
        events = MockEvents()

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert "memory" in chunk
        assert "test_memory" in chunk["memory"]
        assert isinstance(chunk["memory"]["test_memory"], (int, float))


class TestMetadataExtraction:
    """Test chunk metadata extraction."""

    def test_extract_metadata_from_events_without_len(self):
        """Metadata extraction handles events without len()."""

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

        class MockMetadata:
            """Mock metadata object with get method."""

            def get(self, key):
                return {
                    "dataset": "test_dataset",
                    "filename": "test.root",
                    "entrystart": 0,
                    "entrystop": 100,
                    "uuid": "test-uuid",
                }.get(key)

        class EventsWithMetadata:
            """Events with metadata attribute."""

            def __init__(self):
                self.metadata = MockMetadata()

            def __len__(self):
                return 100

        events = EventsWithMetadata()
        metadata = _extract_chunk_metadata(events)

        assert metadata["num_events"] == 100
        assert metadata["dataset"] == "test_dataset"
        assert metadata["file"] == "test.root"
        assert metadata["entry_start"] == 0
        assert metadata["entry_stop"] == 100
        assert metadata["uuid"] == "test-uuid"

    def test_extract_metadata_with_non_dict_metadata(self):
        """Metadata extraction handles non-dict metadata attribute."""

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

    def test_extract_metadata_from_nanoevents_like_object(self):
        """Metadata extraction from NanoEvents-like structure (fallback)."""

        class MockBehavior:
            """Mock NanoEvents behavior."""

            def get(self, key):
                if key == "__events_factory__":
                    return MockFactory()
                return None

        class MockFactory:
            """Mock events factory with partition key."""

            def __init__(self):
                self._partition_key = {
                    "dataset": "my_dataset",
                    "filename": "/path/to/file.root",
                    "entrysteps": [0, 1000],
                }

        class NanoEventsLike:
            """Mock NanoEvents object."""

            def __init__(self):
                self.behavior = MockBehavior()

            def __len__(self):
                return 1000

        events = NanoEventsLike()
        metadata = _extract_chunk_metadata(events)

        assert metadata["num_events"] == 1000
        assert metadata["dataset"] == "my_dataset"
        assert metadata["file"] == "/path/to/file.root"
        assert metadata["entry_start"] == 0
        assert metadata["entry_stop"] == 1000

    def test_extract_metadata_nanoevents_partial_entrysteps(self):
        """Metadata extraction handles partial entry steps."""

        class MockBehavior:
            """Mock behavior with partial entrysteps."""

            def get(self, key):
                if key == "__events_factory__":
                    return MockFactory()
                return None

        class MockFactory:
            """Mock factory with only start entry."""

            def __init__(self):
                self._partition_key = {
                    "dataset": "test",
                    "entrysteps": [500, None],
                }

        class NanoEventsLike:
            """Mock NanoEvents."""

            def __init__(self):
                self.behavior = MockBehavior()

            def __len__(self):
                return 100

        events = NanoEventsLike()
        metadata = _extract_chunk_metadata(events)

        assert metadata["entry_start"] == 500
        assert "entry_stop" not in metadata

    def test_extract_metadata_nanoevents_no_factory(self):
        """Metadata extraction when factory is None."""

        class MockBehavior:
            """Mock behavior returning None for factory."""

            def get(self, key):
                return None

        class NanoEventsLike:
            """Mock NanoEvents without factory."""

            def __init__(self):
                self.behavior = MockBehavior()

            def __len__(self):
                return 200

        events = NanoEventsLike()
        metadata = _extract_chunk_metadata(events)

        # Should still get num_events
        assert metadata["num_events"] == 200
        # But no dataset/file info
        assert "dataset" not in metadata
        assert "file" not in metadata

    def test_extract_metadata_nanoevents_no_partition_key(self):
        """Metadata extraction when factory has no partition key."""

        class MockBehavior:
            """Mock behavior."""

            def get(self, key):
                if key == "__events_factory__":
                    return MockFactory()
                return None

        class MockFactory:
            """Mock factory without _partition_key."""

            pass

        class NanoEventsLike:
            """Mock NanoEvents."""

            def __init__(self):
                self.behavior = MockBehavior()

            def __len__(self):
                return 150

        events = NanoEventsLike()
        metadata = _extract_chunk_metadata(events)

        assert metadata["num_events"] == 150
        assert "dataset" not in metadata

    def test_extract_metadata_nanoevents_non_dict_partition_key(self):
        """Metadata extraction when partition key is not a dict."""

        class MockBehavior:
            """Mock behavior."""

            def get(self, key):
                if key == "__events_factory__":
                    return MockFactory()
                return None

        class MockFactory:
            """Mock factory with non-dict partition key."""

            def __init__(self):
                self._partition_key = "not a dict"

        class NanoEventsLike:
            """Mock NanoEvents."""

            def __init__(self):
                self.behavior = MockBehavior()

            def __len__(self):
                return 75

        events = NanoEventsLike()
        metadata = _extract_chunk_metadata(events)

        assert metadata["num_events"] == 75
        # Should not crash, just skip extraction
        assert "dataset" not in metadata

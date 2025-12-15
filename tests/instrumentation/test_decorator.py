"""Tests for @track_metrics decorator."""

from __future__ import annotations

import pytest

from roastcoffea.decorator import (
    _extract_chunk_metadata,  # noqa: PLC2701
    track_metrics,
)


class MockEventsFactory:
    """Mock events factory that holds the file_handle."""

    def __init__(self, file_handle):
        self.file_handle = file_handle


class MockEvents:
    """Mock events object for testing."""

    def __init__(self, num_events=100, metadata=None, attrs=None):
        self._num_events = num_events
        self.metadata = metadata or {}
        self.attrs = attrs or {}

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
                    data = [0] * 1000  # noqa: F841
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


class TestDecoratorExceptionHandling:
    """Test decorator behavior when process() raises exceptions."""

    def test_decorator_records_error_metrics(self):
        """Decorator records error information when process() raises."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                raise ValueError("Test error")  # noqa: EM101

        processor = TestProcessor()
        events = MockEvents()

        with pytest.raises(ValueError, match="Test error"):
            processor.process(events)

        # Should clean up container
        assert not hasattr(processor, "_roastcoffea_current_chunk")

    def test_decorator_cleans_up_container_on_exception(self):
        """Decorator cleans up metrics container even when exception occurs."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                # Container should be created
                assert hasattr(self, "_roastcoffea_current_chunk")
                raise RuntimeError("Processing failed")  # noqa: EM101

        processor = TestProcessor()
        events = MockEvents()

        with pytest.raises(RuntimeError, match="Processing failed"):
            processor.process(events)

        # Container should be cleaned up
        assert not hasattr(processor, "_roastcoffea_current_chunk")


class TestByteTracking:
    """Test byte tracking from filesource."""

    def test_decorator_tracks_bytes_from_filesource(self):
        """Decorator tracks bytes_read from filesource."""

        class MockFileSource:
            """Mock FSSpecSource from uproot."""

            def __init__(self):
                self._bytes = 1000  # Initial bytes

            @property
            def num_requested_bytes(self):
                return self._bytes

            def simulate_read(self, bytes_to_read):
                """Simulate reading bytes."""
                self._bytes += bytes_to_read

        class MockFile:
            def __init__(self, source):
                self.source = source

        class MockFileHandle:
            def __init__(self, source):
                self.file = MockFile(source)

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                # Simulate reading 500 bytes during processing
                factory = events.attrs["@events_factory"]
                factory.file_handle.file.source.simulate_read(500)
                return {}

        processor = TestProcessor()
        filesource = MockFileSource()
        file_handle = MockFileHandle(filesource)
        factory = MockEventsFactory(file_handle)
        events = MockEvents(attrs={"@events_factory": factory})

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert "bytes_read" in chunk
        assert chunk["bytes_read"] == 500

    def test_decorator_handles_missing_filesource(self):
        """Decorator handles missing filesource gracefully."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {}

        processor = TestProcessor()
        events = MockEvents(metadata={})  # No filesource

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert "bytes_read" in chunk
        assert chunk["bytes_read"] == 0

    def test_decorator_handles_filesource_without_attribute(self):
        """Decorator handles filesource without num_requested_bytes."""

        class BrokenFileSource:
            """File source without the required attribute."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {}

        processor = TestProcessor()
        events = MockEvents(metadata={"filesource": BrokenFileSource()})

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert "bytes_read" in chunk
        assert chunk["bytes_read"] == 0

    def test_decorator_calculates_correct_byte_delta(self):
        """Decorator correctly calculates byte delta."""

        class MockFileSource:
            """Mock FSSpecSource."""

            def __init__(self, start_bytes):
                self._bytes = start_bytes

            @property
            def num_requested_bytes(self):
                return self._bytes

            def add_bytes(self, delta):
                self._bytes += delta

        class MockFile:
            def __init__(self, source):
                self.source = source

        class MockFileHandle:
            def __init__(self, source):
                self.file = MockFile(source)

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                # Start: 5000, read 2500 bytes during processing
                factory = events.attrs["@events_factory"]
                factory.file_handle.file.source.add_bytes(2500)
                return {}

        processor = TestProcessor()
        filesource = MockFileSource(start_bytes=5000)
        file_handle = MockFileHandle(filesource)
        factory = MockEventsFactory(file_handle)
        events = MockEvents(attrs={"@events_factory": factory})

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert chunk["bytes_read"] == 2500
        # Filesource should now be at 7500
        assert filesource.num_requested_bytes == 7500

    def test_decorator_tracks_zero_bytes_when_no_reads(self):
        """Decorator tracks zero bytes when no data is read."""

        class MockFileSource:
            """Mock FSSpecSource that doesn't change."""

            @property
            def num_requested_bytes(self):
                return 1000  # Constant

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                # No reads happen
                return {}

        processor = TestProcessor()
        events = MockEvents(metadata={"filesource": MockFileSource()})

        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert chunk["bytes_read"] == 0

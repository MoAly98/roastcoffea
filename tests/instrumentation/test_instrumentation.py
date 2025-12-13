"""Tests for instrumentation context managers."""

from __future__ import annotations

from roastcoffea.instrumentation import track_bytes, track_memory, track_time


class MockEvents:
    """Mock events object for testing."""

    def __init__(self, metadata=None):
        self.metadata = metadata or {}


class MockFileSource:
    """Mock FSSpecSource from uproot."""

    def __init__(self, start_bytes=1000):
        self._bytes = start_bytes

    @property
    def num_requested_bytes(self):
        return self._bytes

    def simulate_read(self, bytes_to_read):
        """Simulate reading bytes."""
        self._bytes += bytes_to_read


class MockFile:
    """Mock file object containing source."""

    def __init__(self, source):
        self.source = source


class MockFileHandle:
    """Mock filehandle object as expected by decorator."""

    def __init__(self, source):
        self.file = MockFile(source)


class TestTrackBytesContextManager:
    """Test track_bytes() context manager."""

    def test_track_bytes_captures_delta(self):
        """track_bytes() captures byte delta correctly."""

        class TestProcessor:
            def __init__(self):
                self._roastcoffea_current_chunk = {"bytes": {}}

        processor = TestProcessor()
        filesource = MockFileSource(start_bytes=5000)
        filehandle = MockFileHandle(filesource)
        events = MockEvents(metadata={"filehandle": filehandle})

        with track_bytes(processor, events, "test_operation"):
            # Simulate reading 2500 bytes
            filesource.simulate_read(2500)

        # Should have captured the delta
        assert "test_operation" in processor._roastcoffea_current_chunk["bytes"]
        assert processor._roastcoffea_current_chunk["bytes"]["test_operation"] == 2500

    def test_track_bytes_handles_missing_filesource(self):
        """track_bytes() handles missing filesource gracefully."""

        class TestProcessor:
            def __init__(self):
                self._roastcoffea_current_chunk = {"bytes": {}}

        processor = TestProcessor()
        events = MockEvents(metadata={})  # No filesource

        with track_bytes(processor, events, "test_operation"):
            pass  # No reads

        # Should track 0 bytes
        assert processor._roastcoffea_current_chunk["bytes"]["test_operation"] == 0

    def test_track_bytes_handles_filesource_without_attribute(self):
        """track_bytes() handles filesource without num_requested_bytes."""

        class BrokenFileSource:
            """File source without the required attribute."""

            pass

        class TestProcessor:
            def __init__(self):
                self._roastcoffea_current_chunk = {"bytes": {}}

        processor = TestProcessor()
        events = MockEvents(metadata={"filesource": BrokenFileSource()})

        with track_bytes(processor, events, "test_operation"):
            pass

        # Should track 0 bytes
        assert processor._roastcoffea_current_chunk["bytes"]["test_operation"] == 0

    def test_track_bytes_no_collection_active(self):
        """track_bytes() is no-op when no collection is active."""

        class TestProcessor:
            """Processor without metrics container."""

            pass

        processor = TestProcessor()
        filesource = MockFileSource(start_bytes=1000)
        events = MockEvents(metadata={"filesource": filesource})

        # Should not raise, just be a no-op
        with track_bytes(processor, events, "test_operation"):
            filesource.simulate_read(500)

        # Should not have created the container
        assert not hasattr(processor, "_roastcoffea_current_chunk")

    def test_track_bytes_tracks_zero_when_no_reads(self):
        """track_bytes() tracks zero bytes when no reads occur."""

        class MockFileSourceConstant:
            """Mock FSSpecSource that doesn't change."""

            @property
            def num_requested_bytes(self):
                return 1000  # Constant

        class TestProcessor:
            def __init__(self):
                self._roastcoffea_current_chunk = {"bytes": {}}

        processor = TestProcessor()
        events = MockEvents(metadata={"filesource": MockFileSourceConstant()})

        with track_bytes(processor, events, "test_operation"):
            pass  # No reads

        # Should track 0 bytes
        assert processor._roastcoffea_current_chunk["bytes"]["test_operation"] == 0

    def test_track_bytes_creates_container_if_missing(self):
        """track_bytes() creates bytes container if it doesn't exist."""

        class TestProcessor:
            def __init__(self):
                self._roastcoffea_current_chunk = {}  # No bytes dict yet

        processor = TestProcessor()
        filesource = MockFileSource(start_bytes=1000)
        filehandle = MockFileHandle(filesource)
        events = MockEvents(metadata={"filehandle": filehandle})

        with track_bytes(processor, events, "test_operation"):
            filesource.simulate_read(300)

        # Should have created bytes dict and tracked the delta
        assert "bytes" in processor._roastcoffea_current_chunk
        assert processor._roastcoffea_current_chunk["bytes"]["test_operation"] == 300

    def test_track_bytes_multiple_sections(self):
        """track_bytes() can track multiple sections independently."""

        class TestProcessor:
            def __init__(self):
                self._roastcoffea_current_chunk = {"bytes": {}}

        processor = TestProcessor()
        filesource = MockFileSource(start_bytes=1000)
        filehandle = MockFileHandle(filesource)
        events = MockEvents(metadata={"filehandle": filehandle})

        with track_bytes(processor, events, "section_1"):
            filesource.simulate_read(200)

        with track_bytes(processor, events, "section_2"):
            filesource.simulate_read(300)

        # Both sections should be tracked independently
        assert processor._roastcoffea_current_chunk["bytes"]["section_1"] == 200
        assert processor._roastcoffea_current_chunk["bytes"]["section_2"] == 300


class TestTrackTimeContextManager:
    """Test track_time() context manager."""

    def test_track_time_works_without_collection(self):
        """track_time() is no-op when no collection is active."""

        class TestProcessor:
            pass

        processor = TestProcessor()

        # Should not raise
        with track_time(processor, "test_section"):
            pass


class TestTrackMemoryContextManager:
    """Test track_memory() context manager."""

    def test_track_memory_works_without_collection(self):
        """track_memory() is no-op when no collection is active."""

        class TestProcessor:
            pass

        processor = TestProcessor()

        # Should not raise
        with track_memory(processor, "test_section"):
            pass

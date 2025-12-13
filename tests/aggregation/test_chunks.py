"""Tests for chunk-level metrics aggregation."""

from __future__ import annotations

from roastcoffea.aggregation.chunk import build_chunk_info


class TestBuildChunkInfo:
    """Test build_chunk_info() function."""

    def test_build_chunk_info_basic(self):
        """build_chunk_info() transforms chunk metrics correctly."""
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.5,
                "bytes_read": 50000,
            },
            {
                "file": "data2.root",
                "entry_start": 1000,
                "entry_stop": 2000,
                "t_start": 2.5,
                "t_end": 4.0,
                "bytes_read": 75000,
            },
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        assert len(chunk_info) == 2
        assert ("data.root", 0, 1000) in chunk_info
        assert chunk_info[("data.root", 0, 1000)] == (1.0, 2.5, 50000)
        assert ("data2.root", 1000, 2000) in chunk_info
        assert chunk_info[("data2.root", 1000, 2000)] == (2.5, 4.0, 75000)

    def test_build_chunk_info_missing_file_metadata(self):
        """build_chunk_info() skips chunks without file metadata."""
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.5,
                "bytes_read": 50000,
            },
            {
                # Missing file
                "entry_start": 1000,
                "entry_stop": 2000,
                "t_start": 2.5,
                "t_end": 4.0,
                "bytes_read": 75000,
            },
            {
                "file": "data3.root",
                # Missing entry_start
                "entry_stop": 3000,
                "t_start": 4.0,
                "t_end": 5.5,
                "bytes_read": 60000,
            },
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        # Should only have the first chunk
        assert len(chunk_info) == 1
        assert ("data.root", 0, 1000) in chunk_info

    def test_build_chunk_info_missing_timing(self):
        """build_chunk_info() skips chunks without timing data."""
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.5,
                "bytes_read": 50000,
            },
            {
                "file": "data2.root",
                "entry_start": 1000,
                "entry_stop": 2000,
                # Missing t_start
                "t_end": 4.0,
                "bytes_read": 75000,
            },
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        # Should only have the first chunk
        assert len(chunk_info) == 1
        assert ("data.root", 0, 1000) in chunk_info

    def test_build_chunk_info_missing_bytes_defaults_to_zero(self):
        """build_chunk_info() uses 0 for missing bytes_read."""
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.5,
                # Missing bytes_read
            }
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        assert len(chunk_info) == 1
        assert chunk_info[("data.root", 0, 1000)] == (1.0, 2.5, 0)

    def test_build_chunk_info_empty_list(self):
        """build_chunk_info() handles empty list correctly."""
        chunk_info = build_chunk_info([])

        assert chunk_info == {}

    def test_build_chunk_info_all_chunks_invalid(self):
        """build_chunk_info() returns empty dict when all chunks invalid."""
        chunk_metrics = [
            {
                # Missing everything
            },
            {
                "file": "data.root",
                # Missing timing and entry metadata
            },
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        assert chunk_info == {}

    def test_build_chunk_info_duplicate_keys(self):
        """build_chunk_info() handles duplicate chunk keys (last wins)."""
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.0,
                "bytes_read": 50000,
            },
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 2.0,
                "t_end": 3.0,
                "bytes_read": 60000,
            },
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        # Should have one entry with the last value
        assert len(chunk_info) == 1
        assert chunk_info[("data.root", 0, 1000)] == (2.0, 3.0, 60000)

    def test_build_chunk_info_with_extra_fields(self):
        """build_chunk_info() ignores extra fields in chunk metrics."""
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.5,
                "bytes_read": 50000,
                # Extra fields
                "num_events": 1000,
                "mem_delta_mb": 100,
                "timing": {"jet_selection": 0.5},
            }
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        # Should successfully extract the needed fields
        assert len(chunk_info) == 1
        assert chunk_info[("data.root", 0, 1000)] == (1.0, 2.5, 50000)

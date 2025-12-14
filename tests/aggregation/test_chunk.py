"""Tests for chunk metrics aggregation."""

from __future__ import annotations

import pytest

from roastcoffea.aggregation.chunk import aggregate_chunk_metrics


class TestChunkAggregationBasics:
    """Test basic chunk aggregation functionality."""

    def test_empty_chunk_metrics(self):
        """Aggregation with no chunks returns minimal result."""
        result = aggregate_chunk_metrics(chunk_metrics=None)
        assert result["num_chunks"] == 0
        assert "num_successful_chunks" not in result

    def test_single_chunk(self):
        """Aggregation with single chunk."""
        chunk_metrics = [
            {
                "t_start": 1.0,
                "t_end": 2.0,
                "duration": 1.0,
                "num_events": 100,
                "dataset": "test_dataset",
            }
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["num_chunks"] == 1
        assert result["num_successful_chunks"] == 1
        assert result["num_failed_chunks"] == 0
        assert result["chunk_duration_mean"] == 1.0
        assert result["chunk_duration_min"] == 1.0
        assert result["chunk_duration_max"] == 1.0
        assert result["chunk_duration_std"] == 0.0
        assert result["total_events_from_chunks"] == 100

    def test_multiple_chunks(self):
        """Aggregation with multiple chunks."""
        chunk_metrics = [
            {"duration": 1.0, "num_events": 100, "dataset": "test"},
            {"duration": 2.0, "num_events": 200, "dataset": "test"},
            {"duration": 3.0, "num_events": 300, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["num_chunks"] == 3
        assert result["num_successful_chunks"] == 3
        assert result["num_failed_chunks"] == 0
        assert result["chunk_duration_mean"] == 2.0
        assert result["chunk_duration_min"] == 1.0
        assert result["chunk_duration_max"] == 3.0
        assert result["total_events_from_chunks"] == 600
        assert result["chunk_events_mean"] == 200

    def test_failed_chunks(self):
        """Aggregation separates successful and failed chunks."""
        chunk_metrics = [
            {"duration": 1.0, "num_events": 100, "dataset": "test"},
            {"duration": 0.5, "error": "Something went wrong", "dataset": "test"},
            {"duration": 2.0, "num_events": 200, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["num_chunks"] == 3
        assert result["num_successful_chunks"] == 2
        assert result["num_failed_chunks"] == 1
        # Statistics should only include successful chunks
        assert result["chunk_duration_mean"] == 1.5  # (1.0 + 2.0) / 2
        assert result["total_events_from_chunks"] == 300

    def test_all_failed_chunks(self):
        """Aggregation with all failed chunks."""
        chunk_metrics = [
            {"duration": 0.5, "error": "Error 1", "dataset": "test"},
            {"duration": 0.3, "error": "Error 2", "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["num_chunks"] == 2
        assert result["num_successful_chunks"] == 0
        assert result["num_failed_chunks"] == 2
        # No statistics for successful chunks
        assert "chunk_duration_mean" not in result
        assert "total_events_from_chunks" not in result


class TestTimingStatistics:
    """Test timing statistics aggregation."""

    def test_duration_statistics(self):
        """Duration statistics are calculated correctly."""
        chunk_metrics = [
            {"duration": 1.0, "dataset": "test"},
            {"duration": 2.0, "dataset": "test"},
            {"duration": 3.0, "dataset": "test"},
            {"duration": 4.0, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["chunk_duration_mean"] == 2.5
        assert result["chunk_duration_min"] == 1.0
        assert result["chunk_duration_max"] == 4.0
        # Standard deviation for [1, 2, 3, 4] ≈ 1.29
        assert abs(result["chunk_duration_std"] - 1.29) < 0.01

    def test_single_chunk_has_zero_std(self):
        """Single chunk has zero standard deviation."""
        chunk_metrics = [{"duration": 1.5, "dataset": "test"}]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["chunk_duration_std"] == 0.0


class TestMemoryStatistics:
    """Test memory statistics aggregation."""

    def test_memory_statistics(self):
        """Memory statistics are calculated correctly."""
        chunk_metrics = [
            {"duration": 1.0, "mem_delta_mb": 10.0, "dataset": "test"},
            {"duration": 1.0, "mem_delta_mb": 20.0, "dataset": "test"},
            {"duration": 1.0, "mem_delta_mb": 30.0, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["chunk_mem_delta_mean_mb"] == 20.0
        assert result["chunk_mem_delta_min_mb"] == 10.0
        assert result["chunk_mem_delta_max_mb"] == 30.0
        # Standard deviation for [10, 20, 30] = 10.0
        assert result["chunk_mem_delta_std_mb"] == 10.0

    def test_memory_statistics_single_chunk(self):
        """Single chunk memory has zero standard deviation."""
        chunk_metrics = [{"duration": 1.0, "mem_delta_mb": 15.0, "dataset": "test"}]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["chunk_mem_delta_mean_mb"] == 15.0
        assert result["chunk_mem_delta_std_mb"] == 0.0

    def test_chunks_without_memory(self):
        """Chunks without memory data are handled gracefully."""
        chunk_metrics = [
            {"duration": 1.0, "dataset": "test"},
            {"duration": 2.0, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        # No memory statistics if no memory data
        assert "chunk_mem_delta_mean_mb" not in result
        assert "chunk_mem_delta_min_mb" not in result
        assert "chunk_mem_delta_max_mb" not in result

    def test_mixed_memory_availability(self):
        """Some chunks with memory, some without."""
        chunk_metrics = [
            {"duration": 1.0, "mem_delta_mb": 10.0, "dataset": "test"},
            {"duration": 2.0, "dataset": "test"},  # No memory
            {"duration": 3.0, "mem_delta_mb": 30.0, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        # Only includes chunks with memory data
        assert result["chunk_mem_delta_mean_mb"] == 20.0  # (10 + 30) / 2
        assert result["chunk_mem_delta_min_mb"] == 10.0
        assert result["chunk_mem_delta_max_mb"] == 30.0


class TestEventStatistics:
    """Test event count statistics aggregation."""

    def test_event_statistics(self):
        """Event statistics are calculated correctly."""
        chunk_metrics = [
            {"duration": 1.0, "num_events": 100, "dataset": "test"},
            {"duration": 1.0, "num_events": 200, "dataset": "test"},
            {"duration": 1.0, "num_events": 300, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["total_events_from_chunks"] == 600
        assert result["chunk_events_mean"] == 200
        assert result["chunk_events_min"] == 100
        assert result["chunk_events_max"] == 300

    def test_chunks_without_events(self):
        """Chunks without event counts are handled gracefully."""
        chunk_metrics = [
            {"duration": 1.0, "dataset": "test"},
            {"duration": 2.0, "dataset": "test"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        # No event statistics if no event data
        assert "total_events_from_chunks" not in result
        assert "chunk_events_mean" not in result


class TestPerDatasetBreakdown:
    """Test per-dataset metrics breakdown."""

    def test_single_dataset(self):
        """Per-dataset breakdown with single dataset."""
        chunk_metrics = [
            {"duration": 1.0, "num_events": 100, "dataset": "dataset_A"},
            {"duration": 2.0, "num_events": 200, "dataset": "dataset_A"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert "per_dataset" in result
        datasets = result["per_dataset"]
        assert "dataset_A" in datasets
        assert datasets["dataset_A"]["num_chunks"] == 2
        assert datasets["dataset_A"]["total_duration"] == 3.0
        assert datasets["dataset_A"]["mean_duration"] == 1.5
        assert datasets["dataset_A"]["total_events"] == 300
        assert datasets["dataset_A"]["mean_events_per_chunk"] == 150

    def test_multiple_datasets(self):
        """Per-dataset breakdown with multiple datasets."""
        chunk_metrics = [
            {"duration": 1.0, "num_events": 100, "dataset": "dataset_A"},
            {"duration": 2.0, "num_events": 200, "dataset": "dataset_B"},
            {"duration": 3.0, "num_events": 300, "dataset": "dataset_A"},
            {"duration": 4.0, "num_events": 400, "dataset": "dataset_B"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        datasets = result["per_dataset"]

        assert len(datasets) == 2

        # Dataset A: chunks 1 and 3
        assert datasets["dataset_A"]["num_chunks"] == 2
        assert datasets["dataset_A"]["total_duration"] == 4.0
        assert datasets["dataset_A"]["mean_duration"] == 2.0
        assert datasets["dataset_A"]["total_events"] == 400
        assert datasets["dataset_A"]["mean_events_per_chunk"] == 200

        # Dataset B: chunks 2 and 4
        assert datasets["dataset_B"]["num_chunks"] == 2
        assert datasets["dataset_B"]["total_duration"] == 6.0
        assert datasets["dataset_B"]["mean_duration"] == 3.0
        assert datasets["dataset_B"]["total_events"] == 600
        assert datasets["dataset_B"]["mean_events_per_chunk"] == 300

    def test_unknown_dataset(self):
        """Chunks without dataset are labeled as 'unknown'."""
        chunk_metrics = [
            {"duration": 1.0, "num_events": 100},  # No dataset field
            {"duration": 2.0, "num_events": 200, "dataset": "known"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        datasets = result["per_dataset"]
        assert "unknown" in datasets
        assert "known" in datasets
        assert datasets["unknown"]["num_chunks"] == 1
        assert datasets["known"]["num_chunks"] == 1

    def test_dataset_without_events(self):
        """Per-dataset stats when chunks have no event counts."""
        chunk_metrics = [
            {"duration": 1.0, "dataset": "dataset_A"},
            {"duration": 2.0, "dataset": "dataset_A"},
        ]

        result = aggregate_chunk_metrics(chunk_metrics)

        datasets = result["per_dataset"]
        assert datasets["dataset_A"]["total_events"] == 0
        # mean_events_per_chunk not present when total_events is 0
        assert "mean_events_per_chunk" not in datasets["dataset_A"]


class TestSectionMetrics:
    """Test section metrics aggregation."""

    def test_no_section_metrics(self):
        """Aggregation without section metrics."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics=None)

        assert "sections" not in result

    def test_single_section(self):
        """Aggregation with single section."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = [{"name": "jet_selection", "type": "time", "duration": 0.5}]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        assert "sections" in result
        sections = result["sections"]
        assert "jet_selection" in sections
        assert sections["jet_selection"]["count"] == 1
        assert sections["jet_selection"]["total_duration"] == 0.5
        assert sections["jet_selection"]["mean_duration"] == 0.5
        assert sections["jet_selection"]["type"] == "time"

    def test_multiple_sections(self):
        """Aggregation with multiple sections."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = [
            {"name": "jet_selection", "type": "time", "duration": 0.3},
            {"name": "event_selection", "type": "time", "duration": 0.2},
            {"name": "histogram_fill", "type": "time", "duration": 0.1},
        ]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        sections = result["sections"]
        assert len(sections) == 3
        assert sections["jet_selection"]["mean_duration"] == 0.3
        assert sections["event_selection"]["mean_duration"] == 0.2
        assert sections["histogram_fill"]["mean_duration"] == 0.1

    def test_repeated_sections(self):
        """Sections called multiple times are aggregated."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = [
            {"name": "jet_selection", "type": "time", "duration": 0.3},
            {"name": "jet_selection", "type": "time", "duration": 0.5},
            {"name": "jet_selection", "type": "time", "duration": 0.4},
        ]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        sections = result["sections"]
        assert sections["jet_selection"]["count"] == 3
        assert sections["jet_selection"]["total_duration"] == pytest.approx(1.2)
        assert sections["jet_selection"]["mean_duration"] == pytest.approx(0.4)

    def test_memory_tracking_sections(self):
        """Sections with memory tracking."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = [
            {
                "name": "load_branches",
                "type": "memory",
                "duration": 0.3,
                "mem_delta_mb": 100.0,
            },
            {
                "name": "load_branches",
                "type": "memory",
                "duration": 0.4,
                "mem_delta_mb": 200.0,
            },
        ]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        sections = result["sections"]
        assert sections["load_branches"]["count"] == 2
        assert sections["load_branches"]["type"] == "memory"
        assert sections["load_branches"]["mean_duration"] == 0.35
        assert sections["load_branches"]["mean_mem_delta_mb"] == 150.0
        assert sections["load_branches"]["min_mem_delta_mb"] == 100.0
        assert sections["load_branches"]["max_mem_delta_mb"] == 200.0

    def test_mixed_section_and_memory_tracking(self):
        """Mix of time and memory sections."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = [
            {"name": "selection", "type": "time", "duration": 0.3},
            {
                "name": "loading",
                "type": "memory",
                "duration": 0.5,
                "mem_delta_mb": 100.0,
            },
        ]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        sections = result["sections"]
        assert sections["selection"]["type"] == "time"
        assert "mem_delta_mb" not in sections["selection"]
        assert sections["loading"]["type"] == "memory"
        assert sections["loading"]["mean_mem_delta_mb"] == 100.0


class TestEdgeCases:
    """Test edge cases and corner cases."""

    def test_zero_duration_chunk(self):
        """Chunk with zero duration."""
        chunk_metrics = [{"duration": 0.0, "num_events": 100, "dataset": "test"}]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["chunk_duration_mean"] == 0.0
        assert result["chunk_duration_min"] == 0.0
        assert result["chunk_duration_max"] == 0.0

    def test_negative_memory_delta(self):
        """Chunk with negative memory delta (memory freed)."""
        chunk_metrics = [{"duration": 1.0, "mem_delta_mb": -50.0, "dataset": "test"}]

        result = aggregate_chunk_metrics(chunk_metrics)

        assert result["chunk_mem_delta_mean_mb"] == -50.0

    def test_missing_duration(self):
        """Chunk without duration field raises KeyError."""
        chunk_metrics = [{"num_events": 100, "dataset": "test"}]

        with pytest.raises(KeyError):
            aggregate_chunk_metrics(chunk_metrics)

    def test_empty_section_metrics(self):
        """Empty section metrics list."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = []

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        # Empty sections dict should not be present
        assert "sections" not in result or len(result["sections"]) == 0

    def test_section_without_duration(self):
        """Section without duration defaults to 0.0."""
        chunk_metrics = [{"duration": 1.0, "dataset": "test"}]
        section_metrics = [{"name": "test_section", "type": "time"}]

        result = aggregate_chunk_metrics(chunk_metrics, section_metrics)

        sections = result["sections"]
        assert sections["test_section"]["total_duration"] == 0.0
        assert sections["test_section"]["mean_duration"] == 0.0

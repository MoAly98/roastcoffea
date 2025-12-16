"""Tests for branch coverage and data access analysis."""

from __future__ import annotations

import pytest

from roastcoffea.aggregation.branch_coverage import (
    _extract_accessed_branches,  # noqa: PLC2701
    _extract_file_metadata,  # noqa: PLC2701
    aggregate_branch_coverage,
)


class TestExtractFileMetadata:
    """Test file metadata extraction from chunk metrics."""

    def test_extracts_metadata_from_chunks(self):
        """Extracts file-level metadata from chunk metrics."""
        chunk_metrics = [
            {
                "file_metadata": {
                    "filename": "file1.root",
                    "compression_ratio": 0.25,
                    "total_branches": 50,
                    "total_tree_bytes": 10000,
                },
            },
            {
                "file_metadata": {
                    "filename": "file2.root",
                    "compression_ratio": 0.30,
                    "total_branches": 40,
                    "total_tree_bytes": 8000,
                },
            },
        ]

        result = _extract_file_metadata(chunk_metrics)

        assert len(result) == 2
        assert "file1.root" in result
        assert "file2.root" in result
        assert result["file1.root"]["compression_ratio"] == 0.25
        assert result["file2.root"]["total_branches"] == 40

    def test_deduplicates_file_metadata(self):
        """Deduplicates file metadata from multiple chunks of same file."""
        chunk_metrics = [
            {
                "file_metadata": {
                    "filename": "file1.root",
                    "compression_ratio": 0.25,
                    "total_branches": 50,
                    "total_tree_bytes": 10000,
                },
            },
            {
                "file_metadata": {
                    "filename": "file1.root",  # Duplicate
                    "compression_ratio": 0.25,
                    "total_branches": 50,
                    "total_tree_bytes": 10000,
                },
            },
        ]

        result = _extract_file_metadata(chunk_metrics)

        # Should only have one entry
        assert len(result) == 1
        assert "file1.root" in result

    def test_handles_empty_chunk_metrics(self):
        """Handles empty chunk metrics list."""
        result = _extract_file_metadata([])
        assert result == {}

    def test_handles_none_chunk_metrics(self):
        """Handles None chunk metrics."""
        result = _extract_file_metadata(None)
        assert result == {}

    def test_skips_chunks_without_metadata(self):
        """Skips chunks that don't have file_metadata."""
        chunk_metrics = [
            {"file_metadata": {"filename": "file1.root", "compression_ratio": 0.25}},
            {},  # No file_metadata
            {"other_data": "value"},
        ]

        result = _extract_file_metadata(chunk_metrics)

        assert len(result) == 1
        assert "file1.root" in result


class TestExtractAccessedBranches:
    """Test branch extraction from coffea report."""

    def test_extracts_branches_from_columns(self):
        """Extracts unique branch names from awkward columns."""
        coffea_report = {
            "bytesread": 508928,
            "columns": [
                "Jet_pt-data",
                "nJet-offsets",
                "Muon_pt-data",
                "nMuon-offsets",
            ],
            "entries": 11503,
        }

        result = _extract_accessed_branches(coffea_report)

        assert result == {"Jet_pt", "Muon_pt"}

    def test_handles_empty_columns(self):
        """Handles empty columns list."""
        coffea_report = {"bytesread": 0, "columns": [], "entries": 0}

        result = _extract_accessed_branches(coffea_report)

        assert result == set()

    def test_handles_missing_columns(self):
        """Handles missing columns field."""
        coffea_report = {"bytesread": 0, "entries": 0}

        result = _extract_accessed_branches(coffea_report)

        assert result == set()


class TestAggregateBranchCoverage:
    """Test full branch coverage aggregation."""

    def test_calculates_read_percentages(self):
        """Calculates branch and byte read percentages correctly."""
        chunk_metrics = [
            {
                "file": "file1.root",
                "file_metadata": {
                    "filename": "file1.root",
                    "compression_ratio": 0.25,
                    "total_branches": 100,
                    "total_tree_bytes": 10000,
                },
                # Per-chunk metrics (from access_log)
                "accessed_branches": ["Jet_pt", "Muon_pt"],
                "num_branches_accessed": 2,
                "accessed_bytes": 1500,  # Jet_pt (1000) + Muon_pt (500)
                "branches_read_percent": 2.0,  # 2/100
                "bytes_read_percent": 15.0,  # 1500/10000
            },
        ]

        result = aggregate_branch_coverage(chunk_metrics)

        assert "file_read_metrics" in result
        assert "file1.root" in result["file_read_metrics"]

        file_metrics = result["file_read_metrics"]["file1.root"]
        assert file_metrics["total_branches"] == 100
        # 2 branches read out of 100 = 2%
        assert file_metrics["branches_read_percent"] == pytest.approx(2.0)
        # Jet_pt (1000) + Muon_pt (500) = 1500 out of 10000 = 15%
        assert file_metrics["bytes_read_percent"] == pytest.approx(15.0)
        assert file_metrics["bytes_read"] == 1500

    def test_calculates_averages(self):
        """Calculates average read percentages across files."""
        chunk_metrics = [
            {
                "file": "file1.root",
                "file_metadata": {
                    "filename": "file1.root",
                    "compression_ratio": 0.25,
                    "total_branches": 100,
                    "total_tree_bytes": 10000,
                },
                # Per-chunk metrics (from access_log)
                "accessed_branches": ["Jet_pt", "Muon_pt"],
                "num_branches_accessed": 2,
                "accessed_bytes": 4000,  # Jet_pt (2000) + Muon_pt (2000)
                "branches_read_percent": 2.0,  # 2/100
                "bytes_read_percent": 40.0,  # 4000/10000
            },
            {
                "file": "file2.root",
                "file_metadata": {
                    "filename": "file2.root",
                    "compression_ratio": 0.30,
                    "total_branches": 100,
                    "total_tree_bytes": 10000,
                },
                # Per-chunk metrics (from access_log)
                "accessed_branches": ["Jet_pt", "Muon_pt"],
                "num_branches_accessed": 2,
                "accessed_bytes": 6000,  # Jet_pt (3000) + Muon_pt (3000)
                "branches_read_percent": 2.0,  # 2/100
                "bytes_read_percent": 60.0,  # 6000/10000
            },
        ]

        result = aggregate_branch_coverage(chunk_metrics)

        # Both files: 2 branches / 100 = 2%
        assert result["avg_branches_read_percent"] == pytest.approx(2.0)

        # file1: 4000/10000 = 40%, file2: 6000/10000 = 60%, avg = 50%
        assert result["avg_bytes_read_percent"] == pytest.approx(50.0)

    def test_includes_compression_ratios(self):
        """Includes compression ratios list for distribution."""
        chunk_metrics = [
            {"file_metadata": {"filename": "file1.root", "compression_ratio": 0.25}},
            {"file_metadata": {"filename": "file2.root", "compression_ratio": 0.30}},
        ]

        coffea_report = {"columns": []}

        result = aggregate_branch_coverage(chunk_metrics, coffea_report)

        assert "compression_ratios" in result
        assert result["compression_ratios"] == [0.25, 0.30]

    def test_includes_global_branch_count(self):
        """Includes total branches read (global, union across chunks)."""
        chunk_metrics = [
            {
                "file": "file1.root",
                "file_metadata": {
                    "filename": "file1.root",
                    "total_branches": 100,
                    "total_tree_bytes": 10000,
                },
                # Per-chunk metrics (from access_log)
                "accessed_branches": ["Jet_pt", "Muon_pt", "Electron_pt"],
                "num_branches_accessed": 3,
                "accessed_bytes": 0,
                "branches_read_percent": 3.0,
                "bytes_read_percent": 0.0,
            },
        ]

        result = aggregate_branch_coverage(chunk_metrics)

        assert result["total_branches_read"] == 3

    def test_handles_empty_chunk_metrics(self):
        """Handles empty chunk metrics gracefully."""
        coffea_report = {"columns": ["Jet_pt-data"]}

        result = aggregate_branch_coverage([], coffea_report)

        assert result == {}

    def test_handles_none_chunk_metrics(self):
        """Handles None chunk metrics gracefully."""
        coffea_report = {"columns": ["Jet_pt-data"]}

        result = aggregate_branch_coverage(None, coffea_report)

        assert result == {}

    def test_zero_branches_scenario(self):
        """Handles scenario with zero branches accessed."""
        chunk_metrics = [
            {
                "file": "file1.root",
                "file_metadata": {
                    "filename": "file1.root",
                    "total_branches": 100,
                    "total_tree_bytes": 10000,
                },
                # Per-chunk metrics with no branches accessed
                "accessed_branches": [],
                "num_branches_accessed": 0,
                "accessed_bytes": 0,
                "branches_read_percent": 0.0,
                "bytes_read_percent": 0.0,
            },
        ]

        result = aggregate_branch_coverage(chunk_metrics)

        file_metrics = result["file_read_metrics"]["file1.root"]
        assert file_metrics["branches_read_percent"] == 0.0
        assert file_metrics["bytes_read_percent"] == 0.0

    def test_aggregates_accessed_branches_across_chunks(self):
        """Aggregates accessed branches across multiple chunks (union)."""
        chunk_metrics = [
            {
                "file": "file1.root",
                "file_metadata": {
                    "filename": "file1.root",
                    "total_branches": 100,
                    "total_tree_bytes": 10000,
                },
                # First chunk accesses Jet_pt
                "accessed_branches": ["Jet_pt"],
                "num_branches_accessed": 1,
                "accessed_bytes": 1000,
                "branches_read_percent": 1.0,
                "bytes_read_percent": 10.0,
            },
            {
                "file": "file1.root",
                # Second chunk of same file (no file_metadata)
                "accessed_branches": ["Jet_pt", "Muon_pt"],  # Adds Muon_pt
                "num_branches_accessed": 2,
                "accessed_bytes": 1500,
                "branches_read_percent": 2.0,
                "bytes_read_percent": 15.0,
            },
        ]

        result = aggregate_branch_coverage(chunk_metrics)

        # Global accessed branches = union of all chunks
        assert result["total_branches_read"] == 2  # Jet_pt + Muon_pt

        # Per-file metrics come from first chunk per file
        file_metrics = result["file_read_metrics"]["file1.root"]
        assert file_metrics["bytes_read"] == 1000  # First chunk's value
        assert file_metrics["bytes_read_percent"] == pytest.approx(10.0)

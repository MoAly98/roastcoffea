"""Branch coverage and data access analysis.

Analyzes which branches are accessed from ROOT files and calculates
coverage metrics based on both branch count and data volume.
"""

from __future__ import annotations

from typing import Any


def parse_accessed_branches(columns: list[str]) -> set[str]:
    """Extract unique branch names from awkward array columns.

    Only counts -data columns (actual physics branches),
    ignoring -offsets (awkward metadata).

    Parameters
    ----------
    columns : list[str]
        List like ['Jet_pt-data', 'nJet-offsets', 'Muon_pt-data', 'nMuon-offsets']

    Returns
    -------
    set[str]
        Set of branch names like {'Jet_pt', 'Muon_pt'}

    Examples
    --------
    >>> parse_accessed_branches(['Jet_pt-data', 'nJet-offsets', 'Muon_pt-data'])
    {'Jet_pt', 'Muon_pt'}
    """
    branches = set()
    for col in columns:
        if col.endswith("-data"):
            branches.add(col[:-5])  # Strip '-data' suffix
    return branches


def aggregate_branch_coverage(
    chunk_metrics: list[dict[str, Any]] | None,
    coffea_report: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate branch coverage and data access metrics.

    Extracts file-level metadata from chunk_metrics (compression ratio,
    total branches, branch bytes) and combines with accessed branches
    from coffea_report to calculate coverage metrics.

    Note: Currently coffea provides a global set of accessed branches,
    not per-file. The same branches are used for all files.

    Parameters
    ----------
    chunk_metrics : list of dict, optional
        Per-chunk metrics from @track_metrics decorator
    coffea_report : dict
        Coffea report with file-level metrics

    Returns
    -------
    dict
        Branch and byte read metrics including:
        - file_metadata: Dict mapping filename -> file-level metadata
        - compression_ratios: List of compression ratios across files
        - branch_coverage_per_file: Dict mapping filename -> read metrics
        - avg_branches_read_percent: Average % of branches read
        - avg_bytes_read_percent: Average % of bytes read
        - bytes_read_percent_per_file: List of byte read percentages per file
        - total_branches_read: Total number of unique branches read (global)
    """
    metrics: dict[str, Any] = {}

    # Extract and deduplicate file-level metadata from chunk_metrics
    file_metadata = _extract_file_metadata(chunk_metrics)

    if not file_metadata:
        # No file metadata available
        return metrics

    # Extract accessed branches from coffea report (global, not per-file)
    accessed_branches = _extract_accessed_branches(coffea_report)
    num_branches_read = len(accessed_branches)

    # Calculate read metrics per file
    file_read_metrics = {}
    compression_ratios = []
    bytes_read_percentages = []

    for filename, file_info in file_metadata.items():
        # Extract file-level data
        total_branches = file_info.get("total_branches", 0)
        total_tree_bytes = file_info.get("total_tree_bytes", 0)
        branch_bytes = file_info.get("branch_bytes", {})
        compression_ratio = file_info.get("compression_ratio", 0.0)

        # Store compression ratio for distribution
        if compression_ratio > 0:
            compression_ratios.append(compression_ratio)

        # Calculate read percentages
        branches_read_percent = (
            100 * num_branches_read / total_branches
            if total_branches > 0 and num_branches_read > 0
            else 0.0
        )

        bytes_read = (
            sum(branch_bytes.get(branch, 0) for branch in accessed_branches)
            if accessed_branches and branch_bytes
            else 0
        )

        bytes_read_percent = (
            100 * bytes_read / total_tree_bytes if total_tree_bytes > 0 else 0.0
        )

        # Store per-file metrics
        file_read_metrics[filename] = {
            "total_branches": total_branches,
            "branches_read_percent": branches_read_percent,
            "total_tree_bytes": total_tree_bytes,
            "bytes_read": bytes_read,
            "bytes_read_percent": bytes_read_percent,
        }

        # Store bytes read percentage for distribution
        if bytes_read_percent > 0:
            bytes_read_percentages.append(bytes_read_percent)

    # Calculate average read percentages
    if file_read_metrics:
        avg_branches_read_percent = sum(
            f["branches_read_percent"] for f in file_read_metrics.values()
        ) / len(file_read_metrics)
        avg_bytes_read_percent = sum(
            f["bytes_read_percent"] for f in file_read_metrics.values()
        ) / len(file_read_metrics)
    else:
        avg_branches_read_percent = 0.0
        avg_bytes_read_percent = 0.0

    # Assemble metrics
    metrics["file_metadata"] = file_metadata
    metrics["compression_ratios"] = compression_ratios
    metrics["file_read_metrics"] = file_read_metrics
    metrics["avg_branches_read_percent"] = avg_branches_read_percent
    metrics["avg_bytes_read_percent"] = avg_bytes_read_percent
    metrics["bytes_read_percent_per_file"] = bytes_read_percentages
    metrics["total_branches_read"] = num_branches_read

    return metrics


def _extract_file_metadata(
    chunk_metrics: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    """Extract and deduplicate file-level metadata from chunk metrics.

    Each file should only have metadata extracted once (already handled
    by decorator on worker side), but we deduplicate here across workers.

    Parameters
    ----------
    chunk_metrics : list of dict, optional
        Per-chunk metrics from @track_metrics decorator

    Returns
    -------
    dict
        Dict mapping filename -> file_metadata
    """
    if not chunk_metrics:
        return {}

    file_metadata_map = {}

    for chunk in chunk_metrics:
        file_metadata = chunk.get("file_metadata")
        if file_metadata:
            filename = file_metadata.get("filename")
            if filename and filename not in file_metadata_map:
                # First time seeing this file - store metadata
                file_metadata_map[filename] = file_metadata

    return file_metadata_map


def _extract_accessed_branches(
    coffea_report: dict[str, Any],
) -> set[str]:
    """Extract accessed branches from coffea report.

    Parses the 'columns' field from coffea report to determine which
    branches were accessed globally.

    Parameters
    ----------
    coffea_report : dict
        Coffea report with aggregated metrics.
        Structure: {'bytesread': ..., 'columns': [...], 'entries': ..., ...}

    Returns
    -------
    set[str]
        Set of accessed branch names (global across all files)
    """
    # Coffea report structure (when savemetrics=True):
    # {'bytesread': ..., 'columns': [...], 'entries': ..., 'processtime': ..., 'chunks': ...}
    columns = coffea_report.get("columns", [])

    if columns:
        # Parse branch names (only -data columns)
        return parse_accessed_branches(columns)

    return set()

"""Core metrics aggregator combining all aggregation modules."""

from __future__ import annotations

from typing import Any

from roastcoffea.aggregation.backends import get_parser
from roastcoffea.aggregation.branch_coverage import aggregate_branch_coverage
from roastcoffea.aggregation.chunk import aggregate_chunk_metrics, build_chunk_info
from roastcoffea.aggregation.efficiency import calculate_efficiency_metrics
from roastcoffea.aggregation.fine_metrics import parse_fine_metrics
from roastcoffea.aggregation.workflow import aggregate_workflow_metrics


class MetricsAggregator:
    """Main aggregator combining workflow, worker, and efficiency metrics."""

    def __init__(self, backend: str) -> None:
        """Initialize aggregator for specific backend.

        Parameters
        ----------
        backend : str
            Backend name ("dask", "taskvine", etc.)

        Raises
        ------
        ValueError
            If backend is not supported
        """
        self.backend = backend
        self.parser = get_parser(backend)

    def aggregate(
        self,
        coffea_report: dict[str, Any],
        tracking_data: dict[str, Any] | None,
        t_start: float,
        t_end: float,
        custom_metrics: dict[str, Any] | None = None,
        span_metrics: dict[tuple[str, ...], Any] | None = None,
        processor_name: str | None = None,
        chunk_metrics: list[dict[str, Any]] | None = None,
        section_metrics: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Aggregate all metrics from workflow run.

        Parameters
        ----------
        coffea_report : dict
            Coffea report
        tracking_data : dict, optional
            Backend tracking data
        t_start : float
            Start time
        t_end : float
            End time
        custom_metrics : dict, optional
            Per-dataset metrics
        span_metrics : dict, optional
            Dask Spans cumulative_worker_metrics
        processor_name : str, optional
            Name of processor class for filtering fine metrics
        chunk_metrics : list of dict, optional
            Per-chunk metrics from @track_metrics decorator
        section_metrics : list of dict, optional
            Section metrics from track_section() and track_memory()

        Returns
        -------
        dict
            Combined metrics with structure:
            {
                'raw': {
                    'workers': tracking_data,
                    'tasks': span_metrics,
                    'chunks': chunk_metrics,
                    'sections': section_metrics,
                },
                'summary': {
                    'throughput': {...},
                    'events': {...},
                    'resources': {...},
                    'timing': {...},
                    'efficiency': {...},
                    'fine': {...},
                    'data_access': {...},
                }
            }
        """
        # Aggregate workflow metrics (flat dict)
        workflow_metrics = aggregate_workflow_metrics(
            coffea_report=coffea_report,
            t_start=t_start,
            t_end=t_end,
            custom_metrics=custom_metrics,
        )

        # Parse worker metrics if tracking data available
        worker_metrics = {}
        if tracking_data is not None:
            worker_metrics = self.parser.parse_tracking_data(tracking_data)

        # Parse fine metrics from Spans if available
        fine_metrics = {}
        if span_metrics:
            fine_metrics = parse_fine_metrics(
                span_metrics, processor_name=processor_name
            )

        # Aggregate chunk metrics if available
        chunk_info = None
        chunk_agg = {}
        if chunk_metrics:
            chunk_agg = aggregate_chunk_metrics(
                chunk_metrics=chunk_metrics,
                section_metrics=section_metrics,
            )
            chunk_info = build_chunk_info(chunk_metrics)

        # Aggregate branch coverage and data access metrics
        branch_coverage_metrics = aggregate_branch_coverage(
            chunk_metrics=chunk_metrics,
            coffea_report=coffea_report,
        )

        # Calculate efficiency metrics
        efficiency_metrics = calculate_efficiency_metrics(
            workflow_metrics=workflow_metrics,
            worker_metrics=worker_metrics,
        )

        # Build the new nested structure
        return {
            "raw": {
                "workers": tracking_data,
                "tasks": span_metrics,
                "chunks": chunk_metrics,
                "sections": section_metrics,
                "chunk_info": chunk_info,
            },
            "summary": {
                "throughput": {
                    "data_rate_gbps": workflow_metrics.get("data_rate_gbps"),
                    "data_rate_mbps": (
                        workflow_metrics.get("data_rate_gbps", 0) * 1000 / 8
                        if workflow_metrics.get("data_rate_gbps")
                        else None
                    ),
                    "bytes_read": workflow_metrics.get("total_bytes_read"),
                    "bytes_read_spans": fine_metrics.get("total_bytes_memory_read"),
                },
                "events": {
                    "total": workflow_metrics.get("total_events"),
                    "rate_wall_khz": workflow_metrics.get("event_rate_elapsed_khz"),
                    "rate_cpu_khz": workflow_metrics.get("event_rate_cpu_total_khz"),
                    "rate_core_khz": efficiency_metrics.get("event_rate_core_khz"),
                },
                "resources": {
                    "workers_avg": worker_metrics.get("avg_workers"),
                    "workers_peak": worker_metrics.get("peak_workers"),
                    "cores_per_worker": worker_metrics.get("cores_per_worker"),
                    "cores_total": worker_metrics.get("total_cores"),
                    "memory_peak_bytes": worker_metrics.get("peak_memory_bytes"),
                    "memory_avg_bytes": worker_metrics.get(
                        "avg_memory_per_worker_bytes"
                    ),
                },
                "timing": {
                    "wall_seconds": workflow_metrics.get("elapsed_time_seconds"),
                    "cpu_seconds": workflow_metrics.get("total_cpu_time"),
                    "num_chunks": workflow_metrics.get("num_chunks"),
                    "avg_chunk_seconds": workflow_metrics.get("avg_cpu_time_per_chunk"),
                },
                "efficiency": {
                    "core_efficiency": efficiency_metrics.get("core_efficiency"),
                    "speedup": efficiency_metrics.get("speedup_factor"),
                },
                "fine": {
                    "processor_cpu_seconds": fine_metrics.get(
                        "processor_cpu_time_seconds"
                    ),
                    "processor_io_seconds": fine_metrics.get(
                        "processor_io_wait_time_seconds"
                    ),
                    "processor_cpu_percent": fine_metrics.get("processor_cpu_percent"),
                    "processor_io_percent": fine_metrics.get(
                        "processor_io_wait_percent"
                    ),
                    "overhead_cpu_seconds": fine_metrics.get(
                        "overhead_cpu_time_seconds"
                    ),
                    "overhead_io_seconds": fine_metrics.get(
                        "overhead_io_wait_time_seconds"
                    ),
                    "disk_read_bytes": fine_metrics.get("disk_read_bytes"),
                    "disk_write_bytes": fine_metrics.get("disk_write_bytes"),
                    "compression_seconds": fine_metrics.get("compression_time_seconds"),
                    "decompression_seconds": fine_metrics.get(
                        "decompression_time_seconds"
                    ),
                    "serialization_seconds": fine_metrics.get(
                        "serialization_time_seconds"
                    ),
                    "deserialization_seconds": fine_metrics.get(
                        "deserialization_time_seconds"
                    ),
                },
                "data_access": {
                    "total_branches_read": branch_coverage_metrics.get(
                        "total_branches_read"
                    ),
                    "avg_branches_read_percent": branch_coverage_metrics.get(
                        "avg_branches_read_percent"
                    ),
                    "avg_bytes_read_percent": branch_coverage_metrics.get(
                        "avg_bytes_read_percent"
                    ),
                    "bytes_read_percent_per_file": branch_coverage_metrics.get(
                        "bytes_read_percent_per_file"
                    ),
                    "compression_ratios": branch_coverage_metrics.get(
                        "compression_ratios"
                    ),
                    "file_metadata": branch_coverage_metrics.get("file_metadata"),
                    "file_read_metrics": branch_coverage_metrics.get(
                        "file_read_metrics"
                    ),
                },
                "chunks": {
                    "num_chunks": chunk_agg.get("num_chunks", 0),
                    "num_successful": chunk_agg.get("num_successful_chunks"),
                    "num_failed": chunk_agg.get("num_failed_chunks"),
                    "duration_mean": chunk_agg.get("chunk_duration_mean"),
                    "duration_min": chunk_agg.get("chunk_duration_min"),
                    "duration_max": chunk_agg.get("chunk_duration_max"),
                    "duration_std": chunk_agg.get("chunk_duration_std"),
                    "mem_delta_mean_mb": chunk_agg.get("chunk_mem_delta_mean_mb"),
                    "mem_delta_min_mb": chunk_agg.get("chunk_mem_delta_min_mb"),
                    "mem_delta_max_mb": chunk_agg.get("chunk_mem_delta_max_mb"),
                    "events_mean": chunk_agg.get("chunk_events_mean"),
                    "events_min": chunk_agg.get("chunk_events_min"),
                    "events_max": chunk_agg.get("chunk_events_max"),
                    "per_dataset": chunk_agg.get("per_dataset"),
                    "sections": chunk_agg.get("sections"),
                },
            },
        }

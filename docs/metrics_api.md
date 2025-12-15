# Metrics API Reference

This document describes the structure of the metrics dictionary returned by `MetricsCollector.get_metrics()`.

## Overview

```python
from roastcoffea import MetricsCollector

with MetricsCollector(client, processor_instance=processor) as collector:
    output, report = runner(...)
    collector.extract_metrics_from_output(output)
    collector.set_coffea_report(report)

metrics = collector.get_metrics()
```

The `metrics` dict has two top-level keys:

- **`raw`**: Granular per-worker/per-task/per-chunk data for detailed analysis and visualization
- **`summary`**: Aggregated statistics computed from the raw data

---

## Raw Data (`metrics['raw']`)

Raw data preserves the full granularity of collected metrics. Use this for:
- Time-series visualizations
- Per-task analysis
- Debugging performance issues

### `metrics['raw']['workers']`

**Source**: Periodic sampling of Dask scheduler state during execution.

Worker-level time-series data sampled at configurable intervals (default: 1 second).

| Key | Type | Description |
|-----|------|-------------|
| `worker_counts` | `list[dict]` | Time-series of active worker count. Each entry: `{"timestamp": float, "worker_count": int}` |
| `worker_memory` | `dict[str, list]` | Per-worker memory time-series. Key is worker ID, value is list of `{"timestamp": float, "memory_bytes": int}` |
| `worker_occupancy` | `dict[str, list]` | Per-worker task saturation (tasks / nthreads) |
| `worker_executing` | `dict[str, list]` | Per-worker count of currently executing tasks |
| `worker_cpu` | `dict[str, list]` | Per-worker CPU utilization percentage |

**Used by**: `plot_worker_count_timeline()`, `plot_memory_utilization_timeline()`, `plot_cpu_utilization_timeline()`

---

### `metrics['raw']['tasks']`

**Source**: Dask Spans API - cumulative metrics per task execution.

Per-task metrics from Dask's instrumentation. Each key is a tuple identifying the task.

| Field in task dict | Type | Description |
|--------------------|------|-------------|
| `thread-cpu-time` | `float` | CPU time in seconds |
| `thread-noncpu-time` | `float` | I/O and waiting time in seconds |
| `memory-read` | `int` | Bytes read from memory/disk |
| `serialize` | `float` | Time spent serializing results |
| `compress` | `float` | Time spent compressing data |

**Used by**: `plot_per_task_cpu_io()`, `plot_per_task_bytes_read()`, `plot_per_task_overhead()`

---

### `metrics['raw']['chunks']`

**Source**: `@track_metrics` decorator on processor's `process()` method.

Per-chunk metrics collected during processing. Each entry is a dict:

| Key | Type | Description |
|-----|------|-------------|
| `file` | `str` | Source filename |
| `entry_start` | `int` | First event index in chunk |
| `entry_stop` | `int` | Last event index in chunk |
| `num_events` | `int` | Number of events in chunk |
| `t_start` | `float` | Processing start time (perf_counter) |
| `t_end` | `float` | Processing end time |
| `duration` | `float` | Processing duration in seconds |
| `bytes_read` | `int` | Bytes read from file during processing |
| `mem_before_mb` | `float` | Memory before processing (MB) |
| `mem_after_mb` | `float` | Memory after processing (MB) |
| `mem_delta_mb` | `float` | Memory change during processing |
| `timing` | `dict[str, float]` | Custom timing sections from `track_time()` |
| `memory` | `dict[str, float]` | Custom memory sections from `track_memory()` |
| `bytes` | `dict[str, int]` | Custom byte tracking from `track_bytes()` |

**Used by**: `plot_throughput_timeline()`, `plot_runtime_distribution()`, `plot_runtime_vs_events()`

---

### `metrics['raw']['sections']`

**Source**: User-defined `track_time()` and `track_memory()` context managers.

Custom instrumentation sections defined by the user within their processor.

---

## Summary Data (`metrics['summary']`)

Aggregated statistics computed from raw data and the Coffea report.

### `metrics['summary']['throughput']`

| Key | Type | Description |
|-----|------|-------------|
| `data_rate_gbps` | `float` | Data throughput in Gbps |
| `data_rate_mbps` | `float` | Data throughput in MB/s |
| `bytes_read` | `int` | Total bytes read (from Coffea report) |
| `bytes_read_spans` | `int` | Total bytes from Dask Spans (may differ) |

---

### `metrics['summary']['events']`

| Key | Type | Description |
|-----|------|-------------|
| `total` | `int` | Total events processed |
| `rate_wall_khz` | `float` | Events per second (wall clock) in kHz |
| `rate_cpu_khz` | `float` | Events per CPU-second in kHz |
| `rate_core_avg_khz` | `float` | Events per core per second in kHz |

---

### `metrics['summary']['resources']`

| Key | Type | Description |
|-----|------|-------------|
| `workers_avg` | `float` | Time-averaged worker count |
| `workers_peak` | `int` | Maximum simultaneous workers |
| `cores_per_worker` | `float` | Average cores per worker |
| `cores_total` | `int` | Total cores across all workers |
| `memory_peak_gb` | `float` | Peak memory usage per worker (GB) |
| `memory_avg_gb` | `float` | Average memory usage per worker (GB) |

---

### `metrics['summary']['timing']`

| Key | Type | Description |
|-----|------|-------------|
| `wall_seconds` | `float` | Total elapsed wall-clock time |
| `cpu_seconds` | `float` | Total CPU time (sum across all tasks) |
| `num_chunks` | `int` | Number of chunks processed |
| `avg_chunk_seconds` | `float` | Average processing time per chunk |

---

### `metrics['summary']['efficiency']`

| Key | Type | Description |
|-----|------|-------------|
| `core_efficiency` | `float` | CPU time / (wall time × cores), as percentage |
| `speedup` | `float` | CPU time / wall time (parallelization factor) |
| `efficiency_ratio` | `float` | Event rate wall / event rate CPU |

---

### `metrics['summary']['fine']`

**Source**: Aggregated from `metrics['raw']['tasks']` (Dask Spans).

Only available when Dask Spans are enabled.

| Key | Type | Description |
|-----|------|-------------|
| `processor_cpu_seconds` | `float` | CPU time in processor tasks |
| `processor_io_seconds` | `float` | I/O/wait time in processor tasks |
| `processor_cpu_percent` | `float` | CPU % of processor time |
| `processor_io_percent` | `float` | I/O % of processor time |
| `overhead_cpu_seconds` | `float` | CPU time in Dask overhead tasks |
| `overhead_io_seconds` | `float` | I/O time in Dask overhead tasks |

---

### `metrics['summary']['data_access']`

**Source**: Aggregated from `metrics['raw']['chunks']` and Coffea report.

| Key | Type | Description |
|-----|------|-------------|
| `total_branches_read` | `int` | Number of unique branches accessed |
| `avg_branches_read_percent` | `float` | Average % of file branches read |
| `avg_bytes_read_percent` | `float` | Average % of file bytes read |
| `bytes_read_percent_per_file` | `list[float]` | Byte read % for each file |
| `compression_ratios` | `list[float]` | Compression ratio per file |
| `file_metadata` | `dict` | Per-file metadata (branches, sizes) |
| `file_read_metrics` | `dict` | Per-file read statistics |

---

## Visualization Functions

Each visualization function documents which metrics keys it requires:

| Function | Required Keys | Input Parameter |
|----------|---------------|-----------------|
| `plot_worker_count_timeline()` | `raw.workers` | `tracking_data=metrics['raw']['workers']` |
| `plot_memory_utilization_mean_timeline()` | `raw.workers` | `tracking_data=metrics['raw']['workers']` |
| `plot_cpu_utilization_mean_timeline()` | `raw.workers` | `tracking_data=metrics['raw']['workers']` |
| `plot_throughput_timeline()` | `raw.chunk_info` | `chunk_info=metrics['raw']['chunk_info']` |
| `plot_per_task_cpu_io()` | `raw.tasks` | `span_metrics=metrics['raw']['tasks']` |
| `plot_per_task_bytes_read()` | `raw.tasks` | `span_metrics=metrics['raw']['tasks']` |
| `plot_runtime_distribution()` | `raw.chunks` | `chunk_metrics=metrics['raw']['chunks']` |
| `plot_runtime_vs_events()` | `raw.chunks` | `chunk_metrics=metrics['raw']['chunks']` |
| `plot_efficiency_summary()` | `summary.efficiency` | `metrics=metrics` (full dict) |
| `plot_resource_utilization()` | `summary.resources` | `metrics=metrics` (full dict) |
| `plot_compression_ratio_distribution()` | `summary.data_access` | `metrics=metrics` (full dict) |
| `plot_data_access_percentage()` | `summary.data_access` | `metrics=metrics` (full dict) |

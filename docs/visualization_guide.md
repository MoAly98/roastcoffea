# Visualization Guide

roastcoffea provides **19 plot functions** and **6 summary tables** to help you understand your workflow's performance. This guide explains what each visualization shows and when to use it.

## Quick Reference

### At a Glance

| Category | Plots | Tables | Best For |
|----------|-------|--------|----------|
| [Worker Resources](#worker-resource-plots) | 4 | 1 | Cluster utilization |
| [CPU & Task Activity](#cpu--task-activity-plots) | 4 | - | Worker saturation |
| [Memory](#memory-plots) | 2 | - | Memory bottlenecks |
| [Throughput & I/O](#throughput--io-plots) | 5 | 1 | Data rate analysis |
| [Chunk Analysis](#chunk-analysis-plots) | 2 | 1 | Processing variance |
| [Per-Task Analysis](#per-task-analysis-plots) | 3 | 1 | Dask overhead |
| [Summary](#summary-plots) | 2 | 2 | Overall efficiency |

**Total: 19 plots, 6 tables**

---

## Setup

All plots are available from `roastcoffea.visualization.plots`:

```python
from roastcoffea import MetricsCollector
from roastcoffea.visualization.plots import (
    plot_worker_count_timeline,
    plot_efficiency_summary,
    # ... other plots
)

# Collect metrics
with MetricsCollector(client, processor_instance=processor) as collector:
    output, report = runner(fileset, processor_instance=processor)
    collector.extract_metrics_from_output(output)
    collector.set_coffea_report(report)

# Get metrics dict
metrics = collector.get_metrics()

# Extract raw data for plotting
tracking_data = metrics['raw']['workers']      # Worker timeseries
chunk_metrics = metrics['raw']['chunks']       # Per-chunk data
chunk_info = metrics['raw']['chunk_info']      # Chunk timing for throughput
span_metrics = metrics['raw']['tasks']         # Per-task from Dask Spans
```

---

## Worker Resource Plots

These plots show how your Dask cluster resources are being utilized over time.

### `plot_worker_count_timeline()`

**What it shows**: Number of active workers over time.

**When to use**:
- Verify workers scaled as expected
- Identify periods of reduced parallelism
- Debug auto-scaling issues

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_worker_count_timeline(tracking_data)
```

**Interpretation**:
- Flat line = stable cluster
- Dips = workers died or scaled down
- Ramps = auto-scaling in action

---

### `plot_memory_utilization_mean_timeline()`

**What it shows**: Average memory utilization (%) across all workers over time.

**When to use**:
- Check if you're approaching memory limits
- Identify memory pressure periods
- Tune chunk sizes

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_memory_utilization_mean_timeline(tracking_data)
```

**Interpretation**:
- High utilization (>80%) = risk of OOM, reduce chunk size
- Low utilization (<30%) = room for larger chunks
- Spikes = memory-intensive operations

---

### `plot_memory_utilization_per_worker_timeline()`

**What it shows**: Memory utilization for each worker individually.

**When to use**:
- Identify memory imbalance between workers
- Debug specific worker OOM issues

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_memory_utilization_per_worker_timeline(tracking_data)
```

---

### `plot_resource_utilization()`

**What it shows**: Bar chart summarizing workers, cores, and peak memory.

**When to use**: Quick overview of resource allocation.

**Data source**: Full `metrics` dict

```python
fig, ax = plot_resource_utilization(metrics)
```

---

## CPU & Task Activity Plots

These plots show how busy your workers are and how tasks are distributed.

### `plot_cpu_utilization_mean_timeline()`

**What it shows**: Average CPU utilization (%) across workers over time.

**When to use**:
- Identify CPU bottlenecks
- Check if workers are idle waiting for I/O

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_cpu_utilization_mean_timeline(tracking_data)
```

**Interpretation**:
- Low CPU + high I/O wait = I/O bound workload
- High CPU = compute bound (good!)
- Inconsistent = uneven task distribution

---

### `plot_cpu_utilization_per_worker_timeline()`

**What it shows**: CPU utilization for each worker individually.

**When to use**: Identify imbalanced CPU usage across workers.

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_cpu_utilization_per_worker_timeline(tracking_data)
```

---

### `plot_occupancy_timeline()`

**What it shows**: Task saturation per worker (queued tasks / threads).

**When to use**:
- Check if workers have enough work queued
- Identify scheduling bottlenecks

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_occupancy_timeline(tracking_data)
```

**Interpretation**:
- 0 = worker idle
- 1+ = worker has tasks queued (good)
- Very high = tasks accumulating (potential issue)

---

### `plot_executing_tasks_timeline()`

**What it shows**: Number of tasks actively executing per worker.

**When to use**: Verify workers are running tasks concurrently.

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_executing_tasks_timeline(tracking_data)
```

---

## Memory Plots

Detailed memory analysis for debugging memory issues.

### `plot_memory_utilization_mean_timeline()`

See [Worker Resource Plots](#plot_memory_utilization_mean_timeline).

### `plot_memory_utilization_per_worker_timeline()`

See [Worker Resource Plots](#plot_memory_utilization_per_worker_timeline).

---

## Throughput & I/O Plots

These plots analyze data processing rates, task activity, and I/O patterns.

### `plot_worker_activity_timeline()`

**What it shows**: Active tasks (processing + queued) per worker over time.

**When to use**:
- Visualize task distribution across workers
- Identify workers that are starved of work

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_worker_activity_timeline(
    tracking_data,
    max_legend_entries=5  # Hide legend if many workers
)
```

---

### `plot_total_active_tasks_timeline()`

**What it shows**: Total active tasks summed across all workers over time.

**When to use**:
- Monitor overall task pipeline fullness
- Identify periods with low parallelism

**Data source**: `metrics['raw']['workers']`

```python
fig, ax = plot_total_active_tasks_timeline(tracking_data)
```

---

### `plot_throughput_timeline()`

**What it shows**: Data throughput (MB/s or GB/s) over time with worker count overlay.

**When to use**:
- Measure actual data rate achieved
- Correlate throughput with worker count
- Identify I/O bottlenecks

**Data source**: `metrics['raw']['chunk_info']`, `metrics['raw']['workers']`

```python
fig, ax = plot_throughput_timeline(
    chunk_info=chunk_info,
    tracking_data=tracking_data
)
```

**Interpretation**:
- Steady throughput = good I/O pipeline
- Drops = network/disk congestion or slow chunks
- Correlation with workers = I/O scales with parallelism

---

### `plot_compression_ratio_distribution()`

**What it shows**: Histogram of compression ratios across files.

**When to use**:
- Understand storage efficiency
- Estimate decompression overhead

**Data source**: Full `metrics` dict (uses `summary.data_access`)

```python
fig, ax = plot_compression_ratio_distribution(metrics)
```

**Interpretation**:
- Ratio < 1.0 = data is compressed (0.3 = 70% compression)
- High compression = more CPU for decompression
- Variable ratios = different content types

---

### `plot_data_access_percentage()`

**What it shows**: Histogram of what percentage of each file's bytes were read.

**When to use**:
- Measure column pruning effectiveness
- Identify unnecessary data reads

**Data source**: Full `metrics` dict (uses `summary.data_access`)

```python
fig, ax = plot_data_access_percentage(metrics)
```

**Interpretation**:
- Low % (10-30%) = good column pruning
- High % (>80%) = reading most of the file
- 100% = no pruning benefit

---

## Chunk Analysis Plots

Analyze performance variation across chunks.

### `plot_runtime_distribution()`

**What it shows**: Histogram of chunk processing times.

**When to use**:
- Identify outlier chunks
- Check processing time consistency
- Tune chunk sizes

**Data source**: `metrics['raw']['chunks']`

```python
fig, ax = plot_runtime_distribution(chunk_metrics=chunk_metrics)
```

**Interpretation**:
- Narrow distribution = consistent performance
- Long tail = some chunks much slower (stragglers)
- Bimodal = different chunk characteristics

---

### `plot_runtime_vs_events()`

**What it shows**: Scatter plot of chunk runtime vs number of events.

**When to use**:
- Verify linear scaling with events
- Identify chunks with unusual performance

**Data source**: `metrics['raw']['chunks']`

```python
fig, ax = plot_runtime_vs_events(chunk_metrics=chunk_metrics)
```

**Interpretation**:
- Linear trend = expected scaling
- Points above line = slow chunks (investigate)
- Points below line = fast chunks or cached

---

## Per-Task Analysis Plots

Fine-grained analysis using Dask Spans instrumentation.

### `plot_per_task_cpu_io()`

**What it shows**: Stacked bar chart of CPU time vs I/O wait time per task.

**When to use**:
- Understand CPU vs I/O breakdown
- Identify I/O-bound tasks

**Data source**: `metrics['raw']['tasks']`

```python
fig, ax = plot_per_task_cpu_io(span_metrics)
```

**Interpretation**:
- Mostly CPU (blue) = compute bound
- Mostly I/O (orange) = I/O bound, consider caching

---

### `plot_per_task_bytes_read()`

**What it shows**: Bar chart of bytes read per task.

**When to use**: Identify tasks with high I/O.

**Data source**: `metrics['raw']['tasks']`

```python
fig, ax = plot_per_task_bytes_read(span_metrics)
```

---

### `plot_per_task_overhead()`

**What it shows**: Stacked bar of compression, serialization, and other overhead per task.

**When to use**:
- Quantify Dask overhead
- Decide if overhead is acceptable

**Data source**: `metrics['raw']['tasks']`

```python
fig, ax = plot_per_task_overhead(span_metrics)
```

**Interpretation**:
- High serialization = large intermediate results
- High compression = consider disabling compression

---

## Summary Plots

High-level overview visualizations.

### `plot_efficiency_summary()`

**What it shows**: Bar chart of core efficiency and speedup factor.

**When to use**: Quick efficiency assessment.

**Data source**: Full `metrics` dict

```python
fig, ax = plot_efficiency_summary(metrics)
```

**Interpretation**:
- Core efficiency 80%+ = excellent parallelization
- Core efficiency <50% = significant idle time
- Speedup = how much faster than single-core

---

### `plot_resource_utilization()`

**What it shows**: Bar chart of worker count, core count, peak memory.

**When to use**: Summarize resource allocation.

**Data source**: Full `metrics` dict

```python
fig, ax = plot_resource_utilization(metrics)
```

---

## Summary Tables

Rich-formatted tables for terminal or notebook display.

### `format_throughput_table()`

**What it shows**: Data rate, bytes read from Coffea and Dask Spans.

```python
from roastcoffea.export.reporter import format_throughput_table
from rich.console import Console

table = format_throughput_table(metrics)
Console().print(table)
```

| Metric | Example Value |
|--------|---------------|
| Data Rate | 1.25 Gbps (156.3 MB/s) |
| Total Bytes Read (Coffea) | 4.82 GB |
| Memory Read (Dask Spans) | 5.01 GB |

---

### `format_event_processing_table()`

**What it shows**: Event counts and processing rates.

```python
from roastcoffea.export.reporter import format_event_processing_table

table = format_event_processing_table(metrics)
```

| Metric | Example Value |
|--------|---------------|
| Total Events | 10,000,000 |
| Event Rate (Elapsed Time) | 125.4 kHz |
| Event Rate (Total CPU) | 45.2 kHz |
| Event Rate (Core-Averaged) | 11.3 kHz/core |

---

### `format_resources_table()`

**What it shows**: Worker and resource utilization summary.

```python
from roastcoffea.export.reporter import format_resources_table

table = format_resources_table(metrics)
```

| Metric | Example Value |
|--------|---------------|
| Workers (Time-Averaged) | 3.8 |
| Peak Workers | 4 |
| Cores per Worker | 4.0 |
| Total Cores | 16 |
| Core Efficiency | 72.5% |
| Peak Memory (per worker) | 2.15 GB |

---

### `format_timing_table()`

**What it shows**: Wall time, CPU time, and chunk statistics.

```python
from roastcoffea.export.reporter import format_timing_table

table = format_timing_table(metrics)
```

| Metric | Example Value |
|--------|---------------|
| Elapsed Time | 1m 23s |
| Total CPU Time | 6m 42s |
| Number of Chunks | 48 |
| Avg CPU Time/Chunk | 8.4s |

---

### `format_fine_metrics_table()`

**What it shows**: Detailed CPU/I/O breakdown from Dask Spans.

```python
from roastcoffea.export.reporter import format_fine_metrics_table

table = format_fine_metrics_table(metrics)
if table:  # May be None if Spans not available
    Console().print(table)
```

| Metric | Example Value |
|--------|---------------|
| Processor CPU Time | 5m 30s |
| Processor I/O & Waiting Time | 1m 12s |
| CPU % | 82.1% |
| I/O & Wait % | 17.9% |

---

### `format_chunk_metrics_table()`

**What it shows**: Per-chunk statistics and timing breakdown.

```python
from roastcoffea.export.reporter import format_chunk_metrics_table

table = format_chunk_metrics_table(metrics)
if table:  # May be None if no chunks
    Console().print(table)
```

| Metric | Example Value |
|--------|---------------|
| Total Chunks | 48 |
| Mean Chunk Time | 2.8s |
| Min / Max | 1.2s / 5.1s |
| Mean Events/Chunk | 208,333 |

---

## Using `print_summary()`

The easiest way to see all tables at once:

```python
collector.print_summary()
```

This prints all available tables to the console with Rich formatting.

---

## Common Workflows

### Diagnosing Slow Processing

1. Check `plot_efficiency_summary()` for overall efficiency
2. If low efficiency, check `plot_cpu_utilization_mean_timeline()` for idle time
3. If I/O bound, check `plot_per_task_cpu_io()` for CPU vs I/O breakdown
4. Check `plot_runtime_distribution()` for straggler chunks

### Optimizing Memory

1. Check `plot_memory_utilization_mean_timeline()` for peak usage
2. If near limits, check `plot_memory_utilization_per_worker_timeline()` for imbalance
3. Review `format_chunk_metrics_table()` for memory delta per chunk
4. Consider reducing chunk size if memory is tight

### Understanding I/O

1. Check `plot_throughput_timeline()` for data rate
2. Check `plot_compression_ratio_distribution()` for compression overhead
3. Check `plot_data_access_percentage()` for column pruning effectiveness
4. Review `format_throughput_table()` for summary statistics

---

## Plot Customization

All plot functions support common parameters:

```python
fig, ax = plot_worker_count_timeline(
    tracking_data,
    title="Custom Title",           # Override title
    figsize=(12, 6),               # Figure size in inches
    output_path="plot.png",        # Save to file
)
```

For timeline plots with many workers:

```python
fig, ax = plot_worker_activity_timeline(
    tracking_data,
    max_legend_entries=5,  # Hide legend if > 5 workers
)
```

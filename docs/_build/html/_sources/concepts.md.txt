# Core Concepts

## Metrics Collection Levels

roastcoffea provides three levels of metrics collection, each adding more detail:

### 1. Workflow-Level Metrics

The baseline. `MetricsCollector` tracks overall performance without modifying your processor:

- **Throughput**: Data rate (Gbps), event rate (kHz)
- **Resources**: Workers, cores, memory usage
- **Timing**: Wall time, CPU time, chunk count

These come from Coffea's report and Dask's scheduler state.

### 2. Chunk-Level Metrics

Add `@track_metrics` to collect per-chunk data:

- Timing per chunk
- Memory usage per chunk
- Dataset/file/entry range metadata

The decorator injects metrics into your processor's output. Coffea's tree reduction concatenates metrics from all chunks during aggregation.

### 3. Fine-Grained Metrics

Use `track_time()` and `track_memory()` for section-level profiling:

```python
with track_time(self, "jet_selection"):
    selected_jets = jets[jets.pt > 30]
```

This records timing for specific operations within each chunk.

## How Metrics Are Collected

### Distributed Processing

roastcoffea works in distributed Dask mode. Workers run in separate processes, so metrics can't use shared state.

The `@track_metrics` decorator solves this by injecting metrics into the processor output:

```python
# On worker
result = {"sum": len(events)}
result["__roastcoffea_metrics__"] = [chunk_metrics]
return result
```

Coffea's tree reduction naturally concatenates lists: `[a] + [b] = [a, b]`. After all chunks finish, `MetricsCollector.extract_metrics_from_output()` retrieves the full list.

### Fine Metrics (Dask Spans)

Dask Spans provide detailed activity breakdown:

- **Thread-CPU**: Time spent on CPU
- **Thread-NonCPU**: Difference between wall clock time and CPU time (typically I/O time, GPU time, CPU contention, or GIL contention)
- **Memory-Read**: Data read from worker memory
- **Disk-Read/Write**: Data read/written from disk due to memory spilling

When you pass `processor_instance` to `MetricsCollector`, it separates processor work from Dask overhead by matching task prefixes.

### Worker Tracking

`MetricsCollector` samples Dask's scheduler every second to track:

- Number of workers over time
- Memory usage per worker
- Active tasks
- CPU occupancy

This gives time-series data for resource utilization graphs.

## Understanding the Metrics

### Throughput

**Data Rate** measures network/file I/O:
```
(bytesread × 8) / wall_time = Gbps
```

**Event Rate** has three variants:
- **Wall Clock**: Events/second in real time (includes parallelism)
- **Aggregated**: Events per total CPU-second (sum across workers)
- **Core-Averaged**: Events per core per second in real time

### CPU vs Non-CPU Time

From Dask Spans:

- **CPU Time**: Actually executing on CPU
- **Non-CPU Time**: Difference between wall clock time and CPU time (typically I/O time, GPU time, CPU contention, or GIL contention)

A high non-CPU percentage suggests I/O-bound workload or significant time in GPU/GIL contention. A high CPU percentage suggests compute-bound.

### Processor vs Overhead

When `processor_instance` is provided, fine metrics separate:

- **Processor**: Your `process()` method
- **Overhead**: Dask scheduling, data transfer, retries

Zero overhead is normal if no retries occurred and task switching is minimal.

### Efficiency Ratio

```
(Event Rate Wall Clock) / (Event Rate Aggregated) × 100%
```

Measures how effectively parallelism is utilized. 100% = perfect scaling. Lower values indicate overhead from coordination.

## Data Sources

roastcoffea combines multiple sources:

1. **Coffea Report**: Built-in metrics (`bytesread`, `processtime`, `entries`)
2. **Wall Clock**: `time.perf_counter()` for elapsed time
3. **Worker Tracking**: Scheduler sampling for resources
4. **Dask Spans**: Fine-grained activity breakdown
5. **Decorator**: Per-chunk timing and metadata

These are aggregated into a unified metrics dict.

## Next steps

::::{grid} 1
:gutter: 3

:::{grid-item-card} 📊 Full metrics list
:class-header: bg-info text-white
See {doc}`metrics_reference` for every metric with formulas and units.
:::

:::{grid-item-card} 🏗️ System design
:class-header: bg-dark text-white
Read {doc}`architecture` to understand backends, aggregators, and exporters.
:::

:::{grid-item-card} 🔧 Advanced features
:class-header: bg-light
Check {doc}`advanced` for custom instrumentation and extending backends.
:::

::::

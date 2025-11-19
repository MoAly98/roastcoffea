# Performance Metrics Reference

**Version**: 1.0
**Package**: coffea-metrics
**Purpose**: Comprehensive performance monitoring for Coffea-based HEP analysis workflows

---

## Table of Contents

1. [Overview](#overview)
2. [Workflow-Level Metrics](#workflow-level-metrics)
3. [Worker-Level Metrics](#worker-level-metrics)
4. [Chunk-Level Metrics](#chunk-level-metrics)
5. [Fine Performance Metrics](#fine-performance-metrics)
6. [Internal Instrumentation Metrics](#internal-instrumentation-metrics)
7. [Efficiency Metrics](#efficiency-metrics)
8. [Assumptions & Limitations](#assumptions--limitations)
9. [Metric Collection Methods](#metric-collection-methods)

---

## Overview

This document describes all performance metrics collected by the coffea-metrics package. Metrics are organized into categories based on their granularity and data source.

**Data Sources**:
- **Coffea Report**: Built-in metrics from `coffea.processor.Runner`
- **Wall Clock Timing**: Python `time.perf_counter()`
- **Worker Tracking**: Scheduler-side periodic sampling (Dask)
- **Chunk Tracking**: Decorator-based per-chunk measurements
- **Fine Metrics**: Dask Spans API with activity breakdown
- **Instrumentation**: User-defined sections and checkpoints

---

## Workflow-Level Metrics

These metrics describe the overall workflow execution from start to finish.

### Throughput Metrics

| Metric | Formula | Units | Description |
|--------|---------|-------|-------------|
| **Data Rate** | `(bytesread × 8) / wall_time` | Gbps | Network I/O throughput - compressed data read rate |
| **Data Rate** | `bytesread / wall_time / 10⁶` | MB/s | Same as above in MB/s |
| **Event Rate (Wall Clock)** | `total_events / wall_time / 1000` | kHz | Real-world event processing rate with parallelism |
| **Event Rate (Aggregated)** | `total_events / total_cpu_time / 1000` | kHz | Events per CPU-second (sum of all CPU time) |
| **Event Rate (Core-Averaged)** | `total_events / (wall_time × total_cores)` | Hz/core | Real-time per-core processing rate |

**Data Sources**:
- `bytesread`: Coffea report
- `total_events`: Coffea report (`entries`)
- `wall_time`: `t_end - t_start`
- `total_cpu_time`: Coffea report (`processtime`)
- `total_cores`: `time_avg_workers × cores_per_worker`

### Data Volume Metrics

| Metric | Source | Units | Description |
|--------|--------|-------|-------------|
| **Total Bytes Read (Coffea)** | `coffea_report['bytesread']` | bytes | Bytes reported by Coffea as read from files |
| **Memory Read (Dask Spans)** | Dask Spans (`memory-read`) | bytes | In-memory data access tracked by Dask |
| **Disk Read** | Dask Spans (`disk-read`) | bytes | Actual disk I/O tracked by Dask (when available) |
| **Disk Write** | Dask Spans (`disk-write`)| bytes | Disk writes tracked by Dask (spills, etc.) |
| **Total Events** | `coffea_report['entries']` | count | Number of physics events processed |

**Important Notes**:
- **Coffea bytesread**: What Coffea reports - typically compressed bytes for ROOT files, exact meaning depends on file format and access method
- **Dask memory-read**: In-memory data access Dask observes - may be incomplete for ROOT files due to ROOT's internal I/O management
- **Dask disk-read**: Physical disk I/O - may not be available for all file types/access methods
- **These metrics measure different things and cannot be directly compared or used to calculate compression ratio**
- **For ROOT files**: Coffea's bytesread is typically much larger than Dask's memory-read because ROOT handles decompression internally

### Timing Metrics

| Metric | Formula | Units | Description |
|--------|---------|-------|-------------|
| **Wall Time** | `t_end - t_start` | seconds | Total elapsed time (includes startup, processing, shutdown) |
| **Total CPU Time** | `coffea_report['processtime']` | seconds | Sum of CPU time across all chunks/workers |
| **Number of Chunks** | `coffea_report['chunks']` | count | Number of work items processed by executor |
| **Avg CPU Time per Chunk** | `total_cpu_time / num_chunks` | seconds | Average processing time per chunk |

**Notes**:
- `total_cpu_time` includes both compute and I/O time (cannot separate without fine metrics)
- `total_cpu_time` can exceed `wall_time` with parallelism (speedup factor)

---

## Worker-Level Metrics

These metrics describe resource utilization across Dask workers over time.

### Worker Count Metrics

| Metric | Formula | Units | Description |
|--------|---------|-------|-------------|
| **Time-Averaged Workers** | `∫ worker_count(t) dt / total_time` | count | Trapezoidal integration of worker count timeline |
| **Average Workers** | `mean(worker_counts)` | count | Simple arithmetic mean of worker counts |
| **Peak Workers** | `max(worker_counts)` | count | Maximum concurrent workers observed |
| **Total Cores** | `time_avg_workers × cores_per_worker` | count | Time-averaged CPU cores available |

**Data Source**: Scheduler tracking samples worker count every `interval` seconds (default 1.0s)

**Calculation Details**:
- **Time-weighted average**: Uses trapezoidal rule to account for variable worker counts
  ```
  avg = Σ((count[i] + count[i+1]) / 2 × (t[i+1] - t[i])) / total_time
  ```
- More accurate than simple mean when workers scale up/down during execution

### Memory Metrics

| Metric | Aggregation | Units | Description |
|--------|-------------|-------|-------------|
| **Avg Memory per Worker** | `mean(all worker memory samples)` | GB | Average memory usage across all workers and time |
| **Peak Memory per Worker** | `max(all worker memory samples)` | GB | Maximum memory usage observed on any worker |
| **Memory Utilization %** | `(worker_memory / memory_limit) × 100` | % | Memory pressure (timeline, avg, min, max) |

**Data Source**: Scheduler tracking samples `worker_state.memory` every interval

**Notes**:
- Memory includes Python interpreter overhead, not just physics data
- Lazy arrays (awkward/dask arrays) build up memory during access
- High memory utilization (>80%) indicates potential spilling or OOM risk

### CPU Utilization Timeline

| Metric | Formula | Units | Description |
|--------|---------|-------|-------------|
| **CPU Utilization %** | `(active_tasks / cores_per_worker) × 100` | % | Task scheduling efficiency |

**Data Source**: Scheduler tracking samples `worker_state.processing` (active tasks)

**Notes**:
- `active_tasks` = number of currently executing tasks on worker
- Low CPU utilization indicates idle time (waiting for I/O, poor scheduling)
- This is a **proxy** for CPU usage; actual CPU % available via fine metrics

---

## Chunk-Level Metrics

These metrics describe individual chunk processing collected via the `@track_metrics` decorator.

### Per-Chunk Metrics

| Metric | Measurement | Units | Description |
|--------|-------------|-------|-------------|
| **Chunk ID** | Sequential counter | int | Unique identifier for this chunk |
| **Event Count** | `len(events)` | count | Number of events in this chunk |
| **Processing Time** | `t_end - t_start` | seconds | Wall clock time for this chunk |
| **Memory Start** | `psutil.Process().memory_info().rss` | GB | Process memory before processing |
| **Memory End** | `psutil.Process().memory_info().rss` | GB | Process memory after processing |
| **Memory Delta** | `memory_end - memory_start` | GB | Memory consumed by this chunk |
| **Dataset** | `events.metadata['dataset']` | string | Source dataset name |
| **Filename** | `events.metadata['filename']` | string | Source ROOT file name |

**Data Source**: `@track_metrics` decorator wraps `processor.process()` method

**Measurement Points**:
- **Start**: Beginning of `process()` execution (after coffea's `from_root()` call)
- **End**: After `process()` returns (before result serialization)

**Important Notes**:
- **I/O separation limitation**: Cannot separate `from_root()` I/O time from compute time because ROOT reading happens inside coffea before our decorator executes
- **Memory includes lazy arrays**: Awkward/dask arrays materialize during access, so memory_delta captures compute impact, not just inputs
- **Memory is cumulative**: Python GC may not run immediately, so memory_delta can include previous chunks

### Chunk Statistics

From chunk-level data, we can derive:
- **Min/max/mean chunk time**: Processing time distribution
- **Time per event**: `chunk_time / event_count` variability
- **Memory per event**: `memory_delta / event_count` variability
- **Dataset attribution**: Which datasets are slowest/largest

---

## Fine Performance Metrics

**Status**: ✅ Implemented (v0.2+)

These metrics provide activity-level breakdown using Dask's Spans API. Automatically collected when using `MetricsCollector` with Dask backend.

### Raw Activity Metrics (from Dask Spans)

| Activity | Description | Units | Exported Metric |
|----------|-------------|-------|-----------------|
| **thread-cpu** | Pure CPU computation time | seconds | `cpu_time_seconds` |
| **thread-noncpu** | Non-CPU wall time (I/O, GIL contention) | seconds | `io_time_seconds` |
| **disk-read** | Bytes read from disk (includes ROOT decompression) | bytes | `disk_read_bytes` |
| **disk-write** | Bytes written to disk (spill operations) | bytes | `disk_write_bytes` |
| **compress** | Data compression time | seconds | `compression_time_seconds` |
| **decompress** | Data decompression time | seconds | `decompression_time_seconds` |
| **serialize** | Python object serialization | seconds | `serialization_time_seconds` |
| **deserialize** | Python object deserialization | seconds | `deserialization_time_seconds` |

**Data Source**: Dask automatically tracks these activities per task using `distributed.span`

**Collection Method**:
1. MetricsCollector automatically creates span: `with span("coffea-processing")`
2. Dask tracks activities across all tasks within the span
3. Extract `span.cumulative_worker_metrics` after completion
4. Parse and aggregate into standardized metrics

### Derived Fine Metrics

| Metric | Formula | Units | Description |
|--------|---------|-------|-------------|
| **Processor CPU Time** | `cumulative_worker_metrics['thread-cpu']` (filtered) | seconds | Pure compute in processor |
| **Processor Non-CPU Time** | `cumulative_worker_metrics['thread-noncpu']` (filtered) | seconds | Non-CPU time in processor (I/O, waiting, GIL) |
| **Processor CPU Percentage** | `processor_cpu / (processor_cpu + processor_noncpu) × 100` | % | Fraction of processor time spent computing |
| **Processor Non-CPU Percentage** | `processor_noncpu / (processor_cpu + processor_noncpu) × 100` | % | Fraction of processor time on I/O/waiting |
| **Overhead CPU Time** | `cumulative_worker_metrics['thread-cpu']` (non-processor) | seconds | CPU time in Dask coordination overhead |
| **Overhead Non-CPU Time** | `cumulative_worker_metrics['thread-noncpu']` (non-processor) | seconds | Non-CPU time in Dask overhead |
| **Memory Read (Dask)** | `cumulative_worker_metrics['memory-read']` | bytes | In-memory data access tracked by Dask |
| **Disk Read** | `cumulative_worker_metrics['disk-read']` | bytes | Actual disk I/O tracked by Dask |
| **Disk Write** | `cumulative_worker_metrics['disk-write']` | bytes | Total data written (spills) |
| **Total Compression Overhead** | `compress + decompress` | seconds | Time spent compressing/decompressing |
| **Total Serialization Overhead** | `serialize + deserialize` | seconds | Time spent pickling/unpickling |

**Key Features**:
- **Processor separation**: When `processor_instance` is provided, metrics are separated by processor work vs Dask overhead
- **CPU vs Non-CPU breakdown**: Separates compute from waiting/I/O/GIL contention
- **Overhead visibility**: Quantifies time spent on serialization and compression
- **Multiple byte metrics**: Both Coffea's bytesread and Dask's memory-read/disk-read are reported separately

### Availability

Fine metrics are available when:
- Using Dask backend (`backend="dask"`)
- Dask `distributed` package installed with Spans support
- Using `MetricsCollector` context manager (automatic)

### Robustness: Metric Synchronization

**Challenge**: Worker metrics sync to scheduler via heartbeats (~1s interval). Tasks completing right after a heartbeat won't have metrics available until the next heartbeat.

**Solution**: Automatic retry logic with exponential backoff:

```python
# Retry attempts:
# 1. Immediate check (catches already-synced metrics)
# 2. After 0.5s delay
# 3. After 1.0s delay (total: 1.5s)
```

**Validation**: Checks for actual execution metrics (`thread-cpu`, `thread-noncpu`), not just Dask overhead (lambda wrappers).

**Result**: Robust metric collection with no manual `time.sleep()` needed in user code.

**Parameters** (advanced users):
```python
# Customize retry behavior (not typically needed)
backend = DaskBackend(client)
metrics = backend.get_span_metrics(
    span_info,
    max_retries=5,      # More retries
    retry_delay=0.3     # Shorter initial delay
)
```

---

## Internal Instrumentation Metrics

These are **opt-in** metrics collected via user-placed instrumentation context managers.

### Section Timing

**Context Manager**: `track_section(processor, name)`

**Auto-computes**:
- `section_name`: User-provided label
- `time_delta`: `t_end - t_start` (seconds)

**Use Case**: Measure time spent in specific parts of `process()`:
```python
@track_metrics
def process(self, events):
    with track_section(self, "jet_selection"):
        jets = events.Jet[events.Jet.pt > 30]

    with track_section(self, "histogram_filling"):
        self.hist.fill(jets.pt)
```

**Storage**: Appended to `chunk_metrics[i]['sections']`

### Section Memory

**Context Manager**: `track_memory(processor, name)`

**Auto-computes**:
- `section_name`: User-provided label
- `memory_start_gb`: Memory at entry
- `memory_end_gb`: Memory at exit
- `memory_delta_gb`: `memory_end - memory_start`

**Use Case**: Measure memory consumed by specific operations:
```python
with track_memory(self, "histogram_filling"):
    self.hist.fill(jets.pt)  # How much memory do histograms use?
```

**Storage**: Appended to `chunk_metrics[i]['memory_sections']`

### Custom Metrics

**Base Class**: `BaseInstrumentationContext`

Users can create custom instrumentation contexts:
```python
class MyCustomTracker(BaseInstrumentationContext):
    def __exit__(self, exc_type, exc_val, exc_tb):
        custom_value = compute_something()
        self.record_metric("my_metric", custom_value)
```

**Storage**: Stored in `chunk_metrics[i]['custom_metrics']`

---

## Efficiency Metrics

These are **derived** metrics calculated from other measurements.

### CPU Efficiency

**Formula**:
```
cpu_efficiency = total_cpu_time / (wall_time × total_cores)
```

**Units**: Ratio (0.0 to 1.0), often expressed as %

**Meaning**: Fraction of available CPU resources actually doing useful work

**Interpretation**:
- **100%**: Perfect utilization, all cores busy all the time
- **50%**: Half of CPU time is idle (waiting for I/O, poor scheduling)
- **20%**: Significant idle time, likely I/O bound

**With Fine Metrics**:
```
cpu_efficiency = cpu_time (from thread-cpu) / (wall_time × total_cores)
io_overhead = io_time (from thread-noncpu) / wall_time
```

### Speedup Factor

**Formula**:
```
speedup = total_cpu_time / wall_time
```

**Units**: Ratio (>= 1.0), expressed as "Nx"

**Meaning**: How much faster parallel execution is vs single-core

**Interpretation**:
- **1x**: No speedup (serial execution or 100% I/O bound)
- **50x**: Job took 1/50th the time with parallelism
- **Ideal**: Should equal number of cores if compute-bound

**Relationship to Efficiency**:
```
speedup = cpu_efficiency × total_cores
```

### I/O Overhead

**Formula** (requires fine metrics):
```
io_overhead_pct = (io_time / wall_time) × 100
```

**Units**: Percentage

**Meaning**: What fraction of wall time is spent on I/O (not compute)

**Interpretation**:
- **10%**: Compute-bound, I/O not a bottleneck
- **50%**: Half the time waiting for I/O
- **80%+**: Severely I/O bound, need faster storage/network

### Scaling Efficiency

**Measured via**: Comparing throughput vs worker count

**Ideal Scaling**: `throughput ∝ workers` (linear)

**Actual Scaling**: Measured by running at different worker counts

**Bottlenecks indicated by sub-linear scaling**:
- Network bandwidth saturation
- Shared resource contention (e.g., XRootD server limits)
- Scheduler overhead
- Data skew (some workers starved)

---

## Assumptions & Limitations

### Known Limitations

1. **Byte Metrics from Different Sources** ⚠️ Cannot Be Combined
   - **Coffea bytesread**: Reports what Coffea tracks - file format dependent
   - **Dask memory-read**: In-memory access Dask observes - incomplete for ROOT files
   - **Dask disk-read**: Physical disk I/O - may not be available for all access methods
   - **Cannot calculate compression ratio**: These metrics measure different things and cannot be directly compared
   - **For ROOT files**: Coffea bytesread >> Dask memory-read due to ROOT's internal I/O management

2. **I/O vs Compute Separation** ✅ Resolved
   - **Current (v0.2+)**: Dask Spans provides `thread-cpu` vs `thread-noncpu`
   - **Exported as**: `processor_cpu_time_seconds`, `processor_noncpu_time_seconds`, etc.
   - **Limitation**: `thread-noncpu` includes GPU time and GIL contention, not just disk I/O
   - **Interpretation**: For HEP workflows, `thread-noncpu` is primarily I/O (ROOT reading)

3. **Memory Measurement**
   - **Level**: Process-level RSS via `psutil`, not just physics data
   - **Includes**: Python interpreter, libraries, intermediate arrays
   - **Lazy arrays**: Memory builds up during array access (awkward/dask)
   - **GC timing**: Memory may not be freed immediately, delta includes garbage

4. **Chunk Measurement Boundaries**
   - **Starts**: After coffea's `from_root()` completes
   - **Ends**: Before result serialization
   - **Missing**: Time to read ROOT file, time to serialize result
   - **Solution**: Use fine metrics for complete task timing

5. **Scheduler Overhead**
   - **Sampling interval**: Default 1.0s, may miss short-lived workers
   - **Metrics overhead**: Dask fine metrics have negligible overhead
   - **Spans overhead**: Cumulative metrics tracking is lightweight

### Assumptions

1. **Cores per Worker**: Assumes homogeneous workers (same core count)
2. **Memory Limits**: Assumes workers have `memory_limit` set (Dask default)
3. **Network**: Assumes primary I/O is network-based (XRootD, S3)
4. **Timezones**: All timestamps in UTC (from datetime.isoformat())
5. **Dask Spans**: Fine metrics require Dask `distributed` package with Spans support

---

## Metric Collection Methods

### Timeline Sampling (Worker Tracking)

**Method**: Async task on Dask scheduler samples worker state periodically

**Interval**: Configurable, default 1.0 second

**Sampled Data**:
- Worker count: `len(dask_scheduler.workers)`
- Worker memory: `worker_state.memory` for each worker
- Memory limits: `worker_state.memory_limit`
- Active tasks: `len(worker_state.processing)`
- CPU cores: `worker_state.nthreads`

**Storage**: Time-series data stored in JSON:
```json
{
  "worker_counts": [{"timestamp": "...", "worker_count": 10}, ...],
  "worker_memory": {
    "worker-id": [{"timestamp": "...", "memory_bytes": 123}, ...]
  }
}
```

### Point-in-Time Measurement (Chunk Tracking)

**Method**: Decorator wraps `process()` method, measures at entry/exit

**Timing**: `time.perf_counter()` for wall clock

**Memory**: `psutil.Process().memory_info().rss` for resident set size

**Storage**: List of dictionaries, one per chunk

### Cumulative Metrics (Fine Performance)

**Method**: Dask Spans API accumulates activity metrics across all tasks

**Collection**:
1. Create span: `with span("name") as span_id:`
2. Dask automatically tracks activities per task
3. Extract: `span.cumulative_worker_metrics` (dict)

**Aggregation**: Metrics are additive across tasks

**Granularity**: Per-task-prefix breakdown available

### Event-Driven (Coffea Report)

**Method**: Coffea runner automatically collects these metrics

**Data**: Returned as dictionary from `runner.run()`

**Contents**:
- `bytesread`: Total bytes read
- `entries`: Total events processed
- `processtime`: Aggregated CPU time
- `chunks`: Number of work items
- `columns`: List of branches read

---

## Usage Patterns

### Minimal - Workflow Level Only

```python
from coffea_metrics import MetricsCollector

with MetricsCollector(executor, output_dir="benchmarks") as mc:
    output, report = runner(fileset, processor_instance)
    mc.set_coffea_report(report)

# Get: throughput, event rates, timing, worker counts
```

### Standard - Add Chunk Tracking

```python
from coffea_metrics import MetricsCollector, track_metrics

class MyProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        # Normal processing
        return result

with MetricsCollector(executor, output_dir) as mc:
    output, report = runner(fileset, processor_instance)
    mc.set_coffea_report(report)
    mc.set_chunk_metrics(processor_instance._chunk_metrics)

# Get: + per-chunk timing/memory/attribution
```

### Advanced - Internal Instrumentation

```python
from coffea_metrics import track_metrics, track_section, track_memory

class MyProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        with track_section(self, "jet_selection"):
            jets = events.Jet[events.Jet.pt > 30]

        with track_memory(self, "histogram_filling"):
            self.hist.fill(jets.pt)

        return result

# Get: + per-section timing/memory within chunks
```

### Complete - With Fine Metrics (Dask Spans)

```python
# Automatic - fine metrics collected by default with Dask backend (v0.2+)
with MetricsCollector(client) as collector:
    output, report = runner(fileset, processor_instance)
    collector.set_coffea_report(report)

# Print summary (includes fine metrics table if available)
collector.print_summary()

# Access metrics directly
metrics = collector.get_metrics()
print(f"Processor CPU time: {metrics['processor_cpu_time_seconds']:.1f}s")
print(f"Processor Non-CPU time: {metrics['processor_noncpu_time_seconds']:.1f}s")
print(f"Processor CPU %: {metrics['processor_cpu_percentage']:.1f}%")
print(f"Processor Non-CPU %: {metrics['processor_noncpu_percentage']:.1f}%")
print(f"Bytes read (Coffea): {metrics['total_bytes_read_coffea'] / 1e9:.2f} GB")
print(f"Memory read (Dask): {metrics.get('total_bytes_memory_read_dask', 0) / 1e9:.2f} GB")
print(f"Disk read: {metrics.get('disk_read_bytes', 0) / 1e9:.2f} GB")
```

---

## Future Improvements

### Planned Enhancements

1. **Additional Worker Metrics**
   - `worker_state.metrics['cpu']`: Real CPU % (not just active tasks)
   - `worker_state.metrics['spilled_bytes']`: Memory pressure indicator
   - `worker_state.metrics['host_net_io']`: Network I/O rates
   - `worker_state.metrics['host_disk_io']`: Disk I/O rates

2. **TaskVine Backend**
   - Implement `TaskVineMetricsBackend`
   - Map equivalent metrics to Dask's model

3. **Prometheus Integration**
   - Export metrics to Prometheus
   - Enable long-term monitoring and alerting
   - See: https://distributed.dask.org/en/latest/prometheus.html

---

## Glossary

- **Chunk**: A unit of work (file + entry range) processed by a single task
- **Wall Time**: Real elapsed time (what a clock on the wall would show)
- **CPU Time**: Time CPU was actively executing (excludes I/O waits)
- **Aggregated**: Summed across all tasks/workers
- **Time-Weighted Average**: Average accounting for duration at each value
- **Span**: Dask concept for grouping related tasks and collecting metrics
- **Activity**: Dask fine metrics category (cpu, I/O, compress, etc.)
- **Task Prefix**: Dask task name prefix identifying task type
- **RSS**: Resident Set Size - process memory actually in RAM
- **Spilling**: Moving worker memory to disk when limit exceeded

---

**Document Version**: 1.0
**Last Updated**: 2025-11-07
**Maintained By**: coffea-metrics project

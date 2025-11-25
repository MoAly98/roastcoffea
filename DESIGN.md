# roastcoffea Design Document

**Version**: 0.1.0
**Date**: 2025-11-08
**Status**: Draft

---

## Table of Contents

1. [Introduction & Goals](#1-introduction--goals)
2. [Design Principles](#2-design-principles)
3. [Data Sources](#3-data-sources)
4. [Architecture Overview](#4-architecture-overview)
5. [Memory Management Strategy](#5-memory-management-strategy)
6. [Backend Architecture](#6-backend-architecture)
7. [Chunk Tracking & Streaming](#7-chunk-tracking--streaming)
8. [Fine Metrics Integration](#8-fine-metrics-integration)
9. [Aggregation Strategy](#9-aggregation-strategy)
10. [Visualization & Export](#10-visualization--export)
11. [Extensibility Points](#11-extensibility-points)
12. [Error Handling Strategy](#12-error-handling-strategy)
13. [Performance Overhead](#13-performance-overhead)
14. [Thread Safety](#14-thread-safety)
15. [Testing Strategy](#15-testing-strategy)
16. [Implementation Phases](#16-implementation-phases)
17. [Trade-offs & Rationale](#17-trade-offs--rationale)
18. [Future Enhancements](#18-future-enhancements)

---

## 1. Introduction & Goals

### What is roastcoffea?

roastcoffea is a comprehensive performance monitoring and metrics collection package for Coffea-based High Energy Physics (HEP) analysis workflows. It provides deep insights into:

- Workflow throughput and data rates
- Worker resource utilization (CPU, memory)
- Fine-grained performance breakdown (CPU vs I/O time)
- Per-chunk execution profiling
- Custom user instrumentation
- Scaling efficiency and bottleneck identification

### Primary Goals

1. **Comprehensive**: Collect metrics from all available sources (scheduler, workers, Dask internals, Coffea reports, user code)
2. **Flexible**: Support multiple executors (Dask initially, TaskVine future)
3. **Extensible**: Clear APIs for users to add custom metrics and visualizations
4. **Production-ready**: Handle large-scale workloads (10,000+ chunks, adaptive clusters)
5. **Zero-overhead when disabled**: Users can easily turn off metrics collection
6. **Standalone**: Installable via PyPI, works with any Coffea workflow

### Non-Goals

- Real-time alerting (use Prometheus/Grafana for this)
- Modifying Coffea internals (external package only)
- Supporting non-Dask executors in v0.1 (future work)

---

## 2. Design Principles

### 1. Comprehensive Collection, Opinionated Defaults

- Collect everything available from Dask/Coffea
- Provide sensible default dashboards and reports
- Let users dig deeper into raw data if needed

### 2. Multiple Sources, Cross-Validation **(v0.2+)**

- Worker tracking: our implementation (full control)
- Prometheus metrics: Dask's built-in (battle-tested)
- Having both allows validation and complementary insights
- Never trust a single source
- **Note**: v0.1 has single source (scheduler tracking); v0.2 adds Prometheus for dual tracking

### 3. Streaming Over Buffering **(v0.2+)**

- For high-cardinality data (10k+ chunks), stream to client instead of buffering in workers
- Use `distributed.Queue` for reliable delivery
- Prevents worker memory blow-up
- **Note**: v0.1 doesn't have chunk tracking yet; v0.2 implements streaming when chunk tracking added

### 4. Test Structure Mirrors Source Structure

- Every module in `src/` has a corresponding test file in `tests/` at the same relative path
- Example: `src/roastcoffea/backends/dask.py` → `tests/backends/test_dask.py`
- Makes it easy to find tests for any module
- Scales naturally as codebase grows

### 5. Fail Gracefully

- Worker death shouldn't crash metrics collection
- Missing data sources should degrade gracefully
- Partial results are better than no results

### 6. Document Everything

- Clear docstrings for all public APIs
- Tutorials for common and advanced use cases
- Design rationale in comments for future maintainers

### 7. Start Simple, Scale Features

- **Phased implementation**: Start with baseline parity to `metrics/` (proven functionality)
- **Incremental delivery**: Each phase delivers working, testable code
- **Learn and adapt**: v0.1 baseline informs v0.2+ extensions
- **Avoid big bang**: Don't build everything at once
- **Baseline first**: Replicate what works (scheduler tracking, Rich tables, matplotlib plots)
- **Then extend**: Add new capabilities (Prometheus, Spans, streaming, Bokeh dashboards)

---

## 3. Data Sources

We collect metrics from **seven** distinct sources. Implementation is phased:

**v0.1 Baseline (Parity with metrics/):**
- 3.1 Scheduler-Side Worker Tracking ✅
- 3.6 Coffea Reports ✅
- 3.7 Workflow-Level Metrics ✅

**v0.2+ Extensions:**
- 3.2 Prometheus Metrics
- 3.3 Dask Spans (Fine Metrics)
- 3.4 Chunk-Level Tracking
- 3.5 User Instrumentation

---

### 3.1 Scheduler-Side Worker Tracking (Our Implementation) **[v0.1]**

**What**: Periodic sampling of scheduler state to track workers over time.

**Provides**:
- Worker count timeline
- Memory usage per worker (time series)
- CPU utilization per worker (time series)
- Active task counts
- Worker metadata (threads, memory limits, host info)

**Why we need it**: Full control over sampling frequency and aggregation logic. Works even when Prometheus isn't accessible.

**Implementation**: `DaskMetricsBackend.start_tracking()` schedules a periodic function on the Dask scheduler that records worker state.

**Reference**: Similar to `metrics/worker_tracker.py` from intccms.

---

### 3.2 Prometheus Metrics (Dask Built-in) **[v0.2+]**

**What**: Dask workers expose Prometheus-format metrics on `/metrics` endpoints.

**Provides**:
- `process_resident_memory_bytes`: Worker memory (RSS)
- `process_cpu_seconds_total`: Cumulative CPU time
- `dask_worker_tasks`: Tasks by state (processing, waiting, etc.)
- `dask_worker_threads`: Thread pool size
- Additional internal metrics (GC, network, etc.)

**Why we need it**: Battle-tested, detailed worker internals, worker lifecycle events (restarts, crashes).

**Access modes**:
1. **HTTP**: Direct GET from `http://<worker-host>:<dashboard-port>/metrics` (requires network access)
2. **In-process**: Run scraping code inside worker via `client.run()` (works in restricted networks)

**Fallback behavior**: Try HTTP first; if any worker responds, use HTTP for all. Otherwise, fall back to in-process.

**Implementation**: `DaskMetricsBackend` wraps Mo's Prometheus monitoring code (HTTP + in-process scraping).

---

### 3.3 Dask Spans (Fine Performance Metrics) **[v0.2+]**

**What**: Dask's `distributed.span` API tracks task activity at fine granularity.

**Provides** (via `cumulative_worker_metrics`):
- `thread-cpu`: Pure compute time (seconds)
- `thread-noncpu`: I/O + waiting time (wall - cpu)
- `disk-read` / `disk-write`: Spilling to disk
- `compress` / `decompress`: Compression overhead
- `serialize` / `deserialize`: Pickling overhead
- Per-task-prefix breakdown

**Why we need it**: Separates CPU from I/O without manual instrumentation. Key insight: "Is my job slow because of computation or data access?"

**Implementation**:
1. `MetricsCollector.__enter__()` creates a span context
2. Processing happens inside the span
3. `MetricsCollector.__exit__()` extracts `span.cumulative_worker_metrics`
4. `aggregation/fine_metrics.py` parses the nested dict into flat structure

**API**:
```python
from distributed import span

with span("coffea-processing") as span_id:
    output, report = runner(fileset, processor)

# Later: extract metrics from span_id
```

---

### 3.4 Chunk-Level Tracking (@track_metrics Decorator) **[v0.2+]**

**What**: Decorator that users add to `processor.process()` to track per-chunk execution.

**Provides**:
- Per-chunk: timing, memory (start/end/delta), event count
- Dataset and filename metadata
- Nested section timing (from `track_section()`)
- Nested memory deltas (from `track_memory()`)

**Memory concern**: With 10-50k chunks, buffering all chunk metrics in the processor instance would blow up worker memory.

**Solution**: **Streaming via distributed.Queue**:
1. `MetricsCollector` creates a `distributed.Queue` on the client
2. Decorator sends chunk metrics to queue immediately after each chunk completes
3. Background thread on client drains queue into local list
4. Workers never hold more than 1 chunk's metrics

**Thread safety**: Queue operations are thread-safe. Background consumer runs in separate thread to avoid blocking main thread.

**Worker failure handling**: If a worker dies mid-chunk, that chunk's metrics are lost. Acceptable trade-off vs complexity of guaranteed delivery.

**Implementation**:
```python
@track_metrics
def process(self, events):
    # ... user code ...
    return result
```

Decorator automatically:
- Captures start time, start memory
- Executes `process()`
- Captures end time, end memory
- Sends `{chunk_id, time, memory_delta, events, ...}` to queue

---

### 3.5 User Instrumentation (track_section, track_memory) **[v0.2+]**

**What**: Context managers for fine-grained profiling within `process()`.

**Provides**:
- Named section timing (e.g., "jet_selection", "histogram_filling")
- Named memory deltas (e.g., "loading_jets")
- Custom metrics via `BaseInstrumentationContext`

**Usage**:
```python
@track_metrics
def process(self, events):
    with track_section(self, "jet_selection"):
        jets = events.Jet[events.Jet.pt > 30]

    with track_memory(self, "histogram_filling"):
        self.hist.fill(jets.pt)

    return result
```

**Storage**: Section/memory data is stored in `self._current_chunk["sections"]` and `self._current_chunk["memory_sections"]`, which are sent to the queue along with the chunk metrics.

**Optional**: If user doesn't use these, they simply don't get section-level breakdowns. Chunk-level metrics still work.

---

### 3.6 Coffea Reports (savemetrics=True) **[v0.1]**

**What**: Coffea's built-in performance report when running with `savemetrics=True`.

**Provides**:
- Columns read per dataset
- Bytes read (compressed and uncompressed)
- Compression ratio
- Number of chunks processed
- Fileset structure

**Why we need it**: Authoritative source for data volume metrics. We don't need to recompute this ourselves.

**Implementation**: User passes `report` dict to `MetricsCollector.set_coffea_report(report)` after runner completes.

---

### 3.7 Workflow-Level Metrics (MetricsCollector Wrapper) **[v0.1]**

**What**: Metrics collected by wrapping the entire `Runner.run()` call.

**Provides**:
- Wall time (start to finish)
- Total time (from Coffea report or wall time)
- Start/end timestamps
- Orchestration overhead (wall time - worker time)

**Implementation**: `MetricsCollector` context manager records `time.perf_counter()` at entry and exit.

---

### Summary Table

| Source | Granularity | Frequency | Key Metrics | Memory Cost |
|--------|-------------|-----------|-------------|-------------|
| Scheduler tracking | Per-worker, per-tick | ~1 Hz | Worker count, memory, CPU timeline | Low (time-series, aggregated) |
| Prometheus | Per-worker, per-tick | ~1 Hz | Internal worker state, tasks by state | Low (snapshots) |
| Dask Spans | Per-task-prefix | Once (end) | CPU vs I/O, disk, compression, serialization | Low (aggregated dict) |
| Chunk tracking | Per-chunk | Streamed | Timing, memory, events, filename | Low (streamed, not buffered) |
| User instrumentation | Per-section | Streamed (in chunk) | Custom timings, memory, metrics | Low (nested in chunk) |
| Coffea reports | Per-dataset | Once (end) | Columns, bytes, compression | Low (single dict) |
| Workflow wrapper | Global | Once (end) | Wall time, timestamps | Negligible |

---

## 4. Architecture Overview

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                          MetricsCollector                            │
│                     (Context Manager - Client Side)                  │
│                                                                       │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐        │
│  │ Backend        │  │ Chunk Queue    │  │ Span Context   │        │
│  │ (Dask/TaskVine)│  │ Consumer       │  │                │        │
│  └────────────────┘  └────────────────┘  └────────────────┘        │
│           │                   │                    │                 │
│           ▼                   ▼                    ▼                 │
│  ┌─────────────────────────────────────────────────────┐            │
│  │            Aggregation (core.py)                     │            │
│  │  • backends/dask.py (worker metrics parsing)         │            │
│  │  • workflow.py       • efficiency.py                 │            │
│  │  • chunks.py (v0.2+)                                 │            │
│  └─────────────────────────────────────────────────────┘            │
│           │                                                           │
│           ▼                                                           │
│  ┌──────────────────────┬──────────────────────┐                   │
│  │ Reporting            │ Visualization         │                   │
│  │ • Rich tables        │ • Static plots (v0.1) │                   │
│  │ • JSON export        │ • Interactive (v0.2+) │                   │
│  │ • HTML tables (v0.2+)│ • Dashboards (v0.2+)  │                   │
│  └──────────────────────┴──────────────────────┘                   │
└─────────────────────────────────────────────────────────────────────┘

                              │
                              ▼
              ┌───────────────────────────────┐
              │     Dask Cluster              │
              │                               │
              │  Scheduler  Workers  Workers  │
              │      │         │        │     │
              │      ▼         ▼        ▼     │
              │  Tracking  Prometheus Chunks  │
              │    Loop    (v0.2+)   (v0.2+)  │
              │  (v0.1)                        │
              └───────────────────────────────┘
```

### Data Flow

**v0.1 Baseline:**

1. **Setup** (in `MetricsCollector.__enter__()`):
   - Create backend (Dask)
   - Start worker tracking (scheduler-side loop)

2. **Execution** (user calls `runner()`):
   - Scheduler tracking samples worker state periodically
   - Workers process chunks (no decorator yet)

3. **Teardown** (in `MetricsCollector.__exit__()`):
   - Stop worker tracking, retrieve time-series data
   - User calls `set_coffea_report(report)`
   - Aggregate: workflow metrics + worker metrics + efficiency
   - Generate Rich tables and static plots
   - Save to disk (timestamped directory)

**v0.2+ Extensions:**

4. **Additional Setup**:
   - Create distributed.Queue for chunk metrics
   - Start background thread to consume queue
   - Create Dask span context
   - Start Prometheus scraping

5. **Additional Execution**:
   - `@track_metrics` decorator sends metrics to queue after each chunk
   - Prometheus scraper collects worker metrics periodically
   - Dask span records task activities

6. **Additional Teardown**:
   - Stop queue consumer, retrieve all chunk metrics
   - Exit span, extract fine metrics
   - Generate interactive Bokeh plots and HTML dashboard

---

## 5. Memory Management Strategy **[v0.2+ Feature]**

> **Note**: This section describes streaming architecture for chunk-level tracking, which is planned for v0.2+. v0.1 baseline does not include chunk tracking.

### The Problem

**Scenario**: User processes 50,000 chunks. Each chunk produces ~1 KB of metrics (timing, memory, sections, etc.).

**Naive approach**: Store all chunk metrics in `processor._chunk_metrics` list:
- 50,000 chunks × 1 KB = **50 MB per processor instance**
- In Dask, processor is pickled and sent to workers
- Each worker could have its own processor instance
- When all results return to client, **50 MB × N_workers** of data

**Additional problem**: Processors are immutable in Coffea's model (new instance per chunk). Where do we accumulate metrics?

### Why Singleton Won't Work

**Proposed idea**: Use a singleton class to accumulate metrics globally.

**Why it fails in Dask**:
1. Each worker is a separate Python process
2. A "singleton" in worker A is different from the "singleton" in worker B
3. No shared memory between processes (without explicit IPC)
4. Even if we used `multiprocessing.Manager`, we'd need to serialize data back to client eventually

**Conclusion**: Singleton doesn't help with the fundamental problem: getting metrics from N workers back to 1 client.

### Solution: Streaming via distributed.Queue

**Approach**: Instead of buffering, stream chunk metrics to the client as they're produced.

**Implementation**:

```python
# In MetricsCollector.__enter__()
self._metrics_queue = distributed.Queue(name=f"metrics-{uuid.uuid4()}")
self._chunk_metrics = []
self._queue_thread = threading.Thread(target=self._consume_queue, daemon=True)
self._queue_thread.start()

def _consume_queue(self):
    """Background thread that drains queue into local list."""
    while not self._stop_flag.is_set():
        try:
            chunk_data = self._metrics_queue.get(timeout=1.0)
            self._chunk_metrics.append(chunk_data)
        except TimeoutError:
            continue

# In @track_metrics decorator (runs in worker)
def track_metrics(func):
    @wraps(func)
    def wrapper(self, events):
        # ... collect metrics ...

        # Send to client immediately
        if hasattr(self, '_metrics_queue'):
            self._metrics_queue.put(chunk_metrics)

        return result
    return wrapper
```

**Benefits**:
- **Constant memory in workers**: After sending to queue, chunk metrics are garbage collected
- **Reliable delivery**: `distributed.Queue` is backed by Dask's scheduler (not in-memory)
- **Non-blocking**: Queue writes are async, don't slow down worker
- **Survives worker restart**: Queue persists on scheduler

**Trade-offs**:
- **Network overhead**: Each chunk sends ~1 KB over network (negligible compared to data transfer)
- **Complexity**: Need to manage queue lifecycle, background consumer thread
- **Partial results on failure**: If client dies, queue data is lost (acceptable for metrics)

**Overhead estimate**:
- 50,000 chunks × 1 KB = 50 MB total transferred
- Over 10-minute workflow = 5 MB/min = 83 KB/s
- Negligible compared to typical ROOT file I/O (GB/s)

### Alternative Considered: Configurable Detail Level

**Idea**: Let users choose how much to track:
```python
MetricsConfig(
    chunk_detail="full",  # "full" | "summary" | "minimal"
)
```

**Why we rejected it**: Even "minimal" mode with 50k chunks has overhead. Streaming solves the problem universally without compromising on detail.

**When we'd use it**: For reducing overhead when streaming isn't available (e.g., non-Dask executor that doesn't have Queue-like primitive).

---

## 6. Backend Architecture

### Design Goals

1. **Pluggable**: Support multiple executors (Dask, TaskVine, future: Parsl, Ray)
2. **Executor-specific**: Each executor has different APIs for resource tracking
3. **Consistent interface**: User doesn't need to know which backend is used

### AbstractMetricsBackend

**Location**: `src/roastcoffea/backends/base.py`

**Interface**:
```python
class AbstractMetricsBackend(ABC):
    @abstractmethod
    def start_tracking(self, interval: float) -> None:
        """Start periodic resource tracking."""

    @abstractmethod
    def stop_tracking(self) -> dict[str, Any]:
        """Stop tracking and return collected data."""

    @abstractmethod
    def create_span(self, name: str) -> Any:
        """Create a performance span (if supported)."""

    @abstractmethod
    def get_span_metrics(self, span_id: Any) -> dict[str, Any]:
        """Extract metrics from span."""

    @abstractmethod
    def supports_fine_metrics(self) -> bool:
        """Whether fine-grained metrics are available."""
```

---

### DaskMetricsBackend

**Location**: `src/roastcoffea/backends/dask.py`

**Responsibilities**:
1. Scheduler-side worker tracking (our implementation)
2. Prometheus metrics collection (HTTP + in-process fallback)
3. Dask Spans integration
4. Worker lifecycle events (add/remove)

**Data Structure** (returned by `stop_tracking()`):
```python
{
    "scheduler_tracking": {
        "timeline": [  # Time-series snapshots
            {
                "timestamp": pd.Timestamp(...),
                "workers": [
                    {
                        "addr": "tcp://...",
                        "memory_used": 1.2e9,  # bytes
                        "memory_limit": 4.0e9,
                        "active_tasks": 3,
                        "nthreads": 4,
                    },
                    ...
                ],
            },
            ...
        ],
        "events": [  # Worker births/deaths
            {"timestamp": ..., "worker": "tcp://...", "event": "added"},
            {"timestamp": ..., "worker": "tcp://...", "event": "removed"},
        ],
    },
    "prometheus_metrics": {
        "snapshots": [  # Periodic Prometheus scrapes
            {
                "timestamp": pd.Timestamp(...),
                "worker": "tcp://...",
                "metrics": {
                    "process_resident_memory_bytes": 1.2e9,
                    "process_cpu_seconds_total": 45.3,
                    "dask_worker_tasks": {"processing": 2, "waiting": 5},
                    ...
                },
            },
            ...
        ],
    },
}
```

**Implementation Details**:

**Scheduler-side tracking**:
```python
def _start_scheduler_tracking(client, interval):
    """Run on scheduler via client.run_on_scheduler()."""
    tracking_data = []

    def _sample():
        snapshot = {
            "timestamp": pd.Timestamp.now(tz="UTC"),
            "workers": [
                {
                    "addr": w.address,
                    "memory_used": w.memory.process,
                    "memory_limit": w.memory_limit,
                    "active_tasks": len(w.processing),
                    "nthreads": w.nthreads,
                }
                for w in dask_scheduler.workers.values()
            ],
        }
        tracking_data.append(snapshot)

    # Schedule periodic execution
    pc = PeriodicCallback(_sample, interval * 1000)
    pc.start()
    return tracking_data  # Reference stored on scheduler
```

**Prometheus scraping** (using Mo's code):
- Try HTTP `/metrics` endpoints first
- Fall back to in-process scraping via `client.run()`
- Parse Prometheus text format
- Filter to wanted metrics

**Dask Spans**:
```python
def create_span(self, name: str):
    """Return a distributed.span context manager."""
    from distributed import span
    return span(name)

def get_span_metrics(self, span_id):
    """Extract cumulative_worker_metrics from span."""
    def _extract(span_id):
        spans_ext = dask_scheduler.extensions["spans"]
        span_obj = spans_ext.spans[span_id]
        return dict(span_obj.cumulative_worker_metrics)

    return self.client.run_on_scheduler(_extract, span_id=span_id)
```

---

### Backend Factory

**Location**: `src/roastcoffea/backends/__init__.py`

```python
def create_backend(executor) -> AbstractMetricsBackend:
    """Auto-detect executor type and return appropriate backend."""
    # Check for DaskExecutor
    if hasattr(executor, 'client'):
        from roastcoffea.backends.dask import DaskMetricsBackend
        return DaskMetricsBackend(executor.client)

    # Future: TaskVineExecutor
    # if isinstance(executor, TaskVineExecutor):
    #     from roastcoffea.backends.taskvine import TaskVineMetricsBackend
    #     return TaskVineMetricsBackend(executor)

    raise ValueError(f"Unsupported executor type: {type(executor)}")
```

---

### Dual Tracking Rationale

**Why both our tracking AND Prometheus?**

**Our tracking (scheduler-side)**:
- ✅ Full control over sampling frequency
- ✅ Works in any network environment
- ✅ Time-weighted averaging computed exactly how we want
- ✅ Guaranteed to work with any Dask version

**Prometheus**:
- ✅ More detailed internal metrics (GC pauses, thread states)
- ✅ Battle-tested by Dask team
- ✅ Cross-validation of our tracking
- ✅ Worker restarts and crash detection
- ✅ Future: export to Grafana for real-time monitoring

**Cost**: Minimal. Both are sampling-based, low overhead. Storage is cheap.

**Benefit**: Redundancy and richer insights. If they disagree, we know something is wrong.

---

## 7. Chunk Tracking & Streaming **[v0.2+ Feature]**

> **Note**: This section describes chunk-level tracking and streaming, which is planned for v0.2+. v0.1 baseline does not include chunk tracking.

### Decorator Design

**Location**: `src/roastcoffea/decorator.py`

**Usage**:
```python
from roastcoffea import track_metrics

class MyProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        # ... user code ...
        return result
```

**Implementation**:
```python
def track_metrics(func):
    """Decorator for processor.process() to track chunks."""
    @wraps(func)
    def wrapper(self, events):
        # Check if metrics collection is enabled
        if not hasattr(self, '_metrics_queue'):
            # No queue = metrics disabled, pass through
            return func(self, events)

        # Generate chunk ID
        if not hasattr(self, '_chunk_counter'):
            self._chunk_counter = 0
        chunk_id = self._chunk_counter
        self._chunk_counter += 1

        # Extract metadata from events
        metadata = getattr(events, 'metadata', {})
        dataset = metadata.get('dataset', 'unknown')
        filename = metadata.get('filename', 'unknown')

        # Setup chunk context for instrumentation
        self._current_chunk = {
            'chunk_id': chunk_id,
            'dataset': dataset,
            'filename': filename,
            'events': len(events),
            'sections': [],       # Populated by track_section()
            'memory_sections': [], # Populated by track_memory()
        }

        # Label in Dask fine metrics (if available)
        try:
            from distributed.metrics import context_meter
            ctx = context_meter.meter(f"chunk-{chunk_id}")
            ctx.__enter__()
        except ImportError:
            ctx = None

        # Measure
        t0 = time.perf_counter()
        mem_start = psutil.Process().memory_info().rss / 1e9

        try:
            # Execute user function
            result = func(self, events)
        finally:
            t1 = time.perf_counter()
            mem_end = psutil.Process().memory_info().rss / 1e9

            # Exit context meter
            if ctx is not None:
                ctx.__exit__(None, None, None)

            # Complete chunk metrics
            self._current_chunk.update({
                'time': t1 - t0,
                'memory_start_gb': mem_start,
                'memory_end_gb': mem_end,
                'memory_delta_gb': mem_end - mem_start,
            })

            # Stream to client (non-blocking)
            try:
                self._metrics_queue.put_nowait(self._current_chunk)
            except Exception:
                # Queue full or unavailable - drop this chunk's metrics
                # (acceptable loss for metrics, shouldn't block processing)
                pass

            # Clear current chunk context
            self._current_chunk = None

        return result

    return wrapper
```

**Key points**:
1. **Graceful degradation**: If no queue, just pass through (metrics disabled)
2. **Non-blocking**: `put_nowait()` doesn't block if queue is full, just drops metrics
3. **Context setup**: `self._current_chunk` is available for `track_section()` / `track_memory()`
4. **Dask labeling**: `context_meter` adds labels to Dask fine metrics

---

### Queue Lifecycle

**Creation** (in `MetricsCollector.__enter__()`):
```python
self._metrics_queue = distributed.Queue(
    name=f"roastcoffea-metrics-{uuid.uuid4()}",
    maxsize=1000,  # Limit queue size to prevent runaway memory
)
```

**Injection** (before submitting processor to Dask):
```python
processor_instance._metrics_queue = self._metrics_queue
```

**Consumption** (background thread on client):
```python
def _consume_queue(self):
    """Drain queue into local list."""
    while not self._stop_flag.is_set():
        try:
            chunk_data = self._metrics_queue.get(timeout=1.0)
            with self._lock:
                self._chunk_metrics.append(chunk_data)
        except TimeoutError:
            continue
        except Exception as e:
            # Log but don't crash
            print(f"[roastcoffea] Queue consumer error: {e}")
```

**Cleanup** (in `MetricsCollector.__exit__()`):
```python
# Signal consumer to stop
self._stop_flag.set()
self._queue_thread.join(timeout=5.0)

# Drain any remaining items
while True:
    try:
        chunk_data = self._metrics_queue.get_nowait()
        self._chunk_metrics.append(chunk_data)
    except QueueEmpty:
        break
```

---

### Thread Safety

**Concerns**:
1. Queue writes (from worker threads)
2. Queue reads (from consumer thread)
3. `self._chunk_metrics` list (append from consumer, read from main thread)

**Solutions**:
1. **Queue writes**: `distributed.Queue` is thread-safe by design
2. **Queue reads**: Only one consumer thread, no contention
3. **List append**: Use `threading.Lock` around append in consumer, read after consumer stops

**Lock usage**:
```python
self._lock = threading.Lock()

# In consumer thread
with self._lock:
    self._chunk_metrics.append(chunk_data)

# In main thread (after consumer stopped)
with self._lock:
    return list(self._chunk_metrics)
```

---

### Error Handling

**Scenario 1: Worker dies mid-chunk**
- Chunk metrics not yet sent to queue
- **Behavior**: That chunk's metrics are lost
- **Acceptable**: Metrics are best-effort. Job still completes, we just have incomplete metrics.

**Scenario 2: Queue is full**
- `put_nowait()` raises `QueueFull`
- **Behavior**: Catch exception, drop that chunk's metrics
- **Acceptable**: Prevents backpressure on workers. Sample of metrics still useful.

**Scenario 3: Client dies before consuming queue**
- Queue persists on scheduler until garbage collected
- **Behavior**: Metrics lost
- **Acceptable**: If client dies, user has bigger problems than missing metrics.

**Scenario 4: Scheduler restarts (rare)**
- Queue is lost
- **Behavior**: Metrics collection fails, but job may continue
- **Acceptable**: Scheduler restarts are catastrophic, metrics are least of concerns.

---

## 8. Fine Metrics Integration **[v0.2+ Feature]**

> **Note**: This section describes Dask Spans integration for fine-grained metrics, which is planned for v0.2+. v0.1 baseline uses only scheduler-side tracking.

### Dask Spans Overview

**What**: Dask's `distributed.span` API tracks task execution with fine granularity.

**How it works**:
1. Wrap computation in `with span("name") as span_id:`
2. Dask records all tasks executed within the span
3. For each task, Dask measures time spent in various "activities"
4. Activities are bucketed: `thread-cpu`, `thread-noncpu`, `disk-read`, etc.
5. Metrics are aggregated per (context, task_prefix, activity, unit)
6. Access via `span_obj.cumulative_worker_metrics`

**Key insight**: This gives us CPU vs I/O breakdown without manual instrumentation!

---

### cumulative_worker_metrics Format

**Example**:
```python
{
    # Key: (context, task_prefix, activity, unit)
    ("execute", "process-abc123", "thread-cpu", "seconds"): 45.2,
    ("execute", "process-abc123", "thread-noncpu", "seconds"): 12.8,
    ("execute", "process-abc123", "disk-read", "seconds"): 0.5,
    ("execute", "process-abc123", "disk-write", "seconds"): 0.1,
    ("execute", "process-abc123", "compress", "seconds"): 2.3,
    ("execute", "process-abc123", "decompress", "seconds"): 1.7,
    ("execute", "process-abc123", "serialize", "seconds"): 3.1,
    ("execute", "process-abc123", "deserialize", "seconds"): 2.9,

    # Multiple task prefixes if multiple task types
    ("execute", "fetch-xyz789", "thread-cpu", "seconds"): 5.1,
    ...
}
```

**Activities**:
- `thread-cpu`: Pure computation (user code executing)
- `thread-noncpu`: Wall time - CPU time (I/O, waiting, blocking)
- `disk-read` / `disk-write`: Spilling to disk (indicates memory pressure)
- `compress` / `decompress`: Compression overhead
- `serialize` / `deserialize`: Pickling overhead (data transfer)

---

### Parser Implementation

**Location**: `src/roastcoffea/aggregation/fine_metrics.py`

**Function**:
```python
def parse_fine_metrics(span_metrics: dict) -> dict:
    """Parse Dask span cumulative_worker_metrics into structured format.

    Parameters
    ----------
    span_metrics : dict
        Raw cumulative_worker_metrics from Dask span.

    Returns
    -------
    dict
        Parsed metrics:
        {
            "cpu_time": float,           # Total thread-cpu (seconds)
            "io_time": float,            # Total thread-noncpu (seconds)
            "disk_read_time": float,
            "disk_write_time": float,
            "compression_time": float,   # compress + decompress
            "serialization_time": float, # serialize + deserialize
            "by_task_prefix": {
                "process-abc123": {
                    "cpu_time": 45.2,
                    "io_time": 12.8,
                    ...
                },
                ...
            },
        }
    """
    totals = {
        "cpu_time": 0.0,
        "io_time": 0.0,
        "disk_read_time": 0.0,
        "disk_write_time": 0.0,
        "compression_time": 0.0,
        "serialization_time": 0.0,
    }

    by_task = {}

    for (context, task_prefix, activity, unit), value in span_metrics.items():
        if unit != "seconds":
            continue  # Only handle time metrics for now

        # Aggregate totals
        if activity == "thread-cpu":
            totals["cpu_time"] += value
        elif activity == "thread-noncpu":
            totals["io_time"] += value
        elif activity == "disk-read":
            totals["disk_read_time"] += value
        elif activity == "disk-write":
            totals["disk_write_time"] += value
        elif activity in ("compress", "decompress"):
            totals["compression_time"] += value
        elif activity in ("serialize", "deserialize"):
            totals["serialization_time"] += value

        # Per-task breakdown
        if task_prefix not in by_task:
            by_task[task_prefix] = {
                "cpu_time": 0.0,
                "io_time": 0.0,
                "disk_read_time": 0.0,
                "disk_write_time": 0.0,
                "compression_time": 0.0,
                "serialization_time": 0.0,
            }

        if activity == "thread-cpu":
            by_task[task_prefix]["cpu_time"] += value
        # ... (repeat for other activities)

    return {**totals, "by_task_prefix": by_task}
```

---

### Integration with MetricsCollector

**In `__enter__()`**:
```python
if self.backend.supports_fine_metrics():
    self._span_context = self.backend.create_span("coffea-processing")
    self._span_context.__enter__()
```

**In `__exit__()`**:
```python
if self._span_context:
    self._span_id = self._span_context.__exit__(exc_type, exc_val, exc_tb)
    if self._span_id:
        span_metrics = self.backend.get_span_metrics(self._span_id)
        self.fine_metrics = parse_fine_metrics(span_metrics)
```

---

## 9. Aggregation Strategy

All metrics sources are combined in `src/roastcoffea/aggregation/core.py`.

### Module Structure

**Backend-specific aggregation** - Parsing backend tracking data:
```
aggregation/backends/
├── __init__.py
├── dask.py           # parse_tracking_data() - Dask scheduler tracking → worker metrics
└── taskvine.py       # Future: TaskVine tracking → worker metrics
```

**Universal aggregation** - Executor-agnostic:
```
aggregation/
├── core.py          # Main aggregator, delegates to backends
├── workflow.py      # Throughput, rates, volumes (from Coffea report)
├── chunks.py        # Per-chunk aggregation (v0.2+)
└── efficiency.py    # CPU efficiency, speedup
```

**Test structure** (mirrors src):
```
tests/aggregation/
├── backends/
│   ├── test_dask.py
│   └── test_taskvine.py
├── test_core.py
├── test_workflow.py
├── test_chunks.py      # v0.2+
└── test_efficiency.py
```

---

### workflow_metrics.py

**Inputs**:
- Coffea report (`report` dict)
- Wall time (`t_start`, `t_end` from MetricsCollector)

**Computes**:
- `total_time_s`: Wall time or from Coffea report
- `data_read_compressed_bytes`: From Coffea report
- `data_read_uncompressed_bytes`: From Coffea report
- `compression_ratio`: Uncompressed / compressed
- `overall_rate_gbps`: Uncompressed bytes / time (in Gbps)
- `overall_rate_mbps`: In MB/s
- `event_rate_wall_khz`: Total events / wall time (kHz)
- `chunks_processed`: From Coffea report

**Function**:
```python
def compute_workflow_metrics(
    coffea_report: dict,
    t_start: float,
    t_end: float,
) -> dict:
    """Compute workflow-level metrics from Coffea report."""
    wall_time = t_end - t_start

    # Extract from Coffea report
    bytes_compressed = coffea_report.get('bytes_compressed', 0)
    bytes_uncompressed = coffea_report.get('bytes_uncompressed', 0)
    compression_ratio = bytes_uncompressed / bytes_compressed if bytes_compressed > 0 else 1.0

    # Rates
    gbps = (bytes_uncompressed / 1e9) / wall_time
    mbps = gbps * 1000

    # Events
    total_events = coffea_report.get('events_processed', 0)
    event_rate_khz = (total_events / wall_time) / 1000

    return {
        'total_time_s': wall_time,
        'data_read_compressed_bytes': bytes_compressed,
        'data_read_uncompressed_bytes': bytes_uncompressed,
        'compression_ratio': compression_ratio,
        'overall_rate_gbps': gbps,
        'overall_rate_mbps': mbps,
        'event_rate_wall_khz': event_rate_khz,
        'chunks_processed': coffea_report.get('chunks', 0),
    }
```

---

### worker_metrics.py

**Inputs**:
- Tracking data from `DaskMetricsBackend.stop_tracking()`
- Both scheduler tracking and Prometheus metrics

**Computes**:
- `time_averaged_workers`: Trapezoidal integration of worker count over time
- `peak_workers`: Max worker count observed
- `avg_memory_per_worker_gb`: Time-weighted average memory per worker
- `peak_memory_per_worker_gb`: Max memory observed across all workers
- `memory_utilization_pct`: Avg (memory used / memory limit) × 100
- `cpu_utilization_pct`: Avg CPU usage across workers over time
- `total_cores`: Sum of nthreads across all workers

**Implementation** (adapted from `metrics/worker_tracker.py`):
```python
def compute_worker_metrics(tracking_data: dict) -> dict:
    """Compute time-weighted worker metrics."""
    timeline = tracking_data['scheduler_tracking']['timeline']

    # Time-weighted worker count (trapezoidal rule)
    times = [snap['timestamp'] for snap in timeline]
    counts = [len(snap['workers']) for snap in timeline]
    time_avg_workers = np.trapz(counts, times) / (times[-1] - times[0])

    # Peak workers
    peak_workers = max(counts)

    # Memory (time-weighted per worker)
    # ... (similar logic, extract memory from snapshots, compute weighted avg)

    return {
        'time_averaged_workers': time_avg_workers,
        'peak_workers': peak_workers,
        'avg_memory_per_worker_gb': ...,
        'peak_memory_per_worker_gb': ...,
        'memory_utilization_pct': ...,
        'cpu_utilization_pct': ...,
        'total_cores': ...,
    }
```

---

### chunk_metrics.py

**Inputs**:
- List of chunk metrics (from queue consumer)

**Computes**:

**1. Aggregate stats**:
```python
def compute_chunk_aggregates(chunk_metrics: list[dict]) -> dict:
    """Aggregate statistics across all chunks."""
    times = [c['time'] for c in chunk_metrics]
    return {
        'total_chunks': len(chunk_metrics),
        'avg_time_per_chunk_s': np.mean(times),
        'median_time_per_chunk_s': np.median(times),
        'p95_time_per_chunk_s': np.percentile(times, 95),
        'max_time_per_chunk_s': np.max(times),
        'total_events': sum(c['events'] for c in chunk_metrics),
    }
```

**2. Per-file aggregation**:
```python
def aggregate_by_file(chunk_metrics: list[dict]) -> dict[str, dict]:
    """Group chunks by filename and sum timing/memory."""
    by_file = {}
    for chunk in chunk_metrics:
        fname = chunk['filename']
        if fname not in by_file:
            by_file[fname] = {
                'filename': fname,
                'total_time_s': 0.0,
                'total_events': 0,
                'chunks': 0,
                'avg_memory_delta_gb': 0.0,
            }
        by_file[fname]['total_time_s'] += chunk['time']
        by_file[fname]['total_events'] += chunk['events']
        by_file[fname]['chunks'] += 1
        by_file[fname]['avg_memory_delta_gb'] += chunk['memory_delta_gb']

    # Average memory
    for f in by_file.values():
        f['avg_memory_delta_gb'] /= f['chunks']

    return by_file
```

**3. Section aggregates** (if user used `track_section()`):
```python
def aggregate_sections(chunk_metrics: list[dict]) -> dict[str, dict]:
    """Aggregate section timings across all chunks."""
    sections = {}
    for chunk in chunk_metrics:
        for sec in chunk.get('sections', []):
            name = sec['name']
            if name not in sections:
                sections[name] = {'times': []}
            sections[name]['times'].append(sec['time'])

    return {
        name: {
            'total_time_s': sum(data['times']),
            'avg_time_s': np.mean(data['times']),
            'count': len(data['times']),
        }
        for name, data in sections.items()
    }
```

---

### efficiency_metrics.py

**Inputs**:
- Worker metrics
- Workflow metrics
- Fine metrics (if available)

**Computes**:
- `cpu_efficiency_pct`: (Total CPU time) / (Total available core-seconds) × 100
- `speedup_factor`: (Serial time estimate) / (Actual wall time)
- `io_overhead_pct`: (I/O time / Total time) × 100 (from fine metrics)
- `serialization_overhead_pct`: (Serialization time / Total time) × 100

**Implementation**:
```python
def compute_efficiency_metrics(
    worker_metrics: dict,
    workflow_metrics: dict,
    fine_metrics: dict | None,
) -> dict:
    """Compute efficiency and overhead metrics."""
    # CPU efficiency
    total_cores = worker_metrics['total_cores']
    wall_time = workflow_metrics['total_time_s']
    available_core_seconds = total_cores * wall_time

    # CPU time from fine metrics or estimate
    if fine_metrics:
        cpu_time = fine_metrics['cpu_time']
    else:
        # Estimate: assume 80% utilization as proxy
        cpu_time = worker_metrics['cpu_utilization_pct'] / 100 * available_core_seconds

    cpu_efficiency = (cpu_time / available_core_seconds) * 100

    # I/O overhead (only if fine metrics available)
    io_overhead = None
    if fine_metrics:
        total_compute_time = fine_metrics['cpu_time'] + fine_metrics['io_time']
        io_overhead = (fine_metrics['io_time'] / total_compute_time) * 100

    # Serialization overhead
    serialization_overhead = None
    if fine_metrics:
        serialization_overhead = (fine_metrics['serialization_time'] / total_compute_time) * 100

    return {
        'cpu_efficiency_pct': cpu_efficiency,
        'io_overhead_pct': io_overhead,
        'serialization_overhead_pct': serialization_overhead,
    }
```

---

### core.py - Main Aggregator

**Function**:
```python
def aggregate_all_metrics(
    coffea_report: dict,
    tracking_data: dict,
    chunk_metrics: list[dict],
    fine_metrics: dict | None,
    t_start: float,
    t_end: float,
) -> dict:
    """Combine all metrics sources into unified dict."""
    from roastcoffea.aggregation.workflow_metrics import compute_workflow_metrics
    from roastcoffea.aggregation.worker_metrics import compute_worker_metrics
    from roastcoffea.aggregation.chunk_metrics import (
        compute_chunk_aggregates,
        aggregate_by_file,
        aggregate_sections,
    )
    from roastcoffea.aggregation.efficiency_metrics import compute_efficiency_metrics

    # Compute each category
    workflow = compute_workflow_metrics(coffea_report, t_start, t_end)
    workers = compute_worker_metrics(tracking_data)
    chunks_agg = compute_chunk_aggregates(chunk_metrics)
    by_file = aggregate_by_file(chunk_metrics)
    sections = aggregate_sections(chunk_metrics)
    efficiency = compute_efficiency_metrics(workers, workflow, fine_metrics)

    # Combine into single dict
    return {
        **workflow,        # Flatten workflow metrics at top level
        **workers,         # Flatten worker metrics
        **chunks_agg,      # Flatten chunk aggregates
        **efficiency,      # Flatten efficiency metrics
        'fine_metrics': fine_metrics,        # Nested
        'by_file': by_file,                  # Nested
        'sections': sections,                # Nested
        'tracking_data': tracking_data,      # Full raw data (for advanced users)
        'chunk_metrics': chunk_metrics,      # Full raw data
    }
```

**Output structure**:
```python
{
    # Workflow-level (flat)
    'total_time_s': 123.4,
    'overall_rate_gbps': 2.5,
    'event_rate_wall_khz': 45.6,
    ...

    # Worker-level (flat)
    'time_averaged_workers': 8.3,
    'peak_workers': 12,
    'avg_memory_per_worker_gb': 3.2,
    ...

    # Chunk-level (flat aggregates)
    'total_chunks': 1234,
    'avg_time_per_chunk_s': 0.5,
    ...

    # Efficiency (flat)
    'cpu_efficiency_pct': 78.3,
    'io_overhead_pct': 15.2,
    ...

    # Nested detailed data
    'fine_metrics': {...},
    'by_file': {'file1.root': {...}, ...},
    'sections': {'jet_selection': {...}, ...},
    'tracking_data': {...},  # Full time-series
    'chunk_metrics': [{...}, ...],  # Full chunk list
}
```

---

## 10. Visualization & Export

### Structure

```
visualization/
├── plots/                # Individual plot functions
│   ├── workers.py       # Worker count timeline
│   ├── memory.py        # Memory utilization
│   ├── cpu.py           # CPU utilization
│   ├── throughput.py    # Data rates
│   ├── scaling.py       # Scaling efficiency
│   └── chunks.py        # Per-chunk breakdowns
└── dashboards/          # Full HTML dashboards
    └── main.py          # Comprehensive interactive dashboard

reporter.py              # Rich table formatters
measurements.py          # JSON save/load
export/tables.py         # HTML table export
```

---

### Static Plots (Matplotlib)

**Purpose**: Quick PNG/PDF exports for papers, slides.

**Examples**:

**workers.py**:
```python
def plot_worker_count(tracking_data: dict, output_path: Path) -> None:
    """Plot worker count over time with add/remove events."""
    timeline = tracking_data['scheduler_tracking']['timeline']
    events = tracking_data['scheduler_tracking']['events']

    times = [snap['timestamp'] for snap in timeline]
    counts = [len(snap['workers']) for snap in timeline]

    plt.figure()
    plt.plot(times, counts)

    # Mark add/remove events
    for event in events:
        color = 'green' if event['event'] == 'added' else 'red'
        plt.axvline(event['timestamp'], color=color, linestyle='--', alpha=0.5)

    plt.xlabel('Time')
    plt.ylabel('Workers')
    plt.title('Worker Count Timeline')
    plt.savefig(output_path)
    plt.close()
```

**memory.py**, **cpu.py**: Similar, plot time-series data from tracking.

**throughput.py**: Bar charts or time-series of data rates.

**scaling.py**: Scatter plot of workers vs throughput (if multiple runs available).

**chunks.py**: Histograms of chunk times, per-file bar charts.

---

### Interactive Plots (Bokeh)

**Purpose**: Explorable dashboards, hover tooltips, zoom/pan.

**Examples**:

**workers.py** (Bokeh version):
```python
from bokeh.plotting import figure, output_file, save
from bokeh.models import HoverTool

def plot_worker_count_interactive(tracking_data: dict, output_path: Path) -> None:
    """Interactive worker count timeline with hover details."""
    timeline = tracking_data['scheduler_tracking']['timeline']

    times = [snap['timestamp'] for snap in timeline]
    counts = [len(snap['workers']) for snap in timeline]

    p = figure(
        title='Worker Count Timeline',
        x_axis_label='Time',
        y_axis_label='Workers',
        x_axis_type='datetime',
    )

    source = ColumnDataSource(data=dict(times=times, counts=counts))
    p.line('times', 'counts', source=source, line_width=2)

    hover = HoverTool(tooltips=[('Time', '@times{%F %T}'), ('Workers', '@counts')])
    p.add_tools(hover)

    output_file(output_path)
    save(p)
```

**Key features**:
- Hover tooltips showing exact values
- Zoom/pan for detailed inspection
- Linked plots (selecting in one highlights in others)

---

### HTML Dashboard

**Location**: `visualization/dashboards/main.py`

**Function**:
```python
def generate_dashboard(metrics: dict, output_path: Path) -> None:
    """Generate comprehensive HTML dashboard with all metrics."""
    from bokeh.layouts import column, row, gridplot
    from bokeh.models import Div

    # Summary section (HTML)
    summary_html = f"""
    <h1>roastcoffea Metrics Dashboard</h1>
    <h2>Summary</h2>
    <ul>
        <li>Total time: {metrics['total_time_s']:.1f} s</li>
        <li>Throughput: {metrics['overall_rate_gbps']:.2f} Gbps</li>
        <li>Workers (avg): {metrics['time_averaged_workers']:.1f}</li>
        <li>CPU efficiency: {metrics['cpu_efficiency_pct']:.1f}%</li>
    </ul>
    """
    summary_div = Div(text=summary_html)

    # Interactive plots
    worker_plot = plot_worker_count_interactive(metrics['tracking_data'])
    memory_plot = plot_memory_interactive(metrics['tracking_data'])
    cpu_plot = plot_cpu_interactive(metrics['tracking_data'])
    chunks_hist = plot_chunk_histogram(metrics['chunk_metrics'])

    # Layout: summary + grid of plots
    layout = column(
        summary_div,
        row(worker_plot, memory_plot),
        row(cpu_plot, chunks_hist),
    )

    output_file(output_path)
    save(layout)
```

**Sections**:
1. **Summary**: Key metrics at a glance (HTML)
2. **Worker Timeline**: Interactive plots for count, memory, CPU
3. **Chunk Analysis**: Histograms, per-file breakdowns, outlier detection
4. **Fine Metrics**: CPU vs I/O breakdown, serialization overhead
5. **Efficiency**: Scaling analysis, bottleneck identification
6. **Raw Data**: Downloadable JSON with all metrics

---

### Rich Tables

**Location**: `reporter.py`

**Purpose**: Terminal-friendly tables for quick inspection during/after runs.

**Examples** (adapted from `metrics/reporter.py`):

```python
from rich.console import Console
from rich.table import Table

def format_throughput_table(metrics: dict) -> Table:
    """Format throughput metrics as Rich table."""
    table = Table(title="Throughput Metrics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Overall rate", f"{metrics['overall_rate_gbps']:.2f} Gbps")
    table.add_row("Event rate", f"{metrics['event_rate_wall_khz']:.1f} kHz")
    table.add_row("Compression ratio", f"{metrics['compression_ratio']:.2f}")

    return table

def format_worker_table(metrics: dict) -> Table:
    """Format worker metrics as Rich table."""
    table = Table(title="Worker Metrics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Time-averaged workers", f"{metrics['time_averaged_workers']:.1f}")
    table.add_row("Peak workers", f"{metrics['peak_workers']}")
    table.add_row("Avg memory/worker", f"{metrics['avg_memory_per_worker_gb']:.2f} GB")

    return table

def print_all_tables(metrics: dict) -> None:
    """Print all metrics tables to console."""
    console = Console()
    console.print(format_throughput_table(metrics))
    console.print(format_worker_table(metrics))
    # ... (more tables)
```

---

### HTML Table Export

**Location**: `export/tables.py`

**Purpose**: Save Rich tables as HTML for embedding in reports.

```python
from rich.console import Console

def export_tables_html(metrics: dict, output_path: Path) -> None:
    """Export all metrics tables as HTML."""
    console = Console(record=True, width=120)

    # Print all tables to console (in-memory)
    console.print(format_throughput_table(metrics))
    console.print(format_worker_table(metrics))
    # ... (all tables)

    # Export to HTML
    html = console.export_html(inline_styles=True)
    output_path.write_text(html)
```

---

### JSON Persistence

**Location**: `measurements.py`

**Purpose**: Save all metrics to JSON for later analysis, sharing.

**Function** (adapted from `metrics/measurements.py`):
```python
def save_measurements(
    metrics: dict,
    output_dir: Path,
    metadata: dict | None = None,
) -> Path:
    """Save metrics to JSON file in timestamped directory.

    Parameters
    ----------
    metrics : dict
        Full metrics dict from aggregate_all_metrics().
    output_dir : Path
        Base output directory (e.g., "benchmarks/").
    metadata : dict, optional
        Additional metadata to include (processor name, dataset, etc.).

    Returns
    -------
    Path
        Path to created directory.
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / timestamp
    output_path.mkdir(parents=True, exist_ok=True)

    # Save main metrics
    metrics_file = output_path / "metrics.json"
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)  # default=str for Timestamps

    # Save metadata
    if metadata:
        meta_file = output_path / "metadata.json"
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

    return output_path
```

---

### Output Directory Structure

**Auto-created**:
```
<output_dir>/
└── <timestamp>/              # YYYYMMDD-HHMMSS
    ├── metrics.json          # All aggregated metrics
    ├── metadata.json         # Optional user metadata
    ├── worker_timeline.json  # Full time-series (optional)
    ├── chunk_metrics.json    # Full chunk list (optional)
    ├── fine_metrics.json     # Dask span metrics (optional)
    ├── metrics_tables.html   # Rich tables as HTML
    ├── dashboard.html        # Interactive Bokeh dashboard
    └── plots/
        ├── worker_count.png
        ├── memory_util.png
        ├── cpu_util.png
        ├── chunk_histogram.png
        └── ...
```

**Config option**:
```python
MetricsConfig(
    save_raw_data=True,  # Save timeline/chunks as separate JSON files
)
```

---

## 11. Extensibility Points

### 1. Custom Instrumentation Contexts

**Purpose**: Users can create domain-specific instrumentation.

**Base class**:
```python
class BaseInstrumentationContext:
    """Base class for custom instrumentation."""

    def __init__(self, processor, name: str):
        self.processor = processor
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def record_metric(self, key: str, value: Any) -> None:
        """Store custom metric in current chunk."""
        if not hasattr(self.processor, '_current_chunk'):
            return
        if 'custom_metrics' not in self.processor._current_chunk:
            self.processor._current_chunk['custom_metrics'] = {}
        self.processor._current_chunk['custom_metrics'][key] = value
```

**Example usage**:
```python
class TrackJetCount(BaseInstrumentationContext):
    """Custom context to count jets at different cuts."""

    def __enter__(self):
        self.counts = {}
        return self

    def __exit__(self, *args):
        # Record all counts
        for cut, count in self.counts.items():
            self.record_metric(f"jets_{cut}", count)
        return False

    def count(self, name: str, jets):
        """Record jet count at this cut."""
        self.counts[name] = len(jets)

# In processor
with TrackJetCount(self, "jet_analysis") as jc:
    jets = events.Jet[events.Jet.pt > 30]
    jc.count("pt30", jets)

    jets = jets[np.abs(jets.eta) < 2.4]
    jc.count("pt30_eta24", jets)
```

**Metrics recorded**:
```python
chunk_metrics['custom_metrics'] = {
    'jets_pt30': 1234,
    'jets_pt30_eta24': 987,
}
```

---

### 2. Adding New Metrics

**Scenario**: User wants to add "disk I/O bytes" metric.

**Steps**:

1. **Collect** in appropriate location (e.g., decorator):
```python
import psutil

# In @track_metrics decorator
disk_start = psutil.disk_io_counters()
# ... execute process() ...
disk_end = psutil.disk_io_counters()
chunk_metrics['disk_read_bytes'] = disk_end.read_bytes - disk_start.read_bytes
```

2. **Aggregate** in `chunk_metrics.py`:
```python
def compute_chunk_aggregates(chunk_metrics):
    # ... existing code ...
    result['total_disk_read_bytes'] = sum(c.get('disk_read_bytes', 0) for c in chunk_metrics)
    return result
```

3. **Visualize** in new plot function:
```python
def plot_disk_io(metrics: dict, output_path: Path):
    # Bar chart of disk I/O per file
    ...
```

4. **Document** in METRICS_REFERENCE.md.

---

### 3. Custom Backends

**Scenario**: User wants to add TaskVine support.

**Steps**:

1. **Implement backend**:
```python
# src/roastcoffea/backends/taskvine.py
from roastcoffea.backends.base import AbstractMetricsBackend

class TaskVineMetricsBackend(AbstractMetricsBackend):
    def __init__(self, manager):
        self.manager = manager

    def start_tracking(self, interval: float):
        # Poll TaskVine manager for worker stats
        ...

    def stop_tracking(self):
        # Return collected data in same format as DaskMetricsBackend
        ...

    def supports_fine_metrics(self):
        # TaskVine doesn't have Spans equivalent
        return False
```

2. **Register in factory**:
```python
# In backends/__init__.py
def create_backend(executor):
    if hasattr(executor, 'client'):  # Dask
        ...
    elif isinstance(executor, TaskVineExecutor):
        from roastcoffea.backends.taskvine import TaskVineMetricsBackend
        return TaskVineMetricsBackend(executor.manager)
```

3. **Test** with TaskVine workflows.

---

### 4. Custom Plots

**Scenario**: User wants heatmap of chunk time vs file.

**Steps**:

1. **Create plot function**:
```python
# visualization/plots/heatmap.py
def plot_chunk_time_heatmap(chunk_metrics: list[dict], output_path: Path):
    import seaborn as sns

    # Prepare data
    df = pd.DataFrame(chunk_metrics)
    pivot = df.pivot_table(values='time', index='filename', columns='chunk_id')

    # Heatmap
    sns.heatmap(pivot, cmap='viridis')
    plt.savefig(output_path)
```

2. **Call from dashboard** or standalone:
```python
from roastcoffea.visualization.plots.heatmap import plot_chunk_time_heatmap

with MetricsCollector(...) as mc:
    ...

plot_chunk_time_heatmap(mc.chunk_metrics, "heatmap.png")
```

---

## 12. Error Handling Strategy

### Guiding Principles

1. **Graceful degradation**: Missing data sources should not crash collection
2. **Partial results are valuable**: Even if 10% of chunks fail to report metrics, 90% is still useful
3. **Fail separately**: Errors in visualization shouldn't affect data collection
4. **Log, don't raise**: Warn users about issues, but continue

---

### Scenarios & Responses

| Scenario | Response | Rationale |
|----------|----------|-----------|
| Worker dies mid-chunk | Drop that chunk's metrics | Acceptable loss; job continues |
| Queue is full | Drop incoming chunk metrics | Prevent backpressure on workers |
| Client dies before draining queue | Metrics lost | Client death is catastrophic anyway |
| Scheduler restarts | Metrics collection fails | Scheduler restart is rare, catastrophic |
| Prometheus endpoints unreachable | Fall back to in-process scraping | Ensures we still get worker metrics |
| In-process scraping fails on a worker | Skip that worker for this tick | Other workers still contribute |
| Span API not available (old Dask) | `supports_fine_metrics()` returns False | No fine metrics, but rest works |
| Coffea report not provided | Use wall time, estimate data volume | Degraded but functional |
| Aggregation function errors | Catch, log warning, return partial metrics | Some metrics better than none |
| Plot generation fails | Catch, log warning, skip that plot | Other plots still generated |

---

### Implementation

**In decorator** (chunk streaming):
```python
try:
    self._metrics_queue.put_nowait(chunk_metrics)
except QueueFull:
    # Log once per N failures to avoid spam
    if not hasattr(self, '_queue_full_warned'):
        warnings.warn("[roastcoffea] Metrics queue full, dropping metrics")
        self._queue_full_warned = True
except Exception as e:
    # Unexpected error - log but don't crash
    warnings.warn(f"[roastcoffea] Failed to send chunk metrics: {e}")
```

**In aggregation**:
```python
def aggregate_all_metrics(...):
    metrics = {}

    # Try each aggregation step independently
    try:
        metrics.update(compute_workflow_metrics(...))
    except Exception as e:
        warnings.warn(f"[roastcoffea] Workflow metrics failed: {e}")

    try:
        metrics.update(compute_worker_metrics(...))
    except Exception as e:
        warnings.warn(f"[roastcoffea] Worker metrics failed: {e}")

    # ... (repeat for each source)

    return metrics  # Partial metrics better than nothing
```

**In visualization**:
```python
def generate_dashboard(metrics, output_path):
    plots = []

    for plot_func, name in [
        (plot_worker_count, "worker_count"),
        (plot_memory, "memory"),
        # ...
    ]:
        try:
            plots.append(plot_func(metrics))
        except Exception as e:
            warnings.warn(f"[roastcoffea] Plot {name} failed: {e}")
            continue

    # Save dashboard with whatever plots succeeded
    save_dashboard(plots, output_path)
```

---

### User-Facing Errors

**When we SHOULD raise**:
- `create_backend(executor)` fails: User needs to know executor isn't supported
- `MetricsCollector` initialized with invalid config: User needs to fix config
- Output directory not writable: User needs to fix permissions

**Example**:
```python
def create_backend(executor):
    if hasattr(executor, 'client'):
        return DaskMetricsBackend(executor.client)

    raise ValueError(
        f"Unsupported executor type: {type(executor)}. "
        f"roastcoffea currently supports: DaskExecutor."
    )
```

---

## 13. Performance Overhead

### Expected Costs

| Component | CPU Overhead | Memory Overhead | Network Overhead |
|-----------|--------------|-----------------|------------------|
| Scheduler tracking | ~0.1% (1 Hz sampling) | ~10 MB (time-series) | None (scheduler-side) |
| Prometheus scraping | ~0.5% (HTTP requests) | ~5 MB (snapshots) | ~1 MB/min (HTTP) or None (in-process) |
| Dask Spans | ~1% (activity tracking) | ~5 MB (aggregated dict) | None (scheduler-side) |
| Chunk tracking | ~1-2% per chunk (timing/memory) | Negligible (streamed) | ~1 KB/chunk (~83 KB/s for 50k chunks in 10 min) |
| User instrumentation | Depends on usage | Negligible (in chunk) | None (nested in chunk) |

**Total estimated overhead**: **2-4% CPU, ~20 MB memory, ~100 KB/s network**

**Is this acceptable?** For analysis jobs that run for minutes to hours, yes. For sub-second jobs, users should disable metrics.

---

### Config Options to Reduce Overhead

```python
MetricsConfig(
    enable=True,                    # Master switch (False = zero overhead)

    # Sampling frequencies
    worker_tracking_interval=1.0,   # Increase to 5.0 for less frequent sampling
    prometheus_interval=1.0,        # Increase to 5.0

    # Data sources
    track_workers=True,             # Disable if only interested in chunk metrics
    track_prometheus=True,          # Disable if scheduler tracking sufficient
    track_fine_metrics=True,        # Disable Spans (minimal overhead anyway)
    track_chunks=True,              # Disable per-chunk tracking

    # Detail level
    chunk_sections=True,            # Disable track_section() support
    chunk_memory=True,              # Disable memory tracking in chunks

    # Output
    save_measurements=True,         # Disable JSON saving
    generate_plots=True,            # Disable plot generation (most expensive)
    generate_html_tables=True,
    generate_dashboard=True,
)
```

**Minimal overhead mode**:
```python
MetricsConfig(
    track_workers=True,
    worker_tracking_interval=5.0,
    track_chunks=False,
    save_measurements=False,
    generate_plots=False,
)
```
Overhead: <1%

---

### Benchmarks (To Be Added)

**TODO**: Run benchmarks comparing:
1. No metrics
2. Minimal metrics
3. Full metrics
4. Full metrics + plots

Measure:
- Wall time overhead
- Peak memory
- Network traffic

Report in docs/benchmarks.md.

---

## 14. Thread Safety

### GIL Considerations

**Python GIL (Global Interpreter Lock)**:
- Only one thread executes Python bytecode at a time
- C extensions (NumPy, psutil) can release GIL

**Implication**: Most Python-level operations are effectively serialized. Thread safety concerns arise when:
1. Multiple threads access shared mutable state
2. Operations are not atomic (read-modify-write)

---

### Concurrent Access Patterns

| Location | Pattern | Thread-Safe? | Solution |
|----------|---------|--------------|----------|
| `distributed.Queue.put()` | Worker threads → queue | ✅ Yes | Dask's Queue is thread-safe |
| `_chunk_metrics` list (client) | Consumer thread appends, main thread reads | ⚠️ No | Use `threading.Lock` |
| `_current_chunk` dict (worker) | Single-threaded per processor instance | ✅ Yes | No locking needed |
| Decorator state | `_chunk_counter` increments | ⚠️ No | Use `threading.Lock` if processor shared |

---

### Lock Usage

**In MetricsCollector** (client-side):
```python
class MetricsCollector:
    def __init__(self, ...):
        self._chunk_metrics = []
        self._lock = threading.Lock()

    def _consume_queue(self):
        """Background thread: drain queue."""
        while not self._stop_flag.is_set():
            chunk_data = self._metrics_queue.get(timeout=1.0)
            with self._lock:
                self._chunk_metrics.append(chunk_data)

    def __exit__(self, ...):
        # Stop consumer thread
        self._stop_flag.set()
        self._queue_thread.join()

        # Safe to read now (consumer stopped)
        with self._lock:
            final_chunks = list(self._chunk_metrics)
```

**In decorator** (worker-side):

Processors are single-threaded per instance in Coffea's model. Each chunk gets a fresh processor or reuses one serially. **No locking needed** unless user explicitly shares processor across threads (rare).

If needed:
```python
def track_metrics(func):
    @wraps(func)
    def wrapper(self, events):
        # Atomic increment (with lock if processor shared)
        if not hasattr(self, '_chunk_counter_lock'):
            self._chunk_counter_lock = threading.Lock()

        with self._chunk_counter_lock:
            if not hasattr(self, '_chunk_counter'):
                self._chunk_counter = 0
            chunk_id = self._chunk_counter
            self._chunk_counter += 1

        # ... rest of decorator ...
```

**Decision**: Start without lock in decorator (simpler, 99% use case). Document that shared processors need manual synchronization.

---

### Lock-Free Designs

**Preferred approach**: Avoid shared mutable state when possible.

**Example**: Instead of shared list, use message passing (Queue).

**Already done**: Chunk metrics use Queue (thread-safe by design, no explicit locks needed).

---

## 15. Testing Strategy

### Test Pyramid

```
            /\
           /  \
          / E2E \       (Few, slow, high-value)
         /------\
        /        \
       /  Integ.  \     (Moderate, medium speed)
      /----------\
     /            \
    /   Unit Tests \    (Many, fast, focused)
   /----------------\
```

**Ratio**: ~60% unit, ~30% integration, ~10% E2E

---

### Unit Tests

**Purpose**: Test individual functions/classes in isolation.

**Location**: `tests/test_*.py`

**Examples**:

**test_backends.py**:
```python
def test_abstract_backend_interface():
    """AbstractMetricsBackend is properly abstract."""
    with pytest.raises(TypeError):
        AbstractMetricsBackend()  # Can't instantiate

def test_backend_factory_dask(local_cluster):
    """Factory creates DaskMetricsBackend for DaskExecutor."""
    from coffea.processor import DaskExecutor
    executor = DaskExecutor(client=local_cluster)
    backend = create_backend(executor)
    assert isinstance(backend, DaskMetricsBackend)

def test_backend_factory_unsupported():
    """Factory raises for unsupported executor."""
    class FakeExecutor:
        pass

    with pytest.raises(ValueError, match="Unsupported executor"):
        create_backend(FakeExecutor())
```

**test_fine_metrics.py**:
```python
def test_parse_fine_metrics_empty():
    """Empty span metrics returns zeros."""
    result = parse_fine_metrics({})
    assert result['cpu_time'] == 0.0
    assert result['io_time'] == 0.0

def test_parse_fine_metrics_basic():
    """Parse CPU and I/O time."""
    span_metrics = {
        ('execute', 'task-1', 'thread-cpu', 'seconds'): 10.0,
        ('execute', 'task-1', 'thread-noncpu', 'seconds'): 2.0,
    }
    result = parse_fine_metrics(span_metrics)
    assert result['cpu_time'] == 10.0
    assert result['io_time'] == 2.0
```

**test_aggregation.py**:
```python
def test_aggregate_by_file():
    """Chunks are correctly grouped by filename."""
    chunks = [
        {'filename': 'a.root', 'time': 1.0, 'events': 100},
        {'filename': 'a.root', 'time': 1.5, 'events': 150},
        {'filename': 'b.root', 'time': 2.0, 'events': 200},
    ]
    result = aggregate_by_file(chunks)
    assert result['a.root']['total_time_s'] == 2.5
    assert result['a.root']['total_events'] == 250
    assert result['a.root']['chunks'] == 2
```

**Mocking**:
- Use `unittest.mock` to mock Dask client, scheduler, workers
- Use fixtures to provide test data (see `conftest.py`)

---

### Integration Tests

**Purpose**: Test component interactions (e.g., backend + aggregation).

**Examples**:

**test_collector_integration.py**:
```python
def test_collector_full_workflow(local_cluster, sample_processor, tmp_output_dir):
    """MetricsCollector full workflow without actual Coffea run."""
    from coffea.processor import DaskExecutor

    executor = DaskExecutor(client=local_cluster)

    with MetricsCollector(executor, output_dir=tmp_output_dir) as mc:
        # Simulate workflow (don't run real Coffea)
        time.sleep(2)  # Simulate 2-second job

        # Manually inject data
        mc.set_coffea_report({
            'bytes_compressed': 1e9,
            'bytes_uncompressed': 5e9,
            'events_processed': 100000,
            'chunks': 50,
        })

    # Verify metrics aggregated
    assert mc.metrics is not None
    assert 'overall_rate_gbps' in mc.metrics
    assert 'time_averaged_workers' in mc.metrics
```

**test_streaming.py**:
```python
@pytest.mark.slow
def test_chunk_streaming_many_chunks(local_cluster):
    """Streaming handles 1000 chunks without memory blow-up."""
    # Create processor with decorator
    # Simulate 1000 chunk submissions
    # Verify all metrics received
    # Measure memory usage
    ...
```

---

### End-to-End Tests

**Purpose**: Full workflow with real Coffea processor and data.

**Location**: `tests/test_realworld.py`

**Examples**:

**test_realworld.py**:
```python
@pytest.fixture(scope="module")
def opendata_file():
    """Production open data file for testing."""
    return (
        "root://eospublic.cern.ch//eos/opendata/cms/mc/"
        "RunIISummer20UL16NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/"
        "NANOAODSIM/106X_mcRun2_asymptotic_v17-v1/120000/"
        "08FCB2ED-176B-064B-85AB-37B898773B98.root"
    )

@pytest.mark.e2e
@pytest.mark.slow
def test_full_workflow_opendata(local_cluster, opendata_file, tmp_output_dir):
    """Complete workflow with real NanoAOD file."""
    from coffea import processor
    from coffea.nanoevents import NanoAODSchema

    class TestProcessor(processor.ProcessorABC):
        @track_metrics
        def process(self, events):
            with track_section(self, "jet_selection"):
                jets = events.Jet[events.Jet.pt > 30]
            return {"njets": len(jets)}

        def postprocess(self, acc):
            return acc

    fileset = {"TTbar": {"files": {opendata_file: "Events"}, "metadata": {"process": "ttbar"}}}

    executor = DaskExecutor(client=local_cluster)

    with MetricsCollector(executor, output_dir=tmp_output_dir) as mc:
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            savemetrics=True,
        )
        output, report = runner(fileset, TestProcessor())
        mc.set_coffea_report(report)
        mc.set_chunk_metrics(TestProcessor()._chunk_metrics)

    # Verify metrics
    assert mc.metrics['chunks_processed'] > 0
    assert mc.metrics['overall_rate_gbps'] > 0
    assert mc.metrics['time_averaged_workers'] > 0

    # Verify outputs created
    assert (tmp_output_dir / "metrics.json").exists()
    assert (tmp_output_dir / "dashboard.html").exists()
```

**Note**: E2E tests are slow (minutes per test). Run with `pytest -m e2e` separately from main test suite.

---

### Stress Tests

**Purpose**: Validate performance with extreme workloads.

**Examples**:

**test_stress.py**:
```python
@pytest.mark.stress
def test_10k_chunks(local_cluster):
    """Handle 10,000 chunks without memory issues."""
    # Processor that generates 10k tiny chunks
    # Verify streaming keeps memory bounded
    # Verify all chunks accounted for
    ...

@pytest.mark.stress
def test_adaptive_cluster_scaling(adaptive_cluster):
    """Metrics collection survives worker add/remove."""
    # Start with 2 workers
    # Scale to 10 workers mid-job
    # Scale back to 2 workers
    # Verify metrics timeline reflects changes
    ...
```

---

### Mocking Strategies

**Dask client/scheduler**:
```python
from unittest.mock import Mock, MagicMock

@pytest.fixture
def mock_client():
    client = Mock()
    client.scheduler_info.return_value = {
        'workers': {
            'tcp://worker1': {'nthreads': 4, 'memory_limit': 4e9},
            'tcp://worker2': {'nthreads': 4, 'memory_limit': 4e9},
        }
    }
    return client
```

**Dask spans** (for testing without real Dask):
```python
@pytest.fixture
def mock_span():
    span_ctx = MagicMock()
    span_ctx.__enter__.return_value = 'span-id-123'
    span_ctx.__exit__.return_value = None
    return span_ctx
```

---

### Coverage Goals

- **Overall**: >80%
- **Critical paths** (MetricsCollector, backends, aggregation): >90%
- **Visualization** (plots): >60% (harder to test, more subjective)

**Run coverage**:
```bash
pytest --cov=roastcoffea --cov-report=html
```

---

## 16. Implementation Phases

### Phase 0: Project Setup ✅

**Goal**: Bootstrap package structure.

**Tasks**:
- [x] Create projectify config
- [x] Generate structure with projectify
- [x] Copy files to repo (pyproject.toml, .gitignore, etc.)
- [x] Create src/roastcoffea/ structure
- [x] Create tests/conftest.py with fixtures
- [x] Implement AbstractMetricsBackend

**Dependencies**: None

**Deliverable**: Empty package with structure, tests run (but no tests yet).

---

### Phase 1: Test Infrastructure (TDD) ✅

**Goal**: Write failing tests for all features.

**Tasks**:
- [x] Write test_backends.py (backend tests)
- [x] Write test_fine_metrics.py (Spans parsing tests)
- [x] Write test_decorator.py (@track_metrics tests)
- [x] Write test_instrumentation.py (track_section/memory tests)
- [x] Write test_collector.py (MetricsCollector tests)
- [x] Write test_aggregation.py (all aggregation modules)
- [x] Write test_export.py (tables, JSON)

**Dependencies**: Phase 0

**Deliverable**: Test suite that fails (features not implemented).

---

### Phase 2: Backend Architecture ✅ (Mostly Done)

**Goal**: Implement metrics backends.

**Tasks**:
- [x] Implement DaskMetricsBackend (81% coverage)
  - [x] Scheduler-side tracking (adapted from `metrics/worker_tracker.py`)
  - [ ] Prometheus scraping (not implemented)
  - [x] Worker lifecycle events (add/remove)
  - [x] Dask Spans integration
- [x] Implement backend factory (create_backend)
- [x] Make backend tests pass (tests green)

**Dependencies**: Phase 1

**Deliverable**: Functional Dask backend, tests green.

---

### Phase 3: Fine Metrics Parsing ✅

**Goal**: Parse Dask Spans metrics.

**Tasks**:
- [x] Implement parse_fine_metrics() in aggregation/fine_metrics.py (96% coverage)
- [x] Handle all activity types (cpu, io, disk, compression, serialization)
- [x] Per-task-prefix breakdown
- [x] Make fine_metrics tests pass

**Dependencies**: Phase 2 (needs Spans integration)

**Deliverable**: Fine metrics parser, tests green.

---

### Phase 4: Decorator & Instrumentation ✅

**Goal**: Per-chunk tracking with streaming.

**Tasks**:
- [x] Implement @track_metrics decorator (100% coverage)
  - [x] Timing, memory measurement
  - [x] Metadata extraction (filename, dataset)
  - [ ] Queue streaming (not implemented - using list accumulator instead)
  - [ ] Context manager labeling (not implemented)
- [x] Implement track_time() context manager (100% coverage)
- [x] Implement track_memory() context manager (100% coverage)
- [x] Make decorator/instrumentation tests pass

**Dependencies**: Phase 2 (needs backend for queue setup)

**Deliverable**: Decorator and instrumentation working, tests green.

---

### Phase 5: MetricsCollector & Aggregation ⚠️ (Partial)

**Goal**: Main entry point, combine all sources.

**Tasks**:
- [ ] Implement MetricsCollector context manager (19% coverage - major gaps!)
  - [ ] Backend creation (incomplete)
  - [ ] Queue setup and consumer thread (not implemented)
  - [ ] Span lifecycle (incomplete)
  - [ ] Config handling (incomplete)
- [x] Implement all aggregation modules:
  - [x] workflow_metrics.py (96% coverage)
  - [x] chunk.py (100% coverage)
  - [x] efficiency_metrics.py (100% coverage)
  - [x] core.py (89% coverage)
- [x] Implement measurements.py (90% coverage - JSON save/load)
- [ ] Make collector tests pass (many paths untested)

**Dependencies**: Phases 2, 3, 4

**Deliverable**: Full metrics collection pipeline, tests green.

**Status**: Aggregation works, but MetricsCollector needs major work.

---

### Phase 6: Reporting & Visualization ⚠️ (Partial)

**Goal**: Generate plots, tables, dashboards.

**Tasks**:
- [x] Implement reporter.py (98% coverage - Rich tables)
  - [x] Throughput table
  - [x] Worker table
  - [x] Resources table
  - [x] Timing table
  - [x] Fine metrics table
  - [x] Chunk metrics table
- [x] Implement static plots (matplotlib) - basic versions:
  - [x] workers.py (100% coverage)
  - [x] memory.py (95% coverage)
  - [x] cpu.py (100% coverage)
  - [x] throughput.py (100% coverage)
  - [x] scaling.py (100% coverage)
  - [ ] chunks.py (0% - stub only)
  - [ ] per_task.py (7% - mostly not implemented)
- [ ] Implement interactive plots (bokeh) - not implemented
- [ ] Implement HTML dashboard (0% - stub only)
- [ ] Implement HTML table export (0% - stub only)
- [x] Make reporter/visualization tests pass (for implemented features)

**Dependencies**: Phase 5 (needs aggregated metrics)

**Deliverable**: Full reporting suite, tests green.

**Status**: Rich tables and matplotlib plots work. Bokeh, dashboards, and HTML export not done.

---

### Phase 7: Documentation ⚠️ (Partial)

**Goal**: User-facing docs.

**Tasks**:
- [ ] Update README.md (needs work)
- [x] Write API documentation (Sphinx autodoc) - **COMPLETE**
- [x] Write usage tutorial (docs/tutorials.md - exists)
- [x] Write advanced guide (docs/advanced.md - exists)
- [x] Write architecture docs (docs/architecture.md - exists)
- [x] Write concepts docs (docs/concepts.md - complete with accurate I/O descriptions)
- [x] Write metrics reference (docs/metrics_reference.md - complete with detailed TOC and dropdowns)
- [x] Write introduction (docs/introduction.md - exists)
- [x] Write contributing guide (docs/contributing.md - exists)
- [x] Write quickstart (docs/quickstart.md - exists)
- [x] Create example notebooks (basic_usage.ipynb exists)
- [ ] Run pre-commit on all files

**Dependencies**: Phase 6 (needs working code to document)

**Deliverable**: Comprehensive docs.

**Status**: User docs complete with improvements. API reference created with Sphinx autodoc.

---

### Phase 8: Integration & E2E Tests ❌ (Not Done)

**Goal**: Real-world validation.

**Tasks**:
- [ ] Write E2E test with open data file (test_e2e.py exists but tests are deselected/skipped)
- [ ] Write stress tests (10k chunks, adaptive cluster)
- [ ] Run full test suite, verify all pass
- [ ] Measure and document performance overhead

**Dependencies**: Phases 5, 6 (needs full pipeline)

**Deliverable**: E2E tests pass, benchmarks documented.

**Status**: E2E tests exist but are all deselected - not running in test suite.

---

### Phase 9: Package & Publish ❌ (Not Done)

**Goal**: Release to PyPI.

**Tasks**:
- [ ] Build package (`hatch build`)
- [ ] Test local install (`pip install -e .`)
- [ ] Verify imports, basic usage
- [ ] Publish to test PyPI
- [ ] Verify install from test PyPI
- [ ] Publish to PyPI
- [ ] Create GitHub release

**Dependencies**: Phase 8 (all tests pass)

**Deliverable**: roastcoffea v0.1.0 on PyPI.

**Status**: Not started - blocked by Phase 5 & 8 completion.

---

### Dependency Graph

```
Phase 0 (Setup)
    ↓
Phase 1 (Tests - TDD)
    ↓
Phase 2 (Backends)
    ├─→ Phase 3 (Fine Metrics)
    └─→ Phase 4 (Decorator)
         ↓
    Phase 5 (Collector & Aggregation)
         ↓
    Phase 6 (Visualization)
         ↓
    Phase 7 (Docs)
         ↓
    Phase 8 (E2E Tests)
         ↓
    Phase 9 (Publish)
```

---

## 17. Trade-offs & Rationale

### 1. Streaming vs Buffering (Chunk Metrics)

**Decision**: Stream chunk metrics via `distributed.Queue`.

**Alternatives considered**:
1. **Buffer in processor instance**: Simple but causes memory blow-up (50 MB for 50k chunks).
2. **Aggregate on workers, send summary**: Loses per-chunk detail (main value of decorator).
3. **Write to disk on workers, collect at end**: I/O overhead, filesystem requirements.

**Rationale**: Streaming balances memory (constant), detail (full), and overhead (negligible network cost).

---

### 2. Dual Tracking (Scheduler + Prometheus)

**Decision**: Implement both our tracking AND Prometheus.

**Alternatives considered**:
1. **Only our tracking**: Less code, but miss Prometheus's battle-tested insights.
2. **Only Prometheus**: Simpler, but lose control over sampling, may not work in all networks.

**Rationale**: Redundancy is valuable. Cross-validation catches bugs. Complementary data (our tracking: time-series; Prometheus: internal state).

**Cost**: Minimal (both are sampling-based, ~0.5% overhead each).

---

### 3. Bokeh vs Plotly for Interactive Viz

**Decision**: Use Bokeh.

**Alternatives considered**:
1. **Plotly**: More popular, easier to learn, better docs.
2. **Bokeh**: More customizable, better for large data, native Python (no JS dependency for static exports).

**Rationale**:
- Mo's preference (important for maintainability)
- Bokeh handles large datasets better (10k+ points in plots)
- No JavaScript build step (simpler packaging)

**Trade-off**: Steeper learning curve for contributors, smaller community.

---

### 4. Breaking from intccms

**Decision**: No backwards compatibility with intccms metrics package.

**Alternatives considered**:
1. **Maintain compatibility**: Easier migration for Mo, but constrains design.
2. **Gradual migration**: Dual APIs (old + new), complexity.

**Rationale**:
- Mo is the only user, version frozen in intccms.
- Fresh start allows better design (streaming, backends, etc.).
- Standalone package with different goals (general-purpose vs intccms-specific).

**Trade-off**: Mo must update intccms to use roastcoffea (one-time cost).

---

### 5. TDD (Tests-First) Approach

**Decision**: Write tests before implementation.

**Alternatives considered**:
1. **Implementation-first**: Faster initial progress, but risk of untested code.
2. **Hybrid**: Implement + test together, but easy to skip tests under time pressure.

**Rationale**:
- CLAUDE.md mandates comprehensive testing (unit, integration, E2E).
- TDD ensures we think about interfaces before implementation.
- Failing tests drive implementation (clear goal).

**Trade-off**: Slower initial progress (write tests that fail), but higher quality end result.

---

### 6. Config as Dict (Not Pydantic)

**Decision**: Use plain dict with validation function, not Pydantic model.

**Alternatives considered**:
1. **Pydantic**: Type-safe, auto-validation, great for complex schemas.
2. **dataclasses**: Simpler than Pydantic, but less validation.
3. **Plain dict**: Simplest, most flexible.

**Rationale**:
- Config is simple (10-15 keys, all primitives).
- Pydantic adds dependency weight (import overhead, version pinning).
- Users familiar with dicts, no learning curve.

**Implementation**:
```python
def merge_with_defaults(user_config: dict | None) -> dict:
    """Merge user config with defaults, validate types."""
    defaults = {
        'enable': True,
        'track_workers': True,
        'worker_tracking_interval': 1.0,
        # ...
    }
    merged = {**defaults, **(user_config or {})}

    # Validate
    if not isinstance(merged['enable'], bool):
        raise TypeError("'enable' must be bool")
    # ... (more validation)

    return merged
```

**Trade-off**: Manual validation code vs automatic with Pydantic.

---

## 18. Future Enhancements

### Short-term (Post v0.1)

1. **File-level error tracking**:
   - Detect failed files, retries
   - Report error rates per file
   - Useful for debugging problematic files

2. **Network I/O metrics**:
   - Bandwidth usage (if measurable)
   - Cache hit rates (if ROOT cache used)
   - Requires OS-level monitoring (psutil)

3. **Prometheus export**:
   - Optional config flag: `prometheus_export=True`
   - Export aggregate metrics to Prometheus endpoint
   - Enable Grafana dashboards for real-time monitoring
   - Complementary to post-hoc analysis (different use case)

4. **Performance regression detection**:
   - Compare metrics across runs
   - Alert on degradation (e.g., throughput drops 20%)
   - Useful for CI, catching performance regressions

---

### Medium-term (v0.2+)

1. **TaskVine backend**:
   - Implement TaskVineMetricsBackend
   - Similar structure to Dask backend
   - No Spans equivalent, so fine metrics unavailable
   - Scheduler-side tracking still works

2. **Dask task stream integration**:
   - Complement Spans with task timeline
   - Identify scheduling delays, task dependencies
   - Heavier overhead, opt-in only

3. **Automatic anomaly detection**:
   - Flag outlier chunks (10x slower than median)
   - Flag outlier files
   - Suggest optimizations (e.g., "Serialization is 30% of time, consider optimizing data structures")

4. **Cost analysis** (cloud deployments):
   - Estimate cluster cost based on runtime, resources
   - Useful for budget-conscious users on AWS/GCP

---

### Long-term (v1.0+)

1. **Multi-run comparison**:
   - Store metrics in database (SQLite or cloud)
   - Compare metrics across runs, datasets, processors
   - Trend analysis over time

2. **Recommendation engine**:
   - Analyze metrics, suggest optimizations
   - "Your I/O is 40% of time. Consider using columnar caching."
   - "Your memory is 90% utilized. Increase chunk size or worker memory."

3. **Real-time dashboard** (optional web server):
   - Flask/FastAPI server to view metrics live
   - WebSocket updates while job runs
   - Distinct from static dashboard (post-hoc)

4. **Integration with other tools**:
   - Export to MLflow for experiment tracking
   - Export to Weights & Biases
   - Export to custom databases

---

## Summary & Next Steps

This design document provides a comprehensive blueprint for roastcoffea v0.1. Key takeaways:

1. **Seven data sources**: Scheduler tracking, Prometheus, Dask Spans, chunk tracking, user instrumentation, Coffea reports, workflow wrapper.

2. **Streaming architecture**: Chunk metrics streamed via `distributed.Queue` to avoid memory blow-up (critical for 10-50k chunks).

3. **Dual tracking**: Our implementation + Prometheus for redundancy and cross-validation.

4. **Pluggable backends**: Abstract interface supports Dask (now) and TaskVine (future).

5. **TDD approach**: Write tests first, implement to pass tests.

6. **Comprehensive visualization**: Static plots (matplotlib), interactive plots (bokeh), HTML dashboard.

7. **Graceful degradation**: Partial metrics better than no metrics. Don't crash on errors.

8. **9 implementation phases**: Setup → Tests → Backends → Fine Metrics → Decorator → Collector → Visualization → Docs → E2E → Publish.

---

### Immediate Next Steps

1. **Review this design document** with Mo. Confirm architecture decisions.

2. **Begin Phase 1** (Test Infrastructure): Write failing tests for all features.

3. **Iterate on design** as implementation reveals issues.

4. **Update design doc** when decisions change (living document).

---

## Current Status (as of 2025-11-25)

### Documentation Progress

#### Completed ✅

1. **Version/Future References Cleanup**
   - Removed all "v0.2+", "Future", "Planned" mentions from documentation
   - Removed version metadata from metrics_reference.md header
   - Changed all future-tense language to present-tense where features exist
   - Updated TaskVine references (replaced Spark/Ray examples)

2. **Misleading I/O Metrics Descriptions Fixed**
   - **thread-noncpu**: Now correctly described as "Difference between wall clock time and CPU time (typically I/O time, GPU time, CPU contention, or GIL contention)"
   - **memory-read**: Now correctly described as "Data read from worker memory (tracked by Dask) - does NOT measure file I/O"
   - **disk-read/write**: Now correctly described as "Data read/written from disk due to memory spilling (tracked by Dask) - does NOT measure file I/O"
   - Removed misleading ROOT-specific interpretations
   - Removed unreliable comparisons between different byte metrics
   - Updated all instances in metrics_reference.md including:
     - Raw Activity Metrics table (lines 190-195)
     - Derived Fine Metrics table (lines 211-221)
     - Efficiency Metrics section (lines 347-388)
     - Assumptions & Limitations section (lines 411-420)

3. **Complete Documentation Files Created**
   - introduction.md: Full introduction with overview, features, use cases
   - contributing.md: Complete developer guide with setup, testing, PR guidelines

#### In Progress 🔄

1. **Documentation Structure Improvements**
   - Need to add subsections with dropdowns to concepts.md
   - Need to restructure metrics_reference.md with better subsections
   - Need to create searchable tables for users

#### Pending ⏳

1. **API Reference Documentation**
   - Needs to be auto-generated using Sphinx autodoc
   - Should include all public APIs from src/roastcoffea/

### Key Clarifications from Mo

**Accurate Metric Definitions** (to prevent future confusion):

1. **disk-read**: "measures spillages (how much being spilled to disk or read from disk because memory cannot accommodate for data)"

2. **memory-read**: "only about how much was read from the memory on the worker"

3. **thread-noncpu**: "Difference between wall clock time and CPU time spent by tasks while running on workers. This is typically I/O time, GPU time, CPU contention, or GIL contention" (based on Dask docs)

4. **Important principle**: "no need to say what is more reliable than what. we only say facts of what everything is."

### Notes for Future Work

- User documentation should focus on subsections and searchability
- Avoid version references and future-tense language in user-facing docs
- Always use accurate, factual descriptions without comparisons or interpretations
- Use TaskVine as the example alternative backend (not Spark/Ray)

---

**End of Design Document**

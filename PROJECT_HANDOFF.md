# coffea-metrics Project Handoff

**Date**: 2025-11-07
**From**: Claude (intccms project session)
**To**: Claude (new coffea-metrics project session)
**Status**: Planning complete, ready for implementation

---

## Project Summary

We are creating **coffea-metrics**, a standalone Python package for comprehensive performance monitoring of Coffea-based High Energy Physics analysis workflows. This package will be published to PyPI and work with any Coffea workflow, not just intccms.

---

## What Mo Has Already Done

1. **Copied metrics code from intccms**:
   - Source: `intccms/src/intccms/metrics/`
   - Destination: Your new repo's `src/coffea_metrics/`
   - **Excluded**: `inspector/` subdirectory (not needed)
   - **Included**: All other modules (worker_tracker, collector, reporter, visualization, measurements)

2. **Copied METRICS_REFERENCE.md**:
   - Complete documentation of all metrics
   - Located at repo root
   - This is your primary reference for understanding what metrics exist

---

## Current State of Copied Code

### Existing Modules (from intccms)

```
src/coffea_metrics/
├── __init__.py              # Public API exports
├── worker_tracker.py        # Scheduler-based worker/memory tracking
├── collector.py             # Metrics aggregation and calculation
├── reporter.py              # Rich table formatting
├── visualization.py         # Matplotlib plot generation
└── measurements.py          # Save/load metrics to/from JSON
```

### What Works (No Changes Needed)

These modules are functional and tested:
- **worker_tracker.py**: Scheduler sampling, time-weighted averaging
- **measurements.py**: JSON persistence
- **reporter.py**: Rich table generation
- **visualization.py**: 5 plot functions (worker count, memory, CPU, scaling, dashboard)

### What Needs Refactoring

1. **collector.py**: Currently tightly coupled to manual workflow
   - Needs to be wrapped in `MetricsCollector` context manager
   - Add fine metrics integration

2. **Dependencies**: Remove intccms-specific imports
   - Change `from intccms.metrics` → `from coffea_metrics`
   - Remove dependency on `OutputDirectoryManager` (intccms-specific)
   - Make output path management internal to package

3. **Config**: Currently uses pydantic `MetricsConfig`
   - Change to simple dict with validation function
   - Provide defaults, merge user config

---

## The Plan: What Needs to Be Built

### Phase 1: Documentation & Tests (TDD Approach)

#### Already Done ✅
- `METRICS_REFERENCE.md` created and copied to your repo

#### To Do 📋

1. **Create Test Infrastructure** (`tests/`)
   ```
   tests/
   ├── conftest.py               # Fixtures
   ├── test_collector.py         # MetricsCollector tests
   ├── test_decorator.py         # @track_metrics tests
   ├── test_instrumentation.py   # track_section/memory tests
   ├── test_dask_backend.py      # DaskMetricsBackend tests
   ├── test_fine_metrics.py      # Dask Spans integration tests
   └── test_export.py            # HTML/plot export tests
   ```

2. **Write Fixtures** (conftest.py):
   - `local_cluster()`: Dask LocalCluster with 2 workers for testing
   - `sample_processor()`: Simple coffea processor with instrumentation
   - `sample_fileset()`: Mock NanoAOD dataset
   - `metrics_config()`: Standard test config dict

3. **Write Failing Tests First** (TDD):
   - Write tests for features that don't exist yet
   - Tests should fail initially
   - Implement features to make tests pass

### Phase 2: Backend Plugin Architecture

#### To Create 🔨

1. **Abstract Backend** (`src/coffea_metrics/backends/base.py`):
   ```python
   class AbstractMetricsBackend(ABC):
       @abstractmethod
       def start_tracking(self, interval: float):
           """Start worker tracking."""

       @abstractmethod
       def stop_tracking(self) -> Dict:
           """Stop and return tracking data."""

       @abstractmethod
       def create_span(self, name: str):
           """Create performance span (for fine metrics)."""

       @abstractmethod
       def get_span_metrics(self, span_id) -> Dict:
           """Extract metrics from span."""

       @abstractmethod
       def supports_fine_metrics(self) -> bool:
           """Whether fine metrics are available."""
   ```

2. **Dask Backend** (`src/coffea_metrics/backends/dask.py`):
   - Implement `DaskMetricsBackend(AbstractMetricsBackend)`
   - Wrap existing `worker_tracker.py` functions
   - **NEW**: Integrate Dask Spans API for fine metrics
   - **NEW**: Extract `cumulative_worker_metrics` from spans

3. **Backend Factory**:
   ```python
   def create_backend(executor) -> AbstractMetricsBackend:
       if isinstance(executor, DaskExecutor):
           return DaskMetricsBackend(executor.client)
       else:
           raise ValueError(f"Unsupported executor: {type(executor)}")
   ```

### Phase 3: Fine Metrics Integration (Key New Feature)

#### Background: Dask Fine Metrics

Dask provides activity-level breakdown of task execution:
- **thread-cpu**: Pure compute time
- **thread-noncpu**: I/O + waiting time (wall - cpu)
- **disk-read/write**: Spill operations
- **compress/decompress**: Compression overhead
- **serialize/deserialize**: Pickling overhead

This solves the **I/O vs Compute separation** problem!

#### How Dask Spans Work

1. **Wrap processing** in a span context:
   ```python
   from distributed import span

   with span("coffea-processing") as span_id:
       output, report = runner(fileset, processor)
   ```

2. **Extract metrics** from span:
   ```python
   def get_span_metrics(client, span_id):
       def _get(dask_scheduler, span_id):
           spans_ext = dask_scheduler.extensions["spans"]
           span_obj = spans_ext.spans[span_id]
           return dict(span_obj.cumulative_worker_metrics)

       return client.run_on_scheduler(_get, span_id=span_id)
   ```

3. **Metrics format**:
   ```python
   # Keys: (context, task_prefix, activity, unit)
   {
       ("execute", "process-abc123", "thread-cpu", "seconds"): 45.2,
       ("execute", "process-abc123", "thread-noncpu", "seconds"): 12.8,
       ("execute", "process-abc123", "disk-read", "seconds"): 0.5,
       ...
   }
   ```

#### To Create 🔨

1. **Fine Metrics Parser** (`src/coffea_metrics/aggregation/fine_metrics.py`):
   ```python
   def parse_fine_metrics(span_metrics: Dict) -> Dict:
       """Parse Dask span metrics into structured format.

       Returns
       -------
       {
           "cpu_time": float,          # Total thread-cpu
           "io_time": float,           # Total thread-noncpu
           "disk_read_time": float,
           "disk_write_time": float,
           "compression_time": float,
           "serialization_time": float,
           "by_task_prefix": {...}     # Per-task breakdown
       }
       """
   ```

2. **Integrate with Collector**: MetricsCollector creates span, extracts metrics

### Phase 4: Decorator for Chunk Tracking

#### To Create 🔨

1. **@track_metrics Decorator** (`src/coffea_metrics/decorator.py`):
   ```python
   from distributed.metrics import context_meter
   import psutil, time

   def track_metrics(func):
       """Decorator for processor.process() to track chunks."""
       @wraps(func)
       def wrapper(self, events):
           chunk_id = len(getattr(self, '_chunk_metrics', []))

           # Label in Dask fine metrics
           with context_meter.meter(f"chunk-{chunk_id}"):
               # Auto-track start
               t0 = time.perf_counter()
               mem_start = psutil.Process().memory_info().rss / 1e9

               # Setup instrumentation context
               self._current_chunk = {
                   "chunk_id": chunk_id,
                   "events": len(events),
                   "dataset": events.metadata.get("dataset", "unknown"),
                   "filename": events.metadata.get("filename", "unknown"),
                   "sections": [],
                   "memory_sections": [],
               }

               # Execute
               result = func(self, events)

               # Auto-track end
               t1 = time.perf_counter()
               mem_end = psutil.Process().memory_info().rss / 1e9

               # Store
               self._current_chunk.update({
                   "time": t1 - t0,
                   "memory_start_gb": mem_start,
                   "memory_end_gb": mem_end,
                   "memory_delta_gb": mem_end - mem_start,
               })

               if not hasattr(self, '_chunk_metrics'):
                   self._chunk_metrics = []
               self._chunk_metrics.append(self._current_chunk)

           return result
       return wrapper
   ```

### Phase 5: Internal Instrumentation

#### To Create 🔨

1. **track_section() Context Manager** (`src/coffea_metrics/instrumentation.py`):
   ```python
   @contextmanager
   def track_section(processor, name: str):
       """Auto-compute time delta on exit."""
       if not hasattr(processor, '_current_chunk'):
           yield
           return

       t0 = time.perf_counter()
       try:
           yield
       finally:
           t1 = time.perf_counter()
           processor._current_chunk["sections"].append({
               "name": name,
               "time": t1 - t0,
           })
   ```

2. **track_memory() Context Manager**:
   ```python
   @contextmanager
   def track_memory(processor, name: str):
       """Auto-compute memory delta on exit."""
       if not hasattr(processor, '_current_chunk'):
           yield
           return

       mem_start = psutil.Process().memory_info().rss / 1e9
       try:
           yield
       finally:
           mem_end = psutil.Process().memory_info().rss / 1e9
           processor._current_chunk["memory_sections"].append({
               "name": name,
               "memory_delta_gb": mem_end - mem_start,
               "memory_start_gb": mem_start,
               "memory_end_gb": mem_end,
           })
   ```

3. **Base Instrumentation Class**:
   ```python
   class BaseInstrumentationContext:
       """Base class for custom instrumentation."""
       def __init__(self, processor, name: str):
           self.processor = processor
           self.name = name

       def __enter__(self): return self
       def __exit__(self, *args): return False

       def record_metric(self, key, value):
           """Store custom metric in current chunk."""
           if not hasattr(self.processor, '_current_chunk'):
               return
           if 'custom_metrics' not in self.processor._current_chunk:
               self.processor._current_chunk['custom_metrics'] = {}
           self.processor._current_chunk['custom_metrics'][key] = value
   ```

### Phase 6: MetricsCollector Context Manager

#### To Create 🔨

**Main Entry Point** (`src/coffea_metrics/collector.py`):

```python
class MetricsCollector:
    """Context manager for comprehensive metrics collection."""

    def __init__(
        self,
        executor,
        output_dir: str = "benchmarks",
        config: Optional[Dict] = None,
    ):
        self.executor = executor
        self.output_dir = Path(output_dir)
        self.config = merge_with_defaults(config)
        self.backend = create_backend(executor)

        # State
        self.metrics = None
        self.coffea_report = None
        self.chunk_metrics = None
        self.tracking_data = None
        self.fine_metrics = None

        # Timing
        self.t0 = None
        self.t1 = None

        # Span
        self._span_context = None
        self._span_id = None

        # Output
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.measurement_path = self.output_dir / timestamp

    def __enter__(self):
        if not self.config["enable"]:
            return self

        self.measurement_path.mkdir(parents=True, exist_ok=True)
        self.t0 = time.perf_counter()

        # Start worker tracking
        if self.config["track_workers"]:
            self.backend.start_tracking(
                interval=self.config["worker_tracking_interval"]
            )

        # Create span for fine metrics
        if self.backend.supports_fine_metrics():
            self._span_context = self.backend.create_span("coffea-processing")
            self._span_context.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.config["enable"]:
            return False

        self.t1 = time.perf_counter()

        # Exit span, extract fine metrics
        if self._span_context:
            self._span_id = self._span_context.__exit__(exc_type, exc_val, exc_tb)
            if self._span_id:
                span_metrics = self.backend.get_span_metrics(self._span_id)
                self.fine_metrics = parse_fine_metrics(span_metrics)

        # Stop worker tracking
        if self.config["track_workers"]:
            self.tracking_data = self.backend.stop_tracking()

        # Aggregate all metrics
        self.metrics = aggregate_all_metrics(
            coffea_report=self.coffea_report,
            tracking_data=self.tracking_data,
            chunk_metrics=self.chunk_metrics,
            fine_metrics=self.fine_metrics,
            t_start=self.t0,
            t_end=self.t1,
        )

        # Export
        if self.config["save_measurements"]:
            self._save_all_data()
        if self.config["generate_html_tables"]:
            self._export_html_tables()
        if self.config["generate_plots"]:
            self._generate_plots()

        return False

    def set_coffea_report(self, report: Dict):
        """Set coffea runner report after execution."""
        self.coffea_report = report

    def set_chunk_metrics(self, chunk_metrics: List[Dict]):
        """Set chunk metrics from processor."""
        self.chunk_metrics = chunk_metrics
```

### Phase 7: HTML Export

#### To Create 🔨

**Rich Tables to HTML** (`src/coffea_metrics/export/tables.py`):

```python
from rich.console import Console
from pathlib import Path

def export_tables_html(metrics: Dict, output_path: Path):
    """Export all metrics tables as HTML."""
    console = Console(record=True)

    # Generate all tables to console
    console.print(format_throughput_table(metrics))
    console.print(format_event_processing_table(metrics))
    console.print(format_resources_table(metrics))
    console.print(format_timing_table(metrics))

    # Export console to HTML
    html = console.export_html()
    output_path.write_text(html)
```

### Phase 8: Output Organization

#### Directory Structure (Auto-Created)

```
<output_dir>/
└── <timestamp>/              # YYYYMMDD-HHMMSS
    ├── metrics.json          # All aggregated metrics
    ├── worker_timeline.json  # Worker tracking data
    ├── chunk_metrics.json    # Per-chunk data (NEW)
    ├── fine_metrics.json     # Dask span metrics (NEW)
    ├── metrics_tables.html   # Rich tables as HTML (NEW)
    ├── dask_performance.html # Dask performance report
    └── plots/
        ├── summary_dashboard.png
        ├── worker_count.png
        ├── memory_util.png
        ├── cpu_util.png
        └── scaling.png
```

**No OutputDirectoryManager dependency** - package manages its own output structure.

---

## Key Technical Decisions

### 1. Backend Architecture

**Decision**: Pluggable backend pattern with abstract base class

**Rationale**:
- Dask is primary target but TaskVine support planned
- Each executor has different APIs for resource tracking
- Abstract interface allows swapping backends without changing user code

### 2. Context Managers for Instrumentation

**Decision**: `track_section()` and `track_memory()` auto-compute deltas on exit

**Rationale**:
- User doesn't manually calculate `t1 - t0` or `mem_end - mem_start`
- Cleaner API, less error-prone
- Context manager pattern familiar to Python users

### 3. Decorator vs Wrapper

**Decision**: Use `@track_metrics` decorator on `process()` method

**Rationale**:
- Minimal user code changes (just add decorator)
- Automatically captures chunk boundaries
- Stores metrics in processor instance for later retrieval

### 4. Spans for Fine Metrics

**Decision**: Use Dask Spans API instead of custom tracking

**Rationale**:
- Dask already tracks CPU vs I/O at activity level
- No need to reinvent - leverage existing infrastructure
- Negligible overhead, comprehensive breakdown

### 5. No Backwards Compatibility

**Decision**: Breaking changes from intccms version are OK

**Rationale**:
- Mo is only current user, version frozen elsewhere
- Fresh start allows clean API design
- Standalone package with different use cases

---

## Implementation Priority Order

### Week 1: Tests & Documentation ✅ (Partially Done)
- [x] Write METRICS_REFERENCE.md
- [ ] Create test infrastructure (conftest.py, fixtures)
- [ ] Write failing tests for all features

### Week 2: Core Backend & Fine Metrics
- [ ] Implement AbstractMetricsBackend
- [ ] Implement DaskMetricsBackend with Spans
- [ ] Implement fine metrics parsing
- [ ] Make backend tests pass

### Week 3: Decorator & Instrumentation
- [ ] Implement @track_metrics decorator
- [ ] Implement track_section() and track_memory()
- [ ] Implement BaseInstrumentationContext
- [ ] Make decorator/instrumentation tests pass

### Week 4: MetricsCollector & Export
- [ ] Implement MetricsCollector context manager
- [ ] Implement HTML table export
- [ ] Refactor existing plot/table generation
- [ ] Make collector/export tests pass

### Week 5: Polish & Packaging
- [ ] Write README with usage examples
- [ ] Create pyproject.toml for PyPI
- [ ] Add GitHub Actions for CI
- [ ] Publish to test PyPI

---

## Dependencies

**Required**:
- `coffea` - The framework we're monitoring
- `dask[distributed]` - For Dask executor backend
- `rich` - For terminal/HTML table formatting
- `matplotlib` - For plot generation
- `numpy` - For calculations
- `psutil` - For memory measurement
- `awkward` - Used by coffea

**Optional**:
- `crick` - For Dask digest metrics (not critical)
- `pytest` - For testing (dev dependency)

---

## Testing Strategy

### Test Pyramid

1. **Unit Tests** (fast, isolated):
   - Backend creation logic
   - Fine metrics parsing
   - Config validation
   - Individual plot functions

2. **Integration Tests** (moderate):
   - Full MetricsCollector workflow
   - Decorator with real processor
   - Span metrics extraction

3. **End-to-End Tests** (slow, realistic):
   - Complete workflow with LocalCluster
   - Real coffea processor with NanoAOD schema
   - Verify output files created

### Test Fixtures Design

```python
# conftest.py
import pytest
from dask.distributed import LocalCluster, Client
from coffea import processor

@pytest.fixture(scope="session")
def local_cluster():
    """Shared cluster for all tests."""
    cluster = LocalCluster(n_workers=2, threads_per_worker=1)
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()

@pytest.fixture
def sample_processor():
    """Simple processor for testing."""
    from coffea_metrics import track_metrics, track_section

    class TestProcessor(processor.ProcessorABC):
        @track_metrics
        def process(self, events):
            with track_section(self, "test_section"):
                n = len(events)
            return {"count": n}

        def postprocess(self, acc):
            return acc

    return TestProcessor()

@pytest.fixture
def sample_fileset():
    """Mock NanoAOD fileset."""
    # Use coffea's testing utilities or create mock
    return {
        "TestDataset": {
            "files": {"https://example.com/test.root": "Events"}
        }
    }

@pytest.fixture
def metrics_config():
    """Standard test configuration."""
    return {
        "enable": True,
        "track_workers": True,
        "worker_tracking_interval": 0.5,  # Faster for tests
        "save_measurements": False,        # Don't clutter test dir
        "generate_plots": False,
        "generate_html_tables": False,
    }
```

---

## Usage Examples (Target API)

### Minimal Usage

```python
from coffea_metrics import MetricsCollector
from coffea.processor import Runner, DaskExecutor

executor = DaskExecutor(client=dask_client)

with MetricsCollector(executor, output_dir="benchmarks") as mc:
    runner = Runner(executor=executor)
    output, report = runner(fileset, processor_instance)
    mc.set_coffea_report(report)

# Metrics auto-saved to benchmarks/<timestamp>/
print(f"Throughput: {mc.metrics['overall_rate_gbps']:.2f} Gbps")
print(f"Event rate: {mc.metrics['event_rate_wall_khz']:.1f} kHz")
```

### With Chunk Tracking

```python
from coffea_metrics import MetricsCollector, track_metrics

class MyProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        # Normal processing
        jets = events.Jet[events.Jet.pt > 30]
        return {"njets": len(jets)}

with MetricsCollector(executor, "benchmarks") as mc:
    output, report = runner(fileset, processor_instance)
    mc.set_coffea_report(report)
    mc.set_chunk_metrics(processor_instance._chunk_metrics)

# Now have per-chunk timing/memory
for chunk in mc.chunk_metrics:
    print(f"Chunk {chunk['chunk_id']}: {chunk['time']:.2f}s, "
          f"{chunk['events']} events, {chunk['memory_delta_gb']:.2f} GB")
```

### With Internal Instrumentation

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

# Section timing/memory automatically collected per chunk
```

### Accessing Fine Metrics

```python
with MetricsCollector(executor, "benchmarks") as mc:
    # ... run processing ...

# Fine metrics automatically collected with Dask backend
print(f"CPU time: {mc.metrics['cpu_time']:.1f}s")
print(f"I/O time: {mc.metrics['io_time']:.1f}s")
print(f"I/O overhead: {mc.metrics['io_overhead_pct']:.1f}%")
print(f"Disk spilling: {mc.metrics['disk_read_time'] + mc.metrics['disk_write_time']:.1f}s")
```

---

## Common Pitfalls to Avoid

### 1. Span Context Management

**Problem**: Spans need to be properly entered/exited

**Solution**:
```python
# Good
self._span_context = self.backend.create_span("name")
self._span_context.__enter__()
# ... processing ...
self._span_id = self._span_context.__exit__(None, None, None)

# Bad - span not activated
self._span_context = self.backend.create_span("name")
# Processing happens outside span!
```

### 2. Chunk Metrics Storage

**Problem**: Decorator stores in processor instance, need to extract

**Solution**:
```python
# After runner completes
if hasattr(processor_instance, '_chunk_metrics'):
    mc.set_chunk_metrics(processor_instance._chunk_metrics)
```

### 3. Config Defaults

**Problem**: User may not provide all config keys

**Solution**: Merge with comprehensive defaults
```python
def merge_with_defaults(user_config):
    defaults = {
        "enable": True,
        "track_workers": True,
        "worker_tracking_interval": 1.0,
        "save_measurements": True,
        "generate_plots": True,
        "generate_html_tables": True,
    }
    return {**defaults, **(user_config or {})}
```

### 4. Memory Measurement Timing

**Problem**: Memory may include garbage from previous chunks

**Solution**: Document this limitation, consider adding `gc.collect()` before measurement (but has performance cost)

---

## IMPORTANT REMINDER FOR MO 🔔

**TOPIC TO DISCUSS**: Prometheus Metrics Integration

**Reference**: https://distributed.dask.org/en/latest/prometheus.html

**Key Questions**:
1. Should we export metrics to Prometheus endpoint?
2. What metrics would be most useful for alerting/monitoring?
3. Should this be optional (via config flag)?
4. Integration with existing Dask Prometheus exporters?

**Potential Benefits**:
- Long-term trend analysis across multiple runs
- Alerting on performance degradation
- Grafana dashboards for visualization
- Standard monitoring infrastructure

**When to Discuss**: After core implementation is working

---

## Next Steps for New Claude Session

### Immediate Actions

1. **Familiarize yourself** with existing code:
   - Read `METRICS_REFERENCE.md` thoroughly
   - Examine copied modules in `src/coffea_metrics/`
   - Understand data flow from collection → aggregation → export

2. **Create test infrastructure first** (TDD):
   - Write `tests/conftest.py` with fixtures
   - Write failing tests for new features
   - Don't implement yet - just tests

3. **Ask Mo for clarification** if anything is unclear:
   - Design decisions
   - Priority of features
   - Any changes to the plan

### Long-Term Roadmap

**Phase 1**: Tests & Fixtures (Week 1)
**Phase 2**: Backend & Fine Metrics (Week 2)
**Phase 3**: Decorator & Instrumentation (Week 3)
**Phase 4**: MetricsCollector & Export (Week 4)
**Phase 5**: Polish & Publish (Week 5)

### Success Criteria

The project is complete when:
- [ ] All tests pass
- [ ] Package installable via `pip install coffea-metrics`
- [ ] Works with vanilla coffea (no intccms dependency)
- [ ] Comprehensive documentation
- [ ] Example notebooks demonstrating usage
- [ ] Mo can integrate into intccms as external dependency

---

## Questions for Mo (New Claude Should Ask)

1. **Repo Setup**: Is the new repo already initialized with pyproject.toml?
2. **Testing**: Preference for pytest or unittest?
3. **CI/CD**: Should we set up GitHub Actions?
4. **Versioning**: Semantic versioning? Start at 0.1.0?
5. **License**: What license for the package?
6. **Dependencies**: Any constraints on dependency versions?

---

**End of Handoff Document**

Good luck, future Claude! You have everything you need to build this. Follow the TDD approach, start with tests, and implement methodically. Mo is an experienced developer who values clean, maintainable code - make him proud! 🚀

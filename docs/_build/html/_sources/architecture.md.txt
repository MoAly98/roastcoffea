# Architecture

roastcoffea has a modular design with three main components.

## Components

### 1. Backends (`roastcoffea.backends`)

Backends handle metrics collection for specific executors:

- **DaskMetricsBackend**: Dask distributed executor
  - Worker tracking via scheduler sampling
  - Dask Spans for fine metrics
  - Span creation and retrieval

Additional backends can be added to support other executors (e.g., TaskVine) by implementing the backend interface.

**Interface**:
```python
class AbstractMetricsBackend:
    def start_tracking(self, interval: float) -> None: ...
    def stop_tracking(self) -> dict: ...
    def create_span(self, name: str) -> Any: ...
    def get_span_metrics(self, span_info: Any) -> dict: ...
    def supports_fine_metrics(self) -> bool: ...
```

### 2. Aggregators (`roastcoffea.aggregation`)

Aggregators combine metrics from multiple sources:

- **MetricsAggregator**: Main entry point
- **Workflow aggregation**: Coffea report + wall clock timing
- **Worker aggregation**: Resource utilization time-series
- **Fine metrics parsing**: Dask Spans activity breakdown
- **Chunk aggregation**: Per-chunk statistics
- **Efficiency calculation**: Derived metrics (ratios, percentages)

**Data flow**:
```
Coffea Report ──┐
Wall Clock ─────┤
Worker Tracking ├──> MetricsAggregator ──> Unified dict
Dask Spans ─────┤
Chunk Metrics ──┘
```

### 3. Exporters (`roastcoffea.export`)

Exporters handle output:

- **Reporter**: Rich tables (`format_*_table()` functions)
- **Measurements**: Save/load to disk (JSON + metadata)

## Instrumentation

### Decorator (`@track_metrics`)

Wraps `process()` to collect chunk-level metrics:

1. Check `_roastcoffea_collect_metrics` flag
2. Initialize `_roastcoffea_current_chunk` dict
3. Capture timing and memory
4. Extract chunk metadata from events
5. Inject metrics as list into output

### Context Managers

`track_time()` and `track_memory()` write to `_roastcoffea_current_chunk`:

```python
with track_time(self, "section"):
    ...  # your code here
# Writes to self._roastcoffea_current_chunk["timing"]["section"]
```

## Distributed Mode

The list-based accumulator approach:

1. Each worker returns `{..., "__roastcoffea_metrics__": [chunk_data]}`
2. Coffea's tree reduction concatenates: `[a] + [b] = [a, b]`
3. Final output has all chunks: `[chunk1, chunk2, ..., chunkN]`
4. `MetricsCollector.extract_metrics_from_output()` retrieves the list

This works because Coffea uses `+` operator for aggregation, and lists concatenate naturally.

## Extensibility

To add a new backend:

1. Implement `AbstractMetricsBackend`
2. Add backend name to `get_parser()` in `aggregation/backends.py`
3. Update `MetricsCollector` to support the backend

To add new metrics:

1. Collect raw data in backend or decorator
2. Add aggregation logic in `aggregation/`
3. Update reporter to display new metrics

## Next steps

::::{grid} 1
:gutter: 3

:::{grid-item-card} 🔧 Extend the system
:class-header: bg-info text-white
See {doc}`advanced` for custom backends and instrumentation.
:::

:::{grid-item-card} 📚 API Reference
:class-header: bg-dark text-white
Browse the full API documentation for implementation details.
:::

::::

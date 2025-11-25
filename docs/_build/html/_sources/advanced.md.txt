# Advanced Usage

## Custom Backends

To support a new executor (e.g., TaskVine), implement `AbstractMetricsBackend`:

```python
from roastcoffea.backends.base import AbstractMetricsBackend


class TaskVineMetricsBackend(AbstractMetricsBackend):
    def __init__(self, taskvine_manager):
        self.manager = taskvine_manager

    def start_tracking(self, interval: float) -> None:
        # Start collecting TaskVine metrics
        pass

    def stop_tracking(self) -> dict:
        # Return collected metrics
        return {"taskvine_metrics": ...}

    def create_span(self, name: str) -> Any:
        # Optional: return None if spans not supported
        return None

    def get_span_metrics(self, span_info: Any) -> dict:
        return {}

    def supports_fine_metrics(self) -> bool:
        return False
```

Register it in `aggregation/backends.py`:

```python
def get_parser(backend: str):
    if backend == "dask":
        return DaskMetricsParser()
    elif backend == "taskvine":
        return TaskVineMetricsParser()
    # ...
```

## Custom Instrumentation

Beyond `track_time()` and `track_memory()`, you can add custom metrics:

```python
from roastcoffea import track_metrics


class CustomProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        # Track custom metric
        if hasattr(self, "_roastcoffea_current_chunk"):
            self._roastcoffea_current_chunk["custom_count"] = 42

        return {"sum": len(events)}

    def postprocess(self, accumulator):
        return accumulator
```

The custom field will appear in `collector.chunk_metrics`.

## Disabling Metrics Collection

Control collection with the flag:

```python
processor._roastcoffea_collect_metrics = False  # Disable
processor._roastcoffea_collect_metrics = True  # Enable
```

`MetricsCollector` sets this automatically in `__enter__` and `__exit__`.

## Worker Tracking Interval

Adjust sampling rate:

```python
with MetricsCollector(client, worker_tracking_interval=0.5) as collector:
    # Sample every 0.5 seconds instead of 1.0
    ...
```

Lower intervals give finer time resolution but slightly higher overhead.

## Accessing Raw Data

Get unprocessed metrics:

```python
# Raw chunk data
for chunk in collector.chunk_metrics:
    print(chunk["duration"], chunk["num_events"])

# Raw worker tracking
if collector.tracking_data:
    print(collector.tracking_data["worker_counts"])  # {timestamp: count}

# Raw Dask Spans
if collector.span_metrics:
    print(collector.span_metrics)  # cumulative_worker_metrics dict
```

## Extending Aggregation

Add derived metrics in `aggregation/efficiency.py`:

```python
def calculate_efficiency_metrics(workflow_metrics, worker_metrics):
    # ... existing metrics ...

    # Add custom metric
    metrics["custom_efficiency"] = (
        workflow_metrics["total_events"] / worker_metrics["avg_workers"]
    )

    return metrics
```

## Next steps

::::{grid} 1
:gutter: 3

:::{grid-item-card} 🏗️ System design
:class-header: bg-info text-white
Read {doc}`architecture` to understand the component structure.
:::

:::{grid-item-card} 📊 Metrics reference
:class-header: bg-dark text-white
See {doc}`metrics_reference` for the complete list of available metrics.
:::

::::

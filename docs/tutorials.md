# Tutorial

This guide walks through roastcoffea's three levels of metrics collection.

## Basic Metrics

The simplest setup tracks overall workflow performance without modifying your processor:

```python
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from dask.distributed import Client
from roastcoffea import MetricsCollector


class MyProcessor(processor.ProcessorABC):
    def process(self, events):
        jets = events.Jet
        selected = jets[jets.pt > 30]
        return {"sum": len(events), "njets": len(selected)}

    def postprocess(self, accumulator):
        return accumulator


client = Client()
my_processor = MyProcessor()

with MetricsCollector(client, processor_instance=my_processor) as collector:
    executor = processor.DaskExecutor(client=client)
    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=100_000,
        savemetrics=True,
    )

    output, report = runner(fileset, processor_instance=my_processor)
    collector.set_coffea_report(report)

collector.print_summary()
```

This prints tables showing throughput (Gbps, event rate), resource usage (workers, cores, memory), timing (wall time, CPU time), and CPU/IO breakdown.

## Chunk-Level Tracking

Add `@track_metrics` to collect per-chunk performance data:

```python
from roastcoffea import track_metrics


class DetailedProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        jets = events.Jet
        selected = jets[jets.pt > 30]
        return {"sum": len(events), "njets": len(selected)}

    def postprocess(self, accumulator):
        return accumulator


detailed_processor = DetailedProcessor()

with MetricsCollector(client, processor_instance=detailed_processor) as collector:
    # ... same runner setup ...
    output, report = runner(fileset, processor_instance=detailed_processor)

    # Extract chunk metrics from output
    collector.extract_metrics_from_output(output)
    collector.set_coffea_report(report)

collector.print_summary()

# Access chunk data
metrics = collector.get_metrics()
print(f"Total chunks: {metrics['num_chunks']}")
print(f"Average chunk time: {metrics['chunk_duration_mean']:.2f}s")
```

The decorator injects metrics into the output dict during distributed processing. Coffea's tree reduction automatically concatenates metrics from all chunks.

## Fine-Grained Profiling

Use `track_time()` and `track_memory()` to profile specific sections:

```python
import awkward as ak
from roastcoffea import track_metrics, track_time, track_memory


class ProfilingProcessor(processor.ProcessorABC):
    @track_metrics
    def process(self, events):
        with track_time(self, "load_jets"):
            jets = events.Jet

        with track_memory(self, "selection"):
            selected = jets[jets.pt > 30]

        ak.sum(selected.pt, axis=1)  # Force evaluation

        return {"sum": len(events), "njets": len(selected)}

    def postprocess(self, accumulator):
        return accumulator


profiling_processor = ProfilingProcessor()

with MetricsCollector(client, processor_instance=profiling_processor) as collector:
    # ... same runner setup ...
    output, report = runner(fileset, processor_instance=profiling_processor)
    collector.extract_metrics_from_output(output)
    collector.set_coffea_report(report)

# Check section timings
for chunk in collector.chunk_metrics[:3]:
    print(f"\nChunk with {chunk['num_events']} events:")
    for section, duration in chunk["timing"].items():
        print(f"  {section}: {duration:.3f}s")
    for section, delta_mb in chunk["memory"].items():
        print(f"  {section}: {delta_mb:+.1f} MB")
```

## Saving Results

Save metrics for later analysis:

```python
from pathlib import Path

measurement_path = collector.save_measurement(
    output_dir=Path("measurements"), measurement_name="ttbar_analysis"
)
print(f"Saved to: {measurement_path}")
```

Load them back:

```python
from roastcoffea.export.measurements import load_measurement

loaded = load_measurement(measurement_path)
print(f"Processed {loaded['metrics']['total_events']} events")
```

## Next steps

::::{grid} 1
:gutter: 3

:::{grid-item-card} 💡 Understand the metrics
:class-header: bg-info text-white
Read {doc}`concepts` to learn what each metric measures and how it's calculated.
:::

:::{grid-item-card} 🔧 Advanced usage
:class-header: bg-dark text-white
See {doc}`advanced` for custom instrumentation and extending backends.
:::

:::{grid-item-card} 📊 Metrics reference
:class-header: bg-light
Browse {doc}`metrics_reference` for the complete list of available metrics.
:::

::::

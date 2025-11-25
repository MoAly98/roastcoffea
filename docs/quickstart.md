# Quickstart

## Installation

```bash
pip install roastcoffea
```

Or with pixi:

```bash
pixi add roastcoffea
```

## Basic Usage

Wrap your Coffea workflow with `MetricsCollector`:

```python
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from dask.distributed import Client
from roastcoffea import MetricsCollector


# Your processor
class MyProcessor(processor.ProcessorABC):
    def process(self, events):
        jets = events.Jet
        selected = jets[jets.pt > 30]
        return {"sum": len(events), "njets": len(selected)}

    def postprocess(self, accumulator):
        return accumulator


# Setup
client = Client()
my_processor = MyProcessor()

# Collect metrics
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

# View results
collector.print_summary()
```

The summary includes throughput, resource usage, timing, and CPU/IO breakdown.

## Next steps

::::{grid} 1
:gutter: 3

:::{grid-item-card} 📖 Tutorial
:class-header: bg-info text-white
Step through {doc}`tutorials` for chunk tracking and fine-grained profiling examples.
:::

:::{grid-item-card} 💡 Concepts
:class-header: bg-dark text-white
Read {doc}`concepts` to understand what each metric means and how they're calculated.
:::

:::{grid-item-card} 📓 Examples
:class-header: bg-light
Check the [example notebooks](https://github.com/iris-hep/roastcoffea/tree/main/examples) for complete workflows.
:::

::::

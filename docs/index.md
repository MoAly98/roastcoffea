# roastcoffea documentation

roastcoffea provides comprehensive performance metrics collection for Coffea workflows running on Dask. Track throughput, resource usage, and fine-grained profiling data without modifying your analysis code.

## Features

- **Workflow metrics**: Throughput, event rates, resource utilization
- **Chunk tracking**: Per-chunk performance with `@track_metrics` decorator
- **Fine-grained profiling**: Section-level timing with `track_time()` and `track_memory()`
- **Dask Spans integration**: Separate processor work from Dask overhead
- **Worker monitoring**: Time-series resource tracking
- **Export options**: Rich tables, JSON measurements

## Getting started

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} 🚀 Quickstart
:class-header: bg-info text-white
{doc}`quickstart` - Install and run your first metrics collection in minutes.
:::

:::{grid-item-card} 📖 Tutorial
:class-header: bg-success text-white
{doc}`tutorials` - Step through examples covering all collection levels.
:::

:::{grid-item-card} 💡 Concepts
:class-header: bg-warning text-dark
{doc}`concepts` - Understand what metrics mean and how they're calculated.
:::

:::{grid-item-card} 📊 Metrics Reference
:class-header: bg-danger text-white
{doc}`metrics_reference` - Complete catalog of available metrics.
:::

::::

```{toctree}
:maxdepth: 2
:caption: User Guide
:hidden:

introduction
quickstart
tutorials
concepts
metrics_reference
```

```{toctree}
:maxdepth: 2
:caption: Developer Guide
:hidden:

architecture
advanced
contributing
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

api/index
```

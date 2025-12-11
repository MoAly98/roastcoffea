# Introduction

roastcoffea is a comprehensive performance monitoring package for [Coffea](https://coffeateam.github.io/coffea/)-based High Energy Physics (HEP) analysis workflows. It provides detailed metrics collection, analysis, and visualization for distributed data processing on Dask clusters.

## Why roastcoffea?

When running large-scale HEP analyses on distributed systems, understanding performance bottlenecks is critical. roastcoffea helps answer questions like:

- **How fast is my analysis?** Track throughput (Gbps, kHz) and processing rates
- **Where is time spent?** Separate CPU time from I/O wait, identify bottlenecks
- **Is my cluster efficient?** Monitor worker utilization, memory usage, task distribution
- **How do chunks perform?** Track per-chunk timing, memory, and metadata
- **What's the overhead?** Distinguish processor work from Dask scheduling overhead

## Key Features

### Three Levels of Collection

roastcoffea provides progressively detailed metrics:

1. **Workflow-level** - Overall throughput, timing, resource usage (no code changes needed)
2. **Chunk-level** - Per-chunk performance data (add `@track_metrics` decorator)
3. **Fine-grained** - Section-by-section profiling (use `track_time()` and `track_memory()`)

### Automatic Integration

- Works seamlessly with existing Coffea workflows
- Minimal code changes required
- Context manager API for clean setup/teardown
- Automatic detection of Dask Spans for detailed metrics

### Comprehensive Metrics

- **Throughput**: Data rates (Gbps, MB/s), event rates (kHz)
- **Resources**: Worker counts, memory usage, CPU utilization
- **Timing**: Wall time, CPU time, I/O time breakdown
- **Efficiency**: Core utilization, speedup factors, parallelism metrics
- **Activity**: CPU vs non-CPU time, disk I/O, compression overhead
- **I/O Analysis**: File compression ratios, branch access patterns, data read percentages
- **Chunk Performance**: Runtime distributions, event processing correlations

### Visualization & Export

- Rich terminal tables with formatted output
- 17 built-in Matplotlib plots including:
  - Resource timelines (workers, memory, CPU utilization, data rates)
  - Efficiency metrics (CPU efficiency, task distribution)
  - I/O analysis (compression ratios, data access patterns)
  - Chunk-level performance (runtime distributions, event correlations)
- Save/load measurements for comparison
- JSON export for custom analysis

## How It Works

roastcoffea collects metrics from multiple sources:

1. **Coffea Report** - Built-in metrics from `coffea.processor.Runner` (throughput, events, columns)
2. **Wall Clock Timing** - Elapsed time measurement
3. **Worker Tracking** - Periodic scheduler sampling for resource data (CPU, memory, worker counts)
4. **Chunk Decorator** - Per-chunk timing, memory, and file-level metadata extraction
5. **Dask Spans** - Fine-grained activity breakdown (CPU, I/O, disk, memory)
6. **Instrumentation** - User-defined section tracking

These are aggregated into a unified metrics dictionary, providing both high-level summaries and detailed breakdowns.

## Who Should Use This?

roastcoffea is designed for:

- **Analysts** optimizing their Coffea workflows
- **Computing teams** monitoring cluster performance
- **Developers** profiling distributed HEP applications
- **Researchers** studying performance characteristics of data processing

## Next Steps

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} 🚀 Get Started
:class-header: bg-info text-white
Jump to {doc}`quickstart` to install and run your first collection.
:::

:::{grid-item-card} 📖 Learn the Concepts
:class-header: bg-success text-white
Read {doc}`concepts` to understand how metrics are collected and what they mean.
:::

:::{grid-item-card} 📓 Follow the Tutorial
:class-header: bg-warning text-dark
Step through {doc}`tutorials` for detailed examples at each collection level.
:::

:::{grid-item-card} 🏗️ Understand the Design
:class-header: bg-danger text-white
Explore {doc}`architecture` to learn about backends, aggregators, and exporters.
:::

::::

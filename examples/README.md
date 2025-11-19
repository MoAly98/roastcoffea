# roastcoffea Examples

This directory contains example notebooks and scripts demonstrating roastcoffea features.

## Notebooks

### `fine_metrics_demo.ipynb` - Complete Fine Metrics Guide

A comprehensive tutorial covering all fine-grained performance metrics available through Dask Spans:

**What you'll learn:**
- How to collect fine metrics (CPU/I/O breakdown, compression overhead, etc.)
- Understanding cumulative vs per-task granularity
- Visualizing per-task metrics with built-in plots
- Interpreting metrics to identify bottlenecks
- Comparing different analysis scenarios

**What it demonstrates:**
- Real Coffea processor reading ROOT files from scikit-hep-testdata
- Accessing raw Span metrics data structures
- Creating per-task visualizations
- Comparing CPU-bound vs I/O-bound workloads

**Requirements:**
- Dask with Spans support (distributed >= 2024.3.0)
- scikit-hep-testdata for test ROOT files
- Jupyter notebook environment

**Run it:**
```bash
# Using pixi
pixi run -e dev jupyter notebook examples/fine_metrics_demo.ipynb

# Or with pip/conda environment
jupyter notebook examples/fine_metrics_demo.ipynb
```

## Future Examples

Coming soon:
- Multi-worker scaling analysis
- Comparing Dask vs other executors
- Custom per-dataset metrics
- Integration with profiling tools

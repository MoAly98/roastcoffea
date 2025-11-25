# roastcoffea

Performance monitoring and metrics collection for Coffea-based HEP analysis workflows.

## Features

- Automatic worker tracking (Dask worker counts, memory, resource utilization)
- Comprehensive metrics (throughput, event rates, compression ratios, efficiency)
- Rich terminal output with formatted tables
- Matplotlib visualizations (worker timelines, memory utilization)
- Save and load benchmark measurements
- Simple context manager API

## Documentation

Complete documentation is available at [roastcoffea.readthedocs.io](https://roastcoffea.readthedocs.io):

- **[Quickstart](https://roastcoffea.readthedocs.io/en/latest/quickstart.html)**: Get started in minutes
- **[Tutorial](https://roastcoffea.readthedocs.io/en/latest/tutorials.html)**: Step-by-step guide covering all collection levels
- **[Concepts](https://roastcoffea.readthedocs.io/en/latest/concepts.html)**: Understand what metrics mean and how they're calculated
- **[Architecture](https://roastcoffea.readthedocs.io/en/latest/architecture.html)**: System design for developers
- **[Advanced Usage](https://roastcoffea.readthedocs.io/en/latest/advanced.html)**: Custom backends and instrumentation
- **[Metrics Reference](https://roastcoffea.readthedocs.io/en/latest/metrics_reference.html)**: Complete catalog of available metrics
- **[API Reference](https://roastcoffea.readthedocs.io/en/latest/api/index.html)**: Full API documentation

## Installation

```bash
# Clone and install in development mode
git clone https://github.com/MoAly98/roastcoffea.git
cd roastcoffea
pip install -e .
```

## Quick Start

### Complete Example

```python
from coffea import processor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from dask.distributed import Client, LocalCluster
from roastcoffea import MetricsCollector


# Define a simple processor
class MyProcessor(processor.ProcessorABC):
    def process(self, events):
        # Example: select jets with pT > 30 GeV
        jets = events.Jet[events.Jet.pt > 30]

        return {
            "njets": len(jets),
            "jet_pt": jets.pt.flatten().sum(),
        }

    def postprocess(self, accumulator):
        return accumulator


# Define your fileset
fileset = {
    "TTbar": {
        "files": {
            "root://xrootd.example.com//data/TTbar_1.root": "Events",
            "root://xrootd.example.com//data/TTbar_2.root": "Events",
        }
    },
}

# Setup Dask cluster and run with metrics collection
with LocalCluster(n_workers=4, threads_per_worker=2) as cluster:
    with Client(cluster) as client:
        # Create MetricsCollector
        with MetricsCollector(client, track_workers=True) as collector:
            # Run your Coffea workflow
            executor = processor.DaskExecutor(client=client)
            runner = processor.Runner(
                executor=executor,
                savemetrics=True,
                schema=NanoAODSchema,
            )

            output, report = runner(
                fileset,
                treename="Events",
                processor_instance=MyProcessor(),
            )

            # Provide the report to collector
            collector.set_coffea_report(report)

        # After context exit, metrics are aggregated
        # Print Rich tables with all metrics
        collector.print_summary()

        # Access specific metrics
        metrics = collector.get_metrics()
        print(f"\nThroughput: {metrics['overall_rate_gbps']:.2f} Gbps")
        print(f"Event rate: {metrics['event_rate_wall_khz']:.1f} kHz")

        # Fine metrics (automatic with Dask Spans)
        if metrics.get("cpu_percentage") is not None:
            print(f"CPU %: {metrics['cpu_percentage']:.1f}%")
            print(f"I/O %: {metrics['io_percentage']:.1f}%")
            print(f"Compression ratio: {metrics.get('compression_ratio', 'N/A')}")

        # Save measurement for later analysis
        collector.save_measurement(
            output_dir="benchmarks/", measurement_name="ttbar_analysis"
        )
```

### Minimal Example

```python
from dask.distributed import Client
from roastcoffea import MetricsCollector

client = Client()

with MetricsCollector(client) as collector:
    # Your Coffea workflow here
    output, report = runner(fileset, processor_instance=my_processor)
    collector.set_coffea_report(report)

# Print summary
collector.print_summary()
```

## Metrics Reference

### Workflow Metrics (from Coffea Report)

| Metric | Source | Description |
|--------|--------|-------------|
| `wall_time` | Coffea Report | Real elapsed time for the workflow |
| `total_cpu_time` | Coffea Report | Sum of all task durations across workers |
| `num_chunks` | Coffea Report | Number of data chunks processed |
| `avg_cpu_time_per_chunk` | Coffea Report | Average CPU time per chunk |
| `total_events` | Coffea Report | Total number of events processed |
| `total_bytes_compressed` | Coffea Report | Compressed bytes read from files |
| `total_bytes_uncompressed` | Dask Spans (v0.2+) | Actual uncompressed bytes read (stub: returns None until Spans implemented) |
| `compression_ratio` | Dask Spans (v0.2+) | Uncompressed / compressed bytes ratio (stub: returns None until Spans implemented) |
| `overall_rate_gbps` | Derived | Data processing rate in Gbps |
| `overall_rate_mbps` | Derived | Data processing rate in MB/s |
| `event_rate_wall_khz` | Derived | Events/sec from wall time (kHz) |
| `event_rate_agg_khz` | Derived | Events/sec from aggregated CPU time (kHz) |

### Worker Metrics (from Scheduler Tracking)

| Metric | Source | Description |
|--------|--------|-------------|
| `avg_workers` | Scheduler Tracking | Time-weighted average worker count |
| `peak_workers` | Scheduler Tracking | Maximum number of workers observed |
| `cores_per_worker` | Scheduler Tracking | Average cores per worker |
| `total_cores` | Scheduler Tracking | Total cores across all workers |
| `peak_memory_bytes` | Scheduler Tracking | Peak memory usage across all workers |
| `avg_memory_per_worker_bytes` | Scheduler Tracking | Time-averaged memory per worker |

### Efficiency Metrics (Derived)

| Metric | Source | Description |
|--------|--------|-------------|
| `core_efficiency` | Derived | Fraction of available cores actually used (0-1) |
| `speedup_factor` | Derived | Parallel speedup achieved vs single core |
| `event_rate_core_hz` | Derived | Events/sec/core (Hz per core) |

### Per-Worker Time-Series Data (from Scheduler Tracking)

Raw tracking data available in `metrics["tracking_data"]` for visualization:

| Field | Description |
|-------|-------------|
| `worker_counts` | Worker count over time |
| `worker_memory` | Process memory usage per worker over time |
| `worker_memory_limit` | Memory limit per worker over time |
| `worker_cores` | Cores per worker over time |
| `worker_active_tasks` | Tasks assigned (processing + queued) per worker |
| `worker_executing` | Tasks actually running per worker |
| `worker_nbytes` | Data stored on worker (vs process overhead) |
| `worker_occupancy` | Worker saturation metric (0.0 = idle, higher = saturated) |
| `worker_last_seen` | Last heartbeat timestamp (for detecting dead workers) |

## Advanced Usage

### Custom Per-Dataset Metrics

```python
with MetricsCollector(client) as collector:
    output, report = runner(fileset, processor_instance=my_processor)

    # Provide custom per-dataset breakdown
    custom_metrics = {
        "TTbar": {
            "entries": 1_000_000,
            "duration": 45.2,
            "performance_counters": {"num_requested_bytes": 5_000_000_000},
        },
        "WJets": {
            "entries": 500_000,
            "duration": 23.1,
            "performance_counters": {"num_requested_bytes": 2_500_000_000},
        },
    }

    collector.set_coffea_report(report, custom_metrics=custom_metrics)
```

### Fine-Grained Metrics (Dask Spans)

 Automatic collection of CPU/I/O breakdown and real compression ratios.

```python
with MetricsCollector(client) as collector:
    output, report = runner(fileset, processor_instance=my_processor)
    collector.set_coffea_report(report)

# Print summary includes fine metrics table when available
collector.print_summary()

# Access fine metrics directly
metrics = collector.get_metrics()
print(f"CPU %: {metrics['cpu_percentage']:.1f}%")
print(f"I/O %: {metrics['io_percentage']:.1f}%")
print(f"Compression ratio: {metrics.get('compression_ratio', 'N/A')}")
print(f"Compression overhead: {metrics['total_compression_overhead_seconds']:.1f}s")
```

**Fine metrics table output example**:
```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Metric                         ┃ Value       ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ CPU Time                       │ 125.3s      │
│ I/O Time                       │ 48.7s       │
│ CPU %                          │ 72.0%       │
│ I/O %                          │ 28.0%       │
│ Disk Read                      │ 12.50 GB    │
│ Compression Overhead           │ 5.2s        │
│   • Decompress                 │ 4.8s        │
│   • Compress                   │ 0.4s        │
│ Serialization Overhead         │ 3.1s        │
│   • Deserialize                │ 2.1s        │
│   • Serialize                  │ 1.0s        │
└────────────────────────────────┴─────────────┘
```

### Visualization

roastcoffea provides comprehensive visualization capabilities for all collected metrics.

#### Worker Timeline Plots

```python
from roastcoffea import (
    plot_worker_count_timeline,
    plot_memory_utilization_timeline,
    plot_occupancy_timeline,
    plot_executing_tasks_timeline,
    plot_worker_activity_timeline,
    plot_total_active_tasks_timeline,
)

# After collecting metrics with track_workers=True
tracking_data = collector.get_metrics()["tracking_data"]

# Worker count over time
plot_worker_count_timeline(tracking_data=tracking_data, output_path="worker_count.png")

# Memory utilization percentage over time
plot_memory_utilization_timeline(
    tracking_data=tracking_data, output_path="memory_util.png"
)

# Worker occupancy (task saturation) over time
plot_occupancy_timeline(tracking_data=tracking_data, output_path="occupancy.png")

# Executing tasks per worker over time
plot_executing_tasks_timeline(
    tracking_data=tracking_data, output_path="executing_tasks.png"
)

# Active tasks per worker over time
plot_worker_activity_timeline(
    tracking_data=tracking_data, output_path="worker_activity.png"
)

# Total active tasks across all workers
plot_total_active_tasks_timeline(
    tracking_data=tracking_data, output_path="total_activity.png"
)
```

#### Efficiency & Scaling Summary Plots

```python
from roastcoffea import (
    plot_efficiency_summary,
    plot_resource_utilization,
)

metrics = collector.get_metrics()

# Efficiency metrics bar chart
plot_efficiency_summary(metrics=metrics, output_path="efficiency.png")

# Resource utilization summary
plot_resource_utilization(metrics=metrics, output_path="resources.png")
```

#### Per-Task Fine Metrics (Dask Spans)

```python
from roastcoffea import (
    plot_per_task_cpu_io,
    plot_per_task_bytes_read,
    plot_per_task_overhead,
)

# Get span metrics from collector
span_metrics = collector.span_metrics

# CPU vs I/O time per task
plot_per_task_cpu_io(span_metrics=span_metrics, output_path="per_task_cpu_io.png")

# Bytes read per task (if disk-read available)
plot_per_task_bytes_read(span_metrics=span_metrics, output_path="per_task_bytes.png")

# Compression & serialization overhead per task
plot_per_task_overhead(span_metrics=span_metrics, output_path="per_task_overhead.png")
```

#### Loading Saved Measurements

```python
from roastcoffea import load_measurement

# Load a previously saved measurement
metrics, t0, t1 = load_measurement("benchmarks/my_run")

# Use metrics for any of the above plots
tracking_data = metrics.get("tracking_data")
if tracking_data:
    plot_worker_count_timeline(tracking_data, output_path="worker_count.png")
```

### Disable Worker Tracking

```python
# Skip worker tracking if you only want workflow-level metrics
with MetricsCollector(client, track_workers=False) as collector:
    output, report = runner(fileset, processor_instance=my_processor)
    collector.set_coffea_report(report)
```

### Adjust Tracking Interval

```python
# Sample worker metrics every 0.5 seconds instead of default 1.0s
with MetricsCollector(client, worker_tracking_interval=0.5) as collector:
    output, report = runner(fileset, processor_instance=my_processor)
    collector.set_coffea_report(report)
```

## License

BSD-3-Clause - see [LICENSE](LICENSE) for details.

# roastcoffea

Performance monitoring and metrics collection for Coffea-based HEP analysis workflows.

## Features

- Automatic worker tracking (Dask worker counts, memory, resource utilization)
- Comprehensive metrics (throughput, event rates, compression ratios, efficiency)
- Rich terminal output with formatted tables
- Matplotlib visualizations (worker timelines, memory utilization)
- Save and load benchmark measurements
- Simple context manager API

## Installation

```bash
# Clone and install in development mode
git clone https://github.com/MoAly98/roastcoffea.git
cd roastcoffea
pip install -e .
```

## Quick Start

```python
from coffea.processor import Runner, DaskExecutor
from dask.distributed import Client, LocalCluster
from roastcoffea import MetricsCollector

# Setup Dask client and cluster
with LocalCluster(n_workers=4) as cluster, Client(cluster) as client:
    # Wrap your Coffea workflow with MetricsCollector
    with MetricsCollector(client, track_workers=True) as collector:
        # Run your normal Coffea workflow
        executor = DaskExecutor(client=client)
        runner = Runner(executor=executor, savemetrics=True)
        output, report = runner(
            fileset, processor_instance=my_processor, treename="Events"
        )

        # Provide the report to collector
        collector.set_coffea_report(report)

# Access metrics after collector context exit
metrics = collector.get_metrics()
print(f"Throughput: {metrics['overall_rate_gbps']:.2f} Gbps")
print(f"Event rate: {metrics['event_rate_wall_khz']:.1f} kHz")
if metrics['core_efficiency'] is not None:
    print(f"Core efficiency: {metrics['core_efficiency']:.1%}")

# Print summary tables
collector.print_summary()

# Save for later analysis
collector.save_measurement(output_dir="benchmarks/", measurement_name="my_run")
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
| `bytes_read_compressed` | Coffea Report | Compressed bytes read from files |
| `bytes_read_uncompressed` | Coffea Report | Uncompressed bytes read |
| `compression_ratio` | Derived | Uncompressed / compressed bytes ratio |
| `overall_rate_gbps` | Derived | Data processing rate in Gbps |
| `overall_rate_mbs` | Derived | Data processing rate in MB/s |
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

### Visualization

```python
from roastcoffea import (
    plot_worker_count_timeline,
    plot_memory_utilization_timeline,
    load_measurement,
)

# Load a saved measurement
metrics, t0, t1 = load_measurement("benchmarks/my_run")

# Create plots (requires track_workers=True when collecting)
if "tracking_data" in metrics and metrics["tracking_data"] is not None:
    plot_worker_count_timeline(
        tracking_data=metrics["tracking_data"], output_path="worker_timeline.png"
    )

    plot_memory_utilization_timeline(
        tracking_data=metrics["tracking_data"], output_path="memory_timeline.png"
    )
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

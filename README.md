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
        output, report = runner(fileset, processor_instance=my_processor, treename="Events")

        # Provide the report to collector
        collector.set_coffea_report(report)

# Access metrics after collector context exit
metrics = collector.get_metrics()
print(f"Throughput: {metrics['overall_rate_gbps']:.2f} Gbps")
print(f"Event rate: {metrics['event_rate_wall_khz']:.1f} kHz")
print(f"Core efficiency: {metrics['core_efficiency']:.1%}")

# Print summary tables
collector.print_summary()

# Save for later analysis
collector.save_measurement(output_dir="benchmarks/", measurement_name="my_run")
```

## Metrics Collected

### Throughput Metrics
- **Data Rate**: Overall data processing rate (Gbps, MB/s)
- **Compression Ratio**: Uncompressed/compressed data ratio
- **Total Data Volume**: Compressed and uncompressed bytes read

### Event Processing Metrics
- **Total Events**: Number of events processed
- **Event Rate (Wall Clock)**: Events/sec from wall time perspective (kHz)
- **Event Rate (Aggregated)**: Events/sec from aggregated CPU time (kHz)
- **Event Rate (Core-Averaged)**: Events/sec/core (Hz per core)

### Resource Utilization
- **Workers (Time-Averaged)**: Time-weighted average worker count
- **Peak Workers**: Maximum workers observed
- **Total Cores**: Total cores available across all workers
- **Core Efficiency**: Fraction of cores actually used (0-1)
- **Speedup Factor**: Parallel speedup achieved

### Timing Metrics
- **Wall Time**: Real elapsed time
- **Total CPU Time**: Sum of all task durations
- **Number of Chunks**: Chunks processed
- **Avg CPU Time/Chunk**: Average processing time per chunk

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
from roastcoffea.visualization.plots import (
    plot_worker_count_timeline,
    plot_memory_utilization_timeline,
)

# Get tracking data from a saved measurement
from roastcoffea.export.measurements import load_measurement

metrics, t0, t1 = load_measurement("benchmarks/my_run")

# Create plots
plot_worker_count_timeline(
    tracking_data=metrics.get("tracking_data"), output_path="worker_timeline.png"
)

plot_memory_utilization_timeline(
    tracking_data=metrics.get("tracking_data"), output_path="memory_timeline.png"
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

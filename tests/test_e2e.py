"""End-to-end integration tests with real Coffea workflows.

Tests the full MetricsCollector workflow with actual Dask execution.

NOTE: These tests are marked as 'slow' and require network access to CERN Open Data.
They are excluded by default in pre-commit and CI.

Run locally with: pixi run -e dev pytest tests/test_e2e.py -v
Or run all slow tests: pixi run -e dev pytest -m slow
"""

from __future__ import annotations

import pytest
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from dask.distributed import Client, LocalCluster

from roastcoffea import MetricsCollector, track_metrics, track_memory, track_time

NanoAODSchema.warn_missing_crossrefs = False


class DummyProcessor(processor.ProcessorABC):
    """Simple processor for E2E testing."""

    def process(self, events):
        """Process events - just sum event counts."""
        output = {}
        output["sum"] = len(events)
        return output

    def postprocess(self, accumulator):
        """No postprocessing needed."""
        return accumulator


@pytest.fixture(scope="module")
def dask_cluster():
    """Provide a local Dask cluster for testing."""
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=1,
        processes=True,
        dashboard_address=":0",  # Auto-assign port to avoid conflicts
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


@pytest.fixture
def test_fileset():
    """Provide a minimal test fileset."""
    # Use a small NanoAOD test file from CERN Open Data
    return {
        "test_dataset": {
            "files": {
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v1/120000/08FCB2ED-176B-064B-85AB-37B898773B98.root": "Events",
            },
        }
    }


@pytest.mark.slow
@pytest.mark.slow
def test_metrics_collector_e2e_basic(dask_cluster, test_fileset, tmp_path):
    """Test full workflow with MetricsCollector."""
    # Create processor
    test_processor = DummyProcessor()

    # Use MetricsCollector context manager
    with MetricsCollector(
        client=dask_cluster, track_workers=True, worker_tracking_interval=0.5
    ) as collector:
        # Run coffea workflow
        executor = processor.DaskExecutor(client=dask_cluster)
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            chunksize=10_000,
            savemetrics=True,  # IMPORTANT: Returns (output, report) tuple
        )

        _output, report = runner(
            test_fileset, processor_instance=test_processor, treename="Events"
        )

        # Set the coffea report
        collector.set_coffea_report(report)

    # After context exit, metrics should be aggregated
    metrics = collector.get_metrics()

    # Verify core metrics are present
    assert "wall_time" in metrics
    assert "total_events" in metrics
    assert "overall_rate_gbps" in metrics
    assert "avg_workers" in metrics
    assert "peak_workers" in metrics

    # Verify values are reasonable
    assert metrics["wall_time"] > 0
    assert metrics["total_events"] > 0
    assert metrics["avg_workers"] >= 1
    assert metrics["peak_workers"] >= 1

    # Test save_measurement
    measurement_path = collector.save_measurement(
        output_dir=tmp_path, measurement_name="test_e2e"
    )

    assert measurement_path.exists()
    assert (measurement_path / "metrics.json").exists()
    assert (measurement_path / "start_end_time.txt").exists()
    assert (measurement_path / "metadata.json").exists()


@pytest.mark.slow
def test_metrics_collector_e2e_with_custom_metrics(dask_cluster, test_fileset):
    """Test workflow with custom per-dataset metrics."""
    test_processor = DummyProcessor()

    with MetricsCollector(client=dask_cluster, track_workers=True) as collector:
        executor = processor.DaskExecutor(client=dask_cluster)
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            chunksize=10_000,
            savemetrics=True,
        )

        _output, report = runner(
            test_fileset, processor_instance=test_processor, treename="Events"
        )

        # Create custom metrics
        custom_metrics = {
            "test_dataset": {
                "entries": report.get("entries", 0),
                "duration": report.get("processtime", 1.0),
                "performance_counters": {
                    "num_requested_bytes": report.get("bytesread", 0),
                },
            }
        }

        collector.set_coffea_report(report, custom_metrics=custom_metrics)

    metrics = collector.get_metrics()

    # Verify dataset-specific metrics are aggregated
    assert "total_events" in metrics
    assert metrics["total_events"] == report.get("entries", 0)


@pytest.mark.slow
def test_metrics_collector_e2e_print_summary(dask_cluster, test_fileset, capsys):
    """Test print_summary() produces Rich table output."""
    test_processor = DummyProcessor()

    with MetricsCollector(client=dask_cluster) as collector:
        executor = processor.DaskExecutor(client=dask_cluster)
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            chunksize=10_000,
            savemetrics=True,
        )

        _output, report = runner(
            test_fileset, processor_instance=test_processor, treename="Events"
        )
        collector.set_coffea_report(report)

    # Print summary
    collector.print_summary()

    # Capture output
    captured = capsys.readouterr()

    # Verify Rich tables are printed
    assert "Throughput Metrics" in captured.out
    assert "Event Processing Metrics" in captured.out
    assert "Resource Utilization" in captured.out
    assert "Timing Breakdown" in captured.out


@pytest.mark.slow
def test_metrics_collector_e2e_no_worker_tracking(dask_cluster, test_fileset):
    """Test with worker tracking disabled."""
    test_processor = DummyProcessor()

    with MetricsCollector(client=dask_cluster, track_workers=False) as collector:
        executor = processor.DaskExecutor(client=dask_cluster)
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            chunksize=10_000,
            savemetrics=True,
        )

        _output, report = runner(
            test_fileset, processor_instance=test_processor, treename="Events"
        )
        collector.set_coffea_report(report)

    metrics = collector.get_metrics()

    # Worker metrics should be None when tracking disabled
    assert metrics.get("avg_workers") is None
    assert metrics.get("peak_workers") is None
    assert metrics.get("total_cores") is None

    # But workflow metrics should still be present
    assert "wall_time" in metrics
    assert "total_events" in metrics
    assert "overall_rate_gbps" in metrics


class ChunkTrackingProcessor(processor.ProcessorABC):
    """Processor with chunk-level instrumentation for E2E testing."""

    @track_metrics
    def process(self, events):
        """Process events with chunk tracking."""
        output = {}

        with track_time("event_selection"):
            # Simple selection
            selected = events[events.event.event % 2 == 0]

        with track_memory("load_jets"):
            # Load jets branch
            jets = events.Jet

        output["sum"] = len(selected)
        output["num_jets"] = len(jets)
        return output

    def postprocess(self, accumulator):
        """No postprocessing needed."""
        return accumulator


@pytest.mark.slow
def test_metrics_collector_e2e_with_chunk_tracking(dask_cluster, test_fileset):
    """Test full workflow with chunk-level instrumentation."""
    test_processor = ChunkTrackingProcessor()

    with MetricsCollector(
        client=dask_cluster,
        track_workers=True,
        processor_instance=test_processor,
    ) as collector:
        executor = processor.DaskExecutor(client=dask_cluster)
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            chunksize=10_000,
            savemetrics=True,
        )

        _output, report = runner(
            test_fileset, processor_instance=test_processor, treename="Events"
        )
        collector.set_coffea_report(report)

    metrics = collector.get_metrics()

    # Verify chunk metrics are present
    assert "num_chunks" in metrics
    assert metrics["num_chunks"] > 0
    assert metrics["num_successful_chunks"] > 0
    assert metrics["num_failed_chunks"] == 0

    # Verify timing statistics
    assert "chunk_duration_mean" in metrics
    assert "chunk_duration_min" in metrics
    assert "chunk_duration_max" in metrics
    assert metrics["chunk_duration_mean"] > 0

    # Verify event counts from chunks
    assert "total_events_from_chunks" in metrics
    assert metrics["total_events_from_chunks"] > 0

    # Verify per-dataset breakdown
    assert "per_dataset" in metrics
    assert "test_dataset" in metrics["per_dataset"]
    assert metrics["per_dataset"]["test_dataset"]["num_chunks"] > 0

    # Verify section metrics
    assert "sections" in metrics
    assert "event_selection" in metrics["sections"]
    assert "load_jets" in metrics["sections"]
    assert metrics["sections"]["event_selection"]["type"] == "time"
    assert metrics["sections"]["load_jets"]["type"] == "memory"

    # Verify raw chunk data is preserved
    assert "raw_chunk_metrics" in metrics
    assert len(metrics["raw_chunk_metrics"]) == metrics["num_chunks"]

    # Verify raw section data is preserved
    assert "raw_section_metrics" in metrics
    assert len(metrics["raw_section_metrics"]) > 0


@pytest.mark.slow
def test_metrics_collector_e2e_chunk_tracking_print_summary(
    dask_cluster, test_fileset, capsys
):
    """Test that chunk metrics appear in print_summary()."""
    test_processor = ChunkTrackingProcessor()

    with MetricsCollector(
        client=dask_cluster, processor_instance=test_processor
    ) as collector:
        executor = processor.DaskExecutor(client=dask_cluster)
        runner = processor.Runner(
            executor=executor,
            schema=NanoAODSchema,
            chunksize=10_000,
            savemetrics=True,
        )

        _output, report = runner(
            test_fileset, processor_instance=test_processor, treename="Events"
        )
        collector.set_coffea_report(report)

    # Print summary
    collector.print_summary()

    # Capture output
    captured = capsys.readouterr()

    # Verify chunk metrics table is printed
    assert "Chunk Metrics" in captured.out
    assert "Total Chunks" in captured.out
    assert "Mean Chunk Time" in captured.out

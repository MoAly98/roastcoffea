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

from roastcoffea import MetricsCollector

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

"""Pytest fixtures for roastcoffea tests.

Provides shared fixtures for testing metrics collection with Dask.
"""

from __future__ import annotations

import pytest
from dask.distributed import Client, LocalCluster


@pytest.fixture(scope="session")
def local_cluster():
    """Shared Dask LocalCluster for all tests.

    Creates a minimal cluster with 2 workers, 1 thread per worker.
    This is sufficient for testing metrics collection without overwhelming
    the test machine.

    Yields
    ------
    Client
        Connected Dask client.
    """
    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        dashboard_address=None,  # Disable dashboard for tests
        silence_logs=True,
    )
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


@pytest.fixture
def sample_processor():
    """Simple processor for testing metrics collection.

    Returns
    -------
    processor.ProcessorABC
        A minimal processor that counts events.
    """
    from coffea import processor

    class TestProcessor(processor.ProcessorABC):
        def process(self, events):
            """Process events and return count."""
            return {"count": len(events)}

        def postprocess(self, accumulator):
            """Identity postprocessing."""
            return accumulator

    return TestProcessor()


@pytest.fixture
def instrumented_processor():
    """Processor with @track_metrics decorator for testing.

    Returns
    -------
    processor.ProcessorABC
        A processor with metrics tracking enabled.
    """
    from coffea import processor

    # Import will fail until decorator is implemented
    # This is expected for TDD - test will fail until we implement the feature
    try:
        from roastcoffea.decorator import track_metrics
        from roastcoffea.instrumentation import track_memory, track_section

        class InstrumentedProcessor(processor.ProcessorABC):
            @track_metrics
            def process(self, events):
                """Process events with instrumentation."""
                with track_section(self, "counting"):
                    n = len(events)

                with track_memory(self, "result_creation"):
                    result = {"count": n}

                return result

            def postprocess(self, accumulator):
                """Identity postprocessing."""
                return accumulator

        return InstrumentedProcessor()
    except ImportError:
        # Decorator not yet implemented - return None for now
        return None


@pytest.fixture
def sample_fileset():
    """Mock NanoAOD fileset for testing.

    Returns
    -------
    dict
        Fileset dictionary with test dataset.
    """
    # For now, return a minimal structure
    # In real E2E tests, we'll use actual ROOT files from scikit-hep-testdata
    return {
        "TestDataset": {
            "files": {
                "https://example.com/test.root": "Events",
            },
        },
    }


@pytest.fixture
def metrics_config():
    """Standard metrics configuration for tests.

    Returns
    -------
    dict
        Configuration dict with sensible test defaults.
    """
    return {
        "enable": True,
        "track_workers": True,
        "worker_tracking_interval": 0.5,  # Faster sampling for tests
        "save_measurements": False,  # Don't clutter test directory
        "generate_plots": False,  # Skip plots in tests
        "generate_html_tables": False,  # Skip HTML in tests
    }


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Temporary directory for test outputs.

    Parameters
    ----------
    tmp_path : Path
        Pytest's tmp_path fixture.

    Returns
    -------
    Path
        Path to benchmarks directory within tmp_path.
    """
    output_dir = tmp_path / "benchmarks"
    output_dir.mkdir()
    return output_dir

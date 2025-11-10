"""Tests for backend architecture and DaskMetricsBackend."""

import time

import pytest

from roastcoffea.backends.base import AbstractMetricsBackend
from roastcoffea.backends.dask import DaskMetricsBackend


class TestAbstractMetricsBackend:
    """Test that AbstractMetricsBackend enforces interface."""

    def test_abstract_backend_cannot_instantiate(self):
        """AbstractMetricsBackend cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            AbstractMetricsBackend()

    def test_abstract_backend_requires_all_methods(self):
        """Subclass must implement all abstract methods."""

        class IncompleteBackend(AbstractMetricsBackend):
            """Backend missing implementations."""

        with pytest.raises(TypeError):
            IncompleteBackend()


class TestDaskMetricsBackend:
    """Test DaskMetricsBackend implementation."""

    def test_instantiate_with_client(self, local_cluster):
        """Can instantiate DaskMetricsBackend with client."""
        backend = DaskMetricsBackend(client=local_cluster)
        assert isinstance(backend, AbstractMetricsBackend)
        assert backend.client is local_cluster

    def test_instantiate_without_client_raises(self):
        """DaskMetricsBackend requires a client."""
        with pytest.raises(ValueError, match="client"):
            DaskMetricsBackend(client=None)

    def test_start_tracking_initializes_scheduler_state(self, local_cluster):
        """start_tracking initializes tracking on scheduler."""
        backend = DaskMetricsBackend(client=local_cluster)
        backend.start_tracking(interval=0.5)

        # Verify scheduler has tracking state
        result = local_cluster.run_on_scheduler(
            lambda dask_scheduler: hasattr(dask_scheduler, "track_count")
        )
        assert result is True

    def test_start_stop_tracking_returns_data(self, local_cluster):
        """start_tracking and stop_tracking return proper data structure."""
        backend = DaskMetricsBackend(client=local_cluster)

        # Start tracking
        backend.start_tracking(interval=0.5)
        time.sleep(1.5)  # Let it collect some samples

        # Stop tracking
        tracking_data = backend.stop_tracking()

        # Verify data structure
        assert isinstance(tracking_data, dict)
        assert "worker_counts" in tracking_data
        assert "worker_memory" in tracking_data
        assert "worker_memory_limit" in tracking_data
        assert "worker_active_tasks" in tracking_data
        assert "worker_cores" in tracking_data

        # Should have collected at least 2 samples
        assert len(tracking_data["worker_counts"]) >= 2

        # worker_cores should be captured for each worker
        assert tracking_data["worker_cores"]
        for worker_id, cores_timeline in tracking_data["worker_cores"].items():
            assert cores_timeline  # Should have samples
            # Cores should be > 0
            assert all(cores > 0 for _, cores in cores_timeline)

    def test_tracking_captures_worker_count(self, local_cluster):
        """Tracking captures correct worker count."""
        backend = DaskMetricsBackend(client=local_cluster)

        backend.start_tracking(interval=0.2)
        time.sleep(0.5)
        tracking_data = backend.stop_tracking()

        # Verify worker counts match cluster
        num_workers = len(local_cluster.scheduler_info()["workers"])
        worker_counts = tracking_data["worker_counts"]

        # All recorded counts should match cluster size
        for count in worker_counts.values():
            assert count == num_workers

    def test_tracking_captures_memory_data(self, local_cluster):
        """Tracking captures worker memory data."""
        backend = DaskMetricsBackend(client=local_cluster)

        backend.start_tracking(interval=0.2)
        time.sleep(0.5)
        tracking_data = backend.stop_tracking()

        worker_memory = tracking_data["worker_memory"]
        worker_memory_limit = tracking_data["worker_memory_limit"]

        # Should have memory data for each worker
        num_workers = len(local_cluster.scheduler_info()["workers"])
        assert len(worker_memory) == num_workers
        assert len(worker_memory_limit) == num_workers

        # Each worker should have timeline data
        for _worker_id, timeline in worker_memory.items():
            assert len(timeline) >= 2  # At least 2 samples
            # Each sample is (timestamp, memory_bytes)
            for _timestamp, memory_bytes in timeline:
                assert memory_bytes >= 0

        # Memory limits should be positive
        for _worker_id, timeline in worker_memory_limit.items():
            for _timestamp, limit_bytes in timeline:
                assert limit_bytes > 0

    def test_tracking_captures_active_tasks(self, local_cluster):
        """Tracking captures active task counts."""
        backend = DaskMetricsBackend(client=local_cluster)

        backend.start_tracking(interval=0.2)
        time.sleep(0.5)
        tracking_data = backend.stop_tracking()

        worker_active_tasks = tracking_data["worker_active_tasks"]

        # Should have task data for each worker
        num_workers = len(local_cluster.scheduler_info()["workers"])
        assert len(worker_active_tasks) == num_workers

        # Each worker should have timeline data
        for _worker_id, timeline in worker_active_tasks.items():
            assert len(timeline) >= 2
            # Each sample is (timestamp, num_active_tasks)
            for _timestamp, num_tasks in timeline:
                assert num_tasks >= 0

    def test_multiple_start_stop_cycles(self, local_cluster):
        """Can start and stop tracking multiple times."""
        backend = DaskMetricsBackend(client=local_cluster)

        # Cycle 1
        backend.start_tracking(interval=0.2)
        time.sleep(0.5)
        data1 = backend.stop_tracking()
        assert len(data1["worker_counts"]) >= 2

        # Cycle 2
        backend.start_tracking(interval=0.2)
        time.sleep(0.5)
        data2 = backend.stop_tracking()
        assert len(data2["worker_counts"]) >= 2

        # Data should be independent
        assert data1["worker_counts"] != data2["worker_counts"]

    def test_supports_fine_metrics_returns_true(self, local_cluster):
        """DaskMetricsBackend supports fine-grained metrics via Spans."""
        backend = DaskMetricsBackend(client=local_cluster)
        assert backend.supports_fine_metrics() is True

    def test_create_span_returns_span_id(self, local_cluster):
        """create_span returns a valid span identifier."""
        backend = DaskMetricsBackend(client=local_cluster)
        span = backend.create_span(name="test_operation")

        # Should return some identifier (implementation-dependent)
        assert span is not None

    def test_get_span_metrics_returns_dict(self, local_cluster):
        """get_span_metrics returns metrics dictionary for span."""
        backend = DaskMetricsBackend(client=local_cluster)
        span = backend.create_span(name="test_operation")

        # Do some work
        def dummy_work(x):
            return x * 2

        local_cluster.submit(dummy_work, 42).result()

        # Get span metrics
        metrics = backend.get_span_metrics(span)
        assert isinstance(metrics, dict)

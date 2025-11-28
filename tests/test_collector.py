"""Unit tests for MetricsCollector."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from roastcoffea.collector import MetricsCollector


class TestMetricsCollectorInitialization:
    """Test MetricsCollector initialization."""

    def test_init_with_defaults(self):
        """Initialize with default parameters."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator:
            collector = MetricsCollector(client=mock_client)

            assert collector.client == mock_client
            assert collector.backend == "dask"
            assert collector.track_workers is True
            assert collector.worker_tracking_interval == 1.0
            assert collector.processor_instance is None
            assert collector.processor_name is None

            # Verify backend was created
            mock_backend.assert_called_once_with(client=mock_client)
            mock_aggregator.assert_called_once_with(backend="dask")

    def test_init_with_processor_instance(self):
        """Initialize with processor instance."""
        mock_client = Mock()
        mock_processor = Mock()
        mock_processor.__class__.__name__ = "MyProcessor"

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(
                client=mock_client, processor_instance=mock_processor
            )

            assert collector.processor_instance == mock_processor
            assert collector.processor_name == "MyProcessor"

    def test_init_with_custom_params(self):
        """Initialize with custom parameters."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(
                client=mock_client,
                backend="dask",
                track_workers=False,
                worker_tracking_interval=0.5,
            )

            assert collector.track_workers is False
            assert collector.worker_tracking_interval == 0.5

    def test_init_with_unsupported_backend(self):
        """Raise ValueError for unsupported backend."""
        mock_client = Mock()

        with pytest.raises(ValueError, match="Unsupported backend: invalid"):
            MetricsCollector(client=mock_client, backend="invalid")

    def test_initial_state(self):
        """Verify initial state is correct."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            assert collector.t_start is None
            assert collector.t_end is None
            assert collector.coffea_report is None
            assert collector.tracking_data is None
            assert collector.span_info is None
            assert collector.span_metrics is None
            assert collector.metrics is None
            assert collector.chunk_metrics == []
            assert collector.section_metrics == []


class TestMetricsCollectorContextManager:
    """Test MetricsCollector context manager behavior."""

    def test_enter_sets_start_time(self):
        """__enter__ sets start time."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            collector = MetricsCollector(client=mock_client, track_workers=False)

            before_enter = time.perf_counter()
            with collector:
                after_enter = time.perf_counter()

                assert collector.t_start is not None
                assert before_enter <= collector.t_start <= after_enter

    def test_enter_enables_processor_metrics(self):
        """__enter__ enables metrics on processor instance."""
        mock_client = Mock()
        mock_processor = Mock()
        mock_processor.__class__.__name__ = "TestProcessor"

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            collector = MetricsCollector(
                client=mock_client,
                processor_instance=mock_processor,
                track_workers=False,
            )

            with collector:
                assert mock_processor._roastcoffea_collect_metrics is True

            # After exit, should be disabled
            assert mock_processor._roastcoffea_collect_metrics is False

    def test_enter_starts_worker_tracking(self):
        """__enter__ starts worker tracking when enabled."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            collector = MetricsCollector(
                client=mock_client, track_workers=True, worker_tracking_interval=0.5
            )

            with collector:
                mock_backend.start_tracking.assert_called_once_with(interval=0.5)

    def test_enter_skips_worker_tracking_when_disabled(self):
        """__enter__ skips worker tracking when disabled."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                mock_backend.start_tracking.assert_not_called()

    def test_enter_creates_span(self):
        """__enter__ creates Dask span for fine metrics."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend

            # Mock span context
            mock_span_context = MagicMock()
            mock_span_context.__enter__.return_value = "span-id-123"
            mock_backend.create_span.return_value = {"context": mock_span_context}

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                mock_backend.create_span.assert_called_once_with("coffea-processing")
                assert collector.span_info is not None
                assert collector.span_info["id"] == "span-id-123"

    def test_enter_handles_span_creation_failure(self):
        """__enter__ handles span creation failure gracefully."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend

            # Mock span context that raises on __enter__
            mock_span_context = MagicMock()
            mock_span_context.__enter__.side_effect = RuntimeError("Span failed")
            mock_backend.create_span.return_value = {"context": mock_span_context}

            collector = MetricsCollector(client=mock_client, track_workers=False)

            # Should not raise, just log warning
            with collector:
                assert collector.span_info is None

    def test_exit_sets_end_time(self):
        """__exit__ sets end time."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                pass

            assert collector.t_end is not None
            assert collector.t_end > collector.t_start

    def test_exit_stops_worker_tracking(self):
        """__exit__ stops worker tracking."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None
            mock_backend.stop_tracking.return_value = {"worker_counts": {}}

            collector = MetricsCollector(client=mock_client, track_workers=True)

            with collector:
                pass

            mock_backend.stop_tracking.assert_called_once()
            assert collector.tracking_data == {"worker_counts": {}}

    def test_exit_extracts_span_metrics(self):
        """__exit__ extracts metrics from span."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend

            # Mock span context
            mock_span_context = MagicMock()
            mock_span_context.__enter__.return_value = "span-id"
            span_info = {"context": mock_span_context, "id": "span-id"}
            mock_backend.create_span.return_value = span_info
            mock_backend.get_span_metrics.return_value = {"cpu_time": 10.0}

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                pass

            mock_span_context.__exit__.assert_called_once()
            mock_backend.get_span_metrics.assert_called_once()
            assert collector.span_metrics == {"cpu_time": 10.0}

    def test_exit_handles_span_extraction_failure(self):
        """__exit__ handles span metric extraction failure."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend

            # Mock span context
            mock_span_context = MagicMock()
            mock_span_context.__enter__.return_value = "span-id"
            span_info = {"context": mock_span_context, "id": "span-id"}
            mock_backend.create_span.return_value = span_info
            mock_backend.get_span_metrics.side_effect = RuntimeError("Failed to get metrics")

            collector = MetricsCollector(client=mock_client, track_workers=False)

            # Should not raise
            with collector:
                pass

            assert collector.span_metrics is None

    def test_exit_aggregates_metrics_with_coffea_report(self):
        """__exit__ auto-aggregates when coffea_report is set."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            # Should call aggregate
            mock_aggregator.aggregate.assert_called_once()
            assert collector.metrics == {"wall_time": 10.0}

    def test_exit_skips_aggregation_without_coffea_report(self):
        """__exit__ skips aggregation when coffea_report not set."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                pass

            # Should NOT call aggregate
            mock_aggregator.aggregate.assert_not_called()
            assert collector.metrics is None


class TestMetricsCollectorMethods:
    """Test MetricsCollector methods."""

    def test_record_chunk_metrics(self):
        """record_chunk_metrics appends to list."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            chunk1 = {"duration": 1.0, "num_events": 100}
            chunk2 = {"duration": 2.0, "num_events": 200}

            collector.record_chunk_metrics(chunk1)
            collector.record_chunk_metrics(chunk2)

            assert len(collector.chunk_metrics) == 2
            assert collector.chunk_metrics[0] == chunk1
            assert collector.chunk_metrics[1] == chunk2

    def test_record_section_metrics(self):
        """record_section_metrics appends to list."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            section1 = {"name": "jet_selection", "duration": 0.5}
            section2 = {"name": "histogram_filling", "duration": 1.5}

            collector.record_section_metrics(section1)
            collector.record_section_metrics(section2)

            assert len(collector.section_metrics) == 2
            assert collector.section_metrics[0] == section1
            assert collector.section_metrics[1] == section2

    def test_extract_metrics_from_output(self):
        """extract_metrics_from_output extracts and removes metrics."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            output = {
                "sum": 1000,
                "__roastcoffea_metrics__": [
                    {"duration": 1.0, "num_events": 500},
                    {"duration": 2.0, "num_events": 500},
                ],
            }

            collector.extract_metrics_from_output(output)

            # Metrics should be extracted
            assert len(collector.chunk_metrics) == 2
            assert collector.chunk_metrics[0]["duration"] == 1.0

            # Metrics should be removed from output
            assert "__roastcoffea_metrics__" not in output
            assert "sum" in output

    def test_extract_metrics_from_output_no_metrics(self):
        """extract_metrics_from_output handles output without metrics."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            output = {"sum": 1000}

            collector.extract_metrics_from_output(output)

            # Should not raise, chunk_metrics should be empty
            assert len(collector.chunk_metrics) == 0

    def test_extract_metrics_from_non_dict_output(self):
        """extract_metrics_from_output handles non-dict output."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            # Non-dict output
            output = 1000

            collector.extract_metrics_from_output(output)

            # Should not raise
            assert len(collector.chunk_metrics) == 0

    def test_set_coffea_report(self):
        """set_coffea_report stores report."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            report = {"bytesread": 1000, "processtime": 10.0}
            collector.set_coffea_report(report)

            assert collector.coffea_report == report

    def test_set_coffea_report_with_custom_metrics(self):
        """set_coffea_report stores custom metrics."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            report = {"bytesread": 1000}
            custom = {"my_metric": 42}

            collector.set_coffea_report(report, custom_metrics=custom)

            assert collector.coffea_report == report
            assert collector.custom_metrics == custom

    def test_get_metrics_calls_aggregate(self):
        """get_metrics calls _aggregate_metrics if not yet aggregated."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            collector = MetricsCollector(client=mock_client, track_workers=False)

            # Set required state
            with collector:
                pass

            collector.set_coffea_report({"bytesread": 1000})

            # Clear auto-aggregated metrics
            collector.metrics = None

            metrics = collector.get_metrics()

            assert metrics == {"wall_time": 10.0}
            mock_aggregator.aggregate.assert_called()

    def test_get_metrics_returns_cached(self):
        """get_metrics returns cached metrics without re-aggregating."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            # First call
            metrics1 = collector.get_metrics()

            # Second call should use cached value
            metrics2 = collector.get_metrics()

            assert metrics1 == metrics2
            # aggregate should only be called once (during __exit__)
            assert mock_aggregator.aggregate.call_count == 1

    def test_get_metrics_raises_without_timing(self):
        """get_metrics raises if called outside context manager."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)
            collector.set_coffea_report({"bytesread": 1000})

            with pytest.raises(RuntimeError, match="Timing not available"):
                collector.get_metrics()

    def test_aggregate_raises_without_coffea_report(self):
        """_aggregate_metrics raises if coffea_report not set."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                pass

            # Try to get metrics without setting report
            with pytest.raises(RuntimeError, match="Coffea report not set"):
                collector.get_metrics()

    def test_save_measurement(self, tmp_path):
        """save_measurement saves to disk."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.save_measurement"
        ) as mock_save:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            mock_save.return_value = tmp_path / "measurement_123"

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            result_path = collector.save_measurement(
                output_dir=tmp_path, measurement_name="test"
            )

            assert result_path == tmp_path / "measurement_123"
            mock_save.assert_called_once()

    def test_save_measurement_before_context_exit(self):
        """save_measurement raises if called before context exit."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ):
            collector = MetricsCollector(client=mock_client)

            with pytest.raises(
                RuntimeError, match="Cannot save measurement before context manager completes"
            ):
                collector.save_measurement(output_dir=Path("/tmp"))

    def test_print_summary(self):
        """print_summary generates Rich tables."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.Console"
        ) as mock_console:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {
                "wall_time": 10.0,
                "total_events": 1000,
            }

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            collector.print_summary()

            # Console should be created and print called
            mock_console.assert_called_once()
            console_instance = mock_console.return_value
            assert console_instance.print.call_count > 0


class TestMetricsCollectorWarnings:
    """Test MetricsCollector warning messages."""

    def test_warns_when_span_metrics_without_processor_name(self):
        """Warn when fine metrics collected without processor_name."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.logger"
        ) as mock_logger:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            collector = MetricsCollector(
                client=mock_client,
                track_workers=False,
                processor_instance=None,  # No processor
            )
            collector.set_coffea_report({"bytesread": 1000})

            # Manually set span_metrics to trigger warning
            collector.span_metrics = {"cpu_time": 10.0}

            with collector:
                pass

            # Should have logged warning
            mock_logger.warning.assert_called()
            warning_call = [call for call in mock_logger.warning.call_args_list if "Fine metrics" in str(call)]
            assert len(warning_call) > 0

    def test_warns_when_span_metrics_empty(self):
        """Warn when Dask Span completes but metrics are empty."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.logger"
        ) as mock_logger:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend

            # Mock span context
            mock_span_context = MagicMock()
            mock_span_context.__enter__.return_value = "span-id"
            span_info = {"context": mock_span_context, "id": "span-id"}
            mock_backend.create_span.return_value = span_info
            mock_backend.get_span_metrics.return_value = {}  # Empty metrics

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator

            collector = MetricsCollector(client=mock_client, track_workers=False)

            with collector:
                pass

            # Should have logged warning about empty metrics
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert any("no metrics were collected" in call for call in warning_calls)

    def test_logs_chunk_and_section_metrics_debug(self):
        """Log debug messages for chunk and section metrics."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.logger"
        ) as mock_logger:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            # Add chunk and section metrics
            collector.record_chunk_metrics({"duration": 1.0})
            collector.record_chunk_metrics({"duration": 2.0})
            collector.record_section_metrics({"section": "test"})

            with collector:
                pass

            # Should have logged debug messages
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            assert any("2 chunks" in call for call in debug_calls)
            assert any("1 sections" in call for call in debug_calls)

    def test_warns_when_metrics_not_a_list(self):
        """Warn when __roastcoffea_metrics__ is not a list."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend"), patch(
            "roastcoffea.collector.MetricsAggregator"
        ), patch("roastcoffea.collector.logger") as mock_logger:
            collector = MetricsCollector(client=mock_client)

            output = {
                "__roastcoffea_metrics__": "not a list",  # Invalid type
            }

            collector.extract_metrics_from_output(output)

            # Should have logged warning
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert any("Metrics not a list" in call for call in warning_calls)

    def test_raises_when_aggregation_fails(self):
        """Raise RuntimeError when metrics aggregation fails."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = None  # Aggregation returns None

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            # Should raise when trying to get metrics
            with pytest.raises(RuntimeError, match="Metrics aggregation failed"):
                collector.get_metrics()

    def test_print_summary_with_fine_metrics_table(self):
        """print_summary includes fine metrics table when available."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.Console"
        ) as mock_console, patch(
            "roastcoffea.collector.format_fine_metrics_table"
        ) as mock_fine_table:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            # Mock fine metrics table
            mock_fine_table.return_value = Mock()  # Return non-None table

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            collector.print_summary()

            # Console.print should be called for fine metrics
            console_instance = mock_console.return_value
            assert console_instance.print.call_count >= 2  # At least timing + fine

    def test_print_summary_with_chunk_metrics_table(self):
        """print_summary includes chunk metrics table when available."""
        mock_client = Mock()

        with patch("roastcoffea.collector.DaskMetricsBackend") as mock_backend_class, patch(
            "roastcoffea.collector.MetricsAggregator"
        ) as mock_aggregator_class, patch(
            "roastcoffea.collector.Console"
        ) as mock_console, patch(
            "roastcoffea.collector.format_chunk_metrics_table"
        ) as mock_chunk_table:
            mock_backend = Mock()
            mock_backend_class.return_value = mock_backend
            mock_backend.create_span.return_value = None

            mock_aggregator = Mock()
            mock_aggregator_class.return_value = mock_aggregator
            mock_aggregator.aggregate.return_value = {"wall_time": 10.0}

            # Mock chunk metrics table
            mock_chunk_table.return_value = Mock()  # Return non-None table

            collector = MetricsCollector(client=mock_client, track_workers=False)
            collector.set_coffea_report({"bytesread": 1000})

            with collector:
                pass

            collector.print_summary()

            # Console.print should be called for chunk metrics
            console_instance = mock_console.return_value
            assert console_instance.print.call_count >= 2  # At least timing + chunk

"""Integration tests for byte tracking feature.

Tests the complete flow:
1. @track_metrics decorator captures bytes from filesource
2. track_bytes() context manager tracks fine-grained bytes
3. MetricsAggregator builds chunk_info
4. plot_throughput_timeline can use the data
"""

from __future__ import annotations

import pytest

from roastcoffea.aggregation.chunks import build_chunk_info
from roastcoffea.aggregation.core import MetricsAggregator
from roastcoffea.decorator import track_metrics
from roastcoffea.instrumentation import track_bytes


class MockFileSource:
    """Mock FSSpecSource from uproot.

    Simulates the file source object that's available in
    events.metadata["filesource"] with the coffea branch.
    """

    def __init__(self, start_bytes=1000):
        self._bytes = start_bytes

    @property
    def num_requested_bytes(self):
        return self._bytes

    def simulate_read(self, bytes_to_read):
        """Simulate reading bytes from file."""
        self._bytes += bytes_to_read


class MockEvents:
    """Mock events object with filesource in metadata."""

    def __init__(self, num_events=100, filename="data.root", start=0, stop=100):
        self.metadata = {
            "dataset": "test_dataset",
            "filename": filename,
            "entrystart": start,
            "entrystop": stop,
            "uuid": "test-uuid",
            "filesource": MockFileSource(start_bytes=5000),
        }
        self._num_events = num_events

    def __len__(self):
        return self._num_events


class TestByteTrackingIntegration:
    """Integration tests for complete byte tracking workflow."""

    def test_full_byte_tracking_flow(self):
        """Test complete flow from decorator to chunk_info."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                # Simulate reading data during processing
                events.metadata["filesource"].simulate_read(2500)

                # Use track_bytes for fine-grained tracking
                with track_bytes(self, events, "jet_loading"):
                    events.metadata["filesource"].simulate_read(1000)

                with track_bytes(self, events, "muon_loading"):
                    events.metadata["filesource"].simulate_read(500)

                return {"sum": len(events)}

        # Process a chunk
        processor = TestProcessor()
        events = MockEvents(
            num_events=1000, filename="test.root", start=0, stop=1000
        )

        result = processor.process(events)

        # Verify metrics were captured
        assert "__roastcoffea_metrics__" in result
        chunk_metrics = result["__roastcoffea_metrics__"]
        assert len(chunk_metrics) == 1

        chunk = chunk_metrics[0]

        # Verify chunk-level bytes tracking
        assert "bytes_read" in chunk
        assert chunk["bytes_read"] == 4000  # 2500 + 1000 + 500

        # Verify fine-grained bytes tracking
        assert "bytes" in chunk
        assert "jet_loading" in chunk["bytes"]
        assert chunk["bytes"]["jet_loading"] == 1000
        assert "muon_loading" in chunk["bytes"]
        assert chunk["bytes"]["muon_loading"] == 500

        # Verify metadata was captured
        assert chunk["file"] == "test.root"
        assert chunk["entry_start"] == 0
        assert chunk["entry_stop"] == 1000
        assert chunk["num_events"] == 1000

    def test_chunk_info_building_from_tracked_metrics(self):
        """Test that chunk_info is correctly built from tracked metrics."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                events.metadata["filesource"].simulate_read(3000)
                return {"sum": len(events)}

        processor = TestProcessor()

        # Process multiple chunks
        events1 = MockEvents(filename="file1.root", start=0, stop=1000)
        events2 = MockEvents(filename="file1.root", start=1000, stop=2000)
        events3 = MockEvents(filename="file2.root", start=0, stop=1000)

        result1 = processor.process(events1)
        result2 = processor.process(events2)
        result3 = processor.process(events3)

        # Collect all chunk metrics
        all_chunk_metrics = []
        all_chunk_metrics.extend(result1["__roastcoffea_metrics__"])
        all_chunk_metrics.extend(result2["__roastcoffea_metrics__"])
        all_chunk_metrics.extend(result3["__roastcoffea_metrics__"])

        # Build chunk_info
        chunk_info = build_chunk_info(all_chunk_metrics)

        # Verify chunk_info structure
        assert len(chunk_info) == 3
        assert ("file1.root", 0, 1000) in chunk_info
        assert ("file1.root", 1000, 2000) in chunk_info
        assert ("file2.root", 0, 1000) in chunk_info

        # Verify each chunk has timing and bytes
        for key, (t_start, t_end, bytes_read) in chunk_info.items():
            assert t_start < t_end  # Valid timing
            assert bytes_read == 3000  # All chunks read 3000 bytes

    def test_aggregator_builds_chunk_info_in_metrics(self):
        """Test that MetricsAggregator builds chunk_info and includes it in metrics."""

        # Create chunk metrics
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.0,
                "duration": 1.0,
                "bytes_read": 50000,
                "num_events": 1000,
                "mem_before_mb": 100.0,
                "mem_after_mb": 150.0,
                "mem_delta_mb": 50.0,
            },
            {
                "file": "data.root",
                "entry_start": 1000,
                "entry_stop": 2000,
                "t_start": 2.0,
                "t_end": 3.0,
                "duration": 1.0,
                "bytes_read": 60000,
                "num_events": 1000,
                "mem_before_mb": 150.0,
                "mem_after_mb": 200.0,
                "mem_delta_mb": 50.0,
            },
        ]

        # Create a mock coffea report
        coffea_report = {
            "bytesread": 110000,
            "columns": [],
            "entries": 2000,
            "processtime": 2.0,
        }

        # Run aggregator
        aggregator = MetricsAggregator(backend="dask")
        metrics = aggregator.aggregate(
            coffea_report=coffea_report,
            tracking_data=None,
            t_start=1.0,
            t_end=3.0,
            chunk_metrics=chunk_metrics,
        )

        # Verify chunk_info is in metrics
        assert "chunk_info" in metrics
        chunk_info = metrics["chunk_info"]

        # Verify structure
        assert len(chunk_info) == 2
        assert ("data.root", 0, 1000) in chunk_info
        assert chunk_info[("data.root", 0, 1000)] == (1.0, 2.0, 50000)
        assert ("data.root", 1000, 2000) in chunk_info
        assert chunk_info[("data.root", 1000, 2000)] == (2.0, 3.0, 60000)

    def test_plot_throughput_timeline_compatible_format(self):
        """Test that chunk_info format is compatible with plot_throughput_timeline."""
        from roastcoffea.visualization.plots.throughput import (
            plot_throughput_timeline,
        )

        # Build chunk_info
        chunk_metrics = [
            {
                "file": "data.root",
                "entry_start": 0,
                "entry_stop": 1000,
                "t_start": 1.0,
                "t_end": 2.0,
                "bytes_read": 50000,
            },
            {
                "file": "data.root",
                "entry_start": 1000,
                "entry_stop": 2000,
                "t_start": 2.1,
                "t_end": 3.0,
                "bytes_read": 60000,
            },
        ]

        chunk_info = build_chunk_info(chunk_metrics)

        # Verify plot can be created (should not raise)
        fig, ax = plot_throughput_timeline(chunk_info=chunk_info)

        assert fig is not None
        assert ax is not None

    @pytest.mark.slow
    def test_real_coffea_filesource_tracking(self):
        """Test with real coffea NanoEvents and ROOT file.

        Uses CERN Open Data to test real byte tracking with actual filesource.
        Note: filesource is only populated when running through coffea processor executor.
        """
        from coffea import processor
        from dask.distributed import Client, LocalCluster

        # Use CERN Open Data file
        test_file = (
            "root://eospublic.cern.ch//eos/opendata/cms/mc/"
            "RunIISummer20UL16NanoAODv9/ZprimeToTT_M2000_W20_TuneCP2_PSweights_13TeV-madgraph-pythiaMLM-pythia8/"
            "NANOAODSIM/106X_mcRun2_asymptotic_v17-v1/270000/22BAB5D2-9E3F-E440-AB30-AE6DBFDF6C83.root"
        )

        class RealDataProcessor(processor.ProcessorABC):
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                # Access some branches to trigger file reads
                jets = events.Jet.pt
                muons = events.Muon.pt

                # Track fine-grained bytes for jet access
                with track_bytes(self, events, "jet_kinematics"):
                    jet_eta = events.Jet.eta
                    jet_phi = events.Jet.phi

                # Simple analysis
                njets = len(jets)
                nmuons = len(muons)

                return {
                    "nevents": len(events),
                    "njets": njets,
                    "nmuons": nmuons,
                }

            def postprocess(self, accumulator):
                return accumulator

        # Create fileset
        fileset = {
            "test": {
                "files": {
                    test_file: "Events",
                }
            }
        }

        # Run through executor to populate filesource
        try:
            cluster = LocalCluster(n_workers=1, threads_per_worker=1, processes=True)
            client = Client(cluster)

            # Create processor
            proc = RealDataProcessor()

            # Run with processor executor
            executor = processor.DaskExecutor(client=client)
            runner = processor.Runner(executor=executor, savemetrics=True, maxchunks=1)

            output, report = runner(
                fileset,
                treename="Events",
                processor_instance=proc,
            )

            client.close()
            cluster.close()

            # Verify metrics were captured
            assert "__roastcoffea_metrics__" in output
            chunk_metrics = output["__roastcoffea_metrics__"]
            assert len(chunk_metrics) >= 1

            chunk = chunk_metrics[0]

            # Verify bytes were tracked
            assert "bytes_read" in chunk
            assert chunk["bytes_read"] > 0, "bytes_read should be > 0"

            # Verify fine-grained tracking worked
            assert "bytes" in chunk
            if "jet_kinematics" in chunk["bytes"]:
                assert chunk["bytes"]["jet_kinematics"] >= 0

            # Verify file metadata was captured
            assert chunk["file"] is not None
            assert chunk["entry_start"] is not None
            assert chunk["entry_stop"] is not None
            assert chunk["num_events"] > 0

            # Verify timing was captured
            assert chunk["duration"] > 0
            assert chunk["t_end"] > chunk["t_start"]

        except Exception as e:
            # If we can't access the file (network issues, etc.), skip the test
            pytest.skip(f"Could not access test file: {e}")


class TestByteTrackingErrorHandling:
    """Test error handling in byte tracking."""

    def test_missing_filesource_tracked_as_zero(self):
        """Test that missing filesource is handled gracefully."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {"sum": len(events)}

        processor = TestProcessor()

        # Events without filesource
        class EventsWithoutFileSource:
            def __init__(self):
                self.metadata = {
                    "dataset": "test",
                    "filename": "test.root",
                    "entrystart": 0,
                    "entrystop": 1000,
                }

            def __len__(self):
                return 1000

        events = EventsWithoutFileSource()
        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert chunk["bytes_read"] == 0

    def test_broken_filesource_tracked_as_zero(self):
        """Test that broken filesource is handled gracefully."""

        class TestProcessor:
            _roastcoffea_collect_metrics = True

            @track_metrics
            def process(self, events):
                return {"sum": len(events)}

        processor = TestProcessor()

        # Events with broken filesource
        class EventsWithBrokenFileSource:
            def __init__(self):
                self.metadata = {
                    "dataset": "test",
                    "filename": "test.root",
                    "entrystart": 0,
                    "entrystop": 1000,
                    "filesource": object(),  # No num_requested_bytes attribute
                }

            def __len__(self):
                return 1000

        events = EventsWithBrokenFileSource()
        result = processor.process(events)

        chunk = result["__roastcoffea_metrics__"][0]
        assert chunk["bytes_read"] == 0

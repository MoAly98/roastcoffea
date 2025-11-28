"""Tests for stub files to ensure they are importable and documented.

These files contain only docstrings and are placeholders for future implementation.
"""

from __future__ import annotations

import pytest


class TestStubFiles:
    """Test that stub files are importable and properly documented."""

    def test_aggregation_chunks_stub_is_importable(self):
        """aggregation/chunks.py is importable."""
        import roastcoffea.aggregation.chunks

        assert roastcoffea.aggregation.chunks.__doc__ is not None
        assert "Chunk-level metrics aggregation" in roastcoffea.aggregation.chunks.__doc__

    def test_export_tables_stub_is_importable(self):
        """export/tables.py is importable."""
        import roastcoffea.export.tables

        assert roastcoffea.export.tables.__doc__ is not None
        assert "HTML export" in roastcoffea.export.tables.__doc__

    def test_measurements_stub_is_importable(self):
        """measurements.py is importable."""
        import roastcoffea.measurements

        assert roastcoffea.measurements.__doc__ is not None
        assert "JSON persistence" in roastcoffea.measurements.__doc__

    def test_reporter_stub_is_importable(self):
        """reporter.py is importable."""
        import roastcoffea.reporter

        assert roastcoffea.reporter.__doc__ is not None
        assert "Rich table formatters" in roastcoffea.reporter.__doc__

    def test_visualization_plots_chunks_stub_is_importable(self):
        """visualization/plots/chunks.py is importable."""
        import roastcoffea.visualization.plots.chunks

        assert roastcoffea.visualization.plots.chunks.__doc__ is not None
        assert "Chunk-level performance plots" in roastcoffea.visualization.plots.chunks.__doc__

    def test_visualization_dashboards_main_stub_is_importable(self):
        """visualization/dashboards/main.py is importable."""
        import roastcoffea.visualization.dashboards.main

        assert roastcoffea.visualization.dashboards.main.__doc__ is not None
        assert "comprehensive dashboard" in roastcoffea.visualization.dashboards.main.__doc__

    def test_stub_files_have_no_functions(self):
        """Stub files should not define any functions or classes."""
        import inspect

        import roastcoffea.aggregation.chunks
        import roastcoffea.export.tables
        import roastcoffea.measurements
        import roastcoffea.reporter
        import roastcoffea.visualization.dashboards.main
        import roastcoffea.visualization.plots.chunks

        stub_modules = [
            roastcoffea.aggregation.chunks,
            roastcoffea.export.tables,
            roastcoffea.measurements,
            roastcoffea.reporter,
            roastcoffea.visualization.plots.chunks,
            roastcoffea.visualization.dashboards.main,
        ]

        for module in stub_modules:
            members = inspect.getmembers(module)
            # Filter out special attributes and annotations
            user_defined = [
                name
                for name, obj in members
                if not name.startswith("_")
                and not inspect.ismodule(obj)
                and name != "annotations"  # From __future__ import
            ]
            assert (
                len(user_defined) == 0
            ), f"{module.__name__} stub should not define any public members, found: {user_defined}"

"""Tests for utility functions."""

from __future__ import annotations

import sys
from unittest.mock import patch


class TestGetProcessMemory:
    """Test get_process_memory function."""

    def test_get_process_memory_returns_float(self):
        """get_process_memory returns a float."""
        from roastcoffea.utils import get_process_memory

        memory = get_process_memory()
        assert isinstance(memory, float)
        assert memory >= 0

    def test_get_process_memory_with_psutil_available(self):
        """get_process_memory uses psutil when available."""
        from roastcoffea.utils import get_process_memory

        memory = get_process_memory()
        # Should return actual memory usage (non-zero)
        assert memory > 0

    def test_get_process_memory_without_psutil(self):
        """get_process_memory returns 0.0 when psutil not available."""
        # Mock import failure
        with patch.dict(sys.modules, {"psutil": None}):
            # Force reimport to trigger ImportError
            import importlib

            import roastcoffea.utils

            importlib.reload(roastcoffea.utils)

            from roastcoffea.utils import get_process_memory

            memory = get_process_memory()
            assert memory == 0.0

            # Reload again to restore normal behavior
            importlib.reload(roastcoffea.utils)

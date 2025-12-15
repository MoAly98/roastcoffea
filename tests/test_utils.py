"""Tests for utility functions."""

from __future__ import annotations

import sys
from unittest.mock import patch

from roastcoffea.utils import get_nested


class TestGetNested:
    """Test get_nested function for safe nested dict access."""

    def test_gets_nested_value(self):
        """get_nested returns nested value when present."""
        d = {"a": {"b": {"c": 42}}}
        assert get_nested(d, "a", "b", "c") == 42

    def test_returns_default_for_missing_key(self):
        """get_nested returns default when key is missing."""
        d = {"a": {"b": 1}}
        assert get_nested(d, "a", "missing") is None
        assert get_nested(d, "a", "missing", default=99) == 99

    def test_returns_default_for_none_dict(self):
        """get_nested returns default when dict is None."""
        assert get_nested(None, "a", "b") is None
        assert get_nested(None, "a", default={}) == {}

    def test_returns_default_for_non_dict(self):
        """get_nested returns default when intermediate value is not a dict."""
        d = {"a": 42}  # a is not a dict
        assert get_nested(d, "a", "b") is None

    def test_returns_empty_dict_default(self):
        """get_nested can return empty dict as default."""
        d = {"summary": {}}
        result = get_nested(d, "summary", "throughput", default={})
        assert result == {}

    def test_returns_zero_default(self):
        """get_nested can return 0 as default."""
        d = {}
        result = get_nested(d, "a", default=0)
        assert result == 0

    def test_works_with_single_key(self):
        """get_nested works with single key."""
        d = {"a": 42}
        assert get_nested(d, "a") == 42

    def test_typical_metrics_access(self):
        """get_nested works for typical metrics dict access."""
        metrics = {
            "summary": {
                "throughput": {"data_rate_gbps": 1.5},
                "events": {"total": 100000},
            }
        }
        assert get_nested(metrics, "summary", "throughput", "data_rate_gbps") == 1.5
        assert get_nested(metrics, "summary", "events", "total") == 100000
        assert get_nested(metrics, "summary", "missing", default={}) == {}


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

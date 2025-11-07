"""Decorator for chunk-level metrics tracking.

Provides @track_metrics decorator for automatic chunk boundary detection
and metrics collection in processor.process() methods.
"""

from __future__ import annotations

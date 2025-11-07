"""Dask backend for metrics collection.

Implements metrics collection for Dask executors, including:
- Worker resource tracking via scheduler sampling
- Fine-grained metrics via Dask Spans
"""

from __future__ import annotations

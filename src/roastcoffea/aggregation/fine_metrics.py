"""Fine metrics parsing for Dask Spans.

Parses cumulative_worker_metrics from Dask Spans into structured format
with CPU time, I/O time, disk operations, compression, and serialization metrics.
"""

from __future__ import annotations

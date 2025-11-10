"""Backend-specific aggregation parsers.

Each backend provides its own parser for converting raw tracking data
into standardized worker metrics dictionaries.
"""

from __future__ import annotations

from roastcoffea.aggregation.backends.base import AbstractTrackingDataParser
from roastcoffea.aggregation.backends.dask import DaskTrackingDataParser

# Registry mapping backend name to parser class
_PARSER_REGISTRY: dict[str, type[AbstractTrackingDataParser]] = {
    "dask": DaskTrackingDataParser,
}


def get_parser(backend: str) -> AbstractTrackingDataParser:
    """Get tracking data parser for a specific backend.

    Parameters
    ----------
    backend : str
        Backend name ("dask", "taskvine", etc.)

    Returns
    -------
    AbstractTrackingDataParser
        Parser instance for the backend

    Raises
    ------
    ValueError
        If backend is not supported
    """
    if backend not in _PARSER_REGISTRY:
        msg = f"Unsupported backend: {backend}. Available: {list(_PARSER_REGISTRY.keys())}"
        raise ValueError(msg)

    parser_class = _PARSER_REGISTRY[backend]
    return parser_class()


__all__ = ["AbstractTrackingDataParser", "DaskTrackingDataParser", "get_parser"]

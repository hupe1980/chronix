"""Chronix Python client — async-first SDK for Chronix time-series database."""

from importlib.metadata import PackageNotFoundError, version

from chronix_client.client import ChronixClient
from chronix_client.exceptions import (
    BackpressureError,
    ChronixError,
    ConnectionError,
    DeadlineExceeded,
    QueryError,
    WriteError,
)
from chronix_client.models import (
    ColumnSchema,
    FieldValue,
    MeasurementInfo,
    Point,
    QueryResult,
    DeleteResult,
    ServerInfo,
    TimeRange,
)

__all__ = [
    "BackpressureError",
    "ChronixClient",
    "ChronixError",
    "ColumnSchema",
    "ConnectionError",
    "DeadlineExceeded",
    "DeleteResult",
    "FieldValue",
    "MeasurementInfo",
    "Point",
    "QueryError",
    "QueryResult",
    "ServerInfo",
    "TimeRange",
    "WriteError",
]

try:
    __version__ = version("chronix-client")
except PackageNotFoundError:
    # Not installed — e.g. run from a checkout with sdks/python on PYTHONPATH.
    __version__ = "0.0.0+unknown"

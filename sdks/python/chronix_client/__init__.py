"""Chronix Python client — async-first SDK for Chronix time-series database."""

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

__version__ = "0.3.0"

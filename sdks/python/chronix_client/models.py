"""Data models for the Chronix Python client."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FieldValue(Enum):
    """Discriminator for Chronix field types."""

    FLOAT64 = "float64"
    INT64 = "int64"
    UINT64 = "uint64"
    BOOL = "boolean"
    STRING = "string"


@dataclass(slots=True)
class Point:
    """A single data point to write to Chronix.

    Parameters
    ----------
    measurement : str
        Measurement name.
    tags : dict[str, str]
        Tag key-value pairs (indexed, low cardinality).
    fields : dict[str, int | float | bool | str]
        Field key-value pairs (the actual data).
    timestamp : int | None
        Unix timestamp in nanoseconds. Defaults to current time.
    """

    measurement: str
    fields: dict[str, int | float | bool | str]
    tags: dict[str, str] = field(default_factory=dict)
    timestamp: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the JSON write request format."""
        return {
            "measurement": self.measurement,
            "tags": self.tags,
            "fields": self.fields,
            "timestamp": self.timestamp or time.time_ns(),
        }

    def to_line_protocol(self) -> str:
        """Serialize to InfluxDB line protocol format."""
        parts = [self.measurement]

        # Tags (sorted by key for canonical ordering).
        if self.tags:
            tag_str = ",".join(
                f"{_escape_tag(k)}={_escape_tag(v)}"
                for k, v in sorted(self.tags.items())
            )
            parts[0] = f"{self.measurement},{tag_str}"

        # Fields (sorted by key).
        field_parts: list[str] = []
        for k, v in sorted(self.fields.items()):
            field_parts.append(f"{_escape_tag(k)}={_encode_field(v)}")
        parts.append(",".join(field_parts))

        # Timestamp.
        ts = self.timestamp or time.time_ns()
        parts.append(str(ts))

        return " ".join(parts)


@dataclass(slots=True, frozen=True)
class TimeRange:
    """Closed time range ``[start, end]`` in nanoseconds.

    Both ends are **inclusive**, matching the server's ``TimeRangeRequest``.
    This docstring said half-open for as long as it existed, which is a
    one-nanosecond error on a query and a whole-boundary-sample error on a
    delete.
    """

    start: int
    end: int

    def to_dict(self) -> dict[str, int]:
        return {"start": self.start, "end": self.end}


@dataclass(slots=True, frozen=True)
class DeleteResult:
    """Outcome of a predicate delete.

    A delete can be **partial**: a segment the server could not open or read is
    skipped rather than aborting the whole operation, so ``series_tombstoned``
    on its own cannot distinguish "nothing matched" from "some data was never
    scanned". Callers acting on an erasure obligation must check
    :attr:`complete` and retry.
    """

    series_tombstoned: int
    segments_skipped: int

    @property
    def complete(self) -> bool:
        """Whether every matching segment was scanned."""
        return self.segments_skipped == 0

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "DeleteResult":
        return cls(
            series_tombstoned=int(data.get("deleted", 0) or 0),
            segments_skipped=int(data.get("segments_skipped", 0) or 0),
        )


@dataclass(slots=True, frozen=True)
class ColumnSchema:
    """Schema for a single column in a measurement.

    ``data_type`` is ``None`` for timestamp and tag columns: the server omits
    the key rather than sending null.
    """

    name: str
    role: str  # "tag", "field", or "timestamp"
    data_type: str | None = None


@dataclass(slots=True, frozen=True)
class MeasurementInfo:
    """Summary of a measurement."""

    name: str


@dataclass(slots=True, frozen=True)
class ServerInfo:
    """Server metadata."""

    version: str
    uptime_seconds: float
    measurement_count: int


@dataclass(slots=True)
class QueryResult:
    """Result of a query — a list of row dicts.

    ``truncated`` is ``True`` when the server's ``sql_max_rows`` cut the
    answer short. It is not decoration: an aggregate computed over a
    truncated scan is a **wrong** number, not a partial one, and before the
    server reported this there was no way for a caller to tell the two apart.
    """

    rows: list[dict[str, Any]]
    truncated: bool = False

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self):
        return iter(self.rows)

    def to_dataframe(self):
        """Convert to a pandas DataFrame (requires ``pandas`` extra)."""
        import pandas as pd  # noqa: PLC0415 — lazy import

        return pd.DataFrame(self.rows)


# ── helpers ──────────────────────────────────────────────────────

def _escape_tag(s: str) -> str:
    """Escape special characters for line protocol tags/keys."""
    return s.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _encode_field(v: int | float | bool | str) -> str:
    """Encode a field value for line protocol."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    msg = f"unsupported field type: {type(v)}"
    raise TypeError(msg)

"""Exception hierarchy for the Chronix Python client."""

from __future__ import annotations


class ChronixError(Exception):
    """Base exception for all Chronix client errors."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ConnectionError(ChronixError):  # noqa: A001 — intentional shadow
    """Raised when the client cannot reach the Chronix server."""


class WriteError(ChronixError):
    """Raised when a write operation fails."""


class QueryError(ChronixError):
    """Raised when a query operation fails."""

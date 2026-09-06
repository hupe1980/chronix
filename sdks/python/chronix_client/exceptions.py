"""Exception hierarchy for the Chronix Python client.

Every error response carries a machine-readable ``code`` beside its message,
and the API reference tells clients to branch on that rather than on the
sentence.  This module used to throw it away: a full memtable, a query that
ran out of time and a genuine server bug all arrived as a bare
:class:`ChronixError` whose only distinguishing feature was a status number
embedded in a string.  So the one thing an ingestion loop needs to decide —
*should I retry, and when?* — was not available to it.
"""

from __future__ import annotations


class ChronixError(Exception):
    """Base exception for all Chronix client errors.

    Attributes:
        status_code: The HTTP status, when the failure came from the server.
        code: The server's machine-readable error code (``BACKPRESSURE``,
            ``CARDINALITY_EXCEEDED``, …), or ``None`` for a transport failure.
        retry_after: Seconds the server asked the client to wait, from the
            ``Retry-After`` header, or ``None`` when it did not say.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        """Whether re-sending the same request could succeed.

        ``True`` for the conditions the server marks as the deployment's and
        transient — back-pressure, a full disk, the database shutting down —
        and for a deadline, which a smaller query or a quieter moment may
        clear.  ``False`` for anything the caller has to change first.
        """
        return isinstance(self, (BackpressureError, DeadlineExceeded))


class ConnectionError(ChronixError):  # noqa: A001 — intentional shadow
    """Raised when the client cannot reach the Chronix server."""

    @property
    def retryable(self) -> bool:
        """A server that is not there yet may be there in a moment."""
        return True


class WriteError(ChronixError):
    """Raised when a write operation fails."""


class QueryError(ChronixError):
    """Raised when a query operation fails."""


class BackpressureError(ChronixError):
    """The server is temporarily unable to accept the request.

    ``503 BACKPRESSURE`` (the memtable is at capacity and the flush that
    clears it is already running), ``503 OVERLOADED`` (a condition that needs
    an operator), ``503 DATABASE_CLOSED`` (the server is shutting down) and
    ``507 STORAGE_FULL``.  :attr:`~ChronixError.retry_after` carries the wait
    the server asked for, in seconds.
    """


class DeadlineExceeded(ChronixError):
    """The server stopped waiting.

    ``504 QUERY_TIMEOUT`` — the read outran its deadline; narrow the range or
    raise the setting.

    ``504 WRITE_TIMEOUT`` — the server stopped waiting for a write it cannot
    cancel, so **the outcome is unknown and the write may still land**.
    Retrying is safe: a point is identified by its series and its timestamp,
    so writing it twice stores it once.
    """

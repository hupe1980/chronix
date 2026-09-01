"""Async HTTP client for the Chronix REST API."""

from __future__ import annotations

from typing import Any

import httpx

from chronix_client.exceptions import ChronixError, ConnectionError, QueryError, WriteError
from chronix_client.models import (
    ColumnSchema,
    DeleteResult,
    MeasurementInfo,
    Point,
    QueryResult,
    ServerInfo,
    TimeRange,
)

_DEFAULT_TIMEOUT = 30.0


class ChronixClient:
    """Async client for the Chronix time-series database REST API.

    Parameters
    ----------
    base_url : str
        Chronix server URL, e.g. ``"http://localhost:5555"``.
    api_key : str | None
        Optional API key for authentication (sent as ``Authorization: Bearer``).
    timeout : float
        Default request timeout in seconds.
    namespace : str | None
        Optional namespace header for multi-tenant deployments.

    Examples
    --------
    >>> async with ChronixClient("http://localhost:5555") as client:
    ...     await client.write([Point("cpu", {"usage": 42.5}, tags={"host": "a"})])
    ...     result = await client.query("cpu", TimeRange(start=0, end=2**63 - 1))
    ...     print(len(result))
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
        namespace: str | None = None,
    ) -> None:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if api_key is not None:
            headers["Authorization"] = f"Bearer {api_key}"
        if namespace is not None:
            headers["X-Chronix-Namespace"] = namespace

        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
        )

    async def __aenter__(self) -> ChronixClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # ── Health ────────────────────────────────────────────────────

    async def health(self) -> dict[str, Any]:
        """Check server health.

        Returns a dict with ``status`` key.
        """
        return await self._get("/health")

    async def ready(self) -> dict[str, Any]:
        """Check server readiness."""
        return await self._get("/ready")

    async def server_info(self) -> ServerInfo:
        """Get server metadata (version, uptime, measurement count).

        Assembled from ``/health`` and the measurement listing's ``total``;
        there is no ``/api/v1/server_info`` route.
        """
        health = await self._get("/health")
        listing = await self._get("/api/v1/measurements", params={"limit": 1})
        return ServerInfo(
            version=str(health.get("version", "unknown")),
            uptime_seconds=float(health.get("uptime_secs", 0) or 0),
            measurement_count=int(listing.get("total", 0) or 0),
        )

    # ── Write ─────────────────────────────────────────────────────

    async def write(
        self,
        points: list[Point],
        *,
        idempotency_key: str | None = None,
    ) -> int:
        """Write points via JSON endpoint.

        Parameters
        ----------
        points : list[Point]
            Data points to write.
        idempotency_key : str | None
            Optional idempotency key (HTTP 409 on duplicate).

        Returns
        -------
        int
            Number of points written.
        """
        headers: dict[str, str] = {}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        body = [p.to_dict() for p in points]
        # `POST /api/v1/write` answers **204 No Content** — no body to parse,
        # so the count returned here is the caller's own.

        await self._post_no_content("/api/v1/write", json=body, extra_headers=headers)
        return len(points)

    async def write_line_protocol(
        self,
        lines: str | list[str],
        *,
        idempotency_key: str | None = None,
    ) -> int:
        """Write using InfluxDB line protocol.

        Parameters
        ----------
        lines : str | list[str]
            Line protocol string(s).
        idempotency_key : str | None
            Optional idempotency key.

        Returns
        -------
        int
            Number of points written.
        """
        if isinstance(lines, list):
            body = "\n".join(lines)
        else:
            body = lines

        headers: dict[str, str] = {"Content-Type": "text/plain"}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        resp = await self._client.post(
            "/api/v1/write/influx", content=body, headers=headers
        )
        self._check_response(resp)
        data = resp.json()
        return data.get("written", 0)

    # ── Query ─────────────────────────────────────────────────────

    async def query(
        self,
        measurement: str,
        time_range: TimeRange,
        *,
        tags: dict[str, str] | None = None,
        fields: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> QueryResult:
        """Execute a structured query.

        Parameters
        ----------
        measurement : str
            Measurement name.
        time_range : TimeRange
            Time range to query.
        tags : dict[str, str] | None
            Optional tag filter predicates (exact match).
        fields : list[str] | None
            Specific fields to return (default: all).
        limit : int | None
            Maximum rows to return.
        offset : int | None
            Rows to skip.

        Returns
        -------
        QueryResult
            Query results with row dicts.
        """
        body: dict[str, Any] = {
            "measurement": measurement,
            "range": time_range.to_dict(),
        }
        if tags:
            body["tags"] = dict(tags)
        if fields:
            body["fields"] = list(fields)
        if limit is not None:
            body["limit"] = limit
        if offset is not None:
            body["offset"] = offset

        # The response is a bare JSON **array** of rows, not an object with a
        # `rows` key. Every field name here was wrong in the same direction:
        # the body used `tag_filters`/`field_columns`, which the server's
        # `QueryRequest` does not define — and it ignores unknown fields, so
        # the query silently ran unfiltered and unprojected before failing on
        # the response shape.
        rows = await self._post("/api/v1/query", json=body)
        return QueryResult(rows=rows if isinstance(rows, list) else [])

    async def sql(self, query: str) -> QueryResult:
        """Execute a SQL query.

        Parameters
        ----------
        query : str
            SQL query string.

        Returns
        -------
        QueryResult
            Query results.
        """
        # The SQL endpoint answers column-oriented metadata plus **positional**
        # rows — `{"columns": [{"name", "data_type"}], "rows": [[...]],
        # "row_count": n}`. `QueryResult` holds row dicts, so the two are
        # zipped here; handing the raw arrays through would give callers rows
        # with no column names and a `to_dataframe()` with integer headers.
        data = await self._post("/api/v1/sql", json={"query": query})
        names = [c["name"] for c in data.get("columns", [])]
        rows = [dict(zip(names, row)) for row in data.get("rows", [])]
        return QueryResult(rows=rows)

    async def explain(self, measurement: str, time_range: TimeRange) -> dict[str, Any]:
        """Get the query execution plan (EXPLAIN)."""
        body = {"measurement": measurement, "range": time_range.to_dict()}
        return await self._post("/api/v1/query/explain", json=body)

    # ── Schema ────────────────────────────────────────────────────

    async def list_measurements(
        self,
        *,
        offset: int | None = None,
        limit: int | None = None,
    ) -> list[MeasurementInfo]:
        """List measurements.

        The endpoint is **paginated** and answers
        ``{"items": [...], "total": n, "offset": n, "limit": n}``; this method
        read a ``measurements`` key that no response carries, so it always
        returned an empty list. Pass ``limit`` to page explicitly — the server
        applies its own default otherwise.
        """
        params: dict[str, Any] = {}
        if offset is not None:
            params["offset"] = offset
        if limit is not None:
            params["limit"] = limit
        data = await self._get("/api/v1/measurements", params=params or None)
        return [MeasurementInfo(name=m["name"]) for m in data.get("items", [])]

    async def get_schema(self, measurement: str) -> list[ColumnSchema]:
        """Get schema for a measurement."""
        data = await self._get(f"/api/v1/measurements/{measurement}/schema")
        # `data_type` is omitted for timestamp and tag columns — the server
        # skips serialising `None` — so indexing it raised `KeyError` on every
        # measurement that has a tag.
        return [
            ColumnSchema(
                name=c["name"], role=c["role"], data_type=c.get("data_type")
            )
            for c in data.get("columns", [])
        ]

    async def drop_measurement(self, measurement: str) -> None:
        """Drop a measurement and all its data."""
        resp = await self._client.delete(f"/api/v1/measurements/{measurement}")
        self._check_response(resp)

    # ── Delete ────────────────────────────────────────────────────

    async def delete(
        self,
        measurement: str,
        time_range: TimeRange | None = None,
        *,
        tags: dict[str, str] | None = None,
    ) -> DeleteResult:
        """Delete data by measurement, optional time range, and optional tags.

        ``time_range`` is inclusive at both ends. Omitting it deletes
        everything currently stored for the matching series — but not data
        written afterwards, so writing to a deleted series re-creates it.

        Returns a :class:`DeleteResult`. Check
        :attr:`~DeleteResult.complete` before treating the delete as done: the
        server skips segments it cannot read rather than failing the request.
        """
        body: dict[str, Any] = {"measurement": measurement}
        if time_range is not None:
            body["range"] = time_range.to_dict()
        if tags:
            body["tags"] = dict(tags)
        data = await self._post("/api/v1/delete", json=body)
        return DeleteResult.from_dict(data)

    # ── Admin ─────────────────────────────────────────────────────

    async def list_rollups(self) -> list[dict[str, Any]]:
        """List configured rollup rules.

        Paginated, like every listing endpoint: the payload key is ``items``.
        """
        data = await self._get("/api/v1/rollups")
        return data.get("items", [])

    async def list_connectors(self) -> list[dict[str, Any]]:
        """List active connectors (payload key is ``items``)."""
        data = await self._get("/api/v1/connectors")
        return data.get("items", [])

    async def openapi_spec(self) -> dict[str, Any]:
        """Fetch the OpenAPI 3.1 specification."""
        return await self._get("/api/v1/openapi.json")

    # ── PromQL ────────────────────────────────────────────────────

    async def prom_query(self, query: str, *, time: str | None = None) -> dict[str, Any]:
        """Execute an instant PromQL query.

        Parameters
        ----------
        query : str
            PromQL expression.
        time : str | None
            Evaluation timestamp (RFC 3339 or Unix seconds).
        """
        params: dict[str, str] = {"query": query}
        if time is not None:
            params["time"] = time
        return await self._get("/api/v1/prom/query", params=params)

    async def prom_query_range(
        self,
        query: str,
        start: str,
        end: str,
        step: str,
    ) -> dict[str, Any]:
        """Execute a range PromQL query.

        Parameters
        ----------
        query : str
            PromQL expression.
        start, end : str
            Range boundaries (RFC 3339 or Unix seconds).
        step : str
            Query resolution step (e.g. ``"15s"``).
        """
        params = {"query": query, "start": start, "end": end, "step": step}
        return await self._get("/api/v1/prom/query_range", params=params)

    async def prom_labels(self) -> list[str]:
        """List all Prometheus label names."""
        data = await self._get("/api/v1/prom/labels")
        return data.get("data", [])

    async def prom_label_values(self, label: str) -> list[str]:
        """Get values for a Prometheus label."""
        data = await self._get(f"/api/v1/prom/label/{label}/values")
        return data.get("data", [])

    async def prom_series(self, match: list[str]) -> list[dict[str, str]]:
        """Find series matching label selectors."""
        params = {"match[]": match}
        data = await self._get("/api/v1/prom/series", params=params)
        return data.get("data", [])

    # ── Arrow Flight SQL ──────────────────────────────────────────

    @staticmethod
    def flight_sql_uri(host: str = "localhost", port: int = 5557) -> str:
        """Build an ADBC Flight SQL connection URI.

        Use with ``adbc_driver_flightsql`` for Arrow-native queries::

            import adbc_driver_flightsql.dbapi
            uri = ChronixClient.flight_sql_uri()
            conn = adbc_driver_flightsql.dbapi.connect(uri)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM cpu LIMIT 10")
            df = cursor.fetch_arrow_table().to_pandas()
        """
        return f"grpc://{host}:{port}"

    # ── Internal ──────────────────────────────────────────────────

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            resp = await self._client.get(path, params=params)
        except httpx.ConnectError as e:
            raise ConnectionError(str(e)) from e
        self._check_response(resp)
        return resp.json()

    async def _post(
        self,
        path: str,
        *,
        json: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        """POST and decode the JSON body.

        The return is deliberately `Any` rather than `dict`: `/api/v1/query`
        answers a bare JSON **array**. Typing this as a dict is what let
        `data.get("rows", [])` past review on a response that has no keys.
        """
        try:
            resp = await self._client.post(path, json=json, headers=extra_headers)
        except httpx.ConnectError as e:
            raise ConnectionError(str(e)) from e
        self._check_response(resp)
        return resp.json()

    async def _post_no_content(
        self,
        path: str,
        *,
        json: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        """POST to an endpoint that answers `204 No Content`."""
        try:
            resp = await self._client.post(path, json=json, headers=extra_headers)
        except httpx.ConnectError as e:
            raise ConnectionError(str(e)) from e
        self._check_response(resp)

    @staticmethod
    def _check_response(resp: httpx.Response) -> None:
        if resp.is_success:
            return
        status = resp.status_code
        try:
            detail = resp.json().get("error", resp.text)
        except Exception:  # noqa: BLE001
            detail = resp.text

        if status == 409:
            raise WriteError(
                f"duplicate write (idempotency conflict): {detail}",
                status_code=status,
            )
        if 400 <= status < 500:
            raise QueryError(f"client error {status}: {detail}", status_code=status)
        raise ChronixError(f"server error {status}: {detail}", status_code=status)

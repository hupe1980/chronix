"""Tests for the Chronix Python client — uses respx for HTTP mocking."""

from __future__ import annotations

import json

import pytest
import httpx
import respx

from chronix_client import (
    ChronixClient,
    ChronixError,
    ConnectionError,
    Point,
    QueryError,
    QueryResult,
    TimeRange,
    WriteError,
)


BASE = "http://chronix-test:5555"


# ── Model Tests ──────────────────────────────────────────────────


class TestPoint:
    def test_to_dict(self):
        p = Point("cpu", {"usage": 42.5}, tags={"host": "a"}, timestamp=1_000_000_000)
        d = p.to_dict()
        assert d["measurement"] == "cpu"
        assert d["tags"] == {"host": "a"}
        assert d["fields"] == {"usage": 42.5}
        assert d["timestamp"] == 1_000_000_000

    def test_to_dict_auto_timestamp(self):
        p = Point("cpu", {"usage": 1.0})
        d = p.to_dict()
        assert d["timestamp"] > 0

    def test_to_line_protocol_float(self):
        p = Point("cpu", {"usage": 42.5}, tags={"host": "a"}, timestamp=100)
        lp = p.to_line_protocol()
        assert lp == 'cpu,host=a usage=42.5 100'

    def test_to_line_protocol_int(self):
        p = Point("mem", {"total": 1024}, timestamp=200)
        lp = p.to_line_protocol()
        assert lp == "mem total=1024i 200"

    def test_to_line_protocol_string(self):
        p = Point("log", {"msg": "hello world"}, timestamp=300)
        lp = p.to_line_protocol()
        assert lp == 'log msg="hello world" 300'

    def test_to_line_protocol_bool(self):
        p = Point("status", {"ok": True}, timestamp=400)
        lp = p.to_line_protocol()
        assert lp == "status ok=true 400"

    def test_to_line_protocol_escaping(self):
        p = Point("m", {"v": 1.0}, tags={"k": "a b"}, timestamp=500)
        lp = p.to_line_protocol()
        assert "k=a\\ b" in lp

    def test_to_line_protocol_multiple_fields(self):
        p = Point("cpu", {"idle": 80.0, "usage": 20.0}, tags={"host": "b"}, timestamp=600)
        lp = p.to_line_protocol()
        assert "idle=80.0,usage=20.0" in lp


class TestTimeRange:
    def test_to_dict(self):
        tr = TimeRange(start=100, end=200)
        assert tr.to_dict() == {"start": 100, "end": 200}


class TestQueryResult:
    def test_len_iter(self):
        qr = QueryResult(rows=[{"a": 1}, {"a": 2}])
        assert len(qr) == 2
        assert list(qr) == [{"a": 1}, {"a": 2}]


# ── Client Tests (mocked HTTP) ───────────────────────────────────


@pytest.fixture
def mock_api():
    with respx.mock(base_url=BASE) as m:
        yield m


@pytest.mark.asyncio
async def test_health(mock_api):
    mock_api.get("/health").respond(json={"status": "ok"})
    async with ChronixClient(BASE) as c:
        result = await c.health()
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_ready(mock_api):
    mock_api.get("/ready").respond(json={"status": "ready"})
    async with ChronixClient(BASE) as c:
        result = await c.ready()
    assert result["status"] == "ready"


@pytest.mark.asyncio
async def test_server_info(mock_api):
    # There is no `/api/v1/server_info` route. This is assembled from
    # `/health` (which carries `uptime_secs`, not `uptime_seconds`) and the
    # measurement listing's `total`.
    mock_api.get("/health").respond(
        json={"status": "ok", "uptime_secs": 120, "version": "0.1.0"}
    )
    mock_api.get("/api/v1/measurements").respond(
        json={"items": [], "total": 3, "offset": 0, "limit": 1}
    )
    async with ChronixClient(BASE) as c:
        info = await c.server_info()
    assert info.version == "0.1.0"
    assert info.uptime_seconds == 120.0
    assert info.measurement_count == 3


@pytest.mark.asyncio
async def test_write_json(mock_api):
    # `POST /api/v1/write` answers **204 No Content** — there is no body to
    # parse, and parsing one raised on every successful write.
    route = mock_api.post("/api/v1/write").respond(status_code=204)
    pts = [
        Point("cpu", {"usage": 42.5}, tags={"host": "a"}, timestamp=100),
        Point("cpu", {"usage": 55.0}, tags={"host": "b"}, timestamp=200),
    ]
    async with ChronixClient(BASE) as c:
        written = await c.write(pts)
    assert written == 2

    sent = json.loads(route.calls.last.request.content)
    assert sent == [
        {
            "measurement": "cpu",
            "tags": {"host": "a"},
            "fields": {"usage": 42.5},
            "timestamp": 100,
        },
        {
            "measurement": "cpu",
            "tags": {"host": "b"},
            "fields": {"usage": 55.0},
            "timestamp": 200,
        },
    ]


@pytest.mark.asyncio
async def test_write_idempotency_key(mock_api):
    route = mock_api.post("/api/v1/write").respond(status_code=204)
    async with ChronixClient(BASE) as c:
        await c.write(
            [Point("cpu", {"usage": 1.0}, timestamp=100)],
            idempotency_key="dedup-1",
        )
    assert route.calls[0].request.headers["Idempotency-Key"] == "dedup-1"


@pytest.mark.asyncio
async def test_write_duplicate_rejected(mock_api):
    mock_api.post("/api/v1/write").respond(
        status_code=409, json={"error": "duplicate key"}
    )
    async with ChronixClient(BASE) as c:
        with pytest.raises(WriteError, match="duplicate"):
            await c.write([Point("cpu", {"v": 1.0})], idempotency_key="dup")


@pytest.mark.asyncio
async def test_write_line_protocol(mock_api):
    mock_api.post("/api/v1/write/influx").respond(json={"written": 1})
    async with ChronixClient(BASE) as c:
        written = await c.write_line_protocol("cpu,host=a usage=42.5 100")
    assert written == 1


@pytest.mark.asyncio
async def test_write_line_protocol_list(mock_api):
    mock_api.post("/api/v1/write/influx").respond(json={"written": 2})
    async with ChronixClient(BASE) as c:
        written = await c.write_line_protocol([
            "cpu,host=a usage=42.5 100",
            "cpu,host=b usage=55.0 200",
        ])
    assert written == 2


@pytest.mark.asyncio
async def test_query(mock_api):
    # The response is a bare JSON **array** of rows, not `{"rows": [...]}`.
    mock_api.post("/api/v1/query").respond(
        json=[{"timestamp": 100, "tags": {"host": "a"}, "fields": {"usage": 42.5}}]
    )
    async with ChronixClient(BASE) as c:
        result = await c.query("cpu", TimeRange(0, 200))
    assert len(result) == 1
    assert result.rows[0]["fields"]["usage"] == 42.5


@pytest.mark.asyncio
async def test_query_with_filters(mock_api):
    route = mock_api.post("/api/v1/query").respond(json=[])
    async with ChronixClient(BASE) as c:
        await c.query(
            "cpu",
            TimeRange(0, 200),
            tags={"host": "a"},
            fields=["usage"],
            limit=10,
            offset=5,
        )
    # Assert the whole body against the server's `QueryRequest`. The previous
    # version asserted `tag_filters` and `field_columns` — names the server
    # does not define — and since its delete/query bodies ignore unknown
    # fields, the query ran unfiltered and unprojected.
    sent = json.loads(route.calls.last.request.content)
    assert sent == {
        "measurement": "cpu",
        "range": {"start": 0, "end": 200},
        "tags": {"host": "a"},
        "fields": ["usage"],
        "limit": 10,
        "offset": 5,
    }


@pytest.mark.asyncio
async def test_sql(mock_api):
    # SQL answers column metadata plus **positional** rows; the client zips
    # them so callers get named columns.
    mock_api.post("/api/v1/sql").respond(
        json={
            "columns": [
                {"name": "host", "data_type": "Utf8"},
                {"name": "usage", "data_type": "Float64"},
            ],
            "rows": [["a", 42.5]],
            "row_count": 1,
        }
    )
    async with ChronixClient(BASE) as c:
        result = await c.sql("SELECT * FROM cpu LIMIT 1")
    assert len(result) == 1
    assert result.rows[0] == {"host": "a", "usage": 42.5}


@pytest.mark.asyncio
async def test_list_measurements(mock_api):
    # Paginated: the payload key is `items`, not `measurements`.
    mock_api.get("/api/v1/measurements").respond(
        json={
            "items": [{"name": "cpu"}, {"name": "mem"}],
            "total": 2,
            "offset": 0,
            "limit": 100,
        }
    )
    async with ChronixClient(BASE) as c:
        ms = await c.list_measurements()
    assert len(ms) == 2
    assert ms[0].name == "cpu"


@pytest.mark.asyncio
async def test_get_schema(mock_api):
    # The server omits `data_type` entirely for timestamp and tag columns
    # (`skip_serializing_if = "Option::is_none"`), so indexing it raised
    # `KeyError` for every measurement that has a tag.
    mock_api.get("/api/v1/measurements/cpu/schema").respond(
        json={
            "name": "cpu",
            "columns": [
                {"name": "timestamp", "role": "timestamp"},
                {"name": "host", "role": "tag"},
                {"name": "usage", "role": "field", "data_type": "Float64"},
            ],
        }
    )
    async with ChronixClient(BASE) as c:
        schema = await c.get_schema("cpu")
    assert len(schema) == 3
    assert schema[0].data_type is None
    assert schema[2].name == "usage"
    assert schema[2].role == "field"
    assert schema[2].data_type == "Float64"


@pytest.mark.asyncio
async def test_drop_measurement(mock_api):
    mock_api.delete("/api/v1/measurements/old").respond(status_code=200, json={})
    async with ChronixClient(BASE) as c:
        await c.drop_measurement("old")


@pytest.mark.asyncio
async def test_delete(mock_api):
    route = mock_api.post("/api/v1/delete").respond(
        json={"deleted": 42, "segments_skipped": 0, "complete": True}
    )
    async with ChronixClient(BASE) as c:
        result = await c.delete("cpu", TimeRange(0, 100), tags={"host": "a"})

    assert result.series_tombstoned == 42
    assert result.complete

    # Assert the *request body*, not just that a request happened. The tag
    # filters were sent under a field name the server does not read, and the
    # server ignores unknown fields — so every delete through this client
    # applied to the whole measurement. A mocked endpoint that checks only the
    # response cannot see that.
    sent = json.loads(route.calls.last.request.content)
    assert sent == {
        "measurement": "cpu",
        "range": {"start": 0, "end": 100},
        "tags": {"host": "a"},
    }


@pytest.mark.asyncio
async def test_delete_without_range_or_tags_sends_neither(mock_api):
    route = mock_api.post("/api/v1/delete").respond(
        json={"deleted": 1, "segments_skipped": 0}
    )
    async with ChronixClient(BASE) as c:
        await c.delete("cpu")

    sent = json.loads(route.calls.last.request.content)
    assert sent == {"measurement": "cpu"}


@pytest.mark.asyncio
async def test_delete_reports_a_partial_delete(mock_api):
    # A skipped segment means matching data may still be on disk. A caller
    # acting on an erasure obligation has to be able to see this.
    mock_api.post("/api/v1/delete").respond(
        json={"deleted": 3, "segments_skipped": 2, "complete": False}
    )
    async with ChronixClient(BASE) as c:
        result = await c.delete("cpu", tags={"host": "a"})

    assert result.segments_skipped == 2
    assert not result.complete


@pytest.mark.asyncio
async def test_prom_query(mock_api):
    mock_api.get("/api/v1/prom/query").respond(
        json={"status": "success", "data": {"resultType": "vector", "result": []}}
    )
    async with ChronixClient(BASE) as c:
        result = await c.prom_query("up")
    assert result["status"] == "success"


@pytest.mark.asyncio
async def test_prom_query_range(mock_api):
    mock_api.get("/api/v1/prom/query_range").respond(
        json={"status": "success", "data": {"resultType": "matrix", "result": []}}
    )
    async with ChronixClient(BASE) as c:
        result = await c.prom_query_range("up", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "60s")
    assert result["status"] == "success"


@pytest.mark.asyncio
async def test_prom_labels(mock_api):
    mock_api.get("/api/v1/prom/labels").respond(json={"data": ["__name__", "host"]})
    async with ChronixClient(BASE) as c:
        labels = await c.prom_labels()
    assert "host" in labels


@pytest.mark.asyncio
async def test_prom_label_values(mock_api):
    mock_api.get("/api/v1/prom/label/host/values").respond(json={"data": ["a", "b"]})
    async with ChronixClient(BASE) as c:
        values = await c.prom_label_values("host")
    assert values == ["a", "b"]


@pytest.mark.asyncio
async def test_server_error(mock_api):
    mock_api.get("/health").respond(status_code=500, json={"error": "boom"})
    async with ChronixClient(BASE) as c:
        with pytest.raises(ChronixError, match="boom"):
            await c.health()


@pytest.mark.asyncio
async def test_client_error(mock_api):
    mock_api.post("/api/v1/query").respond(
        status_code=400, json={"error": "bad request"}
    )
    async with ChronixClient(BASE) as c:
        with pytest.raises(QueryError, match="bad request"):
            await c.query("cpu", TimeRange(0, 100))


@pytest.mark.asyncio
async def test_api_key_header(mock_api):
    route = mock_api.get("/health").respond(json={"status": "ok"})
    async with ChronixClient(BASE, api_key="secret-key") as c:
        await c.health()
    assert route.calls[0].request.headers["Authorization"] == "Bearer secret-key"


@pytest.mark.asyncio
async def test_namespace_header(mock_api):
    route = mock_api.get("/health").respond(json={"status": "ok"})
    async with ChronixClient(BASE, namespace="prod") as c:
        await c.health()
    assert route.calls[0].request.headers["X-Chronix-Namespace"] == "prod"


@pytest.mark.asyncio
async def test_flight_sql_uri():
    uri = ChronixClient.flight_sql_uri("myhost", 9999)
    assert uri == "grpc://myhost:9999"


@pytest.mark.asyncio
async def test_openapi_spec(mock_api):
    mock_api.get("/api/v1/openapi.json").respond(json={"openapi": "3.1.0"})
    async with ChronixClient(BASE) as c:
        spec = await c.openapi_spec()
    assert spec["openapi"] == "3.1.0"


@pytest.mark.asyncio
async def test_explain(mock_api):
    mock_api.post("/api/v1/query/explain").respond(json={"plan": "scan → filter"})
    async with ChronixClient(BASE) as c:
        result = await c.explain("cpu", TimeRange(0, 100))
    assert "plan" in result


@pytest.mark.asyncio
async def test_list_rollups(mock_api):
    mock_api.get("/api/v1/rollups").respond(
        json={"items": [{"name": "hourly"}], "total": 1, "offset": 0, "limit": 100}
    )
    async with ChronixClient(BASE) as c:
        rules = await c.list_rollups()
    assert len(rules) == 1


@pytest.mark.asyncio
async def test_list_connectors(mock_api):
    mock_api.get("/api/v1/connectors").respond(
        json={"items": [], "total": 0, "offset": 0, "limit": 100}
    )
    async with ChronixClient(BASE) as c:
        conns = await c.list_connectors()
    assert conns == []

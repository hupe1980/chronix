"""End-to-end smoke test: the SDK against a real `chronixd`.

Every method here was wrong against the real server while the mocked unit
suite was green, because a mock asserts the contract its author imagined. The
unit tests now encode the server's actual request and response shapes, but
they still only prove the client agrees with a hand-written fixture. This
script proves it agrees with the server.

Run it against an already-running `chronixd`:

    CHRONIX_URL=http://127.0.0.1:4242 python tests/smoke_live.py

It is not collected by pytest (the filename does not start with `test_`);
CI starts a server and invokes it directly.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import re
import sys
import time

from chronix_client import ChronixClient, Point, TimeRange

URL = os.environ.get("CHRONIX_URL", "http://127.0.0.1:4242")
MEASUREMENT = "sdk_smoke"


def client_paths() -> set[str]:
    """Every server path the client names, with path parameters erased.

    Read out of the source rather than listed by hand: a list maintained
    beside the calls is a second inventory that drifts from the first, which
    is exactly how `/api/v1/query` survived a route rename to
    `/api/v1/chronix/query` under a green mocked suite.
    """
    source = (
        pathlib.Path(__file__).resolve().parent.parent / "chronix_client" / "client.py"
    ).read_text()
    return {
        re.sub(r"\{[^}]*\}", "{}", m)
        for m in re.findall(r'"(/(?:api/v[12]|health|ready)[^"]*)"', source)
    }


async def assert_every_client_path_exists(c: ChronixClient) -> None:
    """The server declares every route the client calls.

    A mocked unit suite asserts the contract its author imagined; this asserts
    the one the server publishes. Without it a renamed route is a 404 (or, as
    it was, a 400 from an unrelated handler that happens to share the path).
    """
    spec = await c.openapi_spec()
    declared = {re.sub(r"\{[^}]*\}", "{}", p) for p in spec.get("paths", {})}
    missing = sorted(client_paths() - declared)
    assert not missing, f"client calls paths the server does not declare: {missing}"


async def main() -> int:
    base_ts = time.time_ns()

    async with ChronixClient(URL) as c:
        # ── health / server_info ────────────────────────────────────
        health = await c.health()
        assert health.get("status") == "ok", health

        info = await c.server_info()
        assert info.version, "server_info must report a version"

        await assert_every_client_path_exists(c)

        # ── write ───────────────────────────────────────────────────
        points = [
            Point(
                MEASUREMENT,
                {"usage": float(i)},
                tags={"host": f"h{i % 2}"},
                timestamp=base_ts + i * 1_000_000_000,
            )
            for i in range(10)
        ]
        written = await c.write(points)
        assert written == 10, written

        # ── query, unfiltered ───────────────────────────────────────
        span = TimeRange(base_ts - 1, base_ts + 20_000_000_000)
        result = await c.query(MEASUREMENT, span)
        assert len(result) == 10, f"expected 10 rows, got {len(result)}"

        # ── query, filtered and projected ───────────────────────────
        #
        # This is the assertion the whole script exists for: the filter has to
        # reach the server. When the field name was wrong the server ignored
        # it and returned all ten rows, which no mocked test could see.
        filtered = await c.query(
            MEASUREMENT, span, tags={"host": "h0"}, fields=["usage"], limit=100
        )
        assert len(filtered) == 5, f"tag filter did not reach the server: {len(filtered)}"

        # ── schema ──────────────────────────────────────────────────
        schema = await c.get_schema(MEASUREMENT)
        names = {col.name for col in schema}
        assert {"host", "usage"} <= names, names

        listing = await c.list_measurements()
        assert any(m.name == MEASUREMENT for m in listing), listing

        # ── sql ─────────────────────────────────────────────────────
        rows = await c.sql(f'SELECT count(*) AS n FROM "{MEASUREMENT}"')
        assert len(rows) == 1, rows
        assert "n" in rows.rows[0], f"SQL rows must carry column names: {rows.rows[0]}"

        # ── delete, ranged ──────────────────────────────────────────
        #
        # Deletes the first three points of host=h0 only. Both the tag filter
        # and the time bound have to reach the server; when the tag filter did
        # not, this removed the whole measurement.
        deleted = await c.delete(
            MEASUREMENT,
            TimeRange(base_ts - 1, base_ts + 4_000_000_000),
            tags={"host": "h0"},
        )
        assert deleted.complete, f"partial delete: {deleted}"

        remaining = await c.query(MEASUREMENT, span)
        assert len(remaining) == 7, (
            f"a ranged, tag-filtered delete must remove exactly 3 rows, "
            f"leaving 7 — got {len(remaining)}"
        )

        # ── re-create a deleted series ──────────────────────────────
        await c.write(
            [
                Point(
                    MEASUREMENT,
                    {"usage": 99.0},
                    tags={"host": "h0"},
                    timestamp=base_ts + 30_000_000_000,
                )
            ]
        )
        wide = TimeRange(base_ts - 1, base_ts + 40_000_000_000)
        after = await c.query(MEASUREMENT, wide)
        assert len(after) == 8, f"a write after a delete must be visible: {len(after)}"

        await c.drop_measurement(MEASUREMENT)

    print("SDK live smoke test passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

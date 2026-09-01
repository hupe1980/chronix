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
import sys
import time

from chronix_client import ChronixClient, Point, TimeRange

URL = os.environ.get("CHRONIX_URL", "http://127.0.0.1:4242")
MEASUREMENT = "sdk_smoke"


async def main() -> int:
    base_ts = time.time_ns()

    async with ChronixClient(URL) as c:
        # ── health / server_info ────────────────────────────────────
        health = await c.health()
        assert health.get("status") == "ok", health

        info = await c.server_info()
        assert info.version, "server_info must report a version"

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

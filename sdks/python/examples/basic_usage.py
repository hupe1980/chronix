"""Basic usage example for the Chronix Python client."""

import asyncio

from chronix_client import ChronixClient, Point, TimeRange


async def main() -> None:
    async with ChronixClient("http://localhost:5555") as client:
        # ── Health check ──────────────────────────────────────
        health = await client.health()
        print(f"Server health: {health}")

        # ── Write points (JSON) ───────────────────────────────
        points = [
            Point(
                measurement="cpu",
                fields={"usage": 42.5, "idle": 57.5},
                tags={"host": "server-1", "region": "us-east"},
                timestamp=1_700_000_000_000_000_000,
            ),
            Point(
                measurement="cpu",
                fields={"usage": 65.2, "idle": 34.8},
                tags={"host": "server-2", "region": "eu-west"},
                timestamp=1_700_000_000_000_000_000,
            ),
        ]
        written = await client.write(points)
        print(f"Wrote {written} points")

        # ── Write via line protocol ───────────────────────────
        lp_written = await client.write_line_protocol([
            "mem,host=server-1 total=16384i,used=8192i 1700000000000000000",
            "mem,host=server-2 total=32768i,used=24576i 1700000000000000000",
        ])
        print(f"Wrote {lp_written} points via line protocol")

        # ── Query ─────────────────────────────────────────────
        result = await client.query(
            measurement="cpu",
            time_range=TimeRange(
                start=1_699_999_999_000_000_000,
                end=1_700_000_001_000_000_000,
            ),
            tag_filters={"region": "us-east"},
            limit=10,
        )
        print(f"Query returned {len(result)} rows:")
        for row in result:
            print(f"  {row}")

        # ── SQL query ─────────────────────────────────────────
        sql_result = await client.sql(
            "SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host"
        )
        print(f"SQL returned {len(sql_result)} rows")

        # ── Schema ────────────────────────────────────────────
        measurements = await client.list_measurements()
        print(f"Measurements: {[m.name for m in measurements]}")

        if measurements:
            schema = await client.get_schema(measurements[0].name)
            print(f"Schema for {measurements[0].name}:")
            for col in schema:
                print(f"  {col.name}: {col.data_type} ({col.role})")

        # ── PromQL ────────────────────────────────────────────
        prom_result = await client.prom_query('cpu_usage{host="server-1"}')
        print(f"PromQL result: {prom_result}")

        # ── Arrow Flight SQL (ADBC) ───────────────────────────
        print(f"\nFlight SQL URI: {ChronixClient.flight_sql_uri()}")
        print("Use with: adbc_driver_flightsql.dbapi.connect(uri)")


if __name__ == "__main__":
    asyncio.run(main())

+++
title = "Testing"
description = "How Chronix is tested: property tests, crash tests with a child that aborts mid-write, deterministic simulation, fuzzing and conformance suites."
weight = 190
aliases = ["/reference/testing/", "/internals/simulation-testing/"]
+++

Chronix tests what matters most in a database: that nothing acknowledged is
lost, and that every answer is the right one. Crash tests abort a real child
process mid-write and check what survived; fault injection makes the disk say
no; property tests and fuzzing cover the parsers and codecs; conformance suites
run the bytes real clients send and upstream's own test cases; and the
documentation is executed, so it cannot drift from the code.

## Crashes are real crashes

Recovery is tested with a child process that calls `abort()` — no unwinding,
no destructors, no flush — in the middle of the work:

| Test | Proves |
|---|---|
| `crash_under_load` | a child killed mid-flush, mid-compaction or mid-rollup loses nothing acknowledged and duplicates nothing |
| `integration::wal_replay_recovers_unflushed_data` | 25 acknowledged writes, an `abort()`, and all 25 replay |
| `rollup_repair_crash` | after a crash inside a rollup repair, every bucket equals the aggregate of its source rows |
| `restart_invariants` | a clean close replays nothing; a refused write never reappears; a failed flush loses nothing; an orphaned segment is removed at open |
| `maintenance_interleaving` | flush, compaction, GC, retention and rollups running at once against live writes lose no acknowledged point |

Directories written by the previous format generation are kept as fixtures, and
every reader must refuse them as a version mismatch — never as corruption.

## Faults on demand

A durable write is defined by what it does when the write **fails**, and a real
filesystem does not fail on request. The WAL and the catalog write through a
sink whose test implementation reports the disk full after a byte budget,
wrapping a real file — truncation, seeking and replay behave exactly as in
production. There is no runtime fault API: an endpoint that makes a production
database fail on purpose would be a liability.

## Properties and fuzzing

- **Every codec** round-trips arbitrary input bit for bit, and rejects arbitrary
  bytes without panicking or over-allocating.
- **Three fuzz targets** — the column decoder, the segment reader and the WAL
  reader — run nightly. The list of codecs the fuzzer walks is generated from
  the same definition as the codecs, so a new codec cannot skip it.
- **Numbers are checked against the same quantity computed another way**, or
  against a reference: forecasting, decomposition and the statistical tests are
  compared with `statsmodels` and `scipy`, never just for sign or finiteness.

## Conformance

- **Wire protocols** are tested with the bytes the real clients send —
  Telegraf's gzipped line protocol, the OpenTelemetry Collector's OTLP,
  Prometheus remote write 1.0 and 2.0 — not with requests the suite built for
  itself.
- **PromQL** runs upstream Prometheus's own test files for the features Chronix
  supports (`promqltest`), with a skip list that may only shrink.
- **Security** walks the whole router: every route must be classified for
  Cedar, refused under a deny-all policy, and scoped to a namespace; every write
  route must reach the quota check.

## The documentation is tested

| Guard | Asks |
|---|---|
| `documented_config` | Does every TOML block on the site load with the real parser? |
| `documented_metrics` | Is every `chronix_*` name on the site emitted by the code? |
| `documented_env_vars` | Is every documented environment variable read? |
| `documented_sql` | Does every documented SQL statement plan? |
| `documented_promql` | Does every documented PromQL query evaluate? |
| `route_inventory` | Is every route in the OpenAPI document, and every documented path a route? |

The README's first example is a compiled example, byte for byte, and
`scripts/check-docs.sh` checks the rest: crate names, ports, install commands,
example links, configuration keys, and that published pages carry no history
or backlog.

## Simulation (frozen tier)

The frozen cluster tier has a deterministic simulator, `chronix-dsim`: in-process
Raft nodes on a simulated network with partitions, delays and packet loss, a
virtual clock with per-node skew, a Wing & Gong linearizability checker, and
invariants — one leader per term, read-your-writes, durability, quorum safety,
monotonic reads — checked under thousands of seeds. Any failure replays exactly
from its seed.

## Related

- [Storage engine](@/explanation/storage.md)
- [Cluster (frozen)](@/reference/cluster.md)
- [Operate a server](@/guides/operate.md)

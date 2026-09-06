+++
title = "Testing & Hardening"
description = "How chronix is tested: real crashes, injected I/O failures, property tests, fuzzing, and the guards that check the documentation against the tree."
weight = 110
+++

## Faults are injected in tests

A durability path is defined by what it does when the write **fails**, and a
real filesystem will not fail on request. The WAL writes through a [`WalSink`]
trait whose test implementation reports the disk full after a byte budget,
wrapping a real file — so truncation, seeking and replay behave exactly as in
production, and only the budget is artificial.

That is the whole fault-injection surface. There is no runtime API: a fault is
worth injecting only where a **verdict** is checked, and an endpoint that makes
a production database fail on purpose is a liability.

## Crashes are real crashes

Recovery is tested with a child process that calls `abort()` — no unwinding,
no destructors, no flush — in the middle of a flush, a compaction or a rollup
materialisation:

| Test | What it proves |
|------|----------------|
| `crash_under_load` | A child killed mid-flush/compaction/materialisation loses nothing acknowledged, duplicates nothing, and its rollups are right |
| `integration::wal_replay_recovers_unflushed_data` | 25 acknowledged writes, an `abort()`, and all 25 replay |
| `rollup_repair_crash` | After a crash *inside* a repair, every rollup bucket equals the aggregate of its source rows — verified by breaking the repair and watching the test fail |
| `restart_invariants` (10 tests) | A clean close replays nothing; a rejected write never reappears; a failed flush loses nothing; an orphaned segment file is removed at open |
| `maintenance_interleaving` | Flush, compaction, GC, retention and rollup materialisation running **at once** against live writes lose no acknowledged point |

## Properties, not examples

- **A property test for every codec** — each round-trips arbitrary input
  bitwise-exactly, and refuses arbitrary bytes without decoding them.
- **3 fuzz targets** covering every encoding, run by a nightly CI job. The
  enum, its tag mapping and the list the fuzzer walks are generated from one
  macro, so a new codec cannot be added without landing in the fuzz corpus.
- **Numeric results are asserted against the same quantity computed a
  different way**, never for sign or finiteness alone.

## The documentation is tested too

Six guards compare prose to the tree:

| Guard | Asks |
|-------|------|
| `documented_config` | Does every ```toml block in these pages load with the real parser? |
| `documented_metrics` | Is every `chronix_*` name here emitted by non-test code? |
| `documented_env_vars` | Is every documented override actually read? |
| `documented_sql` | Does every documented SQL statement plan? |
| `documented_promql` | Does every documented PromQL query return something? |
| `route_inventory` | Is every route in the OpenAPI document, and every documented path a route? |

`scripts/check-docs.sh` covers the rest: crate names, ports, example links,
and configuration keys.

## Simulation

The frozen distributed tier has a deterministic simulator with a virtual
clock and a linearizability checker
([Simulation Testing](@/internals/simulation-testing.md)).

## See Also

- [Analytics Guide](@/docs/analytics.md) — forecast models, anomaly detection, SQL interface
- [Cluster Operations](@/docs/cluster.md) — setup, scaling, failover, replication
- [Operations Guide](@/docs/operations.md) — deployment, configuration, monitoring
- [Performance Tuning](@/docs/performance.md) — workload profiles, memory tuning
- [Security Guide](@/docs/security.md) — authentication, authorization, mTLS
- [Guide](@/docs/_index.md)

[`WalSink`]: https://github.com/hupe1980/chronix/blob/main/crates/chronix-engine/src/wal/sink.rs

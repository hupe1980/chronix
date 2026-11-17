# Vendored upstream PromQL test data

These files are Prometheus's own `promqltest` cases, run by `../promqltest.rs`
against chronix's evaluator. Expected values are upstream's, not ours.

| | |
|---|---|
| Source | https://github.com/prometheus/prometheus, `promql/promqltest/testdata/` |
| Tag | `v3.16.0` (commit `e2e28adeee2c5883d000fc82aca6008cfbf7e96d`) |
| Vendored | 2026-11-17 |
| Licence | Apache License 2.0, Copyright The Prometheus Authors |

| File | Upstream file |
|---|---|
| `duration_expression.test` | `duration_expression.test`, unmodified |
| `extended_vectors.test` | `extended_vectors.test`, unmodified |
| `first_over_time.test` | `functions.test` lines 1858–1957 (one `clear`-bounded block) |

**Re-vendor on every pin move**: when `CONFORMS_TO` in `../promql_surface.rs`
changes, replace these files from the new tag, update this table, and let the
runner's `SKIPPED` table say what changed.

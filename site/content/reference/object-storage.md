+++
title = "Object Storage & Cold Tier"
description = "Tiering segments to S3, GCS or Azure as Hive-partitioned Parquet that any Arrow reader can open."
weight = 90
+++

## Object Storage & Cold Tiering (`chronix-engine::objstore`)

### Archiving, not tiering

Cold storage is an **archive**: data old enough to archive is encoded to
Parquet, uploaded, verified, and then removed from the hot database. It is not
served through the hot read path, because a Parquet object has nowhere to keep a
series bloom or a tag index and a query that silently crossed the boundary would
change cost class with nothing in the plan saying so.

```text
Chronix::archive_cold_segments(&ArchiveConfig { cold_after, remote_url, .. })

  group by (measurement, shard) ─▶ read through the read path ─▶ encode
    ─▶ upload ─▶ verify ─▶ drop catalog entries ─▶ delete files
```

The order is the contract: verify before dropping the entries, drop the entries
before deleting the files. A failure at any step leaves the data hot and
queryable — never a catalog entry without a file.

### What an archive object contains

An object is built from the **read path**, so it holds what a query would
return: deduplicated last-write-wins, with tombstones applied. A deleted row is
not in the archive, and an overwritten point appears once, at its winning value.

Deduplication is only meaningful across every segment covering a range, so the
archive unit is a **`(measurement, shard)` group**. A group is archived only
when it is complete:

- no other live segment of the measurement overlaps its range;
- no unflushed memtable row overlaps it;
- every rollup fed by the measurement is materialised past it.

An incomplete group stays hot and is retried on the next pass.

| Component | Purpose |
|-----------|---------|
| `ObjectStoreBackend` | S3, GCS, Azure or local `file://`, with multipart upload and retry |
| `ParquetArchiveWriter` | Streams query-output batches into one Parquet object |
| `TieringEngine` | Uploads one archive object and verifies it landed |
| `ArchiveConfig` | `cold_after`, `remote_url`, `max_objects_per_run` |
| `ArchiveOutcome` | `objects`, `segments`, `rows`, `bytes`, `failed` |

### Layout

`measurement=<m>/shard=<n>/part-<min>-<max>.parquet` — Hive-style, so
`measurement` and `shard` are prunable columns to DuckDB, Spark, Polars and
DataFusion alike.

There is no `namespace=` level: a namespace is a tag on a series, so one segment
holds rows of many namespaces and there is nothing to name the directory after.
The namespace tag travels in the data, where it is queryable.

```rust
let outcome = db.archive_cold_segments(&ArchiveConfig {
    cold_after: Duration::from_secs(30 * 86_400),
    remote_url: "s3://bucket/chronix".to_string(),
    ..Default::default()
}).await?;
```

`chronixd` runs a pass periodically when `[cold_archive]` is configured, on a
binary built with `--features object-store`. Embedded callers invoke it
directly.

### Why the cold tier is Parquet

The hot tier stays `.csx` because time-series encodings, row-group zone maps and
series blooms measurably beat general Parquet on this workload. That advantage
is irrelevant to data nobody queries hot — what matters about an *archive* is
that something other than chronix can read it.

So cold objects are ordinary Parquet with an ordinary Arrow schema. Nulls
survive as definition levels rather than sentinels; tag columns are
dictionary-encoded explicitly (that is what makes an external reader's predicate
pushdown work on them) and float columns are not. The `key=value` directory
layout is what DuckDB, Spark and Polars read as a partitioned dataset.

The schema is the **hot tier's**: `_time` typed `Timestamp(ns)`, then tags
sorted, then fields sorted — plus the namespace tag when the measurement carries
one. A query moved from the hot table to the archive table is the same query,
`time_bucket` included.

What it gives up: series blooms and the tag index have nowhere to live in
Parquet, so a cold scan prunes on row-group statistics alone. That is the price
of being readable, charged on the data queried least.

```rust
// SQL over the archive, from chronix — one table per measurement.
chronix::sql::cold_tier::register_cold_tier(
    &ctx, "s3://bucket/chronix", "power", "power_cold",
).await?;
ctx.sql("SELECT host, avg(watts) FROM power_cold GROUP BY host").await?;
```

The archive registers as its **own named table** rather than being unioned into
the hot measurement: a query whose range crossed the tiering boundary would
otherwise change cost class by orders of magnitude with nothing in the plan
saying so. One table is one measurement, because a listing table has one schema.
See `crates/chronix/examples/cold_tier.rs` for the whole path.

---

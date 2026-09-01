+++
title = "Object Storage & Cold Tier"
description = "Tiering segments to S3, GCS or Azure as Hive-partitioned Parquet that any Arrow reader can open."
weight = 90
+++

## Object Storage & Cold Tiering (`chronix-engine::objstore`)

### Archiving, not tiering

Cold storage is an **archive**: a segment old enough to archive is uploaded,
verified, and then removed from the hot database. It is not served through the
hot read path, because a Parquet object has nowhere to keep a series bloom or a
tag index and a query that silently crossed the boundary would change cost class
with nothing in the plan saying so.

```text
Chronix::archive_cold_segments(&ArchiveConfig { cold_after, remote_url, .. })

  catalog scan ─▶ re-encode to Parquet ─▶ upload ─▶ verify ─▶ drop catalog
                                                              entry ─▶ delete
                                                                       file
```

The order is the contract: verify before dropping the entry, drop the entry
before deleting the file. A failure at any step leaves the segment hot and
queryable — never a catalog entry without a file.

| Component | Purpose |
|-----------|---------|
| `ObjectStoreBackend` | S3, GCS, Azure or local `file://`, with multipart upload and retry |
| `TieringEngine` | Re-encodes one segment and uploads it, verifying the object landed |
| `ArchiveConfig` | `cold_after`, `remote_url`, `cold_format`, `max_segments_per_run` |
| `ArchiveOutcome` | Segments and bytes archived; segments left hot because a step failed |

Objects are written under a Hive-style partition key,
`namespace=<ns>/shard=<n>/<name>.parquet`, so `namespace` and `shard` are
prunable columns to any reader.

```rust
let outcome = db.archive_cold_segments(&ArchiveConfig {
    cold_after: Duration::from_secs(30 * 86_400),
    remote_url: "s3://bucket/chronix".to_string(),
    ..Default::default()
}).await?;
```

There is no background scheduler yet: the call is the operation.

### Why the cold tier is Parquet

The hot tier stays `.csx` because time-series encodings, row-group zone maps and
series blooms measurably beat general Parquet on this workload. That advantage
is irrelevant to data nobody queries hot — what matters about an *archive* is
that something other than chronix can read it.

So cold objects are ordinary Parquet with an ordinary Arrow schema. Nulls
survive as definition levels rather than sentinels; tag columns are
dictionary-encoded explicitly (that is what makes an external reader's predicate
pushdown work on them) and float columns are not. The `key=value` directory
layout is what DuckDB, Spark and Polars read as a partitioned dataset, so
`namespace` and `shard` become prunable columns for free.

What it gives up: series blooms and the tag index have nowhere to live in
Parquet, so a cold scan prunes on row-group statistics alone. That is the price
of being readable, charged on the data queried least.

```rust
// SQL over the archive, from chronix
chronix::sql::cold_tier::register_cold_tier(&ctx, "s3://bucket/chronix", "power_cold").await?;
ctx.sql("SELECT host, avg(watts) FROM power_cold GROUP BY host").await?;
```

The cold tier registers as its **own named table** rather than being unioned
into the hot measurement: a query whose time range happened to cross the tiering
boundary would otherwise change cost class by orders of magnitude with nothing
in the plan saying so. See `crates/chronix/examples/cold_tier.rs` for the whole path.

---

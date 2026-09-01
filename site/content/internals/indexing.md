+++
title = "Indexing"
description = "Chronix uses multiple indexing structures to minimise the amount of data read from disk during query execution. Together they form a multi-level pruning pipeline that can eliminate >99% of…."
weight = 60
+++

Chronix uses multiple indexing structures to minimise the amount of data
read from disk during query execution. Together they form a **multi-level
pruning pipeline** that can eliminate >99% of segments before any data
decoding occurs.

## Time-Range Index

The simplest index: each segment records its `[time_min, time_max]` range.
A query with a time predicate `WHERE time BETWEEN t₁ AND t₂` skips any
segment whose range does not overlap `[t₁, t₂]`.

The index is stored sorted by `time_min`, enabling **O(log N) binary search**
for the first overlapping segment. For typical time-range queries this
eliminates all historical segments outside the query window.

## Bloom Filters

A **Bloom filter** (Bloom, 1970) is a space-efficient probabilistic data
structure that answers set-membership queries with:

- **No false negatives** — if the filter says "not present", the element
  is definitely absent.
- **Tunable false positive rate** — at the cost of more memory.

### Construction

A Bloom filter is a bit array of *m* bits with *k* hash functions. To insert
element *x*, compute *k* hash positions and set those bits to 1. To query,
check all *k* positions — if any is 0, the element is absent.

### False Positive Probability

For *n* inserted elements:

```
p = (1 − (1 − 1/m)^(kn))^k ≈ (1 − e^(−kn/m))^k
```

The optimal number of hash functions is:

```
k_opt = (m/n) · ln 2
```

At the optimal *k*, the false positive rate is:

```
p = (1/2)^k = (0.6185)^(m/n)
```

### Kirsch-Mitzenmacker Optimisation

Computing *k* independent hash functions is expensive. Kirsch & Mitzenmacker
(2008) proved that only **two** hash functions are needed — all *k* positions
can be generated as linear combinations:

```
hᵢ(x) = h₁(x) + i · h₂(x)   (mod m),   for i = 0, 1, …, k−1
```

This produces no increase in false positive rate while reducing hash
computation to 2 invocations per query (FNV-1a with two independent offset seeds).

### Per-Segment Bloom Filters

Chronix creates one Bloom filter per segment, keyed by `SeriesKey`. During
query execution, the filter answers: "does this segment contain any data for
`measurement=cpu, host=web-01`?" If the answer is "no" (definitively), the
segment is skipped entirely.

Bloom filters are persisted as `.bloom` sidecar files alongside segments and
reloaded into memory on database open.

## Tag Inverted Index

For queries that filter by tag values (e.g. `WHERE host = 'web-01'`), the
tag inverted index maps `(tag_key, tag_value) → {segment_ids}`:

```text
("host", "web-01") → [seg_003, seg_007, seg_012]
("host", "web-02") → [seg_004, seg_008]
("region", "us-east") → [seg_003, seg_004, seg_007, seg_008, seg_012]
```

This enables O(1) lookup of candidate segments for a tag predicate, avoiding
Bloom filter probes entirely when exact tag match is specified.

## Zone Maps (Min/Max Predicate Pushdown)

Each row group within a segment stores per-column **min and max** statistics.
A query predicate `WHERE value > 100` can skip row groups where `max < 100`
without decoding any data.

Zone maps are especially effective for:

- **Value-range filters** on fields (temperature > 50, latency > 200ms)
- **Time sub-filtering** within a segment's row groups
- **NULL column elimination** — skip segments where a column is entirely NULL

This technique is also known as *predicate pushdown* or *data skipping* and
is used by virtually all modern columnar storage systems (Parquet, ORC,
Delta Lake).

## Per-row-group tag blooms

The inverted index operates at segment granularity; a bloom filter per tag
column per row group narrows that to individual row groups, so a row group
that cannot hold the queried value is never decoded.

## Segment Catalog

The catalog maintains a persistent inventory of all segments, their metadata,
and lifecycle state (active, compacting, soft-deleted). It uses a
**manifest WAL** for crash-safe updates:

```text
manifest WAL: [AddSegment(seg_007)][RemoveSegment(seg_002)][...]
periodic snapshot: { active: [seg_003, seg_007, seg_012], ... }
```

On startup, the catalog loads the latest snapshot and replays manifest entries
since the snapshot, achieving fast recovery regardless of database age.

## References

- B. H. Bloom. "Space/Time Trade-offs in Hash Coding with Allowable
  Errors." *Communications of the ACM*, 13(7), 1970.
- A. Kirsch, M. Mitzenmacker. "Less Hashing, Same Performance: Building
  a Better Bloom Filter." *Random Structures & Algorithms*, 33(2), 2008.
- D. Sidirourgos et al. "Column Imprints: A Secondary Index Structure."
  *SIGMOD*, 2013.

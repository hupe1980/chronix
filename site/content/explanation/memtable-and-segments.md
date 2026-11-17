+++
title = "Memtable and segments"
description = "The in-memory memtable and the columnar .csx segment format: row groups, column blocks, blooms, zone maps and flush."
weight = 40
aliases = ["/internals/memtable/", "/internals/segments/"]
+++

A write lands in the memtable, an in-memory sorted structure, and is flushed to an immutable `.csx` segment on disk. This page explains both: how the memtable orders and sizes points, and how a segment lays out row groups, column blocks, blooms and statistics so a query can skip most of it.

## Memtable & Skip Lists

The memtable is the in-memory write buffer that sits between the WAL and
on-disk segment files. It must support **high-concurrency writes** with
ordered iteration (for flushing sorted columnar segments).

### Skip List Theory

Chronix uses a **skip list** (Pugh, 1990) as the underlying data structure
for the memtable. A skip list is a probabilistic alternative to balanced
trees that provides:

- **O(log n)** expected time for search, insert, and delete
- **Lock-free concurrent access** with compare-and-swap operations
- **Sequential iteration** in sorted key order

#### Structure

A skip list is a hierarchy of linked lists. The bottom level (L0) contains
all elements in sorted order. Each higher level is a random subset of the
level below, with each element independently promoted with probability *p*
(typically *p* = 0.5):

```text
Level 3:  ────────────────────── 40 ──────────────────── ∞
Level 2:  ──── 10 ────────────── 40 ──── 60 ──────────── ∞
Level 1:  ──── 10 ──── 25 ────── 40 ──── 60 ──── 80 ──── ∞
Level 0:  5 ── 10 ── 20 ── 25 ── 40 ── 50 ── 60 ── 75 ── 80 ── 90 ── ∞
```

#### Complexity Analysis

For *n* elements with promotion probability *p*:

| Operation | Expected Time | Worst Case |
|-----------|--------------|------------|
| Search | O(log n / log(1/p)) | O(n) |
| Insert | O(log n) | O(n) |
| Delete | O(log n) | O(n) |
| Iteration | O(n) | O(n) |

The expected number of levels is O(log n). With *p* = 0.5, the expected number
of pointers per element is 2 (each level doubles the skip distance), so the
space overhead is O(n).

#### Why Not a B-Tree?

Unlike B-trees, skip lists:

1. **Require no rebalancing** — insertions never cascade structural changes
2. **Support lock-free concurrency** — CAS-based insertion avoids global locks
3. **Have simple and cache-friendly iteration** — bottom-level traversal is a
   linear linked list scan

For the write-heavy workload of a time-series database, these properties
matter more than the B-tree's superior worst-case guarantees.

### Freeze-and-Swap Lifecycle

When the memtable reaches the configured size threshold:

```text
1. Current memtable is atomically swapped with a new empty one
2. The frozen memtable becomes read-only (still serves queries)
3. A background task iterates the frozen memtable in sorted order
4. Points are written to columnar segment file(s)
5. After flush completes:
   - Segment is registered in the catalog
   - WAL is truncated
   - Frozen memtable is dropped
```

The atomic swap uses an `Arc<RwLock<Memtable>>` so that writers never block
on flush and readers see a consistent snapshot of either the active or the
frozen memtable.

### Time-Shard Routing

Time-series writes are often slightly out of order (e.g. batch uploads,
clock skew between sources). The `ShardRouter` routes points to per-shard
memtables based on their timestamp:

```text
Point(ts=T)
    │
    ▼
ShardRouter
    ├── ts in shard_0 window → Memtable_0
    ├── ts in shard_1 window → Memtable_1
    └── ts outside tolerance → reject (TooOld / TooNew)
```

This ensures that each memtable covers a contiguous time range, making
segment time boundaries clean and enabling efficient time-range pruning
at query time.

### References

- W. Pugh. "Skip Lists: A Probabilistic Alternative to Balanced Trees."
  *Communications of the ACM*, 33(6), 1990.
- M. Herlihy, N. Shavit. *The Art of Multiprocessor Programming.*
  Morgan Kaufmann, 2012. (Lock-free data structures)

## Columnar Segments

Chronix stores flushed data in immutable **columnar segment files** (`.csx`).
The columnar format is fundamental to analytical query performance — it
enables compression, vectorised processing, and selective column reads.

### Row-Oriented vs Columnar Storage

Traditional row-oriented storage (e.g. B-tree pages) stores all columns of
a row together. This is efficient for OLTP workloads that read/write entire
rows, but wasteful for analytical queries that touch only a few columns.

**Columnar storage** stores each column contiguously:

```text
Row-oriented:          Columnar:
┌──────────────────┐   ┌────────────┐ ┌────────────┐ ┌────────────┐
│ ts₁ tag₁ val₁    │   │ ts₁        │ │ tag₁       │ │ val₁       │
│ ts₂ tag₂ val₂    │   │ ts₂        │ │ tag₂       │ │ val₂       │
│ ts₃ tag₃ val₃    │   │ ts₃        │ │ tag₃       │ │ val₃       │
│ ...              │   │ ...        │ │ ...        │ │ ...        │
└──────────────────┘   └────────────┘ └────────────┘ └────────────┘
```

Benefits for time-series analytics:

1. **Compression** — homogeneous data types compress far better (timestamps
   are all i64, values are all f64, tags are low-cardinality strings)
2. **Projection pushdown** — only read columns referenced by the query
3. **Vectorised execution** — SIMD instructions operate on contiguous arrays
4. **Cache efficiency** — scanning a single column uses the CPU cache optimally

### Segment File Format

```text
┌──────────────────────────────────────────────────┐
│ Header                                           │
│   magic: "CSX\0"  version: u16  row_count: u64   │
│   time_min: i64   time_max: i64                  │
│   measurement: String                            │
├──────────────────────────────────────────────────┤
│ Row Group 0                                      │
│   ┌─────────────────┐                            │
│   │ Column Block: ts │ encoding + compressed data│
│   │ Column Block: tag│ encoding + compressed data│
│   │ Column Block: val│ encoding + compressed data│
│   └─────────────────┘                            │
├──────────────────────────────────────────────────┤
│ Row Group 1                                      │
│   └── ...                                        │
├──────────────────────────────────────────────────┤
│ Column Metadata                                  │
│   per-column: name, type, encoding, offset, len  │
│   per-column stats: min, max, sum_i128,          │
│                     null_count, count             │
├──────────────────────────────────────────────────┤
│ Footer                                           │
│   metadata_offset: u64   CRC32c: u32             │
└──────────────────────────────────────────────────┘
```

#### Row Groups

Data within a segment is partitioned into **row groups** (typically 10 000
rows each). Row groups serve two purposes:

1. **Granular pruning** — zone maps (per-row-group min/max) enable skipping
   row groups that don't match query predicates, without decoding the data.
2. **Memory control** — the reader only needs to buffer one row group at a
   time, keeping memory usage bounded.

#### CRC32c Integrity

The footer contains a CRC32c checksum covering the entire file content.
This detects silent data corruption (bit rot) from storage media. On read,
the checksum is verified before decoding — corrupted segments are rejected
with `SegmentError::CorruptFile`.

#### Column Stats Binary Format

Each `ColumnStats` struct includes a `sum_i128: i128` field that accumulates
column values with 128-bit precision, avoiding overflow for large segments.
The addition of this field increases the per-column stats binary size from
60 to 76 bytes (the extra 16 bytes store the `i128` value).

### Design Trade-Offs

| Decision | Rationale |
|----------|-----------|
| Immutable files | No concurrency control needed; simple append-only lifecycle |
| Per-measurement segments | Targeted `DROP MEASUREMENT` and efficient single-measurement queries |
| Embedded metadata | Self-describing files; no external catalog needed for basic reads |
| Row-group chunking | Balances pruning granularity vs metadata overhead |
| Memory-mapped I/O | `mmap` with `MADV_SEQUENTIAL` avoids heap copies; OS manages page cache |

### Relationship to Apache Parquet

The `.csx` format draws inspiration from Apache Parquet and Apache ORC:

- **Column-level encoding** per data type (like Parquet's encoding field)
- **Row groups** for scan parallelism (like Parquet's row groups / ORC's stripes)
- **Footer with metadata** (like Parquet's file footer)

The key difference is that `.csx` is optimised for time-series — timestamps
always come first, encoded with pco or delta-of-delta (whichever is smaller),
and every row group carries its time range, so a time predicate skips row
groups without decoding a value column.

### References

- D. J. Abadi, S. R. Madden, N. Hachem. "Column-Stores vs. Row-Stores:
  How Different Are They Really?" *SIGMOD*, 2008.
- Apache Parquet Format Specification. <https://parquet.apache.org/>

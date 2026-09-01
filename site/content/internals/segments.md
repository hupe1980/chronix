+++
title = "Columnar Segments"
description = "Chronix stores flushed data in immutable columnar segment files (.csx). The columnar format is fundamental to analytical query performance — it enables compression, vectorised processing, and…."
weight = 40
+++

Chronix stores flushed data in immutable **columnar segment files** (`.csx`).
The columnar format is fundamental to analytical query performance — it
enables compression, vectorised processing, and selective column reads.

## Row-Oriented vs Columnar Storage

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

## Segment File Format

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

### Row Groups

Data within a segment is partitioned into **row groups** (typically 10 000
rows each). Row groups serve two purposes:

1. **Granular pruning** — zone maps (per-row-group min/max) enable skipping
   row groups that don't match query predicates, without decoding the data.
2. **Memory control** — the reader only needs to buffer one row group at a
   time, keeping memory usage bounded.

### CRC32c Integrity

The footer contains a CRC32c checksum covering the entire file content.
This detects silent data corruption (bit rot) from storage media. On read,
the checksum is verified before decoding — corrupted segments are rejected
with `SegmentError::CorruptFile`.

### Column Stats Binary Format

Each `ColumnStats` struct includes a `sum_i128: i128` field that accumulates
column values with 128-bit precision, avoiding overflow for large segments.
The addition of this field increases the per-column stats binary size from
60 to 76 bytes (the extra 16 bytes store the `i128` value).

## Design Trade-Offs

| Decision | Rationale |
|----------|-----------|
| Immutable files | No concurrency control needed; simple append-only lifecycle |
| Per-measurement segments | Targeted `DROP MEASUREMENT` and efficient single-measurement queries |
| Embedded metadata | Self-describing files; no external catalog needed for basic reads |
| Row-group chunking | Balances pruning granularity vs metadata overhead |
| Memory-mapped I/O | `mmap` with `MADV_SEQUENTIAL` avoids heap copies; OS manages page cache |

## Relationship to Apache Parquet

The `.csx` format draws inspiration from Apache Parquet and Apache ORC:

- **Column-level encoding** per data type (like Parquet's encoding field)
- **Row groups** for scan parallelism (like Parquet's row groups / ORC's stripes)
- **Footer with metadata** (like Parquet's file footer)

The key difference is that `.csx` is optimised for time-series — timestamps
always come first and are always delta-of-delta encoded, enabling fast
time-range predicate evaluation without decoding value columns.

## References

- D. J. Abadi, S. R. Madden, N. Hachem. "Column-Stores vs. Row-Stores:
  How Different Are They Really?" *SIGMOD*, 2008.
- Apache Parquet Format Specification. <https://parquet.apache.org/>

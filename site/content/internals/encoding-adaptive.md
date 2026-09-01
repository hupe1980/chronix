+++
title = "Adaptive Encoding Selection"
description = "A time-series database ingests many different columns with wildly different data characteristics. The optimal encoding varies per type and per workload."
weight = 130
+++

## The Selection Problem

A time-series database ingests many different columns with wildly different
data characteristics. The optimal encoding varies per type and per workload:

| Column Type | Common Pattern | Best Encoding |
|-------------|---------------|---------------|
| Timestamps | Regular intervals | Delta-of-delta (§2.1) |
| Float metrics | Values are decimals with few fractional digits | ALP (§2.2) |
| Float metrics | Small changes between values | XOR / Chimp (§2.2) |
| Integer counters | Monotonically increasing | ZigZag + bit-packing (§2.3) |
| String tags | Low cardinality | Dictionary (§2.4) |
| Booleans | Sparse flags | Bitmap (§2.4) |

Hard-coding a single encoding per column type wastes compression for
workloads that don't match the assumption.

## Adaptive Selection Strategy

Chronix selects encodings **per column per row-group** at segment flush time.
This means a single segment file can use different encodings for different
row groups of the same column if the data characteristics shift.

### Decision Flow

```text
Column Type
├── Timestamp → always delta-of-delta
├── Bool     → always bitmap
├── String   → dictionary (fallback: raw bytes if cardinality > threshold)
├── Integer
│   ├── sample deltas → if narrow range → ZigZag + bit-pack
│   └── fallback → raw varint
└── Float
    ├── trial-encode the sample → ALP, Chimp128, Patas, Chimp, Gorilla
    └── fallback → IEEE 754 raw
```

### Sampling Heuristic

For integer and float columns, the encoder inspects a **stratified sample** of
values (4 strata across the dataset, budget/4 values drawn from each stratum) to estimate:

1. **Bit-width of deltas** (integers) — determines whether bit-packing
   provides sufficient reduction
2. **Trial encoding on a stratified sample** (floats) — every candidate
   codec encodes the sample and the smallest output wins. ALP leads each
   candidate list because decimal-valued data is the common case; when the
   values are genuinely non-decimal it loses the trial and an XOR codec is
   selected instead

The sample cost is O(n/16) ≈ O(n) in the same pass as writing the row group.

## Encoding Pipeline

Each column passes through a two-stage pipeline:

```text
Stage 1: Type-specific encoding
  ┌─────────────┐     ┌──────────────────┐
  │  Raw values  │ ──▸ │  Encoded bytes   │
  └─────────────┘     │  (delta, XOR, …) │
                       └──────────────────┘

Stage 2: Optional block compression
  ┌──────────────────┐     ┌──────────────────────┐
  │  Encoded bytes   │ ──▸ │  Compressed block     │
  └──────────────────┘     │  (LZ4, Zstd, None)   │
                            └──────────────────────┘
```

**Stage 1** (type-specific encoding) reduces redundancy by exploiting the
mathematical structure of the data. **Stage 2** (general-purpose compression)
catches any remaining byte-level redundancy.

### Stage 2 Compression

| Algorithm | Ratio | Speed | Notes |
|-----------|-------|-------|-------|
| None | 1× | ∞ | Already-compressed or latency-critical |
| LZ4 | 2–3× | ~4 GB/s decode | Default — excellent decode speed |
| Zstd | 3–5× | ~1.5 GB/s decode | Archival / cold tier |

The stage-1 encoding is critical because general-purpose compressors cannot
exploit columnar structure as effectively. A well-chosen stage-1 encoding
typically reduces data 4–60× before stage-2 compression adds another 2–5×.

## Metadata

The chosen encoding is stored in the **column metadata** within the segment
file's footer. This allows the decoder to select the correct decompression
path without probing the data:

```text
ColumnMeta {
    name: "cpu_utilization",
    data_type: Float64,
    encoding: Chimp,
    compression: Lz4,
    offset: 0x1A00,
    length: 4096,
    stats: { min: 0.01, max: 99.87, null_count: 0 },
}
```

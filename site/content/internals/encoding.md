+++
title = "Column Encoding"
description = "Time-series data has strong statistical regularities that general-purpose compressors (LZ4, zstd) cannot fully exploit. Chronix uses type-specific encoders that understand the structure of…."
weight = 80
+++

Time-series data has strong statistical regularities that general-purpose
compressors (LZ4, zstd) cannot fully exploit. Chronix uses **type-specific
encoders** that understand the structure of timestamps, floating-point
values, integers, strings, and booleans to achieve compression ratios
far beyond what generic algorithms provide.

## Design Philosophy

1. **Type-specific** — each column type gets a specialised encoder
2. **Lossless** — every encoder guarantees bitwise-exact roundtrip
3. **Zero-copy where possible** — decoders operate on byte slices
4. **Auto-selection** — the unified API picks the best encoder per column
5. **Graceful fallback** — if a specialised encoder doesn't achieve 2×
   compression, the plain (uncompressed) encoder is used instead

## Compression Landscape

These are **per-column** ratios, not whole-segment ones. A segment stores
timestamps, tags and fields together, so its overall ratio is dominated by the
worst-compressing column and by fixed metadata — the measured end-to-end figure
is **6.2×** on 2-decimal meter readings (`chronix/tests/segment_compression.rs`).
Quoting a column ratio as a database ratio is how compression claims get
inflated by an order of magnitude.

| Column Type | Technique | Typical Ratio | Key Insight |
|-------------|-----------|---------------|-------------|
| Timestamps | pco (delta-of-delta second) | 2.7× jittered, 345× with gaps, 1092× regular | pco entropy-codes the interval deltas, so millisecond jitter costs bits rather than a varint per point. Delta-of-delta made a jittered column *larger* than plain (0.9×) |
| Floats (decimal) | pco (ALP second) | 5–29× realistic, 46–85× clean counters | The value was a scaled integer before IEEE-754 stored it; pco then entropy-codes the deltas |
| Floats (non-decimal) | Gorilla / Chimp / Patas | 1.3–2× | XOR of successive values is sparse |
| Floats (random) | Plain | 1× | Incompressible by construction |
| Integers | Delta + ZigZag | 10–25× | Monotonic sequences → small deltas |
| Integers (narrow) | Frame-of-Reference | 15–40× | Values in small range → min-offset bit-packing |
| Strings | Dictionary | 20–50× | Low-cardinality tags → integer indices |
| Booleans | Bitmap | 8× | 1 bit per value vs 1 byte |

## Encoding Pipeline

When a segment is written, each column passes through the encoding pipeline:

```text
Raw column data
     │
     ▼
AdaptiveSelector (analyse sample → detect pattern)
     │
     ├─ Constant      → single-value encoding
     ├─ NarrowRange   → Frame-of-Reference (FOR) encoding
     ├─ Decimal-valued → ALP
     ├─ SlowlyVarying → Patas, Chimp or Gorilla
     ├─ Periodic      → delta + repeat
     └─ Random        → plain fallback
     │
     ▼
TypeEncoder (delta, gorilla, chimp, integer, dict, bitmap)
     │
     ▼
Optional LZ4 post-compression
     │
     ▼
EncodedBlock (stored in segment file)
```

The encoding choice is stored in the column metadata so the reader can
select the correct decoder without trial decoding.

See the sub-pages for detailed theory on each encoding method:

- **[Delta-of-Delta Timestamps](@/internals/encoding-timestamps.md)**
- **[ALP, Gorilla, Chimp & Patas Float Compression](@/internals/encoding-floats.md)**
- **[Integer & ZigZag Encoding](@/internals/encoding-integers.md)**
- **[Dictionary & Bitmap Encoding](@/internals/encoding-dictionary.md)**
- **[Adaptive Selection](@/internals/encoding-adaptive.md)**

## References

- T. Pelkonen et al. "Gorilla: A Fast, Scalable, In-Memory Time Series
  Database." *Proc. VLDB*, 2015.
- A. Afroozeh, L. Kuffo, P. Boncz. "ALP: Adaptive Lossless floating-Point
  Compression." *Proc. ACM Manag. Data* 2(1), SIGMOD 2024.
- P. Liakos, K. Papakonstantinopoulou, Y. Kotidis. "Chimp: Efficient
  Lossless Floating Point Compression for Time Series Databases."
  *Proc. VLDB*, 2022.

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
| Decimals | The integer stack, on the mantissa | 475× noisy power, 1040× monotone register, 2731× constant tariff | The value *is* a scaled integer — nothing to recover, so RLE, FOR and pco apply directly |

### What a decimal column costs against the float it replaces

The row above is the "Floats (decimal)" row from the other end. A `231.45`
stored as an `f64` has had its integer destroyed by IEEE-754, and ALP and pco
spend their first stage recovering it; a `Decimal` column never destroyed it.
The scale is stored once in the column's metadata, so what reaches the codec
is a column of plain `i128` mantissas.

Two physical forms, chosen per block:

- **Narrow** — every mantissa fits an `i64`, which is every realistic one.
  The block goes straight to the integer encoder, so a decimal column
  inherits RLE, frame-of-reference and pco from the same
  trial-and-keep-the-smallest selection an `i64` field gets.
- **Wide** — 38-digit financial quantities, or a scale large enough that
  ordinary values overflow 63 bits. Delta + ZigZag + LEB128, at most 19 bytes
  per value against the 16 a raw `i128` would take.

The same values as `f64` reach 85× on the noisy power column and 1074× on the
monotone register: the exact type is worth 5.6× where the float codec's
mantissa recovery struggles, and costs two bytes a block — the form byte and
the delegated tag — where it does not
(`chronix-encoding/tests/decimal_codec_ratios.rs`).

## Encoding Pipeline

When a segment is written, each column passes through the encoding pipeline:

```text
Raw column data
     │
     ▼
AdaptiveSelector — sample the column, detect its pattern
     │   floats:   Constant │ Periodic │ SlowlyVarying │ Random
     │   integers: Constant │ RegularInterval │ NarrowRange │ Irregular
     │   strings:  LowCardinality │ HighCardinality
     ▼
The pattern orders a candidate list; it does not pick the codec.
For every non-constant float list that is pco, then ALP, then the
XOR codecs; narrow integers put Frame-of-Reference first, and
low-cardinality strings the dictionary.
     │
     ▼
Trial-encode a stratified sample with each candidate; smallest wins
     │
     ▼
Optional LZ4 or Zstd post-compression (skipped above ≈8× already)
     │
     ▼
EncodedBlock (stored in segment file)
```

**The winner is measured, not predicted.** A candidate that loses costs one
sample encode, which is why a workload the leading codecs are bad at costs a
trial rather than a bad ratio. The choice is recorded in the column metadata,
so the reader decodes directly with no trial of its own.

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

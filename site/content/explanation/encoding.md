+++
title = "Encoding"
description = "How Chronix compresses columns: pcodec first, ALP and the XOR codecs behind it, and per-block adaptive selection by trial encoding."
weight = 60
aliases = ["/internals/encoding/", "/reference/encoding/", "/internals/encoding-adaptive/"]
+++

Every column block of a segment is encoded by a codec chosen for its data,
then optionally compressed with LZ4 (the default) or Zstd. The choice is made
per block by **trial**: a stratified sample is encoded with each candidate
codec and the smallest output wins, so a workload the usual favourite handles
badly costs one sample encode rather than a bad ratio. The winner is recorded in
the block's metadata; the reader decodes without trying anything. Every codec is
lossless, bit for bit. The codecs live in `chronix-encoding`, which is also
published as a standalone crate.

## What each column type gets

| Column | Candidates (tried; smallest wins) | Measured ratio | Pinned by |
|---|---|---|---|
| Timestamps | pco, delta-of-delta | 2.7× jittered 1 s sampler, 345× with gaps, 1 092× perfectly regular | `chronix-encoding/tests/timestamp_codec_check.rs` |
| Floats that are decimals (`231.45`) | pco, ALP, Chimp128, Patas, Chimp, Gorilla | 5–29× realistic meter and inverter data; 46–85× clean low-precision counters | `chronix-encoding/tests/codec_field_check.rs`, `float_codec_ratios.rs` |
| Floats that are not | same list; an XOR codec usually wins | 1.0–2.5× | (same) |
| Constant floats | RLE | a few bytes per block | |
| Integers | RLE, frame-of-reference, pco, delta + zigzag | depends on range and steps | |
| `Decimal` | the integer codecs, on the `i128` mantissa | 475× noisy power, 1 040× monotone register, 2 731× constant tariff | `chronix-encoding/tests/decimal_codec_ratios.rs` |
| Strings (tags) | dictionary | depends on cardinality | |
| Booleans | bitmap | 1 bit per value | |

These are **column** ratios. A segment's ratio is set by its worst column and its
fixed metadata: **6.2×** end to end on two-decimal meter readings
(`chronix/tests/segment_compression.rs`). A column ratio quoted as a database
ratio overstates it by an order of magnitude.

## Why pcodec first

Most metric values began as decimals — a meter reporting `231.45` W. IEEE 754
stores that as a binary fraction, and XOR codecs (Gorilla, Chimp) then compress
the bit pattern, which is nearly random in the low bits: about 1.0–1.2× on
real data. **pco** and **ALP** instead recover the decimal integer behind the
double (`23145 × 10⁻²`), check that it round-trips exactly, and encode the
integers; pco then entropy-codes their deltas, which is where it pulls ahead of
ALP (4–9× on the same data). Values that are not decimals fall through to the
XOR codecs, which remain in the list for exactly that case. See
[Float encoding](@/explanation/encoding-floats.md).

The same reasoning applies to timestamps: a sampler that jitters by a few
milliseconds defeats delta-of-delta — its varint per point made a jittered
column *larger* than plain (0.9×) — while pco entropy-codes the interval deltas
(2.7×). Delta-of-delta stays a candidate for perfectly regular clocks. See
[Integer, timestamp and dictionary encoding](@/explanation/encoding-integers-and-time.md).

## Decimals cost less than the floats they replace

A `Decimal` column never destroys the integer, so there is nothing to recover:
the scale is stored once per column and the codec sees plain mantissas.

- **Narrow** blocks — every mantissa fits an `i64`, which is every realistic
  one — go to the integer codecs and get the same trial.
- **Wide** blocks — 38-digit quantities — use delta + zigzag + LEB128, at most
  19 bytes per value.

The same data as `f64` reaches 85× on noisy power and 1 074× on the monotone
register: exactness is worth 5.6× where float recovery struggles and costs two
bytes per block where it does not.

## Block compression

After encoding, a block may be compressed again:

| Codec | Use |
|---|---|
| LZ4 (default) | fast decode; little gain on top of a good encoding |
| Zstd (`zstd_level`, optional dictionary training) | smaller files when CPU is cheaper than disk |
| none | latency first |

A block that encoding already shrank about 8× is not compressed again — the
second pass would cost CPU for nothing.

## Related

- [Float encoding](@/explanation/encoding-floats.md) — pco, ALP and the XOR codecs in detail
- [Integer, timestamp and dictionary encoding](@/explanation/encoding-integers-and-time.md)
- [Memtable and segments](@/explanation/memtable-and-segments.md) — where blocks live

References: Pelkonen et al., *Gorilla* (VLDB 2015); Liakos et al., *Chimp*
(VLDB 2022); Afroozeh, Kuffo & Boncz, *ALP* (SIGMOD 2024); the pcodec format
specification.

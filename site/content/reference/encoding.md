+++
title = "Column Encoding"
description = "The codec layer: ALP for decimal floats, Gorilla/Chimp/Patas XOR codecs, delta-of-delta timestamps, frame-of-reference integers, dictionary tags, and adaptive per-block selection."
weight = 30
+++

## Column Encoding (`chronix-encoding`)

The encoding crate provides type-specific compression for columnar data:

| Encoding          | Type       | Description                                          |
|-------------------|------------|------------------------------------------------------|
| Delta-of-delta    | Timestamps | Variable-length bit-packing of second derivatives    |
| ALP (SIGMOD'24)   | Floats     | Reconstructs the decimal integer behind each double, then FOR + bit-packing |
| Chimp (VLDB'22)   | Floats     | Leading-zero bucketing XOR compression               |
| Chimp128          | Floats     | Chimp with 128-entry ring buffer for periodic data   |
| Patas (VLDB'23)   | Floats     | Byte-aligned XOR — faster decode, similar ratio      |
| Gorilla (FB'15)   | Floats     | Classic XOR-based float compression (fallback)       |
| Delta+ZigZag      | Integers   | Bit-packed deltas with ZigZag variable encoding      |
| FOR / PFOR        | Integers   | Frame-of-reference for narrow-range values           |
| Dictionary        | Strings    | Index-based encoding for low-cardinality values      |
| Bitmap            | Booleans   | 1-bit-per-value with separate null bitmap            |
| RLE               | Any        | Run-length for constant / repetitive columns         |
| Plain             | Any        | Uncompressed fallback for all types                  |

### ALP — the primary float codec

XOR codecs treat a double as an opaque bit pattern and hope consecutive
values share a prefix. Most doubles in a time-series database never had 15
significant digits: they are decimals a sensor or meter produced with two or
three fractional places. `231.45` is not a random 64-bit pattern — it is the
integer `23145` with a known scale.

ALP picks one exponent pair `(e, f)` per block and stores
`i = round(v · 10^e · 10^-f)`, decoding as `v = i · 10^f · 10^-e`. The
integers are frame-of-reference coded and bit-packed, so a meter reporting
200–250 W to two decimals needs 13 bits per value instead of 64. Every value
is verified by decoding it and comparing bit patterns; anything that does not
round-trip exactly — genuine high-precision doubles, `NaN`, `±inf`, `-0.0` —
is stored verbatim as an exception, so the codec is bitwise lossless like
every other encoder in the crate.

Measured against the same data (`cargo test -p chronix-encoding --test
float_codec_ratios -- --nocapture`):

| Workload | ALP | Chimp | Chimp128 | Gorilla | Patas |
|---|---|---|---|---|---|
| Power meter, W, 2 dp   | **4.1×** | 1.1× | 0.9× | 1.2× | 1.1× |
| Temperature, °C, 1 dp  | **9.1×** | 1.1× | 5.9× | 1.2× | 1.0× |
| Energy, kWh, 3 dp      | **4.6×** | 1.5× | 1.5× | 1.5× | 1.3× |
| Scientific doubles     | 0.9× | 1.0× | 0.9× | 1.0× | 0.9× |

The last row is the point of keeping the XOR codecs: on genuinely
non-decimal doubles ALP loses the trial encoding and the adaptive selector
picks one of them instead. The paper's second scheme, `ALP_RD`, exists for
that case; Chronix covers it with the codecs it already had rather than
implementing a fallback twice.
**Throughput** (`cargo bench -p chronix-encoding`, 1M values, Apple M-series):

| | ALP | Chimp |
|---|---|---|
| encode | **2.86 ms** (350M values/s) | 4.40 ms |
| decode | **5.47 ms** (183M values/s) | 18.16 ms |

The per-block exponent search does not cost anything net: ALP is 1.5× faster
to encode than Chimp and 3.3× faster to decode, because bit-packed integers
are far cheaper to unpack than a serial XOR window.

### Unified API

`ColumnEncoder` auto-selects the best encoding per column type with a
configurable `MIN_COMPRESSION_RATIO` (default 1.5×) threshold. If the
type-specific encoding doesn't achieve sufficient compression, it falls back
to `Plain`. For floats the candidates are trial-encoded against a stratified
sample and the smallest wins; ALP leads every non-constant candidate list,
with the fallback chain ALP → Chimp128 → Patas → Chimp → Gorilla → Plain.
Chimp128 is preferred over plain Chimp for periodic sensor data (selected via
`FloatPattern::Periodic`) where its 128-entry ring buffer achieves 5–15%
better compression.

`ColumnDecoder` dispatches on the `EncodingType` tag stored in each
`EncodedBlock` header for zero-configuration decoding.

### Shared Utilities (`coding.rs`)

All encoders share a common wire format through the `coding` module:

- `write_header(buf, encoding_type, value_count)` — writes a 5-byte header
  (`EncodingType` tag + `u32` count)
- `read_header(data)` — parses and validates the header, returning a `Header`
  struct with the remaining payload slice
- This eliminates header format duplication across encoder/decoder pairs
- `pack_bits` / `unpack_bits` — u64 word-accumulator bit-packing for
  fixed-width integer encoding (replaces per-bit offset tracking)

### SIMD & Encoding Performance (`simd.rs`, `delta.rs`)

The encoding crate uses three levels of optimization:

1. **Word-accumulator `BitWriter`** — The shared `BitWriter` (used by Delta,
   Chimp, Gorilla) packs bits into a `u64` accumulator, flushing 8-byte words
   as needed. Each `write_bits()` call does at most two word-level operations
   regardless of bit count, eliminating the previous per-bit function call
   overhead (e.g. writing 64 bits now requires 1–2 ops instead of 64).

2. **Byte-at-a-time `BitReader`** — The shared `BitReader::read_bits()` extracts
   up to 8 bits per iteration (≤9 iterations for a 64-bit read vs. 64 before),
   with a single bounds check per byte boundary.

3. **Auto-vectorizable batch operations** (`simd.rs`) — Batch XOR, leading-zero,
   trailing-zero, delta, and ZigZag functions use tight loops over contiguous
   slices that LLVM auto-vectorizes into SIMD instructions (SSE2/AVX2, NEON)
   when compiled with `RUSTFLAGS="-C target-cpu=native"`.

**Design rationale:** XOR-based float compression (Gorilla, Chimp) is inherently
serial at the encoding *decision* level — each value's encoding depends on the
previous value's XOR window. Explicit SIMD intrinsics yield minimal benefit on the
serial chain. The largest speedup comes from the `BitWriter`/`BitReader` rewrite,
which benefits *all* encoders transparently.
## Encoding (`chronix-encoding`)

### Adaptive Encoding Selection

`AdaptiveSelector` uses stratified sampling (4 strata across the dataset, budget/4 values per stratum) to choose the optimal encoding:

| Pattern | Encoding |
|---------|----------|
| Constant floats | Plain (identity) |
| Decimal-valued floats (most metric data) | ALP |
| Periodic floats | Chimp128 (128-entry ring buffer) |
| Slowly varying floats | Patas / Chimp (XOR-based) |
| Random floats | Gorilla (XOR-based) |
| Low-cardinality strings | Dictionary |
| High-cardinality strings | Plain |
| Constant integers | Plain |
| Regular interval integers | Delta-of-delta |

### Chimp Optimization

Buffer-reuse pattern with `encode_core()` / `decode_core()` shared methods.
`decode_into()` allows zero-allocation decoding into a pre-allocated buffer.

### Chimp128 Encoder

`Chimp128Encoder` / `Chimp128Decoder` extend the Chimp algorithm with a
128-entry ring buffer (`EncodingType::Chimp128 = 16`). Instead of XOR-ing
against only the previous value, Chimp128 searches the ring buffer for the
best reference value (lowest XOR cost). This exploits periodic patterns in
sensor data where values repeat at regular intervals.

- **Compression improvement**: 5–15% smaller output on periodic sensor data
  compared to standard Chimp
- **Auto-selection**: The `AdaptiveSelector` chooses Chimp128 when it detects
  `FloatPattern::Periodic` in the sample window
- **Fallback**: Non-periodic data falls through to standard Chimp → Gorilla → Plain

### Compression

- **LZ4** — default for hot data (fast decompression)
- **Zstd** — configurable levels (default 3 for balanced, level 9 for warm tier)
- **Zstd with dictionary** — optional dictionary training from first row group
  samples for 20–40% better compression on homogeneous schemas; dictionary
  embedded in segment metadata
- Auto-detect on decompression via codec tag: LZ4 (0x01), Zstd (0x02),
  Zstd-dict (0x03)

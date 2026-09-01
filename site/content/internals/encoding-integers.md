+++
title = "Integer & ZigZag Encoding"
description = "Integer columns in time-series data (counters, sequence numbers, byte counts) are often monotonically increasing. Delta encoding stores differences."
weight = 110
+++

## Delta Encoding for Integers

Integer columns in time-series data (counters, sequence numbers, byte counts)
are often monotonically increasing. Delta encoding stores differences:

```
δᵢ = xᵢ − xᵢ₋₁
```

For a counter incrementing by ~100 per sample, the raw values might be
64-bit, but the deltas fit in 7–8 bits — a ~9× reduction before any
further compression.

## ZigZag Encoding

After delta encoding, the residuals are **signed** integers (they can be
negative if the series decreases). Variable-length unsigned integer encoding
(varint) wastes bits on negative values because their two's-complement
representation has the sign bit set in the most significant position.

**ZigZag encoding** (used by Protocol Buffers and Apache Avro) maps signed
integers to unsigned integers such that small-magnitude values map to small
unsigned values:

```
encode(n) = (n << 1) ^ (n >> 63)     // for i64
decode(n) = (n >> 1) ^ -(n & 1)
```

The mapping is:

| Signed | Unsigned |
|--------|----------|
| 0 | 0 |
| −1 | 1 |
| 1 | 2 |
| −2 | 3 |
| 2 | 4 |
| −127 | 253 |
| 127 | 254 |
| −128 | 255 |

Both `+1` and `−1` map to small unsigned values (2 and 1), whereas in
two's-complement, `−1` is `0xFFFFFFFFFFFFFFFF` — requiring the full 64 bits.

## Variable-Length Bit-Packing

After ZigZag encoding, the unsigned values are packed with a variable-length
scheme similar to the timestamp encoder. The bit-width per value adapts to
the actual range:

```text
If max_value < 2^7   → 7 bits per value
If max_value < 2^14  → 14 bits per value
If max_value < 2^21  → 21 bits per value
Otherwise            → 64 bits per value
```

The bit-width is stored once in the block header, so all values in a block
use the same width — this enables fast SIMD-friendly decoding.

## Compression Analysis

For a monotonically increasing counter with step ~100 (±10):

```
Raw:     1000 × 64 bits = 8000 bytes
Deltas:  ~100 each → ZigZag → ~200 each → 8 bits each
Packed:  1000 × 8 bits + header = ~1008 bits ≈ 126 bytes
Ratio:   63×
```

For a more variable counter with deltas in [−1000, +1000]:

```
ZigZag → [0, 2000] → 11 bits each
Packed: 1000 × 11 bits ≈ 1375 bytes
Ratio:  5.8×
```

## Frame-of-Reference (FOR) Encoding

When integer values cluster in a **narrow range** (max − min ≤ 65535) but
are not sequential, delta encoding produces large residuals. FOR encoding
stores values as offsets from the block minimum:

```
offsetᵢ = xᵢ − min(x)
```

The bit width equals `ceil(log₂(max − min + 1))`, and all offsets are
bit-packed at that width.

### Wire Format

```text
[count: u32 LE][reference: i64 LE][bit_width: u8][packed offsets]
```

### When FOR Wins

| Data Pattern | Delta+ZigZag | FOR | Winner |
|---|---|---|---|
| HTTP status codes (200–504) | ~10 bits/val | 9 bits/val | FOR |
| Port numbers (1024–65535) | ~16 bits/val | 16 bits/val | Tie |
| Monotonic counter +100 ±10 | 8 bits/val | ~17 bits/val | Delta |

The adaptive encoder runs a **3-way competition** (fixed, varint, FOR)
on every integer block and picks the smallest encoding.

### Compression Analysis (FOR)

For HTTP status codes [200, 201, 301, 302, 404, 500, 502, 503, 504]:

```
Range: 504 − 200 = 304 → 9 bits per offset
1000 values × 9 bits = 1125 bytes + 13 bytes header
Ratio: 8000 / 1138 ≈ 7×
```

### Native u64 Path

The FOR encoder uses **separate code paths** for signed and unsigned
integers. The `encode_u64` / `decode_u64` functions perform all
arithmetic natively in `u64` space, avoiding the i64 reinterpretation
that would cause bit-width inflation at the sign boundary
(e.g. values near `u64::MAX / 2` would span `[i64::MAX, i64::MIN]`,
yielding bit_width = 64 instead of the correct 1–2 bits).

### Encoding Type Tags

`ForI64 = 17` and `ForU64 = 18` are assigned in the `EncodingType` enum,
appended after the existing 16 encoding variants.

## U64 Encoding

Unsigned 64-bit integers skip the ZigZag step and go directly to
variable-length bit-packing. This is used for fields that are inherently
unsigned (byte counts, packet counts, hash values).

+++
title = "Dictionary & Bitmap Encoding"
description = "String columns in time-series data are almost always low-cardinality — tag values like 'us-east', 'us-west', 'production', 'staging'. A measurement with millions of rows might have only 5–50…."
weight = 120
+++

## Dictionary Encoding (Strings)

### The Insight

String columns in time-series data are almost always **low-cardinality** —
tag values like `"us-east"`, `"us-west"`, `"production"`, `"staging"`. A
measurement with millions of rows might have only 5–50 unique tag values.

Dictionary encoding exploits this by storing unique strings once in a
**string table** and replacing each row's value with a compact integer index:

```text
Original:    ["us-east", "us-west", "us-east", "us-east", "us-west"]

Dictionary:  { 0: "us-east", 1: "us-west" }
Indices:     [0, 1, 0, 0, 1]
```

### Bit-Width Selection

If there are *k* unique values, each index requires ⌈log₂(k)⌉ bits:

| Unique Values | Bits per Index | 1000 Rows (bytes) | vs Raw (avg 8 chars) |
|---------------|---------------|-------------------|---------------------|
| 2 | 1 | 125 | 64× |
| 8 | 3 | 375 | 21× |
| 64 | 6 | 750 | 10.7× |
| 256 | 8 | 1 000 | 8× |
| 65 536 | 16 | 2 000 | 4× |
| 4 294 967 296 | 32 | 4 000 | 2× |

The dictionary itself adds a one-time cost proportional to the total unique
string length — negligible for low-cardinality columns.

### Relationship to Other Systems

Dictionary encoding is a standard technique in columnar storage:

- **Apache Parquet** uses dictionary encoding as its default for string columns
- **Apache ORC** uses dictionary + run-length encoding
- **Apache Arrow** dictionaries are the in-memory counterpart

The Chronix implementation is compatible with Arrow dictionary arrays,
enabling zero-copy handoff to the Arrow-based query engine.

## Bitmap Encoding (Booleans)

### The Insight

Boolean columns store one-bit values (`true` / `false`) but are typically
represented as one-byte values in memory (`0x00` or `0x01`). Bitmap encoding
packs 8 boolean values into a single byte:

```text
Original (bytes): [T, F, T, T, F, F, T, F]  = 8 bytes
Bitmap (bits):     1  0  1  1  0  0  1  0   = 1 byte
```

### Implementation

- Bits are packed MSB-first within each byte
- The final byte is zero-padded if the count is not a multiple of 8
- Decoding uses shift-and-mask: `bit_i = (byte[i/8] >> (7 - i%8)) & 1`

### Compression

The compression ratio is exactly **8×** (8 bits per byte ÷ 1 bit per value),
regardless of the data distribution. This is optimal — boolean values have
at most 1 bit of entropy.

For columns with long runs of identical values, **run-length encoding** (RLE)
would compress further (e.g. 1000 consecutive `true` values → a single
run descriptor). Chronix currently uses simple bitmap encoding, which
provides consistent 8× compression without the complexity of RLE.

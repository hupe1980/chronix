+++
title = "Delta-of-Delta Timestamp Encoding"
description = "Time-series timestamps are monotonically increasing and typically arrive at regular intervals (e.g. every 10 seconds). Delta encoding stores the difference δᵢ = tᵢ − tᵢ₋₁ instead of the raw value,…."
weight = 90
+++

## The Insight

Time-series timestamps are monotonically increasing and typically arrive at
regular intervals (e.g. every 10 seconds). **Delta encoding** stores the
difference `δᵢ = tᵢ − tᵢ₋₁` instead of the raw value, reducing the dynamic
range from 64-bit nanosecond timestamps to small interval values.

**Delta-of-delta** then stores `δδᵢ = δᵢ − δᵢ₋₁`. For perfectly regular
series, every `δδᵢ = 0`, giving near-infinite compression.

## Example

Consider timestamps at 1-second intervals (in nanoseconds):

```text
Raw:           1700000000000000000  1700000001000000000  1700000002000000000  1700000003000000000
Delta (δ):                         1000000000           1000000000           1000000000
Delta-of-delta (δδ):                                    0                    0
```

The raw values require 64 bits each. The δδ values are all zero — requiring
only **1 bit each** (a '0' flag).

## Variable-Length Bit-Packing

For irregular timestamps where `δδ ≠ 0`, the residuals are packed with a
variable-length encoding that assigns fewer bits to smaller values:

| δδ value range | Encoding | Total bits |
|----------------|----------|------------|
| 0 | `0` | 1 |
| −63 … +64 | `10` + 7-bit value | 9 |
| −255 … +256 | `110` + 9-bit value | 12 |
| −2047 … +2048 | `1110` + 12-bit value | 16 |
| everything else | `1111` + 64-bit value | 68 |

This scheme was introduced in the **Facebook Gorilla paper** (Pelkonen et al.,
2015) and is optimised for the observation that most δδ values are zero or
very small.

## Compression Analysis

For a perfectly regular series with period *T*:

```
Compressed size = header (128 bits: first timestamp + first delta) + n × 1 bit
```

For 1 000 timestamps at 1-second intervals:

- Raw: 1 000 × 64 bits = 8 000 bytes
- Compressed: 128 + 1 000 = 1 128 bits ≈ 141 bytes
- **Ratio: 56.7×**

For slightly irregular series (e.g. ±1 ms jitter):

- Most δδ fall in the ±63 range → 9 bits each
- Compressed: 128 + 1 000 × 9 = 9 128 bits ≈ 1 141 bytes
- **Ratio: 7.0×**

Even with jitter, the compression is substantial compared to raw 8-byte
timestamps.

## Decoding

Decoding reconstructs the original timestamps by reversing the process:

```text
1. Read first timestamp t₀ and first delta δ₀
2. t₁ = t₀ + δ₀
3. For each subsequent value:
   a. Read the variable-length δδ
   b. δᵢ = δᵢ₋₁ + δδᵢ
   c. tᵢ = tᵢ₋₁ + δᵢ
```

Decoding is sequential (each value depends on the previous), but the
computation per value is trivial (one addition) so throughput is limited
by memory bandwidth rather than compute.

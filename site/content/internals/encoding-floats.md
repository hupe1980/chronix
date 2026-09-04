+++
title = "ALP, Gorilla, Chimp & Patas Float Compression"
description = "IEEE 754 double-precision floating-point numbers occupy 64 bits with a complex internal structure."
weight = 100
+++

## The Challenge

IEEE 754 double-precision floating-point numbers occupy 64 bits with a
complex internal structure:

```text
┌─────┬──────────────┬──────────────────────────────────────────────────┐
│sign │  exponent    │               mantissa                          │
│1 bit│  11 bits     │               52 bits                           │
└─────┴──────────────┴──────────────────────────────────────────────────┘
```

General-purpose compressors treat floats as arbitrary byte sequences and
achieve poor compression ratios. Time-series float compressors exploit the
observation that **consecutive values in a sensor stream change slowly**.

## ALP — Adaptive Lossless floating-Point Compression

Gorilla, Chimp and Patas all start from the same premise: a double is an
opaque bit pattern, and consecutive values will share a prefix. **ALP**
(Afroozeh, Kuffo & Boncz, *SIGMOD 2024*) starts from a different one — and
**Pcodec** (Loncaric, 2025, the `pco` crate) takes that premise one step
further, which is why pco is Chronix's primary float codec and ALP the
second candidate. Both are described below; the adaptive selector tries
pco first, ALP second, and the XOR codecs after, per block.

### Pcodec

pco recovers the same latent integer ALP does ("float-multiple" mode),
then *delta-encodes* the integers and entropy-codes the deltas with ANS
against bins learned per chunk. Where ALP frame-of-reference bit-packs the
integers — 13 bits each for a meter reporting 200–250 W to two decimals —
pco spends bits only on what is unpredictable from the previous value. On
the workloads the ratio test pins that is 85× against ALP's 4.1× on a
power meter, 46× against 9.1× on temperatures, 16× against 5.8× on a noisy
sensor — and it decodes faster than ALP. The wire format carries Chronix's
own value count in front of the pco file, checked against the decode
ceiling before a byte is decompressed; pco's headers are never trusted for
an allocation.

### The observation

Most doubles in a time-series database never had 15 significant digits. They
are *decimals*: a meter reported `231.45` W, a thermometer reported `18.3` °C,
a price feed reported `104.27`. IEEE-754 stored those approximately, but the
number that was actually measured is a small integer with a known scale.
`231.45` is not a random 64-bit pattern — it is `23145` divided by 100.

XOR codecs cannot see this. Two consecutive decimals with the same number of
fractional digits have essentially unrelated mantissas, so the XOR has few
leading or trailing zeros and the codec emits close to 64 bits per value.
This is why Gorilla and Chimp measure ~1.1× on real meter data — barely
better than storing the raw bytes.

### The transform

For each block ALP chooses one exponent pair `(e, f)` and stores

```text
encode:  i = round(v · 10^e · 10^-f)
decode:  v = i · 10^f · 10^-e
```

The resulting integers are frame-of-reference coded (subtract the block
minimum) and bit-packed. A meter reporting 200–250 W to two decimals produces
integers spanning 5,000 values, which is 13 bits — against 64 for the raw
double.

`(e, f)` is chosen by scoring every `0 ≤ f ≤ e ≤ 18` combination against a
stratified sample of the block: packed width for the values that round-trip,
plus a fixed cost for the ones that do not, cheapest wins.

### Exactness

The transform is a *guess*, so ALP does not trust it. Every value is decoded
again and compared **bit for bit** against the original. Anything that does
not reproduce exactly becomes an **exception**, stored verbatim next to its
position:

- genuine high-precision doubles (`π`, results of transcendental functions)
- `NaN` and `±inf`
- `-0.0`, which numerically equals `0.0` but has a different bit pattern
- anything that would fall outside `i64` after scaling

Encoder and decoder call the same `decode_value` function, so they cannot
disagree about what a stored integer means. The codec is bitwise lossless,
like every other encoder in the crate.

### Measured

| Workload | ALP | Chimp | Chimp128 | Gorilla | Patas |
|---|---|---|---|---|---|
| Power meter, W, 2 dp   | **4.1×** | 1.1× | 0.9× | 1.2× | 1.1× |
| Temperature, °C, 1 dp  | **9.1×** | 1.1× | 5.9× | 1.2× | 1.0× |
| Energy, kWh, 3 dp      | **4.6×** | 1.5× | 1.5× | 1.5× | 1.3× |
| Scientific doubles     | 0.9× | 1.0× | 0.9× | 1.0× | 0.9× |

Reproduce with `cargo test -p chronix-encoding --test float_codec_ratios --
--nocapture`.
**Throughput** (`cargo bench -p chronix-encoding`, 1M values, Apple M-series):

| | ALP | Chimp |
|---|---|---|
| encode | **2.86 ms** (350M values/s) | 4.40 ms |
| decode | **5.47 ms** (183M values/s) | 18.16 ms |

The per-block exponent search does not cost anything net: ALP is 1.5× faster
to encode than Chimp and 3.3× faster to decode, because bit-packed integers
are far cheaper to unpack than a serial XOR window.

### Why the XOR codecs stay

The last row. On genuinely non-decimal doubles — scientific measurements,
hashes, anything that used its full mantissa — ALP has an exception for every
value and loses the adaptive selector's trial encoding, which then picks an
XOR codec. The ALP paper's second scheme, `ALP_RD`, exists for exactly that
case (it splits the bit pattern into a dictionary-coded left part and a
packed right part). Chronix does not implement it: Gorilla, Chimp, Chimp128
and Patas already cover that shape, and the selector already routes to them.

## Gorilla XOR Encoding

**Gorilla encoding** (Pelkonen et al., *VLDB 2015*) compresses floats by
XOR-ing successive values:

```
xᵢ = vᵢ ⊕ vᵢ₋₁
```

When consecutive values are identical, `xᵢ = 0` and only a single `0`-bit
is emitted. When they differ, the XOR result has a contiguous block of
*meaningful bits* (non-zero bits) surrounded by leading and trailing zeros:

```text
XOR:  00000000 00000000 0000[1011 0110 0]000 00000000 00000000
                             ▲                ▲
                         leading=20        trailing=24
                         meaningful=9
```

### Encoding Cases

```text
Case 0 (xᵢ == 0):
    Emit '0'                                         → 1 bit

Case 1 (meaningful bits fit within previous window):
    Emit '10' + meaningful bits                      → 2 + len bits

Case 2 (new window needed):
    Emit '11' + leading(5 bits) + length(6 bits) + meaningful bits
                                                     → 13 + len bits
```

Case 1 reuses the leading/trailing counts from the previous XOR, so if
consecutive XOR values have similar bit patterns, only the changed bits
are stored.

### Compression Performance

| Data Pattern | Bits per Value | Compression Ratio |
|--------------|---------------|-------------------|
| Constant | 1 | 64× |
| Slowly varying (±0.01%) | 10–20 | 3–6× |
| Moderately varying (±1%) | 25–40 | 1.6–2.5× |
| Random | 65–68 | 0.94–0.98× (slight expansion) |

## Chimp Encoding

**Chimp** (Liakos et al., *VLDB 2022*) improves on Gorilla with a key
observation: for IEEE 754 doubles, the **least-significant mantissa bits**
are most likely to change. This means:

1. The *trailing zeros* count tends to remain stable across successive XOR
   values (the high-order bits change rarely).
2. The *leading zeros* count is less predictable (it depends on which
   exponent/mantissa bits flip).

### Chimp's Optimisation

Instead of storing the trailing zeros count explicitly (as Gorilla does in
Case 2), Chimp uses the **previous XOR's trailing-zero count** as a
predictor. If the current trailing-zero count matches the prediction, no
metadata is needed. If it differs, only the delta is encoded.

This saves **1–3 bits per value** on typical slowly-varying workloads,
because the trailing-zero count is highly autocorrelated in time-series data.

### Chimp Encoding Cases

```text
Case 0 (xᵢ == 0):
    Emit '0'                                         → 1 bit

Case 1 (trailing zeros ≥ predicted):
    Emit '10' + leading(3 bits) + meaningful bits    → 5 + len bits

Case 2 (trailing zeros < predicted):
    Emit '11' + leading(3 bits) + trailing(6 bits) + meaningful bits
                                                     → 12 + len bits
```

Note Chimp uses only 3 bits for leading zeros (vs Gorilla's 5), quantising
to 8 buckets. This works because leading zeros are concentrated in a narrow
range for real sensor data.

### Gorilla vs Chimp

| Metric | Gorilla | Chimp |
|--------|---------|-------|
| Bits per value (slow) | 10–20 | 8–17 |
| Bits per value (random) | 65–68 | 65–68 |
| Case 2 overhead | 13 bits | 12 bits |
| Trailing-zero prediction | None | Previous XOR |
| Compression (typical) | 1.3× | 1.4× |

Chimp matches or slightly exceeds Gorilla on all workloads and degrades
identically on random data (where no technique can help).

## Implementation Notes

Both Gorilla and Chimp encoders use a `BitWriter` that accumulates bits
into 64-bit words and flushes them to a byte buffer. The `BitReader` reads
back bits from the byte stream. These are SIMD-friendly — the
word-at-a-time access pattern enables auto-vectorisation of the decoding
loop.

Patas avoids bit-packing entirely and operates on whole bytes, which makes
it particularly friendly to modern CPUs with fast byte-lane operations.

## Patas Byte-Aligned XOR Encoding

**Patas** (Afroozeh et al., *VLDB 2023*) takes a different approach to
XOR-based float compression: instead of bit-packing the meaningful bits,
it stores them as **whole bytes**. This trades a small amount of compression
ratio for significantly simpler and faster decoding.

### Key Insight

After XOR-ing consecutive values, the non-zero portion of the result can be
described by two byte-granularity counts:

- **Trailing-zero bytes** (`tz_bytes`): number of trailing all-zero bytes
  (0–7)
- **Significant bytes** (`sig_bytes`): number of bytes to store (0 = values
  are identical, 1–8)

### Encoding Format

```text
┌──────────────┬─────────────────┬─────────────────────────────────────┐
│ count (u32)  │ first value (8B)│ [flag byte + significant bytes]...  │
└──────────────┴─────────────────┴─────────────────────────────────────┘
```

Each subsequent value is encoded as:

```text
Flag byte:  high nibble = tz_bytes (0–7)
            low nibble  = sig_bytes (0 = identical, 1–8)

If sig_bytes > 0:  sig_bytes raw bytes of the XOR result (after
                   removing trailing zeros)
```

### Encoding Cases

```text
Case 0 (xᵢ == 0):
    Emit flag byte 0x00                              → 1 byte

Case N (sig_bytes = N):
    Emit flag byte + N significant bytes             → 1 + N bytes
```

### Comparison

| Metric | Gorilla | Chimp | Patas |
|--------|---------|-------|-------|
| Granularity | Bit | Bit | Byte |
| Bits per value (slow) | 10–20 | 8–17 | 16–32 |
| Decode speed | Good | Good | **Excellent** |
| Implementation complexity | Moderate | Moderate | **Simple** |
| Best for | Compression ratio | Compression ratio | Decode throughput |

Patas excels when decode speed is more important than achieving the absolute
best compression ratio — common in real-time query engines that decompress
on every read.

## References

- T. Pelkonen, S. Franklin, J. Teller, et al. "Gorilla: A Fast,
  Scalable, In-Memory Time Series Database." *Proc. VLDB*, 2015.
- P. Liakos, K. Papakonstantinopoulou, Y. Kotidis. "Chimp: Efficient
  Lossless Floating Point Compression for Time Series Databases."
  *Proc. VLDB*, 2022.
- A. Afroozeh, P. Boncz, et al. "Patas: Fast, Byte-Aligned Compression
  for Floating-Point Data." *Proc. VLDB*, 2023.
- A. Afroozeh, L. Kuffo, P. Boncz. "ALP: Adaptive Lossless floating-Point
  Compression." *Proc. ACM Manag. Data* 2(1), SIGMOD 2024.

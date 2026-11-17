+++
title = "Float encoding"
description = "Why pcodec and ALP beat XOR codecs on metric data: recovering the decimal behind a double, with measured ratios on real meter data."
weight = 70
aliases = ["/internals/encoding-floats/"]
+++

Most metric values are decimals stored as doubles — `231.45`, not a random
64-bit pattern. Chronix's leading float codecs recover the decimal integer
behind each double and compress that, which is why they beat the XOR codecs by
an order of magnitude on real data. The adaptive selector tries **pco** first,
**ALP** second and the XOR codecs (Chimp128, Patas, Chimp, Gorilla) after, per
block, and keeps the smallest result.

## The observation

A meter reports `231.45` W: the number measured is the integer `23145` with a
scale of 10⁻². IEEE 754 stores an approximation of that as a binary fraction,
and consecutive decimals with the same number of digits have nearly unrelated
mantissas. An XOR of successive values therefore has few leading or trailing
zeros, and Gorilla and Chimp spend close to 64 bits per value — about 1.1× on
real meter data.

## ALP

ALP (Afroozeh, Kuffo & Boncz, SIGMOD 2024) picks one exponent pair *(e, f)* per
block and stores integers:

```text
encode:  i = round(v · 10^e · 10^-f)
decode:  v = i · 10^f · 10^-e
```

The integers are frame-of-reference coded and bit-packed: a meter reporting
200–250 W to two decimals needs 13 bits a value instead of 64. *(e, f)* is
chosen by scoring every 0 ≤ f ≤ e ≤ 18 on a sample of the block.

**Exact, always.** Every value is decoded again and compared bit for bit. One
that does not reproduce — a high-precision double, NaN, ±∞, `-0.0`, anything
outside `i64` after scaling — is stored verbatim as an exception. Encoder and
decoder share one `decode_value`, so they cannot disagree.

## Pcodec

pco (the `pco` crate) recovers the same integers, then **delta-encodes** them
and entropy-codes the deltas with bins learned per chunk. Where ALP spends
13 bits on every value of that meter, pco spends bits only on what the previous
value does not predict — and decodes faster than ALP. Chronix writes its own
value count before the pco data and checks it against the decode limit before
decompressing, so a corrupt header cannot drive an allocation.

## Measured

| Workload | pco | ALP | Chimp | Chimp128 | Gorilla | Patas |
|---|---|---|---|---|---|---|
| Power meter, W, 2 dp | **85×** | 4.1× | 1.1× | 0.9× | 1.2× | 1.1× |
| Temperature, °C, 1 dp | **46×** | 9.1× | 1.1× | 5.9× | 1.2× | 1.0× |
| Noisy sensor | **16×** | 5.8× | | | | |
| Energy, kWh, 3 dp | | 4.6× | 1.5× | 1.5× | 1.5× | 1.3× |
| Scientific doubles | | 0.9× | 1.0× | 0.9× | 1.0× | 0.9× |

Clean synthetic series flatter every codec; on realistic meter and inverter
data with noise, plateaus and dropouts pco reaches 5–29×. Reproduce with
`cargo test -p chronix-encoding --test float_codec_ratios -- --nocapture` and
`--test codec_field_check`.

On 1 M values (Apple M-series, `cargo bench -p chronix-encoding`) ALP encodes
in 2.9 ms and decodes in 5.5 ms, against Chimp's 4.4 ms and 18.2 ms: bit-packed
integers unpack faster than a serial XOR window.

## The XOR codecs

They stay for the last row: doubles that use their whole mantissa — scientific
values, results of transcendental functions — where ALP and pco turn every value
into an exception and lose the trial.

| Codec | Idea | Strength |
|---|---|---|
| Gorilla (VLDB 2015) | XOR with the previous value; store the meaningful bits | simple; slowly changing values |
| Chimp (VLDB 2022) | Gorilla with cheaper cases for trailing zeros | better ratio than Gorilla |
| Chimp128 | XOR with the best of the last 128 values | repeating levels |
| Patas | byte-aligned XOR: a flag byte plus significant bytes | fastest decode |

ALP's second scheme for non-decimal data (ALP_RD) is not implemented; these
four cover that shape and the selector already routes to them.

## Related

- [Encoding](@/explanation/encoding.md) — selection and block compression
- [Integer, timestamp and dictionary encoding](@/explanation/encoding-integers-and-time.md)

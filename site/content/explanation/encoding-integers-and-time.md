+++
title = "Integer, timestamp and dictionary encoding"
description = "How Chronix encodes integers, timestamps and strings: delta-of-delta, frame-of-reference bit packing, pcodec timestamps and dictionaries."
weight = 80
aliases = ["/internals/encoding-integers/", "/internals/encoding-timestamps/", "/internals/encoding-dictionary/"]
+++

Integers, timestamps and strings each have their own codecs. Timestamps go to
pco or delta-of-delta; integers to run-length, frame-of-reference, pco or
delta with zigzag; tags to a dictionary; booleans to a bitmap. As with floats,
every block tries its candidates on a sample and keeps the smallest
([Encoding](@/explanation/encoding.md)).

## Timestamps

A sampler ticking every second produces deltas of 1 000 000 000 ns, and the
deltas of those deltas are zero.

| Codec | Stores | Wins on |
|---|---|---|
| Delta-of-delta | the first timestamp and delta, then each change of the delta in a variable-length code — one bit when it is zero | perfectly regular clocks |
| pco | the deltas, entropy-coded against bins learned per chunk | real clocks with jitter |

Measured (`chronix-encoding/tests/timestamp_codec_check.rs`):

| Series | Result |
|---|---|
| 1 s sampler with up to 10 ms of random jitter | pco **2.7×**, within 5 % of the 2.75× no lossless codec can beat; delta-of-delta 0.9×, larger than plain |
| Regular with gaps | **345×** (best codec) |
| Perfectly regular | **1 092×** (best codec) |

Jitter is what defeats delta-of-delta: every non-zero change costs a varint,
while pco charges only the entropy of the jitter itself. Both decode sequentially; the
work per value is an addition.

## Integers

| Codec | Stores | Wins on |
|---|---|---|
| Run-length | value and run length | constant stretches: states, flags, setpoints |
| Frame-of-reference | offsets from the block minimum, bit-packed at ⌈log₂(max−min+1)⌉ bits | values in a narrow range — HTTP status codes take 9 bits |
| Delta + zigzag + bit-packing | differences, mapped to unsigned (0, −1, 1, −2 → 0, 1, 2, 3) | counters and slow trends |
| pco | entropy-coded deltas | anything else |

Unsigned integers have their own frame-of-reference path, so values near the
middle of the `u64` range do not inflate to 64 bits through a signed
reinterpretation.

`Decimal` columns use these codecs on their `i128` mantissas, which is why an
exact type compresses better than the float it replaces: 475× on noisy power,
1 040× on a monotone register.

## Strings

Tag columns hold few distinct values — regions, hosts, device ids — repeated on
millions of rows. A **dictionary** stores each distinct string once and every
row as an index of ⌈log₂ *k*⌉ bits for *k* distinct values: two values cost one
bit a row, 256 cost eight. The dictionary lines up with Arrow dictionary
arrays, so the query engine reads it without rebuilding. Tag columns also carry a
bloom filter per row group for pruning
([Query execution](@/explanation/query.md)).

## Booleans

Eight values per byte, always 8× against one byte per value. Long runs of the
same value are left to the block compressor.

## Related

- [Encoding](@/explanation/encoding.md)
- [Float encoding](@/explanation/encoding-floats.md)

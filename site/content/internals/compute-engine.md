+++
title = "Compute Engine & SIMD"
description = "Time-series analytics involves repetitive numerical operations over large arrays — summing millions of floats, comparing timestamps, filtering by tag values. Modern CPUs provide SIMD (Single…."
weight = 360
+++

## Motivation

Time-series analytics involves repetitive numerical operations over large
arrays — summing millions of floats, comparing timestamps, filtering by
tag values. Modern CPUs provide **SIMD** (Single Instruction, Multiple Data)
instructions that process 4–16 values simultaneously.

## SIMD Instruction Sets

| ISA | Register Width | f64 per Op | Availability |
|-----|---------------|------------|--------------|
| SSE2 | 128-bit | 2 | All x86-64 |
| AVX2 | 256-bit | 4 | Intel Haswell+ (2013) |
| AVX-512 | 512-bit | 8 | Intel Skylake-X+ (2017) |
| NEON | 128-bit | 2 | All ARM64 |

Chronix auto-detects the available ISA at startup and dispatches to the
widest supported implementation.

## Vectorized Operations

### Aggregation

Scalar sum of *n* floats requires *n* additions (latency-bound by
data dependencies). SIMD sum uses *k* accumulators in parallel:

```text
Scalar:   a₁ + a₂ + a₃ + a₄ + a₅ + a₆ + a₇ + a₈  = 8 ops

AVX2:     [a₁ a₂ a₃ a₄] + [a₅ a₆ a₇ a₈]  = 1 vector op
          horizontal sum of result          = 3 ops
          Total: 4 ops (2× fewer)
```

For large arrays, the speedup approaches **k×** where *k* is the SIMD
width in elements.

### Filtering

Timestamp range checks can be vectorized:

```text
Scalar:  for each ts: if ts >= start && ts <= end → emit

AVX2:    compare 4 timestamps simultaneously
         pack matching indices
         gather matching values
```

### Arrow Integration

Apache Arrow's columnar layout is **SIMD-friendly** by design — values
are stored in contiguous, aligned arrays. The compute kernels in
`chronix_analytics::compute` operate directly on Arrow arrays without copying.

## Kernel Catalog

`chronix_analytics::compute::simd`, all over `&[f64]` or `&[i64]` with a
scalar fallback:

| Kernel | Operation |
|--------|-----------|
| `simd_sum` | Sum |
| `simd_mean` | Arithmetic mean |
| `simd_variance` | Variance about a supplied mean (sample, `n − 1`) |
| `simd_population_variance` | Variance about a supplied mean (`n`) |
| `simd_mean_variance` | Both, in one pass |
| `simd_std_dev` | Standard deviation |
| `simd_min_max` | Minimum and maximum, in one pass |
| `simd_range_filter_i64` | Indices of timestamps inside `[min, max]` |
| `simd_dot_product` | Dot product — the correlation and regression helper |
| `simd_tier` | Which instruction set was selected at runtime |

`CpuEngine::batch_z_score` sits above them: one `simd_mean_variance` pass over
the whole input, then the per-element score.

The column codecs — Gorilla's XOR, delta-of-delta, PFOR — live in
`chronix-encoding`, not here.

## Why there is no GPU backend

A wgpu/WGSL compute backend was built and then **deleted**, and the reasoning
is worth keeping because it is the kind of feature that gets re-proposed.

Exponential smoothing — the workload it was meant to accelerate — is inherently
sequential: each smoothed value depends on the previous one, so the shader ran
at `workgroup_size(1)` and used one lane of the device. The operations that
*do* parallelise cleanly were not the bottleneck in any measured profile. What
remained was a large dependency tree, a driver-compatibility surface, and a
second code path to audit, in exchange for a benchmark line.

The SIMD tiers below stay, and NEON is the tier that matters most in practice:
the primary deployment target is a 64-bit ARM gateway.

## Benchmarking

Chronix includes micro-benchmarks for all compute kernels:

```bash
cargo bench --package chronix-analytics
```

Results include throughput in GB/s and elements/second, enabling
comparison across hardware platforms.

## Fallback Strategy

```text
Runtime dispatch:
  ├── AVX-512 available?  → use 512-bit kernels
  ├── AVX2 available?     → use 256-bit kernels
  ├── NEON available?     → use 128-bit kernels (ARM)
  └── fallback            → scalar implementation
```

All SIMD kernels have a scalar fallback, ensuring correctness on any
platform. The scalar path uses auto-vectorization hints (`#[inline]`,
loop structure) to enable compiler-generated SIMD where possible.

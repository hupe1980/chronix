//! # Chronix Compute Engine
//!
//! CPU compute engine for Chronix analytics operations — dot products,
//! matrix solving, exponential smoothing, differencing, autocorrelation, and
//! batch statistics. Built on multi-tier SIMD acceleration and rayon
//! parallelism.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────┐
//! │        ComputeEngine trait       │
//! ├──────────────────────────────────┤
//! │  batch_dot_product()             │
//! │  batch_matrix_solve()            │
//! │  batch_exponential_smooth()      │
//! │  batch_difference()              │
//! │  batch_autocorrelation()         │
//! └──────────────┬───────────────────┘
//!                │
//!          ┌─────┴────┐
//!          │ CpuEngine │
//!          │ rayon+SIMD│
//!          └───────────┘
//! ```
//!
//! ## SIMD Operations
//!
//! Multi-tier SIMD for common statistical operations:
//! - `simd_sum`, `simd_mean`, `simd_variance`, `simd_min_max`, `simd_dot_product`, `simd_std_dev`
//! - x86_64: runtime-detected AVX-512F (8×f64) → AVX2+FMA (4×f64) → SSE2 (2×f64, baseline)
//! - aarch64: NEON (2×f64, baseline)
//! - Scalar 4-wide ILP fallback on other architectures
//! - `simd_tier()` returns the active tier for diagnostics
//!
//! ## Buffer Pool
//!
//! `BufferPool<T>` provides reusable `Vec<T>` to minimize allocation overhead
//! in hot compute paths.

#![warn(missing_docs)]

mod buffer_pool;
mod engine;
mod error;
// The SIMD module is the one audited exception to the crate-wide
// unsafe_code deny: every unsafe block is a runtime-feature-gated
// std::arch intrinsic call with a // SAFETY: justification.
#[allow(unsafe_code)]
// chunks_exact(N) with SIMD lane constants reads clearer than as_chunks::<N>() here.
#[allow(clippy::chunks_exact_to_as_chunks)]
mod simd;

pub use buffer_pool::BufferPool;
pub use engine::{ComputeConfig, ComputeEngine, CpuEngine};
pub use error::ComputeError;
pub use simd::{
    simd_dot_product, simd_mean, simd_mean_variance, simd_min_max, simd_population_variance,
    simd_range_filter_i64, simd_std_dev, simd_sum, simd_tier, simd_variance,
};

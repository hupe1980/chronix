//! Native histograms — a whole distribution as one sample.
//!
//! A classic Prometheus histogram is a *family* of series: `foo_bucket` once
//! per boundary, plus `foo_sum` and `foo_count`, with the boundaries fixed by
//! whoever instrumented the code. Getting a useful quantile means guessing the
//! right boundaries in advance, and getting them wrong means re-instrumenting
//! and losing the history. A **native histogram** is one sample carrying the
//! whole distribution, with buckets that are implied by a *schema* rather than
//! enumerated — so the resolution is a number, the buckets are sparse, and
//! nothing has to be guessed up front.
//!
//! Prometheus made them stable in **v3.8**. They arrive over remote write with
//! `send_native_histograms`, and OTLP carries the same idea as an exponential
//! histogram. A scrape configured for them reaches a database that cannot
//! store them as data that is simply lost, which is why this type exists.
//!
//! # The model
//!
//! This follows the Prometheus specification exactly, because the whole point
//! is to be the thing a Prometheus writes to:
//!
//! - **`schema`** fixes the resolution. For `-4 ..= 8`, bucket boundaries are
//!   powers of `2^(2^-schema)`: each bucket's upper bound is the previous
//!   one's times that factor, so schema *n* has twice the resolution of *n*−1.
//!   [`CUSTOM_BUCKETS_SCHEMA`] (`-53`) means the boundaries are listed
//!   explicitly in [`custom_values`](Histogram::custom_values) — which is what
//!   a *classic* histogram converts into, so it is the bridge rather than an
//!   extra.
//! - **The zero bucket** counts observations in the closed interval
//!   `[-zero_threshold, +zero_threshold]`. It exists because an exponential
//!   scheme has no bucket for zero and infinitely many just above it.
//! - **Positive and negative buckets** are sparse: only the indices that were
//!   observed are stored.
//! - **`count`** and **`sum`** are the totals, and `sum` may be any float
//!   including `NaN` — a `NaN` observation poisons it, which is a fact
//!   [`quantile`](Histogram::quantile) has to know about.
//!
//! # Two decisions worth knowing about
//!
//! **Buckets hold absolute counts, not deltas, and are `(index, count)` pairs
//! rather than spans.** The wire format carries spans plus deltas because that
//! is a compression trick; `chronix-encoding` does better than that with pco,
//! and re-deriving the absolutes on every quantile would be work done for
//! nothing. Spans also have a normalisation question in them — a span may name
//! a bucket whose count is zero — and two representations of one value is how
//! the canonical series form went wrong once already. There is exactly one
//! in-memory form: **sorted by index, no zero counts**, which
//! [`canonicalise`](Histogram::canonicalise) establishes and
//! [`validate`](Histogram::validate) requires.
//!
//! **Counts are `f64`.** Prometheus distinguishes integer histograms (a
//! scrape) from float histograms (a recording rule's output, which arrives
//! over remote write just the same), and supporting only the first would
//! refuse ordinary traffic. One representation covers both, and the exactness
//! that matters is *checked* rather than assumed: [`validate`](Histogram::validate)
//! refuses a count above [`MAX_EXACT_COUNT`] — 2⁵³, where `f64` stops
//! representing consecutive integers — so an integer histogram is exact by
//! construction or refused, never silently rounded.

use serde::{Deserialize, Serialize};

/// The schema value meaning "the boundaries are listed, not computed".
///
/// Prometheus calls these NHCB — native histograms with custom buckets — and
/// uses them to carry a classic `le`-bucketed histogram in the native shape.
pub const CUSTOM_BUCKETS_SCHEMA: i8 = -53;

/// Coarsest standard exponential schema: each bucket is 2¹⁶ times the last.
pub const MIN_SCHEMA: i8 = -4;

/// Finest standard exponential schema: 256 buckets per power of two.
pub const MAX_SCHEMA: i8 = 8;

/// The largest count `f64` represents exactly.
///
/// Above 2⁵³ consecutive integers are no longer distinguishable, so a count
/// beyond it could not be stored and read back unchanged. Refused rather than
/// rounded.
pub const MAX_EXACT_COUNT: f64 = 9_007_199_254_740_992.0; // 2^53

/// What is known about counter resets between this sample and the previous.
///
/// A histogram is usually a counter: buckets only grow. A reset — a process
/// restart — has to be visible to `rate()`, and the scraper often knows
/// without the database having to guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ResetHint {
    /// Nothing is known; a reader must detect resets by comparing samples.
    #[default]
    Unknown,
    /// This is the first histogram after a counter reset.
    Reset,
    /// There was no reset between this sample and the previous one.
    NoReset,
    /// A gauge histogram: counter resets do not apply.
    Gauge,
}

/// One populated bucket.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    /// The bucket's index in the schema's implied sequence.
    ///
    /// May be negative: index 0 is the bucket ending at 1.
    pub index: i32,
    /// Observations that fell in it. Never zero in a canonical histogram, and
    /// never negative.
    pub count: f64,
}

/// Everything that can be wrong with a histogram.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum HistogramError {
    /// The schema is outside `-4 ..= 8` and is not [`CUSTOM_BUCKETS_SCHEMA`].
    #[error(
        "histogram schema {0} is not a valid resolution: expected {MIN_SCHEMA}..={MAX_SCHEMA} \
         for exponential buckets, or {CUSTOM_BUCKETS_SCHEMA} for custom boundaries"
    )]
    InvalidSchema(i8),

    /// A count was negative, or not finite.
    #[error("histogram {what} is {value}, which is not a count")]
    InvalidCount {
        /// Which count — `count`, `zero_count`, or a bucket's.
        what: &'static str,
        /// The offending value.
        value: f64,
    },

    /// A count exceeded what `f64` represents exactly.
    #[error(
        "histogram {what} is {value}, above 2^53 — beyond that an f64 cannot \
         distinguish consecutive integers, so the value could not be read back unchanged"
    )]
    CountNotExact {
        /// Which count overflowed the exact range.
        what: &'static str,
        /// The offending value.
        value: f64,
    },

    /// Buckets were not sorted, or an index appeared twice.
    #[error("histogram {side} buckets are not strictly ascending by index (at {index})")]
    UnorderedBuckets {
        /// `"positive"` or `"negative"`.
        side: &'static str,
        /// The index at which the order broke.
        index: i32,
    },

    /// `zero_threshold` was negative or not finite.
    #[error("histogram zero_threshold is {0}, which cannot bound an interval around zero")]
    InvalidZeroThreshold(f64),

    /// Custom boundaries were supplied for an exponential schema, or omitted
    /// for [`CUSTOM_BUCKETS_SCHEMA`].
    #[error("{0}")]
    CustomValues(&'static str),

    /// The bucket counts add up to more than `count`.
    #[error(
        "histogram buckets total {observed} but count is {declared}: a histogram \
         whose parts exceed its whole cannot be interpolated"
    )]
    CountMismatch {
        /// What the buckets and zero bucket add up to.
        observed: f64,
        /// What `count` claims.
        declared: f64,
    },
}

/// `2^k`, built from the IEEE-754 exponent field rather than computed.
///
/// Exact for every `k` a power of two can represent, and — unlike `exp2`,
/// `powi` or `powf` — specified to the last bit, so two machines agree.
/// Saturates to `0.0` and `f64::INFINITY` outside the representable range.
fn exp2i(k: i32) -> f64 {
    const MAX_NORMAL_EXP: i32 = 1023;
    const MIN_NORMAL_EXP: i32 = -1022;
    /// Below this, `2^k` is not representable even as a subnormal.
    const MIN_SUBNORMAL_EXP: i32 = -1074;

    if k > MAX_NORMAL_EXP {
        return f64::INFINITY;
    }
    if k < MIN_SUBNORMAL_EXP {
        return 0.0;
    }
    if k >= MIN_NORMAL_EXP {
        // Biased exponent, zero mantissa.
        #[allow(clippy::cast_sign_loss)] // k + 1023 is in 1..=2046 here
        return f64::from_bits(((k + 1023) as u64) << 52);
    }
    // Subnormal: the single set mantissa bit carries the exponent.
    #[allow(clippy::cast_sign_loss)] // k - MIN_SUBNORMAL_EXP is in 0..=51 here
    f64::from_bits(1u64 << ((k - MIN_SUBNORMAL_EXP) as u32))
}

/// `2^(1/2^n)` — `n` successive square roots of two.
///
/// `sqrt` is the one root operation IEEE 754 requires to be correctly
/// rounded, so this is reproducible everywhere. `powf` is not, which is the
/// whole reason this function exists.
fn root_of_two(n: u32) -> f64 {
    let mut v = 2.0_f64;
    for _ in 0..n {
        v = v.sqrt();
    }
    v
}

/// `2^(r / 2^s)` for `0 <= r < 2^s`, without a transcendental.
///
/// `r / 2^s` is a sum of distinct negative powers of two — one per set bit of
/// `r` — so the result is the product of the corresponding roots of two.
/// Multiplied low bit first, so the order (and therefore the rounding) is
/// fixed rather than incidental.
fn frac_power_of_two(r: u32, s: u32) -> f64 {
    let mut acc = 1.0_f64;
    for bit in 0..s {
        if r & (1 << bit) != 0 {
            // Bit `bit` of `r` contributes 2^bit / 2^s = 1 / 2^(s - bit).
            acc *= root_of_two(s - bit);
        }
    }
    acc
}

/// A distribution captured as one sample.
///
/// See the [module documentation](self) for the model and the two
/// representation decisions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Histogram {
    /// Bucket resolution — see [`CUSTOM_BUCKETS_SCHEMA`], [`MIN_SCHEMA`], [`MAX_SCHEMA`].
    pub schema: i8,
    /// Half-width of the zero bucket, which covers `[-t, +t]` inclusive.
    pub zero_threshold: f64,
    /// Observations in the zero bucket.
    pub zero_count: f64,
    /// Total observations, including the zero bucket.
    pub count: f64,
    /// Sum of all observations. May be `NaN` if any observation was.
    pub sum: f64,
    /// Buckets for positive observations, ascending by index, none zero.
    pub positive: Vec<Bucket>,
    /// Buckets for negative observations, ascending by index, none zero.
    ///
    /// Index *i* covers `[-base^i, -base^(i-1))` — the mirror of the positive
    /// side, so larger indices are *more* negative.
    pub negative: Vec<Bucket>,
    /// Upper-inclusive boundaries, when `schema == CUSTOM_BUCKETS_SCHEMA`.
    ///
    /// Ascending. Bucket *i* of `positive` covers
    /// `(custom_values[i-1], custom_values[i]]`, with `-Inf` below the first
    /// and `+Inf` above the last.
    pub custom_values: Vec<f64>,
    /// What is known about counter resets before this sample.
    pub reset_hint: ResetHint,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::empty(0)
    }
}

impl Histogram {
    /// An empty histogram at the given schema.
    #[must_use]
    pub fn empty(schema: i8) -> Self {
        Self {
            schema,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive: Vec::new(),
            negative: Vec::new(),
            custom_values: Vec::new(),
            reset_hint: ResetHint::Unknown,
        }
    }

    /// Whether the boundaries are listed rather than computed.
    #[must_use]
    pub const fn has_custom_buckets(&self) -> bool {
        self.schema == CUSTOM_BUCKETS_SCHEMA
    }

    /// The growth factor between consecutive exponential bucket bounds.
    ///
    /// `2^(2^-schema)`. Meaningless for [`CUSTOM_BUCKETS_SCHEMA`].
    #[must_use]
    pub fn base(schema: i8) -> f64 {
        if schema <= 0 {
            // 2^(2^|schema|) — an exact power of two.
            exp2i(1i32 << (-i32::from(schema)).min(30))
        } else {
            // 2^(1/2^schema) — `schema` correctly-rounded square roots of 2.
            root_of_two(u32::from(schema.unsigned_abs()))
        }
    }

    /// The index of the bucket an observation falls in.
    ///
    /// From the specification's boundary rule — bucket *i* covers
    /// `(base^(i-1), base^i]` — inverted: `i = ⌈log₂(v) · 2^schema⌉`.
    ///
    /// `value` must be finite and strictly positive; callers route zero and
    /// near-zero observations to the zero bucket first.
    #[must_use]
    pub fn index_for(schema: i8, value: f64) -> i32 {
        debug_assert!(value > 0.0 && value.is_finite());
        let scaled = value.log2() * (2.0_f64).powi(i32::from(schema));
        // `ceil` is the rule, but a value that *is* a bucket bound must land
        // in the bucket it closes, and `log2` does not return exact halves:
        // `log2(√2)` comes back a hair above 0.5, so schema 1 scales it to
        // 1.0000000000000002 and `ceil` gives 2 — one bucket too high, for
        // every observation that is exactly a bound. Since latencies in
        // seconds are mostly exact powers of two, that is most of them.
        //
        // So: snap to the nearest integer when we are within rounding error
        // of one, and take the ceiling otherwise. The tolerance is relative,
        // because `scaled` grows with the index.
        let nearest = scaled.round();
        let tolerance = 1e-12 * scaled.abs().max(1.0);
        let i = if (scaled - nearest).abs() <= tolerance {
            nearest
        } else {
            scaled.ceil()
        };
        i as i32
    }

    /// The `(lower, upper)` bounds of a bucket, lower exclusive, upper inclusive.
    ///
    /// For the negative side, pass `negative = true`: index *i* then covers
    /// `[-base^i, -base^(i-1))`.
    #[must_use]
    pub fn bucket_bounds(&self, index: i32, negative: bool) -> (f64, f64) {
        if self.has_custom_buckets() {
            let n = self.custom_values.len();
            let i = index as usize;
            let lower = if i == 0 {
                f64::NEG_INFINITY
            } else {
                self.custom_values
                    .get(i - 1)
                    .copied()
                    .unwrap_or(f64::NEG_INFINITY)
            };
            let upper = if i >= n {
                f64::INFINITY
            } else {
                self.custom_values.get(i).copied().unwrap_or(f64::INFINITY)
            };
            return (lower, upper);
        }
        let upper = Self::bound(self.schema, index);
        let lower = Self::bound(self.schema, index - 1);
        if negative {
            (-upper, -lower)
        } else {
            (lower, upper)
        }
    }

    /// The upper bound of bucket `index`: `2^(index / 2^schema)`.
    ///
    /// Computed **without a transcendental**, which is the point. `powf` and
    /// `powi` are not correctly rounded and their last bit is not specified by
    /// IEEE 754 or by Rust, so the same stored histogram could report
    /// different boundaries on two machines — and a bucket boundary is a
    /// storage-format semantic, not a display detail. It showed up as
    /// `bucket_bounds(0)` returning `0.4999999999999999` instead of `0.5` at
    /// schema 0, where the answer is an exact power of two.
    ///
    /// Instead, `index` is split as `index = q · 2^schema + r`, giving
    /// `2^q · 2^(r / 2^schema)`. The first factor is built straight from the
    /// IEEE exponent field, and the second from repeated `sqrt` — the one
    /// root operation IEEE 754 *does* require to be correctly rounded. Every
    /// step is exact or correctly rounded, so the result is identical on
    /// every platform.
    #[must_use]
    pub fn bound(schema: i8, index: i32) -> f64 {
        let s = i32::from(schema);
        if s <= 0 {
            // The bound is 2^(index · 2^|s|) — an exact power of two, and the
            // shift cannot overflow for the defined schema range.
            let shift = (-s).min(30);
            return match index.checked_shl(shift as u32) {
                Some(k) => exp2i(k),
                None if index > 0 => f64::INFINITY,
                None => 0.0,
            };
        }
        // Euclidean division, so a negative index still yields 0 <= r < 2^s.
        let m = 1i32 << s;
        let q = index.div_euclid(m);
        let r = index.rem_euclid(m);
        let frac = frac_power_of_two(r as u32, s as u32);
        let scale = exp2i(q);
        if scale == 0.0 || !scale.is_finite() {
            return scale * frac.signum().abs();
        }
        scale * frac
    }

    /// Record one observation.
    ///
    /// Present so a histogram can be built in a test or by a connector that
    /// receives raw values; ingest from Prometheus or OTLP builds the buckets
    /// directly.
    pub fn observe(&mut self, value: f64) {
        self.count += 1.0;
        self.sum += value;
        if value.abs() <= self.zero_threshold {
            self.zero_count += 1.0;
            return;
        }
        let (side, magnitude) = if value < 0.0 {
            (&mut self.negative, -value)
        } else {
            (&mut self.positive, value)
        };
        let index = if self.schema == CUSTOM_BUCKETS_SCHEMA {
            // Custom boundaries are upper-inclusive and ascending.
            self.custom_values
                .iter()
                .position(|&b| value <= b)
                .unwrap_or(self.custom_values.len()) as i32
        } else {
            Self::index_for(self.schema, magnitude)
        };
        match side.binary_search_by_key(&index, |b| b.index) {
            Ok(at) => side[at].count += 1.0,
            Err(at) => side.insert(at, Bucket { index, count: 1.0 }),
        }
    }

    /// Put the histogram in canonical form: sorted by index, no empty buckets.
    ///
    /// Merges duplicate indices rather than dropping one, because a wire
    /// decoder that produced two entries for one bucket meant their sum.
    pub fn canonicalise(&mut self) {
        for side in [&mut self.positive, &mut self.negative] {
            side.sort_by_key(|b| b.index);
            side.dedup_by(|b, a| {
                if a.index == b.index {
                    a.count += b.count;
                    true
                } else {
                    false
                }
            });
            side.retain(|b| b.count != 0.0);
        }
    }

    /// Check every invariant the rest of the engine relies on.
    ///
    /// # Errors
    ///
    /// See [`HistogramError`]. The checks are the ones whose violation would
    /// produce a *plausible wrong number* rather than a crash: an unsorted
    /// bucket list interpolates to the wrong quantile, and a `count` smaller
    /// than its buckets makes every rank fraction exceed one.
    pub fn validate(&self) -> Result<(), HistogramError> {
        if self.schema != CUSTOM_BUCKETS_SCHEMA && !(MIN_SCHEMA..=MAX_SCHEMA).contains(&self.schema)
        {
            return Err(HistogramError::InvalidSchema(self.schema));
        }
        if self.has_custom_buckets() {
            if self.custom_values.is_empty() {
                return Err(HistogramError::CustomValues(
                    "schema -53 means the bucket boundaries are listed, and none were given",
                ));
            }
            // `>=` rather than `!(<)`: a NaN boundary is incomparable, and
            // `partial_cmp` returning `None` must count as *not ascending*
            // rather than as ascending.
            if self
                .custom_values
                .windows(2)
                .any(|w| !matches!(w[0].partial_cmp(&w[1]), Some(std::cmp::Ordering::Less)))
            {
                return Err(HistogramError::CustomValues(
                    "custom bucket boundaries must be strictly ascending",
                ));
            }
            if !self.negative.is_empty() {
                return Err(HistogramError::CustomValues(
                    "custom buckets carry their own boundaries, which may be negative, \
                     so the negative bucket list must be empty",
                ));
            }
        } else if !self.custom_values.is_empty() {
            return Err(HistogramError::CustomValues(
                "custom bucket boundaries were given for an exponential schema, \
                 where the boundaries are computed from the schema",
            ));
        }

        if !self.zero_threshold.is_finite() || self.zero_threshold < 0.0 {
            return Err(HistogramError::InvalidZeroThreshold(self.zero_threshold));
        }

        let mut total = 0.0;
        for (what, v) in [("count", self.count), ("zero_count", self.zero_count)] {
            if !v.is_finite() || v < 0.0 {
                return Err(HistogramError::InvalidCount { what, value: v });
            }
            if v > MAX_EXACT_COUNT {
                return Err(HistogramError::CountNotExact { what, value: v });
            }
        }
        total += self.zero_count;

        for (side, name) in [(&self.positive, "positive"), (&self.negative, "negative")] {
            let mut previous: Option<i32> = None;
            for b in side {
                if !b.count.is_finite() || b.count < 0.0 {
                    return Err(HistogramError::InvalidCount {
                        what: "bucket count",
                        value: b.count,
                    });
                }
                if b.count > MAX_EXACT_COUNT {
                    return Err(HistogramError::CountNotExact {
                        what: "bucket count",
                        value: b.count,
                    });
                }
                if previous.is_some_and(|p| p >= b.index) {
                    return Err(HistogramError::UnorderedBuckets {
                        side: name,
                        index: b.index,
                    });
                }
                previous = Some(b.index);
                total += b.count;
            }
        }

        // The whole must be at least the sum of its parts. Equality is not
        // required: a histogram whose `sum` is NaN had NaN observations, and
        // those are counted in `count` and fall in no bucket.
        if total > self.count {
            return Err(HistogramError::CountMismatch {
                observed: total,
                declared: self.count,
            });
        }
        Ok(())
    }

    /// The mean observation, or `NaN` for an empty histogram.
    #[must_use]
    pub fn avg(&self) -> f64 {
        if self.count == 0.0 {
            return f64::NAN;
        }
        self.sum / self.count
    }

    /// The value each bucket is taken to represent, for moment calculations.
    ///
    /// Prometheus uses the **geometric** mean of the bounds for exponential
    /// buckets and the arithmetic midpoint for custom ones. That is not a
    /// detail: a bucket spanning `(1, 2]` has geometric mean 1.414 and
    /// arithmetic midpoint 1.5, and on a log-scaled bucket the geometric one
    /// is the representative value.
    fn representative(&self, index: i32, negative: bool) -> f64 {
        let (lower, upper) = self.bucket_bounds(index, negative);
        if self.has_custom_buckets() {
            if lower.is_infinite() {
                return upper;
            }
            if upper.is_infinite() {
                return lower;
            }
            return (upper + lower) / 2.0;
        }
        if lower <= 0.0 && upper >= 0.0 {
            return 0.0;
        }
        let g = (upper.abs() * lower.abs()).sqrt();
        if negative { -g } else { g }
    }

    /// Variance of the observations, estimated from bucket representatives.
    #[must_use]
    pub fn variance(&self) -> f64 {
        if self.count == 0.0 || self.sum.is_nan() {
            return f64::NAN;
        }
        let mean = self.avg();
        let mut acc = 0.0;
        if self.zero_count > 0.0 {
            acc += self.zero_count * (0.0 - mean).powi(2);
        }
        for (side, negative) in [(&self.positive, false), (&self.negative, true)] {
            for b in side {
                if b.count == 0.0 {
                    continue;
                }
                acc += b.count * (self.representative(b.index, negative) - mean).powi(2);
            }
        }
        acc / self.count
    }

    /// Standard deviation of the observations.
    #[must_use]
    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Every populated bucket, ascending by lower bound.
    ///
    /// Negative buckets first (most negative first), then the zero bucket if
    /// populated, then positive. This is the order every rank calculation
    /// walks, and having exactly one function produce it is what stops
    /// `quantile` and `fraction` disagreeing about where a bucket starts.
    fn ordered_buckets(&self) -> Vec<(f64, f64, f64)> {
        let mut out: Vec<(f64, f64, f64)> = Vec::with_capacity(
            self.negative.len() + self.positive.len() + usize::from(self.zero_count > 0.0),
        );
        // Negative side: a larger index is more negative, so descending index
        // is ascending value.
        for b in self.negative.iter().rev() {
            let (lo, hi) = self.bucket_bounds(b.index, true);
            out.push((lo, hi, b.count));
        }
        if self.zero_count > 0.0 {
            out.push((-self.zero_threshold, self.zero_threshold, self.zero_count));
        }
        for b in &self.positive {
            let (lo, hi) = self.bucket_bounds(b.index, false);
            out.push((lo, hi, b.count));
        }
        out
    }

    /// Interpolate a value inside one bucket at `fraction` of its population.
    ///
    /// **Exponential buckets interpolate on a log₂ scale**, which is the
    /// upstream rule and the one a naive implementation gets wrong: a linear
    /// interpolation inside `(1, 1024]` puts the midpoint at 512, while the
    /// bucket's geometry puts it at 32. Custom buckets and any bucket
    /// straddling zero interpolate linearly, because neither has that
    /// geometry.
    fn interpolate(&self, lower: f64, upper: f64, fraction: f64) -> f64 {
        // The endpoints are returned as themselves rather than computed. A
        // log₂ round trip does not land back on its own input — `2^log2(2)`
        // came back as `2.000000000000003`, one ULP *outside* the bucket that
        // produced it — and a quantile outside the bucket it was located in is
        // wrong by definition, not merely imprecise.
        //
        // A `NaN` fraction satisfies neither test and falls through to the
        // arithmetic below, which propagates it — as it did before these two
        // guards existed. Writing the first as `!(fraction > 0.0)` would have
        // swallowed it and returned `lower`.
        if fraction <= 0.0 {
            return lower;
        }
        if fraction >= 1.0 {
            return upper;
        }

        let linear = lower + (upper - lower) * fraction;
        if self.has_custom_buckets() || (lower <= 0.0 && upper >= 0.0) {
            return linear;
        }
        if !lower.is_finite() || !upper.is_finite() || lower == 0.0 || upper == 0.0 {
            return linear;
        }
        let interpolated = if lower < 0.0 {
            // Mirror: interpolate over the magnitudes, from the larger
            // magnitude (the lower, more negative bound) downwards.
            let log_hi = (-lower).log2();
            let log_lo = (-upper).log2();
            -(2.0_f64).powf(log_hi + (log_lo - log_hi) * fraction)
        } else {
            let log_lo = lower.log2();
            let log_hi = upper.log2();
            (2.0_f64).powf(log_lo + (log_hi - log_lo) * fraction)
        };
        // `powf` is not correctly rounded, so the strictly-inside case can
        // still land a ULP outside. The bucket is the authority.
        interpolated.clamp(lower, upper)
    }

    /// The φ-quantile of the observations.
    ///
    /// Follows Prometheus's `histogramQuantile`: `q < 0` is `-Inf`, `q > 1` is
    /// `+Inf`, and an empty histogram or a `NaN` φ is `NaN`. The value is
    /// found by walking buckets in ascending order until the cumulative count
    /// reaches `q · count`, then interpolating inside that bucket.
    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        if q.is_nan() || self.count == 0.0 {
            return f64::NAN;
        }
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        let buckets = self.ordered_buckets();
        if buckets.is_empty() {
            return f64::NAN;
        }
        let rank = q * self.count;
        let mut cumulative = 0.0;
        for (lower, upper, count) in &buckets {
            if cumulative + count >= rank {
                let fraction = if *count == 0.0 {
                    0.0
                } else {
                    ((rank - cumulative) / count).clamp(0.0, 1.0)
                };
                // A bucket open at the bottom (custom `-Inf`) can only answer
                // with its upper bound.
                if !lower.is_finite() {
                    return *upper;
                }
                if !upper.is_finite() {
                    return *lower;
                }
                return self.interpolate(*lower, *upper, fraction);
            }
            cumulative += count;
        }
        // Every bucket was below the rank: either NaN observations are
        // unaccounted for, or floating-point drift. The largest bound is the
        // honest answer.
        let (_, upper, _) = buckets[buckets.len() - 1];
        if upper.is_finite() {
            upper
        } else {
            f64::INFINITY
        }
    }

    /// The fraction of observations in `[lower, upper)`.
    ///
    /// `NaN` for an empty histogram or a `NaN` bound; `0` when the interval is
    /// empty or inverted.
    #[must_use]
    pub fn fraction(&self, lower: f64, upper: f64) -> f64 {
        if self.count == 0.0 || lower.is_nan() || upper.is_nan() {
            return f64::NAN;
        }
        if lower >= upper {
            return 0.0;
        }
        (self.rank_at(upper) - self.rank_at(lower)) / self.count
    }

    /// How many observations lie below `value`, interpolating within a bucket.
    fn rank_at(&self, value: f64) -> f64 {
        if value == f64::NEG_INFINITY {
            return 0.0;
        }
        if value == f64::INFINITY {
            return self.count;
        }
        let mut cumulative = 0.0;
        for (lo, hi, count) in self.ordered_buckets() {
            if value <= lo {
                break;
            }
            if value >= hi {
                cumulative += count;
                continue;
            }
            // Inside this bucket: invert the interpolation.
            let fraction = if self.has_custom_buckets() || (lo <= 0.0 && hi >= 0.0) {
                if hi == lo {
                    0.0
                } else {
                    (value - lo) / (hi - lo)
                }
            } else if lo > 0.0 {
                let (a, b) = (lo.log2(), hi.log2());
                if (b - a).abs() < f64::EPSILON {
                    0.0
                } else {
                    (value.log2() - a) / (b - a)
                }
            } else {
                let (a, b) = ((-lo).log2(), (-hi).log2());
                if (b - a).abs() < f64::EPSILON {
                    0.0
                } else {
                    ((-value).log2() - a) / (b - a)
                }
            };
            cumulative += count * fraction.clamp(0.0, 1.0);
            break;
        }
        cumulative
    }

    /// The classic `le`-bucketed view of this histogram.
    ///
    /// Returns `(le, cumulative_count)` pairs ascending, ending at `+Inf`.
    /// This is what makes a dashboard written against `foo_bucket` keep
    /// working once the storage holds a native histogram — the classic
    /// exposition is a *view*, not a second stored form, which is also
    /// upstream's direction for composite samples.
    #[must_use]
    pub fn classic_buckets(&self) -> Vec<(f64, f64)> {
        let mut out = Vec::new();
        let mut cumulative = 0.0;
        for (_, upper, count) in self.ordered_buckets() {
            cumulative += count;
            if upper.is_finite() {
                out.push((upper, cumulative));
            }
        }
        out.push((f64::INFINITY, self.count));
        out
    }

    /// Add `other` into `self`.
    ///
    /// Defined only for histograms that share a schema and zero threshold —
    /// two different resolutions cannot be added without re-bucketing, and
    /// re-bucketing to the coarser of the two is a *decision* rather than
    /// something to do silently inside `+`.
    ///
    /// # Errors
    ///
    /// [`HistogramError::CustomValues`] when the schemas or boundaries differ.
    pub fn merge(&mut self, other: &Self) -> Result<(), HistogramError> {
        if self.schema != other.schema {
            return Err(HistogramError::CustomValues(
                "cannot merge histograms of different schemas without re-bucketing, \
                 which changes the resolution and must be asked for explicitly",
            ));
        }
        if self.has_custom_buckets() && self.custom_values != other.custom_values {
            return Err(HistogramError::CustomValues(
                "cannot merge custom-bucket histograms with different boundaries",
            ));
        }
        if self.count == 0.0 && self.positive.is_empty() && self.negative.is_empty() {
            self.zero_threshold = other.zero_threshold;
        } else if (self.zero_threshold - other.zero_threshold).abs() > f64::EPSILON {
            return Err(HistogramError::CustomValues(
                "cannot merge histograms with different zero thresholds: the zero \
                 bucket would cover different intervals on either side",
            ));
        }
        self.count += other.count;
        self.sum += other.sum;
        self.zero_count += other.zero_count;
        for (mine, theirs) in [
            (&mut self.positive, &other.positive),
            (&mut self.negative, &other.negative),
        ] {
            for b in theirs {
                match mine.binary_search_by_key(&b.index, |x| x.index) {
                    Ok(at) => mine[at].count += b.count,
                    Err(at) => mine.insert(at, *b),
                }
            }
        }
        self.reset_hint = ResetHint::Unknown;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

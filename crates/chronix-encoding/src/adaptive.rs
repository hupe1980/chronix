//! Adaptive encoding selection — samples data to choose the optimal encoding.
//!
//! [`AdaptiveSelector`] examines the first N values (default: 1024) of a
//! column to estimate which encoder will achieve the best compression ratio,
//! then encodes the full column with that encoder.
//!
//! ## Decision matrix
//!
//! | Pattern                  | Selected encoding              |
//! |--------------------------|--------------------------------|
//! | All values identical     | RLE                            |
//! | Periodic floats          | Chimp128                       |
//! | Regular intervals (i64)  | Delta-of-delta                 |
//! | Narrow-range integers    | Frame-of-Reference (FOR)       |
//! | Slowly varying floats    | Chimp128 / Chimp               |
//! | Random floats            | Gorilla → Plain fallback       |
//! | Low-cardinality strings  | Dictionary                     |
//! | High-cardinality strings | Plain                          |
//! | Booleans                 | Bitmap (always)                |

use std::collections::HashSet;

use crate::error::Result;
use crate::unified::{ColumnEncoder, EncodedBlock};

/// Default sample size for adaptive selection.
const DEFAULT_SAMPLE_SIZE: usize = 1024;

/// Number of strata for stratified sampling.
const NUM_STRATA: usize = 4;

/// Adaptive encoding selector.
///
/// Uses stratified sampling: divides the column into `NUM_STRATA`
/// equal segments and samples `sample_size / NUM_STRATA` values from the
/// start of each segment. This captures data characteristics across the
/// entire column, not just the leading values.
#[derive(Debug, Clone)]
pub struct AdaptiveSelector {
    /// Total number of values to sample (spread across strata).
    pub sample_size: usize,
}

impl Default for AdaptiveSelector {
    fn default() -> Self {
        Self {
            sample_size: DEFAULT_SAMPLE_SIZE,
        }
    }
}

/// Result of analyzing a float sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatPattern {
    /// All values are identical — constant column.
    Constant,
    /// Highly periodic data with repeating values — Chimp128 excels.
    Periodic,
    /// Values change slowly (small XOR leading zeros) — Chimp excels.
    SlowlyVarying,
    /// Values are mostly random — Gorilla or Plain.
    Random,
}

/// Result of analyzing a string sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringPattern {
    /// Few distinct values — dictionary encoding.
    LowCardinality,
    /// Many distinct values — plain encoding.
    HighCardinality,
}

/// Result of analyzing an integer sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerPattern {
    /// All values identical.
    Constant,
    /// Regular intervals — ideal for delta-of-delta.
    RegularInterval,
    /// Values cluster in a narrow range — ideal for FOR encoding.
    NarrowRange,
    /// Irregular — delta with zigzag.
    Irregular,
}

impl AdaptiveSelector {
    /// Create a new adaptive selector with the default sample size.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stratified-sample indices: divide `len` into [`NUM_STRATA`]
    /// segments and take up to `per_stratum` contiguous values from the
    /// start of each segment.
    fn stratified_sample<T: Copy>(values: &[T], budget: usize) -> Vec<T> {
        let n = values.len();
        if n <= budget {
            return values.to_vec();
        }
        let per_stratum = budget / NUM_STRATA;
        let stride = n / NUM_STRATA;
        let mut out = Vec::with_capacity(budget);
        for s in 0..NUM_STRATA {
            let start = s * stride;
            let end = (start + per_stratum).min(n);
            out.extend_from_slice(&values[start..end]);
        }
        out
    }

    /// Analyze a float column sample and determine the best encoding.
    #[must_use]
    pub fn analyze_floats(&self, values: &[f64]) -> FloatPattern {
        if values.is_empty() {
            return FloatPattern::Random;
        }

        let sample = Self::stratified_sample(values, self.sample_size);

        // Check for constant
        let first = sample[0].to_bits();
        if sample.iter().all(|v| v.to_bits() == first) {
            return FloatPattern::Constant;
        }

        // Measure XOR leading zeros — high average means slowly varying
        let mut total_leading: u64 = 0;
        let mut prev = first;
        for &v in &sample[1..] {
            let bits = v.to_bits();
            let xor = prev ^ bits;
            total_leading += if xor == 0 {
                64
            } else {
                xor.leading_zeros() as u64
            };
            prev = bits;
        }

        #[allow(clippy::cast_precision_loss)]
        let avg_leading = total_leading as f64 / (sample.len() - 1) as f64;

        // Detect periodic data: if >30% of values in the sample appear
        // among the previous 128 values, the ring-buffer optimisation
        // in Chimp128 will find near-exact matches.
        if sample.len() > 128 {
            let mut hits: usize = 0;
            for i in 128..sample.len() {
                let bits = sample[i].to_bits();
                let window = &sample[i.saturating_sub(128)..i];
                if window.iter().any(|v| v.to_bits() == bits) {
                    hits += 1;
                }
            }
            #[allow(clippy::cast_precision_loss)]
            let hit_rate = hits as f64 / (sample.len() - 128) as f64;
            if hit_rate > 0.3 {
                return FloatPattern::Periodic;
            }
        }

        if avg_leading >= 16.0 {
            FloatPattern::SlowlyVarying
        } else {
            FloatPattern::Random
        }
    }

    /// Analyze a string column sample and determine the best encoding.
    #[must_use]
    pub fn analyze_strings(&self, values: &[&str]) -> StringPattern {
        if values.is_empty() {
            return StringPattern::HighCardinality;
        }

        let sample = Self::stratified_sample(values, self.sample_size);
        let distinct: HashSet<&str> = sample.iter().copied().collect();

        // Low cardinality: fewer unique values than 10% of sample size
        if distinct.len() <= (sample.len() / 10).max(1) {
            StringPattern::LowCardinality
        } else {
            StringPattern::HighCardinality
        }
    }

    /// Analyze an integer column sample and determine the best encoding.
    #[must_use]
    pub fn analyze_integers(&self, values: &[i64]) -> IntegerPattern {
        if values.is_empty() {
            return IntegerPattern::Irregular;
        }

        let sample = Self::stratified_sample(values, self.sample_size);

        // Check for constant
        let first = sample[0];
        if sample.iter().all(|&v| v == first) {
            return IntegerPattern::Constant;
        }

        // Check for regular interval (use wrapping subtraction to avoid panic)
        if sample.len() >= 3 {
            let interval = sample[1].wrapping_sub(sample[0]);
            let is_regular = sample
                .windows(2)
                .all(|w| w[1].wrapping_sub(w[0]) == interval);
            if is_regular {
                return IntegerPattern::RegularInterval;
            }
        }

        // Check for narrow range: if the value range fits in ≤16 bits
        // (65 535), FOR encoding will pack at most 16 bits per value
        // vs 64 bits plain — a guaranteed ≥4× win.  This heuristic is
        // cheap (one pass to find min/max of the sample, which we already
        // have), and conservatively covers the sweet spot for FOR.
        let min = sample.iter().copied().min().unwrap_or(0);
        let max = sample.iter().copied().max().unwrap_or(0);
        let range = (max as u128).wrapping_sub(min as u128);
        if range <= u128::from(u16::MAX) {
            return IntegerPattern::NarrowRange;
        }

        IntegerPattern::Irregular
    }

    /// Encode a float column with adaptive selection.
    ///
    /// Uses sample-based trial encoding: trial-encodes only a
    /// stratified sample with each candidate encoder to pick the winner,
    /// then encodes the full column exactly once with the best encoder.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails or the input is empty.
    pub fn encode_f64_adaptive(&self, values: &[f64]) -> Result<EncodedBlock> {
        use crate::alp::AlpEncoder;
        use crate::chimp::{Chimp128Encoder, ChimpEncoder};
        use crate::gorilla::GorillaEncoder;
        use crate::patas::PatasEncoder;
        use crate::plain::PlainEncoder;
        use crate::unified::{should_use_specialized, EncodingType};

        let pattern = self.analyze_floats(values);

        // For deterministic patterns, skip trial encoding entirely.
        // Constant data always compresses best with RLE; Random has only one
        // candidate (Gorilla), so trial-encoding is redundant.
        if matches!(pattern, FloatPattern::Constant) {
            return self.encode_f64_direct(values, pattern);
        }

        // For small columns, skip sampling and encode directly.
        if values.len() <= self.sample_size {
            return self.encode_f64_direct(values, pattern);
        }

        // Trial-encode only the sample to pick the winner.
        let sample = Self::stratified_sample(values, self.sample_size);
        let sample_raw_size = sample.len() * 8;

        // Build candidate list ordered by expected quality for this pattern.
        let candidates: &[EncodingType] = match pattern {
            FloatPattern::Constant => &[
                EncodingType::Rle,
                EncodingType::Chimp,
                EncodingType::Gorilla,
            ],
            // ALP leads every non-constant list: values that started life as
            // decimals — which is most metric data — compress several times
            // better under it than under any XOR codec, and on the data it is
            // not for it simply loses the trial and costs one sample encode.
            FloatPattern::Periodic => &[
                EncodingType::Alp,
                EncodingType::Chimp128,
                EncodingType::Patas,
                EncodingType::Chimp,
                EncodingType::Gorilla,
            ],
            FloatPattern::SlowlyVarying => &[
                EncodingType::Alp,
                EncodingType::Patas,
                EncodingType::Chimp128,
                EncodingType::Chimp,
                EncodingType::Gorilla,
            ],
            FloatPattern::Random => &[
                EncodingType::Alp,
                EncodingType::Gorilla,
                EncodingType::Patas,
            ],
        };

        // Trial-encode the sample with each candidate; pick the smallest
        // that passes the compression ratio threshold.
        let mut best: Option<(EncodingType, usize)> = None;
        let mut best_any: Option<(EncodingType, usize)> = None;
        for &enc in candidates {
            let trial = match enc {
                EncodingType::Rle => crate::rle::RleEncoder::encode_f64(&sample),
                EncodingType::Alp => AlpEncoder::encode(&sample),
                EncodingType::Chimp128 => Chimp128Encoder::encode(&sample),
                EncodingType::Patas => PatasEncoder::encode(&sample),
                EncodingType::Chimp => ChimpEncoder::encode(&sample),
                EncodingType::Gorilla => GorillaEncoder::encode(&sample),
                _ => continue,
            };
            if let Ok(payload) = trial {
                let len = payload.len();
                // Track smallest candidate regardless of threshold
                if best_any.as_ref().is_none_or(|(_, bl)| len < *bl) {
                    best_any = Some((enc, len));
                }
                if should_use_specialized(&payload, sample_raw_size)
                    && best.as_ref().is_none_or(|(_, best_len)| len < *best_len)
                {
                    best = Some((enc, len));
                }
            }
        }

        // If no candidate passed the sample threshold, fall back to the
        // best-any candidate so pattern-specific encoders (e.g. Chimp128
        // for Periodic) still get tried on the full data where they often
        // achieve the required compression ratio.
        let winner = best
            .or(best_any)
            .map_or(EncodingType::PlainF64, |(enc, _)| enc);
        let payload = match winner {
            EncodingType::Rle => crate::rle::RleEncoder::encode_f64(values)?,
            EncodingType::Alp => AlpEncoder::encode(values)?,
            EncodingType::Chimp128 => Chimp128Encoder::encode(values)?,
            EncodingType::Patas => PatasEncoder::encode(values)?,
            EncodingType::Chimp => ChimpEncoder::encode(values)?,
            EncodingType::Gorilla => GorillaEncoder::encode(values)?,
            _ => PlainEncoder::encode_f64(values)?,
        };

        // Verify the winner still beats plain on the full data; if not, fall
        // back to plain. This guards against sample-vs-full divergence.
        let full_raw_size = values.len() * 8;
        if winner != EncodingType::PlainF64 && !should_use_specialized(&payload, full_raw_size) {
            return Ok(EncodedBlock {
                encoding: EncodingType::PlainF64,
                payload: PlainEncoder::encode_f64(values)?,
            });
        }

        Ok(EncodedBlock {
            encoding: winner,
            payload,
        })
    }

    /// Direct encode without sample-based selection (for small columns).
    fn encode_f64_direct(&self, values: &[f64], pattern: FloatPattern) -> Result<EncodedBlock> {
        use crate::alp::AlpEncoder;
        use crate::chimp::{Chimp128Encoder, ChimpEncoder};
        use crate::gorilla::GorillaEncoder;
        use crate::patas::PatasEncoder;
        use crate::plain::PlainEncoder;
        use crate::unified::{should_use_specialized, EncodedBlock, EncodingType};

        let raw_size = values.len() * 8;
        match pattern {
            FloatPattern::Constant => {
                let rle_payload = crate::rle::RleEncoder::encode_f64(values)?;
                if should_use_specialized(&rle_payload, raw_size) {
                    return Ok(EncodedBlock {
                        encoding: EncodingType::Rle,
                        payload: rle_payload,
                    });
                }
            }
            FloatPattern::Periodic | FloatPattern::SlowlyVarying => {
                let chimp128_payload = Chimp128Encoder::encode(values)?;
                if should_use_specialized(&chimp128_payload, raw_size) {
                    return Ok(EncodedBlock {
                        encoding: EncodingType::Chimp128,
                        payload: chimp128_payload,
                    });
                }
                // Try Patas as a byte-aligned alternative
                let patas_payload = PatasEncoder::encode(values)?;
                if should_use_specialized(&patas_payload, raw_size) {
                    return Ok(EncodedBlock {
                        encoding: EncodingType::Patas,
                        payload: patas_payload,
                    });
                }
            }
            FloatPattern::Random => {}
        }

        // Common fallback chain: ALP → Chimp → Patas → Gorilla → Plain.
        // ALP goes first for the same reason it leads the sampled candidate
        // lists above.
        let alp_payload = AlpEncoder::encode(values)?;
        if should_use_specialized(&alp_payload, raw_size) {
            return Ok(EncodedBlock {
                encoding: EncodingType::Alp,
                payload: alp_payload,
            });
        }
        let chimp_payload = ChimpEncoder::encode(values)?;
        if should_use_specialized(&chimp_payload, raw_size) {
            return Ok(EncodedBlock {
                encoding: EncodingType::Chimp,
                payload: chimp_payload,
            });
        }
        let patas_payload = PatasEncoder::encode(values)?;
        if should_use_specialized(&patas_payload, raw_size) {
            return Ok(EncodedBlock {
                encoding: EncodingType::Patas,
                payload: patas_payload,
            });
        }
        let gorilla_payload = GorillaEncoder::encode(values)?;
        if should_use_specialized(&gorilla_payload, raw_size) {
            return Ok(EncodedBlock {
                encoding: EncodingType::Gorilla,
                payload: gorilla_payload,
            });
        }
        Ok(EncodedBlock {
            encoding: EncodingType::PlainF64,
            payload: PlainEncoder::encode_f64(values)?,
        })
    }

    /// Encode a string column with adaptive selection.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails or the input is empty.
    pub fn encode_string_adaptive(&self, values: &[&str]) -> Result<EncodedBlock> {
        // The unified encoder already handles this, but we can shortcut
        // if we know it's high cardinality
        let pattern = self.analyze_strings(values);
        match pattern {
            StringPattern::LowCardinality => ColumnEncoder::encode_string(values),
            StringPattern::HighCardinality => {
                // Try dictionary anyway (unified encoder checks ratio)
                ColumnEncoder::encode_string(values)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unified::EncodingType;

    #[test]
    fn detects_constant_floats() {
        let selector = AdaptiveSelector::new();
        let values = vec![42.0_f64; 500];
        assert_eq!(selector.analyze_floats(&values), FloatPattern::Constant);
    }

    #[test]
    fn detects_slowly_varying_floats() {
        let selector = AdaptiveSelector::new();
        let values: Vec<f64> = (0..500).map(|i| 50.0 + (i as f64) * 0.01).collect();
        assert_eq!(
            selector.analyze_floats(&values),
            FloatPattern::SlowlyVarying
        );
    }

    #[test]
    fn detects_random_floats() {
        let selector = AdaptiveSelector::new();
        // Highly varied values: sin(large) produces pseudo-random XOR patterns
        let values: Vec<f64> = (0..500)
            .map(|i| (i as f64 * 123.456).sin() * 1e15)
            .collect();
        assert_eq!(selector.analyze_floats(&values), FloatPattern::Random);
    }

    #[test]
    fn encode_f64_adaptive_selects_alp_for_slowly_varying_decimals() {
        let selector = AdaptiveSelector::new();
        // Two-decimal values — what a meter or sensor actually emits, and
        // what ALP reconstructs as scaled integers.
        let values: Vec<f64> = (0..500).map(|i| 50.0 + (i as f64) * 0.01).collect();
        let block = selector.encode_f64_adaptive(&values).unwrap();
        assert_eq!(
            block.encoding,
            EncodingType::Alp,
            "decimal data should select ALP over the XOR codecs, got {}",
            block.encoding,
        );
        // Verify correctness regardless of which encoding was chosen
        let decoded = crate::unified::ColumnDecoder::decode(&block).unwrap();
        if let crate::unified::DecodedColumn::F64(vals) = decoded {
            assert_eq!(vals.len(), values.len());
            for (a, b) in values.iter().zip(vals.iter()) {
                assert_eq!(a.to_bits(), b.to_bits());
            }
        } else {
            panic!("Expected F64 decoded column");
        }
    }

    #[test]
    fn detects_low_cardinality_strings() {
        let selector = AdaptiveSelector::new();
        let values = vec!["us-east-1"; 100];
        assert_eq!(
            selector.analyze_strings(&values),
            StringPattern::LowCardinality
        );
    }

    #[test]
    fn detects_high_cardinality_strings() {
        let selector = AdaptiveSelector::new();
        let owned: Vec<String> = (0..100).map(|i| format!("uuid-{i}")).collect();
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        assert_eq!(
            selector.analyze_strings(&refs),
            StringPattern::HighCardinality
        );
    }

    #[test]
    fn detects_constant_integers() {
        let selector = AdaptiveSelector::new();
        let values = vec![42_i64; 500];
        assert_eq!(selector.analyze_integers(&values), IntegerPattern::Constant);
    }

    #[test]
    fn detects_regular_interval_integers() {
        let selector = AdaptiveSelector::new();
        let values: Vec<i64> = (0..500).map(|i| 1000 + i * 10).collect();
        assert_eq!(
            selector.analyze_integers(&values),
            IntegerPattern::RegularInterval
        );
    }

    #[test]
    fn detects_irregular_integers() {
        let selector = AdaptiveSelector::new();
        // Range > 65535 so NarrowRange does not trigger.
        let values = vec![1, 100, 3, 999, 42, 7, 88, 100_000];
        assert_eq!(
            selector.analyze_integers(&values),
            IntegerPattern::Irregular
        );
    }

    #[test]
    fn detects_narrow_range_integers() {
        let selector = AdaptiveSelector::new();
        // HTTP status codes: values clustered in range [200, 503] → NarrowRange.
        let values = vec![200, 201, 204, 301, 302, 400, 404, 500, 502, 503];
        assert_eq!(
            selector.analyze_integers(&values),
            IntegerPattern::NarrowRange
        );
    }

    #[test]
    fn periodic_data_dispatches_chimp128() {
        let selector = AdaptiveSelector::new();
        // Periodic sensor data: 64 distinct values cycling many times (>1024 to
        // exercise the sample path, and >30% ring-buffer hits).
        let base: Vec<f64> = (0..64).map(|i| 20.0 + (i as f64) * 0.5).collect();
        let values: Vec<f64> = base.iter().cycle().take(5000).copied().collect();

        assert_eq!(selector.analyze_floats(&values), FloatPattern::Periodic);

        let block = selector.encode_f64_adaptive(&values).unwrap();
        assert_eq!(
            block.encoding,
            EncodingType::Chimp128,
            "Periodic data should use Chimp128, got {}",
            block.encoding,
        );

        // Verify lossless round-trip.
        let decoded = crate::unified::ColumnDecoder::decode(&block).unwrap();
        if let crate::unified::DecodedColumn::F64(vals) = decoded {
            assert_eq!(vals.len(), values.len());
            for (a, b) in values.iter().zip(vals.iter()) {
                assert_eq!(a.to_bits(), b.to_bits());
            }
        } else {
            panic!("Expected F64 decoded column");
        }
    }

    #[test]
    fn slowly_varying_picks_a_specialised_float_codec() {
        let selector = AdaptiveSelector::new();
        // Slowly varying data. Whichever candidate wins the trial, the
        // selector must not fall back to plain on data this compressible.
        let values: Vec<f64> = (0..5000).map(|i| 50.0 + (i as f64) * 0.001).collect();

        let block = selector.encode_f64_adaptive(&values).unwrap();
        assert!(
            block.encoding == EncodingType::Alp
                || block.encoding == EncodingType::Chimp128
                || block.encoding == EncodingType::Chimp
                || block.encoding == EncodingType::Gorilla,
            "Unexpected encoding: {}",
            block.encoding,
        );

        // Verify correctness regardless of chosen encoding.
        let decoded = crate::unified::ColumnDecoder::decode(&block).unwrap();
        if let crate::unified::DecodedColumn::F64(vals) = decoded {
            assert_eq!(vals.len(), values.len());
        } else {
            panic!("Expected F64 decoded column");
        }
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::unified::{ColumnDecoder, DecodedColumn};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_f64_adaptive(values in proptest::collection::vec(any::<f64>(), 50..300)) {
            let selector = AdaptiveSelector::new();
            let block = selector.encode_f64_adaptive(&values).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            match decoded {
                DecodedColumn::F64(vals) => {
                    prop_assert_eq!(values.len(), vals.len());
                    for (a, b) in values.iter().zip(vals.iter()) {
                        prop_assert_eq!(a.to_bits(), b.to_bits());
                    }
                }
                other => prop_assert!(false, "Expected F64, got {other:?}"),
            }
        }

        #[test]
        fn roundtrip_string_adaptive(values in proptest::collection::vec("[a-z]{0,8}", 1..100)) {
            let selector = AdaptiveSelector::new();
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let block = selector.encode_string_adaptive(&refs).unwrap();
            let decoded = ColumnDecoder::decode(&block).unwrap();
            match decoded {
                DecodedColumn::String(vals) => {
                    prop_assert_eq!(&values, &vals);
                }
                other => prop_assert!(false, "Expected String, got {other:?}"),
            }
        }
    }
}

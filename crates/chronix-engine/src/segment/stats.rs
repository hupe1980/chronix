//! Per-column statistics for row groups and segments.
//!
//! Statistics enable query predicate pushdown: if a row group's min/max
//! timestamps don't overlap with a query's time range, the entire row group
//! can be skipped.
//!
//! # Float ordering
//!
//! Float values are stored as i64 using a total-order mapping that preserves
//! comparison semantics for negative values. See [`f64_to_ordered_i64`].

use serde::{Deserialize, Serialize};

use crate::segment::error::{Result, SegmentError};
use crate::segment::to_array;

/// Convert an `f64` to an `i64` that preserves total ordering.
///
/// IEEE 754 doubles are sign-magnitude, which means positive values sort
/// correctly when reinterpreted as i64, but negative values sort backwards.
/// For negatives (sign bit set), we flip the value bits (XOR with `i64::MAX`)
/// so that more-negative floats map to more-negative i64 values.
///
/// Resulting order: `-∞ < -max < … < -0 < +0 < … < +max < +∞ < NaN`
#[inline]
#[must_use]
pub fn f64_to_ordered_i64(value: f64) -> i64 {
    let bits = value.to_bits() as i64;
    if bits < 0 {
        bits ^ i64::MAX // flip all bits except the sign bit
    } else {
        bits
    }
}

/// Reverse the transformation done by [`f64_to_ordered_i64`].
#[inline]
#[must_use]
pub fn ordered_i64_to_f64(ordered: i64) -> f64 {
    let bits = if ordered < 0 {
        ordered ^ i64::MAX
    } else {
        ordered
    };
    f64::from_bits(bits as u64)
}

/// Statistics for a single column within a row group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value (as raw i64 bits for i64/f64/bool types).
    ///
    /// For f64 columns this uses the total-order mapping from
    /// [`f64_to_ordered_i64`] so that min/max comparisons are correct
    /// even for negative values.
    pub min_value: i64,
    /// Maximum value (as raw i64 bits for i64/f64/bool types).
    pub max_value: i64,
    /// Minimum u64 value — used only for u64 columns.
    pub min_value_u64: u64,
    /// Maximum u64 value — used only for u64 columns.
    pub max_value_u64: u64,
    /// Number of null values.
    pub null_count: u64,
    /// Number of non-null values.
    pub value_count: u64,
    /// Sum of all values (for numeric types, 0 for others).
    ///
    /// For float columns this is the canonical sum.  For integer columns,
    /// use `sum_i128` for full precision.
    pub sum: f64,
    /// Precise integer sum (i128) for i64/u64 columns.
    ///
    /// Avoids the 2^53 precision loss of the f64 `sum` field.  Set to
    /// zero for float and non-numeric columns.
    pub sum_i128: i128,
    /// Number of distinct values (for string/tag columns, 0 for others).
    pub distinct_count: u32,
}

impl ColumnStats {
    /// Create empty statistics.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            min_value: i64::MAX,
            max_value: i64::MIN,
            min_value_u64: u64::MAX,
            max_value_u64: u64::MIN,
            null_count: 0,
            value_count: 0,
            sum: 0.0,
            sum_i128: 0,
            distinct_count: 0,
        }
    }

    /// `true` when these statistics say nothing about the data.
    ///
    /// A block with no observed value — every row null, or a block whose
    /// statistics were deliberately suppressed because it is encrypted —
    /// leaves `min`/`max` at their sentinels. A zone map must read that as
    /// "unknown", never as an empty range: `min = i64::MAX` and
    /// `max = i64::MIN` make every comparison false, so a predicate on an
    /// encrypted column pruned every row group and the query returned no
    /// rows at all, with the correct key configured.
    ///
    /// The distinction from a genuinely all-null block is `null_count`: an
    /// all-null block *is* impossible to match, and pruning it is correct.
    #[must_use]
    pub fn says_nothing(&self) -> bool {
        self.value_count == 0 && self.null_count == 0
    }

    /// Update statistics with a new `i64` value.
    pub fn update_i64(&mut self, value: i64) {
        self.min_value = self.min_value.min(value);
        self.max_value = self.max_value.max(value);
        self.sum += value as f64;
        self.sum_i128 += i128::from(value);
        self.value_count += 1;
    }

    /// Update statistics with a new `u64` value.
    ///
    /// Tracks full-precision u64 min/max in dedicated fields. Clamping to
    /// `i64::MAX` instead breaks predicate pushdown above 2^63.
    pub fn update_u64(&mut self, value: u64) {
        self.min_value_u64 = self.min_value_u64.min(value);
        self.max_value_u64 = self.max_value_u64.max(value);
        self.sum += value as f64;
        self.sum_i128 += i128::from(value);
        self.value_count += 1;
    }

    /// Update statistics with a new `f64` value.
    ///
    /// Uses [`f64_to_ordered_i64`] for correct ordering of negative values.
    /// NaN values are excluded from min/max tracking (they would sort above
    /// +∞, corrupting predicate pushdown) but are still counted in
    /// `value_count`. Only finite values contribute to `sum`, so computing
    /// an average as `sum / value_count` is only correct when the data
    /// contains no NaN or Infinity values.
    pub fn update_f64(&mut self, value: f64) {
        if !value.is_nan() {
            let ordered = f64_to_ordered_i64(value);
            self.min_value = self.min_value.min(ordered);
            self.max_value = self.max_value.max(ordered);
        }
        if value.is_finite() {
            self.sum += value;
        }
        self.value_count += 1;
    }

    /// Update statistics with a new boolean value.
    pub fn update_bool(&mut self, value: bool) {
        let v = i64::from(value);
        self.min_value = self.min_value.min(v);
        self.max_value = self.max_value.max(v);
        self.value_count += 1;
    }

    /// Record a null value.
    pub fn record_null(&mut self) {
        self.null_count += 1;
    }

    /// Serialize stats to bytes (fixed 60-byte layout).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(76);
        buf.extend_from_slice(&self.min_value.to_le_bytes());
        buf.extend_from_slice(&self.max_value.to_le_bytes());
        buf.extend_from_slice(&self.min_value_u64.to_le_bytes());
        buf.extend_from_slice(&self.max_value_u64.to_le_bytes());
        buf.extend_from_slice(&self.null_count.to_le_bytes());
        buf.extend_from_slice(&self.value_count.to_le_bytes());
        buf.extend_from_slice(&self.sum.to_le_bytes());
        buf.extend_from_slice(&self.sum_i128.to_le_bytes());
        buf.extend_from_slice(&self.distinct_count.to_le_bytes());
        buf
    }

    /// Deserialize stats from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `data` is shorter than 60 bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 76 {
            return Err(SegmentError::CorruptFile {
                detail: format!("ColumnStats requires 76 bytes, got {}", data.len()),
            });
        }
        Ok(Self {
            min_value: i64::from_le_bytes(to_array!(data[0..8], "stats min_value")?),
            max_value: i64::from_le_bytes(to_array!(data[8..16], "stats max_value")?),
            min_value_u64: u64::from_le_bytes(to_array!(data[16..24], "stats min_value_u64")?),
            max_value_u64: u64::from_le_bytes(to_array!(data[24..32], "stats max_value_u64")?),
            null_count: u64::from_le_bytes(to_array!(data[32..40], "stats null_count")?),
            value_count: u64::from_le_bytes(to_array!(data[40..48], "stats value_count")?),
            sum: f64::from_le_bytes(to_array!(data[48..56], "stats sum")?),
            sum_i128: i128::from_le_bytes(to_array!(data[56..72], "stats sum_i128")?),
            distinct_count: u32::from_le_bytes(to_array!(data[72..76], "stats distinct_count")?),
        })
    }
}

/// Size of serialized [`ColumnStats`] in bytes.
pub const COLUMN_STATS_SIZE: usize = 76;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_roundtrip() {
        let mut stats = ColumnStats::empty();
        stats.update_i64(10);
        stats.update_i64(20);
        stats.update_i64(30);

        let bytes = stats.to_bytes();
        assert_eq!(bytes.len(), COLUMN_STATS_SIZE);
        let recovered = ColumnStats::from_bytes(&bytes).unwrap();
        assert_eq!(stats, recovered);
    }

    #[test]
    fn stats_i64_tracking() {
        let mut stats = ColumnStats::empty();
        stats.update_i64(-5);
        stats.update_i64(10);
        stats.update_i64(3);

        assert_eq!(stats.min_value, -5);
        assert_eq!(stats.max_value, 10);
        assert_eq!(stats.value_count, 3);
        assert!((stats.sum - 8.0).abs() < f64::EPSILON);
        assert_eq!(stats.sum_i128, 8);
    }

    #[test]
    fn stats_f64_tracking() {
        let mut stats = ColumnStats::empty();
        stats.update_f64(1.5);
        stats.update_f64(2.5);
        stats.update_f64(f64::NAN);

        assert_eq!(stats.value_count, 3);
        // NaN doesn't contribute to sum
        assert!((stats.sum - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stats_f64_negative_ordering() {
        let mut stats = ColumnStats::empty();
        stats.update_f64(-10.0);
        stats.update_f64(-1.0);
        stats.update_f64(5.0);

        // Recover the actual f64 min/max via the ordered mapping
        let min_f = ordered_i64_to_f64(stats.min_value);
        let max_f = ordered_i64_to_f64(stats.max_value);
        assert!(
            (min_f - (-10.0)).abs() < f64::EPSILON,
            "min should be -10.0, got {min_f}"
        );
        assert!(
            (max_f - 5.0).abs() < f64::EPSILON,
            "max should be 5.0, got {max_f}"
        );
    }

    #[test]
    fn f64_total_ordering() {
        // Verify the ordered mapping preserves total order
        let values = [
            f64::NEG_INFINITY,
            -1e100,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1e100,
            f64::INFINITY,
        ];
        for window in values.windows(2) {
            let a = f64_to_ordered_i64(window[0]);
            let b = f64_to_ordered_i64(window[1]);
            assert!(
                a <= b,
                "ordering violated: {:?} (={a}) should <= {:?} (={b})",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn stats_u64_large_values() {
        let mut stats = ColumnStats::empty();
        stats.update_u64(u64::MAX);
        stats.update_u64(0);

        // u64 values tracked in dedicated fields with full precision
        assert_eq!(stats.max_value_u64, u64::MAX);
        assert_eq!(stats.min_value_u64, 0);
        // sum_i128 preserves full precision even beyond 2^53
        assert_eq!(stats.sum_i128, u64::MAX as i128);
    }

    #[test]
    fn stats_u64_above_i64_max() {
        let mut stats = ColumnStats::empty();
        let big = (i64::MAX as u64) + 100;
        stats.update_u64(big);
        stats.update_u64(42);

        assert_eq!(stats.max_value_u64, big);
        assert_eq!(stats.min_value_u64, 42);
        assert_eq!(stats.value_count, 2);
        assert_eq!(stats.sum_i128, big as i128 + 42);
    }

    #[test]
    fn stats_from_bytes_too_short() {
        assert!(ColumnStats::from_bytes(&[0u8; 10]).is_err());
    }

    #[test]
    fn stats_null_tracking() {
        let mut stats = ColumnStats::empty();
        stats.update_i64(42);
        stats.record_null();
        stats.record_null();

        assert_eq!(stats.value_count, 1);
        assert_eq!(stats.null_count, 2);
    }

    #[test]
    fn stats_empty() {
        let stats = ColumnStats::empty();
        assert_eq!(stats.min_value, i64::MAX);
        assert_eq!(stats.max_value, i64::MIN);
        assert_eq!(stats.value_count, 0);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn stats_f64_nan_excluded_from_min_max() {
        let mut stats = ColumnStats::empty();
        stats.update_f64(f64::NAN);
        // NaN should not update min/max — they should remain at sentinel
        assert_eq!(stats.min_value, i64::MAX, "NaN corrupted min_value");
        assert_eq!(stats.max_value, i64::MIN, "NaN corrupted max_value");
        assert_eq!(stats.value_count, 1);

        // After adding a real value, NaN should not affect min/max
        stats.update_f64(42.0);
        stats.update_f64(f64::NAN);
        let max_f = ordered_i64_to_f64(stats.max_value);
        assert!(
            (max_f - 42.0).abs() < f64::EPSILON,
            "max should be 42.0, got {max_f}"
        );
    }

    #[test]
    fn stats_bool_tracking() {
        let mut stats = ColumnStats::empty();
        stats.update_bool(false);
        stats.update_bool(true);
        stats.update_bool(false);

        assert_eq!(stats.min_value, 0);
        assert_eq!(stats.max_value, 1);
        assert_eq!(stats.value_count, 3);
    }

    #[test]
    fn stats_f64_infinity() {
        let mut stats = ColumnStats::empty();
        stats.update_f64(f64::NEG_INFINITY);
        stats.update_f64(f64::INFINITY);

        let min_f = ordered_i64_to_f64(stats.min_value);
        let max_f = ordered_i64_to_f64(stats.max_value);
        assert!(min_f.is_infinite() && min_f.is_sign_negative());
        assert!(max_f.is_infinite() && max_f.is_sign_positive());
        // Infinities are not finite, should not contribute to sum
        assert!(stats.sum.abs() < f64::EPSILON);
    }

    #[test]
    fn stats_i128_precision_beyond_f64() {
        // i64 values above 2^53 lose precision when cast to f64.
        // sum_i128 must preserve exact precision.
        let mut stats = ColumnStats::empty();
        let big: i64 = (1_i64 << 53) + 1; // 9_007_199_254_740_993
        stats.update_i64(big);
        stats.update_i64(big);

        // f64 sum may have lost precision
        let expected_i128 = 2 * (big as i128);
        assert_eq!(stats.sum_i128, expected_i128);

        // Roundtrip through serialization
        let bytes = stats.to_bytes();
        let recovered = ColumnStats::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.sum_i128, expected_i128);
    }
}

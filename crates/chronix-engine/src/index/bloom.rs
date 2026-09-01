//! Bloom filter for series key membership testing.
//!
//! Per-segment bloom filters enable fast exclusion of segments that definitely
//! do not contain a queried series key. This is the second level of segment
//! pruning (after time-range pruning).
//!
//! # Implementation
//!
//! Uses the Kirsch–Mitzenmacker double-hashing technique: `k` hash functions
//! are derived from two base hashes `h1` and `h2` as `h_i(x) = h1(x) + i * h2(x)`.
//! The base hashes use FNV-1a with different seeds.
//!
//! # Memory
//!
//! At the default 1% false positive rate, each key costs ~10 bits of memory.
//! 10,000 keys ≈ 12 KB.

use chronix_core::SeriesKey;

/// Per-segment bloom filter for series key pruning.
///
/// A space-efficient probabilistic data structure that answers set membership
/// queries with possible false positives but **no false negatives**.
///
/// # False Positive Rate
///
/// The default target FP rate is 1%. At this rate, the filter uses approximately
/// 10 bits per inserted key, with 7 hash functions.
#[derive(Debug, Clone)]
pub struct SeriesBloomFilter {
    /// Bit vector stored as 64-bit words.
    bits: Vec<u64>,
    /// Total number of bits in the filter.
    num_bits: usize,
    /// Number of hash functions to apply.
    num_hashes: u32,
    /// Number of items inserted.
    count: usize,
}

impl SeriesBloomFilter {
    /// Create a new bloom filter sized for the expected number of items.
    ///
    /// # Parameters
    ///
    /// - `expected_items`: Expected number of unique series keys
    /// - `fp_rate`: Target false positive rate (e.g., `0.01` for 1%)
    ///
    /// # Panics
    ///
    /// Panics if `fp_rate` is not in `(0.0, 1.0)` or `expected_items` is 0.
    #[must_use]
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        assert!(expected_items > 0, "expected_items must be > 0");
        assert!(fp_rate > 0.0 && fp_rate < 1.0, "fp_rate must be in (0, 1)");

        #[allow(clippy::cast_precision_loss)]
        let n = expected_items as f64;
        let p = fp_rate;

        // Optimal bit count: m = -n * ln(p) / (ln(2))^2
        let ln2 = std::f64::consts::LN_2;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let m = (-n * p.ln() / (ln2 * ln2)).ceil() as usize;
        let m = m.max(64); // minimum 64 bits

        // Optimal hash count: k = (m/n) * ln(2)
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let k = ((m as f64 / n) * ln2).ceil() as u32;
        // Clamp k to [1, 15] for u8 serialization format.
        // Log when clamping changes the optimal k, as this inflates
        // the actual false positive rate above the requested rate.
        let k_clamped = k.clamp(1, 15);
        if k_clamped != k {
            // Elevated from debug! to warn! so operators notice
            // that the actual FPR exceeds the requested rate.
            tracing::warn!(
                optimal_k = k,
                clamped_k = k_clamped,
                requested_fpr = fp_rate,
                "bloom filter hash count clamped — actual FPR will exceed requested rate"
            );
        }

        // Round up to next multiple of 64
        let num_bits = (m + 63) & !63;
        let num_words = num_bits / 64;

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes: k_clamped,
            count: 0,
        }
    }

    /// Create a bloom filter with explicit parameters (for deserialization).
    #[must_use]
    fn from_raw(bits: Vec<u64>, num_bits: usize, num_hashes: u32, count: usize) -> Self {
        Self {
            bits,
            num_bits,
            num_hashes,
            count,
        }
    }

    /// Insert a series key into the filter.
    ///
    /// Returns `true` if the key was *probably* new (not previously present),
    /// `false` if it was *definitely* already present. The count is only
    /// incremented for new insertions, giving a more accurate FP-rate estimate.
    pub fn insert(&mut self, key: &SeriesKey) -> bool {
        let (h1, h2) = Self::double_hash(key);
        let mut all_set = true;
        for i in 0..self.num_hashes {
            let bit_idx = self.bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;
            if self.bits[word_idx] & (1u64 << bit_pos) == 0 {
                all_set = false;
            }
            self.bits[word_idx] |= 1u64 << bit_pos;
        }
        if !all_set {
            self.count += 1;
        }
        !all_set
    }

    /// Test whether the filter may contain the given series key.
    ///
    /// Returns `false`: the key is **definitely not** in the set.
    /// Returns `true`: the key is **probably** in the set (with FP rate `p`).
    #[must_use]
    pub fn may_contain(&self, key: &SeriesKey) -> bool {
        let (h1, h2) = Self::double_hash(key);
        for i in 0..self.num_hashes {
            let bit_idx = self.bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;
            if self.bits[word_idx] & (1u64 << bit_pos) == 0 {
                return false;
            }
        }
        true
    }

    /// Returns the number of items inserted.
    #[must_use]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Returns the total number of bits in the filter.
    #[must_use]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Returns the number of hash functions.
    #[must_use]
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Serialize the bloom filter to bytes.
    ///
    /// Format: `[num_bits: u32][num_hashes: u8][count: u32][bits...]`
    ///
    /// # Errors
    ///
    /// Returns `IndexError::Corrupt` if `num_bits` or `count` exceeds
    /// `u32::MAX` (bloom filter too large for the v1 wire format).
    pub fn to_bytes(&self) -> std::result::Result<Vec<u8>, crate::index::error::IndexError> {
        let header_size = 4 + 1 + 4; // num_bits + num_hashes + count
        let data_size = self.bits.len() * 8;
        let mut buf = Vec::with_capacity(header_size + data_size);

        let num_bits_u32 =
            u32::try_from(self.num_bits).map_err(|_| crate::index::error::IndexError::Corrupt {
                detail: format!(
                    "bloom filter too large to serialize (num_bits {} > u32::MAX)",
                    self.num_bits
                ),
            })?;
        let count_u32 =
            u32::try_from(self.count).map_err(|_| crate::index::error::IndexError::Corrupt {
                detail: format!(
                    "bloom filter too large to serialize (count {} > u32::MAX)",
                    self.count
                ),
            })?;
        buf.extend_from_slice(&num_bits_u32.to_le_bytes());
        buf.push(self.num_hashes as u8);
        buf.extend_from_slice(&count_u32.to_le_bytes());

        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }

        // Append a 4-byte XOR checksum of the bit data so that
        // `from_bytes` can detect bit-flip corruption at read time.
        let mut checksum: u32 = 0;
        for word in &self.bits {
            let bytes = word.to_le_bytes();
            for chunk in bytes.chunks(4) {
                let v = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                checksum ^= v;
            }
        }
        buf.extend_from_slice(&checksum.to_le_bytes());

        Ok(buf)
    }

    /// Deserialize a bloom filter from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the data is too short or corrupt.
    pub fn from_bytes(data: &[u8]) -> std::result::Result<Self, crate::index::error::IndexError> {
        if data.len() < 9 {
            return Err(crate::index::error::IndexError::Corrupt {
                detail: "bloom filter data too short".into(),
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let num_bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        if num_bits == 0 {
            return Err(crate::index::error::IndexError::Corrupt {
                detail: "bloom filter num_bits must be > 0".into(),
            });
        }

        let num_hashes = u32::from(data[4]);
        #[allow(clippy::cast_possible_truncation)]
        let count = u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;

        let num_words = num_bits.div_ceil(64);
        let expected_data_len = 9 + num_words * 8;

        if data.len() < expected_data_len {
            return Err(crate::index::error::IndexError::Corrupt {
                detail: format!(
                    "bloom filter data too short: need {expected_data_len}, got {}",
                    data.len()
                ),
            });
        }

        let mut bits = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let offset = 9 + i * 8;
            let word = u64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            bits.push(word);
        }

        // Verify XOR checksum if present (4 bytes after bit data).
        let checksum_offset = expected_data_len;
        if data.len() >= checksum_offset + 4 {
            let stored_checksum = u32::from_le_bytes([
                data[checksum_offset],
                data[checksum_offset + 1],
                data[checksum_offset + 2],
                data[checksum_offset + 3],
            ]);
            let mut computed: u32 = 0;
            for word in &bits {
                let bytes = word.to_le_bytes();
                for chunk in bytes.chunks(4) {
                    let v = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    computed ^= v;
                }
            }
            if stored_checksum != computed {
                return Err(crate::index::error::IndexError::Corrupt {
                    detail: format!(
                        "bloom filter checksum mismatch (stored={stored_checksum:#010x}, computed={computed:#010x})"
                    ),
                });
            }
        }

        Ok(Self::from_raw(bits, num_bits, num_hashes, count))
    }

    /// Compute double hash for Kirsch–Mitzenmacker technique.
    ///
    /// Returns `(h1, h2)` where `h1` is FNV-1a and `h2` is FNV-1a with
    /// a different seed.
    fn double_hash(key: &SeriesKey) -> (u64, u64) {
        use std::hash::Hasher;

        let canonical = key.canonical_form();
        let bytes = canonical.as_bytes();

        // h1: FNV-1a (same as SeriesKey::hash_fnv)
        let mut h1 = fnv::FnvHasher::default();
        h1.write(bytes);
        let h1 = h1.finish();

        // h2: FNV-1a with different initial state (XOR the seed)
        let mut h2 = fnv::FnvHasher::with_key(0x517c_c1b7_2722_0a95);
        h2.write(bytes);
        let h2 = h2.finish();

        (h1, h2)
    }

    /// Compute bit index for the i-th hash function.
    #[inline]
    fn bit_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        let combined = h1.wrapping_add((u64::from(i)).wrapping_mul(h2));
        #[allow(clippy::cast_possible_truncation)]
        let idx = (combined as usize) % self.num_bits;
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_key(measurement: &str, host: &str) -> SeriesKey {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        SeriesKey::new(measurement, tags).unwrap()
    }

    #[test]
    fn insert_and_lookup() {
        let mut bf = SeriesBloomFilter::new(100, 0.01);
        let k1 = make_key("cpu", "srv1");
        let k2 = make_key("cpu", "srv2");
        let k3 = make_key("mem", "srv1");

        bf.insert(&k1);
        bf.insert(&k2);

        assert!(bf.may_contain(&k1));
        assert!(bf.may_contain(&k2));
        // k3 was not inserted — should return false (no false negatives)
        assert!(!bf.may_contain(&k3));
    }

    #[test]
    fn no_false_negatives() {
        let mut bf = SeriesBloomFilter::new(1000, 0.01);
        let keys: Vec<SeriesKey> = (0..1000)
            .map(|i| make_key("cpu", &format!("host-{i}")))
            .collect();

        for key in &keys {
            bf.insert(key);
        }

        // Every inserted key must be found
        for key in &keys {
            assert!(bf.may_contain(key), "false negative for key: {key}");
        }
    }

    #[test]
    fn false_positive_rate_within_bounds() {
        let n = 10_000;
        let mut bf = SeriesBloomFilter::new(n, 0.01);

        // Insert n keys
        for i in 0..n {
            bf.insert(&make_key("cpu", &format!("host-{i}")));
        }

        // Test with n non-inserted keys
        let mut false_positives = 0;
        for i in n..(2 * n) {
            if bf.may_contain(&make_key("cpu", &format!("host-{i}"))) {
                false_positives += 1;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let fp_rate = f64::from(false_positives) / n as f64;
        assert!(
            fp_rate < 0.05,
            "false positive rate too high: {fp_rate:.4} (expected < 0.05)"
        );
    }

    #[test]
    fn serialization_roundtrip() {
        let mut bf = SeriesBloomFilter::new(100, 0.01);
        let k1 = make_key("cpu", "srv1");
        let k2 = make_key("cpu", "srv2");
        bf.insert(&k1);
        bf.insert(&k2);

        let bytes = bf.to_bytes().unwrap();
        let bf2 = SeriesBloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(bf2.num_bits(), bf.num_bits());
        assert_eq!(bf2.num_hashes(), bf.num_hashes());
        assert_eq!(bf2.count(), bf.count());
        assert!(bf2.may_contain(&k1));
        assert!(bf2.may_contain(&k2));
        assert!(!bf2.may_contain(&make_key("mem", "srv1")));
    }

    #[test]
    fn from_bytes_too_short() {
        let result = SeriesBloomFilter::from_bytes(&[0u8; 5]);
        assert!(result.is_err());
    }

    #[test]
    fn count_tracking() {
        let mut bf = SeriesBloomFilter::new(100, 0.01);
        assert_eq!(bf.count(), 0);
        assert!(bf.insert(&make_key("cpu", "srv1"))); // new
        assert_eq!(bf.count(), 1);
        assert!(bf.insert(&make_key("cpu", "srv2"))); // new
        assert_eq!(bf.count(), 2);
    }

    #[test]
    fn duplicate_insert_does_not_inflate_count() {
        let mut bf = SeriesBloomFilter::new(100, 0.01);
        let key = make_key("cpu", "srv1");
        assert!(bf.insert(&key)); // new → true
        assert_eq!(bf.count(), 1);
        assert!(!bf.insert(&key)); // dup → false
        assert_eq!(bf.count(), 1);
        assert!(!bf.insert(&key)); // dup → false
        assert_eq!(bf.count(), 1);
    }

    #[test]
    fn memory_approximately_10_bits_per_key() {
        let bf = SeriesBloomFilter::new(10_000, 0.01);
        #[allow(clippy::cast_precision_loss)]
        let bits_per_key = bf.num_bits() as f64 / 10_000.0;
        // Should be approximately 10 bits per key (±2)
        assert!(
            (8.0..=12.0).contains(&bits_per_key),
            "bits_per_key = {bits_per_key:.1}, expected ~10"
        );
    }
}

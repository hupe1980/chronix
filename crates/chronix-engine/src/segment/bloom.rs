//! Bloom filter for tag column filtering.
//!
//! Provides a simple, self-contained bloom filter implementation using
//! double-hashing (two independent FNV-1a hash functions) for tag-value
//! membership testing in segment metadata.  Enables segment-level
//! predicate pushdown: if a queried tag value is definitely not in the
//! bloom filter, the entire segment can be skipped without reading any
//! column blocks.
//!
//! # Design
//!
//! - ~10 bits per element for approximately 1% false positive rate
//! - Double-hashing: `h_i = h1 + i * h2` using two FNV-1a variants
//!   with different offset bases (matching chronix-index approach)
//! - k=7 hash probes (optimal for 10 bits/element)
//! - Serialized as a raw byte array (`Vec<u8>`)

use std::collections::HashSet;

/// FNV-1a hash offset basis (64-bit).
const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
/// Second FNV-1a offset basis for independent double-hashing.
const FNV_OFFSET_2: u64 = 0x517c_c1b7_2722_0a95;
/// FNV-1a hash prime (64-bit).
const FNV_PRIME: u64 = 1_099_511_628_211;

/// Number of hash probes. For m/n ≈ 10 bits/element, k=7 is optimal,
/// yielding a theoretical false positive rate of ~0.8%.
const K: u32 = 7;

/// Compute FNV-1a hash of a byte slice with a given offset basis.
fn fnv1a_seeded(data: &[u8], offset: u64) -> u64 {
    let mut hash = offset;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Build a bloom filter from a set of string values.
///
/// The filter is sized based on the number of **unique** values and the
/// requested false positive rate, using the formula:
///
/// m = -n·ln(p) / (ln 2)²
///
/// where *n* is the number of unique values and *p* is `false_positive_rate`.
///
/// # Arguments
///
/// - `values` — String values to insert into the filter.
/// - `false_positive_rate` — Target FPR (e.g., 0.01 for ~1%).
///
/// # Returns
///
/// Serialized bit array as a `Vec<u8>`. Empty if `values` is empty.
pub fn bloom_filter_build(values: &[&str], false_positive_rate: f64) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    // Deduplicate for optimal sizing
    let unique: HashSet<&str> = values.iter().copied().collect();
    let n = unique.len().max(1);

    // Calculate optimal number of bits
    let m_f = -(n as f64) * false_positive_rate.ln() / (std::f64::consts::LN_2.powi(2));
    let m = (m_f as usize).max(8);
    let m_bytes = m.div_ceil(8);
    let m_bits = (m_bytes * 8) as u64;

    let mut bits = vec![0u8; m_bytes];

    for &val in &unique {
        let h1 = fnv1a_seeded(val.as_bytes(), FNV_OFFSET);
        let h2 = fnv1a_seeded(val.as_bytes(), FNV_OFFSET_2);
        for i in 0..K {
            let idx = h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % m_bits;
            bits[(idx / 8) as usize] |= 1 << (idx % 8);
        }
    }

    bits
}

/// Check if a bloom filter possibly contains a value.
///
/// Returns `true` if the value **might** be in the set (possible false
/// positive), or `false` if the value is **definitely not** in the set
/// (no false negatives).
///
/// # Arguments
///
/// - `filter` — Serialized bloom filter bytes (from [`bloom_filter_build`]).
/// - `value` — The string value to test.
pub fn bloom_filter_contains(filter: &[u8], value: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    let m_bits = (filter.len() * 8) as u64;
    let h1 = fnv1a_seeded(value.as_bytes(), FNV_OFFSET);
    let h2 = fnv1a_seeded(value.as_bytes(), FNV_OFFSET_2);

    for i in 0..K {
        let idx = h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % m_bits;
        if filter[(idx / 8) as usize] & (1 << (idx % 8)) == 0 {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_contains_known_values() {
        let values = vec!["server-1", "server-2", "server-3", "us-east-1", "us-west-2"];
        let filter = bloom_filter_build(&values, 0.01);

        // All inserted values must be found (no false negatives)
        for &v in &values {
            assert!(
                bloom_filter_contains(&filter, v),
                "bloom filter should contain '{v}'"
            );
        }
    }

    #[test]
    fn rejects_unknown_values() {
        // Insert a reasonable number of values
        let owned: Vec<String> = (0..100).map(|i| format!("host-{i}")).collect();
        let values: Vec<&str> = owned.iter().map(String::as_str).collect();
        let filter = bloom_filter_build(&values, 0.01);

        // Test with 1000 values that were NOT inserted.
        // At ~1% FPR, we expect roughly 10 false positives out of 1000 tests.
        let mut false_positives = 0;
        for i in 1000..2000 {
            let test_val = format!("unknown-{i}");
            if bloom_filter_contains(&filter, &test_val) {
                false_positives += 1;
            }
        }

        // Allow up to 5% to account for statistical variance
        assert!(
            false_positives < 50,
            "too many false positives: {false_positives}/1000 (expected <50)"
        );
    }

    #[test]
    fn empty_filter() {
        let filter = bloom_filter_build(&[], 0.01);
        assert!(filter.is_empty());
        assert!(!bloom_filter_contains(&filter, "anything"));
    }

    #[test]
    fn single_value() {
        let filter = bloom_filter_build(&["hello"], 0.01);
        assert!(bloom_filter_contains(&filter, "hello"));
        assert!(!bloom_filter_contains(&filter, "world"));
    }

    #[test]
    fn duplicate_values_handled() {
        let values = vec!["a", "a", "b", "b", "c", "c"];
        let filter = bloom_filter_build(&values, 0.01);
        assert!(bloom_filter_contains(&filter, "a"));
        assert!(bloom_filter_contains(&filter, "b"));
        assert!(bloom_filter_contains(&filter, "c"));
    }
}

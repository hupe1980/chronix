//! Memtable key types for ordering entries in the skip list.
//!
//! The [`MemtableKey`] orders entries by `(series_key_hash, timestamp,
//! series_canonical)` so that all points for the same series are
//! physically adjacent and ordered by time. The canonical form
//! tie-breaker prevents hash collisions from silently overwriting
//! distinct series data.
//!
//! [`MemtableEntry`] uses `Arc<str>` for measurement names, tag keys
//! and tag values so that a [`StringInterner`](super::interner::StringInterner)
//! can de-duplicate the thousands of identical strings that arise when
//! many points share the same series.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Compute a secondary hash (SipHash) of a canonical string for fast Ord comparison.
/// Uses a different algorithm from FNV-1a to minimize correlated collisions.
#[inline]
fn canonical_siphash(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// Key used for ordering entries in the memtable's skip list.
///
/// Points are sorted first by `series_key_hash` (fast grouping), then
/// by `timestamp`, then by `series_canonical_hash2` (fast integer
/// comparison), and finally by `series_canonical` as a rare-case
/// tie-breaker for the astronomically unlikely double-hash collision.
///
/// `series_canonical` is `Arc<str>` (interned) to avoid
/// per-insert heap allocation of duplicate canonical strings.
///
/// `series_canonical_hash2` (SipHash) provides a fast
/// integer comparison path — the byte-by-byte string comparison only
/// fires when both FNV and SipHash collide simultaneously.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MemtableKey {
    /// FNV-1a hash of the canonical series key (fast routing).
    pub series_key_hash: u64,
    /// Nanosecond-precision Unix epoch timestamp.
    pub timestamp: i64,
    /// Secondary SipHash of canonical form for fast Ord.
    series_canonical_hash2: u64,
    /// Interned canonical series key — collision-proof tie-breaker.
    pub series_canonical: Arc<str>,
}

impl Hash for MemtableKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.series_key_hash.hash(state);
        self.timestamp.hash(state);
        self.series_canonical.hash(state);
    }
}

impl MemtableKey {
    /// Create a new memtable key.
    ///
    /// Accepts `Arc<str>` (interned) instead of `String`.
    #[inline]
    #[must_use]
    pub fn new(series_key_hash: u64, timestamp: i64, series_canonical: Arc<str>) -> Self {
        let series_canonical_hash2 = canonical_siphash(&series_canonical);
        Self {
            series_key_hash,
            timestamp,
            series_canonical_hash2,
            series_canonical,
        }
    }

    /// Create a lower-bound key for range scans.
    ///
    /// Uses `u64::MIN` for the secondary hash and an empty string so
    /// this key sorts before all real keys with the same `(hash, ts)`.
    #[inline]
    #[must_use]
    pub fn lower_bound(series_key_hash: u64, timestamp: i64) -> Self {
        Self {
            series_key_hash,
            timestamp,
            series_canonical_hash2: 0,
            series_canonical: Arc::from(""),
        }
    }

    /// Create an upper-bound key for range scans.
    ///
    /// Uses `u64::MAX` for the secondary hash and a max-codepoint string
    /// so this key sorts after all real keys with the same `(hash, ts)`.
    #[inline]
    #[must_use]
    pub fn upper_bound(series_key_hash: u64, timestamp: i64) -> Self {
        Self {
            series_key_hash,
            timestamp,
            series_canonical_hash2: u64::MAX,
            series_canonical: Arc::from("\u{10FFFF}"),
        }
    }
}

impl Ord for MemtableKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.series_key_hash
            .cmp(&other.series_key_hash)
            .then(self.timestamp.cmp(&other.timestamp))
            // Fast integer comparison — string fallback
            // only fires on double-hash collision (probability ~2^-128).
            .then(self.series_canonical_hash2.cmp(&other.series_canonical_hash2))
            .then_with(|| self.series_canonical.cmp(&other.series_canonical))
    }
}

impl PartialOrd for MemtableKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Value stored in the memtable's skip list alongside each key.
///
/// Holds the full field data for a single point, keyed by field name.
/// Measurement names, tag keys, tag values, and field keys are stored
/// as `Arc<str>` so that identical strings across many points share a
/// single heap allocation via the
/// [`StringInterner`](super::interner::StringInterner).
#[derive(Debug, Clone)]
pub struct MemtableEntry {
    /// Measurement name (interned).
    pub measurement: Arc<str>,
    /// Tag key-value pairs (interned).
    pub tags: BTreeMap<Arc<str>, Arc<str>>,
    /// Field name (interned) → field value.
    pub fields: BTreeMap<Arc<str>, chronix_core::FieldValue>,
}

impl MemtableEntry {
    /// Estimated heap size of this entry in bytes.
    #[must_use]
    pub fn estimated_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        size += self.measurement.len();
        for (k, v) in &self.tags {
            size += k.len() + v.len();
        }
        for (k, v) in &self.fields {
            size += k.len();
            size += match v {
                chronix_core::FieldValue::String(s) => s.len(),
                _ => 8,
            };
        }
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ordering_by_hash_first() {
        let k1 = MemtableKey::new(1, 100, Arc::from("a"));
        let k2 = MemtableKey::new(2, 50, Arc::from("a"));
        assert!(k1 < k2, "lower hash should sort first");
    }

    #[test]
    fn key_ordering_by_timestamp_second() {
        let k1 = MemtableKey::new(1, 100, Arc::from("a"));
        let k2 = MemtableKey::new(1, 200, Arc::from("a"));
        assert!(k1 < k2, "same hash → lower timestamp first");
    }

    #[test]
    fn key_ordering_by_canonical_third() {
        let k1 = MemtableKey::new(1, 100, Arc::from("cpu\0host=a"));
        let k2 = MemtableKey::new(1, 100, Arc::from("cpu\0host=b"));
        // With the secondary hash, ordering may differ from pure string order,
        // but the keys must remain distinct (collision-proof).
        assert_ne!(k1, k2, "different canonical forms are distinct keys");
    }

    #[test]
    fn key_equality() {
        let k1 = MemtableKey::new(42, 999, Arc::from("cpu"));
        let k2 = MemtableKey::new(42, 999, Arc::from("cpu"));
        assert_eq!(k1, k2);
    }

    #[test]
    fn hash_collision_does_not_merge_keys() {
        // Simulate two distinct series that produce the same hash
        let k1 = MemtableKey::new(42, 1000, Arc::from("series_a\0tag=1"));
        let k2 = MemtableKey::new(42, 1000, Arc::from("series_b\0tag=2"));
        assert_ne!(
            k1, k2,
            "collision-proof: different canonical forms are different keys"
        );
    }

    #[test]
    fn entry_estimated_size() {
        let entry = MemtableEntry {
            measurement: Arc::from("cpu"),
            tags: [(Arc::from("host"), Arc::from("a"))].into_iter().collect(),
            fields: [(Arc::from("value"), chronix_core::FieldValue::F64(42.0))]
                .into_iter()
                .collect(),
        };
        let size = entry.estimated_size();
        assert!(size > 0);
        // Should include at least struct size + string lengths
        assert!(size >= std::mem::size_of::<MemtableEntry>() + 3 + 5 + 1 + 5);
    }
}

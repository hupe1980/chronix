//! Per-block validity bitmaps (`.csx` v2, D-NULL).
//!
//! Before v2 an absent field was encoded as a type-specific sentinel — `0`,
//! `""` or `false` — and only an aggregate `null_count` survived in the block
//! statistics. A stored zero and an absent field were therefore
//! indistinguishable at read time, so `IS NULL` never matched, `= 0` matched
//! rows that had no value at all, and `AVG`/`MIN`/`COUNT` silently folded
//! sentinels into their results.
//!
//! v2 stores an Arrow-compatible validity bitmap next to each column block:
//! LSB-first packed bits, one per row, **set = the row holds a real value**.
//! Blocks with no nulls store no bitmap at all (`validity_length == 0`), so
//! dense time-series data — the common case — pays nothing.
//!
//! The bitmap is written uncompressed and unencrypted. It leaks only *which*
//! rows have values, never what they are, and keeping it outside the
//! compressed/encrypted payload lets the reader build an Arrow `NullBuffer`
//! with a single copy.

use arrow::buffer::{BooleanBuffer, NullBuffer};

/// Number of bytes needed to hold `len` validity bits.
#[must_use]
pub const fn bitmap_len(len: usize) -> usize {
    len.div_ceil(8)
}

/// Incrementally builds a validity bitmap while a column block is encoded.
#[derive(Debug)]
pub struct ValidityBuilder {
    bits: Vec<u8>,
    len: usize,
    nulls: usize,
}

impl ValidityBuilder {
    /// Create a builder sized for `capacity` rows.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bits: vec![0u8; bitmap_len(capacity)],
            len: 0,
            nulls: 0,
        }
    }

    /// Record one row: `valid == true` means a real value is present.
    pub fn push(&mut self, valid: bool) {
        let byte = self.len / 8;
        if byte >= self.bits.len() {
            self.bits.resize(byte + 1, 0);
        }
        if valid {
            self.bits[byte] |= 1u8 << (self.len % 8);
        } else {
            self.nulls += 1;
        }
        self.len += 1;
    }

    /// Number of rows recorded so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no rows have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of null rows recorded so far.
    #[must_use]
    pub fn null_count(&self) -> usize {
        self.nulls
    }

    /// Finish the bitmap.
    ///
    /// Returns `None` when every row was valid — the overwhelmingly common
    /// case for time-series columns — so that dense blocks store no bitmap.
    #[must_use]
    pub fn finish(mut self) -> Option<Vec<u8>> {
        if self.nulls == 0 {
            return None;
        }
        self.bits.truncate(bitmap_len(self.len));
        Some(self.bits)
    }
}

/// Rebuild an Arrow [`NullBuffer`] from a stored bitmap.
///
/// `bytes` must hold at least [`bitmap_len(len)`](bitmap_len) bytes; extra
/// trailing bytes are ignored. Returns `None` if the bitmap marks every row
/// valid, which lets the caller skip attaching a null buffer entirely.
#[must_use]
pub fn null_buffer_from_bytes(bytes: &[u8], len: usize) -> Option<NullBuffer> {
    if len == 0 || bytes.len() < bitmap_len(len) {
        return None;
    }
    let buffer = BooleanBuffer::new(bytes[..bitmap_len(len)].to_vec().into(), 0, len);
    let nulls = NullBuffer::new(buffer);
    if nulls.null_count() == 0 {
        None
    } else {
        Some(nulls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_block_stores_no_bitmap() {
        let mut b = ValidityBuilder::with_capacity(100);
        for _ in 0..100 {
            b.push(true);
        }
        assert_eq!(b.null_count(), 0);
        assert!(b.finish().is_none());
    }

    #[test]
    fn roundtrips_sparse_pattern() {
        let pattern: Vec<bool> = (0..137).map(|i| i % 3 != 0).collect();
        let mut b = ValidityBuilder::with_capacity(pattern.len());
        for &v in &pattern {
            b.push(v);
        }
        let expected_nulls = pattern.iter().filter(|v| !**v).count();
        assert_eq!(b.null_count(), expected_nulls);

        let bytes = b.finish().expect("has nulls");
        assert_eq!(bytes.len(), bitmap_len(pattern.len()));

        let nulls = null_buffer_from_bytes(&bytes, pattern.len()).expect("has nulls");
        assert_eq!(nulls.null_count(), expected_nulls);
        for (i, &want) in pattern.iter().enumerate() {
            assert_eq!(nulls.is_valid(i), want, "row {i}");
        }
    }

    #[test]
    fn all_null_block() {
        let mut b = ValidityBuilder::with_capacity(9);
        for _ in 0..9 {
            b.push(false);
        }
        let bytes = b.finish().expect("has nulls");
        let nulls = null_buffer_from_bytes(&bytes, 9).expect("has nulls");
        assert_eq!(nulls.null_count(), 9);
    }

    #[test]
    fn truncated_bitmap_is_rejected() {
        assert!(null_buffer_from_bytes(&[0xFF], 100).is_none());
        assert!(null_buffer_from_bytes(&[], 0).is_none());
    }

    #[test]
    fn all_valid_bitmap_yields_no_null_buffer() {
        // A bitmap that happens to mark everything valid needs no NullBuffer.
        assert!(null_buffer_from_bytes(&[0xFF], 8).is_none());
    }

    #[test]
    fn bitmap_len_rounds_up() {
        assert_eq!(bitmap_len(0), 0);
        assert_eq!(bitmap_len(1), 1);
        assert_eq!(bitmap_len(8), 1);
        assert_eq!(bitmap_len(9), 2);
    }
}

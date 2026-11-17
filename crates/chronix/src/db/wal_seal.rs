//! Encrypted column values in the write-ahead log.
//!
//! Per-column encryption covers the segment and every backup taken from it.
//! Without this module the WAL would hold the same values in the clear from
//! the append until the memtable they belong to is flushed — a window an
//! attacker who reads the disk at an arbitrary moment can use.
//!
//! **The shape.** A batch that touches an encrypted column is written as its
//! points *without* their encrypted values, followed by one `Sealed` entry:
//! one AES-256-GCM section per key, each holding `(point index, column,
//! value)` for every value that key protects. One section per key, not per
//! value, so a 28-byte nonce and tag is paid once per batch per key rather
//! than once per `f64` (which would make the WAL five times the size on a
//! flash card), and a holder of one column's key still cannot read another's.
//!
//! **The binding.** Each section's associated data is the record's plain
//! entries, so a section moved into another record fails to authenticate.
//!
//! **Replay** opens every section before inserting a point. A key the
//! process was not given refuses the open by name
//! ([`WalError::MissingKey`](chronix_core::WalError::MissingKey)): replaying
//! the record without the values would acknowledge a write and then lose part
//! of it.

use chronix_core::{FieldValue, Point, SealedSection, WalEntry, WalError};

use crate::error::{DbError, Result};

/// One sealed value: the index of its point in the record, its column, the value.
type SealedValue = (u32, String, FieldValue);

/// The associated data a record's sections are sealed under: its plain
/// entries, length-prefixed.
fn associated_data(plain: &[Vec<u8>]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(32 + plain.iter().map(|p| p.len() + 4).sum::<usize>());
    aad.extend_from_slice(b"chronix-wal-seal-v1");
    for p in plain {
        aad.extend_from_slice(&(p.len() as u32).to_le_bytes());
        aad.extend_from_slice(p);
    }
    aad
}

/// Encode `points` as WAL payloads, sealing every encrypted column's values.
///
/// Returns `None` when no point touches an encrypted column — the ordinary
/// case, which copies nothing.
#[cfg(feature = "field-encryption")]
pub(super) fn seal_batch(
    points: &[&Point],
    encryption: &chronix_engine::segment::field_encryption::FieldEncryptionConfig,
) -> Result<Option<Vec<Vec<u8>>>> {
    use std::collections::BTreeMap;

    if encryption.columns.is_empty()
        || !points
            .iter()
            .any(|p| p.field_keys().any(|k| encryption.is_encrypted(k)))
    {
        return Ok(None);
    }

    // key id → (key bytes, sealed values)
    let mut by_key: BTreeMap<&str, (&[u8; 32], Vec<SealedValue>)> = BTreeMap::new();
    let mut plain = Vec::with_capacity(points.len() + 1);
    for (index, point) in points.iter().enumerate() {
        let mut stripped = (*point).clone();
        let taken = stripped.take_fields(|k| encryption.is_encrypted(k));
        for (column, value) in taken {
            let Some(key) = encryption.key_for(&column) else {
                continue;
            };
            by_key
                .entry(key.key_id.as_str())
                .or_insert_with(|| (key.key_bytes(), Vec::new()))
                .1
                .push((index as u32, column, value));
        }
        plain.push(
            chronix_core::wal_encode_write_point(&stripped)
                .map_err(|e| DbError::Internal(format!("Failed to serialize WAL entry: {e}")))?,
        );
    }

    let aad = associated_data(&plain);
    let mut sections = Vec::with_capacity(by_key.len());
    for (key_id, (key, values)) in by_key {
        let plaintext = postcard::to_stdvec(&values)
            .map_err(|e| DbError::Internal(format!("Failed to serialize sealed values: {e}")))?;
        let sealed = chronix_engine::segment::field_encryption::seal(&plaintext, key, &aad)?;
        sections.push(SealedSection {
            key_id: key_id.to_string(),
            sealed,
        });
    }
    plain.push(
        chronix_core::wal_encode(&WalEntry::Sealed { sections })
            .map_err(|e| DbError::Internal(format!("Failed to serialize WAL entry: {e}")))?,
    );
    Ok(Some(plain))
}

/// The key material replay can open sections with.
#[cfg(feature = "field-encryption")]
pub(super) type Keys<'a> =
    Option<&'a dyn chronix_engine::segment::field_encryption::FieldKeyProvider>;
/// Without the feature there are no keys, and a sealed record is refused.
#[cfg(not(feature = "field-encryption"))]
pub(super) type Keys<'a> = Option<&'a ()>;

/// Decode one record's payloads into the points to replay, opening any sealed
/// section.
///
/// An entry that does not decode is skipped with a warning, as it always has
/// been; a sealed section that cannot be opened refuses the whole open.
pub(super) fn open_record(
    payloads: &[Vec<u8>],
    sequence: u64,
    keys: Keys<'_>,
) -> Result<Vec<Point>> {
    let mut points = Vec::with_capacity(payloads.len());
    let mut sealed = None;
    let mut plain_len = payloads.len();
    for (i, payload) in payloads.iter().enumerate() {
        match chronix_core::wal_decode(payload) {
            Ok(WalEntry::Write { point }) => points.push(point),
            Ok(WalEntry::Sealed { sections }) => {
                sealed = Some(sections);
                plain_len = i;
            }
            Err(e) => {
                tracing::warn!(seq = sequence, error = %e, "Skipping unparseable WAL record");
            }
        }
    }
    let Some(sections) = sealed else {
        return Ok(points);
    };
    let aad = associated_data(&payloads[..plain_len]);
    for section in sections {
        let values = open_section(&section, &aad, sequence, keys)?;
        for (index, column, value) in values {
            if let Some(point) = points.get_mut(index as usize) {
                point.put_field(&column, value);
            }
        }
    }
    Ok(points)
}

#[cfg(feature = "field-encryption")]
fn open_section(
    section: &SealedSection,
    aad: &[u8],
    sequence: u64,
    keys: Keys<'_>,
) -> Result<Vec<SealedValue>> {
    let missing = || {
        DbError::Wal(WalError::MissingKey {
            key_id: section.key_id.clone(),
            sequence,
        })
    };
    let key = keys
        .and_then(|k| k.get_key(&section.key_id))
        .ok_or_else(missing)?;
    let broken = || {
        DbError::Wal(WalError::SealBroken {
            key_id: section.key_id.clone(),
            sequence,
        })
    };
    let plaintext =
        chronix_engine::segment::field_encryption::open_sealed(&section.sealed, &key, aad)
            .map_err(|_| broken())?;
    postcard::from_bytes(&plaintext).map_err(|_| broken())
}

#[cfg(not(feature = "field-encryption"))]
fn open_section(
    section: &SealedSection,
    _aad: &[u8],
    sequence: u64,
    _keys: Keys<'_>,
) -> Result<Vec<SealedValue>> {
    Err(DbError::Wal(WalError::MissingKey {
        key_id: section.key_id.clone(),
        sequence,
    }))
}

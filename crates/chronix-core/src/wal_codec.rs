//! Compact binary codec for WAL entries.
//!
//! All records use the binary v1 format (version-prefixed + `postcard`
//! serialisation of the variant fields). Legacy JSON is **not** supported.
//!
//! ## Wire format (v1)
//!
//! ```text
//! [1 byte  version = 0x01]
//! [1 byte  variant discriminant]
//! [N bytes postcard-encoded variant fields]
//! ```
//!
//! Discriminants:
//! - `0x00` → `WalEntry::Write { point }`
//! - `0x01` → retired (was `Delete`; tombstones are catalog state)
//!
//! Schema changes are not WAL entries: the catalog manifest is their
//! durable record, and it is written before the data that needs them.

use crate::types::{Point, WalEntry};
use std::fmt;

/// Binary version prefix.
const WAL_BINARY_V1: u8 = 0x01;

// Variant discriminants.
const DISC_WRITE: u8 = 0x00;
/// Retired. A `Delete` record carried the tombstones a delete resolved to,
/// and nothing depended on it: the catalog manifest is the durable record and
/// is fsynced first. The number is not reused, so an old log decodes as an
/// unknown discriminant rather than as something else.
const _RETIRED_DISC_DELETE: u8 = 0x01;

/// Errors produced by the WAL codec.
#[derive(Debug)]
pub enum CodecError {
    /// The payload is empty or too short to contain a valid record.
    Truncated,
    /// The version prefix is not recognised (e.g. legacy JSON or corrupt data).
    UnknownVersion(u8),
    /// The binary discriminant byte is not recognised.
    UnknownDiscriminant(u8),
    /// `postcard` serialisation/deserialisation failed.
    Postcard(postcard::Error),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "WAL record truncated"),
            Self::UnknownVersion(v) => write!(f, "unknown WAL version prefix: 0x{v:02x}"),
            Self::UnknownDiscriminant(d) => write!(f, "unknown WAL binary discriminant: 0x{d:02x}"),
            Self::Postcard(e) => write!(f, "WAL postcard error: {e}"),
        }
    }
}

impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postcard(e) => Some(e),
            _ => None,
        }
    }
}

impl From<postcard::Error> for CodecError {
    fn from(e: postcard::Error) -> Self {
        Self::Postcard(e)
    }
}

/// Encode a [`WalEntry`] into the compact binary v1 format.
///
/// The returned `Vec<u8>` can be passed directly to the WAL writer.
/// The encoding is ~5–10× smaller and faster than the previous JSON format.
/// # Errors
///
/// Returns [`CodecError::Postcard`] when postcard serialization fails.
pub fn encode(entry: &WalEntry) -> Result<Vec<u8>, CodecError> {
    // Pre-allocate a reasonable buffer (version byte + discriminant + payload).
    let mut buf = Vec::with_capacity(256);
    buf.push(WAL_BINARY_V1);

    match entry {
        WalEntry::Write { point } => {
            buf.push(DISC_WRITE);
            let data = postcard::to_stdvec(point)?;
            buf.extend_from_slice(&data);
        }
    }

    Ok(buf)
}

/// Encode a single write-point directly without constructing a [`WalEntry`].
///
/// This avoids the `Point::clone()` that would otherwise be needed to build
/// a `WalEntry::Write { point }` variant.  On the hot write path — especially
/// batch inserts — this eliminates one `String` + `Vec` heap allocation per
/// point.
/// # Errors
///
/// Returns [`CodecError::Postcard`] when postcard serialization fails.
pub fn encode_write_point(point: &Point) -> Result<Vec<u8>, CodecError> {
    let mut buf = Vec::with_capacity(256);
    buf.push(WAL_BINARY_V1);
    buf.push(DISC_WRITE);
    let data = postcard::to_stdvec(point)?;
    buf.extend_from_slice(&data);
    Ok(buf)
}

/// Decode a WAL payload into a [`WalEntry`].
///
/// Only binary v1 records (starting with `0x01`) are accepted.
/// Legacy JSON records are rejected with [`CodecError::UnknownVersion`].
/// # Errors
///
/// Returns [`CodecError::Truncated`] on short input,
/// [`CodecError::UnknownVersion`] / [`CodecError::UnknownDiscriminant`] on
/// unrecognised bytes, and a deserialization error on corrupt payloads.
#[allow(clippy::indexing_slicing)] // all accesses guarded by explicit length checks above them
pub fn decode(data: &[u8]) -> Result<WalEntry, CodecError> {
    if data.is_empty() {
        return Err(CodecError::Truncated);
    }

    if data[0] != WAL_BINARY_V1 {
        return Err(CodecError::UnknownVersion(data[0]));
    }

    if data.len() < 2 {
        return Err(CodecError::Truncated);
    }

    let payload = &data[2..];
    match data[1] {
        DISC_WRITE => {
            let point: Point = postcard::from_bytes(payload)?;
            Ok(WalEntry::Write { point })
        }
        d => Err(CodecError::UnknownDiscriminant(d)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldValue, SeriesKey};
    use std::collections::BTreeMap;

    fn sample_point() -> Point {
        let key = SeriesKey::new("cpu", BTreeMap::from([("host".into(), "srv-1".into())])).unwrap();
        let fields = BTreeMap::from([("value".to_string(), FieldValue::F64(42.5))]);
        Point::new(key, fields, 1_000_000_000).unwrap()
    }

    #[test]
    fn a_decimal_field_round_trips_through_the_wal() {
        // postcard carries an `i128` as a varint and the scale as a byte, so
        // the WAL record holds the digits themselves. If this went through
        // an `f64` — as it would if the field were serialised as a number —
        // 0.30000000000000004 would come back as 0.3 and nobody would see it.
        let key = SeriesKey::new("meter", BTreeMap::new()).unwrap();
        let fields = BTreeMap::from([
            (
                "z1nb".to_string(),
                FieldValue::Decimal("1234.5678".parse().unwrap()),
            ),
            (
                "tiny".to_string(),
                FieldValue::Decimal("0.30000000000000004".parse().unwrap()),
            ),
            (
                "widest".to_string(),
                FieldValue::Decimal("99999999999999999999999999999999999999".parse().unwrap()),
            ),
        ]);
        let entry = WalEntry::Write {
            point: Point::new(key, fields, 1).unwrap(),
        };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        assert_eq!(entry, decoded);

        let WalEntry::Write { point } = decoded;
        // Equality alone would pass if both sides had lost the same digits.
        match point.field("z1nb") {
            Some(FieldValue::Decimal(d)) => {
                assert_eq!(d.to_string(), "1234.5678");
                assert_eq!(d.scale(), 4, "the scale travels with the value");
            }
            other => panic!("expected a decimal, got {other:?}"),
        }
        match point.field("tiny") {
            Some(FieldValue::Decimal(d)) => assert_eq!(d.to_string(), "0.30000000000000004"),
            other => panic!("expected a decimal, got {other:?}"),
        }
    }

    #[test]
    fn write_roundtrip() {
        let entry = WalEntry::Write {
            point: sample_point(),
        };
        let encoded = encode(&entry).unwrap();
        assert_eq!(encoded[0], WAL_BINARY_V1);
        assert_eq!(encoded[1], DISC_WRITE);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    /// The retired `Delete` discriminant decodes as unknown, rather than as
    /// something else.
    ///
    /// A number that is reused is how an old log becomes a wrong answer
    /// instead of an error.
    #[test]
    fn the_retired_delete_discriminant_is_not_reused() {
        let old = [WAL_BINARY_V1, 0x01, 0x00];
        assert!(
            matches!(decode(&old), Err(CodecError::UnknownDiscriminant(0x01))),
            "a retired record must fail loudly"
        );
    }

    #[test]
    fn decode_json_rejected_with_unknown_version() {
        // JSON starts with `{` (0x7B) — must be rejected, not silently decoded.
        let json = b"{\"Write\":{}}";
        let err = decode(json).unwrap_err();
        assert!(
            matches!(err, CodecError::UnknownVersion(0x7B)),
            "expected UnknownVersion(0x7B), got {err:?}"
        );
    }

    #[test]
    fn decode_empty_payload_returns_error() {
        assert!(decode(&[]).is_err());
    }

    #[test]
    fn decode_truncated_binary_returns_error() {
        assert!(decode(&[WAL_BINARY_V1]).is_err());
    }

    #[test]
    fn decode_unknown_discriminant_returns_error() {
        assert!(decode(&[WAL_BINARY_V1, 0xFF, 0x00]).is_err());
    }
}

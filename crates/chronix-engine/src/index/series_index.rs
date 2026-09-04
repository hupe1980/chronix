//! Per-segment series index — the distinct series a segment holds,
//! persisted beside it as `<segment>.series`.
//!
//! Three things are derived from it at open, and none of them needs the
//! segment itself to be decoded:
//!
//! - the segment's series bloom filter (rebuilt in memory — a few
//!   milliseconds for a million keys, so it is not stored);
//! - the inverted tag index, from the keys' tag pairs;
//! - the exact set of series the database holds, which is what the
//!   cardinality budget is enforced against.
//!
//! Before this sidecar existed the tag index was rebuilt by decoding every
//! row of every segment at open — O(all data on disk), materialised one
//! whole segment at a time — and the series set was rebuilt from whatever
//! the active WAL file happened to contain, so the cardinality limit reset
//! at every restart.
//!
//! Format: `CXSI`, one version byte, a CRC-32C of the body, and the body —
//! an LZ4 block (size-prepended) of a postcard-encoded record holding the
//! segment's [`SegmentStamp`] and the key list. A sidecar that is missing,
//! fails its checks, or carries another segment's stamp is rebuilt from the
//! segment's tag columns by the caller; it is derived data, never the
//! source of truth.
//!
//! The stamp is what binds a sidecar to *its* segment. Without it a sidecar
//! left beside a different file of the same name — a warm-tier copy, a
//! restored backup — passed every check, and the bloom rebuilt from it then
//! pruned the segment for series the segment did hold: silently missing
//! rows, the one failure a derived index must never produce.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use chronix_core::SeriesKey;

use crate::index::bloom::SeriesBloomFilter;
use crate::index::error::IndexError;

/// File extension of the sidecar.
pub const EXTENSION: &str = "series";
const MAGIC: &[u8; 4] = b"CXSI";
const VERSION: u8 = 2;
/// False-positive rate of the bloom filter rebuilt from the index.
const BLOOM_FPR: f64 = 0.01;

#[derive(Serialize, Deserialize)]
struct Entry {
    measurement: String,
    tags: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    stamp: SegmentStamp,
    entries: Vec<Entry>,
}

/// What identifies the segment file a sidecar describes.
///
/// Taken from the segment header, which is 48 bytes at the start of the
/// file — so checking it costs one small read, not a decode. A rewrite of
/// the file changes at least `created_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentStamp {
    /// The header's creation timestamp.
    pub created_at: i64,
    /// The header's row count.
    pub row_count: u64,
    /// The header's series count.
    pub series_count: u32,
}

impl SegmentStamp {
    /// The stamp of a segment, from its header.
    #[must_use]
    pub fn of(header: &crate::segment::header::SegmentHeader) -> Self {
        Self {
            created_at: header.created_at,
            row_count: header.row_count,
            series_count: header.series_count,
        }
    }

    /// Read the stamp of the segment at `segment_path` — the header only.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the header is invalid.
    pub fn read(segment_path: &Path) -> Result<Self, IndexError> {
        use std::io::Read;
        let mut buf = [0u8; crate::segment::header::HEADER_SIZE];
        let mut file = std::fs::File::open(segment_path)?;
        file.read_exact(&mut buf)?;
        let header = crate::segment::header::SegmentHeader::from_bytes(&buf).map_err(|e| {
            IndexError::Corrupt {
                detail: format!("segment header: {e}"),
            }
        })?;
        Ok(Self::of(&header))
    }
}

/// Path of the sidecar for a segment file.
#[must_use]
pub fn sidecar_path(segment_path: &Path) -> PathBuf {
    segment_path.with_extension(EXTENSION)
}

/// Write the sidecar for `segment_path`, atomically, bound to `stamp`.
///
/// # Errors
///
/// Returns an error if serialisation or the file write fails.
pub fn write(
    segment_path: &Path,
    keys: &[SeriesKey],
    stamp: SegmentStamp,
) -> Result<(), IndexError> {
    let entries: Vec<Entry> = keys
        .iter()
        .map(|k| Entry {
            measurement: k.measurement().to_string(),
            tags: k
                .tags()
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        })
        .collect();
    let raw = postcard::to_stdvec(&Record { stamp, entries })
        .map_err(|e| IndexError::BinarySerialization(e.to_string()))?;
    let body = lz4_flex::compress_prepend_size(&raw);

    let mut buf = Vec::with_capacity(9 + body.len());
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
    buf.extend_from_slice(&body);

    let path = sidecar_path(segment_path);
    let tmp = path.with_extension("series.tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Read the sidecar for `segment_path`, verifying it describes the segment
/// whose stamp is `expected`.
///
/// # Errors
///
/// Returns an error if the sidecar is missing, corrupt, from a newer
/// version, or stamped for a different segment.
pub fn read(segment_path: &Path, expected: SegmentStamp) -> Result<Vec<SeriesKey>, IndexError> {
    let data = std::fs::read(sidecar_path(segment_path))?;
    if data.len() < 9 || &data[..4] != MAGIC {
        return Err(IndexError::Corrupt {
            detail: "series index: bad magic".into(),
        });
    }
    if data[4] != VERSION {
        return Err(IndexError::Corrupt {
            detail: format!("series index: unsupported version {}", data[4]),
        });
    }
    let crc = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
    let body = &data[9..];
    if crc32c::crc32c(body) != crc {
        return Err(IndexError::Corrupt {
            detail: "series index: checksum mismatch".into(),
        });
    }
    let raw = lz4_flex::decompress_size_prepended(body).map_err(|e| IndexError::Corrupt {
        detail: format!("series index: {e}"),
    })?;
    let record: Record =
        postcard::from_bytes(&raw).map_err(|e| IndexError::BinarySerialization(e.to_string()))?;
    if record.stamp != expected {
        return Err(IndexError::Corrupt {
            detail: format!(
                "series index: stamped for another segment ({:?}, expected {expected:?})",
                record.stamp
            ),
        });
    }
    record
        .entries
        .into_iter()
        .map(|e| {
            SeriesKey::new(
                e.measurement,
                e.tags.into_iter().collect::<BTreeMap<_, _>>(),
            )
            .map_err(|err| IndexError::Corrupt {
                detail: format!("series index: invalid key: {err}"),
            })
        })
        .collect()
}

/// Rewrite the sidecar for `segment_path` without the series in `gone`
/// (canonical forms).
///
/// A whole-series delete releases the series from the cardinality budget,
/// and the budget is rebuilt from the sidecars at open — so the sidecars
/// have to forget the series too, or the count comes back at the next
/// restart. The rows themselves stay in the segment, masked by the
/// tombstone, until compaction rewrites it.
///
/// Returns `true` if the sidecar changed.
///
/// # Errors
///
/// Returns an error if the sidecar or the segment header cannot be read or
/// the sidecar cannot be rewritten.
pub fn remove_series(
    segment_path: &Path,
    gone: &std::collections::HashSet<&str>,
) -> Result<bool, IndexError> {
    let stamp = SegmentStamp::read(segment_path)?;
    let keys = read(segment_path, stamp)?;
    let kept: Vec<SeriesKey> = keys
        .iter()
        .filter(|k| !gone.contains(k.canonical_form()))
        .cloned()
        .collect();
    if kept.len() == keys.len() {
        return Ok(false);
    }
    write(segment_path, &kept, stamp)?;
    Ok(true)
}

/// Delete the sidecar for `segment_path`, tolerating its absence.
///
/// # Errors
///
/// Returns any I/O error other than "not found".
pub fn remove(segment_path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(sidecar_path(segment_path)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// The series bloom filter for a key list, or `None` for an empty one.
#[must_use]
pub fn bloom(keys: &[SeriesKey]) -> Option<SeriesBloomFilter> {
    if keys.is_empty() {
        return None;
    }
    let mut bloom = SeriesBloomFilter::new(keys.len(), BLOOM_FPR);
    for key in keys {
        bloom.insert(key);
    }
    Some(bloom)
}

/// The distinct `(tag key, tag value)` pairs of a key list, for the
/// inverted tag index.
#[must_use]
pub fn tag_pairs(keys: &[SeriesKey]) -> Vec<(&str, &str)> {
    let mut pairs: Vec<(&str, &str)> = keys
        .iter()
        .flat_map(|k| k.tags().iter().map(|(a, b)| (a.as_ref(), b.as_ref())))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    pairs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sidecar left beside a different segment is rejected, and a
    /// whole-series delete rewrites the sidecar without the series.
    #[test]
    fn a_sidecar_is_bound_to_its_segment_and_can_forget_a_series() {
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join("s1.csx");
        let keys = vec![key("cpu", &[("host", "a")]), key("cpu", &[("host", "b")])];
        let stamp = SegmentStamp {
            created_at: 10,
            row_count: 2,
            series_count: 2,
        };
        write(&seg, &keys, stamp).unwrap();
        let other = SegmentStamp {
            created_at: 11,
            ..stamp
        };
        assert!(
            matches!(read(&seg, other), Err(IndexError::Corrupt { .. })),
            "a sidecar for another segment must not be trusted"
        );

        // A real header on disk, so `remove_series` can read the stamp.
        let header = crate::segment::header::SegmentHeader {
            version: 2,
            flags: 0,
            created_at: 10,
            min_timestamp: 0,
            max_timestamp: 0,
            row_count: 2,
            column_count: 2,
            series_count: 2,
            compression: 2,
            sort_order: 1,
        };
        std::fs::write(&seg, header.to_bytes()).unwrap();
        assert_eq!(SegmentStamp::read(&seg).unwrap(), stamp);
        let gone: std::collections::HashSet<&str> = [keys[0].canonical_form()].into();
        assert!(remove_series(&seg, &gone).unwrap());
        let back = read(&seg, stamp).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].tag("host"), Some("b"));
        assert!(!remove_series(&seg, &gone).unwrap(), "already gone");
    }

    fn key(m: &str, tags: &[(&str, &str)]) -> SeriesKey {
        SeriesKey::new(
            m,
            tags.iter()
                .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn round_trips_keys_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join("s1.csx");
        let keys = vec![
            key("cpu", &[("host", "a"), ("region", "eu")]),
            key("cpu", &[("host", "b"), ("region", "eu")]),
            key("mem", &[]),
        ];
        let stamp = SegmentStamp {
            created_at: 1,
            row_count: 3,
            series_count: 3,
        };
        write(&seg, &keys, stamp).unwrap();
        let back = read(&seg, stamp).unwrap();
        assert_eq!(back.len(), 3);
        for (a, b) in keys.iter().zip(&back) {
            assert_eq!(a.canonical_form(), b.canonical_form());
        }
        assert_eq!(
            tag_pairs(&back),
            vec![("host", "a"), ("host", "b"), ("region", "eu")]
        );
        let bloom = bloom(&back).unwrap();
        assert!(bloom.may_contain(&keys[0]));
        assert!(!bloom.may_contain(&key("cpu", &[("host", "zzz")])));

        let mut bytes = std::fs::read(sidecar_path(&seg)).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(sidecar_path(&seg), bytes).unwrap();
        assert!(matches!(read(&seg, stamp), Err(IndexError::Corrupt { .. })));

        remove(&seg).unwrap();
        remove(&seg).unwrap();
        assert!(matches!(read(&seg, stamp), Err(IndexError::Io(_))));
    }
}

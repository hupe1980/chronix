//! WAL reader — sequential record reading with CRC verification.

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use chronix_core::WalError;

use crate::wal::{WAL_HEADER_SIZE, WAL_MAGIC, WAL_RECORD_HEADER_SIZE, WAL_VERSION};

/// Maximum payload size per WAL record (256 MB).
///
/// The ceiling, not the bound. A record is also refused when its declared
/// length runs past the end of the file, which is the check that actually
/// matters on the devices this engine targets: 256 MB is a quarter of the
/// gateway's RAM, and a single flipped bit in a length field is enough to ask
/// for it. Bounding an allocation by a constant when the real bound — the
/// bytes that exist — is one `metadata()` call away is the same mistake the
/// block decoders made (D33).
const MAX_PAYLOAD_SIZE: u32 = 256 * 1024 * 1024;

/// A single WAL record as read from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Monotonically increasing sequence number.
    pub sequence_no: u64,
    /// Record type discriminant.
    pub record_type: u8,
    /// Payload serialisation version.
    pub payload_version: u8,
    /// Raw payload bytes.
    pub payload: Vec<u8>,
}

/// Sequential WAL file reader with CRC verification.
///
/// Reads records from a single WAL file. Implements [`Iterator`] for
/// convenient sequential consumption.
pub struct WalReader {
    reader: BufReader<File>,
    path: PathBuf,
    offset: u64,
    finished: bool,
    /// Size of the file when it was opened, used to bound payload allocation.
    file_len: u64,
}

impl WalReader {
    /// Open a WAL file for reading, validating the file header.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::InvalidHeader`] if magic bytes or version are wrong.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Read and validate header
        let mut magic = [0u8; 4];
        reader
            .read_exact(&mut magic)
            .map_err(|e| WalError::InvalidHeader {
                path: path.clone(),
                detail: format!("Failed to read magic bytes: {e}"),
            })?;

        if &magic != WAL_MAGIC {
            return Err(WalError::InvalidHeader {
                path,
                detail: format!("Invalid magic bytes: expected CXWL, got {magic:?}"),
            });
        }

        let mut version_bytes = [0u8; 2];
        reader
            .read_exact(&mut version_bytes)
            .map_err(|e| WalError::InvalidHeader {
                path: path.clone(),
                detail: format!("Failed to read version: {e}"),
            })?;
        let file_version = u16::from_le_bytes(version_bytes);

        if file_version != WAL_VERSION {
            return Err(WalError::InvalidHeader {
                path,
                detail: format!("Unsupported WAL version: {file_version} (expected {WAL_VERSION})"),
            });
        }

        Ok(Self {
            reader,
            path,
            offset: WAL_HEADER_SIZE as u64,
            finished: false,
            file_len,
        })
    }

    /// Read the next record from the WAL file.
    ///
    /// Returns `Ok(None)` at end of file.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Corruption`] if CRC verification fails.
    /// Returns `Ok(None)` with a tracing warning if the record is truncated
    /// (crash during write).
    pub fn read_next(&mut self) -> Result<Option<WalRecord>, WalError> {
        if self.finished {
            return Ok(None);
        }

        // Read CRC (4 bytes)
        let mut crc_bytes = [0u8; 4];
        match self.reader.read_exact(&mut crc_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.finished = true;
                return Ok(None); // Clean EOF
            }
            Err(e) => return Err(WalError::Io(e)),
        }
        let stored_crc = u32::from_le_bytes(crc_bytes);

        // Read length (4 bytes)
        let mut length_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut length_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                tracing::warn!(
                    path = %self.path.display(),
                    offset = self.offset,
                    "Truncated WAL record (incomplete header), skipping"
                );
                self.finished = true;
                return Ok(None);
            }
            return Err(WalError::Io(e));
        }
        let length = u32::from_le_bytes(length_bytes);

        // Read sequence number (8 bytes)
        let mut seq_bytes = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut seq_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                tracing::warn!(
                    path = %self.path.display(),
                    offset = self.offset,
                    "Truncated WAL record (incomplete sequence), skipping"
                );
                self.finished = true;
                return Ok(None);
            }
            return Err(WalError::Io(e));
        }
        let sequence_no = u64::from_le_bytes(seq_bytes);

        // Read record_type and payload_version (2 bytes)
        let mut type_ver = [0u8; 2];
        if let Err(e) = self.reader.read_exact(&mut type_ver) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                tracing::warn!(
                    path = %self.path.display(),
                    offset = self.offset,
                    "Truncated WAL record (incomplete type/version), skipping"
                );
                self.finished = true;
                return Ok(None);
            }
            return Err(WalError::Io(e));
        }
        let (record_type, payload_version) = (type_ver[0], type_ver[1]);

        // Guard against corrupted length fields causing huge allocations.
        if length > MAX_PAYLOAD_SIZE {
            self.finished = true;
            return Err(WalError::Corruption {
                offset: self.offset,
                path: self.path.clone(),
                detail: format!("payload length {length} exceeds maximum of {MAX_PAYLOAD_SIZE}"),
            });
        }

        // The tighter bound: a payload cannot be longer than the bytes left in
        // the file. Without this, a corrupt length reserves up to 256 MB before
        // the read that would have failed — and the CRC that would have caught
        // the corruption is verified *after* the payload is in memory, so it
        // arrives too late to prevent the allocation.
        let remaining = self
            .file_len
            .saturating_sub(self.offset + WAL_RECORD_HEADER_SIZE as u64);
        if u64::from(length) > remaining {
            tracing::warn!(
                path = %self.path.display(),
                offset = self.offset,
                length,
                remaining,
                "Truncated WAL record (payload runs past end of file), skipping"
            );
            self.finished = true;
            return Ok(None);
        }

        // Read payload
        let mut payload = vec![0u8; length as usize];
        if let Err(e) = self.reader.read_exact(&mut payload) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                tracing::warn!(
                    path = %self.path.display(),
                    offset = self.offset,
                    sequence_no = sequence_no,
                    "Truncated WAL record (incomplete payload), skipping"
                );
                self.finished = true;
                return Ok(None);
            }
            return Err(WalError::Io(e));
        }

        // CRC covers [length, seq, record_type, payload_version, payload]
        let computed_crc = {
            let crc = crc32c::crc32c(&length_bytes);
            let crc = crc32c::crc32c_append(crc, &seq_bytes);
            let crc = crc32c::crc32c_append(crc, &[record_type, payload_version]);
            crc32c::crc32c_append(crc, &payload)
        };

        if stored_crc != computed_crc {
            self.offset += WAL_RECORD_HEADER_SIZE as u64 + u64::from(length);
            return Err(WalError::Corruption {
                offset: self.offset - WAL_RECORD_HEADER_SIZE as u64 - u64::from(length),
                path: self.path.clone(),
                detail: format!(
                    "CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
                ),
            });
        }

        self.offset += WAL_RECORD_HEADER_SIZE as u64 + u64::from(length);

        // Transparently decompress LZ4-compressed payloads.
        let payload =
            crate::wal::decompress_wal_payload(payload).map_err(|detail| WalError::Corruption {
                offset: self.offset - WAL_RECORD_HEADER_SIZE as u64 - u64::from(length),
                path: self.path.clone(),
                detail,
            })?;

        Ok(Some(WalRecord {
            sequence_no,
            record_type,
            payload_version,
            payload,
        }))
    }
}

impl Iterator for WalReader {
    type Item = Result<WalRecord, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Replay all WAL files in a directory in sequence order.
///
/// Reads all `wal_*.cxwl` files sorted by their starting sequence number,
/// returning all records in order.
///
/// # Errors
///
/// Returns [`WalError`] if a WAL file cannot be opened (I/O error).
/// Mid-file corruption is tolerated: corrupted records are skipped and
/// replay continues with subsequent records in the same file (and any
/// remaining files) to maximize data recovery.  Unrecoverable
/// corruption (e.g. a garbled length field) terminates replay of that
/// file only.
///
/// **Sequence monotonicity validation**: records whose sequence number is
/// not strictly greater than the previous one are logged as warnings and
/// skipped, preventing corrupted or duplicate entries from silently
/// entering the replay stream.
pub fn replay_all(dir: &Path) -> Result<Vec<WalRecord>, WalError> {
    let files = crate::wal::writer::list_wal_files(dir)?;

    let mut records = Vec::with_capacity(files.len().saturating_mul(128));
    let mut last_seq: u64 = 0;

    for (_, file_path) in &files {
        let reader = WalReader::open(file_path)?;
        for result in reader {
            match result {
                Ok(record) => {
                    if record.sequence_no <= last_seq && last_seq > 0 {
                        tracing::warn!(
                            wal_path = %file_path.display(),
                            expected_gt = last_seq,
                            got = record.sequence_no,
                            "WAL sequence monotonicity violation — skipping record"
                        );
                        continue;
                    }
                    last_seq = record.sequence_no;
                    records.push(record);
                }
                Err(WalError::Corruption {
                    offset,
                    path,
                    detail,
                }) => {
                    // Emit corruption metric so operators
                    // can detect progressive storage degradation.
                    metrics::counter!("chronix_wal_corrupted_records_total").increment(1);
                    tracing::warn!(
                        wal_path = %file_path.display(),
                        corrupt_offset = offset,
                        corrupt_file = %path.display(),
                        detail = %detail,
                        recovered = records.len(),
                        "WAL corruption detected — skipping record, continuing recovery"
                    );
                    continue; // Try to recover subsequent records
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(records)
}

/// Replay WAL records from `start_sequence` (exclusive) up to and including
/// `target_sequence`.
///
/// Only records with `start_sequence < seq <= target_sequence` are returned.
/// Useful for **Point-in-Time Recovery (PITR)** — given a base backup at
/// `start_sequence`, this replays the delta needed to reach `target_sequence`.
///
/// WAL files whose entire range falls outside the window are skipped entirely
/// for efficiency.
///
/// # Errors
///
/// Returns [`WalError`] if a WAL file cannot be opened. Corrupted records
/// are skipped (same tolerance as [`replay_all`]).
pub fn replay_range(
    dir: &Path,
    start_sequence: u64,
    target_sequence: u64,
) -> Result<Vec<WalRecord>, WalError> {
    let files = crate::wal::writer::list_wal_files(dir)?;

    let mut records = Vec::with_capacity(files.len().saturating_mul(64));
    let mut last_seq: u64 = 0;

    for (file_start, file_path) in &files {
        // Skip files that are entirely before our start window.
        // If the next file starts at N, this file contains seqs < N.
        // We can't easily know each file's max seq without reading,
        // but we can skip files whose start is > target_sequence.
        if *file_start > target_sequence {
            break; // Files are sorted — all subsequent files are past target
        }

        let reader = WalReader::open(file_path)?;
        for result in reader {
            match result {
                Ok(record) => {
                    // Past our target — done
                    if record.sequence_no > target_sequence {
                        return Ok(records);
                    }

                    // Before our window — skip
                    if record.sequence_no <= start_sequence {
                        continue;
                    }

                    // Monotonicity check
                    if record.sequence_no <= last_seq && last_seq > 0 {
                        tracing::warn!(
                            wal_path = %file_path.display(),
                            expected_gt = last_seq,
                            got = record.sequence_no,
                            "WAL sequence monotonicity violation — skipping record"
                        );
                        continue;
                    }

                    last_seq = record.sequence_no;
                    records.push(record);
                }
                Err(WalError::Corruption {
                    offset,
                    path,
                    detail,
                }) => {
                    tracing::warn!(
                        wal_path = %file_path.display(),
                        corrupt_offset = offset,
                        corrupt_file = %path.display(),
                        detail = %detail,
                        "WAL corruption — skipping record during range replay"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalWriter;
    use chronix_core::{FsyncPolicy, WalConfig};
    use tempfile::TempDir;

    fn test_config() -> WalConfig {
        WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 32 * 1024 * 1024,
            max_unflushed_wals: 10,
            compress: true,
            ..WalConfig::default()
        }
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        writer.append(b"hello").unwrap();
        writer.append(b"world").unwrap();
        writer.sync().unwrap();

        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence_no, 1);
        assert_eq!(records[0].payload, b"hello");
        assert_eq!(records[1].sequence_no, 2);
        assert_eq!(records[1].payload, b"world");
    }

    #[test]
    fn batch_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        let payloads: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        writer.append_batch(&payloads).unwrap();
        writer.sync().unwrap();

        // Batch is stored as a single atomic WAL record.
        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 1);
        let sub = crate::wal::decode_batch_payload(&records[0].payload).unwrap();
        assert_eq!(sub.len(), 3);
        assert_eq!(sub[0], b"alpha");
        assert_eq!(sub[1], b"beta");
        assert_eq!(sub[2], b"gamma");
    }

    #[test]
    fn read_across_rotated_files() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 100, // Very small file to trigger rotation
            max_unflushed_wals: 20,
            compress: true,
            ..WalConfig::default()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();

        let payload = vec![42u8; 50];
        for _ in 0..8 {
            writer.append(&payload).unwrap();
        }
        writer.sync().unwrap();

        let file_count = writer.file_count().unwrap();
        assert!(file_count >= 2, "Expected rotation, got {file_count} files");

        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 8);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.sequence_no, (i + 1) as u64);
            assert_eq!(record.payload, payload);
        }
    }

    #[test]
    fn crc_corruption_detected() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        writer.append(b"data").unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Find the WAL file and corrupt a byte
        let mut wal_path = None;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "cxwl") {
                wal_path = Some(path);
                break;
            }
        }
        let wal_path = wal_path.unwrap();
        let mut data = std::fs::read(&wal_path).unwrap();
        // Corrupt the payload area (after header + record header)
        let corrupt_offset = WAL_HEADER_SIZE + WAL_RECORD_HEADER_SIZE + 1;
        if corrupt_offset < data.len() {
            data[corrupt_offset] ^= 0xFF;
            std::fs::write(&wal_path, &data).unwrap();
        }

        let result = replay_all(dir.path());
        // After the fix, replay_all tolerates corruption by skipping the
        // rest of the corrupted file and continuing with subsequent files.
        // With only one file and one record (which is corrupt), we get 0
        // records back instead of an error.
        assert!(result.is_ok(), "Expected Ok, got: {result:?}");
        let records = result.unwrap();
        assert_eq!(records.len(), 0, "Corrupt record should be skipped");
    }

    /// The documented guarantee is that a corrupt record is skipped and
    /// replay continues **within the same file**. Existing coverage only
    /// exercised a corrupt first record and corruption across files, so the
    /// in-file resync path was untested.
    #[test]
    fn corruption_mid_file_recovers_later_records_in_the_same_file() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        for i in 0..5u8 {
            writer.append(&[b'r', i]).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        let wal_path = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                (p.extension().is_some_and(|x| x == "cxwl")).then_some(p)
            })
            .next()
            .expect("wal file");

        let clean = replay_all(dir.path()).unwrap();
        assert_eq!(clean.len(), 5, "baseline replay should see all records");

        // Corrupt the payload of the *third* record, leaving its length
        // field intact so the reader can resync.
        let record_stride = WAL_RECORD_HEADER_SIZE + 2;
        let third_payload = WAL_HEADER_SIZE + 2 * record_stride + WAL_RECORD_HEADER_SIZE;
        let mut data = std::fs::read(&wal_path).unwrap();
        assert!(third_payload < data.len(), "layout assumption broken");
        data[third_payload] ^= 0xFF;
        std::fs::write(&wal_path, &data).unwrap();

        let recovered = replay_all(dir.path()).expect("replay should tolerate corruption");
        assert_eq!(
            recovered.len(),
            4,
            "expected the corrupt record to be skipped and the rest recovered, got {}",
            recovered.len()
        );
        // Specifically: the records *after* the corruption must survive.
        let seqs: Vec<u64> = recovered.iter().map(|r| r.sequence_no).collect();
        assert!(
            seqs.len() == 4 && seqs.windows(2).all(|w| w[0] < w[1]),
            "recovered sequences not strictly increasing: {seqs:?}"
        );
    }

    /// Verify that corruption in one WAL file does not prevent recovery
    /// of records from subsequent WAL files.
    #[test]
    fn corruption_does_not_lose_subsequent_files() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 64, // small files → force rotation
            max_unflushed_wals: 100,
            compress: true,
            ..WalConfig::default()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();

        // Write enough records to force multiple WAL files
        for i in 0..10 {
            let payload = format!("record_{i}");
            writer.append(payload.as_bytes()).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        // Find all WAL files and corrupt the first one
        let mut wal_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                if p.extension().is_some_and(|e| e == "cxwl") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        wal_files.sort();

        assert!(
            wal_files.len() >= 2,
            "Need multiple WAL files for this test"
        );

        // Corrupt the first WAL file
        let mut data = std::fs::read(&wal_files[0]).unwrap();
        let corrupt_offset = WAL_HEADER_SIZE + WAL_RECORD_HEADER_SIZE + 1;
        if corrupt_offset < data.len() {
            data[corrupt_offset] ^= 0xFF;
            std::fs::write(&wal_files[0], &data).unwrap();
        }

        // Replay should still recover records from subsequent files
        let records = replay_all(dir.path()).unwrap();
        assert!(
            !records.is_empty(),
            "Should recover records from non-corrupt files"
        );
        // We wrote 10 records; at least 1 was in the corrupt file,
        // so we should get fewer than 10 back, but more than 0.
        assert!(
            records.len() < 10,
            "Should have skipped records in corrupt file"
        );
    }

    #[test]
    fn truncated_tail_tolerance() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        writer.append(b"complete record").unwrap();
        writer.append(b"another complete record").unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Find the WAL file and truncate it in the middle of where a 3rd record would be
        let mut wal_path = None;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "cxwl") {
                wal_path = Some(path);
                break;
            }
        }
        let wal_path = wal_path.unwrap();
        let mut data = std::fs::read(&wal_path).unwrap();
        // Append a partial record header (simulate crash mid-write)
        data.extend_from_slice(&[0u8; 5]); // Incomplete header
        std::fs::write(&wal_path, &data).unwrap();

        // Should recover the two complete records and skip the truncated tail
        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"complete record");
        assert_eq!(records[1].payload, b"another complete record");
    }

    #[test]
    fn crash_recovery_per_batch() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        // Write and sync some records, then "crash" (drop without close)
        {
            let writer = WalWriter::open(&dir_path, test_config()).unwrap();
            let payloads: Vec<&[u8]> = vec![b"one", b"two", b"three"];
            writer.append_batch(&payloads).unwrap();
            // PerBatch: sync happens in append_batch
            // Drop writer without explicit close — simulates crash
        }

        // Replay — the atomic batch record should be recoverable.
        let records = replay_all(&dir_path).unwrap();
        assert_eq!(records.len(), 1);
        let sub = crate::wal::decode_batch_payload(&records[0].payload).unwrap();
        assert_eq!(sub.len(), 3);
        assert_eq!(sub[0], b"one");
        assert_eq!(sub[1], b"two");
        assert_eq!(sub[2], b"three");
    }

    #[test]
    fn crash_recovery_per_write() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        {
            let config = WalConfig {
                fsync_policy: FsyncPolicy::PerWrite,
                max_file_size: 32 * 1024 * 1024,
                max_unflushed_wals: 10,
                compress: true,
                ..WalConfig::default()
            };
            let writer = WalWriter::open(&dir_path, config).unwrap();
            writer.append(b"A").unwrap();
            writer.append(b"B").unwrap();
            writer.append(b"C").unwrap();
            // Drop = crash simulation
        }

        let records = replay_all(&dir_path).unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn crash_recovery_across_rotation() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        {
            let config = WalConfig {
                fsync_policy: FsyncPolicy::PerBatch,
                max_file_size: 80,
                max_unflushed_wals: 20,
                compress: true,
                ..WalConfig::default()
            };
            let writer = WalWriter::open(&dir_path, config).unwrap();
            let payload = vec![0u8; 40];
            for _ in 0..6 {
                writer.append(&payload).unwrap();
            }
            // Crash simulation
        }

        let records = replay_all(&dir_path).unwrap();
        assert_eq!(records.len(), 6);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.sequence_no, (i + 1) as u64);
        }
    }

    #[test]
    fn invalid_header_magic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.cxwl");
        std::fs::write(&path, b"BADM\x01\x00").unwrap();

        let result = WalReader::open(&path);
        assert!(matches!(result, Err(WalError::InvalidHeader { .. })));
    }

    #[test]
    fn invalid_header_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.cxwl");
        let mut data = Vec::new();
        data.extend_from_slice(WAL_MAGIC);
        data.extend_from_slice(&99u16.to_le_bytes()); // Bad version
        std::fs::write(&path, &data).unwrap();

        let result = WalReader::open(&path);
        assert!(matches!(result, Err(WalError::InvalidHeader { .. })));
    }

    #[test]
    fn replay_empty_directory() {
        let dir = TempDir::new().unwrap();
        let records = replay_all(dir.path()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn replay_nonexistent_directory() {
        let records = replay_all(Path::new("/nonexistent/path/surely")).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn large_payload() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        let payload = vec![0xAB; 1_000_000]; // 1 MB
        writer.append(&payload).unwrap();
        writer.sync().unwrap();

        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload.len(), 1_000_000);
        assert!(records[0].payload.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn sequence_continuity() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 100,
            max_unflushed_wals: 20,
            compress: true,
            ..WalConfig::default()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();

        let payload = vec![0u8; 40];
        for _ in 0..20 {
            writer.append(&payload).unwrap();
        }
        writer.sync().unwrap();

        let records = replay_all(dir.path()).unwrap();
        for (i, record) in records.iter().enumerate() {
            assert_eq!(
                record.sequence_no,
                (i + 1) as u64,
                "Sequence gap at index {i}"
            );
        }
    }

    #[test]
    fn empty_wal_file_header_only() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        // Drop writer immediately — file has only the 6-byte header.
        drop(writer);

        let records = replay_all(dir.path()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn empty_payload_roundtrip() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        writer.append(b"").unwrap();
        writer.sync().unwrap();

        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].payload.is_empty());
    }

    #[test]
    fn iterator_error_propagation() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        writer.append(b"good").unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Corrupt the CRC of the first record
        let mut wal_path = None;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "cxwl") {
                wal_path = Some(path);
                break;
            }
        }
        let wal_path = wal_path.unwrap();
        let mut data = std::fs::read(&wal_path).unwrap();
        // Flip a bit in the CRC field (bytes 6..10)
        data[6] ^= 0xFF;
        std::fs::write(&wal_path, &data).unwrap();

        let reader = WalReader::open(&wal_path).unwrap();
        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[test]
    fn single_element_batch() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();
        let payloads: Vec<&[u8]> = vec![b"only_one"];
        let last_seq = writer.append_batch(&payloads).unwrap();
        assert_eq!(last_seq, 1);
        writer.sync().unwrap();

        let records = replay_all(dir.path()).unwrap();
        assert_eq!(records.len(), 1);
        // Single-element batch is still framed as a batch record.
        let sub = crate::wal::decode_batch_payload(&records[0].payload).unwrap();
        assert_eq!(sub.len(), 1);
        assert_eq!(sub[0], b"only_one");
    }

    #[test]
    fn replay_range_basic() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        for i in 0..10 {
            writer.append(format!("rec_{i}").as_bytes()).unwrap();
        }
        writer.sync().unwrap();

        // Replay records 4..=7 (seqs 4, 5, 6, 7)
        let records = replay_range(dir.path(), 3, 7).unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].sequence_no, 4);
        assert_eq!(records[3].sequence_no, 7);
    }

    #[test]
    fn replay_range_from_start() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        for i in 0..5 {
            writer.append(format!("rec_{i}").as_bytes()).unwrap();
        }
        writer.sync().unwrap();

        // Replay from beginning (start=0) up to seq 3
        let records = replay_range(dir.path(), 0, 3).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].sequence_no, 1);
        assert_eq!(records[2].sequence_no, 3);
    }

    #[test]
    fn replay_range_full() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        for i in 0..5 {
            writer.append(format!("rec_{i}").as_bytes()).unwrap();
        }
        writer.sync().unwrap();

        // Replay everything
        let records = replay_range(dir.path(), 0, u64::MAX).unwrap();
        assert_eq!(records.len(), 5);
    }

    #[test]
    fn replay_range_empty_window() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path(), test_config()).unwrap();

        for i in 0..5 {
            writer.append(format!("rec_{i}").as_bytes()).unwrap();
        }
        writer.sync().unwrap();

        // Window past all records
        let records = replay_range(dir.path(), 10, 20).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn replay_range_across_rotated_files() {
        let dir = TempDir::new().unwrap();
        let config = WalConfig {
            fsync_policy: FsyncPolicy::PerBatch,
            max_file_size: 100,
            max_unflushed_wals: 20,
            compress: true,
            ..WalConfig::default()
        };
        let writer = WalWriter::open(dir.path(), config).unwrap();

        let payload = vec![42u8; 50];
        for _ in 0..8 {
            writer.append(&payload).unwrap();
        }
        writer.sync().unwrap();

        let file_count = writer.file_count().unwrap();
        assert!(file_count >= 2, "Expected rotation, got {file_count} files");

        // Replay only middle records
        let records = replay_range(dir.path(), 2, 6).unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].sequence_no, 3);
        assert_eq!(records[3].sequence_no, 6);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::wal::WalWriter;
    use chronix_core::{FsyncPolicy, WalConfig};
    use proptest::prelude::*;
    use tempfile::TempDir;

    proptest! {
        #[test]
        fn arbitrary_payloads_roundtrip(
            payloads in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..512),
                1..20
            )
        ) {
            let dir = TempDir::new().unwrap();
            let config = WalConfig {
                fsync_policy: FsyncPolicy::PerBatch,
                max_file_size: 32 * 1024 * 1024,
                max_unflushed_wals: 100,
                compress: true,
                ..WalConfig::default()
            };
            let writer = WalWriter::open(dir.path(), config).unwrap();

            for payload in &payloads {
                writer.append(payload).unwrap();
            }
            writer.sync().unwrap();

            let records = replay_all(dir.path()).unwrap();
            prop_assert_eq!(records.len(), payloads.len());
            for (i, (record, payload)) in records.iter().zip(payloads.iter()).enumerate() {
                prop_assert_eq!(record.sequence_no, (i + 1) as u64);
                prop_assert_eq!(&record.payload, payload);
            }
        }

        #[test]
        fn batch_arbitrary_roundtrip(
            payloads in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 1..256),
                1..10
            )
        ) {
            let dir = TempDir::new().unwrap();
            let config = WalConfig {
                fsync_policy: FsyncPolicy::PerBatch,
                max_file_size: 32 * 1024 * 1024,
                max_unflushed_wals: 100,
                compress: true,
                ..WalConfig::default()
            };
            let writer = WalWriter::open(dir.path(), config).unwrap();

            let refs: Vec<&[u8]> = payloads.iter().map(std::vec::Vec::as_slice).collect();
            writer.append_batch(&refs).unwrap();
            writer.sync().unwrap();

            // Batch is stored as a single atomic WAL record.
            let records = replay_all(dir.path()).unwrap();
            prop_assert_eq!(records.len(), 1);
            let decoded = crate::wal::decode_batch_payload(&records[0].payload)
                .expect("batch decode should succeed");
            prop_assert_eq!(decoded.len(), payloads.len());
            for (sub, payload) in decoded.iter().zip(payloads.iter()) {
                prop_assert_eq!(sub, payload);
            }
        }
    }
}

//! # chronix-segment
//!
//! Columnar segment file format (`.csx`) for the Chronix time-series database.
//!
//! Segment files are immutable, columnar data files that store time-series data
//! after it has been flushed from the memtable. Each segment contains:
//!
//! - A **header** with file metadata (version, row count, time range).
//! - One or more **row groups**, each containing encoded column blocks.
//! - **Column metadata** enabling selective column reads.
//! - A **footer** with `CRC32c` integrity checksum.

#![warn(missing_docs)]

pub mod bloom;
pub mod compression;
pub mod error;
#[cfg(feature = "field-encryption")]
pub mod field_encryption;
pub mod header;
pub mod metadata;
pub mod reader;
pub mod stats;
pub mod validity;
pub mod writer;

/// Convert a byte slice to a fixed-size array without panicking.
///
/// Returns `SegmentError::CorruptFile` if the slice length doesn't match.
macro_rules! to_array {
    ($slice:expr, $ctx:expr) => {
        $slice
            .try_into()
            .map_err(|_| $crate::segment::error::SegmentError::CorruptFile {
                detail: format!("invalid byte slice length in {}", $ctx),
            })
    };
}
pub(crate) use to_array;

pub use error::SegmentError;
#[cfg(feature = "field-encryption")]
pub use field_encryption::{
    FieldEncryptionConfig, FieldEncryptionKey, FieldKeyProvider, StaticKeyProvider,
};
pub use header::{SegmentFooter, SegmentHeader};
pub use metadata::{ColumnBlockMeta, ColumnMeta, SegmentMetadata};
pub use reader::SegmentReader;
pub use reader::{FieldPredicate, ZoneMapOp};
pub use stats::ColumnStats;
pub use validity::{null_buffer_from_bytes, ValidityBuilder};
pub use writer::{SegmentMeta, SegmentWriter, SegmentWriterConfig};

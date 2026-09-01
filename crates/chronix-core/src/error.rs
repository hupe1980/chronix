//! Error types for the Chronix time-series database.
//!
//! Errors are organized hierarchically: each crate defines its own error type,
//! and [`ChronixError`] composes them all via `#[from]` conversions.

use std::path::PathBuf;

use thiserror::Error;

/// Top-level error type for the Chronix database.
///
/// All crate-level errors are composable into this type via `#[from]`.
#[derive(Debug, Error)]
pub enum ChronixError {
    /// WAL (Write-Ahead Log) errors.
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// Schema validation and evolution errors.
    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),

    /// Configuration errors.
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    /// Capacity exceeded (backpressure).
    #[error("Capacity exceeded: {0}")]
    Capacity(String),

    /// I/O errors.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Write-Ahead Log errors.
#[derive(Debug, Error)]
pub enum WalError {
    /// Underlying I/O error.
    #[error("WAL I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data corruption detected (CRC mismatch).
    #[error("WAL corruption at offset {offset} in {path}: {detail}")]
    Corruption {
        /// Byte offset in the WAL file where corruption was detected.
        offset: u64,
        /// Path to the corrupted WAL file.
        path: PathBuf,
        /// Human-readable detail about the corruption.
        detail: String,
    },

    /// Sequence number discontinuity detected during replay.
    #[error("WAL sequence skip: expected {expected}, got {got}")]
    SequenceSkip {
        /// The expected next sequence number.
        expected: u64,
        /// The actual sequence number encountered.
        got: u64,
    },

    /// WAL is full — too many unflushed WAL files.
    #[error("WAL full: {count} unflushed files exceed limit of {limit}")]
    Full {
        /// Current number of unflushed WAL files.
        count: usize,
        /// Maximum allowed unflushed WAL files.
        limit: usize,
    },

    /// Invalid WAL file header (wrong magic bytes or unsupported version).
    #[error("Invalid WAL header in {path}: {detail}")]
    InvalidHeader {
        /// Path to the WAL file with the invalid header.
        path: PathBuf,
        /// Description of what's wrong with the header.
        detail: String,
    },

    /// Payload exceeds the maximum allowed size per WAL record.
    #[error("WAL payload too large: {size} bytes exceeds limit of {limit}")]
    PayloadTooLarge {
        /// Actual payload size in bytes.
        size: usize,
        /// Maximum allowed payload size in bytes.
        limit: usize,
    },

    /// Internal lock was poisoned (indicates a prior panic while holding the lock).
    #[error("WAL lock poisoned: {0}")]
    LockPoisoned(String),

    /// WAL writer is poisoned after a previous unrecoverable I/O error
    ///. Call `clear_poison()` after verifying WAL integrity to
    /// resume writes.
    #[error("WAL writer is poisoned — call clear_poison() to resume")]
    Poisoned,
}

/// Schema validation and evolution errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// Name is empty.
    #[error("{label} must not be empty")]
    EmptyName {
        /// Which entity had the empty name (e.g., "measurement name", "tag key").
        label: String,
    },

    /// Name exceeds maximum length.
    #[error("{label} too long: {length} bytes exceeds maximum of {max}")]
    NameTooLong {
        /// Which entity exceeded the limit.
        label: String,
        /// Actual byte length.
        length: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Name contains null byte.
    #[error("{label} contains null byte (\\0)")]
    NullByte {
        /// Which entity contained the null byte.
        label: String,
    },

    /// FINDING-06: Name/value contains a character that would break
    /// the canonical form encoding (e.g. `=` in tag values).
    #[error("{label} contains invalid character '{ch}' in \"{value}\"")]
    InvalidCharacter {
        /// Which entity contained the invalid character.
        label: String,
        /// The forbidden character.
        ch: char,
        /// The offending value for diagnostic context.
        value: String,
    },

    /// Too many tags on a series key.
    #[error("Too many tags: {count} exceeds maximum of {max}")]
    TooManyTags {
        /// Actual tag count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// Point has no fields.
    #[error("Point must have at least one field")]
    EmptyFields,

    /// Too many fields on a single point.
    #[error("Too many fields: {count} exceeds maximum of {max}")]
    TooManyFields {
        /// Actual field count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// A field value failed validation (e.g., non-finite f64, oversized string).
    #[error("Invalid value for field '{field}': {reason}")]
    InvalidFieldValue {
        /// Field name with the invalid value.
        field: String,
        /// Why the value is invalid.
        reason: String,
    },

    /// Type conflict: a field already exists with a different type.
    #[error("Type conflict for field '{field}' in measurement '{measurement}': expected {expected}, got {got}")]
    TypeConflict {
        /// Measurement name where the conflict occurred.
        measurement: String,
        /// Field name with the type conflict.
        field: String,
        /// The existing (expected) type.
        expected: String,
        /// The conflicting (received) type.
        got: String,
    },

    /// Measurement not found.
    #[error("Measurement '{0}' not found")]
    MeasurementNotFound(String),

    /// Internal lock was poisoned (indicates a prior panic while holding the lock).
    #[error("Schema lock poisoned: {0}")]
    LockPoisoned(String),

    /// An entity name failed validation.
    #[error("Invalid name '{name}': {reason}")]
    InvalidName {
        /// The invalid name.
        name: String,
        /// Why the name is invalid.
        reason: String,
    },
}

/// Configuration errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required field was not set.
    #[error("missing required field: {field}")]
    MissingField {
        /// Name of the missing field.
        field: &'static str,
    },

    /// A configuration value failed validation.
    #[error("invalid configuration: {message}")]
    Validation {
        /// Human-readable validation failure description.
        message: String,
    },

    /// Failed to read the configuration file.
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to parse TOML content.
    #[error("failed to parse TOML: {0}")]
    Parse(#[from] toml::de::Error),
}

/// A specialized `Result` type for Chronix operations.
pub type Result<T> = std::result::Result<T, ChronixError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chronix_error_from_wal() {
        let wal_err = WalError::Full { count: 5, limit: 4 };
        let err: ChronixError = wal_err.into();
        let msg = format!("{err}");
        assert!(msg.contains("WAL"));
        assert!(msg.contains('5'));
    }

    #[test]
    fn chronix_error_from_schema() {
        let schema_err = SchemaError::EmptyFields;
        let err: ChronixError = schema_err.into();
        assert!(format!("{err}").contains("field"));
    }

    #[test]
    fn chronix_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err: ChronixError = io_err.into();
        assert!(format!("{err}").contains("gone"));
    }

    #[test]
    fn schema_error_type_conflict_message() {
        let err = SchemaError::TypeConflict {
            measurement: "cpu".to_string(),
            field: "value".to_string(),
            expected: "f64".to_string(),
            got: "i64".to_string(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("value"));
        assert!(msg.contains("f64"));
        assert!(msg.contains("i64"));
    }

    #[test]
    fn wal_error_corruption_message() {
        let err = WalError::Corruption {
            offset: 1024,
            path: PathBuf::from("/data/wal_1.cxwl"),
            detail: "CRC mismatch".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("1024"));
        assert!(msg.contains("CRC"));
    }

    #[test]
    fn wal_error_sequence_skip_message() {
        let err = WalError::SequenceSkip {
            expected: 5,
            got: 8,
        };
        let msg = err.to_string();
        assert!(msg.contains('5'));
        assert!(msg.contains('8'));
    }

    #[test]
    fn wal_error_invalid_header_message() {
        let err = WalError::InvalidHeader {
            path: PathBuf::from("/data/wal.cxwl"),
            detail: "bad magic".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("bad magic"));
    }

    #[test]
    fn wal_error_lock_poisoned_message() {
        let err = WalError::LockPoisoned("inner mutex".into());
        assert!(err.to_string().contains("inner mutex"));
    }

    #[test]
    fn wal_error_io_message() {
        let err = WalError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "read-only",
        ));
        assert!(err.to_string().contains("read-only"));
    }

    #[test]
    fn wal_error_full_message() {
        let err = WalError::Full {
            count: 10,
            limit: 4,
        };
        let msg = err.to_string();
        assert!(msg.contains("10"));
        assert!(msg.contains('4'));
    }

    #[test]
    fn wal_error_payload_too_large_message() {
        let err = WalError::PayloadTooLarge {
            size: 512_000_000,
            limit: 268_435_456,
        };
        let msg = err.to_string();
        assert!(msg.contains("512000000"));
        assert!(msg.contains("268435456"));
    }

    #[test]
    fn schema_error_lock_poisoned_message() {
        let err = SchemaError::LockPoisoned("registry lock".into());
        assert!(err.to_string().contains("registry lock"));
    }

    #[test]
    fn schema_error_measurement_not_found_message() {
        let err = SchemaError::MeasurementNotFound("disk".into());
        assert!(err.to_string().contains("disk"));
    }

    #[test]
    fn config_error_missing_field_message() {
        let err = ConfigError::MissingField { field: "data_dir" };
        assert!(err.to_string().contains("data_dir"));
    }

    #[test]
    fn config_error_validation_message() {
        let err = ConfigError::Validation {
            message: "too small".into(),
        };
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn chronix_error_from_config() {
        let config_err = ConfigError::MissingField { field: "data_dir" };
        let err: ChronixError = config_err.into();
        assert!(err.to_string().contains("data_dir"));
    }
}

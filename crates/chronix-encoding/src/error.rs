//! Encoding error types.

use thiserror::Error;

/// Errors that can occur during encoding or decoding.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EncodingError {
    /// The input data is empty.
    #[error("empty input: {context}")]
    EmptyInput {
        /// Description of which encoder received empty input.
        context: &'static str,
    },

    /// The encoded data is truncated or malformed.
    #[error("corrupt encoded data: {detail}")]
    CorruptData {
        /// Human-readable description of the corruption.
        detail: String,
    },

    /// An unsupported encoding type was encountered.
    #[error("unsupported encoding type tag: {tag}")]
    UnsupportedEncoding {
        /// The unknown encoding type discriminant.
        tag: u8,
    },

    /// Value count mismatch between null bitmap and data.
    #[error("value count mismatch: expected {expected}, got {actual}")]
    CountMismatch {
        /// Expected number of values.
        expected: usize,
        /// Actual number of values.
        actual: usize,
    },

    /// Dictionary overflow — too many unique values.
    #[error("dictionary overflow: {count} unique values exceed limit of {limit}")]
    DictionaryOverflow {
        /// Actual number of unique values.
        count: usize,
        /// Maximum allowed unique values.
        limit: usize,
    },
}

/// A specialized `Result` type for encoding operations.
pub type Result<T> = std::result::Result<T, EncodingError>;

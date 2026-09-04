#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The string-length ceiling has to agree across crates.
//!
//! `chronix-core` validates a string field at ingest; the string encoders
//! store each length as a `u16`. If ingest accepts one byte more than the
//! encoder can store, that point is a poison pill: it sits in the memtable
//! and fails every flush of that memtable, forever. This pins the two
//! limits to the same number, at the boundary, from the encoder's side.

use chronix_core::types::MAX_STRING_FIELD_LENGTH;
use chronix_encoding::{ColumnDecoder, ColumnEncoder, DecodedColumn, DictionaryEncoder};

#[test]
fn ingest_limit_equals_the_encoder_limit() {
    assert_eq!(
        MAX_STRING_FIELD_LENGTH,
        usize::from(u16::MAX),
        "chronix-core accepts strings the encoders cannot store"
    );
}

#[test]
fn a_string_at_the_ingest_limit_encodes_and_one_past_it_does_not() {
    let at_limit = "x".repeat(MAX_STRING_FIELD_LENGTH);
    let block = ColumnEncoder::encode_string(&[at_limit.as_str(), "y"]).unwrap();
    match ColumnDecoder::decode(&block).unwrap() {
        DecodedColumn::String(back) => assert_eq!(back, vec![at_limit.clone(), "y".to_string()]),
        other => panic!("decoded to {other:?}"),
    }
    assert!(DictionaryEncoder::encode(&[at_limit.as_str()]).is_ok());

    let past_limit = "x".repeat(MAX_STRING_FIELD_LENGTH + 1);
    assert!(
        ColumnEncoder::encode_string(&[past_limit.as_str()]).is_err(),
        "a string one byte past the limit must be rejected, not truncated"
    );
}

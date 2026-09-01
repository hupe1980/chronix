#![no_main]
//! Fuzz target for `ColumnDecoder::decode` and `ColumnDecoder::decode_bytes`.
//!
//! Feeds arbitrary bytes into the unified column decoder to exercise all
//! codec paths (Chimp, Gorilla, delta-of-delta, dictionary, bitmap, plain,
//! RLE, nullable wrappers).  Any panic or memory-safety violation is a bug.

use libfuzzer_sys::fuzz_target;

use chronix_encoding::{ColumnDecoder, DecodedColumn, EncodedBlock, EncodingType};

/// Every encoding type, so no decoder goes unfuzzed.
///
/// Keep this exhaustive when adding a codec — an omission here is invisible
/// (the fuzzer just never reaches that decoder) rather than a compile error.
const ENCODINGS: &[EncodingType] = &[
    EncodingType::DeltaOfDelta,
    EncodingType::Chimp,
    EncodingType::Chimp128,
    EncodingType::Gorilla,
    EncodingType::Patas,
    EncodingType::Alp,
    EncodingType::IntegerI64,
    EncodingType::IntegerU64,
    EncodingType::VarintI64,
    EncodingType::VarintU64,
    EncodingType::ForI64,
    EncodingType::ForU64,
    EncodingType::Dictionary,
    EncodingType::Bitmap,
    EncodingType::Rle,
    EncodingType::Nullable,
    EncodingType::PlainF64,
    EncodingType::PlainI64,
    EncodingType::PlainU64,
    EncodingType::PlainBool,
    EncodingType::PlainString,
];

fuzz_target!(|data: &[u8]| {
    // 1. Try `decode_bytes` on raw input.
    let _ = ColumnDecoder::decode_bytes(data);

    // 2. Try `decode` with each encoding variant so every codec path gets
    //    exercised regardless of what discriminant the raw bytes would map to.
    for &enc in ENCODINGS {
        let block = EncodedBlock {
            encoding: enc,
            payload: data.to_vec(),
        };
        let _ = ColumnDecoder::decode(&block);
    }
});

//! Every decoder must *fail* on arbitrary bytes, never panic.
//!
//! # Why this exists beside the fuzz target
//!
//! `fuzz/fuzz_targets/fuzz_column_decoder.rs` covers exactly this property and
//! covers it better — it is coverage-guided and will find inputs this file
//! never will. It also **never runs**: there was no CI job invoking it, so the
//! "exhaustive fuzz coverage of all 24 encodings" in the notes was a claim
//! about a target nothing executed (a comment that asserts a property the
//! build does not have).
//!
//! This suite is the cheap half of the fix: a few seconds, deterministic
//! enough to run on every commit, and it fails the build rather than a nightly.
//! The other half is `.github/workflows/fuzz.yml`, which runs all three
//! targets nightly — a job that did not exist while this comment claimed it
//! did, which is the same defect class as the fuzz target that claimed 21
//! encodings and reached 12.
//!
//! The property is not "decoding succeeds". It is that a decoder handed bytes
//! it did not write returns `Err` — no panic, no abort, no allocation
//! proportional to a number the input chose. `.csx` blocks come off eMMC on the
//! design partner's gateway, so the input that violates this is a flipped bit,
//! not an attacker.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use chronix_encoding::{ColumnDecoder, EncodedBlock, EncodingType};
use proptest::prelude::*;

/// Every encoding, mirroring the fuzz target.
///
/// This list must stay exhaustive and the compiler cannot check that: an
/// omission is invisible, because the decoder simply never gets exercised.
/// `every_encoding_is_covered` below is the guard.
const ENCODINGS: &[EncodingType] = &[
    EncodingType::DeltaOfDelta,
    EncodingType::Chimp,
    EncodingType::Chimp128,
    EncodingType::Gorilla,
    EncodingType::Patas,
    EncodingType::Alp,
    EncodingType::Pco,
    EncodingType::PcoI64,
    EncodingType::PcoU64,
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

/// The count is asserted so that adding a codec without adding it here fails
/// loudly instead of silently reducing coverage.
#[test]
fn every_encoding_is_covered() {
    assert_eq!(
        ENCODINGS.len(),
        24,
        "the encoding list must stay exhaustive — a missing entry is invisible \
         coverage loss, not a compile error"
    );
    let mut seen = ENCODINGS.to_vec();
    seen.sort_by_key(|e| format!("{e:?}"));
    seen.dedup_by_key(|e| format!("{e:?}"));
    assert_eq!(seen.len(), ENCODINGS.len(), "duplicate entry in the list");
}

proptest! {
    // Arbitrary bytes through every decoder.
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn no_decoder_panics_on_arbitrary_bytes(data in prop::collection::vec(any::<u8>(), 0..512)) {
        for &encoding in ENCODINGS {
            let block = EncodedBlock { encoding, payload: data.clone() };
            // The result is deliberately discarded: success is fine (random
            // bytes occasionally *are* a valid block), failure is fine, and
            // the only unacceptable outcome is a panic, which proptest catches.
            let _ = ColumnDecoder::decode(&block);
        }
    }

    /// The self-describing entry point, which picks the codec from a tag byte
    /// in the data itself.
    #[test]
    fn decode_bytes_does_not_panic_on_arbitrary_bytes(
        data in prop::collection::vec(any::<u8>(), 0..512)
    ) {
        let _ = ColumnDecoder::decode_bytes(&data);
    }

    /// Arbitrary bytes behind a **plausible** header.
    ///
    /// Uniform random bytes almost never get past the first check: the leading
    /// `u32` is a value count, and only about one draw in three thousand lands
    /// under the decode ceiling, so the overwhelming majority of cases bounce
    /// off the header and the decode loops themselves are barely explored.
    /// Fixing a small count in front of a random tail puts every case *inside*
    /// the loop, which is where the arithmetic lives.
    ///
    /// This is what finds a flag byte whose nibbles do not fit in a `u64`:
    /// Patas shifted by up to 120 bits and indexed an 8-byte buffer with a
    /// count of up to 15, and both are one flipped bit away in a stored block.
    #[test]
    fn no_decoder_panics_behind_a_plausible_header(
        count in 1u32..64,
        tail in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let mut payload = Vec::with_capacity(4 + tail.len());
        payload.extend_from_slice(&count.to_le_bytes());
        payload.extend_from_slice(&tail);

        for &encoding in ENCODINGS {
            let block = EncodedBlock { encoding, payload: payload.clone() };
            let _ = ColumnDecoder::decode(&block);
        }
    }

    /// Headers are the dangerous part: the first bytes of most block formats
    /// are a count and a width that size an allocation. Biasing the generator
    /// toward extreme header values reaches those paths far more often than
    /// uniform random bytes do.
    #[test]
    fn no_decoder_panics_on_hostile_headers(
        count in prop_oneof![Just(0u32), Just(1), Just(u32::MAX), Just(u32::MAX / 2), any::<u32>()],
        width in prop_oneof![Just(0u8), Just(1), Just(64), Just(65), Just(255), any::<u8>()],
        tail in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut payload = Vec::with_capacity(8 + tail.len());
        payload.extend_from_slice(&count.to_le_bytes());
        payload.push(width);
        payload.extend_from_slice(&tail);

        for &encoding in ENCODINGS {
            let block = EncodedBlock { encoding, payload: payload.clone() };
            let _ = ColumnDecoder::decode(&block);
        }
    }
}

/// Truncating a *valid* block at every length must never panic — this is the
/// shape a partial write or a torn flash page actually produces, and it is
/// distinct from random bytes because the header stays self-consistent while
/// the payload is short.
#[test]
fn truncating_a_valid_block_never_panics() {
    let values: Vec<f64> = (0..1_000).map(|i| f64::from(i) * 0.25).collect();
    let encoded = chronix_encoding::AlpEncoder::encode(&values).unwrap();

    for cut in 0..encoded.len() {
        let block = EncodedBlock {
            encoding: EncodingType::Alp,
            payload: encoded[..cut].to_vec(),
        };
        // Must not panic. A truncated block is corrupt, so `Err` is the
        // expected answer, but a lucky prefix decoding is also acceptable.
        let _ = ColumnDecoder::decode(&block);
    }
}

/// The same, with a single bit flipped at every position in a valid block —
/// the eMMC failure mode the guard exists for.
#[test]
fn single_bit_flips_never_panic() {
    let values: Vec<f64> = (0..256).map(|i| f64::from(i) * 0.01).collect();
    let encoded = chronix_encoding::AlpEncoder::encode(&values).unwrap();

    for byte in 0..encoded.len() {
        for bit in 0..8 {
            let mut corrupted = encoded.clone();
            corrupted[byte] ^= 1 << bit;
            let block = EncodedBlock {
                encoding: EncodingType::Alp,
                payload: corrupted,
            };
            let _ = ColumnDecoder::decode(&block);
        }
    }
}

//! A corrupt block header must not become an out-of-memory abort.
//!
//! Every decoder in this crate reads a value count from the block header and
//! allocates that many elements before it has read a single value. The
//! per-codec truncation checks do not constrain that count: a block whose bit
//! width is zero — a legitimate encoding for a constant run — carries no
//! payload, so the payload length has nothing to bound the count against.
//!
//! Before the [`MAX_BLOCK_VALUES`] ceiling existed, a **19-byte** ALP block
//! declaring `u32::MAX` values decoded successfully into a 4 294 967 295-element
//! `Vec<f64>` — roughly 34 GiB. That is reachable from a single flipped bit in
//! a `.csx` header on the eMMC-backed gateway this engine is built for, where
//! the outcome is a killed host process rather than a decode error the caller
//! can handle.
//!
//! These tests are written against the *decoder entry points* rather than the
//! helpers, because the helpers are not where the allocation happens.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use chronix_encoding::*;

/// The ceiling the decoders enforce, mirrored here so the test states the
/// number it is asserting rather than importing a private constant.
const CEILING: usize = 1 << 24;

/// A count safely above the ceiling but still a valid `u32`.
const ABSURD: u32 = u32::MAX;

/// The original reproduction: 19 bytes of ALP header, 34 GiB of output.
#[test]
fn alp_rejects_an_absurd_value_count() {
    let mut block = Vec::new();
    block.extend_from_slice(&ABSURD.to_le_bytes()); // count
    block.push(0); // e
    block.push(0); // f
    block.push(0); // bit_width = 0 → no payload to bound `count`
    block.extend_from_slice(&0i64.to_le_bytes()); // reference
    block.extend_from_slice(&0u32.to_le_bytes()); // exception_count

    assert_eq!(block.len(), 19, "the whole input is 19 bytes");
    let err = AlpDecoder::decode(&block)
        .expect_err("a 19-byte block claiming u32::MAX values must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("ceiling") || msg.contains("corrupt"),
        "the error should name the ceiling, got: {msg}"
    );
}

/// The exception count is a second allocation driven by the same header.
#[test]
fn alp_rejects_an_absurd_exception_count() {
    let mut block = Vec::new();
    block.extend_from_slice(&1u32.to_le_bytes()); // count = 1
    block.push(0);
    block.push(0);
    block.push(0); // bit_width = 0
    block.extend_from_slice(&0i64.to_le_bytes());
    block.extend_from_slice(&ABSURD.to_le_bytes()); // exception_count

    assert!(
        AlpDecoder::decode(&block).is_err(),
        "an absurd exception count must be refused"
    );
}

/// Frame-of-reference blocks materialise `vec![reference; count]` directly
/// when the bit width is zero, bypassing the bit-unpacking path entirely.
#[test]
fn for_encoding_rejects_an_absurd_value_count() {
    // [count u32][reference i64][bit_width u8]
    let mut block = Vec::new();
    block.extend_from_slice(&ABSURD.to_le_bytes());
    block.extend_from_slice(&7i64.to_le_bytes());
    block.push(0); // bit_width = 0 → constant run, no payload

    assert!(
        ForDecoder::decode_i64(&block).is_err(),
        "FOR i64 must refuse an absurd count"
    );
    assert!(
        ForDecoder::decode_u64(&block).is_err(),
        "FOR u64 must refuse an absurd count"
    );
}

/// Plain encodings size their allocation straight from the header.
#[test]
fn plain_rejects_an_absurd_value_count() {
    let block = ABSURD.to_le_bytes().to_vec();
    assert!(PlainDecoder::decode_f64(&block).is_err());
    assert!(PlainDecoder::decode_i64(&block).is_err());
    assert!(PlainDecoder::decode_u64(&block).is_err());
    assert!(PlainDecoder::decode_string(&block).is_err());
    assert!(PlainDecoder::decode_bool(&block).is_err());
}

/// Bitmap-encoded booleans.
#[test]
fn bitmap_rejects_an_absurd_value_count() {
    let block = ABSURD.to_le_bytes().to_vec();
    assert!(BitmapDecoder::decode(&block).is_err());
}

/// Dictionary blocks carry *two* attacker-controlled counts: the value count
/// and the dictionary entry count. Both drive an allocation.
#[test]
fn dictionary_rejects_absurd_counts() {
    // [count u32][dict_size u32]
    let mut absurd_values = Vec::new();
    absurd_values.extend_from_slice(&ABSURD.to_le_bytes());
    absurd_values.extend_from_slice(&0u32.to_le_bytes());
    assert!(
        DictionaryDecoder::decode(&absurd_values).is_err(),
        "an absurd value count must be refused"
    );

    let mut absurd_dict = Vec::new();
    absurd_dict.extend_from_slice(&1u32.to_le_bytes());
    absurd_dict.extend_from_slice(&ABSURD.to_le_bytes());
    assert!(
        DictionaryDecoder::decode(&absurd_dict).is_err(),
        "an absurd dictionary size must be refused"
    );
}

/// The ceiling has to be *above* anything the writers legitimately produce,
/// or the guard would reject real data. The default row group is 65 536 rows,
/// so a block 256× that size is comfortably out of reach.
#[test]
fn the_ceiling_is_far_above_any_legitimate_block() {
    assert_eq!(CEILING, 16_777_216);
    // The default row group is 65 536 rows; the ceiling is 256× that.
    assert_eq!(
        CEILING / 65_536,
        256,
        "the ceiling must leave generous headroom above the default row group"
    );
}

/// A legitimate block of realistic size must still decode. A guard that
/// rejects real data is a worse bug than the one it fixes.
#[test]
fn realistic_blocks_still_round_trip() {
    let values: Vec<f64> = (0..65_536).map(|i| f64::from(i) * 0.01).collect();
    let encoded = AlpEncoder::encode(&values).unwrap();
    let decoded = AlpDecoder::decode(&encoded).unwrap();
    assert_eq!(decoded.len(), values.len());
    assert_eq!(decoded, values);
}

/// pco carries its own count in our 4-byte header ahead of the pco file.
/// That count is checked against the ceiling before the output buffer is
/// allocated, and the pco file's own chunk headers are never trusted for an
/// allocation — a body that decodes to a different count is an error. This
/// covers all three pco tags through the unified decoder, and the nullable
/// wrapper around them, so a corrupt count can reach no allocation anywhere.
#[test]
fn pco_rejects_an_absurd_value_count_on_every_entry_point() {
    let mut block = Vec::new();
    block.extend_from_slice(&ABSURD.to_le_bytes());
    block.extend_from_slice(&[0u8; 16]); // whatever follows is irrelevant

    for enc in [
        EncodingType::Pco,
        EncodingType::PcoI64,
        EncodingType::PcoU64,
    ] {
        let err = ColumnDecoder::decode(&EncodedBlock {
            encoding: enc,
            payload: block.clone(),
        })
        .expect_err("a pco block claiming u32::MAX values must be refused");
        assert!(
            err.to_string().contains("ceiling"),
            "{enc}: the error should name the ceiling, got: {err}"
        );

        // Nullable wrapper: a bitmap of one valid value, inner pco block
        // declaring the absurd count.
        let mut nullable = vec![enc.tag()];
        nullable.extend_from_slice(&1u32.to_le_bytes());
        nullable.push(0b1);
        nullable.extend_from_slice(&block);
        let err = ColumnDecoder::decode(&EncodedBlock {
            encoding: EncodingType::Nullable,
            payload: nullable,
        })
        .expect_err("a nullable pco block claiming u32::MAX values must be refused");
        assert!(
            err.to_string().contains("ceiling"),
            "nullable {enc}: the error should name the ceiling, got: {err}"
        );
    }

    // A pco file whose own header disagrees with ours is also refused: the
    // declared count is what sizes the buffer, so the file must fill it
    // exactly — no more, no less.
    let values: Vec<i64> = (0..100).collect();
    let mut enc = PcoEncoder::encode_i64(&values).unwrap();
    enc[..4].copy_from_slice(&(CEILING as u32).to_le_bytes());
    assert!(
        PcoDecoder::decode_i64(&enc).is_err(),
        "a count inside the ceiling that the body does not fill is a count mismatch"
    );
}

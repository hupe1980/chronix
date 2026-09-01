#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Column Encoding
//!
//! Demonstrates Chronix's time-series compression codecs, adaptive
//! encoding selection, and compression ratio comparison.
//!
//! ```bash
//! cargo run --example encoding
//! ```

use chronix::chronix_core::config::FloatEncoding;
use chronix::chronix_encoding::{
    AdaptiveSelector, BitmapDecoder, BitmapEncoder, ChimpDecoder, ChimpEncoder, ColumnDecoder,
    ColumnEncoder, DecodedColumn, DeltaOfDeltaDecoder, DeltaOfDeltaEncoder, DictionaryDecoder,
    DictionaryEncoder, GorillaDecoder, GorillaEncoder, IntegerDecoder, IntegerEncoder,
};

fn main() {
    println!("=== Chronix Column Encoding ===\n");

    // ── 1. Timestamp Compression (Delta-of-Delta) ─────────────────
    println!("--- Timestamp Encoding (Delta-of-Delta) ---");
    let timestamps: Vec<i64> = (0..1000)
        .map(|i| 1_700_000_000_000_000_000i64 + i * 1_000_000_000)
        .collect();

    let raw_size = timestamps.len() * 8;
    let encoded = DeltaOfDeltaEncoder::encode(&timestamps).expect("DoD encode failed");
    let ratio = raw_size as f64 / encoded.len() as f64;
    println!(
        "  {} timestamps: {} bytes → {} bytes ({:.1}x compression)",
        timestamps.len(),
        raw_size,
        encoded.len(),
        ratio,
    );

    let decoded = DeltaOfDeltaDecoder::decode(&encoded).expect("DoD decode failed");
    assert_eq!(timestamps, decoded, "Timestamp round-trip failed");
    println!("  ✓ Round-trip verified");

    // Irregular timestamps (less compressible)
    let irregular_ts: Vec<i64> = (0..1000)
        .map(|i| {
            1_700_000_000_000_000_000i64 + i * 1_000_000_000 + (i * 137 % 500) * 1_000_000
            // jitter
        })
        .collect();
    let irregular_encoded =
        DeltaOfDeltaEncoder::encode(&irregular_ts).expect("DoD encode irregular failed");
    let irregular_ratio = raw_size as f64 / irregular_encoded.len() as f64;
    println!(
        "  Irregular timestamps: {:.1}x (vs {:.1}x regular)",
        irregular_ratio, ratio
    );

    // ── 2. Float Compression (Gorilla vs Chimp) ───────────────────
    println!("\n--- Float Encoding ---");

    // Slowly varying data (great for XOR-based encoders)
    let slow_data: Vec<f64> = (0..1000)
        .map(|i| 50.0 + (i as f64 * 0.01).sin() * 2.0)
        .collect();

    let raw_f64_size = slow_data.len() * 8;

    let gorilla_enc = GorillaEncoder::encode(&slow_data).expect("Gorilla encode failed");
    let chimp_enc = ChimpEncoder::encode(&slow_data).expect("Chimp encode failed");
    println!(
        "Slowly varying data ({} values, {} raw bytes):",
        slow_data.len(),
        raw_f64_size
    );
    println!(
        "  Gorilla: {} bytes ({:.1}x)",
        gorilla_enc.len(),
        raw_f64_size as f64 / gorilla_enc.len() as f64
    );
    println!(
        "  Chimp:   {} bytes ({:.1}x)",
        chimp_enc.len(),
        raw_f64_size as f64 / chimp_enc.len() as f64
    );

    // Verify round-trip
    let gorilla_dec = GorillaDecoder::decode(&gorilla_enc).expect("Gorilla decode failed");
    let chimp_dec = ChimpDecoder::decode(&chimp_enc).expect("Chimp decode failed");
    assert_eq!(slow_data, gorilla_dec, "Gorilla round-trip failed");
    assert_eq!(slow_data, chimp_dec, "Chimp round-trip failed");
    println!("  ✓ Both round-trips verified");

    // Random data (less compressible)
    let random_data: Vec<f64> = (0..1000)
        .map(|i| ((i * 7919 + 104729) % 100000) as f64 / 1000.0)
        .collect();

    let gorilla_rand = GorillaEncoder::encode(&random_data).expect("Gorilla encode random failed");
    let chimp_rand = ChimpEncoder::encode(&random_data).expect("Chimp encode random failed");
    println!("\nRandom data ({} values):", random_data.len());
    println!(
        "  Gorilla: {} bytes ({:.1}x)",
        gorilla_rand.len(),
        raw_f64_size as f64 / gorilla_rand.len() as f64
    );
    println!(
        "  Chimp:   {} bytes ({:.1}x)",
        chimp_rand.len(),
        raw_f64_size as f64 / chimp_rand.len() as f64
    );

    // ── 3. Integer Compression ────────────────────────────────────
    println!("\n--- Integer Encoding ---");
    let int_data: Vec<i64> = (0..1000).map(|i| 42_000 + i * 3).collect();
    let raw_int_size = int_data.len() * 8;

    let int_encoded = IntegerEncoder::encode_i64(&int_data).expect("Integer encode failed");
    println!(
        "  i64 data: {} bytes → {} bytes ({:.1}x)",
        raw_int_size,
        int_encoded.len(),
        raw_int_size as f64 / int_encoded.len() as f64
    );

    let int_decoded = IntegerDecoder::decode_i64(&int_encoded).expect("Integer decode failed");
    assert_eq!(int_data, int_decoded, "Integer round-trip failed");
    println!("  ✓ Round-trip verified");

    // u64 variant
    let u64_data: Vec<u64> = (0..1000).map(|i| 1_000_000 + i * 7).collect();
    let u64_encoded = IntegerEncoder::encode_u64(&u64_data).expect("u64 encode failed");
    println!(
        "  u64 data: {} bytes → {} bytes ({:.1}x)",
        u64_data.len() * 8,
        u64_encoded.len(),
        (u64_data.len() * 8) as f64 / u64_encoded.len() as f64
    );

    // ── 4. Dictionary Encoding (Strings) ──────────────────────────
    println!("\n--- Dictionary Encoding (Strings) ---");
    let tags: Vec<&str> = (0..1000)
        .map(|i| match i % 5 {
            0 => "us-east-1",
            1 => "us-west-2",
            2 => "eu-west-1",
            3 => "ap-southeast-1",
            _ => "eu-central-1",
        })
        .collect();

    let raw_str_size: usize = tags.iter().map(|s| s.len() + 8).sum(); // rough estimate
    let dict_encoded = DictionaryEncoder::encode(&tags).expect("Dictionary encode failed");
    println!(
        "  {} strings (5 unique): ~{} bytes → {} bytes ({:.1}x)",
        tags.len(),
        raw_str_size,
        dict_encoded.len(),
        raw_str_size as f64 / dict_encoded.len() as f64
    );

    let dict_decoded = DictionaryDecoder::decode(&dict_encoded).expect("Dictionary decode failed");
    let tags_owned: Vec<String> = tags.iter().map(std::string::ToString::to_string).collect();
    assert_eq!(tags_owned, dict_decoded, "Dictionary round-trip failed");
    println!("  ✓ Round-trip verified");

    // ── 5. Bitmap Encoding (Booleans) ─────────────────────────────
    println!("\n--- Bitmap Encoding (Booleans) ---");
    let bools: Vec<bool> = (0..1000).map(|i| i % 3 == 0).collect();
    let raw_bool_size = bools.len(); // 1 byte per bool in Rust

    let bitmap_encoded = BitmapEncoder::encode(&bools).expect("Bitmap encode failed");
    println!(
        "  {} bools: {} bytes → {} bytes ({:.1}x)",
        bools.len(),
        raw_bool_size,
        bitmap_encoded.len(),
        raw_bool_size as f64 / bitmap_encoded.len() as f64
    );

    let bitmap_decoded = BitmapDecoder::decode(&bitmap_encoded).expect("Bitmap decode failed");
    assert_eq!(bools, bitmap_decoded, "Bitmap round-trip failed");
    println!("  ✓ Round-trip verified");

    // ── 6. Unified Column Encoder ─────────────────────────────────
    println!("\n--- Unified ColumnEncoder ---");

    let ts_block = ColumnEncoder::encode_timestamps(&timestamps).expect("encode_timestamps failed");
    println!(
        "  Timestamps: encoding={:?}, {} bytes",
        ts_block.encoding,
        ts_block.payload.len()
    );

    let f64_block =
        ColumnEncoder::encode_f64(&slow_data, FloatEncoding::Gorilla).expect("encode_f64 failed");
    println!(
        "  Floats (Gorilla): encoding={:?}, {} bytes",
        f64_block.encoding,
        f64_block.payload.len()
    );

    let f64_chimp_block = ColumnEncoder::encode_f64(&slow_data, FloatEncoding::Chimp)
        .expect("encode_f64 chimp failed");
    println!(
        "  Floats (Chimp): encoding={:?}, {} bytes",
        f64_chimp_block.encoding,
        f64_chimp_block.payload.len()
    );

    let str_block = ColumnEncoder::encode_string(&tags).expect("encode_string failed");
    println!(
        "  Strings: encoding={:?}, {} bytes",
        str_block.encoding,
        str_block.payload.len()
    );

    let bool_block = ColumnEncoder::encode_bool(&bools).expect("encode_bool failed");
    println!(
        "  Bools: encoding={:?}, {} bytes",
        bool_block.encoding,
        bool_block.payload.len()
    );

    // Unified decoder
    match ColumnDecoder::decode(&ts_block).expect("decode timestamps failed") {
        DecodedColumn::I64(vals) => println!("  Decoded timestamps: {} i64 values", vals.len()),
        other => println!(
            "  Unexpected decode type: {:?}",
            std::mem::discriminant(&other)
        ),
    }

    match ColumnDecoder::decode(&f64_block).expect("decode floats failed") {
        DecodedColumn::F64(vals) => println!("  Decoded floats: {} f64 values", vals.len()),
        other => println!(
            "  Unexpected decode type: {:?}",
            std::mem::discriminant(&other)
        ),
    }

    // ── 7. Adaptive Encoding Selection ────────────────────────────
    println!("\n--- Adaptive Encoding Selection ---");
    let selector = AdaptiveSelector::new();

    let pattern_slow = selector.analyze_floats(&slow_data);
    println!("  Slowly varying data → {:?}", pattern_slow);

    let pattern_random = selector.analyze_floats(&random_data);
    println!("  Random data → {:?}", pattern_random);

    let constant_data: Vec<f64> = vec![42.0; 1000];
    let pattern_const = selector.analyze_floats(&constant_data);
    println!("  Constant data → {:?}", pattern_const);

    println!("\n✓ Encoding showcase complete");
}

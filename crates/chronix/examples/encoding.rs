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
    AdaptiveSelector, AlpDecoder, AlpEncoder, BitmapDecoder, BitmapEncoder, ChimpDecoder,
    ChimpEncoder, ColumnDecoder, ColumnEncoder, DecodedColumn, DeltaOfDeltaDecoder,
    DeltaOfDeltaEncoder, DictionaryDecoder, DictionaryEncoder, GorillaDecoder, GorillaEncoder,
    IntegerDecoder, IntegerEncoder,
};

fn main() {
    println!("=== Chronix Column Encoding ===\n");

    // ── 1. Timestamps: pco, with delta-of-delta as the fallback ───
    //
    // A real 1-second sampler jitters, and that is where the two codecs
    // part company: delta-of-delta pays a varint for every non-zero second
    // difference, so jitter makes its output *larger* than plain, while pco
    // entropy-codes the deltas and pays bits.
    println!("--- Timestamp Encoding ---");
    let regular: Vec<i64> = (0..1000)
        .map(|i| 1_700_000_000_000_000_000i64 + i * 1_000_000_000)
        .collect();
    // ±250 ms of jitter around the same 1-second cadence.
    let jittered: Vec<i64> = (0..1000)
        .map(|i| 1_700_000_000_000_000_000i64 + i * 1_000_000_000 + (i * 137 % 500) * 1_000_000)
        .collect();

    let raw_size = regular.len() * 8;
    println!("  {:<22} {:>10} {:>10}", "shape", "DoD", "pco");
    for (label, series) in [("regular 1 s", &regular), ("1 s ± 250 ms", &jittered)] {
        let dod = DeltaOfDeltaEncoder::encode(series).expect("DoD encode failed");
        assert_eq!(
            &DeltaOfDeltaDecoder::decode(&dod).expect("DoD decode failed"),
            series,
            "delta-of-delta round-trip failed"
        );
        let block = ColumnEncoder::encode_timestamps(series).expect("timestamp encode failed");
        match ColumnDecoder::decode(&block).expect("timestamp decode failed") {
            DecodedColumn::I64(back) => assert_eq!(&back, series, "timestamp round-trip failed"),
            other => unreachable!("timestamps decoded to {other:?}"),
        }
        println!(
            "  {:<22} {:>9.1}x {:>9.1}x  (chose {})",
            label,
            raw_size as f64 / dod.len() as f64,
            raw_size as f64 / (block.payload.len() + 1) as f64,
            block.encoding,
        );
    }
    println!("  ✓ Round-trips verified for both codecs and both shapes");

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

    // ── 2b. ALP: the primary float codec ──────────────────────────
    // Most metric values started life as decimals — a meter reporting
    // 231.45 W — and ALP recovers that integer instead of XOR-ing bit
    // patterns. On the same kind of data the XOR codecs manage ~1.1×.
    println!("\n--- ALP (decimal metric data) ---");
    let meter_data: Vec<f64> = (0..1000)
        .map(|i| {
            // two-decimal power readings between 200 and 250 W
            let raw = 200.0 + ((i * 37) % 5000) as f64 / 100.0;
            (raw * 100.0).round() / 100.0
        })
        .collect();
    let alp_enc = AlpEncoder::encode(&meter_data).expect("ALP encode failed");
    let gorilla_meter = GorillaEncoder::encode(&meter_data).expect("Gorilla encode failed");
    let chimp_meter = ChimpEncoder::encode(&meter_data).expect("Chimp encode failed");
    println!(
        "Power meter, 2 dp ({} values, {} raw bytes):",
        meter_data.len(),
        raw_f64_size
    );
    println!(
        "  ALP:     {} bytes ({:.1}x)",
        alp_enc.len(),
        raw_f64_size as f64 / alp_enc.len() as f64
    );
    println!(
        "  Gorilla: {} bytes ({:.1}x)",
        gorilla_meter.len(),
        raw_f64_size as f64 / gorilla_meter.len() as f64
    );
    println!(
        "  Chimp:   {} bytes ({:.1}x)",
        chimp_meter.len(),
        raw_f64_size as f64 / chimp_meter.len() as f64
    );
    let alp_dec = AlpDecoder::decode(&alp_enc).expect("ALP decode failed");
    assert_eq!(meter_data, alp_dec, "ALP round-trip failed");
    println!("  ✓ ALP round-trip verified (bit-exact)");

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

    let ts_block = ColumnEncoder::encode_timestamps(&regular).expect("encode_timestamps failed");
    println!(
        "  Timestamps: encoding={:?}, {} bytes",
        ts_block.encoding,
        ts_block.payload.len()
    );

    // The `FloatEncoding` argument is a *hint*: the unified encoder trials
    // the candidates and keeps whatever is smallest, so what comes back is
    // the codec that won, not the one that was asked for. Printing the hint
    // as if it were the answer is how a table of "Gorilla" numbers ends up
    // describing pco.
    let mut f64_block = None;
    for hint in [FloatEncoding::Gorilla, FloatEncoding::Chimp] {
        let block = ColumnEncoder::encode_f64(&slow_data, hint).expect("encode_f64 failed");
        println!(
            "  Floats (hint {hint:?}): chose {:?}, {} bytes",
            block.encoding,
            block.payload.len()
        );
        f64_block = Some(block);
    }
    let f64_block = f64_block.expect("at least one encoding was tried");

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

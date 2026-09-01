#![allow(clippy::unwrap_used)] // benches may unwrap
//! Benchmarks for the chronix-encoding crate.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use chronix_core::FloatEncoding;
use chronix_encoding::{
    alp::{AlpDecoder, AlpEncoder},
    bitmap::{BitmapDecoder, BitmapEncoder},
    chimp::{ChimpDecoder, ChimpEncoder},
    delta::{DeltaOfDeltaDecoder, DeltaOfDeltaEncoder},
    dictionary::{DictionaryDecoder, DictionaryEncoder},
    gorilla::{GorillaDecoder, GorillaEncoder},
    integer::{IntegerDecoder, IntegerEncoder},
    ColumnEncoder,
};

const N: usize = 1_000_000;

fn generate_timestamps(n: usize) -> Vec<i64> {
    (0..n)
        .map(|i| 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000)
        .collect()
}

fn generate_floats(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| 20.0 + (i as f64 * 0.01).sin() * 5.0 + (i as f64) * 0.001)
        .collect()
}

/// Two-decimal meter readings — the shape ALP is for, and the shape most
/// metric data has.
fn generate_decimals(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| 200.0 + ((i * 37) % 5000) as f64 / 100.0)
        .collect()
}

fn generate_integers(n: usize) -> Vec<i64> {
    (0..n).map(|i| 1000 + (i as i64) % 100).collect()
}

fn bench_delta_of_delta(c: &mut Criterion) {
    let timestamps = generate_timestamps(N);

    c.bench_function("delta_encode_1M", |b| {
        b.iter(|| {
            black_box(DeltaOfDeltaEncoder::encode(black_box(&timestamps)).unwrap());
        });
    });

    let encoded = DeltaOfDeltaEncoder::encode(&timestamps).unwrap();
    c.bench_function("delta_decode_1M", |b| {
        b.iter(|| {
            black_box(DeltaOfDeltaDecoder::decode(black_box(&encoded)).unwrap());
        });
    });
}

fn bench_alp(c: &mut Criterion) {
    let decimals = generate_decimals(N);

    // Encode includes the per-block (e, f) exponent search, which is work the
    // XOR codecs do not do — the point of measuring it separately.
    c.bench_function("alp_encode_1M", |b| {
        b.iter(|| {
            black_box(AlpEncoder::encode(black_box(&decimals)).unwrap());
        });
    });

    let encoded = AlpEncoder::encode(&decimals).unwrap();
    c.bench_function("alp_decode_1M", |b| {
        b.iter(|| {
            black_box(AlpDecoder::decode(black_box(&encoded)).unwrap());
        });
    });

    // Same data through the codecs ALP replaced, for a like-for-like read.
    c.bench_function("chimp_encode_decimals_1M", |b| {
        b.iter(|| {
            black_box(ChimpEncoder::encode(black_box(&decimals)).unwrap());
        });
    });
    let chimp_encoded = ChimpEncoder::encode(&decimals).unwrap();
    c.bench_function("chimp_decode_decimals_1M", |b| {
        b.iter(|| {
            black_box(ChimpDecoder::decode(black_box(&chimp_encoded)).unwrap());
        });
    });
}

fn bench_chimp(c: &mut Criterion) {
    let floats = generate_floats(N);

    c.bench_function("chimp_encode_1M", |b| {
        b.iter(|| {
            black_box(ChimpEncoder::encode(black_box(&floats)).unwrap());
        });
    });

    let encoded = ChimpEncoder::encode(&floats).unwrap();
    c.bench_function("chimp_decode_1M", |b| {
        b.iter(|| {
            black_box(ChimpDecoder::decode(black_box(&encoded)).unwrap());
        });
    });
}

fn bench_gorilla(c: &mut Criterion) {
    let floats = generate_floats(N);

    c.bench_function("gorilla_encode_1M", |b| {
        b.iter(|| {
            black_box(GorillaEncoder::encode(black_box(&floats)).unwrap());
        });
    });

    let encoded = GorillaEncoder::encode(&floats).unwrap();
    c.bench_function("gorilla_decode_1M", |b| {
        b.iter(|| {
            black_box(GorillaDecoder::decode(black_box(&encoded)).unwrap());
        });
    });
}

fn bench_integer(c: &mut Criterion) {
    let integers = generate_integers(N);

    c.bench_function("integer_encode_i64_1M", |b| {
        b.iter(|| {
            black_box(IntegerEncoder::encode_i64(black_box(&integers)).unwrap());
        });
    });

    let encoded = IntegerEncoder::encode_i64(&integers).unwrap();
    c.bench_function("integer_decode_i64_1M", |b| {
        b.iter(|| {
            black_box(IntegerDecoder::decode_i64(black_box(&encoded)).unwrap());
        });
    });
}

fn bench_dictionary(c: &mut Criterion) {
    let values: Vec<String> = (0..100_000).map(|i| format!("host-{}", i % 100)).collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();

    c.bench_function("dict_encode_100K", |b| {
        b.iter(|| {
            black_box(DictionaryEncoder::encode(black_box(&refs)).unwrap());
        });
    });

    let encoded = DictionaryEncoder::encode(&refs).unwrap();
    c.bench_function("dict_decode_100K", |b| {
        b.iter(|| {
            black_box(DictionaryDecoder::decode(black_box(&encoded)).unwrap());
        });
    });
}

fn bench_bitmap(c: &mut Criterion) {
    let bools: Vec<bool> = (0..N).map(|i| i % 3 != 0).collect();

    c.bench_function("bitmap_encode_1M", |b| {
        b.iter(|| {
            black_box(BitmapEncoder::encode(black_box(&bools)).unwrap());
        });
    });

    let encoded = BitmapEncoder::encode(&bools).unwrap();
    c.bench_function("bitmap_decode_1M", |b| {
        b.iter(|| {
            black_box(BitmapDecoder::decode(black_box(&encoded)).unwrap());
        });
    });
}

fn bench_unified(c: &mut Criterion) {
    let timestamps = generate_timestamps(N);
    let floats = generate_floats(N);

    c.bench_function("unified_timestamps_1M", |b| {
        b.iter(|| {
            black_box(ColumnEncoder::encode_timestamps(black_box(&timestamps)).unwrap());
        });
    });

    c.bench_function("unified_floats_chimp_1M", |b| {
        b.iter(|| {
            black_box(ColumnEncoder::encode_f64(black_box(&floats), FloatEncoding::Chimp).unwrap());
        });
    });
}

/// Story 9.2: Compression ratio benchmarks.
///
/// Measures bytes-in vs bytes-out for Chimp, Gorilla, and delta-of-delta
/// on realistic time-series data patterns.
fn bench_compression_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_ratio");

    // Regular metrics: smooth monotonic with small perturbations
    let regular_floats: Vec<f64> = (0..100_000)
        .map(|i| 50.0 + (i as f64) * 0.001 + ((i as f64) * 0.1).sin() * 0.5)
        .collect();

    // Irregular metrics: random-ish values
    let irregular_floats: Vec<f64> = (0..100_000)
        .map(|i| {
            let x = (i as f64) * 1.618033988749;
            (x.sin() * 100.0 + x.cos() * 50.0).abs()
        })
        .collect();

    let raw_size_bytes = regular_floats.len() * 8; // 8 bytes per f64

    // Chimp on regular
    group.bench_function("chimp_regular_100K", |b| {
        b.iter(|| {
            let encoded = ChimpEncoder::encode(black_box(&regular_floats)).unwrap();
            let ratio = raw_size_bytes as f64 / encoded.len() as f64;
            black_box(ratio);
        });
    });

    // Chimp on irregular
    group.bench_function("chimp_irregular_100K", |b| {
        b.iter(|| {
            let encoded = ChimpEncoder::encode(black_box(&irregular_floats)).unwrap();
            let ratio = raw_size_bytes as f64 / encoded.len() as f64;
            black_box(ratio);
        });
    });

    // Gorilla on regular (for comparison: Chimp should achieve ≤ 60% of Gorilla's space)
    group.bench_function("gorilla_regular_100K", |b| {
        b.iter(|| {
            let encoded = GorillaEncoder::encode(black_box(&regular_floats)).unwrap();
            let ratio = raw_size_bytes as f64 / encoded.len() as f64;
            black_box(ratio);
        });
    });

    // Delta-of-delta on timestamps
    let timestamps = generate_timestamps(100_000);
    let ts_raw = timestamps.len() * 8;
    group.bench_function("delta_timestamps_100K", |b| {
        b.iter(|| {
            let encoded = DeltaOfDeltaEncoder::encode(black_box(&timestamps)).unwrap();
            let ratio = ts_raw as f64 / encoded.len() as f64;
            black_box(ratio);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_delta_of_delta,
    bench_alp,
    bench_chimp,
    bench_gorilla,
    bench_integer,
    bench_dictionary,
    bench_bitmap,
    bench_unified,
    bench_compression_ratios,
);
criterion_main!(benches);

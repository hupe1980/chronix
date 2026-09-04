#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The standing obligation to re-check the field: ALP against Pcodec
//! (Loncaric 2025, the `pco` crate) on the same workloads the README
//! quotes. Run with `--nocapture` to see the table.
//!
//! This is a measurement, not a gate — it asserts only that both codecs
//! round-trip. The numbers it prints are what decides whether `pco` becomes
//! a candidate in the adaptive selector.

use chronix_encoding::{AlpDecoder, AlpEncoder, Chimp128Encoder};
use pco::standalone::{simple_compress, simple_decompress};
use pco::ChunkConfig;

fn workloads() -> Vec<(&'static str, Vec<f64>)> {
    vec![
        (
            "power meter, W, 2dp",
            (0..8192)
                .map(|i| 230.0 + f64::from((i * 37) % 4000) / 100.0)
                .collect(),
        ),
        (
            "temperature, C, 1dp",
            (0..8192)
                .map(|i| 18.0 + f64::from((i * 7) % 120) / 10.0)
                .collect(),
        ),
        (
            "energy, kWh, 3dp cumulative",
            (0..8192).map(|i| 1000.0 + f64::from(i) / 1000.0).collect(),
        ),
        (
            "scientific doubles",
            (0..8192)
                .map(|i| f64::from(i).sin() * std::f64::consts::PI)
                .collect(),
        ),
        (
            "noisy sensor, 2dp + jitter",
            (0..8192)
                .map(|i| {
                    let base = 50.0 + (f64::from(i) * 0.01).sin() * 10.0;
                    (base * 100.0 + f64::from((i * 7919) % 97) / 10.0).round() / 100.0
                })
                .collect(),
        ),
    ]
}

#[test]
fn alp_versus_pco() {
    println!(
        "\n{:<32} {:>8} {:>8} {:>8}",
        "workload", "ALP", "pco", "Chimp128"
    );
    for (name, values) in workloads() {
        let raw = values.len() * 8;
        let alp = AlpEncoder::encode(&values).unwrap();
        assert_eq!(AlpDecoder::decode(&alp).unwrap(), values);
        let pco = simple_compress(&values, &ChunkConfig::default()).unwrap();
        let back: Vec<f64> = simple_decompress(&pco).unwrap();
        assert_eq!(back, values);
        let chimp = Chimp128Encoder::encode(&values).unwrap();

        let t = std::time::Instant::now();
        for _ in 0..20 {
            let _ = AlpDecoder::decode(&alp).unwrap();
        }
        let alp_dec = t.elapsed() / 20;
        let t = std::time::Instant::now();
        for _ in 0..20 {
            let _: Vec<f64> = simple_decompress(&pco).unwrap();
        }
        let pco_dec = t.elapsed() / 20;

        println!(
            "{:<32} {:>7.1}x {:>7.1}x {:>7.1}x   decode ALP {:?} / pco {:?}",
            name,
            raw as f64 / alp.len() as f64,
            raw as f64 / pco.len() as f64,
            raw as f64 / chimp.len() as f64,
            alp_dec,
            pco_dec,
        );
    }
}

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Compression ratios the float codecs must hold on realistic workloads.
//!
//! The README and CONCEPT quote these numbers, so they are asserted rather
//! than measured once and written down. Run with `--nocapture` to see the
//! full table.

use chronix_encoding::{AlpEncoder, Chimp128Encoder, ChimpEncoder, GorillaEncoder, PatasEncoder};

struct Workload {
    name: &'static str,
    values: Vec<f64>,
    /// Minimum ratio ALP must reach (raw bytes / encoded bytes).
    min_alp_ratio: f64,
    /// Whether ALP is expected to beat every XOR codec here.
    alp_should_win: bool,
}

fn workloads() -> Vec<Workload> {
    vec![
        Workload {
            name: "power meter, W, 2dp",
            values: (0..8192)
                .map(|i| 230.0 + f64::from((i * 37) % 4000) / 100.0)
                .collect(),
            min_alp_ratio: 3.5,
            alp_should_win: true,
        },
        Workload {
            name: "temperature, C, 1dp",
            values: (0..8192)
                .map(|i| 18.0 + f64::from((i * 7) % 120) / 10.0)
                .collect(),
            min_alp_ratio: 8.0,
            alp_should_win: true,
        },
        Workload {
            name: "energy, kWh, 3dp cumulative",
            values: (0..8192).map(|i| 1000.0 + f64::from(i) / 1000.0).collect(),
            min_alp_ratio: 4.0,
            alp_should_win: true,
        },
        Workload {
            // Genuinely non-decimal doubles: ALP is not for these, and the
            // adaptive selector is expected to pick an XOR codec instead.
            name: "scientific doubles",
            values: (0..8192)
                .map(|i| f64::from(i).sin() * std::f64::consts::PI)
                .collect(),
            min_alp_ratio: 0.0,
            alp_should_win: false,
        },
    ]
}

#[test]
fn float_codec_ratios_hold() {
    for w in workloads() {
        let raw = w.values.len() * 8;
        let alp = AlpEncoder::encode(&w.values).unwrap().len();
        let chimp = ChimpEncoder::encode(&w.values).unwrap().len();
        let chimp128 = Chimp128Encoder::encode(&w.values).unwrap().len();
        let gorilla = GorillaEncoder::encode(&w.values).unwrap().len();
        let patas = PatasEncoder::encode(&w.values).unwrap().len();

        let ratio = |n: usize| raw as f64 / n as f64;
        println!(
            "{:<30} alp {:5.1}x  chimp {:4.1}x  chimp128 {:4.1}x  gorilla {:4.1}x  patas {:4.1}x",
            w.name,
            ratio(alp),
            ratio(chimp),
            ratio(chimp128),
            ratio(gorilla),
            ratio(patas),
        );

        assert!(
            ratio(alp) >= w.min_alp_ratio,
            "{}: ALP ratio {:.2}x fell below the documented {:.2}x",
            w.name,
            ratio(alp),
            w.min_alp_ratio
        );
        if w.alp_should_win {
            let best_xor = chimp.min(chimp128).min(gorilla).min(patas);
            assert!(
                alp < best_xor,
                "{}: ALP ({alp} bytes) no longer beats the best XOR codec ({best_xor} bytes)",
                w.name
            );
        }
    }
}

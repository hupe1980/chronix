#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Compression ratios the float codecs must hold on realistic workloads.
//!
//! The README and CONCEPT quote these numbers, so they are asserted rather
//! than measured once and written down. Run with `--nocapture` to see the
//! full table.

use chronix_encoding::{
    AlpEncoder, Chimp128Encoder, ChimpEncoder, GorillaEncoder, PatasEncoder, PcoEncoder,
};

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
        let pco = PcoEncoder::encode_f64(&w.values).unwrap().len();

        let ratio = |n: usize| raw as f64 / n as f64;
        println!(
            "{:<30} pco {:5.1}x  alp {:5.1}x  chimp {:4.1}x  chimp128 {:4.1}x  gorilla {:4.1}x  patas {:4.1}x",
            w.name,
            ratio(pco),
            ratio(alp),
            ratio(chimp),
            ratio(chimp128),
            ratio(gorilla),
            ratio(patas),
        );

        // pco is the primary codec: on decimal data it must beat ALP by a
        // wide margin, and it must never lose to ALP by more than a hair.
        if w.alp_should_win {
            assert!(
                ratio(pco) >= w.min_alp_ratio * 2.5,
                "{}: pco {:.1}x is no longer well ahead of ALP's floor {:.1}x",
                w.name,
                ratio(pco),
                w.min_alp_ratio
            );
        }
        assert!(
            pco as f64 <= alp as f64 * 1.05,
            "{}: pco ({pco} B) lost to ALP ({alp} B)",
            w.name
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

// ───────────────────────────────────────────────────────────────────────
// Realistic workloads
// ───────────────────────────────────────────────────────────────────────
//
// The workloads above are *synthetic*: every one of them is a modular
// arithmetic sequence, so the value set is tiny and strictly periodic
// (`(i * 37) % 4000` visits 4000 values and then repeats forever). Any
// codec with a dictionary or a learned bin table crushes that, and the
// ratios it produces say more about the generator than about the codec.
// They are kept because they are the ratios the older docs quote, and
// changing what a number measures without saying so is worse than a wrong
// number.
//
// These are what a gateway actually sees: sensor noise on every sample, a
// dynamic range that spans orders of magnitude, plateaus where a device
// saturates or idles, and dropouts where a reading is missing. The
// difference in the printed table is the point of this file.

/// Deterministic LCG.
fn lcg(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed >> 33
}

/// Uniform in [-1, 1).
fn unit(seed: &mut u64) -> f64 {
    (lcg(seed) % 2_000_001) as f64 / 1_000_000.0 - 1.0
}

/// Approximately Gaussian noise, scaled to `sigma`.
fn noise(seed: &mut u64, sigma: f64) -> f64 {
    (unit(seed) + unit(seed) + unit(seed)) / 3.0 * sigma * 3.0
}

/// Round to `dp` decimal places, the way a sensor's protocol does.
fn quantize(v: f64, dp: i32) -> f64 {
    let scale = 10f64.powi(dp);
    (v * scale).round() / scale
}

const REALISTIC_N: usize = 8192;

fn realistic_workloads() -> Vec<Realistic> {
    let mut seed = 0xC0FF_EE00_u64;

    // PV inverter AC power. A diurnal bell curve, clipped at the inverter
    // limit around noon (a long plateau), with cloud events that drop
    // output by 30-80% for a few minutes, plus measurement noise. 1 dp.
    let pv: Vec<f64> = (0..REALISTIC_N)
        .map(|i| {
            let day = i as f64 / REALISTIC_N as f64 * 2.0;
            let sun = (day * std::f64::consts::PI).sin().max(0.0);
            let mut w = 9000.0 * sun.powf(1.4);
            if w > 6500.0 {
                w = 6500.0; // inverter clipping: a real plateau
            }
            if lcg(&mut seed) % 97 < 6 {
                w *= 0.2 + (lcg(&mut seed) % 500) as f64 / 1000.0;
            }
            quantize((w + noise(&mut seed, 4.0)).max(0.0), 1)
        })
        .collect();

    // Household load. A low baseline with appliance steps on top: a fridge
    // cycling, a kettle, an EV charger. Spiky, wide range, 2 dp.
    let mut load_base = 240.0;
    let mut appliance = 0.0;
    let mut appliance_left = 0usize;
    let load: Vec<f64> = (0..REALISTIC_N)
        .map(|_| {
            if appliance_left == 0 {
                if lcg(&mut seed).is_multiple_of(40) {
                    appliance = [2000.0, 3600.0, 7400.0, 800.0][(lcg(&mut seed) % 4) as usize];
                    appliance_left = 5 + (lcg(&mut seed) % 60) as usize;
                } else {
                    appliance = 0.0;
                }
            } else {
                appliance_left -= 1;
            }
            load_base += noise(&mut seed, 3.0);
            load_base = load_base.clamp(150.0, 400.0);
            quantize(load_base + appliance + noise(&mut seed, 2.0), 2)
        })
        .collect();

    // Flow temperature. Slow physical drift plus sensor noise at the
    // sensor's own resolution (0.1 K), so consecutive samples differ by a
    // quantum or two — the shape XOR codecs were designed for.
    let temp: Vec<f64> = (0..REALISTIC_N)
        .map(|i| {
            let drift = 45.0 + 12.0 * (i as f64 / 900.0).sin();
            quantize(drift + noise(&mut seed, 0.12), 1)
        })
        .collect();

    // Cumulative energy register, 3 dp, spanning five orders of magnitude
    // over the window and never decreasing. Deltas are tiny; the values
    // are not.
    let mut kwh = 0.317_f64;
    let energy: Vec<f64> = (0..REALISTIC_N)
        .map(|i| {
            kwh += (pv[i] / 1000.0 / 3600.0).max(0.0) + 0.000_2;
            quantize(kwh, 3)
        })
        .collect();

    // Battery state of charge: long saturated plateaus at 100.0 and at the
    // reserve floor, with ramps between. 1 dp.
    let soc: Vec<f64> = (0..REALISTIC_N)
        .map(|i| {
            let phase = (i as f64 / 700.0).sin();
            let raw = 50.0 + 60.0 * phase;
            quantize(raw.clamp(10.0, 100.0) + noise(&mut seed, 0.03), 1)
        })
        .collect();

    // Raw ADC voltage after a calibration multiply: genuinely non-decimal
    // doubles with full mantissas, the case where decimal recovery has
    // nothing to recover.
    let adc: Vec<f64> = (0..REALISTIC_N)
        .map(|i| {
            let counts = 2048.0 + 1800.0 * (i as f64 / 311.0).sin() + noise(&mut seed, 12.0);
            counts * (3.3 / 4095.0) * 1.000_137_2
        })
        .collect();

    vec![
        // (name, values, pco floor, ALP floor)
        Realistic::new("PV inverter, W, 1dp", pv.clone(), 6.0, 3.5),
        Realistic::new("house load, W, 2dp", load, 4.5, 2.8),
        Realistic::new("flow temp, C, 1dp", temp, 14.0, 7.0),
        Realistic::new("energy counter, kWh, 3dp", energy, 25.0, 4.0),
        Realistic::new("battery SoC, %, 1dp", soc, 17.0, 5.5),
        // Decimal recovery has nothing to recover here, and ALP goes below
        // 1.0 — it expands the column. pco degrades to roughly plain.
        Realistic::new("raw ADC volts, non-decimal", adc, 1.2, 0.0),
    ]
}

struct Realistic {
    name: &'static str,
    values: Vec<f64>,
    min_pco_ratio: f64,
    min_alp_ratio: f64,
}

impl Realistic {
    fn new(name: &'static str, values: Vec<f64>, min_pco_ratio: f64, min_alp_ratio: f64) -> Self {
        Self {
            name,
            values,
            min_pco_ratio,
            min_alp_ratio,
        }
    }
}

/// What the codecs do on data that was not generated by `%`.
///
/// **These are the numbers to quote.** The synthetic table above reports pco
/// at 46-85x and ALP at 4-9x; on realistic sensor data pco lands at
/// **5.3-28.8x** and ALP at **3.2-8.0x**. The gap is not a regression in
/// either codec, it is the difference between a 4000-element periodic value
/// set and a signal with a noise floor: noise is incompressible, and a real
/// sensor puts a fresh draw of it in the low decimal place of every sample.
///
/// pco still leads ALP on every workload, by 1.7x to 6.2x, and the ordering
/// of the codec families is unchanged. Only the magnitudes move.
#[test]
fn realistic_float_codec_ratios() {
    println!(
        "\n{:<28} {:>9} {:>8} {:>8} {:>9} {:>8} {:>8}",
        "realistic workload", "pco", "alp", "chimp", "chimp128", "gorilla", "patas"
    );
    for w in realistic_workloads() {
        let raw = w.values.len() * 8;
        let alp = AlpEncoder::encode(&w.values).unwrap().len();
        let chimp = ChimpEncoder::encode(&w.values).unwrap().len();
        let chimp128 = Chimp128Encoder::encode(&w.values).unwrap().len();
        let gorilla = GorillaEncoder::encode(&w.values).unwrap().len();
        let patas = PatasEncoder::encode(&w.values).unwrap().len();
        let pco = PcoEncoder::encode_f64(&w.values).unwrap().len();
        let r = |n: usize| raw as f64 / n as f64;
        println!(
            "{:<28} {:8.2}x {:7.2}x {:7.2}x {:8.2}x {:7.2}x {:7.2}x",
            w.name,
            r(pco),
            r(alp),
            r(chimp),
            r(chimp128),
            r(gorilla),
            r(patas)
        );

        assert!(
            r(pco) >= w.min_pco_ratio,
            "{}: pco {:.2}x fell below the documented {:.2}x",
            w.name,
            r(pco),
            w.min_pco_ratio
        );
        assert!(
            r(alp) >= w.min_alp_ratio,
            "{}: ALP {:.2}x fell below the documented {:.2}x",
            w.name,
            r(alp),
            w.min_alp_ratio
        );
        assert!(
            pco < alp,
            "{}: pco ({pco} B) no longer beats ALP ({alp} B) on realistic data",
            w.name
        );
    }
}

/// The docs must not quote the synthetic figures as if they were realistic.
///
/// This is the assertion that keeps "16-85x pco" from creeping back into
/// CONCEPT text: on realistic decimal sensor data pco does not reach 30x,
/// and on the synthetic modular-arithmetic data it comfortably exceeds 80x.
/// Both facts are true; only one of them describes a gateway.
#[test]
fn synthetic_ratios_are_an_order_of_magnitude_above_realistic_ones() {
    let best_realistic = realistic_workloads()
        .iter()
        .map(|w| {
            w.values.len() as f64 * 8.0 / PcoEncoder::encode_f64(&w.values).unwrap().len() as f64
        })
        .fold(0.0_f64, f64::max);
    let best_synthetic = workloads()
        .iter()
        .map(|w| {
            w.values.len() as f64 * 8.0 / PcoEncoder::encode_f64(&w.values).unwrap().len() as f64
        })
        .fold(0.0_f64, f64::max);
    println!("best pco ratio: synthetic {best_synthetic:.1}x, realistic {best_realistic:.1}x");
    assert!(
        best_realistic < 30.0,
        "realistic pco reached {best_realistic:.1}x - re-derive the documented range"
    );
    assert!(
        best_synthetic > 80.0,
        "synthetic pco fell to {best_synthetic:.1}x - the older documented range no longer holds either"
    );
}

/// Missing values are the shape the ratio tables never show.
///
/// A gateway drops readings: a sensor times out, a Modbus poll fails, a
/// device is asleep. Those arrive as NULLs, and the nullable wrapper pays a
/// validity bitmap (1 bit per row) on top of the inner block. On a column
/// that compresses well the bitmap can be the *majority* of the block, so
/// the ratio a sparse column achieves is bounded by the bitmap long before
/// it is bounded by the codec.
#[test]
fn missing_values_cost_a_validity_bitmap() {
    use chronix_core::config::FloatEncoding;
    use chronix_encoding::{ColumnDecoder, ColumnEncoder, DecodedColumn};

    let dense = realistic_workloads().swap_remove(0).values;
    let mut seed = 0x51DE_u64;
    // 2% dropouts.
    let sparse: Vec<Option<f64>> = dense
        .iter()
        .map(|&v| {
            if lcg(&mut seed).is_multiple_of(50) {
                None
            } else {
                Some(v)
            }
        })
        .collect();
    let nulls = sparse.iter().filter(|v| v.is_none()).count();
    assert!(nulls > 100, "generator produced only {nulls} nulls");

    let block = ColumnEncoder::encode_f64_nullable(&sparse, FloatEncoding::Chimp).unwrap();
    match ColumnDecoder::decode(&block).unwrap() {
        DecodedColumn::NullableF64(back) => assert_eq!(back, sparse),
        other => panic!("nullable f64 decoded to {other:?}"),
    }

    let raw = sparse.len() * 8;
    let bitmap_bytes = sparse.len().div_ceil(8);
    let ratio = raw as f64 / (block.payload.len() + 1) as f64;
    println!(
        "PV with {:.1}% dropouts: {ratio:.2}x ({} B, of which {bitmap_bytes} B is the validity bitmap)",
        nulls as f64 / sparse.len() as f64 * 100.0,
        block.payload.len()
    );
    assert!(ratio >= 5.0, "nullable PV fell to {ratio:.2}x");
    // The bound the docs need: a nullable column can never exceed 64x, one
    // bit per row, however well the values themselves compress.
    assert!(
        block.payload.len() > bitmap_bytes,
        "block is smaller than its own validity bitmap"
    );
}

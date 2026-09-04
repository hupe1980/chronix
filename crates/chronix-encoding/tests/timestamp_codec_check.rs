#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Timestamp codec field check: delta-of-delta against Pcodec on the
//! timestamp shapes a gateway actually produces. Run with `--nocapture` to
//! see the table.
//!
//! Delta-of-delta is perfect on a metronome and falls apart the moment the
//! clock jitters, because every non-zero second difference costs a varint.
//! pco entropy-codes the deltas against learned bins, so jitter of a few
//! milliseconds costs bits, not bytes. The table below is what decided the
//! default timestamp codec; the assertions pin the decision.

use chronix_encoding::{
    ColumnDecoder, ColumnEncoder, DecodedColumn, DeltaOfDeltaDecoder, DeltaOfDeltaEncoder,
    EncodingType, PcoDecoder, PcoEncoder,
};

const N: usize = 8192;
const SECOND: i64 = 1_000_000_000;
const BASE: i64 = 1_700_000_000_000_000_000;

/// Deterministic LCG so the table is reproducible.
fn lcg(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed >> 33
}

fn workloads() -> Vec<(&'static str, Vec<i64>)> {
    let mut seed = 0x5eed_u64;
    let regular: Vec<i64> = (0..N as i64).map(|i| BASE + i * SECOND).collect();
    let jitter: Vec<i64> = (0..N as i64)
        .map(|i| {
            // ±5 ms of jitter around a 1 s cadence.
            let j = (lcg(&mut seed) % 10_000_001) as i64 - 5_000_000;
            BASE + i * SECOND + j
        })
        .collect();
    let mut gappy = Vec::with_capacity(N);
    let mut t = BASE;
    for _ in 0..N {
        gappy.push(t);
        t += SECOND;
        // 1% of readings are lost: the next one lands a few seconds later.
        if lcg(&mut seed).is_multiple_of(100) {
            t += SECOND * (1 + (lcg(&mut seed) % 5) as i64);
        }
    }
    let buckets: Vec<i64> = (0..N as i64).map(|i| BASE + i * 15 * 60 * SECOND).collect();
    vec![
        ("regular 1 s", regular),
        ("1 s ± 5 ms jitter", jitter),
        ("1 s with 1% gaps", gappy),
        ("15-min buckets", buckets),
    ]
}

fn ratio(raw: usize, encoded: usize) -> f64 {
    raw as f64 / encoded as f64
}

#[test]
fn delta_of_delta_versus_pco_on_timestamps() {
    println!(
        "\n{:<20} {:>8} {:>8} {:>8}",
        "workload", "DoD", "pco", "unified"
    );
    let mut results = Vec::new();
    for (name, values) in workloads() {
        let raw = values.len() * 8;
        let dod = DeltaOfDeltaEncoder::encode(&values).unwrap();
        assert_eq!(DeltaOfDeltaDecoder::decode(&dod).unwrap(), values);
        let pco = PcoEncoder::encode_timestamps(&values).unwrap();
        assert_eq!(PcoDecoder::decode_i64(&pco).unwrap(), values);

        let block = ColumnEncoder::encode_timestamps(&values).unwrap();
        match ColumnDecoder::decode(&block).unwrap() {
            DecodedColumn::I64(back) => assert_eq!(back, values),
            other => panic!("timestamps decoded to {other:?}"),
        }

        println!(
            "{:<20} {:>7.1}x {:>7.1}x {:>7.1}x ({})",
            name,
            ratio(raw, dod.len()),
            ratio(raw, pco.len()),
            ratio(raw, block.payload.len() + 1),
            block.encoding,
        );
        results.push((name, ratio(raw, dod.len()), ratio(raw, pco.len()), block));
    }

    // The decision: pco must win clearly on the jittered and gappy shapes and
    // not lose more than 20% on the regular one. If a future pco release
    // shifts these numbers, this is the test that says so.
    for (name, dod, pco, block) in &results {
        match *name {
            "regular 1 s" | "15-min buckets" => assert!(
                *pco >= *dod * 0.8,
                "{name}: pco {pco:.1}x lost more than 20% to DoD {dod:.1}x"
            ),
            _ => assert!(
                *pco >= *dod * 2.0,
                "{name}: pco {pco:.1}x is no longer well ahead of DoD {dod:.1}x"
            ),
        }
        // The unified encoder keeps DoD available but pco is the default:
        // whichever wins, the block must be at least as small as either.
        let best = dod.max(*pco);
        let unified = ratio(N * 8, block.payload.len() + 1);
        assert!(
            unified >= best * 0.95,
            "{name}: unified {unified:.1}x ({}) is behind the better codec {best:.1}x",
            block.encoding
        );
        if *name == "1 s ± 5 ms jitter" {
            assert_eq!(
                block.encoding,
                EncodingType::PcoI64,
                "jittered timestamps must be pco-encoded by default"
            );
        }
    }
}

/// The headline figures the docs quote, checked against the Shannon bound
/// rather than against a number somebody picked.
///
/// The jittered workload adds a uniform draw from 10_000_001 distinct
/// nanosecond offsets to each timestamp. That draw is genuine, incompressible
/// information: log2(10_000_001) = 23.25 bits = 2.907 bytes per sample, so
/// **no lossless codec can exceed 2.75x on this data**. An earlier version of
/// this test asserted `>= 3.0x`, which is not a bar pco was failing to clear
/// but a bar on the wrong side of information theory.
///
/// So the assertion is that pco lands within 5% of the bound — which pins
/// something real (that pco is still finding the structure and spending bits
/// only on the noise) instead of pinning a wish.
#[test]
fn timestamp_headline_ratios() {
    /// Distinct jitter offsets in `workloads()`'s "1 s ± 5 ms jitter".
    const JITTER_STATES: f64 = 10_000_001.0;

    let (_, jitter) = workloads().swap_remove(1);
    let raw = jitter.len() * 8;
    let dod = DeltaOfDeltaEncoder::encode(&jitter).unwrap().len();
    let pco = PcoEncoder::encode_timestamps(&jitter).unwrap().len();

    let bound_bytes_per_value = JITTER_STATES.log2() / 8.0;
    let bound_ratio = 8.0 / bound_bytes_per_value;
    println!(
        "jittered 1 s: DoD {:.1}x, pco {:.1}x (Shannon bound {:.2}x)",
        ratio(raw, dod),
        ratio(raw, pco),
        bound_ratio
    );

    // DoD does not merely compress badly here: it makes the column bigger
    // than the raw i64s. That is the finding that moved the default.
    assert!(
        ratio(raw, dod) < 1.0,
        "DoD on jittered timestamps is no longer expanding ({:.1}x) — recheck the default",
        ratio(raw, dod)
    );
    assert!(
        ratio(raw, pco) >= bound_ratio * 0.95,
        "pco on jittered 1 s fell to {:.2}x, more than 5% off the {bound_ratio:.2}x bound",
        ratio(raw, pco)
    );
}

/// Scratch: which pco delta spec suits timestamps. Prints only.
#[test]
fn pco_delta_spec_comparison() {
    use pco::standalone::simple_compress;
    use pco::{ChunkConfig, DeltaSpec};
    for (name, values) in workloads() {
        let raw = values.len() * 8;
        let auto = simple_compress(&values, &ChunkConfig::default())
            .unwrap()
            .len();
        let c1 = simple_compress(
            &values,
            &ChunkConfig::default().with_delta_spec(DeltaSpec::TryConsecutive(1)),
        )
        .unwrap()
        .len();
        let c2 = simple_compress(
            &values,
            &ChunkConfig::default().with_delta_spec(DeltaSpec::TryConsecutive(2)),
        )
        .unwrap()
        .len();
        let l = simple_compress(
            &values,
            &ChunkConfig::default().with_delta_spec(DeltaSpec::TryLookback),
        )
        .unwrap()
        .len();
        let lvl12 = simple_compress(&values, &ChunkConfig::default().with_compression_level(12))
            .unwrap()
            .len();
        println!(
            "{name:<20} auto {:.1}x  c1 {:.1}x  c2 {:.1}x  lookback {:.1}x  auto@12 {:.1}x",
            ratio(raw, auto),
            ratio(raw, c1),
            ratio(raw, c2),
            ratio(raw, l),
            ratio(raw, lvl12)
        );
    }
}

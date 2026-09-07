#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! What an exact decimal column costs against the float it replaces.
//!
//! The README and the encoding notes make a specific claim: choosing
//! `Decimal` over `f64` for a meter register does not cost compression, it
//! *buys* it, because the mantissa is the integer that ALP and pco spend
//! their first stage recovering from an IEEE-754 double. A claim like that
//! rots the moment either codec changes, so it is asserted here.
//!
//! Run with `--nocapture` for the table.

use chronix_core::config::FloatEncoding;
use chronix_encoding::{ColumnDecoder, ColumnEncoder, DecodedColumn};

/// One workload, in both representations of the same numbers.
struct Workload {
    name: &'static str,
    /// Digits after the decimal point.
    scale: u8,
    /// The unscaled integers — what a decimal column stores.
    mantissas: Vec<i128>,
    /// The minimum ratio the decimal column must reach.
    min_ratio: f64,
    /// The most the decimal column may cost against the same values stored
    /// as `f64`, **in bytes per block**. `None` where the comparison is
    /// meaningless because an `f64` cannot hold the values at all.
    ///
    /// Bytes rather than a ratio, because the difference is a constant: a
    /// decimal block carries a form byte and the tag of the integer block it
    /// delegates to. On a column that already compresses a thousandfold that
    /// is 3% and on one that compresses twofold it is nothing, and neither
    /// number says anything about the codec.
    max_overhead_bytes: Option<usize>,
}

impl Workload {
    /// The same values as `f64`, which is what they would have been stored
    /// as before the exact type existed.
    #[allow(clippy::cast_precision_loss)]
    fn floats(&self) -> Vec<f64> {
        let divisor = 10f64.powi(i32::from(self.scale));
        self.mantissas.iter().map(|&m| m as f64 / divisor).collect()
    }
}

fn workloads() -> Vec<Workload> {
    vec![
        Workload {
            // A quarter-hour settlement register in Wh at four decimal
            // places: strictly rising, roughly constant increment. This is
            // the design partner's `Z1NB¼`.
            name: "quarter-hour register, Wh, 4dp, monotone",
            scale: 4,
            mantissas: (0..8192).map(|i| 10_000_000_000 + i * 2_500_000).collect(),
            min_ratio: 8.0,
            max_overhead_bytes: Some(2),
        },
        Workload {
            // A tariff that does not move. Constant columns are the shape a
            // settlement store is full of.
            name: "tariff, EUR/kWh, 5dp, constant",
            scale: 5,
            mantissas: vec![28_500; 8192],
            min_ratio: 100.0,
            max_overhead_bytes: Some(2),
        },
        Workload {
            // Instantaneous power at the connection point, 2dp, noisy.
            name: "connection-point power, W, 2dp, noisy",
            scale: 2,
            mantissas: (0..8192).map(|i| 23_000 + (i * 37) % 4_000).collect(),
            min_ratio: 2.0,
            max_overhead_bytes: Some(0),
        },
        Workload {
            // Beyond an i64 mantissa, so the wide form is what encodes it.
            // No claim that it beats a float — an f64 cannot hold these
            // values at all, which is the point.
            name: "38-digit quantity, 2dp, wide form",
            scale: 2,
            mantissas: (0..4096)
                .map(|i| 99_999_999_999_999_999_999_999_999_999_999_000_000 + i128::from(i))
                .collect(),
            min_ratio: 1.5,
            max_overhead_bytes: None,
        },
    ]
}

fn ratio(raw: usize, encoded: usize) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    {
        raw as f64 / encoded as f64
    }
}

#[test]
fn decimal_codec_ratios_hold() {
    println!(
        "\n{:<44} {:>10} {:>10} {:>10}",
        "workload", "decimal", "as f64", "vs f64"
    );
    for w in workloads() {
        let block = ColumnEncoder::encode_decimal(&w.mantissas).unwrap();
        // The tag byte counts: it is what the segment writes.
        let decimal_bytes = block.to_bytes().len();
        // Raw is what the column would occupy uncompressed. A mantissa is an
        // i128 on the way in, but the honest comparison is against the 8
        // bytes the same number occupies as an `f64` — the representation
        // this type replaces — so the ratio is not inflated by the width of
        // the in-memory form.
        let raw = w.mantissas.len() * 8;
        let float_block = ColumnEncoder::encode_f64(&w.floats(), FloatEncoding::Chimp).unwrap();
        let float_bytes = float_block.to_bytes().len();

        // The float columns of a workload an `f64` cannot represent are not
        // a comparison — every value there rounds to the same double, so the
        // float "column" is a constant and compresses spectacularly while
        // holding none of the data. Printing that number invites quoting it.
        if w.max_overhead_bytes.is_some() {
            println!(
                "{:<44} {:>9.1}x {:>9.1}x {:>9.2}x",
                w.name,
                ratio(raw, decimal_bytes),
                ratio(raw, float_bytes),
                ratio(float_bytes, decimal_bytes),
            );
        } else {
            println!(
                "{:<44} {:>9.1}x {:>10} {:>10}",
                w.name,
                ratio(raw, decimal_bytes),
                "n/a",
                "n/a",
            );
        }

        assert!(
            ratio(raw, decimal_bytes) >= w.min_ratio,
            "{}: decimal reached {:.1}x, below the documented {:.1}x",
            w.name,
            ratio(raw, decimal_bytes),
            w.min_ratio
        );
        if let Some(limit) = w.max_overhead_bytes {
            assert!(
                decimal_bytes <= float_bytes + limit,
                "{}: the exact form cost {decimal_bytes} B against the float form's \
                 {float_bytes} B — more than the documented {limit} bytes of block header",
                w.name
            );
        }

        // Lossless, which is the only property that actually matters.
        match ColumnDecoder::decode(&block).unwrap() {
            DecodedColumn::Decimal(back) => assert_eq!(back, w.mantissas, "{}", w.name),
            other => panic!("{}: decoded as {other:?}", w.name),
        }
    }
    println!();
}

#[test]
fn a_constant_decimal_column_costs_almost_nothing() {
    // The claim in the encoding notes: a decimal column inherits RLE from
    // the integer stack, so a tariff that does not change for a day of
    // quarter-hours is a handful of bytes.
    let values = vec![28_500i128; 96];
    let block = ColumnEncoder::encode_decimal(&values).unwrap();
    assert!(
        block.to_bytes().len() < 32,
        "a constant column took {} bytes",
        block.to_bytes().len()
    );
}

#[test]
fn the_narrow_form_is_what_realistic_registers_use() {
    // If this ever stops holding, the "inherits the whole i64 stack" claim
    // stops holding with it. A register in Wh at 4dp needs 19 digits to
    // reach 10^14 kWh, which is inside an i64.
    // 100 TWh is 10^14 Wh; four decimal places make the mantissa 10^18,
    // inside an i64's 9.2 × 10^18.
    let a_hundred_terawatt_hours_in_wh_at_4dp: i128 = 100_000_000_000_000 * 10_000;
    assert!(i64::try_from(a_hundred_terawatt_hours_in_wh_at_4dp).is_ok());

    let block = ColumnEncoder::encode_decimal(&[a_hundred_terawatt_hours_in_wh_at_4dp]).unwrap();
    // Form byte 0 is the narrow form; the byte after the encoding tag.
    assert_eq!(block.payload[0], 0, "expected the narrow form");
}

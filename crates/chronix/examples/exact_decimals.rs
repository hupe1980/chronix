#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Exact decimals — meter registers a settlement is computed from
//!
//! The two kinds of number a metering gateway records, in one database:
//!
//! - **Instantaneous power** at the connection point, sampled every second.
//!   A measurement. `f64` is right for it, and the compression stack is
//!   built around it.
//! - **Quarter-hour registers** — `Z1NB¼` and its siblings, under the German
//!   settlement rules. A *legal quantity*: someone is billed from these, and
//!   a settlement that went through a `double` is one nobody can reproduce.
//!   `Decimal` is right for those, and nothing else is.
//!
//! ```sh
//! cargo run -p chronix --example exact_decimals
//! ```

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;
    let db = Chronix::open(config)?;
    println!("📂 {}", dir.path().display());

    // ── 1. Declare the register's scale before the first write ─
    //
    // A decimal column stores a fixed number of fractional digits, and it is
    // fixed by whatever creates the column. Declaring it means the first
    // meter reading does not get to decide: a device that happens to report
    // `231.4` before `231.45` would otherwise pin the column at one digit
    // and every later reading would be refused.
    db.declare_field("meter", "z1nb_q", ColumnType::Decimal { scale: 4 })?;
    println!("✅ meter.z1nb_q declared as decimal(38, 4) — kWh to four places");

    // ── 2. Write both kinds of number ──────────────────────────
    let meter = SeriesKey::new("meter", tags! { "device" => "main" })?;
    let base = 1_700_000_000_000_000_000_i64;
    let quarter_hour = 900_000_000_000_i64;

    // Four quarter-hour registers. In an f64 these four values sum to
    // 1.0000000000000002; here they sum to exactly 1.
    let readings = ["0.1", "0.2", "0.3", "0.4"];
    for (i, text) in readings.iter().enumerate() {
        let value: Decimal = text.parse()?;
        db.insert(&Point::new(
            meter.clone(),
            fields! { "z1nb_q" => value },
            base + i as i64 * quarter_hour,
        )?)?;
    }

    // A 1 s diagnostic series in the same database, as an ordinary float.
    let power = SeriesKey::new("power", tags! { "device" => "main" })?;
    for i in 0..60 {
        db.insert(&Point::new(
            power.clone(),
            fields! { "watts" => 231.45 + f64::from(i % 7) },
            base + i64::from(i) * 1_000_000_000,
        )?)?;
    }
    db.flush()?;
    println!("✅ 4 registers + 60 s of power written and flushed to a segment");

    // ── 3. A narrower value is widened, not rejected ───────────
    //
    // `1.5` into a scale-4 column is stored as `1.5000`. The value is
    // unchanged; only the number of digits it is written with.
    db.insert(&Point::new(
        meter.clone(),
        fields! { "z1nb_q" => "1.5".parse::<Decimal>()? },
        base + 4 * quarter_hour,
    )?)?;

    // ── 4. A value that would have to be rounded is refused ────
    let err = db
        .insert(&Point::new(
            meter,
            fields! { "z1nb_q" => "1.00005".parse::<Decimal>()? },
            base + 5 * quarter_hour,
        )?)
        .unwrap_err();
    println!("🚫 five decimal places into a four-place column:\n   {err}");

    // ── 5. The sum is the sum ──────────────────────────────────
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Sum)
        .field("z1nb_q")
        .build()?;
    let batch = db.execute(&plan)?;
    let column = batch.column_by_name("z1nb_q_sum").expect("a sum column");
    let sum = column
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .expect("a decimal sum stays a decimal");
    let scale = u8::try_from(sum.scale())?;
    println!(
        "\n∑ z1nb_q = {}   over 0.1 + 0.2 + 0.3 + 0.4 + 1.5",
        Decimal::new(sum.value(0), scale)?
    );
    // The same four registers added as doubles, printed with enough digits
    // to see what happened to them.
    println!(
        "   the same four registers in an f64: {:.17}",
        0.1 + 0.2 + 0.3 + 0.4
    );

    // ── 6. And SQL sees a DECIMAL(38, 4) ───────────────────────
    #[cfg(feature = "sql")]
    {
        let batches = db.sql("SELECT sum(z1nb_q) AS total FROM meter")?;
        println!(
            "   SQL says the column is {}",
            batches[0].schema().field(0).data_type()
        );
    }

    // The 1 s series is untouched by any of this: a float column in the
    // same database, on the same disk, with the same compression.
    let plan = db
        .query()
        .measurement("power")
        .range(i64::MIN, i64::MAX)
        .aggregate(AggFn::Avg)
        .field("watts")
        .build()?;
    let batch = db.execute(&plan)?;
    let avg = batch
        .column_by_name("watts_avg")
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::Float64Array>())
        .map(|a| a.value(0))
        .unwrap_or_default();
    println!("   avg(watts) over the 1 s series = {avg:.4} (a float, as it should be)");

    db.close()?;
    Ok(())
}

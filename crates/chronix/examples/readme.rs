use chronix::arrow::util::pretty::print_batches;
use chronix::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Chronix::open_small("./chronix-data")?; // the gateway preset
    let key = SeriesKey::new("solar", tags! { "panel" => "roof" })?;
    let t0 = 1_700_000_000_000_000_000_i64; // nanoseconds since the epoch
    for i in 0..60 {
        let watts = 100.0 + 20.0 * f64::from(i); // a morning ramp, one point a minute
        let at = t0 + i64::from(i) * 60_000_000_000;
        db.insert(&Point::new(key.clone(), fields! { "watts" => watts }, at)?)?;
    }
    let sql = "SELECT time_bucket('10m', _time) AS t, avg(watts) FROM solar GROUP BY t ORDER BY t";
    print_batches(&db.sql(sql)?)?;
    let next = db.auto_forecast("solar", "watts", &[], t0, t0 + 3_600_000_000_000, 10, None)?;
    let model = &next.selection.label; // picked by cross-validation
    println!("{model} forecasts {:?} …", &next.result.values[..3]);
    db.close()?;
    std::fs::remove_dir_all("./chronix-data")?;
    Ok(())
}

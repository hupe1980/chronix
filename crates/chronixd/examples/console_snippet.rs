use chronix::Chronix;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Chronix::open_small("./data")?;

    // 2026-11-17T01:00:00Z to 2026-11-17T07:00:00Z, one point every 30s
    let result = db.promql_range(
        "pv_power_w",
        1_794_877_200_000_000_000,
        1_794_898_800_000_000_000,
        30_000_000_000,
    )?;
    println!("{result:?}");
    Ok(())
}

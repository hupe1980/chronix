//! `chronixd --demo`: a generated gateway week in a temporary directory.
//!
//! The dataset is a house with a PV array, a battery, a grid connection and a
//! heat pump, sampled every 10 s for the seven days before startup, with two
//! anomalies planted where the banner's queries find them. It is generated
//! from a fixed seed, so two runs started at the same moment hold the same
//! data, and it is written before the listener binds, so the first request
//! already sees all of it.
//!
//! The demo binds loopback unless `--bind` says otherwise, has no
//! authentication, and refuses `--data-dir` and `--config`: it never writes
//! into a real directory.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use chronix::Chronix;
use chronix::prelude::*;

use crate::cli::ServerArgs;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::http::AppState;
use crate::server::{Extension, ExtensionContext};

/// Sample interval: 10 s.
pub const STEP_NS: i64 = 10 * NS_PER_SEC;
/// The generated window: seven days.
pub const WINDOW_NS: i64 = 7 * DAY_NS;
/// The fixed seed every run starts from.
pub const SEED: u64 = 42;

const NS_PER_SEC: i64 = 1_000_000_000;
const DAY_NS: i64 = 86_400 * NS_PER_SEC;
const HOUR_NS: i64 = 3_600 * NS_PER_SEC;
const MIN_NS: i64 = 60 * NS_PER_SEC;

/// The address the demo serves on when `--bind` is not given.
pub const DEFAULT_BIND: &str = "127.0.0.1:8086";

/// The banner's queries. The tests run exactly these strings, so a banner
/// line that stops answering fails the build rather than a first impression.
pub mod queries {
    /// PromQL: the PV array's current output.
    pub const PROMQL: &str = "pv_power_w";
    /// SQL: average power per device.
    pub const SQL: &str = "SELECT device, avg(power_w) FROM pv GROUP BY device";
    /// SQL: the next hour of PV output, forecast in the engine.
    pub const FORECAST: &str = "SELECT unnest(forecast(power_w, _time, 360)) AS next_hour FROM pv";
    /// SQL: the heat pump's five most anomalous samples. `anomaly_score` is
    /// 0.5 at the threshold (3 standard deviations) and near 1 above it.
    pub const ANOMALIES: &str = "SELECT * FROM (SELECT _time, power_w, \
        anomaly_score(power_w, 3.0) OVER (PARTITION BY device ORDER BY _time) AS score \
        FROM heat_pump) WHERE score > 0.5 ORDER BY score DESC LIMIT 5";
    /// SQL: the PV dropout, found as the two largest step changes.
    pub const PV_DROPOUT: &str = "SELECT * FROM (SELECT _time, \
        anomaly_score(d, 3.0) OVER (PARTITION BY device ORDER BY _time) AS score \
        FROM (SELECT _time, device, power_w - lag(power_w) \
        OVER (PARTITION BY device ORDER BY _time) AS d FROM pv) WHERE d IS NOT NULL) \
        ORDER BY score DESC LIMIT 5";
}

/// The two anomalies the generator plants, as `[start, end)` in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Planted {
    /// `pv_power_w` drops to 0 W ("inverter trip"), 20 minutes at noon.
    pub pv_dropout: (i64, i64),
    /// `heat_pump_power_w` jumps to 2 700 W ("defrost stuck"), 15 minutes.
    pub heat_pump_spike: (i64, i64),
}

impl Planted {
    /// Where the anomalies sit in the window ending at `end_ns`.
    #[must_use]
    pub fn for_window(end_ns: i64) -> Self {
        let day0 = floor_day(end_ns - WINDOW_NS);
        let at = |day: i64, h: i64, m: i64| day0 + day * DAY_NS + h * HOUR_NS + m * MIN_NS;
        Self {
            pv_dropout: (at(2, 12, 0), at(2, 12, 20)),
            heat_pump_spike: (at(4, 3, 10), at(4, 3, 25)),
        }
    }
}

/// The end of the demo window for a wall-clock time: floored to the step,
/// so the newest sample is at most 10 s old and an instant PromQL query at
/// "now" finds it.
#[must_use]
pub fn window_end(now_ns: i64) -> i64 {
    now_ns - now_ns.rem_euclid(STEP_NS)
}

fn floor_day(ns: i64) -> i64 {
    ns - ns.rem_euclid(DAY_NS)
}

/// SplitMix64: small, fast and stable across dependency upgrades, which a
/// generator whose output tests hash needs more than statistical quality.
struct SplitMix(u64);

impl SplitMix {
    fn next_f64(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // 53 random bits into [0, 1).
        #[allow(clippy::cast_precision_loss)]
        let f = (z >> 11) as f64 / (1_u64 << 53) as f64;
        f
    }
}

/// The house's state between samples.
struct House {
    rng: SplitMix,
    cloud: f64,
    soc_pct: f64,
}

/// One sample of every series at one instant.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    /// Nanoseconds since the epoch.
    pub ts: i64,
    /// PV output, W.
    pub pv_w: f64,
    /// Battery power, W: positive charging, negative discharging.
    pub battery_w: f64,
    /// Battery state of charge, %.
    pub soc_pct: f64,
    /// Grid import, W.
    pub grid_import_w: f64,
    /// Heat pump electrical power, W.
    pub heat_pump_w: f64,
    /// Heat pump flow temperature, °C.
    pub flow_temp_c: f64,
}

impl House {
    fn new() -> Self {
        Self {
            rng: SplitMix(SEED),
            cloud: 0.85,
            soc_pct: 50.0,
        }
    }

    fn sample(&mut self, ts: i64, planted: &Planted) -> Sample {
        #[allow(clippy::cast_precision_loss)]
        let h = ts.rem_euclid(DAY_NS) as f64 / HOUR_NS as f64;
        let within = |(a, b): (i64, i64)| ts >= a && ts < b;

        // Clouds: a slow mean-reverting walk between 0.55 and 1.0.
        let shock = (self.rng.next_f64() - 0.5) * 0.04;
        self.cloud = (self.cloud + 0.002 * (0.8 - self.cloud) + shock).clamp(0.55, 1.0);
        let mut pv = if (6.0..20.0).contains(&h) {
            6000.0 * (std::f64::consts::PI * (h - 6.0) / 14.0).sin() * self.cloud
        } else {
            0.0
        };
        if within(planted.pv_dropout) {
            pv = 0.0;
        }

        // Heat pump: 20 minutes on, 25 off, overnight and in the evening.
        let minute = ts.rem_euclid(DAY_NS) / MIN_NS;
        let scheduled = !(9.0..17.0).contains(&h);
        let mut heat_pump = if scheduled && minute % 45 < 20 {
            900.0
        } else {
            0.0
        };
        if within(planted.heat_pump_spike) {
            heat_pump = 2700.0;
        }
        let flow_temp = 35.0 + 10.0 * heat_pump / 900.0;

        let base = if (7.0..9.0).contains(&h) || (18.0..22.0).contains(&h) {
            1550.0
        } else {
            350.0
        };
        let load = base + heat_pump;

        // Battery: charge from surplus, discharge in the evening, 10 kWh.
        let surplus = pv - load;
        let mut battery = if surplus > 0.0 && self.soc_pct < 95.0 {
            surplus.min(3000.0)
        } else if (17.0..23.0).contains(&h) && self.soc_pct > 10.0 {
            -(load - pv).clamp(0.0, 2500.0)
        } else {
            0.0
        };
        #[allow(clippy::cast_precision_loss)]
        let step_h = STEP_NS as f64 / HOUR_NS as f64;
        let next_soc = self.soc_pct + battery * step_h / 10_000.0 * 100.0;
        if !(10.0..=95.0).contains(&next_soc) {
            battery = 0.0;
        } else {
            self.soc_pct = next_soc;
        }

        let grid = (load + battery - pv).max(0.0);
        let round = |v: f64| (v * 10.0).round() / 10.0;
        Sample {
            ts,
            pv_w: round(pv),
            battery_w: round(battery),
            soc_pct: (self.soc_pct * 100.0).round() / 100.0,
            grid_import_w: round(grid),
            heat_pump_w: round(heat_pump),
            flow_temp_c: round(flow_temp),
        }
    }
}

/// Every sample of the window ending at `end_ns`, oldest first.
#[must_use]
pub fn generate(end_ns: i64) -> Vec<Sample> {
    let planted = Planted::for_window(end_ns);
    let mut house = House::new();
    let start = end_ns - WINDOW_NS;
    (0..=(WINDOW_NS / STEP_NS))
        .map(|i| house.sample(start + i * STEP_NS, &planted))
        .collect()
}

fn key(measurement: &str) -> Result<SeriesKey, chronix_core::SchemaError> {
    SeriesKey::new(
        measurement,
        BTreeMap::from([
            ("device".to_owned(), measurement.to_owned()),
            ("site".to_owned(), "demo".to_owned()),
        ]),
    )
}

/// Write the window ending at `end_ns` into `db`; returns the points written.
///
/// # Errors
///
/// Any write the database refuses.
pub fn ingest(db: &Chronix, end_ns: i64) -> Result<usize, DbError> {
    let schema = |e: chronix_core::SchemaError| DbError::InvalidRequest(e.to_string());
    let (pv, battery, grid, heat_pump) = (
        key("pv").map_err(schema)?,
        key("battery").map_err(schema)?,
        key("grid").map_err(schema)?,
        key("heat_pump").map_err(schema)?,
    );
    let samples = generate(end_ns);
    let mut written = 0;
    // One day per call: one WAL record each, and a bounded batch in memory.
    for chunk in samples.chunks((DAY_NS / STEP_NS) as usize) {
        let mut points = Vec::with_capacity(chunk.len() * 4);
        for s in chunk {
            let point = |k: &SeriesKey, f: BTreeMap<String, FieldValue>| {
                Point::new(k.clone(), f, s.ts).map_err(schema)
            };
            points.push(point(&pv, fields! { "power_w" => s.pv_w })?);
            points.push(point(
                &battery,
                fields! { "power_w" => s.battery_w, "soc_pct" => s.soc_pct },
            )?);
            points.push(point(&grid, fields! { "import_w" => s.grid_import_w })?);
            points.push(point(
                &heat_pump,
                fields! { "power_w" => s.heat_pump_w, "flow_temp_c" => s.flow_temp_c },
            )?);
        }
        // History is written through `backfill`: a week in the past is
        // outside the live out-of-order window by design.
        let result = db.backfill(&points)?;
        written += result.accepted;
    }
    Ok(written)
}

/// The text printed once the listener is bound.
#[must_use]
pub fn banner(addr: SocketAddr, planted: &Planted, console: Option<&str>) -> String {
    use std::fmt::Write as _;
    let sql = |q: &str| {
        format!(
            "curl -s http://{addr}/api/v1/chronix/sql -H 'content-type: application/json' \\\n               -d '{{\"query\":\"{q}\"}}'"
        )
    };
    let mut out = String::new();
    let _ = writeln!(
        out,
        "chronix demo — a generated gateway week (seed {SEED}) in a temporary directory, deleted on exit."
    );
    let _ = writeln!(out, "No authentication. Bound to {addr} only.\n");
    if let Some(path) = console {
        let _ = writeln!(out, "  console    http://{addr}{path}");
    }
    let _ = writeln!(
        out,
        "  PromQL     curl -s 'http://{addr}/api/v1/query?query={}'",
        queries::PROMQL
    );
    let _ = writeln!(out, "  SQL        {}", sql(queries::SQL));
    let _ = writeln!(out, "  forecast   {}", sql(queries::FORECAST));
    let _ = writeln!(out, "  anomalies  {}", sql(queries::ANOMALIES));
    let _ = writeln!(
        out,
        "  Grafana    add a Prometheus data source at http://{addr}\n"
    );
    let _ = writeln!(
        out,
        "Planted anomalies: pv_power_w = 0 W on {}; heat_pump_power_w at 2 700 W on {}.",
        window_text(planted.pv_dropout),
        window_text(planted.heat_pump_spike),
    );
    out
}

fn window_text((start, end): (i64, i64)) -> String {
    let fmt = |ns: i64, f: &str| {
        chrono::DateTime::from_timestamp_nanos(ns)
            .format(f)
            .to_string()
    };
    format!("{}–{} UTC", fmt(start, "%Y-%m-%d %H:%M"), fmt(end, "%H:%M"))
}

/// Ingests the week when the server starts and prints the banner once the
/// listener answers.
struct DemoExtension {
    end_ns: i64,
    addr: SocketAddr,
    console: Option<&'static str>,
}

#[async_trait::async_trait]
impl Extension for DemoExtension {
    fn name(&self) -> &'static str {
        "demo"
    }

    async fn start(&self, ctx: ExtensionContext) -> Result<Router<AppState>, ServerError> {
        let db = Arc::clone(&ctx.db);
        let end_ns = self.end_ns;
        let started = Instant::now();
        let written = tokio::task::spawn_blocking(move || ingest(&db, end_ns))
            .await
            .map_err(|e| ServerError::Internal(format!("demo generator panicked: {e}")))??;
        tracing::info!(
            points = written,
            seconds = started.elapsed().as_secs_f64(),
            "demo data written"
        );

        let (addr, console) = (self.addr, self.console);
        let planted = Planted::for_window(end_ns);
        tokio::spawn(async move {
            // The banner is the promise that the commands in it work, so it
            // is printed only once the listener accepts a connection.
            let deadline = Instant::now() + Duration::from_secs(30);
            while tokio::net::TcpStream::connect(addr).await.is_err() {
                if Instant::now() > deadline {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            eprint!("\n{}", banner(addr, &planted, console));
        });
        Ok(Router::new())
    }

    async fn stop(&self) {}
}

/// Why `--demo` did not start.
#[derive(Debug)]
pub enum DemoError {
    /// `--data-dir` or `--config` was given (exit status 2).
    RealDirectory,
    /// The bind address is taken (exit status 1).
    PortInUse(SocketAddr),
    /// Anything else (exit status 1).
    Other(String),
}

impl std::fmt::Display for DemoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RealDirectory => f.write_str(
                "--demo never writes into a real directory; drop --data-dir/--config, or run without --demo",
            ),
            Self::PortInUse(addr) => write!(
                f,
                "{addr} is in use; try --demo --bind {}:0",
                addr.ip()
            ),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for DemoError {}

/// A free port on `ip`, chosen by the operating system.
fn free_port(ip: std::net::IpAddr) -> Result<SocketAddr, DemoError> {
    TcpListener::bind((ip, 0))
        .and_then(|l| l.local_addr())
        .map_err(|e| DemoError::Other(format!("no free port on {ip}: {e}")))
}

/// Run the demo until Ctrl-C; the temporary directory is removed on return.
///
/// Only `--bind`, `--log-format` and `--log-level` are honoured; the
/// configuration file and the environment are not read, so nothing a
/// deployment has set can point the demo at real data.
///
/// # Errors
///
/// [`DemoError`], or the server's own error.
pub async fn run(
    args: &ServerArgs,
    console: Option<&'static str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.data_dir.is_some() || args.config.is_some() {
        return Err(Box::new(DemoError::RealDirectory));
    }
    let requested: SocketAddr = args
        .bind
        .as_deref()
        .unwrap_or(DEFAULT_BIND)
        .parse()
        .map_err(|e| DemoError::Other(format!("invalid bind address: {e}")))?;
    // Port 0 means "any": choose it now, so the banner can name it.
    let http_addr = if requested.port() == 0 {
        free_port(requested.ip())?
    } else {
        TcpListener::bind(requested).map_err(|_| DemoError::PortInUse(requested))?;
        requested
    };

    let dir = tempfile::Builder::new()
        .prefix("chronix-demo-")
        .tempdir()
        .map_err(|e| DemoError::Other(format!("cannot create a temporary directory: {e}")))?;
    let mut config = ServerConfig::default();
    config.database.data_dir = dir.path().join("data");
    config.server.http_addr = http_addr;
    // gRPC and Flight SQL on free loopback ports, so a second demo or a real
    // server on the default ports does not stop this one.
    config.server.grpc_addr = Some(free_port(http_addr.ip())?);
    config.server.flight_addr = Some(free_port(http_addr.ip())?);
    config.server.log_level = args.log_level.clone().unwrap_or_else(|| "warn".to_owned());
    if let Some(format) = args.log_format {
        config.server.log_format = format;
    }

    let now = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| DemoError::Other("the system clock is out of range".to_owned()))?;
    let demo = DemoExtension {
        end_ns: window_end(now),
        addr: http_addr,
        console,
    };
    let result = crate::cli::serve(args, config, vec![Box::new(demo)]).await;
    drop(dir);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const END: i64 = 1_763_380_800_000_000_000; // 2025-11-17 12:00:00 UTC

    #[test]
    fn window_is_a_week_of_ten_second_samples() {
        let s = generate(END);
        assert_eq!(s.len() as i64, WINDOW_NS / STEP_NS + 1);
        assert_eq!(s.last().map(|x| x.ts), Some(END));
        assert!(s.windows(2).all(|w| w[1].ts - w[0].ts == STEP_NS));
    }

    #[test]
    fn values_stay_physical() {
        for s in generate(END) {
            assert!((0.0..=6000.0).contains(&s.pv_w), "{s:?}");
            assert!((10.0..=95.0).contains(&s.soc_pct), "{s:?}");
            assert!(s.grid_import_w >= 0.0, "{s:?}");
            assert!((-2500.0..=3000.0).contains(&s.battery_w), "{s:?}");
        }
    }

    #[test]
    fn planted_windows_hold_the_anomalies() {
        let p = Planted::for_window(END);
        let s = generate(END);
        let inside = |(a, b): (i64, i64), x: &Sample| x.ts >= a && x.ts < b;
        let dropout: Vec<_> = s.iter().filter(|x| inside(p.pv_dropout, x)).collect();
        assert_eq!(dropout.len(), 120);
        assert!(dropout.iter().all(|x| x.pv_w == 0.0));
        let spike: Vec<_> = s.iter().filter(|x| inside(p.heat_pump_spike, x)).collect();
        assert_eq!(spike.len(), 90);
        assert!(
            spike
                .iter()
                .all(|x| (x.heat_pump_w - 2700.0).abs() < f64::EPSILON)
        );
        // The dropout is at noon on a day the sun shines: the minutes before it are not dark.
        let before = s.iter().find(|x| x.ts == p.pv_dropout.0 - STEP_NS).unwrap();
        assert!(before.pv_w > 2500.0, "{before:?}");
    }

    #[test]
    fn banner_names_the_address_and_the_windows() {
        let addr: SocketAddr = "127.0.0.1:8086".parse().unwrap();
        let b = banner(addr, &Planted::for_window(END), None);
        assert!(b.contains("Bound to 127.0.0.1:8086 only."));
        assert!(b.contains("query=pv_power_w"));
        assert!(!b.contains("console"));
        assert!(b.contains("2025-11-12 12:00–12:20 UTC"), "{b}");
        assert!(b.contains("2025-11-14 03:10–03:25 UTC"), "{b}");
        let with = banner(addr, &Planted::for_window(END), Some("/console"));
        assert!(with.contains("console    http://127.0.0.1:8086/console"));
    }
}

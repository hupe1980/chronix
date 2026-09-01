#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # PromQL Queries
//!
//! Demonstrates PromQL instant and range queries against Chronix,
//! including `rate()`, `avg_over_time()`, aggregation operators,
//! and label matching.
//!
//! ```sh
//! cargo run -p chronix --example promql_queries
//! ```

use std::sync::Arc;

use chronix::prelude::*;
use chronix::promql::eval::QueryParams;
use chronix::promql::{parse, PromQLEvaluator, PromQLValue};
use chronix::{fields, tags, Chronix};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;

    let config = ChronixConfig::builder().data_dir(dir.path()).build()?;

    let db = Arc::new(Chronix::open(config)?);

    // ── Seed Prometheus-style metrics ──────────────────────────
    // PromQL convention: single field named "value"
    let base_ts = 1_700_000_000_000_000_000_i64;
    let step_ns = 15_000_000_000_i64; // 15-second scrape interval

    // up{job="api", instance="inst-1"} = monotonic counter
    for i in 0..40 {
        let key = SeriesKey::new(
            "http_requests_total",
            tags! { "job" => "api", "instance" => "inst-1", "method" => "GET" },
        )?;
        let point = Point::new(
            key,
            fields! { "value" => (i as f64) * 10.0 },
            base_ts + i * step_ns,
        )?;
        db.insert(&point)?;
    }

    for i in 0..40 {
        let key = SeriesKey::new(
            "http_requests_total",
            tags! { "job" => "api", "instance" => "inst-2", "method" => "POST" },
        )?;
        let point = Point::new(
            key,
            fields! { "value" => (i as f64) * 7.0 },
            base_ts + i * step_ns,
        )?;
        db.insert(&point)?;
    }

    // Gauge metric: cpu_usage
    for i in 0..40 {
        for (inst, base_cpu) in [("inst-1", 45.0), ("inst-2", 65.0)] {
            let key = SeriesKey::new("cpu_usage", tags! { "job" => "api", "instance" => inst })?;
            let point = Point::new(
                key,
                fields! { "value" => base_cpu + (i as f64 * 0.1).sin() * 10.0 },
                base_ts + i * step_ns,
            )?;
            db.insert(&point)?;
        }
    }

    println!("✅ Seeded PromQL metrics (2 counters + 2 gauges, 40 samples each)\n");

    let evaluator = PromQLEvaluator::new(db.clone());
    let eval_time = base_ts + 39 * step_ns; // latest sample

    // ── 1. Instant vector selector ─────────────────────────────
    println!("─── 1. Instant: http_requests_total{{job=\"api\"}} ───");
    run_instant(&evaluator, r#"http_requests_total{job="api"}"#, eval_time)?;

    // ── 2. rate() — per-second counter rate ────────────────────
    println!("\n─── 2. rate(http_requests_total[5m]) ───");
    run_instant(
        &evaluator,
        r#"rate(http_requests_total{job="api"}[5m])"#,
        eval_time,
    )?;

    // ── 3. avg_over_time() — gauge smoothing ───────────────────
    println!("\n─── 3. avg_over_time(cpu_usage[2m]) ───");
    run_instant(
        &evaluator,
        r#"avg_over_time(cpu_usage{job="api"}[2m])"#,
        eval_time,
    )?;

    // ── 4. Aggregation: sum by (method) ────────────────────────
    println!("\n─── 4. sum by (method) (rate(http_requests_total[5m])) ───");
    run_instant(
        &evaluator,
        r#"sum by (method) (rate(http_requests_total[5m]))"#,
        eval_time,
    )?;

    // ── 5. Binary operation: ratio ─────────────────────────────
    println!("\n─── 5. Arithmetic: cpu_usage > 50 ───");
    run_instant(&evaluator, r#"cpu_usage{job="api"} > 50"#, eval_time)?;

    // ── 6. Range query ─────────────────────────────────────────
    println!("\n─── 6. Range query: rate(http_requests_total[1m]) over 2 minutes ───");
    let start = eval_time - 120_000_000_000; // 2 minutes before eval_time
    let step = 30_000_000_000_i64; // 30s step

    let expr = parse(r#"rate(http_requests_total{instance="inst-1"}[1m])"#)?;
    let params = QueryParams {
        time: eval_time,
        start: Some(start),
        end: Some(eval_time),
        step: Some(step),
        ..Default::default()
    };

    let result = evaluator.range_query(&expr, &params)?;
    print_promql_result(&result);

    db.close()?;
    println!("\n✅ Done");

    Ok(())
}

/// Helper: run an instant query and print results.
fn run_instant(
    evaluator: &PromQLEvaluator,
    query: &str,
    eval_time: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("   query: {query}");
    let expr = parse(query)?;
    let params = QueryParams {
        time: eval_time,
        ..Default::default()
    };
    let result = evaluator.instant_query(&expr, &params)?;
    print_promql_result(&result);
    Ok(())
}

/// Pretty-print a PromQL result.
fn print_promql_result(result: &PromQLValue) {
    match result {
        PromQLValue::Scalar(v) => println!("   scalar: {v}"),
        PromQLValue::Vector(series_vec) => {
            for s in series_vec {
                let labels: Vec<String> = s
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{k}=\"{v}\""))
                    .collect();
                let label_str = labels.join(", ");
                for sample in &s.samples {
                    println!(
                        "   {{{label_str}}} => {} @{}",
                        sample.value, sample.timestamp
                    );
                }
            }
        }
        PromQLValue::Matrix(series_vec) => {
            for s in series_vec {
                let labels: Vec<String> = s
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{k}=\"{v}\""))
                    .collect();
                let label_str = labels.join(", ");
                println!("   {{{label_str}}}:");
                for sample in &s.samples {
                    println!("      {} @{}", sample.value, sample.timestamp);
                }
            }
        }
        PromQLValue::String(s) => println!("   string: {s}"),
    }
}

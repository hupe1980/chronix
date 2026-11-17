#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Multi-Tenant Isolation
//!
//! A namespace is the value of a series' `__namespace__` tag. The database
//! admits every new series against its namespace's limits — in the same step,
//! and from the same exact set of known series, as the database-wide
//! cardinality budget — so usage is never an estimate, and a delete or a
//! retention pass gives the headroom back.
//!
//! `chronixd` keeps each namespace's quota in its registry and pushes it into
//! the database at startup and on every change; an embedded host calls
//! [`Chronix::set_namespace_limits`] itself.
//!
//! ```bash
//! cargo run --example tenant_isolation
//! ```

use std::collections::BTreeMap;

use chronix::chronix_core::NAMESPACE_TAG;
use chronix::prelude::*;
use chronix::{Chronix, DbError, NamespaceLimits, fields};

fn point(namespace: &str, host: &str, ts: i64) -> Point {
    let mut tags = BTreeMap::new();
    tags.insert(NAMESPACE_TAG.to_string(), namespace.to_string());
    tags.insert("host".to_string(), host.to_string());
    Point::new(
        SeriesKey::new("cpu", tags).unwrap(),
        fields! { "usage" => 0.5 },
        ts,
    )
    .unwrap()
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap(),
    )
    .unwrap();

    // ── 1. Limits per tenant ──────────────────────────────────────────
    db.set_namespace_limits(
        "team-alpha",
        NamespaceLimits {
            max_series: 3,
            max_measurements: 10,
        },
    );
    println!("team-alpha may hold 3 series; team-beta is unlimited\n");

    // ── 2. Writes are admitted until the quota is full ─────────────────
    for host in ["web-1", "web-2", "web-3"] {
        db.insert(&point("team-alpha", host, 1)).unwrap();
    }
    for i in 0..10 {
        db.insert(&point("team-beta", &format!("db-{i}"), 1))
            .unwrap();
    }

    // ── 3. A fourth series is refused, whole, naming the quota ──────────
    let refused = db.insert(&point("team-alpha", "web-4", 1)).unwrap_err();
    assert!(matches!(refused, DbError::NamespaceQuotaExceeded { .. }));
    println!("refused: {refused}");
    // A series the tenant already has is still writable at the quota.
    db.insert(&point("team-alpha", "web-1", 2)).unwrap();

    // ── 4. Usage is exact ─────────────────────────────────────────────
    for ns in ["team-alpha", "team-beta"] {
        let usage = db.namespace_usage(ns);
        println!(
            "{ns:<10} {} series, {} measurement(s)",
            usage.series, usage.measurements
        );
    }

    // ── 5. A delete gives the headroom back, with no restart ───────────
    let mut tags = BTreeMap::new();
    tags.insert(NAMESPACE_TAG.to_string(), "team-alpha".to_string());
    tags.insert("host".to_string(), "web-3".to_string());
    db.delete_series("cpu", &tags).unwrap();
    db.insert(&point("team-alpha", "web-4", 3)).unwrap();
    println!(
        "\nafter deleting web-3, web-4 fits: team-alpha holds {} series",
        db.namespace_usage("team-alpha").series
    );

    db.close().unwrap();
}

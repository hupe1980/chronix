//! A trigger's `DELIVER` clause must produce delivery.
//!
//! It did not. The clause was parsed into a `DeliveryTarget`, converted to a
//! string, stored on the trigger definition, printed by `SHOW TRIGGERS` — and
//! then dropped. Three separate things were missing, and together they meant
//! every webhook trigger ever written fired into nothing:
//!
//! 1. **Nothing constructed a channel.** `DELIVER webhook('https://…')`
//!    validated the URL and stopped there; no `WebhookChannel` was ever built
//!    from it.
//! 2. **The router ignored the clause.** `DeliveryRouter::deliver` fanned
//!    every signal out to every registered channel, so even with channels
//!    present, two triggers could not deliver to two different places.
//! 3. **`nats(…)` and `mqtt(…)` parsed.** Broker delivery was removed long
//!    before, so those accepted a trigger that could never deliver, and the
//!    user's only clue was that nothing arrived.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chronix::{Pipeline, PipelineConfig};
use chronix_streaming::signal::error::Result as SignalResult;
use chronix_streaming::signal::{DeliveryChannel, Severity, SignalEvent};

/// A channel that records what it was handed, under a name we choose.
struct Recorder {
    name: String,
    seen: Arc<AtomicUsize>,
}

impl DeliveryChannel for Recorder {
    fn name(&self) -> &str {
        &self.name
    }
    fn deliver(&self, _event: &SignalEvent) -> SignalResult<()> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn signal_for(trigger: &str, targets: &[&str]) -> SignalEvent {
    SignalEvent {
        event_id: "e1".into(),
        trigger_id: trigger.into(),
        trigger_name: trigger.into(),
        measurement: "cpu".into(),
        tags: Default::default(),
        timestamp: 0,
        signal_type: "threshold".into(),
        severity: Severity::Warning,
        value: 1.0,
        metadata: Default::default(),
        delivery_targets: targets.iter().map(|s| (*s).to_string()).collect(),
    }
}

/// Two triggers naming two channels must reach one each, not both.
/// Long enough that a loaded machine does not fail the test, short enough
/// that a genuinely stuck worker does not hang the suite.
const FLUSH: std::time::Duration = std::time::Duration::from_secs(5);

#[test]
fn a_signal_goes_only_to_the_channels_its_trigger_named() {
    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "chan-a".into(),
        seen: a.clone(),
    }));
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "chan-b".into(),
        seen: b.clone(),
    }));

    // `deliver` queues on the channel's own worker; `flush` waits for it.
    pipeline
        .delivery_router()
        .deliver(&signal_for("t1", &["chan-a"]));
    assert!(pipeline.delivery_router().flush(FLUSH));
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 0, "chan-b was not asked for");

    pipeline
        .delivery_router()
        .deliver(&signal_for("t2", &["chan-b"]));
    assert!(pipeline.delivery_router().flush(FLUSH));
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);
}

/// A signal with no clause behind it — an anomaly alert — still goes
/// everywhere. That is the case the fan-out behaviour was right for.
#[test]
fn a_signal_with_no_targets_goes_to_every_channel() {
    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    for (name, seen) in [("chan-a", &a), ("chan-b", &b)] {
        pipeline.delivery_router().add_channel(Box::new(Recorder {
            name: name.into(),
            seen: seen.clone(),
        }));
    }

    pipeline
        .delivery_router()
        .deliver(&signal_for("alert", &[]));
    assert!(pipeline.delivery_router().flush(FLUSH));
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);
}

/// `DELIVER webhook(…)` must build the channel, and must refuse when it
/// cannot sign.
#[test]
fn a_webhook_trigger_needs_a_configured_signing_secret() {
    let pipeline = Pipeline::with_config(PipelineConfig::default());
    let err = pipeline
        .execute_signal_sql(
            "CREATE TRIGGER t ON cpu WHEN value > 1.0 \
             DELIVER webhook('https://alerts.example.com/hook')",
        )
        .expect_err("an unsignable webhook must be refused, not accepted and dropped");
    assert!(
        err.to_string().contains("webhook_signing_secret"),
        "the error must name what is missing, got: {err}"
    );
}

/// With a secret configured, the channel exists after the statement — and it
/// is named by its URL, so two endpoints are two channels.
#[test]
fn a_webhook_trigger_registers_a_channel_named_by_its_url() {
    let pipeline = Pipeline::with_config(PipelineConfig {
        webhook_signing_secret: Some("s3cret".into()),
        ..PipelineConfig::default()
    });

    pipeline
        .execute_signal_sql(
            "CREATE TRIGGER t1 ON cpu WHEN value > 1.0 \
             DELIVER webhook('https://alerts.example.com/a')",
        )
        .expect("a signable webhook trigger must be accepted");
    assert!(pipeline
        .delivery_router()
        .has_channel("webhook:https://alerts.example.com/a"));

    let before = pipeline.delivery_router().channel_count();
    pipeline
        .execute_signal_sql(
            "CREATE TRIGGER t2 ON cpu WHEN value > 2.0 \
             DELIVER webhook('https://alerts.example.com/b')",
        )
        .unwrap();
    assert_eq!(
        pipeline.delivery_router().channel_count(),
        before + 1,
        "a second URL is a second channel"
    );

    // The same URL again reuses the channel: it owns a runtime and a thread.
    pipeline
        .execute_signal_sql(
            "CREATE TRIGGER t3 ON cpu WHEN value > 3.0 \
             DELIVER webhook('https://alerts.example.com/a')",
        )
        .unwrap();
    assert_eq!(
        pipeline.delivery_router().channel_count(),
        before + 1,
        "the same URL must not build a second channel"
    );
}

/// A channel that was cut must be refused by name, with the alternative.
#[test]
fn broker_delivery_is_refused_with_the_alternative() {
    let pipeline = Pipeline::with_config(PipelineConfig::default());
    let err = pipeline
        .execute_signal_sql("CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER nats('a.b')")
        .expect_err("a channel that does not exist must be refused");
    assert!(err.to_string().contains("does not exist"));
}

/// The whole chain, with no network: a CDC event evaluates a trigger created
/// through SQL, and the signal reaches the one channel that trigger named.
///
/// This is the property the three defects above added up to breaking. Each of
/// them individually left `SHOW TRIGGERS` looking correct.
#[test]
fn a_write_reaches_the_channel_the_trigger_named() {
    use chronix_core::FieldValue;
    use chronix_streaming::cdc::CdcEvent;

    let pipeline = Pipeline::with_config(PipelineConfig {
        // The built-in log channel is off, and a recorder takes its name, so
        // the assertion is about routing rather than about logging.
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });

    let hot = Arc::new(AtomicUsize::new(0));
    let unused = Arc::new(AtomicUsize::new(0));
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "log".into(),
        seen: hot.clone(),
    }));
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "unused".into(),
        seen: unused.clone(),
    }));

    pipeline
        .execute_signal_sql("CREATE TRIGGER hot_cpu ON cpu WHEN value > 90.0 DELIVER log")
        .expect("the trigger must be accepted");

    let mut fields = std::collections::BTreeMap::new();
    fields.insert("value".to_string(), FieldValue::F64(95.0));
    let fired = pipeline.process_cdc_event(&CdcEvent::PointWritten {
        measurement: "cpu".into(),
        tags: std::collections::BTreeMap::new(),
        fields,
        timestamp: 1_000_000_000,
        seq: 1,
    });

    assert_eq!(fired, 1, "the trigger must fire on a breaching write");
    assert!(pipeline.delivery_router().flush(FLUSH));
    assert_eq!(
        hot.load(Ordering::SeqCst),
        1,
        "the signal must reach the channel the DELIVER clause named"
    );
    assert_eq!(unused.load(Ordering::SeqCst), 0, "and no other channel");

    // A write under the threshold fires nothing.
    let mut fields = std::collections::BTreeMap::new();
    fields.insert("value".to_string(), FieldValue::F64(10.0));
    pipeline.process_cdc_event(&CdcEvent::PointWritten {
        measurement: "cpu".into(),
        tags: std::collections::BTreeMap::new(),
        fields,
        timestamp: 2_000_000_000,
        seq: 2,
    });
    assert_eq!(hot.load(Ordering::SeqCst), 1);
}

// ── Multi-tenancy ───────────────────────────────────────────────────────
//
// A trigger names a *measurement*, and a measurement is shared: every tenant
// writing `cpu` writes the same measurement, told apart only by the namespace
// tag on the series. An unscoped trigger therefore fires on every tenant's
// data — and delivers one tenant's values to another tenant's webhook, which
// is a forgotten namespace filter with an outbound HTTP request attached.

fn point_event(namespace: Option<&str>, value: f64, seq: u64) -> chronix_streaming::cdc::CdcEvent {
    use chronix_core::FieldValue;
    let mut tags = std::collections::BTreeMap::new();
    if let Some(ns) = namespace {
        tags.insert(chronix_core::NAMESPACE_TAG.to_string(), ns.to_string());
    }
    let mut fields = std::collections::BTreeMap::new();
    fields.insert("value".to_string(), FieldValue::F64(value));
    chronix_streaming::cdc::CdcEvent::PointWritten {
        measurement: "cpu".into(),
        tags,
        fields,
        timestamp: seq as i64 * 1_000_000_000,
        seq,
    }
}

/// One tenant's trigger must not fire on another tenant's write.
#[test]
fn a_scoped_trigger_only_sees_its_own_namespace() {
    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    let seen = Arc::new(AtomicUsize::new(0));
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "log".into(),
        seen: seen.clone(),
    }));

    pipeline
        .execute_signal_sql_scoped(
            "CREATE TRIGGER hot ON cpu WHEN value > 90.0 DELIVER log",
            Some("tenant-a"),
        )
        .unwrap();

    // Tenant B breaches the threshold. Tenant A's trigger must ignore it.
    pipeline.process_cdc_event(&point_event(Some("tenant-b"), 99.0, 1));
    assert!(pipeline.delivery_router().flush(FLUSH));
    assert_eq!(
        seen.load(Ordering::SeqCst),
        0,
        "a trigger must not fire on another tenant's data"
    );

    // Tenant A breaches it.
    pipeline.process_cdc_event(&point_event(Some("tenant-a"), 99.0, 2));
    assert!(pipeline.delivery_router().flush(FLUSH));
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}

/// Two tenants may use the same trigger name, and neither can touch the
/// other's.
#[test]
fn trigger_names_are_per_namespace() {
    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "log".into(),
        seen: Arc::new(AtomicUsize::new(0)),
    }));

    for ns in ["tenant-a", "tenant-b"] {
        pipeline
            .execute_signal_sql_scoped(
                "CREATE TRIGGER hot ON cpu WHEN value > 90.0 DELIVER log",
                Some(ns),
            )
            .unwrap_or_else(|e| panic!("{ns} must be able to name its trigger `hot`: {e}"));
    }

    // Each sees exactly one trigger, under the name it wrote.
    for ns in ["tenant-a", "tenant-b"] {
        let result = pipeline
            .execute_signal_sql_scoped("SHOW TRIGGERS", Some(ns))
            .unwrap();
        let chronix_streaming::signal::SqlResult::Triggers(triggers) = result else {
            panic!("expected a trigger listing");
        };
        assert_eq!(triggers.len(), 1, "{ns} must see only its own trigger");
        assert_eq!(triggers[0].name, "hot", "the tenant's own name comes back");
    }

    // Dropping one leaves the other.
    pipeline
        .execute_signal_sql_scoped("DROP TRIGGER hot", Some("tenant-a"))
        .unwrap();
    let result = pipeline
        .execute_signal_sql_scoped("SHOW TRIGGERS", Some("tenant-b"))
        .unwrap();
    let chronix_streaming::signal::SqlResult::Triggers(triggers) = result else {
        panic!("expected a trigger listing");
    };
    assert_eq!(triggers.len(), 1, "tenant-b's trigger must survive");
}

/// A tenant cannot drop a trigger it does not own.
#[test]
fn a_tenant_cannot_drop_another_tenants_trigger() {
    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        name: "log".into(),
        seen: Arc::new(AtomicUsize::new(0)),
    }));
    pipeline
        .execute_signal_sql_scoped(
            "CREATE TRIGGER hot ON cpu WHEN value > 90.0 DELIVER log",
            Some("tenant-a"),
        )
        .unwrap();

    assert!(
        pipeline
            .execute_signal_sql_scoped("DROP TRIGGER hot", Some("tenant-b"))
            .is_err(),
        "tenant-b must not be able to drop tenant-a's trigger"
    );
    assert!(
        pipeline
            .execute_signal_sql_scoped("ALTER TRIGGER hot DISABLE", Some("tenant-b"))
            .is_err(),
        "nor disable it"
    );
}

/// A restored trigger must come back with its channels.
///
/// The catalog restores trigger *definitions*. The channels are built from the
/// `DELIVER` clause when the statement runs, and a restart does not run the
/// statement — so a webhook trigger came back after a restart and fired into
/// nothing, silently, at the worst possible moment.
#[test]
fn a_restored_trigger_keeps_its_delivery_channel() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = dir.path().join("triggers.json");

    let config = || PipelineConfig {
        trigger_catalog_path: Some(catalog.clone()),
        webhook_signing_secret: Some("s3cret".into()),
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    };

    {
        let pipeline = Pipeline::with_config(config());
        pipeline
            .execute_signal_sql(
                "CREATE TRIGGER t ON cpu WHEN value > 1.0 \
                 DELIVER webhook('https://alerts.example.com/hook')",
            )
            .unwrap();
        assert!(pipeline
            .delivery_router()
            .has_channel("webhook:https://alerts.example.com/hook"));
    }

    // A fresh pipeline over the same catalog.
    let reopened = Pipeline::with_config(config());
    let listed = reopened.execute_signal_sql("SHOW TRIGGERS").unwrap();
    let chronix_streaming::signal::SqlResult::Triggers(triggers) = listed else {
        panic!("expected a trigger listing");
    };
    assert_eq!(triggers.len(), 1, "the trigger must be restored");
    assert!(
        reopened
            .delivery_router()
            .has_channel("webhook:https://alerts.example.com/hook"),
        "and so must the channel it delivers to"
    );
}

/// A signal is recorded whether or not its delivery succeeds.
///
/// Delivery ran *before* the store, so a channel that fails — the common case
/// while a webhook is being set up — made its signals invisible to
/// `GET /api/v1/signals` for the length of the retry schedule. The operator
/// debugging the broken webhook could not see the alerts it was failing to
/// send, which is exactly when they need to.
#[test]
fn a_signal_is_stored_even_when_delivery_fails() {
    struct AlwaysFails;
    impl DeliveryChannel for AlwaysFails {
        fn name(&self) -> &str {
            "log"
        }
        fn deliver(&self, _event: &SignalEvent) -> SignalResult<()> {
            Err(chronix_streaming::signal::SignalError::InvalidConfig(
                "no".into(),
            ))
        }
    }

    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    pipeline
        .delivery_router()
        .add_channel(Box::new(AlwaysFails));
    pipeline
        .execute_signal_sql("CREATE TRIGGER hot ON cpu WHEN value > 90.0 DELIVER log")
        .unwrap();

    pipeline.process_cdc_event(&point_event(None, 99.0, 1));

    assert_eq!(
        pipeline.signal_store().all().len(),
        1,
        "the signal must be recorded even though every delivery attempt failed"
    );
}

/// A trigger can watch a **rollup tier**, not only a raw measurement.
///
/// The backlog said it could not. It can: materialisation writes its points
/// through the ordinary write path, so the target measurement publishes the
/// same `PointWritten` events any other write does, and the DSL's
/// `<field> <op> <value>` names a rollup's aggregate column as readily as a
/// raw field. What was missing was a test saying so.
#[test]
fn a_trigger_can_watch_a_rollup_tier() {
    use chronix::prelude::*;
    use std::time::Duration;

    const MIN: i64 = 60_000_000_000;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .shard_duration(Duration::from_secs(600))
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    db.create_rollup(
        RollupBuilder::new()
            .name("r")
            .source("raw")
            .target("raw_1m")
            .bucket(chronix::timebucket::TimeBucket::fixed_ns(MIN))
            .aggregation(RollupAggFn::Avg)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    let pipeline = Pipeline::with_config(PipelineConfig {
        enable_log_delivery: false,
        enable_metric_delivery: false,
        ..PipelineConfig::default()
    });
    let seen = Arc::new(AtomicUsize::new(0));
    pipeline.delivery_router().add_channel(Box::new(Recorder {
        // Named `log` because that is the channel the DSL accepts; the
        // recorder stands in for it, as the other tests here do.
        name: "log".into(),
        seen: seen.clone(),
    }));
    // The rollup's own column, `v_avg` — an aggregate, not a raw field.
    pipeline
        .execute_signal_sql("CREATE TRIGGER hot_avg ON raw_1m WHEN v_avg > 100.0 DELIVER log")
        .expect("a trigger on a rollup target must be accepted");

    // A minute whose average is well over the threshold.
    let mut points = Vec::new();
    for i in 0..60i64 {
        points.push(
            Point::new(
                SeriesKey::new("raw", chronix::tags! { "h" => "a" }).unwrap(),
                chronix::fields! { "v" => 500.0 + i as f64 },
                i * 1_000_000_000,
            )
            .unwrap(),
        );
    }
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    // A point far ahead, so the first minute is past the live floor and final.
    db.insert(
        &Point::new(
            SeriesKey::new("raw", chronix::tags! { "h" => "a" }).unwrap(),
            chronix::fields! { "v" => 1.0 },
            40 * MIN,
        )
        .unwrap(),
    )
    .unwrap();
    db.flush().unwrap();

    // Feed the materialisation's own CDC events through the pipeline, which is
    // what `spawn_cdc_listener` does in the server.
    let mut sub = db.event_bus().try_subscribe().expect("subscribe");
    let written = db.materialise_rollups().unwrap();
    assert!(written > 0, "the rollup must have materialised something");

    let mut fired = 0;
    while let Some(event) = sub.try_recv() {
        fired += pipeline.process_cdc_event(&event);
    }
    assert!(pipeline.delivery_router().flush(FLUSH));

    assert!(fired > 0, "a trigger on a rollup tier must fire");
    assert!(
        seen.load(Ordering::SeqCst) > 0,
        "and its signal must reach the channel it named"
    );
}

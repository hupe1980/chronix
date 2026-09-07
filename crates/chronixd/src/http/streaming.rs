//! SSE streaming and annotation endpoint handlers.

use axum::extract::{Json, State};
use serde::{Deserialize, Serialize};

use chronix::prelude::*;

use crate::error::ServerError;

use super::types::AppState;

// ── CDC SSE stream ─────────────────────────────────────────────────────

/// Query parameters for the CDC SSE stream.
#[derive(Debug, Deserialize, Default)]
pub struct CdcStreamParams {
    /// Comma-separated list of measurements to filter on.
    #[serde(default)]
    pub measurement: Option<String>,
    /// Comma-separated list of event types to filter on
    /// (e.g. `point_written,series_deleted,measurement_dropped`).
    #[serde(default)]
    pub event_type: Option<String>,
}

/// `GET /api/v1/cdc/stream` — real-time CDC event stream via Server-Sent Events.
///
/// Subscribes to the in-process `EventBus` and streams matching CDC events
/// as JSON-encoded SSE data frames. Supports optional filtering via query
/// parameters:
///
/// - `measurement=cpu,mem` — only emit events for these measurements
/// - `event_type=point_written` — only emit these event types
///
/// This is the primary integration point for external consumers that need
/// real-time change notifications without polling.
pub async fn cdc_stream_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
    axum::extract::Query(params): axum::extract::Query<CdcStreamParams>,
) -> axum::response::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::Event;
    use chronix::chronix_streaming::cdc::SubscriptionFilter;

    // Build the filter from query parameters
    let mut filter = SubscriptionFilter::all();

    if let Some(measurements) = &params.measurement {
        for m in measurements
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            filter = filter.measurement(m);
        }
    }

    if let Some(event_types) = &params.event_type {
        for et in event_types
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            filter = filter.event_type(et);
        }
    }

    // The bus carries every tenant's writes, **with their field values**, so
    // an unscoped subscription is a continuous cross-tenant data leak — the
    // worst shape of the missing-scope class, because it needs no query.
    // Events are matched on the namespace tag the write path stamps, and the
    // tag itself is stripped before the event is sent: it is the server's
    // bookkeeping, not the subscriber's data.
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let bus = state.db.event_bus();
    let mut subscription = chronix::chronix_streaming::cdc::FilteredSubscription::new(bus, filter);

    let stream = async_stream::stream! {
        // Loop until the channel closes (bus dropped), then end the stream.
        while let Some(event) = subscription.recv().await {
            let Some(event) = scope_cdc_event(event, scope.as_deref()) else {
                continue;
            };
            match serde_json::to_string(&event) {
                Ok(json) => {
                    let sse_event = Event::default()
                        .event(event.event_type())
                        .data(json);
                    yield Ok(sse_event);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CDC SSE: failed to serialize event");
                }
            }
        }
    };

    metrics::counter!("chronix_cdc_sse_connections_total").increment(1);

    axum::response::Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keepalive"),
    )
}

/// Keep an event only if it belongs to `scope`, and strip the namespace tag
/// from what the subscriber sees.
///
/// `None` means single-tenant, where every event belongs to the one tenant
/// and the tag is absent anyway.
fn scope_cdc_event(
    mut event: chronix::chronix_streaming::cdc::CdcEvent,
    scope: Option<&str>,
) -> Option<chronix::chronix_streaming::cdc::CdcEvent> {
    use chronix::chronix_streaming::cdc::CdcEvent;
    let Some(ns) = scope else {
        return Some(event);
    };
    let tags = match &mut event {
        CdcEvent::PointWritten { tags, .. } | CdcEvent::SeriesDeleted { tags, .. } => tags,
        // A measurement drop names no series, so it carries no tag to match
        // on. Under multi-tenancy a "drop" is a scoped delete, which arrives
        // as `SeriesDeleted`, so this variant is not reachable from a
        // tenant's action — and forwarding it would leak the fact that
        // another tenant dropped something.
        CdcEvent::MeasurementDropped { .. } => return None,
    };
    if tags
        .get(crate::namespace::NAMESPACE_TAG)
        .map(String::as_str)
        != Some(ns)
    {
        return None;
    }
    tags.remove(crate::namespace::NAMESPACE_TAG);
    Some(event)
}

// ── Grafana annotations ────────────────────────────────────────────────

/// Grafana-compatible annotation response.
#[derive(Debug, Serialize)]
pub struct GrafanaAnnotation {
    /// Annotation text.
    pub text: String,
    /// Unix timestamp in milliseconds.
    pub time: i64,
    /// Tags for grouping.
    pub tags: Vec<String>,
}

/// `GET /api/v1/annotations` — query past signal events as Grafana annotations.
pub async fn annotations_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
) -> Result<Json<Vec<GrafanaAnnotation>>, ServerError> {
    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let db = state.db.clone();

    let annotations = tokio::task::spawn_blocking(move || -> Vec<GrafanaAnnotation> {
        collect_annotations(&db, scope.as_deref(), 100)
    })
    .await
    .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(annotations))
}

/// Collect Grafana annotations from signal/alert measurements.
fn collect_annotations(
    db: &Chronix,
    scope: Option<&str>,
    max_per_measurement: usize,
) -> Vec<GrafanaAnnotation> {
    let mut anns = Vec::new();
    let registry = db.schema_registry();
    let names = registry.measurement_names();

    for name in &names {
        if !name.starts_with("_signals") && !name.starts_with("_alerts") {
            continue;
        }
        // Scoped like every other read: a dashboard used to show every
        // tenant's signal firings as annotations on its own panels.
        let plan = match db.query().measurement(name).namespace_scope(scope).build() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let batch = match db.execute(&plan) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let len = batch.num_rows().min(max_per_measurement);
        if let Some(ts_col) = batch.column_by_name(chronix_core::TIME_COLUMN) {
            if let Some(ts_arr) = ts_col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                for i in 0..len {
                    anns.push(GrafanaAnnotation {
                        text: format!("Signal event in {name}"),
                        time: ts_arr.value(i) / 1_000_000, // ns → ms
                        tags: vec![name.clone()],
                    });
                }
            }
        }
    }

    anns
}

/// `GET /api/v1/annotations/stream` — SSE stream of signal events for Grafana live annotations.
///
/// # How it wakes up
///
/// The loop is **pushed, not polled**: it subscribes to the CDC event bus and
/// re-scans when a write arrives, so an annotation reaches Grafana as fast as
/// the write does. The 30-second timer beside it is a *fallback*, there to
/// catch anything that reaches the database without publishing an event —
/// not an interval anybody waits for. It used to be a bare 2-second poll, and
/// this comment went on describing that for some time after it stopped being
/// true, which is why the number is now stated next to the code that produces
/// it. A subscription is required: if the bus is at its subscriber limit the
/// stream reports an error rather than silently degrading to polling.
///
/// # Dedup key strategy
///
/// Deduplication uses the **full JSON serialisation** of each annotation
/// as the `HashSet` key. This is simple and semantically correct but
/// allocates ~200–500 bytes per entry. A hash-based approach (e.g.
/// `u64` via `DefaultHasher` on the JSON bytes) would reduce the
/// per-entry overhead from `O(n)` string to a fixed 8 bytes, at the
/// cost of accepting theoretical (2⁻⁶⁴ probability) hash collisions.
pub async fn annotations_stream_handler(
    State(state): State<AppState>,
    ns_ctx: Option<axum::extract::Extension<crate::namespace::NamespaceContext>>,
) -> axum::response::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::Event;

    let scope = crate::namespace::scope(&state, ns_ctx.as_ref().map(|e| &e.0)).map(str::to_string);
    let db = state.db.clone();
    let bus = state.db.event_bus();
    // Use try_subscribe to avoid panic on max subscribers.
    let subscription_result = bus.try_subscribe();

    let stream = async_stream::stream! {
        // Gracefully handle max-subscriber limit.
        let mut subscription = match subscription_result {
            Some(sub) => sub,
            None => {
                tracing::warn!("annotations SSE: max subscribers reached, falling back to polling only");
                // Yield an error event and return — cannot subscribe.
                yield Ok(Event::default().event("error").data("max subscribers reached"));
                return;
            }
        };

        let mut seen_order: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        let mut seen_set: std::collections::HashSet<String> = std::collections::HashSet::new();
        const MAX_SEEN: usize = 10_000;

        // Do an initial poll immediately.
        let mut poll_now = true;

        loop {
            if !poll_now {
                // Wait for either a CDC write event (instant wakeup) or a
                // fallback timer (30 s) so we still catch writes that bypass
                // the event bus.
                tokio::select! {
                    _ = subscription.recv() => { /* new write arrived */ }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                }
            }
            poll_now = false;

            let db_ref = db.clone();
            let scope_ref = scope.clone();
            let annotations = match tokio::task::spawn_blocking(move || {
                collect_annotations(&db_ref, scope_ref.as_deref(), 1_000)
            }).await {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "annotations SSE: spawn_blocking task panicked");
                    continue;
                }
            };

            for ann in &annotations {
                let id = match serde_json::to_string(ann) {
                    Ok(json) => json,
                    Err(e) => {
                        tracing::warn!(error = %e, "annotations SSE: serialization failed");
                        continue;
                    }
                };

                if seen_set.insert(id.clone()) {
                    seen_order.push_back(id.clone());
                    yield Ok(Event::default().data(id));
                }
            }

            while seen_order.len() > MAX_SEEN {
                if let Some(old) = seen_order.pop_front() {
                    seen_set.remove(&old);
                }
            }
        }
    };

    axum::response::Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grafana_annotation_serialization() {
        let ann = GrafanaAnnotation {
            text: "CPU spike detected".into(),
            time: 1_700_000_000_000,
            tags: vec!["_signals".into(), "production".into()],
        };
        let json = serde_json::to_value(&ann).unwrap();
        assert_eq!(json["text"], "CPU spike detected");
        assert_eq!(json["time"], 1_700_000_000_000i64);
        assert!(json["tags"].is_array());
        assert_eq!(json["tags"][0], "_signals");
    }

    #[test]
    fn collect_annotations_empty_db() {
        let dir = tempfile::tempdir().unwrap();
        let config = chronix::prelude::ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();
        let anns = collect_annotations(&db, None, 100);
        assert!(anns.is_empty());
    }
}

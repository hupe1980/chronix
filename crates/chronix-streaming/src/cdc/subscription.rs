//! Filtered, resumable subscriptions for CDC events.
//!
//! A [`SubscriptionFilter`] restricts which events a subscriber sees.
//! The [`FilteredSubscription`] wraps a raw bus subscription and applies
//! the filter on each received event before delivering to the consumer.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio_stream::{wrappers::BroadcastStream, Stream};

use crate::cdc::bus::{BusStats, EventBus, Subscription};
use crate::cdc::event::CdcEvent;

/// Maximum number of tag predicates per subscription.
const MAX_TAG_PREDICATES: usize = 128;

/// Predicate filter for CDC subscriptions.
///
/// All filter fields are optional — `None` means "match all".
/// When multiple fields are set, they are ANDed together.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionFilter {
    /// Only deliver events for these measurements.
    pub measurements: Option<HashSet<String>>,
    /// Only deliver events matching these tag key=value pairs.
    /// An event must contain ALL specified tag pairs to match.
    /// Capped at `MAX_TAG_PREDICATES` entries.
    pub tag_predicates: Option<Vec<(String, String)>>,
    /// Only deliver these event types (e.g. `"point_written"`).
    pub event_types: Option<HashSet<String>>,
}

impl SubscriptionFilter {
    /// Creates a filter that matches all events.
    pub fn all() -> Self {
        Self::default()
    }

    /// Filter to a specific measurement.
    pub fn measurement(mut self, name: impl Into<String>) -> Self {
        self.measurements
            .get_or_insert_with(HashSet::new)
            .insert(name.into());
        self
    }

    /// Filter to specific measurements.
    pub fn measurements(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let set = self.measurements.get_or_insert_with(HashSet::new);
        for name in names {
            set.insert(name.into());
        }
        self
    }

    /// Add a tag predicate (key=value). All predicates must match.
    ///
    /// Silently ignores predicates beyond `MAX_TAG_PREDICATES`
    /// to prevent O(N) per-event filter cost from unbounded predicate lists.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let preds = self.tag_predicates.get_or_insert_with(Vec::new);
        if preds.len() < MAX_TAG_PREDICATES {
            preds.push((key.into(), value.into()));
        } else {
            tracing::warn!(
                max = MAX_TAG_PREDICATES,
                "tag predicate limit reached; ignoring additional predicate"
            );
        }
        self
    }

    /// Filter to specific event types.
    pub fn event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_types
            .get_or_insert_with(HashSet::new)
            .insert(event_type.into());
        self
    }

    /// Returns `true` if the event matches this filter.
    pub fn matches(&self, event: &CdcEvent) -> bool {
        // Check measurement filter
        if let Some(measurements) = &self.measurements {
            if !measurements.contains(event.measurement()) {
                return false;
            }
        }

        // Check event type filter
        if let Some(event_types) = &self.event_types {
            if !event_types.contains(event.event_type()) {
                return false;
            }
        }

        // Check tag predicates (only for events that have tags)
        if let Some(predicates) = &self.tag_predicates {
            let event_tags = match event {
                CdcEvent::PointWritten { tags, .. } | CdcEvent::SeriesDeleted { tags, .. } => {
                    Some(tags)
                }
                CdcEvent::MeasurementDropped { .. } => None,
            };
            if let Some(tags) = event_tags {
                for (key, value) in predicates {
                    if tags.get(key) != Some(value) {
                        return false;
                    }
                }
            } else {
                // MeasurementDropped has no tags — if predicates are required, skip
                if !predicates.is_empty() {
                    return false;
                }
            }
        }

        true
    }
}

/// A filtered subscription that only delivers events matching the filter.
///
/// Implements `tokio_stream::Stream` for async iteration.
pub struct FilteredSubscription {
    inner: Subscription,
    filter: SubscriptionFilter,
}

impl FilteredSubscription {
    /// Creates a new filtered subscription from an event bus.
    pub fn new(bus: &EventBus, filter: SubscriptionFilter) -> Self {
        Self {
            inner: bus.subscribe(),
            filter,
        }
    }

    /// Receives the next matching event, waiting asynchronously.
    pub async fn recv(&mut self) -> Option<CdcEvent> {
        loop {
            let event = self.inner.recv().await?;
            if self.filter.matches(&event) {
                return Some(event);
            }
        }
    }

    /// Non-blocking attempt to receive the next matching event.
    pub fn try_recv(&mut self) -> Option<CdcEvent> {
        loop {
            {
                let event = self.inner.try_recv()?;
                if self.filter.matches(&event) {
                    return Some(event);
                }
            }
        }
    }

    /// Returns the gap count from the underlying subscription.
    pub fn gap_count(&self) -> u64 {
        self.inner.gap_count()
    }

    /// Returns a reference to the active filter.
    pub fn filter(&self) -> &SubscriptionFilter {
        &self.filter
    }
}

/// Stream adapter for CDC events with filtering.
///
/// Built directly from an [`EventBus`] and [`SubscriptionFilter`],
/// this wraps a `BroadcastStream` and filters events inline.
/// Uses `tokio_stream::wrappers::BroadcastStream` internally to
/// avoid per-poll heap allocation when the channel is empty.
///
/// Lag events (dropped items from a slow consumer) are reported to
/// bus-level stats and metrics, matching the behaviour of the
/// [`Subscription`] type.
pub struct CdcStream {
    /// Wrapped broadcast stream (handles recv future pinning internally).
    stream: Pin<Box<BroadcastStream<CdcEvent>>>,
    /// Filter to apply to each event.
    filter: SubscriptionFilter,
    /// Gap counter for lagged events.
    gaps: u64,
    /// Counter for events that did not pass the filter.
    filtered_count: u64,
    /// Shared bus-level statistics for lag reporting.
    stats: Arc<BusStats>,
}

impl CdcStream {
    /// Creates a new CDC stream from an event bus with a filter.
    pub fn new(sub: FilteredSubscription) -> Self {
        let stats = sub.inner.stats.clone();
        Self {
            stream: Box::pin(BroadcastStream::new(sub.inner.receiver)),
            filter: sub.filter,
            gaps: sub.inner.gaps,
            filtered_count: 0,
            stats,
        }
    }

    /// Creates a CDC stream directly from a bus and filter.
    pub fn from_bus(bus: &EventBus, filter: SubscriptionFilter) -> Self {
        let stats = bus.stats();
        let sub = bus.subscribe();
        Self {
            stream: Box::pin(BroadcastStream::new(sub.receiver)),
            filter,
            gaps: 0,
            filtered_count: 0,
            stats,
        }
    }

    /// Returns the gap count from lagged events.
    pub fn gap_count(&self) -> u64 {
        self.gaps
    }

    /// Returns the number of events that did not pass the filter.
    pub fn filtered_count(&self) -> u64 {
        self.filtered_count
    }
}

impl Stream for CdcStream {
    type Item = CdcEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    if this.filter.matches(&event) {
                        return Poll::Ready(Some(event));
                    }
                    // Track filtered-out events for observability
                    this.filtered_count += 1;
                    metrics::counter!("chronix_cdc_events_filtered_total").increment(1);
                    continue;
                }
                Poll::Ready(Some(Err(
                    tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n),
                ))) => {
                    this.gaps += n;
                    this.stats.lagged.fetch_add(n, Ordering::Relaxed);
                    metrics::counter!("chronix_cdc_events_lagged_total").increment(n);
                    continue;
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chronix_core::types::FieldValue;

    use super::*;

    fn write_event(measurement: &str, host: &str, seq: u64) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: measurement.into(),
            tags: BTreeMap::from([("host".into(), host.into())]),
            fields: BTreeMap::from([("value".into(), FieldValue::F64(1.0))]),
            timestamp: seq as i64 * 1_000_000_000,
            seq,
        }
    }

    fn delete_event(measurement: &str, host: &str, seq: u64) -> CdcEvent {
        CdcEvent::SeriesDeleted {
            measurement: measurement.into(),
            tags: BTreeMap::from([("host".into(), host.into())]),
            series_hash: 0,
            seq,
        }
    }

    fn drop_event(measurement: &str, seq: u64) -> CdcEvent {
        CdcEvent::MeasurementDropped {
            measurement: measurement.into(),
            seq,
        }
    }

    // ── SubscriptionFilter tests ────────────────────────────────────

    #[test]
    fn filter_all_matches_everything() {
        let f = SubscriptionFilter::all();
        assert!(f.matches(&write_event("cpu", "srv1", 1)));
        assert!(f.matches(&delete_event("mem", "srv2", 2)));
        assert!(f.matches(&drop_event("disk", 3)));
    }

    #[test]
    fn filter_by_measurement() {
        let f = SubscriptionFilter::all().measurement("cpu");
        assert!(f.matches(&write_event("cpu", "srv1", 1)));
        assert!(!f.matches(&write_event("mem", "srv1", 2)));
    }

    #[test]
    fn filter_by_multiple_measurements() {
        let f = SubscriptionFilter::all().measurements(["cpu", "mem"]);
        assert!(f.matches(&write_event("cpu", "srv1", 1)));
        assert!(f.matches(&write_event("mem", "srv1", 2)));
        assert!(!f.matches(&write_event("disk", "srv1", 3)));
    }

    #[test]
    fn filter_by_event_type() {
        let f = SubscriptionFilter::all().event_type("point_written");
        assert!(f.matches(&write_event("cpu", "srv1", 1)));
        assert!(!f.matches(&delete_event("cpu", "srv1", 2)));
        assert!(!f.matches(&drop_event("cpu", 3)));
    }

    #[test]
    fn filter_by_tag_predicate() {
        let f = SubscriptionFilter::all().tag("host", "srv1");
        assert!(f.matches(&write_event("cpu", "srv1", 1)));
        assert!(!f.matches(&write_event("cpu", "srv2", 2)));
    }

    #[test]
    fn filter_combined() {
        let f = SubscriptionFilter::all()
            .measurement("cpu")
            .tag("host", "srv1")
            .event_type("point_written");

        assert!(f.matches(&write_event("cpu", "srv1", 1)));
        assert!(!f.matches(&write_event("mem", "srv1", 2))); // wrong measurement
        assert!(!f.matches(&write_event("cpu", "srv2", 3))); // wrong tag
        assert!(!f.matches(&delete_event("cpu", "srv1", 4))); // wrong type
    }

    #[test]
    fn filter_tag_predicate_on_drop_event() {
        let f = SubscriptionFilter::all().tag("host", "srv1");
        // MeasurementDropped has no tags — should not match tag predicate
        assert!(!f.matches(&drop_event("cpu", 1)));
    }

    // ── FilteredSubscription tests ──────────────────────────────────

    #[tokio::test]
    async fn filtered_subscription_delivers_matching() {
        let bus = EventBus::new(64);
        let mut sub = FilteredSubscription::new(&bus, SubscriptionFilter::all().measurement("cpu"));

        bus.publish(write_event("mem", "srv1", 1)); // should be filtered
        bus.publish(write_event("cpu", "srv1", 2)); // should pass

        let received = sub.recv().await.unwrap();
        assert_eq!(received.measurement(), "cpu");
        assert_eq!(received.seq(), 2);
    }

    #[tokio::test]
    async fn filtered_subscription_try_recv() {
        let bus = EventBus::new(64);
        let mut sub = FilteredSubscription::new(&bus, SubscriptionFilter::all().measurement("cpu"));

        assert!(sub.try_recv().is_none());

        bus.publish(write_event("mem", "srv1", 1));
        bus.publish(write_event("cpu", "srv1", 2));

        let received = sub.try_recv().unwrap();
        assert_eq!(received.seq(), 2);
    }

    #[tokio::test]
    async fn multiple_filtered_subscribers() {
        let bus = EventBus::new(64);
        let mut sub_cpu =
            FilteredSubscription::new(&bus, SubscriptionFilter::all().measurement("cpu"));
        let mut sub_mem =
            FilteredSubscription::new(&bus, SubscriptionFilter::all().measurement("mem"));

        bus.publish(write_event("cpu", "srv1", 1));
        bus.publish(write_event("mem", "srv1", 2));
        bus.publish(write_event("cpu", "srv1", 3));

        assert_eq!(sub_cpu.recv().await.unwrap().seq(), 1);
        assert_eq!(sub_cpu.recv().await.unwrap().seq(), 3);
        assert_eq!(sub_mem.recv().await.unwrap().seq(), 2);
    }
}

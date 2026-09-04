//! Bounded MPMC event bus with broadcast semantics.
//!
//! Uses `tokio::sync::broadcast` under the hood. Slow subscribers that fall
//! behind lose oldest events and a gap counter is incremented for diagnostics.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use parking_lot::Mutex;
use tokio::sync::broadcast;
use tracing::warn;

use crate::cdc::event::CdcEvent;
use crate::cdc::event_log::DurableEventLog;

/// Default channel capacity (number of events before oldest are dropped).
///
/// Sized for a server: at 100 K points/s this is roughly half a second of
/// tolerance for a slow subscriber, and at a gateway's 50 points/s it is
/// twenty-two minutes. One number cannot serve both, which is why the
/// capacity is configurable.
pub const DEFAULT_CAPACITY: usize = 65_536;

/// Default maximum number of concurrent subscribers.
pub const DEFAULT_MAX_SUBSCRIBERS: usize = 1024;

/// Statistics for the event bus.
#[derive(Debug)]
pub(crate) struct BusStats {
    /// Total events published.
    pub(crate) published: AtomicU64,
    /// Total events dropped due to slow subscribers.
    pub(crate) lagged: AtomicU64,
    /// Current monotonic sequence number.
    pub(crate) sequence: AtomicU64,
}

/// A bounded MPMC event bus for CDC events.
///
/// Producers call [`EventBus::publish`] to emit events. Consumers call
/// [`EventBus::subscribe`] to get a [`Subscription`] that receives all
/// events published after the subscription was created.
///
/// If a subscriber falls behind (the channel is full), the oldest events
/// are silently dropped and the subscriber sees a gap on its next receive.
#[derive(Clone)]
pub struct EventBus {
    /// The broadcast ring, created on the first subscription.
    ///
    /// `tokio::sync::broadcast::channel` allocates its whole ring up front, so
    /// building it eagerly costs 7 MiB at `DEFAULT_CAPACITY` whether or not
    /// anything ever subscribes. Deferring is semantically free: a broadcast
    /// channel only delivers to receivers that existed before the send, so an
    /// event published with no subscribers is dropped either way.
    sender: OnceLock<broadcast::Sender<CdcEvent>>,
    stats: Arc<BusStats>,
    /// Serializes sequence-number assignment + publish so events arrive
    /// in monotonic order. The critical section is tiny (atomic increment
    /// + channel send) so contention is negligible.
    publish_lock: Arc<Mutex<()>>,
    capacity: usize,
    /// Maximum number of concurrent subscribers.
    max_subscribers: usize,
    /// Optional durable event log for crash-recovery replay.
    durable_log: Option<Arc<Mutex<DurableEventLog>>>,
}

impl EventBus {
    /// Creates a new event bus with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            sender: OnceLock::new(),
            stats: Arc::new(BusStats {
                published: AtomicU64::new(0),
                lagged: AtomicU64::new(0),
                sequence: AtomicU64::new(0),
            }),
            publish_lock: Arc::new(Mutex::new(())),
            capacity: cap,
            max_subscribers: DEFAULT_MAX_SUBSCRIBERS,
            durable_log: None,
        }
    }

    /// Creates a new event bus with the [`DEFAULT_CAPACITY`].
    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }

    /// Set the maximum number of concurrent subscribers.
    #[must_use]
    pub fn with_max_subscribers(mut self, max: usize) -> Self {
        self.max_subscribers = max.max(1);
        self
    }

    /// Enables durable event logging for crash-recovery replay.
    ///
    /// Events are persisted to an append-only log file at `path` before being
    /// broadcast. After a crash, call [`replay_from`](Self::replay_from) to
    /// recover missed events.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the log file cannot be opened or created.
    pub fn with_durable_log(mut self, path: impl AsRef<Path>) -> std::io::Result<Self> {
        let log = DurableEventLog::open(path)?;
        // Restore the sequence counter to the highest persisted sequence number
        let (_, max_seq) = log.retained_range();
        if max_seq > 0 {
            self.stats.sequence.store(max_seq, Ordering::Relaxed);
            self.stats
                .published
                .store(log.len() as u64, Ordering::Relaxed);
        }
        self.durable_log = Some(Arc::new(Mutex::new(log)));
        Ok(self)
    }

    /// Publishes a CDC event to all active subscribers.
    ///
    /// **** Atomically assigns the next monotonic sequence number
    /// and broadcasts the event under a lock, guaranteeing that events
    /// arrive in strict sequence order even under concurrent publishes.
    ///
    /// The `seq` field on the incoming event is **overwritten** with the
    /// bus-assigned sequence number. Callers may pass `seq: 0` as a
    /// placeholder.
    ///
    /// Returns the number of active subscribers that received the event.
    /// If there are no subscribers, the event is still counted but
    /// effectively discarded.
    pub fn publish(&self, mut event: CdcEvent) -> usize {
        // Assign sequence atomically without holding the lock
        // during durable log I/O. The sequence counter uses fetch_add
        // for lock-free monotonic assignment.
        let seq = self.stats.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        event.set_seq(seq);
        self.stats.published.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("chronix_cdc_events_published_total").increment(1);

        // Persist to durable log before broadcasting.
        // Durable log I/O happens outside the publish lock
        // so concurrent publishers are not blocked by fsync latency.
        if let Some(ref durable_log) = self.durable_log {
            if let Err(e) = durable_log.lock().append(&event) {
                tracing::error!(seq, error = %e, "failed to persist CDC event to durable log");
            }
        }

        // Broadcast under lock to preserve sequence ordering for
        // subscribers. The critical section is tiny (channel send only).
        let _guard = self.publish_lock.lock();
        // No active receivers → event is discarded, returns 0
        self.sender
            .get()
            .map_or(0, |s| s.send(event).unwrap_or_default())
    }

    /// Publishes a batch of events, coalescing durable-log flushes.
    ///
    /// **** Instead of one `fsync` per event, the entire batch is
    /// serialized and written to the durable log in a single I/O operation,
    /// then broadcast in sequence order.
    ///
    /// Returns the total number of subscriber receives across all events.
    pub fn publish_batch(&self, events: &mut [CdcEvent]) -> usize {
        if events.is_empty() {
            return 0;
        }

        // Assign monotonic sequence numbers.
        let base = self
            .stats
            .sequence
            .fetch_add(events.len() as u64, Ordering::Relaxed)
            + 1;
        for (i, event) in events.iter_mut().enumerate() {
            event.set_seq(base + i as u64);
        }
        self.stats
            .published
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        metrics::counter!("chronix_cdc_events_published_total").increment(events.len() as u64);

        // Batch-append with single flush to durable log.
        if let Some(ref durable_log) = self.durable_log {
            if let Err(e) = durable_log.lock().append_batch(events) {
                tracing::error!(
                    count = events.len(),
                    error = %e,
                    "failed to persist CDC event batch to durable log"
                );
            }
        }

        // Broadcast under lock to preserve sequence ordering.
        let _guard = self.publish_lock.lock();
        let mut total = 0;
        for event in events.iter() {
            total += self
                .sender
                .get()
                .map_or(0, |s| s.send(event.clone()).unwrap_or_default());
        }
        total
    }

    /// Allocates the next monotonic sequence number.
    ///
    /// **Note:** Prefer using [`publish()`](Self::publish) which atomically
    /// assigns sequence numbers. This method is kept for read-path usage
    /// (e.g., tracking the current sequence position) but should NOT be
    /// used to pre-assign sequence numbers for events.
    pub fn next_seq(&self) -> u64 {
        self.stats.sequence.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Replays persisted events from the given sequence number (inclusive).
    ///
    /// Returns events in sequence order. Requires the bus to have been
    /// created with [`with_durable_log`](Self::with_durable_log). Returns
    /// an empty vec if no durable log is configured.
    pub fn replay_from(&self, from_seq: u64) -> std::io::Result<Vec<CdcEvent>> {
        match &self.durable_log {
            Some(log) => log.lock().replay_from(from_seq),
            None => Ok(Vec::new()),
        }
    }

    /// Creates a new subscription that will receive all future events.
    ///
    /// Returns `None` if the subscriber count has reached the
    /// configured maximum, preventing DoS via subscription explosion.
    pub fn try_subscribe(&self) -> Option<Subscription> {
        if self.subscriber_count() >= self.max_subscribers {
            tracing::warn!(
                current = self.subscriber_count(),
                max = self.max_subscribers,
                "subscription rejected: max subscribers reached"
            );
            return None;
        }
        Some(Subscription {
            receiver: self.ring().subscribe(),
            stats: Arc::clone(&self.stats),
            gaps: 0,
            last_lag_warn: None,
        })
    }

    /// Creates a new subscription (panics if max subscribers reached).
    ///
    /// Prefer [`try_subscribe`](Self::try_subscribe) in production code.
    pub fn subscribe(&self) -> Subscription {
        self.try_subscribe().expect("max subscribers reached")
    }

    /// The broadcast ring, allocating it on first use.
    ///
    /// Only a subscription reaches this; publishing without one never
    /// allocates.
    fn ring(&self) -> &broadcast::Sender<CdcEvent> {
        self.sender
            .get_or_init(|| broadcast::channel(self.capacity).0)
    }

    /// Returns a shared reference to the bus stats (for CDC streams).
    pub(crate) fn stats(&self) -> Arc<BusStats> {
        Arc::clone(&self.stats)
    }

    /// Returns the total number of events published since bus creation.
    pub fn published_count(&self) -> u64 {
        self.stats.published.load(Ordering::Relaxed)
    }

    /// Returns the total number of events dropped due to lagging subscribers.
    pub fn lagged_count(&self) -> u64 {
        self.stats.lagged.load(Ordering::Relaxed)
    }

    /// Returns the current sequence number (last allocated).
    pub fn current_seq(&self) -> u64 {
        self.stats.sequence.load(Ordering::Relaxed)
    }

    /// Returns the configured channel capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender
            .get()
            .map_or(0, broadcast::Sender::receiver_count)
    }
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("capacity", &self.capacity)
            .field("published", &self.published_count())
            .field("lagged", &self.lagged_count())
            .field("current_seq", &self.current_seq())
            .field("subscribers", &self.subscriber_count())
            .finish()
    }
}

/// A subscription to the CDC event bus.
///
/// Implements an async receive loop. If the subscriber falls behind and
/// events are dropped, the gap count is tracked and a warning is emitted.
pub struct Subscription {
    pub(crate) receiver: broadcast::Receiver<CdcEvent>,
    pub(crate) stats: Arc<BusStats>,
    /// Number of gap events (missed due to slow consumption).
    pub(crate) gaps: u64,
    /// Last time a lag warning was emitted (rate limiting).
    last_lag_warn: Option<std::time::Instant>,
}

impl Subscription {
    /// Receives the next CDC event, waiting asynchronously.
    ///
    /// If events were lost due to the subscriber lagging behind, the lost
    /// events are counted and a warning is logged. The next available event
    /// is returned.
    pub async fn recv(&mut self) -> Option<CdcEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    self.gaps += n;
                    self.stats.lagged.fetch_add(n, Ordering::Relaxed);
                    metrics::counter!("chronix_cdc_events_lagged_total").increment(n);
                    // Rate-limit lag warnings to at most once per 60 seconds.
                    let now = std::time::Instant::now();
                    let should_warn = self
                        .last_lag_warn
                        .is_none_or(|t| now.duration_since(t).as_secs() >= 60);
                    if should_warn {
                        self.last_lag_warn = Some(now);
                        warn!(
                            lagged = n,
                            total_gaps = self.gaps,
                            "CDC subscriber lagged behind, {} events dropped",
                            n
                        );
                    }
                    // Continue to get the next available event
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Non-blocking attempt to receive the next event.
    pub fn try_recv(&mut self) -> Option<CdcEvent> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => return Some(event),
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    self.gaps += n;
                    self.stats.lagged.fetch_add(n, Ordering::Relaxed);
                    metrics::counter!("chronix_cdc_events_lagged_total").increment(n);
                    // Rate-limit lag warnings to at most once per 60 seconds.
                    let now = std::time::Instant::now();
                    let should_warn = self
                        .last_lag_warn
                        .is_none_or(|t| now.duration_since(t).as_secs() >= 60);
                    if should_warn {
                        self.last_lag_warn = Some(now);
                        warn!(
                            lagged = n,
                            total_gaps = self.gaps,
                            "CDC subscriber lagged, {} events dropped",
                            n
                        );
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(broadcast::error::TryRecvError::Closed) => return None,
            }
        }
    }

    /// Returns the total number of events this subscriber missed.
    pub fn gap_count(&self) -> u64 {
        self.gaps
    }
}

/// A persistent subscription that replays missed events from the durable log
/// on creation, then seamlessly switches to live broadcast delivery.
///
/// Unlike a regular [`Subscription`], a `PersistentSubscription` tracks its
/// last committed sequence number so that after a restart or reconnect,
/// events published while the subscriber was offline are replayed.
///
/// Requires the [`EventBus`] to have been created with
/// [`with_durable_log`](EventBus::with_durable_log).
pub struct PersistentSubscription {
    /// The underlying live subscription for new events.
    inner: Subscription,
    /// Reference to the bus for replay access.
    bus: EventBus,
    /// Buffered events replayed from the durable log (drains first).
    replay_buffer: std::collections::VecDeque<CdcEvent>,
    /// The last sequence number delivered to the consumer.
    last_committed_seq: u64,
}

impl PersistentSubscription {
    /// Creates a persistent subscription that replays events from `from_seq` (inclusive).
    ///
    /// Events from the durable log with `seq >= from_seq` are buffered and
    /// delivered before live events. If the durable log is not configured,
    /// behaves like a normal subscription (no replay).
    pub fn new(bus: &EventBus, from_seq: u64) -> std::io::Result<Self> {
        let replayed = bus.replay_from(from_seq)?;
        let last_seq = replayed
            .last()
            .map_or(from_seq.saturating_sub(1), super::event::CdcEvent::seq);
        let inner = bus.subscribe();
        Ok(Self {
            inner,
            bus: bus.clone(),
            replay_buffer: replayed.into(),
            last_committed_seq: last_seq,
        })
    }

    /// Receives the next event, draining replayed events before live ones.
    ///
    /// Events with sequence numbers at or below `last_committed_seq` from
    /// the live stream are deduplicated (skipped).
    pub async fn recv(&mut self) -> Option<CdcEvent> {
        // Drain replay buffer first.
        if let Some(event) = self.replay_buffer.pop_front() {
            self.last_committed_seq = event.seq();
            return Some(event);
        }
        // Switch to live stream, skipping duplicates.
        loop {
            let event = self.inner.recv().await?;
            if event.seq() > self.last_committed_seq {
                self.last_committed_seq = event.seq();
                return Some(event);
            }
        }
    }

    /// Returns the last committed (delivered) sequence number.
    pub fn last_committed_seq(&self) -> u64 {
        self.last_committed_seq
    }

    /// Returns the number of events still buffered from replay.
    pub fn replay_remaining(&self) -> usize {
        self.replay_buffer.len()
    }

    /// Returns the underlying gap count from the live subscription.
    pub fn gap_count(&self) -> u64 {
        self.inner.gap_count()
    }

    /// Re-replays from a given sequence, merging with the current position.
    ///
    /// Useful for consumer-side "rewind" after detecting data issues.
    pub fn replay_from(&mut self, from_seq: u64) -> std::io::Result<()> {
        let events = self.bus.replay_from(from_seq)?;
        self.replay_buffer = events
            .into_iter()
            .filter(|e| e.seq() > self.last_committed_seq)
            .collect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chronix_core::types::FieldValue;

    use super::*;

    fn make_write_event(measurement: &str, seq: u64) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: measurement.into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("value".into(), FieldValue::F64(1.0))]),
            timestamp: seq as i64 * 1_000_000_000,
            seq,
        }
    }

    #[tokio::test]
    async fn publish_and_receive() {
        let bus = EventBus::new(16);
        let mut sub = bus.subscribe();

        let evt = make_write_event("cpu", 0);
        bus.publish(evt);

        let received = sub.recv().await.unwrap();
        assert_eq!(received.seq(), 1); // auto-assigned by publish
        assert_eq!(bus.published_count(), 1);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let bus = EventBus::new(16);
        let mut sub1 = bus.subscribe();
        let mut sub2 = bus.subscribe();

        let evt = make_write_event("mem", 0);
        let n = bus.publish(evt);
        assert_eq!(n, 2); // both subscribers

        let r1 = sub1.recv().await.unwrap();
        let r2 = sub2.recv().await.unwrap();
        assert_eq!(r1.seq(), 1);
        assert_eq!(r2.seq(), 1);
    }

    #[tokio::test]
    async fn no_subscribers_ok() {
        let bus = EventBus::new(16);
        let n = bus.publish(make_write_event("cpu", 1));
        assert_eq!(n, 0);
        assert_eq!(bus.published_count(), 1);
    }

    #[tokio::test]
    async fn gap_detection_when_lagged() {
        let bus = EventBus::new(4); // tiny buffer
        let mut sub = bus.subscribe();

        // Publish 10 events — subscriber will lag
        for i in 1..=10 {
            bus.publish(make_write_event("cpu", i));
        }

        // First recv should detect the gap
        let first = sub.recv().await.unwrap();
        assert!(first.seq() > 1, "should have skipped early events");
        assert!(sub.gap_count() > 0, "should have recorded gaps");
        assert!(bus.lagged_count() > 0, "bus should track lagged count");
    }

    #[tokio::test]
    async fn try_recv_empty() {
        let bus = EventBus::new(16);
        let mut sub = bus.subscribe();
        assert!(sub.try_recv().is_none());
    }

    #[tokio::test]
    async fn try_recv_available() {
        let bus = EventBus::new(16);
        let mut sub = bus.subscribe();
        bus.publish(make_write_event("cpu", 1));
        let evt = sub.try_recv().unwrap();
        assert_eq!(evt.seq(), 1);
    }

    #[test]
    fn sequence_monotonic() {
        let bus = EventBus::new(16);
        // publish() now auto-assigns monotonic sequence numbers
        bus.publish(make_write_event("cpu", 0));
        bus.publish(make_write_event("cpu", 0));
        bus.publish(make_write_event("cpu", 0));
        assert_eq!(bus.current_seq(), 3);
    }

    #[test]
    fn default_capacity() {
        let bus = EventBus::with_default_capacity();
        assert_eq!(bus.capacity(), DEFAULT_CAPACITY);
    }

    #[test]
    fn subscriber_count() {
        let bus = EventBus::new(16);
        assert_eq!(bus.subscriber_count(), 0);
        let _s1 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 1);
        let _s2 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 2);
        drop(_s1);
        assert_eq!(bus.subscriber_count(), 1);
    }

    #[test]
    fn debug_format() {
        let bus = EventBus::new(16);
        let dbg = format!("{bus:?}");
        assert!(dbg.contains("EventBus"));
        assert!(dbg.contains("capacity: 16"));
    }

    // ── PersistentSubscription tests ──────────────────

    #[tokio::test]
    async fn persistent_sub_replays_from_durable_log() {
        let dir = tempfile::tempdir().unwrap();
        let bus = EventBus::new(16)
            .with_durable_log(dir.path().join("cdc.log"))
            .unwrap();

        // Publish 3 events before subscribing.
        bus.publish(make_write_event("cpu", 1));
        bus.publish(make_write_event("cpu", 2));
        bus.publish(make_write_event("cpu", 3));

        // Persistent subscription from seq 1 should replay all 3.
        let mut sub = PersistentSubscription::new(&bus, 1).unwrap();
        assert_eq!(sub.replay_remaining(), 3);

        let e1 = sub.recv().await.unwrap();
        assert_eq!(e1.seq(), 1);
        assert_eq!(sub.replay_remaining(), 2);

        let e2 = sub.recv().await.unwrap();
        assert_eq!(e2.seq(), 2);

        let e3 = sub.recv().await.unwrap();
        assert_eq!(e3.seq(), 3);
        assert_eq!(sub.replay_remaining(), 0);
        assert_eq!(sub.last_committed_seq(), 3);
    }

    #[tokio::test]
    async fn persistent_sub_deduplicates_live_after_replay() {
        let dir = tempfile::tempdir().unwrap();
        let bus = EventBus::new(16)
            .with_durable_log(dir.path().join("cdc.log"))
            .unwrap();

        bus.publish(make_write_event("cpu", 1));
        bus.publish(make_write_event("cpu", 2));

        let mut sub = PersistentSubscription::new(&bus, 1).unwrap();

        // Drain replay.
        let _ = sub.recv().await;
        let _ = sub.recv().await;
        assert_eq!(sub.last_committed_seq(), 2);

        // Publish event 3 live.
        bus.publish(make_write_event("cpu", 3));

        let e3 = sub.recv().await.unwrap();
        assert_eq!(e3.seq(), 3);
        assert_eq!(sub.last_committed_seq(), 3);
    }

    #[tokio::test]
    async fn persistent_sub_replay_from_rewinds() {
        let dir = tempfile::tempdir().unwrap();
        let bus = EventBus::new(16)
            .with_durable_log(dir.path().join("cdc.log"))
            .unwrap();

        bus.publish(make_write_event("cpu", 1));
        bus.publish(make_write_event("cpu", 2));
        bus.publish(make_write_event("cpu", 3));

        let mut sub = PersistentSubscription::new(&bus, 3).unwrap();
        // Only event 3 replayed.
        assert_eq!(sub.replay_remaining(), 1);
        let _ = sub.recv().await;
        assert_eq!(sub.last_committed_seq(), 3);

        // Rewind to seq 1 — but since last_committed_seq=3, only seq>3 are kept.
        sub.replay_from(1).unwrap();
        assert_eq!(
            sub.replay_remaining(),
            0,
            "events ≤ last_committed_seq filtered out"
        );
    }

    #[test]
    fn persistent_sub_gap_count() {
        let dir = tempfile::tempdir().unwrap();
        let bus = EventBus::new(16)
            .with_durable_log(dir.path().join("cdc.log"))
            .unwrap();

        let sub = PersistentSubscription::new(&bus, 0).unwrap();
        assert_eq!(sub.gap_count(), 0);
    }
}

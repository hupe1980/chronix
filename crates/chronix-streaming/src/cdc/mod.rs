//! # Chronix Stream — CDC Event Streaming
//!
//! Change Data Capture (CDC) event generation and filtered subscriptions for
//! the Chronix time-series database.
//!
//! ## Architecture
//!
//! ```text
//! Write Path (WAL + Memtable)
//!       │
//!       ▼
//!   EventBus (broadcast)  ◄── bounded MPMC, configurable capacity
//!       │
//!       ├── Subscription (raw)
//!       ├── FilteredSubscription (measurement / tag / type filters)
//!       ├── CdcStream (async Stream<Item = CdcEvent>)
//!       └── CdcBatchExporter (Arrow RecordBatch encoding — `arrow` feature)
//! ```
//!
//! ## Event Types
//!
//! - [`CdcEvent::PointWritten`] — emitted after every successful write
//! - [`CdcEvent::SeriesDeleted`] — emitted when a series is tombstoned
//! - [`CdcEvent::MeasurementDropped`] — emitted when a measurement is dropped
//!
//! Each event carries a monotonic [`SequenceNumber`] for ordering guarantees.
//!
//! ## Usage
//!
//! ```no_run
//! use chronix_streaming::cdc::{EventBus, SubscriptionFilter, FilteredSubscription};
//!
//! let bus = EventBus::with_default_capacity();
//!
//! // Create a filtered subscription
//! let filter = SubscriptionFilter::all()
//!     .measurement("cpu")
//!     .event_type("point_written");
//! let mut sub = FilteredSubscription::new(&bus, filter);
//!
//! // In the write path, publish events:
//! // bus.publish(cdc_event);
//!
//! // In the consumer:
//! // let event = sub.recv().await;
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

#[cfg(feature = "arrow")]
pub mod arrow_batch;
mod bus;
mod error;
mod event;
pub mod event_log;
#[cfg(test)]
mod perf;
mod subscription;

pub use bus::{EventBus, PersistentSubscription, Subscription, DEFAULT_CAPACITY};
pub use error::StreamError;
pub use event::{CdcEvent, SequenceNumber};
pub use event_log::DurableEventLog;
pub use subscription::{CdcStream, FilteredSubscription, SubscriptionFilter};

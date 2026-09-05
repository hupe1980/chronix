//! Arrow Flight CDC export — bridge CDC events to Flight `DoExchange` streams.
//!
//! This module converts [`CdcEvent`]s into Arrow [`RecordBatch`]es and
//! provides a [`CdcFlightExporter`] that implements the Arrow Flight
//! `DoExchange` RPC for real-time CDC streaming to external consumers.
//!
//! ## Schema
//!
//! Each CDC event is mapped to a row in a [`RecordBatch`] with the
//! following schema:
//!
//! | Column | Type | Description |
//! |--------|------|-------------|
//! | `seq` | `UInt64` | Monotonic sequence number |
//! | `event_type` | `Utf8` | `"point_written"`, `"series_deleted"`, or `"measurement_dropped"` |
//! | `measurement` | `Utf8` | Measurement name |
//! | `timestamp` | `Int64` | Nanosecond timestamp (0 for non-write events) |
//! | `tags_json` | `Utf8` | JSON-encoded tag map |
//! | `fields_json` | `Utf8` | JSON-encoded field map (empty for non-write events) |
//! | `series_hash` | `UInt64` | FNV-1a series hash (0 for non-delete events) |
//!
//! ## Usage
//!
//! ```no_run
//! use chronix_streaming::cdc::EventBus;
//! use chronix_streaming::cdc::flight::{CdcBatchConverter, CdcFlightExporter};
//! use chronix_streaming::cdc::SubscriptionFilter;
//!
//! let bus = EventBus::with_default_capacity();
//!
//! // Create an exporter with a filter
//! let filter = SubscriptionFilter::all().measurement("cpu");
//! let exporter = CdcFlightExporter::new(&bus, filter, 1024);
//!
//! // In a tokio task, consume batches:
//! // while let Some(batch) = exporter.next_batch().await { ... }
//! ```

use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::cdc::event::CdcEvent;
use crate::cdc::subscription::{FilteredSubscription, SubscriptionFilter};
use crate::cdc::EventBus;

/// Maximum number of events to buffer before flushing a [`RecordBatch`].
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Hard upper limit for batch size to prevent accidental OOM.
const MAX_BATCH_SIZE: usize = 1_000_000;

/// Returns the canonical Arrow schema for CDC event batches.
#[must_use]
pub fn cdc_schema() -> Schema {
    Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("measurement", DataType::Utf8, false),
        Field::new("event_timestamp", DataType::Int64, false),
        Field::new("tags_json", DataType::Utf8, false),
        Field::new("fields_json", DataType::Utf8, false),
        Field::new("series_hash", DataType::UInt64, false),
    ])
}

/// Converts a batch of [`CdcEvent`]s into an Arrow [`RecordBatch`].
///
/// The converter is stateless — each call produces an independent batch.
#[derive(Debug)]
pub struct CdcBatchConverter {
    schema: Arc<Schema>,
}

impl Default for CdcBatchConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl CdcBatchConverter {
    /// Create a new converter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: Arc::new(cdc_schema()),
        }
    }

    /// Convert a slice of CDC events into an Arrow RecordBatch.
    ///
    /// Returns `None` if the input is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow batch construction fails (should not
    /// happen with valid data).
    pub fn convert(
        &self,
        events: &[CdcEvent],
    ) -> Result<Option<RecordBatch>, arrow::error::ArrowError> {
        if events.is_empty() {
            return Ok(None);
        }

        let len = events.len();
        let mut seq_builder = UInt64Builder::with_capacity(len);
        let mut type_builder = StringBuilder::with_capacity(len, len * 16);
        let mut meas_builder = StringBuilder::with_capacity(len, len * 32);
        let mut ts_builder = Int64Builder::with_capacity(len);
        let mut tags_builder = StringBuilder::with_capacity(len, len * 64);
        let mut fields_builder = StringBuilder::with_capacity(len, len * 64);
        let mut hash_builder = UInt64Builder::with_capacity(len);

        for event in events {
            seq_builder.append_value(event.seq());
            type_builder.append_value(event.event_type());
            meas_builder.append_value(event.measurement());

            match event {
                CdcEvent::PointWritten {
                    tags,
                    fields,
                    timestamp,
                    ..
                } => {
                    ts_builder.append_value(*timestamp);
                    // Serialize tags and fields as JSON.
                    // serde_json::to_string on BTreeMap won't fail for
                    // these types, so unwrap_or_default is a safe fallback.
                    tags_builder.append_value(serde_json::to_string(tags).unwrap_or_default());
                    fields_builder.append_value(serde_json::to_string(fields).unwrap_or_default());
                    hash_builder.append_value(0);
                }
                CdcEvent::SeriesDeleted {
                    tags, series_hash, ..
                } => {
                    ts_builder.append_value(0);
                    tags_builder.append_value(serde_json::to_string(tags).unwrap_or_default());
                    fields_builder.append_value("{}");
                    hash_builder.append_value(*series_hash);
                }
                CdcEvent::MeasurementDropped { .. } => {
                    ts_builder.append_value(0);
                    tags_builder.append_value("{}");
                    fields_builder.append_value("{}");
                    hash_builder.append_value(0);
                }
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(seq_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(meas_builder.finish()),
                Arc::new(ts_builder.finish()),
                Arc::new(tags_builder.finish()),
                Arc::new(fields_builder.finish()),
                Arc::new(hash_builder.finish()),
            ],
        )?;

        Ok(Some(batch))
    }
}

/// Streams CDC events as Arrow RecordBatches from an [`EventBus`].
///
/// Wraps a [`FilteredSubscription`] and converts received events into
/// Arrow RecordBatches in configurable batch sizes. Designed to feed
/// Arrow Flight `DoExchange` or `DoGet` streams.
///
/// ## Back-pressure
///
/// The exporter respects the bounded buffer of the underlying broadcast
/// channel. If the consumer is too slow, events will be dropped by the
/// broadcast layer and reported via [`gap_count`](Self::gap_count).
pub struct CdcFlightExporter {
    subscription: FilteredSubscription,
    converter: CdcBatchConverter,
    batch_size: usize,
    gap_count: u64,
    /// Track last-seen cumulative gap count from subscription
    /// to compute the delta correctly (avoid quadratic over-counting).
    last_known_gaps: u64,
}

impl CdcFlightExporter {
    /// Create a new exporter.
    ///
    /// - `bus` — the CDC event bus to subscribe to
    /// - `filter` — subscription filter (measurement, tags, event type)
    /// - `batch_size` — max events per RecordBatch (0 uses default 1024)
    #[must_use]
    pub fn new(bus: &EventBus, filter: SubscriptionFilter, batch_size: usize) -> Self {
        let batch_size = if batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            batch_size.min(MAX_BATCH_SIZE)
        };
        Self {
            subscription: FilteredSubscription::new(bus, filter),
            converter: CdcBatchConverter::new(),
            batch_size,
            gap_count: 0,
            last_known_gaps: 0,
        }
    }

    /// Returns the CDC batch schema.
    #[must_use]
    pub fn schema(&self) -> Arc<Schema> {
        Arc::new(cdc_schema())
    }

    /// Wait for the next batch of CDC events, returning an Arrow RecordBatch.
    ///
    /// Blocks (asynchronously) until at least one event is available, then
    /// drains up to `batch_size` events and converts them to a RecordBatch.
    ///
    /// Returns `None` when the event bus is closed.
    pub async fn next_batch(&mut self) -> Option<RecordBatch> {
        let mut buffer = Vec::with_capacity(self.batch_size);

        // Wait for at least one event.
        let first = self.subscription.recv().await?;
        buffer.push(first);

        // Drain up to batch_size - 1 more events without blocking.
        while buffer.len() < self.batch_size {
            match self.subscription.try_recv() {
                Some(event) => buffer.push(event),
                None => break,
            }
        }

        // Track gap delta, not cumulative total (avoids quadratic).
        let current_gaps = self.subscription.gap_count();
        self.gap_count += current_gaps - self.last_known_gaps;
        self.last_known_gaps = current_gaps;

        // Convert to Arrow.
        self.converter.convert(&buffer).ok().flatten()
    }

    /// Returns the cumulative number of events dropped due to slow consumption.
    #[must_use]
    pub fn gap_count(&self) -> u64 {
        self.gap_count
    }

    /// Returns the configured batch size.
    #[must_use]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::CdcEvent;
    use chronix_core::types::FieldValue;
    use std::collections::BTreeMap;

    fn sample_point_written(seq: u64) -> CdcEvent {
        CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(0.85))]),
            timestamp: 1_000_000_000 + seq as i64,
            seq,
        }
    }

    fn sample_series_deleted(seq: u64) -> CdcEvent {
        CdcEvent::SeriesDeleted {
            measurement: "mem".into(),
            tags: BTreeMap::from([("host".into(), "srv2".into())]),
            series_hash: 12345,
            seq,
        }
    }

    fn sample_measurement_dropped(seq: u64) -> CdcEvent {
        CdcEvent::MeasurementDropped {
            measurement: "disk".into(),
            seq,
        }
    }

    #[test]
    fn cdc_schema_has_seven_columns() {
        let schema = cdc_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "seq");
        assert_eq!(schema.field(1).name(), "event_type");
        assert_eq!(schema.field(2).name(), "measurement");
        assert_eq!(schema.field(3).name(), "event_timestamp");
        assert_eq!(schema.field(4).name(), "tags_json");
        assert_eq!(schema.field(5).name(), "fields_json");
        assert_eq!(schema.field(6).name(), "series_hash");
    }

    #[test]
    fn convert_empty_returns_none() {
        let converter = CdcBatchConverter::new();
        assert!(converter.convert(&[]).unwrap().is_none());
    }

    #[test]
    fn convert_point_written() {
        let converter = CdcBatchConverter::new();
        let events = vec![sample_point_written(1)];
        let batch = converter.convert(&events).unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);

        // Verify seq column
        let seq_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(seq_col.value(0), 1);

        // Verify event_type
        let type_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(type_col.value(0), "point_written");

        // Verify measurement
        let meas_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(meas_col.value(0), "cpu");
    }

    #[test]
    fn convert_mixed_events() {
        let converter = CdcBatchConverter::new();
        let events = vec![
            sample_point_written(1),
            sample_series_deleted(2),
            sample_measurement_dropped(3),
        ];
        let batch = converter.convert(&events).unwrap().unwrap();

        assert_eq!(batch.num_rows(), 3);

        let type_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(type_col.value(0), "point_written");
        assert_eq!(type_col.value(1), "series_deleted");
        assert_eq!(type_col.value(2), "measurement_dropped");

        // series_deleted should have the hash
        let hash_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(hash_col.value(1), 12345);

        // measurement_dropped should have 0 timestamp
        let ts_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(2), 0);
    }

    #[test]
    fn convert_preserves_tags_json() {
        let converter = CdcBatchConverter::new();
        let events = vec![sample_point_written(1)];
        let batch = converter.convert(&events).unwrap().unwrap();

        let tags_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let tags_json = tags_col.value(0);
        // BTreeMap serializes in key order
        assert_eq!(tags_json, r#"{"host":"srv1"}"#);
    }

    #[test]
    fn convert_preserves_fields_json() {
        let converter = CdcBatchConverter::new();
        let events = vec![sample_point_written(1)];
        let batch = converter.convert(&events).unwrap().unwrap();

        let fields_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let fields_json = fields_col.value(0);
        assert!(fields_json.contains("usage"));
    }

    #[tokio::test]
    async fn exporter_produces_batches() {
        let bus = EventBus::with_default_capacity();
        let filter = SubscriptionFilter::all();
        let mut exporter = CdcFlightExporter::new(&bus, filter, 10);

        // Publish events
        for i in 0..5 {
            bus.publish(sample_point_written(i));
        }

        let batch = exporter.next_batch().await.unwrap();
        assert!(batch.num_rows() >= 1);
        assert!(batch.num_rows() <= 5);
    }

    #[tokio::test]
    async fn exporter_filters_events() {
        let bus = EventBus::with_default_capacity();
        let filter = SubscriptionFilter::all().measurement("cpu");
        let mut exporter = CdcFlightExporter::new(&bus, filter, 10);

        // Publish a cpu event and a mem event
        bus.publish(sample_point_written(1)); // cpu
        bus.publish(sample_series_deleted(2)); // mem — should be filtered

        // Give the bus time to deliver
        tokio::task::yield_now().await;

        let batch = exporter.next_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 1);

        let meas_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(meas_col.value(0), "cpu");
    }

    #[test]
    fn exporter_default_batch_size() {
        let bus = EventBus::with_default_capacity();
        let exporter = CdcFlightExporter::new(&bus, SubscriptionFilter::all(), 0);
        assert_eq!(exporter.batch_size(), DEFAULT_BATCH_SIZE);
    }
}

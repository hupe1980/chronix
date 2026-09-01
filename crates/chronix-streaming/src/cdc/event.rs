//! CDC event types for change data capture.
//!
//! Every mutation (write, delete, drop) in Chronix produces a [`CdcEvent`]
//! with a monotonically increasing sequence number for ordering guarantees.

use std::collections::BTreeMap;

use chronix_core::types::{FieldValue, Timestamp};
use serde::{Deserialize, Serialize};

/// Monotonically increasing sequence number for CDC event ordering.
pub type SequenceNumber = u64;

/// A change-data-capture event emitted after a mutation is durably committed.
///
/// Each variant carries the full context needed for downstream consumers to
/// reconstruct or react to the change without re-reading the database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CdcEvent {
    /// A data point was written (after WAL append + memtable insert).
    PointWritten {
        /// Measurement name this point belongs to.
        measurement: String,
        /// Tag key-value pairs identifying the series.
        tags: BTreeMap<String, String>,
        /// Field key-value pairs (the actual data).
        fields: BTreeMap<String, FieldValue>,
        /// Nanosecond timestamp of the data point.
        timestamp: Timestamp,
        /// Monotonic sequence number for ordering.
        seq: SequenceNumber,
    },

    /// A series (identified by measurement + tags) was deleted.
    SeriesDeleted {
        /// Measurement the deleted series belongs to.
        measurement: String,
        /// Tag key-value pairs that uniquely identified the series.
        tags: BTreeMap<String, String>,
        /// FNV-1a hash of the canonical series key.
        series_hash: u64,
        /// Monotonic sequence number.
        seq: SequenceNumber,
    },

    /// An entire measurement was dropped (all series removed).
    MeasurementDropped {
        /// Name of the dropped measurement.
        measurement: String,
        /// Monotonic sequence number.
        seq: SequenceNumber,
    },
}

impl CdcEvent {
    /// Returns the sequence number of this event.
    #[inline]
    pub fn seq(&self) -> SequenceNumber {
        match self {
            Self::PointWritten { seq, .. }
            | Self::SeriesDeleted { seq, .. }
            | Self::MeasurementDropped { seq, .. } => *seq,
        }
    }

    /// Returns the measurement name this event relates to.
    #[inline]
    pub fn measurement(&self) -> &str {
        match self {
            Self::PointWritten { measurement, .. }
            | Self::SeriesDeleted { measurement, .. }
            | Self::MeasurementDropped { measurement, .. } => measurement,
        }
    }

    /// Returns the event type as a static string (for filtering/logging).
    #[inline]
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::PointWritten { .. } => "point_written",
            Self::SeriesDeleted { .. } => "series_deleted",
            Self::MeasurementDropped { .. } => "measurement_dropped",
        }
    }

    /// Sets the sequence number on this event.
    ///
    /// # Visibility
    ///
    /// This is `pub(crate)` deliberately: sequence numbers are assigned
    /// atomically by [`EventBus::publish`] *after* the caller constructs
    /// the event.  Taking `&mut self` here is safe because the event has
    /// not yet been broadcast — it is still exclusively owned by the
    /// `publish` call.  External consumers receive an immutable clone
    /// via the broadcast channel and cannot mutate the sequence number.
    #[inline]
    pub(crate) fn set_seq(&mut self, new_seq: SequenceNumber) {
        match self {
            Self::PointWritten { seq, .. }
            | Self::SeriesDeleted { seq, .. }
            | Self::MeasurementDropped { seq, .. } => *seq = new_seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_written_accessors() {
        let evt = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(0.85))]),
            timestamp: 1_000_000_000,
            seq: 42,
        };
        assert_eq!(evt.seq(), 42);
        assert_eq!(evt.measurement(), "cpu");
        assert_eq!(evt.event_type(), "point_written");
    }

    #[test]
    fn series_deleted_accessors() {
        let evt = CdcEvent::SeriesDeleted {
            measurement: "mem".into(),
            tags: BTreeMap::from([("host".into(), "srv2".into())]),
            series_hash: 12345,
            seq: 100,
        };
        assert_eq!(evt.seq(), 100);
        assert_eq!(evt.measurement(), "mem");
        assert_eq!(evt.event_type(), "series_deleted");
    }

    #[test]
    fn measurement_dropped_accessors() {
        let evt = CdcEvent::MeasurementDropped {
            measurement: "disk".into(),
            seq: 200,
        };
        assert_eq!(evt.seq(), 200);
        assert_eq!(evt.measurement(), "disk");
        assert_eq!(evt.event_type(), "measurement_dropped");
    }

    #[test]
    fn serde_round_trip() {
        let evt = CdcEvent::PointWritten {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "srv1".into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::F64(0.85))]),
            timestamp: 1_000_000_000,
            seq: 42,
        };
        let json = serde_json::to_string(&evt).unwrap();
        let deserialized: CdcEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(evt, deserialized);
    }

    #[test]
    fn serde_all_variants() {
        let events = vec![
            CdcEvent::PointWritten {
                measurement: "m".into(),
                tags: BTreeMap::new(),
                fields: BTreeMap::from([("v".into(), FieldValue::I64(1))]),
                timestamp: 0,
                seq: 1,
            },
            CdcEvent::SeriesDeleted {
                measurement: "m".into(),
                tags: BTreeMap::new(),
                series_hash: 99,
                seq: 2,
            },
            CdcEvent::MeasurementDropped {
                measurement: "m".into(),
                seq: 3,
            },
        ];

        for evt in &events {
            let json = serde_json::to_string(evt).unwrap();
            let rt: CdcEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(*evt, rt);
        }
    }
}

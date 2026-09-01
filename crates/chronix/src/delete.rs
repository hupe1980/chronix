//! Fluent delete builder for predicate-based deletes.
//!
//! Provides a builder API for constructing delete operations that can
//! target specific data based on measurement, tag filters, and time ranges.
//!
//! # Example
//!
//! ```no_run
//! # use chronix::delete::DeleteBuilder;
//! let result = DeleteBuilder::new()
//!     .measurement("cpu")
//!     .tag("host", "srv-1")
//!     .range(1000, 5000)
//!     .build();
//! ```

use std::collections::BTreeMap;

use chronix_core::SeriesKey;

use crate::error::{DbError, Result};

/// The result of a predicate delete.
///
/// A delete can be *partially* applied — a segment that cannot be
/// opened or read is skipped rather than aborting the whole operation, so a
/// bare success count could not distinguish "nothing matched" from "some data
/// was never scanned". Callers with a compliance obligation (erasure requests,
/// § 14a evidence records) should treat a non-zero
/// [`segments_skipped`](Self::segments_skipped) as a failed delete and retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeleteOutcome {
    /// Number of series tombstoned by this delete.
    pub series_tombstoned: u64,
    /// Number of segments that could not be scanned and may therefore still
    /// hold matching data. Zero means the delete was complete.
    pub segments_skipped: u64,
}

impl DeleteOutcome {
    /// Whether every matching segment was scanned.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.segments_skipped == 0
    }
}

/// A predicate-based delete request.
#[derive(Debug, Clone)]
pub struct DeleteRequest {
    /// Target measurement name.
    pub measurement: String,
    /// Tag filter predicates (all must match).
    pub tag_filters: Vec<(String, String)>,
    /// Time range start (inclusive). `None` = beginning of time.
    pub time_start: Option<i64>,
    /// Time range end (inclusive). `None` = end of time.
    pub time_end: Option<i64>,
}

impl DeleteRequest {
    /// Returns the effective time range as `(start, end)`.
    ///
    /// An absent lower bound is `i64::MIN`, not the epoch. Timestamps before
    /// 1970 are storable — the shard router and the bucket arithmetic both
    /// handle them deliberately — so defaulting to `0` made an unbounded
    /// delete quietly skip pre-epoch data, and made the segment overlap filter
    /// exclude any segment lying entirely before the epoch from the scan.
    #[must_use]
    pub fn effective_range(&self) -> (i64, i64) {
        (
            self.time_start.unwrap_or(i64::MIN),
            self.time_end.unwrap_or(i64::MAX),
        )
    }

    /// Build a `SeriesKey` from the measurement and tag filters.
    ///
    /// Returns `None` if no tag filters are set (delete applies to all series
    /// in the measurement).
    ///
    /// # Errors
    ///
    /// Returns an error if the series key cannot be constructed.
    pub fn series_key(&self) -> Result<Option<SeriesKey>> {
        if self.tag_filters.is_empty() {
            return Ok(None);
        }
        let tags: BTreeMap<String, String> = self.tag_filters.iter().cloned().collect();
        let key = SeriesKey::new(&self.measurement, tags)
            .map_err(|e| DbError::Internal(format!("Invalid series key: {e}")))?;
        Ok(Some(key))
    }
}

/// Fluent builder for constructing delete requests.
#[derive(Debug, Default)]
pub struct DeleteBuilder {
    measurement: Option<String>,
    tag_filters: Vec<(String, String)>,
    time_start: Option<i64>,
    time_end: Option<i64>,
}

impl DeleteBuilder {
    /// Create a new delete builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the target measurement.
    #[must_use]
    pub fn measurement(mut self, name: &str) -> Self {
        self.measurement = Some(name.to_string());
        self
    }

    /// Add a tag filter predicate.
    #[must_use]
    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tag_filters.push((key.to_string(), value.to_string()));
        self
    }

    /// Set the time range for the delete.
    #[must_use]
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.time_start = Some(start);
        self.time_end = Some(end);
        self
    }

    /// Set only the start of the time range.
    #[must_use]
    pub fn after(mut self, start: i64) -> Self {
        self.time_start = Some(start);
        self
    }

    /// Set only the end of the time range.
    #[must_use]
    pub fn before(mut self, end: i64) -> Self {
        self.time_end = Some(end);
        self
    }

    /// Build the delete request.
    ///
    /// # Errors
    ///
    /// Returns an error if the measurement is not specified.
    pub fn build(self) -> Result<DeleteRequest> {
        let measurement = self
            .measurement
            .ok_or_else(|| DbError::Internal("delete requires a measurement".into()))?;

        Ok(DeleteRequest {
            measurement,
            tag_filters: self.tag_filters,
            time_start: self.time_start,
            time_end: self.time_end,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_full() {
        let req = DeleteBuilder::new()
            .measurement("cpu")
            .tag("host", "srv-1")
            .tag("region", "us-east")
            .range(1000, 5000)
            .build()
            .unwrap();

        assert_eq!(req.measurement, "cpu");
        assert_eq!(req.tag_filters.len(), 2);
        assert_eq!(req.effective_range(), (1000, 5000));
    }

    #[test]
    fn builder_no_tags() {
        let req = DeleteBuilder::new()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();

        assert!(req.series_key().unwrap().is_none());
    }

    #[test]
    fn builder_missing_measurement_errors() {
        let result = DeleteBuilder::new().tag("host", "srv-1").build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_series_key_from_tags() {
        let req = DeleteBuilder::new()
            .measurement("cpu")
            .tag("host", "srv-1")
            .build()
            .unwrap();

        let key = req.series_key().unwrap().unwrap();
        assert_eq!(key.measurement(), "cpu");
    }

    #[test]
    fn effective_range_defaults() {
        let req = DeleteBuilder::new().measurement("cpu").build().unwrap();

        assert_eq!(req.effective_range(), (i64::MIN, i64::MAX));
    }

    #[test]
    fn builder_after_only() {
        let req = DeleteBuilder::new()
            .measurement("cpu")
            .after(5000)
            .build()
            .unwrap();

        assert_eq!(req.effective_range(), (5000, i64::MAX));
    }
}

//! Query planning and fluent builder API.
//!
//! The [`QueryBuilder`] provides an ergonomic fluent API for constructing
//! queries. Queries are validated at build time and compiled into a
//! [`QueryPlan`] for execution.

use std::time::Duration;

use chronix_core::Timestamp;

use crate::aggregate::AggFn;
use crate::error::{QueryError, Result};
use crate::window::WindowFn;

pub use chronix_engine::segment::{FieldPredicate, ZoneMapOp};

/// A time range for query filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    /// Start timestamp (inclusive).
    pub start: Timestamp,
    /// End timestamp (inclusive).
    pub end: Timestamp,
}

/// A tag filter predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagFilter {
    /// Tag key to filter on.
    pub key: String,
    /// Tag value to match (equality).
    pub value: String,
}

/// A logical query plan.
///
/// Describes what data to read and how to process it, without specifying
/// the physical execution strategy.
#[derive(Debug, Clone)]
pub enum QueryPlan {
    /// Scan a measurement with optional filters and projection.
    Scan {
        /// Measurement name to query.
        measurement: String,
        /// Tag equality filters.
        tag_filters: Vec<TagFilter>,
        /// Columns to return (empty = all fields).
        projection: Vec<String>,
        /// Time range filter.
        time_range: TimeRange,
        /// Zone-map field predicates for late-materialisation row-group
        /// pruning.  Each predicate compares a numeric column against a
        /// threshold; row groups whose per-block min/max stats rule out
        /// any match are skipped entirely.
        field_predicates: Vec<FieldPredicate>,
        /// Structural namespace scope.
        ///
        /// When set, the query engine can validate that every scanned
        /// segment belongs to this namespace — providing defense-in-depth
        /// even if the tag filter injection is bypassed.
        namespace_id: Option<String>,
    },
    /// Aggregate results from a scan.
    Aggregate {
        /// The underlying scan plan.
        source: Box<QueryPlan>,
        /// Aggregation functions to apply.
        functions: Vec<AggFn>,
        /// Columns to group by.
        group_by: Vec<String>,
        /// Estimated group-key cardinality from segment
        /// column statistics. When available, the engine chooses hash
        /// aggregation for high cardinality and sort-based grouping for
        /// low cardinality. `None` means unknown (hash is the default).
        estimated_cardinality: Option<usize>,
    },
    /// Downsample results into time buckets.
    ///
    /// **Fixed-width buckets only.** A `Duration` is a span of nanoseconds,
    /// so this cannot express a calendar bucket — a day in a time zone is 23
    /// or 25 hours across a transition, and a month is not a span at all. The
    /// type says so rather than pretending: the two surfaces that *do* offer
    /// calendar buckets are `time_bucket()` in SQL and a rollup tier, and both
    /// go through `chronix::timebucket::TimeBucket`. Threading that through
    /// here would put the tz database inside the query engine for a surface
    /// nobody has asked it of.
    Downsample {
        /// The underlying scan plan.
        source: Box<QueryPlan>,
        /// Bucket interval — a fixed span, aligned to the Unix epoch in UTC.
        interval: Duration,
        /// Aggregation function per bucket.
        function: AggFn,
    },
    /// Limit the number of returned rows, optionally skipping some.
    Limit {
        /// The underlying plan.
        source: Box<QueryPlan>,
        /// Maximum number of rows to return.
        limit: usize,
        /// Number of rows to skip before returning.
        offset: usize,
    },
    /// Apply window functions that produce one output row per input row.
    Window {
        /// The underlying plan.
        source: Box<QueryPlan>,
        /// Window functions to compute.
        functions: Vec<WindowFn>,
        /// Columns to partition by (each partition is independent).
        partition_by: Vec<String>,
        /// The value column to apply window functions to.
        value_column: String,
    },
}

/// Fluent query builder.
///
/// # Example
///
/// ```ignore
/// let plan = QueryBuilder::new()
///     .measurement("cpu")
///     .tag("host", "server-01")
///     .field("usage_idle")
///     .range(start_ts, end_ts)
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct QueryBuilder {
    measurement: Option<String>,
    tag_filters: Vec<TagFilter>,
    fields: Vec<String>,
    time_range: Option<TimeRange>,
    aggregate_fns: Vec<AggFn>,
    group_by: Vec<String>,
    downsample: Option<(Duration, AggFn)>,
    limit: Option<usize>,
    offset: Option<usize>,
    window_fns: Vec<WindowFn>,
    window_partition_by: Vec<String>,
    window_value_column: Option<String>,
    /// Maximum number of series (segments) to scan.
    /// Namespace scope for tenant isolation.
    namespace_id: Option<String>,
    /// Estimated group-key cardinality from segment stats.
    estimated_cardinality: Option<usize>,
}

impl QueryBuilder {
    /// Create a new empty query builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            measurement: None,
            tag_filters: Vec::new(),
            fields: Vec::new(),
            time_range: None,
            aggregate_fns: Vec::new(),
            group_by: Vec::new(),
            downsample: None,
            limit: None,
            offset: None,
            window_fns: Vec::new(),
            window_partition_by: Vec::new(),
            window_value_column: None,
            namespace_id: None,
            estimated_cardinality: None,
        }
    }

    /// Set the measurement to query.
    #[must_use]
    pub fn measurement(mut self, name: impl Into<String>) -> Self {
        self.measurement = Some(name.into());
        self
    }

    /// Add a tag equality filter.
    #[must_use]
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_filters.push(TagFilter {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Add multiple tag equality filters.
    #[must_use]
    pub fn tags(mut self, tags: &[(&str, &str)]) -> Self {
        for (key, value) in tags {
            self.tag_filters.push(TagFilter {
                key: (*key).to_string(),
                value: (*value).to_string(),
            });
        }
        self
    }

    /// Add a field to the projection (select only these columns).
    #[must_use]
    pub fn field(mut self, name: impl Into<String>) -> Self {
        self.fields.push(name.into());
        self
    }

    /// Add multiple fields to the projection.
    #[must_use]
    pub fn fields(mut self, names: &[&str]) -> Self {
        for name in names {
            self.fields.push((*name).to_string());
        }
        self
    }

    /// Set the time range filter.
    #[must_use]
    pub fn range(mut self, start: Timestamp, end: Timestamp) -> Self {
        self.time_range = Some(TimeRange { start, end });
        self
    }

    /// Set the time range to the last `duration` from now.
    ///
    /// Uses the current system time as the end point.
    #[must_use]
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    pub fn last(mut self, duration: Duration) -> Self {
        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let start = now_ns.saturating_sub(i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX));
        self.time_range = Some(TimeRange { start, end: now_ns });
        self
    }

    /// Add an aggregation function.
    #[must_use]
    pub fn aggregate(mut self, function: AggFn) -> Self {
        self.aggregate_fns.push(function);
        self
    }

    /// Set the group-by columns for aggregation.
    #[must_use]
    pub fn group_by(mut self, columns: &[&str]) -> Self {
        self.group_by = columns.iter().map(|s| (*s).to_string()).collect();
        self
    }

    /// Set the estimated group-key cardinality.
    ///
    /// When set, the query engine selects between hash-based and sort-based
    /// aggregation strategies. Low cardinality (≤ 1024) uses sort-based
    /// grouping for better cache locality and sorted output.
    #[must_use]
    pub fn estimated_cardinality(mut self, cardinality: usize) -> Self {
        self.estimated_cardinality = Some(cardinality);
        self
    }

    /// Set downsampling parameters.
    #[must_use]
    /// Bucket results into fixed spans of `interval`, aligned to the Unix
    /// epoch in UTC.
    ///
    /// For a bucket that follows a **calendar** — local midnight to local
    /// midnight, or a calendar month — use `time_bucket()` in SQL or declare
    /// a rollup tier; see `chronix::timebucket`. A `Duration` cannot be
    /// either, and this API does not pretend it can.
    pub fn downsample(mut self, interval: Duration, function: AggFn) -> Self {
        self.downsample = Some((interval, function));
        self
    }

    /// Limit the number of returned rows.
    #[must_use]
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first `n` rows before returning results.
    ///
    /// Must be used together with [`limit()`](Self::limit).
    #[must_use]
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Add a window function.
    #[must_use]
    pub fn window(mut self, func: WindowFn) -> Self {
        self.window_fns.push(func);
        self
    }

    /// Set the partition-by columns for window functions.
    #[must_use]
    pub fn window_partition_by(mut self, columns: &[&str]) -> Self {
        self.window_partition_by = columns.iter().map(|s| (*s).to_string()).collect();
        self
    }

    /// Set the value column for window functions.
    ///
    /// If not set, the first numeric field column is used by default.
    #[must_use]
    pub fn window_value_column(mut self, column: impl Into<String>) -> Self {
        self.window_value_column = Some(column.into());
        self
    }

    /// Set the namespace scope for tenant isolation.
    ///
    /// This embeds the `NamespaceId` structurally in the query plan AND
    /// injects the `__namespace__` tag filter, ensuring data isolation
    /// even if the HTTP layer is bypassed.
    #[must_use]
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        let ns = ns.into();
        // Inject the namespace tag filter for data-level filtering.
        self.tag_filters.push(TagFilter {
            key: chronix_core::NAMESPACE_TAG.to_string(),
            value: ns.clone(),
        });
        self.namespace_id = Some(ns);
        self
    }

    /// Apply an optional namespace scope.
    ///
    /// `None` means "no tenancy" — the single-tenant server and the embedded
    /// API, where points carry no namespace tag and a filter on one would
    /// match nothing. Call sites use this rather than branching, so that
    /// scoping a read is one uniform call instead of an `if` each of them can
    /// forget.
    #[must_use]
    pub fn namespace_scope(self, ns: Option<&str>) -> Self {
        match ns {
            Some(ns) => self.namespace(ns),
            None => self,
        }
    }

    /// Build and validate the query plan.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Validation`] if:
    /// No measurement is specified
    /// Time range is invalid (start > end)
    pub fn build(self) -> Result<QueryPlan> {
        let measurement = self
            .measurement
            .ok_or_else(|| QueryError::Validation("measurement is required".into()))?;

        let time_range = self.time_range.unwrap_or(TimeRange {
            start: i64::MIN,
            end: i64::MAX,
        });

        if time_range.start > time_range.end {
            return Err(QueryError::InvalidTimeRange {
                start: time_range.start,
                end: time_range.end,
            });
        }

        if self.offset.is_some() && self.limit.is_none() {
            return Err(QueryError::Validation(
                "offset requires a limit; set limit() before offset()".into(),
            ));
        }

        let scan = QueryPlan::Scan {
            measurement,
            tag_filters: self.tag_filters,
            projection: self.fields,
            time_range,
            field_predicates: Vec::new(),
            namespace_id: self.namespace_id,
        };

        // Reject ambiguous combination of downsample + aggregate.
        if self.downsample.is_some() && !self.aggregate_fns.is_empty() {
            return Err(QueryError::Validation(
                "cannot combine downsample with aggregate functions; use one or the other".into(),
            ));
        }

        // Wrap in downsample if requested
        let plan = if let Some((interval, function)) = self.downsample {
            QueryPlan::Downsample {
                source: Box::new(scan),
                interval,
                function,
            }
        } else if !self.aggregate_fns.is_empty() {
            QueryPlan::Aggregate {
                source: Box::new(scan),
                functions: self.aggregate_fns,
                group_by: self.group_by,
                estimated_cardinality: self.estimated_cardinality,
            }
        } else {
            scan
        };

        // Wrap in Window if requested
        let plan = if !self.window_fns.is_empty() {
            let value_column = self
                .window_value_column
                .unwrap_or_else(|| "value".to_string());
            QueryPlan::Window {
                source: Box::new(plan),
                functions: self.window_fns,
                partition_by: self.window_partition_by,
                value_column,
            }
        } else {
            plan
        };

        // Wrap in Limit if requested
        let plan = if let Some(limit) = self.limit {
            QueryPlan::Limit {
                source: Box::new(plan),
                limit,
                offset: self.offset.unwrap_or(0),
            }
        } else {
            plan
        };

        Ok(plan)
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract the scan parameters from a query plan.
///
/// Works through `Aggregate` and `Downsample` wrappers to find the
/// underlying `Scan` plan.
#[must_use]
pub fn extract_scan(plan: &QueryPlan) -> Option<(&str, &[TagFilter], &[String], &TimeRange)> {
    match plan {
        QueryPlan::Scan {
            measurement,
            tag_filters,
            projection,
            time_range,
            ..
        } => Some((measurement, tag_filters, projection, time_range)),
        QueryPlan::Aggregate { source, .. }
        | QueryPlan::Downsample { source, .. }
        | QueryPlan::Limit { source, .. }
        | QueryPlan::Window { source, .. } => extract_scan(source),
    }
}

/// Extract the namespace scope from the underlying `Scan` plan.
///
/// Returns `Some(&str)` if the query was built with `.namespace()`,
/// `None` otherwise. Used for defense-in-depth validation at the
/// execution layer.
#[must_use]
pub fn extract_namespace(plan: &QueryPlan) -> Option<&str> {
    match plan {
        QueryPlan::Scan { namespace_id, .. } => namespace_id.as_deref(),
        QueryPlan::Aggregate { source, .. }
        | QueryPlan::Downsample { source, .. }
        | QueryPlan::Limit { source, .. }
        | QueryPlan::Window { source, .. } => extract_namespace(source),
    }
}

/// Extract zone-map field predicates from the underlying `Scan` plan.
#[must_use]
pub fn extract_field_predicates(plan: &QueryPlan) -> &[FieldPredicate] {
    match plan {
        QueryPlan::Scan {
            field_predicates, ..
        } => field_predicates,
        QueryPlan::Aggregate { source, .. }
        | QueryPlan::Downsample { source, .. }
        | QueryPlan::Limit { source, .. }
        | QueryPlan::Window { source, .. } => extract_field_predicates(source),
    }
}

/// Set zone-map field predicates on the underlying `Scan` plan.
pub fn set_field_predicates(plan: &mut QueryPlan, preds: Vec<FieldPredicate>) {
    match plan {
        QueryPlan::Scan {
            field_predicates, ..
        } => *field_predicates = preds,
        QueryPlan::Aggregate { source, .. }
        | QueryPlan::Downsample { source, .. }
        | QueryPlan::Limit { source, .. }
        | QueryPlan::Window { source, .. } => set_field_predicates(source, preds),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_simple_scan() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(100, 200)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Scan {
                measurement,
                time_range,
                ..
            } => {
                assert_eq!(measurement, "cpu");
                assert_eq!(
                    time_range,
                    TimeRange {
                        start: 100,
                        end: 200
                    }
                );
            }
            _ => panic!("expected Scan plan"),
        }
    }

    #[test]
    fn build_with_tags_and_fields() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .tag("host", "srv1")
            .tag("dc", "eu")
            .field("usage_idle")
            .field("usage_system")
            .range(100, 200)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Scan {
                tag_filters,
                projection,
                ..
            } => {
                assert_eq!(tag_filters.len(), 2);
                assert_eq!(tag_filters[0].key, "host");
                assert_eq!(tag_filters[0].value, "srv1");
                assert_eq!(projection.len(), 2);
                assert_eq!(projection[0], "usage_idle");
            }
            _ => panic!("expected Scan plan"),
        }
    }

    #[test]
    fn build_aggregate() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(100, 200)
            .aggregate(AggFn::Avg)
            .group_by(&["host"])
            .build()
            .unwrap();

        assert!(matches!(plan, QueryPlan::Aggregate { .. }));
    }

    #[test]
    fn build_downsample() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 3_600_000_000_000)
            .downsample(Duration::from_secs(60), AggFn::Avg)
            .build()
            .unwrap();

        assert!(matches!(plan, QueryPlan::Downsample { .. }));
    }

    #[test]
    fn missing_measurement_fails() {
        let err = QueryBuilder::new().range(0, 100).build().unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn invalid_time_range_fails() {
        let err = QueryBuilder::new()
            .measurement("cpu")
            .range(200, 100) // start > end
            .build()
            .unwrap_err();
        assert!(matches!(err, QueryError::InvalidTimeRange { .. }));
    }

    #[test]
    fn default_time_range_is_all() {
        let plan = QueryBuilder::new().measurement("cpu").build().unwrap();

        match plan {
            QueryPlan::Scan { time_range, .. } => {
                assert_eq!(time_range.start, i64::MIN);
                assert_eq!(time_range.end, i64::MAX);
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn extract_scan_from_aggregate() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(100, 200)
            .aggregate(AggFn::Sum)
            .build()
            .unwrap();

        let (m, _, _, tr) = extract_scan(&plan).unwrap();
        assert_eq!(m, "cpu");
        assert_eq!(tr.start, 100);
    }

    #[test]
    fn tags_helper() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .tags(&[("host", "srv1"), ("dc", "eu")])
            .range(0, 100)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Scan { tag_filters, .. } => {
                assert_eq!(tag_filters.len(), 2);
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn fields_helper() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .fields(&["a", "b", "c"])
            .range(0, 100)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Scan { projection, .. } => {
                assert_eq!(projection, vec!["a", "b", "c"]);
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn combined_downsample_and_aggregate_rejected() {
        let err = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .downsample(Duration::from_secs(60), AggFn::Max)
            .aggregate(AggFn::Avg)
            .build()
            .unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn build_with_limit() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .limit(10)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Limit { limit, offset, .. } => {
                assert_eq!(limit, 10);
                assert_eq!(offset, 0);
            }
            _ => panic!("expected Limit plan"),
        }
    }

    #[test]
    fn build_with_limit_and_offset() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .limit(20)
            .offset(5)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Limit {
                limit,
                offset,
                source,
            } => {
                assert_eq!(limit, 20);
                assert_eq!(offset, 5);
                assert!(matches!(*source, QueryPlan::Scan { .. }));
            }
            _ => panic!("expected Limit plan"),
        }
    }

    #[test]
    fn limit_wraps_aggregate() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .aggregate(AggFn::Avg)
            .limit(5)
            .build()
            .unwrap();

        match plan {
            QueryPlan::Limit { source, limit, .. } => {
                assert_eq!(limit, 5);
                assert!(matches!(*source, QueryPlan::Aggregate { .. }));
            }
            _ => panic!("expected Limit wrapping Aggregate"),
        }
    }

    #[test]
    fn extract_scan_from_limit() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(100, 200)
            .limit(10)
            .build()
            .unwrap();

        let (m, _, _, tr) = extract_scan(&plan).unwrap();
        assert_eq!(m, "cpu");
        assert_eq!(tr.start, 100);
    }

    #[test]
    fn offset_without_limit_rejected() {
        // offset without limit is ambiguous and must be rejected.
        let err = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .offset(5)
            .build()
            .unwrap_err();

        assert!(
            matches!(err, QueryError::Validation(ref msg) if msg.contains("offset requires a limit")),
            "unexpected error: {err:?}",
        );
    }

    #[test]
    fn offset_with_limit_accepted() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .limit(10)
            .offset(5)
            .build()
            .unwrap();

        assert!(matches!(
            plan,
            QueryPlan::Limit {
                offset: 5,
                limit: 10,
                ..
            }
        ));
    }

    #[test]
    fn namespace_sets_id_and_tag_filter() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .namespace("tenant-a")
            .build()
            .unwrap();

        // Structural namespace_id is set
        assert_eq!(extract_namespace(&plan), Some("tenant-a"));

        // __namespace__ tag filter is injected
        match &plan {
            QueryPlan::Scan {
                tag_filters,
                namespace_id,
                ..
            } => {
                assert_eq!(namespace_id.as_deref(), Some("tenant-a"));
                assert!(tag_filters
                    .iter()
                    .any(|f| f.key == "__namespace__" && f.value == "tenant-a"));
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn namespace_none_by_default() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .build()
            .unwrap();

        assert_eq!(extract_namespace(&plan), None);
    }

    #[test]
    fn namespace_through_aggregate() {
        let plan = QueryBuilder::new()
            .measurement("cpu")
            .range(0, 100)
            .namespace("ns-x")
            .aggregate(AggFn::Sum)
            .group_by(&["host"])
            .build()
            .unwrap();

        assert_eq!(extract_namespace(&plan), Some("ns-x"));
    }
}

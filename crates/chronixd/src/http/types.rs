//! Shared types, constants, and utility functions for the HTTP module.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use chronix::prelude::*;
use chronix::Chronix;

/// Cached SQL logical plans keyed by `(namespace, query)`, each with an
/// insertion `Instant` (TTL) and last-access `Instant` (LRU).
pub type SqlPlanCache = std::collections::HashMap<
    (String, String),
    (
        Arc<datafusion::logical_expr::LogicalPlan>,
        std::time::Instant,
        std::time::Instant,
    ),
>;

use crate::connector::ConnectorManager;

// ── Write deduplication cache ──────────────────────────────────────────

/// TTL cache for write idempotency keys.
///
/// Maps `Idempotency-Key` header values (full strings) to the time
/// they were first seen.  Duplicate writes within the dedup window are
/// rejected with HTTP 409.
///
/// Uses the full key string instead of a hash to prevent silent write
/// rejection on hash collision.
///
/// Eviction is O(1) amortized — a `VecDeque` tracks insertion order
/// so the oldest entries can be drained from the front without sorting.
#[derive(Debug, Clone)]
pub struct WriteDedupCache {
    /// Insertion-ordered map + deque for O(1) eviction.
    inner: Arc<parking_lot::Mutex<DedupInner>>,
    /// Time window for dedup (keys older than this are evicted).
    window: std::time::Duration,
    /// Maximum number of entries before oldest-quarter eviction.
    max_entries: usize,
}

/// A claimed idempotency key, released on drop unless committed.
///
/// The guard exists so that *every* early return from a write handler —
/// including the `?` on a database error — releases the key. A handler that
/// had to remember to release it is a handler that will forget on the path
/// that matters, which is the failing one.
#[derive(Debug)]
pub struct WriteClaim<'a> {
    cache: &'a WriteDedupCache,
    key: String,
    committed: bool,
}

impl WriteClaim<'_> {
    /// The write succeeded: keep the key for the dedup window.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for WriteClaim<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.cache.release(&self.key);
        }
    }
}

/// Internal state for [`WriteDedupCache`].
#[derive(Debug)]
struct DedupInner {
    /// Full idempotency key strings → insertion instant.
    map: std::collections::HashMap<String, std::time::Instant>,
    /// Insertion-ordered queue for O(1) oldest-first eviction.
    order: std::collections::VecDeque<(String, std::time::Instant)>,
}

impl WriteDedupCache {
    /// Create a new dedup cache.  Returns `None` when the window is zero
    /// (disabled).
    #[must_use]
    pub fn new(window_secs: u64, max_entries: usize) -> Option<Self> {
        if window_secs == 0 {
            return None;
        }
        Some(Self {
            inner: Arc::new(parking_lot::Mutex::new(DedupInner {
                map: std::collections::HashMap::new(),
                order: std::collections::VecDeque::new(),
            })),
            window: std::time::Duration::from_secs(window_secs),
            max_entries,
        })
    }

    /// Claim `key` for a write that has not happened yet.
    ///
    /// Returns `None` when the key is already claimed or committed — the
    /// caller answers `409`. Otherwise returns a guard that **releases the
    /// key again unless `WriteClaim::commit` is called**, because an
    /// idempotency key is a promise about a write that *succeeded*: keeping
    /// one for a failed write turns the client's retry into a `409` and the
    /// data is stored never.
    ///
    /// The claim is taken before the write so that two concurrent requests
    /// with the same key resolve to one — check and insert are a single
    /// critical section.
    pub fn claim<'a>(&'a self, key: &str) -> Option<WriteClaim<'a>> {
        if self.check_duplicate(key) {
            return None;
        }
        Some(WriteClaim {
            cache: self,
            key: key.to_string(),
            committed: false,
        })
    }

    /// Forget a key, so a retry may claim it again.
    fn release(&self, key: &str) {
        let mut inner = self.inner.lock();
        inner.map.remove(key);
        inner.order.retain(|(k, _)| k != key);
    }

    fn check_duplicate(&self, key: &str) -> bool {
        let now = std::time::Instant::now();
        let mut inner = self.inner.lock();

        // Drain expired entries from the front of the deque (O(1)
        // amortized).  Entries are in insertion order, so the front is
        // always the oldest.
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        while let Some((front_key, front_ts)) = inner.order.front() {
            if *front_ts > cutoff {
                break; // everything remaining is newer
            }
            let front_key = front_key.clone();
            let front_ts = *front_ts;
            inner.order.pop_front();
            // Remove from map only if the timestamp matches (handles
            // re-inserts of the same key with a newer timestamp).
            if let Some(&map_ts) = inner.map.get(&front_key) {
                if map_ts == front_ts {
                    inner.map.remove(&front_key);
                }
            }
        }

        // If still over capacity, evict oldest quarter from the front.
        if inner.map.len() > self.max_entries {
            let to_evict = inner.map.len() / 4;
            let mut evicted = 0;
            while evicted < to_evict {
                if let Some((front_key, front_ts)) = inner.order.pop_front() {
                    if let Some(&map_ts) = inner.map.get(&front_key) {
                        if map_ts == front_ts {
                            inner.map.remove(&front_key);
                            evicted += 1;
                            continue;
                        }
                    }
                    // Stale deque entry — skip without counting.
                } else {
                    break;
                }
            }
        }

        // Check if key exists and is within the window.
        if let Some(&ts) = inner.map.get(key) {
            if now.duration_since(ts) < self.window {
                return true; // duplicate
            }
            // Expired — re-insert below.  Stale deque entry cleaned
            // up lazily on next eviction pass.
        }

        inner.map.insert(key.to_string(), now);
        inner.order.push_back((key.to_string(), now));
        false
    }
}

// ── Constants and helper functions ─────────────────────────────────────

/// The tag that carries a point's namespace.
///
/// One definition, in `chronix_core`, shared by the server, the query builder,
/// the SQL provider and the PromQL evaluator.
pub(super) use crate::namespace::NAMESPACE_TAG;

// ─── Safe numeric helpers ────────────────────────────────────────────

/// The wall clock in nanoseconds since the epoch, clamped.
#[inline]
pub(super) fn now_nanos_i64() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(i64::MAX)
}

// ── Shared state types ─────────────────────────────────────────────────

/// Application state shared across all HTTP handlers.
pub type AppState = Arc<SharedState>;

/// State shared across all HTTP handlers.
pub struct SharedState {
    /// The embedded Chronix database.
    pub db: Arc<Chronix>,
    /// Server configuration (for CORS, SQL limits, etc.).
    pub config: crate::config::ServerConfig,
    /// Server start time (for uptime calculation).
    pub start_time: std::time::Instant,
    /// Optional connector manager for `/api/v1/connectors`.
    pub connector_manager: Option<Arc<ConnectorManager>>,
    /// Per-namespace DataFusion session contexts for SQL queries.
    ///
    /// One shared context makes every SQL query read every tenant's rows: the
    /// namespace has to scope the *tables*, not merely the plan cache. Reach
    /// for one with [`SharedState::sql_ctx`].
    pub sql_contexts: crate::namespace::SqlContexts,
    /// Optional authentication state for key management.
    pub auth_state: Option<crate::auth::AuthState>,
    /// Optional cluster meta client for admin operations.
    #[cfg(feature = "cluster")]
    pub meta_client: Option<Arc<dyn chronix_cluster::MetaClient>>,
    /// Optional namespace registry for multi-tenancy.
    pub namespace_registry: Option<Arc<chronix_security::tenant::NamespaceRegistry>>,
    /// Model catalog for analytics model management.
    pub model_catalog: Arc<parking_lot::RwLock<chronix::chronix_analytics::forecast::ModelCatalog>>,
    /// SQL plan cache, keyed by (namespace, SQL) to
    /// prevent cross-tenant plan leakage.  Each entry carries an insertion
    /// `Instant` for TTL-based expiry and a last-access `Instant` for LRU
    /// eviction .
    pub sql_plan_cache: parking_lot::Mutex<SqlPlanCache>,
    /// Maximum duration for a single write batch (`0` = no deadline).
    /// Prevents stuck writes from exhausting the thread-pool.
    pub write_timeout: std::time::Duration,
    /// Optional Cedar authorization engine for policy enforcement.
    pub authz_engine: Option<Arc<chronix_security::authz::AuthzEngine>>,
    /// Optional audit logger for security and compliance auditing.
    pub audit_logger: Option<Arc<chronix_security::audit::AuditLogger>>,
    /// Per-namespace rate limiter for tenant-level request throttling.
    pub namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter,
    /// Optional write deduplication cache for idempotency keys.
    pub write_dedup_cache: Option<WriteDedupCache>,
    /// Signal-trigger pipeline, when `[triggers]` is configured.
    ///
    /// `None` means the trigger endpoints answer 404. Absent for the whole
    /// life of the subsystem before now: the trigger engine, the delivery
    /// router and the signal store all worked, and no protocol surface could
    /// reach any of them.
    pub pipeline: Option<Arc<chronix::Pipeline>>,
    /// Memoized OpenAPI JSON document.
    ///
    /// Per server rather than per process: the document embeds this server's
    /// configuration, so a process running two servers (or a test harness
    /// starting several) must not serve the first one's spec for all of them.
    pub openapi_json: std::sync::OnceLock<String>,
}

impl SharedState {
    /// SQL session context scoped to `namespace`.
    ///
    /// Every table it exposes carries a mandatory namespace filter, so the
    /// isolation does not depend on the SQL text a client sends.
    #[must_use]
    pub fn sql_ctx(&self, namespace: Option<&str>) -> Arc<datafusion::prelude::SessionContext> {
        self.sql_contexts.get(namespace)
    }
}

// ── Request / response types shared across handlers ────────────────────

/// Time range in the query body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRangeRequest {
    /// Start timestamp (nanoseconds, inclusive).
    pub start: i64,
    /// End timestamp (nanoseconds, inclusive).
    pub end: i64,
}

// ── Pagination types ────────────────────────────────────────────

/// Default page size when `limit` is omitted from pagination query parameters.
pub const DEFAULT_LIST_LIMIT: usize = 100;

/// Pagination query parameters for list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    /// Number of items to skip (default: 0).
    pub offset: Option<usize>,
    /// Maximum number of items to return (default: [`DEFAULT_LIST_LIMIT`]).
    pub limit: Option<usize>,
}

/// Generic paginated response envelope.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T: Serialize> {
    /// The page of items.
    pub items: Vec<T>,
    /// Total number of items available (before pagination).
    pub total: usize,
    /// The offset used for this page.
    pub offset: usize,
    /// The limit used for this page.
    pub limit: usize,
}

/// Info about a measurement for the list endpoint.
#[derive(Debug, Serialize)]
pub struct MeasurementInfo {
    /// Measurement name.
    pub name: String,
    /// Schema columns.
    pub columns: Vec<ColumnInfo>,
}

/// Request body for declaring a field column before it is written.
///
/// `POST /api/v1/measurements/{name}/schema/fields`.
///
/// The reason this endpoint exists is decimals: a decimal column's scale is
/// part of its type and is fixed by whatever creates the column, so letting
/// the first meter reading decide how many fractional digits a settlement
/// register keeps is a coin toss. Declaring it makes that a decision.
#[derive(Debug, Deserialize)]
pub struct DeclareFieldRequest {
    /// Field (column) name.
    pub name: String,
    /// Column type, in the vocabulary the schema endpoint reports:
    /// `float64`, `int64`, `uint64`, `bool`, `string`, or
    /// `decimal(38, <scale>)`.
    #[serde(rename = "type")]
    pub column_type: String,
    /// Fractional digits for a decimal column — a shorthand for writing
    /// `decimal(38, 4)` in `type`. Ignored for every other type.
    #[serde(default)]
    pub scale: Option<u8>,
}

/// Column info in a measurement schema.
#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column role ("timestamp", "tag", or "field").
    pub role: String,
    /// Arrow data type (for field columns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Convert an Arrow array value at a given row to JSON.
///
/// Re-exported from [`crate::wire::value`], which is the single encoding
/// every response shares — see its module docs for the type table and for
/// why coverage is a compile error rather than a promise.
pub(super) use crate::wire::value::to_json as arrow_value_to_json;

/// Convert a [`MeasurementSchema`] to the REST API info struct.
pub(super) fn measurement_schema_to_info(
    name: &str,
    schema: &MeasurementSchema,
) -> MeasurementInfo {
    let mut columns = Vec::new();

    // The timestamp column, under the name a query can use.
    //
    // It was reported as `timestamp`, which is the *storage* column name and
    // the key each JSON row carries — but SQL knows it as `_time`, so a
    // reader who did what this endpoint invites (read the schema, then write
    // a query) got `No field named timestamp`. There is one name here, and it
    // is the one every query surface accepts.
    columns.push(ColumnInfo {
        name: chronix_core::TIME_COLUMN.to_string(),
        role: "timestamp".to_string(),
        data_type: Some("int64".to_string()),
    });

    // Tag columns. The namespace marker is internal and never reported.
    for tag in schema.tag_names() {
        if tag == NAMESPACE_TAG {
            continue;
        }
        columns.push(ColumnInfo {
            name: tag.to_string(),
            role: "tag".to_string(),
            data_type: Some("string".to_string()),
        });
    }

    // Field columns
    for col in schema.columns() {
        if col.role == ColumnRole::Field {
            columns.push(ColumnInfo {
                name: col.name.clone(),
                role: "field".to_string(),
                data_type: Some(crate::util::column_type_to_str(col.column_type)),
            });
        }
    }

    MeasurementInfo {
        name: name.to_string(),
        columns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_schema_to_info_complete() {
        let mut schema = MeasurementSchema::new("cpu");
        let _ = schema.add_tag("host");
        let _ = schema.add_tag("region");
        schema.add_field("usage", &FieldValue::F64(0.0)).unwrap();
        schema.add_field("count", &FieldValue::I64(0)).unwrap();

        let info = measurement_schema_to_info("cpu", &schema);
        assert_eq!(info.name, "cpu");
        // timestamp + 2 tags + 2 fields = 5
        assert_eq!(info.columns.len(), 5);
        // The name a query accepts, not the storage column name: the
        // endpoint exists to be read and then typed back in.
        assert_eq!(info.columns[0].name, chronix_core::TIME_COLUMN);
        assert_eq!(info.columns[0].role, "timestamp");
        assert_eq!(info.columns[1].name, "host");
        assert_eq!(info.columns[1].role, "tag");
        assert_eq!(info.columns[3].name, "usage");
        assert_eq!(info.columns[3].role, "field");
    }

    // ── WriteDedupCache tests (: O(1) amortized eviction) ───────

    #[test]
    fn dedup_cache_detects_duplicate() {
        let cache = WriteDedupCache::new(60, 100).unwrap();
        let claim = cache.claim("key-1").expect("first claim");
        assert!(cache.claim("key-1").is_none(), "concurrent duplicate");
        claim.commit();
        assert!(cache.claim("key-1").is_none(), "committed duplicate");
    }

    /// A write that fails must not poison its idempotency key: the client's
    /// retry is the whole reason it sent one. The key used to be recorded
    /// before the write and kept regardless, so a 503 turned every retry
    /// into a 409 and the data was stored never.
    #[test]
    fn a_failed_write_releases_its_key() {
        let cache = WriteDedupCache::new(60, 100).unwrap();
        {
            let _claim = cache.claim("key-1").expect("first claim");
            // dropped without commit — the write failed
        }
        let retry = cache.claim("key-1").expect("the retry must be allowed");
        retry.commit();
        assert!(
            cache.claim("key-1").is_none(),
            "and once it succeeds, it deduplicates"
        );
    }

    #[test]
    fn dedup_cache_disabled_when_zero_window() {
        assert!(WriteDedupCache::new(0, 100).is_none());
    }

    #[test]
    fn dedup_cache_evicts_over_capacity() {
        let cache = WriteDedupCache::new(3600, 4).unwrap();
        // Fill to capacity
        for i in 0..5 {
            assert!(!cache.check_duplicate(&format!("k{i}")));
        }
        // After capacity exceeded, oldest quarter (1 entry) should be evicted.
        // The cache should still function correctly.
        let inner = cache.inner.lock();
        assert!(inner.map.len() <= 5); // at most 5 (eviction removes oldest quarter)
    }

    #[test]
    fn dedup_cache_reinsert_after_expiry_works() {
        // Use a very short window to test expiry.
        let cache = WriteDedupCache::new(1, 100).unwrap();
        assert!(!cache.check_duplicate("key-1")); // first insert
        assert!(cache.check_duplicate("key-1")); // duplicate within window
                                                 // Sleep past the window
        std::thread::sleep(std::time::Duration::from_millis(1100));
        // Should no longer be a duplicate — expired.
        assert!(!cache.check_duplicate("key-1"));
    }

    #[test]
    fn dedup_cache_clone_shares_state() {
        let a = WriteDedupCache::new(60, 100).unwrap();
        let b = a.clone();
        assert!(!a.check_duplicate("shared"));
        assert!(b.check_duplicate("shared")); // sees insert from `a`
    }
}

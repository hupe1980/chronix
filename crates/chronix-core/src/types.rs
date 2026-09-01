//! Fundamental data types for Chronix time-series data.
//!
//! This module defines the core types that represent time-series data points,
//! series identifiers, and field values. All types are designed to be
//! allocation-efficient, deterministically hashable, and serializable.

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::SchemaError;

/// Sorted tag key-value pairs stored as a flat `Vec` for allocation efficiency.
///
/// Keys are `Arc<str>` for zero-copy sharing. The vector is kept sorted by
/// key to enable O(log n) binary-search lookups.
pub type Tags = Vec<(Arc<str>, Arc<str>)>;

/// Sorted field key-value pairs stored as a flat `Vec` for allocation efficiency.
///
/// Keys are `Arc<str>` for zero-copy sharing. The vector is kept sorted by
/// key to enable O(log n) binary-search lookups.
pub type Fields = Vec<(Arc<str>, FieldValue)>;

/// Binary-search a sorted slice of `(Arc<str>, V)` pairs by key.
#[inline]
fn sorted_vec_get<'a, V>(slice: &'a [(Arc<str>, V)], key: &str) -> Option<&'a V> {
    slice
        .binary_search_by(|(k, _)| k.as_ref().cmp(key))
        .ok()
        .and_then(|idx| slice.get(idx))
        .map(|(_, v)| v)
}

/// Serialize an `Arc<Tags>` as a JSON map for backward-compatible wire format.
fn serialize_tags<S>(tags: &Arc<Tags>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(tags.len()))?;
    for (k, v) in tags.as_ref() {
        map.serialize_entry(k.as_ref(), v.as_ref())?;
    }
    map.end()
}

/// Deserialize an `Arc<Tags>` from a JSON map.
fn deserialize_tags<'de, D>(deserializer: D) -> Result<Arc<Tags>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map = BTreeMap::<String, String>::deserialize(deserializer)?;
    // BTreeMap iterates in sorted order, so the resulting Vec is sorted.
    let vec: Tags = map
        .into_iter()
        .map(|(k, v)| (Arc::from(k.as_str()), Arc::from(v.as_str())))
        .collect();
    Ok(Arc::new(vec))
}

/// Serialize `Fields` as a JSON map for backward-compatible wire format.
fn serialize_fields<S>(fields: &Fields, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(fields.len()))?;
    for (k, v) in fields {
        map.serialize_entry(k.as_ref(), v)?;
    }
    map.end()
}

/// Deserialize `Fields` from a JSON map.
fn deserialize_fields<'de, D>(deserializer: D) -> Result<Fields, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map = BTreeMap::<String, FieldValue>::deserialize(deserializer)?;
    // BTreeMap iterates in sorted order, so the resulting Vec is sorted.
    let vec: Fields = map
        .into_iter()
        .map(|(k, v)| (Arc::from(k.as_str()), v))
        .collect();
    Ok(vec)
}

/// Nanosecond-precision Unix epoch timestamp.
///
/// Stored as a signed 64-bit integer to support pre-epoch timestamps.
/// Range: approximately ±292 years from epoch (1677–2262).
pub type Timestamp = i64;

/// Unique segment identifier (monotonically increasing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub u64);

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "seg-{}", self.0)
    }
}

/// Shard identifier derived from timestamp and shard duration.
///
/// Shards partition time into fixed-duration windows (default: 1 hour).
/// The shard ID is the truncated timestamp at the shard boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId(pub i64);

/// Unique namespace identifier for multi-tenancy.
///
/// Namespaces isolate measurements, schemas, models, and storage
/// to support multi-tenant deployments. Namespace names must be
/// lowercase alphanumeric with hyphens, 1–63 characters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);

impl NamespaceId {
    /// The default namespace used when multi-tenancy is disabled.
    pub const DEFAULT: &'static str = "default";

    /// Create a new namespace ID.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError` if the name is empty, too long (>63 chars),
    /// or contains invalid characters.
    pub fn new(name: impl Into<String>) -> std::result::Result<Self, SchemaError> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(Self(name))
    }

    /// Create the default namespace.
    #[must_use]
    pub fn default_namespace() -> Self {
        Self(Self::DEFAULT.to_string())
    }

    /// Returns the namespace name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate a namespace name.
    fn validate(name: &str) -> std::result::Result<(), SchemaError> {
        if name.is_empty() || name.len() > 63 {
            return Err(SchemaError::InvalidName {
                name: name.to_string(),
                reason: "namespace name must be 1–63 characters".to_string(),
            });
        }

        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(SchemaError::InvalidName {
                name: name.to_string(),
                reason: "namespace name must be lowercase alphanumeric with hyphens".to_string(),
            });
        }

        if name.starts_with('-') || name.ends_with('-') {
            return Err(SchemaError::InvalidName {
                name: name.to_string(),
                reason: "namespace name must not start or end with a hyphen".to_string(),
            });
        }

        // FINDING-24: First character must be a letter (DNS-label convention).
        if !name.starts_with(|c: char| c.is_ascii_lowercase()) {
            return Err(SchemaError::InvalidName {
                name: name.to_string(),
                reason: "namespace name must start with a lowercase letter".to_string(),
            });
        }

        Ok(())
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Resource quota limits for a namespace.
///
/// Enforced at write-time to prevent any single tenant from
/// monopolising cluster resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceQuota {
    /// Maximum number of distinct time series.
    pub max_series_count: u64,
    /// Maximum ingestion rate in points per second.
    pub max_ingestion_rate: u64,
    /// Maximum total storage in bytes across all tiers.
    pub max_storage_bytes: u64,
    /// Maximum number of measurements (tables).
    pub max_measurements: u32,
    /// Maximum HTTP request rate per second (0 = unlimited).
    #[serde(default)]
    pub max_request_rps: u64,
    /// Burst size for HTTP request rate limiting (0 = defaults to `max_request_rps`).
    #[serde(default)]
    pub max_request_burst: u32,
}

impl NamespaceQuota {
    /// Upper bounds for resource limits to catch misconfiguration.
    const MAX_SERIES_COUNT: u64 = 1_000_000_000; // 1B series
    const MAX_INGESTION_RATE: u64 = 100_000_000; // 100M pts/s
    const MAX_STORAGE_BYTES: u64 = 100 * 1024 * 1024 * 1024 * 1024; // 100 TB
    const MAX_MEASUREMENTS: u32 = 1_000_000; // 1M measurements

    /// Validate quota values are within sane upper bounds.
    ///
    /// # Errors
    ///
    /// Returns a description of the out-of-range field.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.max_series_count > Self::MAX_SERIES_COUNT {
            return Err(format!(
                "max_series_count {} exceeds upper bound {}",
                self.max_series_count,
                Self::MAX_SERIES_COUNT
            ));
        }
        if self.max_ingestion_rate > Self::MAX_INGESTION_RATE {
            return Err(format!(
                "max_ingestion_rate {} exceeds upper bound {}",
                self.max_ingestion_rate,
                Self::MAX_INGESTION_RATE
            ));
        }
        if self.max_storage_bytes > Self::MAX_STORAGE_BYTES {
            return Err(format!(
                "max_storage_bytes {} exceeds upper bound {}",
                self.max_storage_bytes,
                Self::MAX_STORAGE_BYTES
            ));
        }
        if self.max_measurements > Self::MAX_MEASUREMENTS {
            return Err(format!(
                "max_measurements {} exceeds upper bound {}",
                self.max_measurements,
                Self::MAX_MEASUREMENTS
            ));
        }
        Ok(())
    }
}

impl Default for NamespaceQuota {
    fn default() -> Self {
        Self {
            max_series_count: 1_000_000,
            max_ingestion_rate: 100_000,
            max_storage_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            max_measurements: 1000,
            max_request_rps: 0,   // unlimited by default
            max_request_burst: 0, // defaults to max_request_rps
        }
    }
}

/// Real-time resource usage for a namespace.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NamespaceUsage {
    /// Current number of active time series.
    pub series_count: u64,
    /// Recent ingestion rate in points per second (windowed counter,
    /// resets each second to prevent infinite accumulation).
    pub ingestion_rate: f64,
    /// Epoch-second of the current rate window. When the wall-clock
    /// second changes, `ingestion_rate` is reset to zero.
    #[serde(default)]
    pub ingestion_rate_window_s: u64,
    /// Total storage used in bytes.
    pub storage_bytes: u64,
    /// Current number of measurements.
    pub measurements: u32,
}

impl ShardId {
    /// Compute the shard ID for a given timestamp and shard duration.
    ///
    /// The shard ID is the floor of `timestamp / shard_duration_ns`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn from_timestamp(timestamp: Timestamp, shard_duration: Duration) -> Self {
        let duration_ns = i64::try_from(shard_duration.as_nanos()).unwrap_or_else(|_| {
            tracing::warn!(
                duration_nanos = %shard_duration.as_nanos(),
                "FINDING-05: shard_duration overflows i64 — falling back to i64::MAX"
            );
            i64::MAX
        });
        // div_euclid handles negative timestamps correctly without
        // overflow: it always rounds toward negative infinity.
        let id = timestamp.div_euclid(duration_ns);
        Self(id)
    }

    /// Returns the start timestamp (inclusive) of this shard.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn start_timestamp(&self, shard_duration: Duration) -> Timestamp {
        self.0.saturating_mul(
            i64::try_from(shard_duration.as_nanos()).unwrap_or_else(|_| {
                tracing::warn!(
                    duration_nanos = %shard_duration.as_nanos(),
                    "FINDING-05: shard_duration overflows i64 — falling back to i64::MAX"
                );
                i64::MAX
            }),
        )
    }

    /// Returns the end timestamp (exclusive) of this shard.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn end_timestamp(&self, shard_duration: Duration) -> Timestamp {
        self.0.saturating_add(1).saturating_mul(
            i64::try_from(shard_duration.as_nanos()).unwrap_or_else(|_| {
                tracing::warn!(
                    duration_nanos = %shard_duration.as_nanos(),
                    "FINDING-05: shard_duration overflows i64 — falling back to i64::MAX"
                );
                i64::MAX
            }),
        )
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard-{}", self.0)
    }
}

/// A typed field value in a time-series data point.
///
/// Chronix supports five field types matching the `InfluxDB` Line Protocol model
/// extended with unsigned integers and bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    /// 64-bit IEEE 754 floating-point number.
    F64(f64),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// Boolean value.
    Bool(bool),
    /// UTF-8 string value.
    String(String),
}

impl FieldValue {
    /// Returns a human-readable type name for error messages.
    #[inline]
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::F64(_) => "f64",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
            Self::Bool(_) => "bool",
            Self::String(_) => "string",
        }
    }

    /// Returns `true` if this value is the same type as `other`.
    #[inline]
    #[must_use]
    pub fn same_type_as(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::F64(v) => write!(f, "{v}"),
            Self::I64(v) => write!(f, "{v}i"),
            Self::U64(v) => write!(f, "{v}u"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::String(v) => {
                // FINDING-33: Escape backslashes and double-quotes inside string values.
                let escaped = v.replace('\\', "\\\\").replace('"', "\\\"");
                write!(f, "\"{escaped}\"")
            }
        }
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<u64> for FieldValue {
    fn from(v: u64) -> Self {
        Self::U64(v)
    }
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

/// Maximum length for measurement names, tag keys, and tag values (bytes).
pub const MAX_NAME_LENGTH: usize = 256;

/// Maximum number of tags per series.
pub const MAX_TAGS_PER_SERIES: usize = 64;

/// Maximum number of fields per point.
///
/// Prevents unbounded schema growth and WAL bloat from pathological payloads.
/// `InfluxDB` caps at 1M fields per measurement; we use a more conservative limit.
pub const MAX_FIELDS_PER_POINT: usize = 1024;

/// Maximum length of a string field value in bytes.
///
/// Prevents WAL record bloat and encoding OOM from oversized string payloads.
pub const MAX_STRING_FIELD_LENGTH: usize = 65_536; // 64 KB

/// A unique series identifier: measurement name + sorted tag set.
///
/// # Canonical form
///
/// The canonical form uses NUL (`\0`) as the separator between the
/// measurement name and each `key=value` tag pair:
///
/// ```text
/// measurement\0tag1=v1\0tag2=v2
/// ```
///
/// **Why NUL?**  NUL bytes are forbidden in measurement names, tag keys,
/// and tag values (enforced by [`SeriesKey::validate_name`]).  This makes
/// NUL an unambiguous separator that cannot appear inside any component,
/// guaranteeing that the canonical form is a unique, deterministic, and
/// reversible encoding of the series identity.
///
/// Tags are sorted lexicographically by key, and `=` inside tag values
/// is also forbidden to prevent parse ambiguity.
///
/// The canonical form is pre-computed in the constructor and
/// stored as an `Arc<str>`, eliminating `OnceLock` synchronization
/// overhead on hot paths and making `clone()` allocation-free.
#[derive(Debug, Serialize)]
pub struct SeriesKey {
    measurement: String,
    #[serde(serialize_with = "serialize_tags")]
    tags: Arc<Tags>,
    /// Pre-computed canonical string form.  Excluded from equality, hash,
    /// and serde because it is a pure function of (`measurement`, `tags`).
    #[serde(skip)]
    canonical: Arc<str>,
}

/// Shadow struct for serde deserialization of `SeriesKey`.
/// After deserialization, the canonical form is recomputed.
#[derive(Deserialize)]
struct SeriesKeyWire {
    measurement: String,
    #[serde(deserialize_with = "deserialize_tags")]
    tags: Arc<Tags>,
}

impl<'de> serde::Deserialize<'de> for SeriesKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = SeriesKeyWire::deserialize(deserializer)?;
        let canonical = Self::compute_canonical(&wire.measurement, &wire.tags);
        Ok(Self {
            measurement: wire.measurement,
            tags: wire.tags,
            canonical,
        })
    }
}

impl Clone for SeriesKey {
    fn clone(&self) -> Self {
        Self {
            measurement: self.measurement.clone(),
            tags: self.tags.clone(),
            // Arc<str> clone is just a reference count increment.
            canonical: self.canonical.clone(),
        }
    }
}

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        self.measurement == other.measurement && self.tags == other.tags
    }
}

impl Eq for SeriesKey {}

impl Hash for SeriesKey {
    /// Hash the canonical form so that `Hash` trait and
    /// `hash_fnv()` produce consistent orderings for the same series key.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical.hash(state);
    }
}

/// The tag key that carries a point's namespace.
///
/// Multi-tenancy is enforced by this one tag: every ingestion surface stamps
/// it, every read surface scopes to it. It is *internal* — overwritten if a
/// client supplies it, and absent from every result and schema (D45, D47).
///
/// Defined here because the server, the query builder and the PromQL
/// evaluator must all name the same key; they used to spell it out
/// separately, and the copies drifted (R1).
pub const NAMESPACE_TAG: &str = "__namespace__";

/// Separator between `(key, value)` pairs in a [`SeriesKey`] canonical form.
///
/// Reserved: rejected by [`SeriesKey::validate_name`] in measurement names,
/// tag keys and tag values.
pub const TAG_SEPARATOR: char = '\0';

/// Separator between a tag key and its value in a canonical form.
///
/// Also reserved. Using a control character rather than `=` is what allows a
/// tag *value* to contain `=`, which InfluxDB Line Protocol permits (escaped)
/// and real-world tags — URLs with query strings, base64 padding, Kubernetes
/// label selectors — routinely contain. The canonical form only has to be
/// **injective** (nothing parses it back apart from the measurement prefix),
/// so reserving two control characters that cannot appear in user data is
/// strictly less restrictive than banning `=`.
pub const KV_SEPARATOR: char = '\x01';

/// Append a canonical series form to `out`.
///
/// `tags` **must** already be sorted by key — the canonical form is
/// order-sensitive, and [`SeriesKey`] keeps its tags sorted.
///
/// Query and dedup paths reconstruct the canonical form from Arrow columns
/// rather than from a [`SeriesKey`], and each used to inline the format. When
/// the separators changed, tombstone matching broke silently because those
/// copies still emitted the old layout. Everything that needs a canonical form
/// goes through this function so the format has exactly one definition.
pub fn push_canonical<'a, I>(out: &mut String, measurement: &str, tags: I)
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    out.push_str(measurement);
    for (key, value) in tags {
        out.push(TAG_SEPARATOR);
        out.push_str(key);
        out.push(KV_SEPARATOR);
        out.push_str(value);
    }
}

/// Build a canonical series form from a measurement and **sorted** tag pairs.
///
/// See [`push_canonical`].
#[must_use]
pub fn canonical_from_pairs<'a, I>(measurement: &str, tags: I) -> String
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut s = String::new();
    push_canonical(&mut s, measurement, tags);
    s
}

impl SeriesKey {
    /// Compute the canonical string form from measurement + tags.
    fn compute_canonical(measurement: &str, tags: &Tags) -> Arc<str> {
        let mut s = String::with_capacity(
            measurement.len()
                + tags
                    .iter()
                    .map(|(k, v)| 1 + k.len() + 1 + v.len())
                    .sum::<usize>(),
        );
        push_canonical(
            &mut s,
            measurement,
            tags.as_slice().iter().map(|(k, v)| (&**k, &**v)),
        );
        Arc::from(s.as_str())
    }

    /// Create a new `SeriesKey`, validating all constraints.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] if any constraint is violated:
    /// Measurement name > 256 bytes, or contains null bytes
    /// Tag key/value > 256 bytes, or contains null bytes
    /// More than 64 tags
    pub fn new(
        measurement: impl Into<String>,
        tags: BTreeMap<String, String>,
    ) -> Result<Self, SchemaError> {
        let measurement = measurement.into();
        Self::validate_name(&measurement, "measurement name")?;

        if tags.len() > MAX_TAGS_PER_SERIES {
            return Err(SchemaError::TooManyTags {
                count: tags.len(),
                max: MAX_TAGS_PER_SERIES,
            });
        }

        for (key, value) in &tags {
            Self::validate_name(key, "tag key")?;
            Self::validate_name(value, "tag value")?;
        }

        // BTreeMap iterates in sorted order, so the resulting Vec is sorted.
        let tags: Arc<Tags> = Arc::new(
            tags.into_iter()
                .map(|(k, v)| (Arc::from(k.as_str()), Arc::from(v.as_str())))
                .collect(),
        );
        let canonical = Self::compute_canonical(&measurement, &tags);

        Ok(Self {
            measurement,
            tags,
            canonical,
        })
    }

    /// Returns the measurement name.
    #[inline]
    #[must_use]
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns the tag set (sorted by key).
    #[inline]
    #[must_use]
    pub fn tags(&self) -> &Tags {
        &self.tags
    }

    /// Returns a shared reference-counted handle to the tag set.
    ///
    /// Use this when you need cheap shared ownership (e.g., CDC events)
    /// without deep-cloning the set.
    #[inline]
    #[must_use]
    pub fn tags_arc(&self) -> &Arc<Tags> {
        &self.tags
    }

    /// Insert a tag in-place using copy-on-write (FINDING-22).
    ///
    /// Only clones the inner `Vec` if another `Arc` handle exists;
    /// otherwise the insertion happens without allocation.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] if the tag key or value is invalid.
    pub fn inject_tag(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), SchemaError> {
        let key = key.into();
        let value = value.into();
        Self::validate_name(&key, "tag key")?;
        Self::validate_name(&value, "tag value")?;
        if value.contains('=') {
            return Err(SchemaError::InvalidCharacter {
                label: "tag value".to_string(),
                ch: '=',
                value: value.clone(),
            });
        }
        let tags = Arc::make_mut(&mut self.tags);
        match tags.binary_search_by(|(k, _)| k.as_ref().cmp(&key)) {
            Ok(idx) => {
                if let Some(entry) = tags.get_mut(idx) {
                    entry.1 = Arc::from(value.as_str());
                }
            }
            Err(idx) => {
                if tags.len() >= MAX_TAGS_PER_SERIES {
                    return Err(SchemaError::TooManyTags {
                        count: tags.len() + 1,
                        max: MAX_TAGS_PER_SERIES,
                    });
                }
                tags.insert(idx, (Arc::from(key.as_str()), Arc::from(value.as_str())));
            }
        }
        // Recompute the pre-computed canonical form since tags changed.
        self.canonical = Self::compute_canonical(&self.measurement, &self.tags);
        Ok(())
    }

    /// Returns the value of a specific tag key via binary search.
    #[inline]
    #[must_use]
    pub fn tag(&self, key: &str) -> Option<&str> {
        sorted_vec_get(&self.tags, key).map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over tag keys.
    #[inline]
    pub fn tag_keys(&self) -> impl Iterator<Item = &str> {
        self.tags.iter().map(|(k, _)| k.as_ref())
    }

    /// Compute a deterministic 64-bit hash of this series key.
    ///
    /// Uses FNV-1a on the canonical form: `measurement\0tag1=v1\0tag2=v2`.
    #[must_use]
    pub fn hash_fnv(&self) -> u64 {
        use fnv::FnvHasher;
        use std::hash::Hasher;

        let mut hasher = FnvHasher::default();
        hasher.write(self.measurement.as_bytes());
        for (key, value) in self.tags.iter() {
            // Must use the same reserved separators as `compute_canonical`.
            // Hashing with `=` while `=` is legal inside a tag value made the
            // byte stream ambiguous, so `{a: "b=c"}` and `{"a=b": "c"}` —
            // distinct series with distinct canonical forms — hashed equal.
            hasher.write_u8(TAG_SEPARATOR as u8);
            hasher.write(key.as_bytes());
            hasher.write_u8(KV_SEPARATOR as u8);
            hasher.write(value.as_bytes());
        }
        hasher.finish()
    }

    /// Returns the canonical string form:
    /// `measurement\0tag1\x01v1\0tag2\x01v2`.
    ///
    /// [`TAG_SEPARATOR`] (`\0`) separates pairs and [`KV_SEPARATOR`]
    /// (`\x01`) separates a key from its value. Both are rejected by
    /// [`Self::validate_name`] in measurement names, tag keys and tag values,
    /// which makes the encoding injective — the property that matters for
    /// hashing, tombstone sets and WAL encoding. Using a reserved control
    /// character rather than `=` is what lets a tag value contain `=`.
    ///
    /// The canonical form is pre-computed in the constructor
    /// and stored as `Arc<str>`. This call is a simple field access with
    /// no allocation or synchronization overhead.
    #[inline]
    #[must_use]
    pub fn canonical_form(&self) -> &str {
        &self.canonical
    }

    /// Validate a name (measurement or tag key/value) against naming rules.
    ///
    /// Rejects empty names, names exceeding [`MAX_NAME_LENGTH`], and names
    /// containing null bytes.
    /// # Errors
    ///
    /// Returns [`SchemaError`] when the name is empty, too long, contains a
    /// NUL byte, or is not valid UTF-8 for the given label.
    pub fn validate_name(name: &str, label: &str) -> Result<(), SchemaError> {
        if name.is_empty() {
            return Err(SchemaError::EmptyName {
                label: label.to_string(),
            });
        }
        if name.len() > MAX_NAME_LENGTH {
            return Err(SchemaError::NameTooLong {
                label: label.to_string(),
                length: name.len(),
                max: MAX_NAME_LENGTH,
            });
        }
        if name.contains(TAG_SEPARATOR) {
            return Err(SchemaError::NullByte {
                label: label.to_string(),
            });
        }
        if name.contains(KV_SEPARATOR) {
            return Err(SchemaError::InvalidCharacter {
                label: label.to_string(),
                ch: KV_SEPARATOR,
                value: name.to_string(),
            });
        }
        Ok(())
    }
}

impl fmt::Display for SeriesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.measurement)?;
        for (key, value) in self.tags.iter() {
            write!(f, ",{key}={value}")?;
        }
        Ok(())
    }
}

/// A tombstone: a masked `[min, max]` timestamp interval of one series.
///
/// Two fields, and each of them is load-bearing for a defect this type used to
/// have.
///
/// **Every tombstone carries a time range** (D42). There is no "delete the
/// series forever" variant, because a time-series delete is a statement about data
/// that exists, not a standing order against data that does not exist yet.
/// The unranged form used to mask every future write to the same series as
/// well, so re-provisioning a device under an identifier that had once been
/// deleted silently discarded everything it sent. `delete_series` now resolves
/// its upper bound to the newest timestamp actually stored for that series, so
/// later points re-create it. This is also what Prometheus and InfluxDB do: a
/// delete names an interval, and re-ingesting *into* a deleted interval stays
/// masked until compaction materialises the delete.
///
/// **`segments` records the segments the delete was issued against** (D44) — every
/// active segment that could hold a matching row at that moment. It is not
/// consulted on the read path; it exists so that reclaiming a tombstone is a
/// provable step rather than a guess. A tombstone may be dropped exactly when
/// none of these segments is still in the catalog, because a segment leaves
/// the catalog only by being rewritten (compaction applies the tombstone) or
/// by being deleted outright (the rows are gone). The previous rule — drop the
/// tombstone once its series left `known_series` — held after a compaction of
/// *any* segment, so an unrelated compaction pass resurrected deleted rows
/// that lived in a segment it never touched.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tombstone {
    /// Canonical series key (`measurement\0tag1\x01v1\0tag2\x01v2`).
    pub series_canonical: String,
    /// Inclusive time range that this tombstone masks.
    pub time_range: (i64, i64),
    /// Ids of the segments that were active when the delete was issued.
    ///
    /// Used only to decide when the tombstone may be reclaimed — never on the
    /// read path, so a row never needs to know which segment it came from.
    pub segments: std::collections::BTreeSet<u64>,
}

impl Tombstone {
    /// Create a tombstone masking `[min_ts, max_ts]` of `canonical`.
    #[must_use]
    pub fn ranged(canonical: impl Into<String>, min_ts: i64, max_ts: i64) -> Self {
        Self {
            series_canonical: canonical.into(),
            time_range: (min_ts, max_ts),
            segments: std::collections::BTreeSet::new(),
        }
    }

    /// Create a tombstone masking every timestamp of `canonical`.
    ///
    /// Prefer [`ranged`](Self::ranged) with a resolved upper bound wherever the
    /// newest stored timestamp is known: an open-ended tombstone also masks
    /// data written *after* the delete, which is almost never what a caller
    /// means. This constructor exists for the one case where it is — dropping
    /// a series whose segments are being removed in the same operation.
    #[must_use]
    pub fn all_time(canonical: impl Into<String>) -> Self {
        Self::ranged(canonical, i64::MIN, i64::MAX)
    }

    /// Record the segments this tombstone was issued against.
    #[must_use]
    pub fn with_segments(mut self, segments: impl IntoIterator<Item = u64>) -> Self {
        self.segments = segments.into_iter().collect();
        self
    }

    /// Check whether a (`series_canonical`, timestamp) pair is tombstoned.
    #[must_use]
    pub fn matches(&self, canonical: &str, timestamp: i64) -> bool {
        self.series_canonical == canonical
            && timestamp >= self.time_range.0
            && timestamp <= self.time_range.1
    }
}

/// A collection of tombstones optimized for O(1) lookup by series key.
///
/// Internally stores tombstones grouped by canonical key. During compaction,
/// each row is checked against tombstones for its series, supporting both
/// full-series and ranged deletes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TombstoneSet {
    /// Map from `series_canonical` → list of tombstones for that series.
    inner: std::collections::HashMap<String, Vec<Tombstone>>,
}

impl TombstoneSet {
    /// Create an empty tombstone set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: std::collections::HashMap::new(),
        }
    }

    /// Insert a tombstone, ignoring an exact duplicate.
    ///
    /// Deduplicating matters because retrying a delete is the documented
    /// response to a partial one (`segments_skipped > 0`), so the same
    /// tombstone genuinely arrives twice. A duplicate masks nothing extra and
    /// only costs memory, but it costs it permanently — a tombstone lives
    /// until compaction materialises it.
    pub fn insert(&mut self, tombstone: Tombstone) {
        let entry = self
            .inner
            .entry(tombstone.series_canonical.clone())
            .or_default();
        if !entry.contains(&tombstone) {
            entry.push(tombstone);
        }
    }

    /// Check whether a (canonical, timestamp) pair is tombstoned.
    ///
    /// This is the **only** admissible tombstone test on a read path. A
    /// series-only variant used to sit beside it, and the two disagreed: the
    /// segment scan and compaction asked this question, while the memtable
    /// scan, `last_value` and the streaming scan asked "does this series have
    /// any tombstone at all", so a delete of one hour of one series removed
    /// that series entirely from three of the five read paths.
    #[must_use]
    pub fn is_tombstoned(&self, canonical: &str, timestamp: i64) -> bool {
        self.inner
            .get(canonical)
            .is_some_and(|tombstones| tombstones.iter().any(|t| t.matches(canonical, timestamp)))
    }

    /// Whether any tombstone exists for a series, at any timestamp.
    ///
    /// For bookkeeping only — reclaiming tombstones, reporting, tests. Using
    /// it to filter rows is the bug described on
    /// [`is_tombstoned`](Self::is_tombstoned).
    #[must_use]
    pub fn contains_series(&self, canonical: &str) -> bool {
        self.inner.contains_key(canonical)
    }

    /// Iterate over every tombstone in the set.
    pub fn iter(&self) -> impl Iterator<Item = &Tombstone> {
        self.inner.values().flatten()
    }

    /// Total number of tombstones, counting each range separately.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.inner.values().map(Vec::len).sum()
    }

    /// Drop every tombstone that does not satisfy `keep`.
    ///
    /// Returns the tombstones that were removed, so the caller can persist the
    /// removals rather than let memory and disk drift apart.
    pub fn retain_tombstones<F: FnMut(&Tombstone) -> bool>(
        &mut self,
        mut keep: F,
    ) -> Vec<Tombstone> {
        let mut removed = Vec::new();
        self.inner.retain(|_, tombstones| {
            tombstones.retain(|t| {
                if keep(t) {
                    true
                } else {
                    removed.push(t.clone());
                    false
                }
            });
            !tombstones.is_empty()
        });
        removed
    }

    /// Number of unique series with tombstones.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterate over all series canonical keys.
    pub fn series_keys(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(String::as_str)
    }

    /// Remove a series from the tombstone set. Returns true if it was present.
    pub fn remove(&mut self, canonical: &str) -> bool {
        self.inner.remove(canonical).is_some()
    }

    /// Retain only the series matching a predicate.
    pub fn retain<F: FnMut(&str) -> bool>(&mut self, mut f: F) {
        self.inner.retain(|k, _| f(k));
    }
}

/// A typed WAL record envelope.
///
/// The WAL stores opaque byte payloads. This enum provides a discriminator so
/// that different record kinds (writes, deletes) can coexist in the same log
/// and be correctly replayed on crash recovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WalEntry {
    /// A data-point write.
    Write {
        /// The point to insert.
        point: Point,
    },
    /// A delete: the tombstones it produced.
    ///
    /// One variant rather than the previous `DeleteSeries` + `DeletePredicate`
    /// pair. Those carried the *request* — measurement, tag filters, time
    /// bounds — and replay used none of it except the list of canonical keys,
    /// which it then reconstructed as unranged tombstones. A ranged delete
    /// therefore widened into a whole-series delete at the next startup.
    /// Logging the resolved tombstones instead makes replay exact by
    /// construction, and there is nothing left for the two shapes to disagree
    /// about.
    Delete {
        /// The tombstones the delete resolved to.
        tombstones: Vec<Tombstone>,
    },
    /// A schema evolution action (create measurement or add column).
    ///
    /// Logged to WAL **before** the point that triggers the schema change,
    /// ensuring crash recovery can rebuild the schema from the log alone.
    SchemaChange {
        /// The schema actions to apply (create measurement and/or add columns).
        actions: Vec<crate::schema::SchemaAction>,
    },
}

/// Lifecycle state for a segment file.
///
/// Tracks whether a segment is actively serving queries, being compacted,
/// or has been soft-deleted and is waiting for garbage collection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentState {
    /// Active — serving reads and writes.
    #[default]
    Active,
    /// Being compacted — still serving reads.
    Compacting,
    /// Soft-deleted — queued for garbage collection.
    SoftDeleted {
        /// When the segment was marked for deletion (millis since epoch).
        deleted_at_ms: u64,
    },
}

impl std::fmt::Display for SegmentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "Active"),
            Self::Compacting => write!(f, "Compacting"),
            Self::SoftDeleted { deleted_at_ms } => {
                write!(f, "SoftDeleted(at={deleted_at_ms})")
            }
        }
    }
}

/// A single time-series data point.
///
/// Combines a [`SeriesKey`] (measurement + tags), a timestamp, and one or more
/// typed field values. Fields are stored as a sorted `Vec<(Arc<str>, FieldValue)>`
/// for allocation efficiency and cache-friendly access.
///
/// Fields are private to ensure invariants (at least one field, valid field
/// names) are maintained — use [`Point::new()`] to construct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    series_key: SeriesKey,
    #[serde(
        serialize_with = "serialize_fields",
        deserialize_with = "deserialize_fields"
    )]
    fields: Fields,
    timestamp: Timestamp,
}

impl Point {
    /// Create a new point with validation.
    ///
    /// Accepts a `BTreeMap` for ergonomic construction; internally converts
    /// to a sorted `Vec` for cache-friendly storage.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] if the series key is invalid or fields are empty.
    pub fn new(
        series_key: SeriesKey,
        fields: BTreeMap<String, FieldValue>,
        timestamp: Timestamp,
    ) -> Result<Self, SchemaError> {
        if fields.is_empty() {
            return Err(SchemaError::EmptyFields);
        }
        // FINDING-02: Reject points with too many fields.
        if fields.len() > MAX_FIELDS_PER_POINT {
            return Err(SchemaError::TooManyFields {
                count: fields.len(),
                max: MAX_FIELDS_PER_POINT,
            });
        }
        for (key, value) in &fields {
            SeriesKey::validate_name(key, "field name")?;

            // FINDING-27: Reject the reserved field name "time".
            if key == "time" {
                return Err(SchemaError::InvalidFieldValue {
                    field: key.clone(),
                    reason: "'time' is a reserved column name and cannot be used as a field".into(),
                });
            }

            // FINDING-01: Reject non-finite f64 values (NaN, Infinity).
            // NaN breaks dedup (NaN ≠ NaN), sorting, and equality checks.
            if let FieldValue::F64(v) = value {
                if !v.is_finite() {
                    return Err(SchemaError::InvalidFieldValue {
                        field: key.clone(),
                        reason: format!("non-finite f64 value: {v}"),
                    });
                }
            }

            // FINDING-03: Reject oversized string field values.
            if let FieldValue::String(s) = value {
                if s.len() > MAX_STRING_FIELD_LENGTH {
                    return Err(SchemaError::InvalidFieldValue {
                        field: key.clone(),
                        reason: format!(
                            "string value too long: {} bytes (max {})",
                            s.len(),
                            MAX_STRING_FIELD_LENGTH,
                        ),
                    });
                }
            }
        }
        // BTreeMap iterates in sorted order, so the resulting Vec is sorted.
        let sorted_fields: Fields = fields
            .into_iter()
            .map(|(k, v)| (Arc::from(k.as_str()), v))
            .collect();
        Ok(Self {
            series_key,
            fields: sorted_fields,
            timestamp,
        })
    }

    /// Returns the series key for this point.
    #[inline]
    #[must_use]
    pub fn series_key(&self) -> &SeriesKey {
        &self.series_key
    }

    /// Returns the field values as a sorted slice.
    #[inline]
    #[must_use]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Look up a field value by key using binary search.
    #[inline]
    #[must_use]
    pub fn field(&self, key: &str) -> Option<&FieldValue> {
        sorted_vec_get(&self.fields, key)
    }

    /// Returns an iterator over field keys.
    #[inline]
    pub fn field_keys(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|(k, _)| k.as_ref())
    }

    /// Returns the timestamp.
    #[inline]
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    // ── Delegation from SeriesKey ─────────────────────────────────────
    // These convenience methods delegate to the inner `SeriesKey` so
    // callers can write `point.tag("host")` instead of
    // `point.series_key().tag("host")`.

    /// Returns the measurement name (delegates to [`SeriesKey::measurement`]).
    #[inline]
    #[must_use]
    pub fn measurement(&self) -> &str {
        self.series_key.measurement()
    }

    /// Returns the tag set (delegates to [`SeriesKey::tags`]).
    #[inline]
    #[must_use]
    pub fn tags(&self) -> &Tags {
        self.series_key.tags()
    }

    /// Look up a tag value by key (delegates to [`SeriesKey::tag`]).
    #[inline]
    #[must_use]
    pub fn tag(&self, key: &str) -> Option<&str> {
        self.series_key.tag(key)
    }

    /// Iterator over tag keys (delegates to [`SeriesKey::tag_keys`]).
    #[inline]
    pub fn tag_keys(&self) -> impl Iterator<Item = &str> {
        self.series_key.tag_keys()
    }

    /// Inject a tag into this point's series key.
    ///
    /// Inject a tag into this point's series key using copy-on-write (FINDING-22).
    ///
    /// Delegates to [`SeriesKey::inject_tag`] which uses `Arc::make_mut`
    /// to avoid unnecessary deep clones.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] if the tag name is invalid.
    pub fn inject_tag(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), SchemaError> {
        self.series_key.inject_tag(key, value)
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.series_key)?;
        let mut first = true;
        for (key, value) in &self.fields {
            if first {
                write!(f, " ")?;
                first = false;
            } else {
                write!(f, ",")?;
            }
            write!(f, "{key}={value}")?;
        }
        write!(f, " {}", self.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn field_value_type_name() {
        assert_eq!(FieldValue::F64(1.0).type_name(), "f64");
        assert_eq!(FieldValue::I64(1).type_name(), "i64");
        assert_eq!(FieldValue::U64(1).type_name(), "u64");
        assert_eq!(FieldValue::Bool(true).type_name(), "bool");
        assert_eq!(FieldValue::String("x".into()).type_name(), "string");
    }

    #[test]
    fn field_value_same_type() {
        assert!(FieldValue::F64(1.0).same_type_as(&FieldValue::F64(2.0)));
        assert!(!FieldValue::F64(1.0).same_type_as(&FieldValue::I64(1)));
    }

    #[test]
    fn field_value_display() {
        assert_eq!(format!("{}", FieldValue::F64(72.5)), "72.5");
        assert_eq!(format!("{}", FieldValue::I64(42)), "42i");
        assert_eq!(format!("{}", FieldValue::U64(99)), "99u");
        assert_eq!(format!("{}", FieldValue::Bool(true)), "true");
        assert_eq!(format!("{}", FieldValue::String("hi".into())), "\"hi\"");
    }

    #[test]
    fn field_value_serde_roundtrip() {
        let values = vec![
            FieldValue::F64(72.5),
            FieldValue::I64(-42),
            FieldValue::U64(u64::MAX),
            FieldValue::Bool(false),
            FieldValue::String("hello world".into()),
        ];
        for v in &values {
            let json = serde_json::to_string(v).unwrap();
            let back: FieldValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, &back);
        }
    }

    #[test]
    fn series_key_valid() {
        let key = SeriesKey::new("cpu_usage", make_tags(&[("host", "srv1"), ("dc", "eu")]));
        assert!(key.is_ok());
        let key = key.unwrap();
        assert_eq!(key.measurement(), "cpu_usage");
        assert_eq!(key.tag("host"), Some("srv1"));
        assert_eq!(key.tag("dc"), Some("eu"));
        assert_eq!(key.tag("missing"), None);
    }

    #[test]
    fn series_key_empty_measurement() {
        let r = SeriesKey::new("", BTreeMap::new());
        assert!(matches!(r, Err(SchemaError::EmptyName { .. })));
    }

    #[test]
    fn series_key_measurement_too_long() {
        let long = "x".repeat(MAX_NAME_LENGTH + 1);
        let r = SeriesKey::new(long, BTreeMap::new());
        assert!(matches!(r, Err(SchemaError::NameTooLong { .. })));
    }

    #[test]
    fn series_key_null_byte_in_measurement() {
        let r = SeriesKey::new("cpu\0usage", BTreeMap::new());
        assert!(matches!(r, Err(SchemaError::NullByte { .. })));
    }

    /// Tag values may contain `=` — InfluxDB Line Protocol permits it
    /// (escaped) and real tags carry it. The canonical form stays injective
    /// because it separates with reserved control characters instead.
    #[test]
    fn tag_values_may_contain_equals() {
        let key = SeriesKey::new(
            "http",
            BTreeMap::from([("url".to_string(), "/s?q=1&n=2".to_string())]),
        )
        .expect("'=' in a tag value must be accepted");
        assert_eq!(key.tag("url"), Some("/s?q=1&n=2"));
    }

    /// The canonical form must be injective: no two distinct tag sets may
    /// produce the same string.
    #[test]
    fn canonical_form_is_injective_across_equals_placements() {
        let a = SeriesKey::new("m", BTreeMap::from([("a".to_string(), "b=c".to_string())]))
            .expect("valid");
        let b = SeriesKey::new("m", BTreeMap::from([("a=b".to_string(), "c".to_string())]))
            .expect("valid");
        assert_ne!(
            a.canonical_form(),
            b.canonical_form(),
            "distinct tag sets collided in the canonical form"
        );
        assert_ne!(a.hash_fnv(), b.hash_fnv());
    }

    /// Both separators are reserved and must be rejected in user data.
    #[test]
    fn reserved_separators_are_rejected() {
        for bad in ["a\0b", "a\x01b"] {
            assert!(
                SeriesKey::new("m", BTreeMap::from([("k".to_string(), bad.to_string())])).is_err(),
                "reserved separator accepted in tag value: {bad:?}"
            );
            assert!(
                SeriesKey::new("m", BTreeMap::from([(bad.to_string(), "v".to_string())])).is_err(),
                "reserved separator accepted in tag key: {bad:?}"
            );
            assert!(
                SeriesKey::new(bad, BTreeMap::new()).is_err(),
                "reserved separator accepted in measurement: {bad:?}"
            );
        }
    }

    #[test]
    fn series_key_tag_key_too_long() {
        let long_key = "k".repeat(MAX_NAME_LENGTH + 1);
        let r = SeriesKey::new("cpu", make_tags(&[(&long_key, "v")]));
        assert!(matches!(r, Err(SchemaError::NameTooLong { .. })));
    }

    #[test]
    fn series_key_tag_value_too_long() {
        let long_val = "v".repeat(MAX_NAME_LENGTH + 1);
        let r = SeriesKey::new("cpu", make_tags(&[("k", &long_val)]));
        assert!(matches!(r, Err(SchemaError::NameTooLong { .. })));
    }

    #[test]
    fn series_key_too_many_tags() {
        let tags: BTreeMap<String, String> = (0..=MAX_TAGS_PER_SERIES)
            .map(|i| (format!("key{i}"), format!("val{i}")))
            .collect();
        let r = SeriesKey::new("cpu", tags);
        assert!(matches!(r, Err(SchemaError::TooManyTags { .. })));
    }

    #[test]
    fn series_key_max_tags_ok() {
        let tags: BTreeMap<String, String> = (0..MAX_TAGS_PER_SERIES)
            .map(|i| (format!("key{i:02}"), format!("val{i}")))
            .collect();
        assert!(SeriesKey::new("cpu", tags).is_ok());
    }

    #[test]
    fn series_key_hash_deterministic() {
        let key1 = SeriesKey::new("cpu", make_tags(&[("host", "srv1"), ("dc", "eu")])).unwrap();
        let key2 = SeriesKey::new("cpu", make_tags(&[("dc", "eu"), ("host", "srv1")])).unwrap();
        // BTreeMap sorts by key, so order shouldn't matter
        assert_eq!(key1.hash_fnv(), key2.hash_fnv());
    }

    #[test]
    fn series_key_hash_different_for_different_keys() {
        let key1 = SeriesKey::new("cpu", make_tags(&[("host", "srv1")])).unwrap();
        let key2 = SeriesKey::new("cpu", make_tags(&[("host", "srv2")])).unwrap();
        assert_ne!(key1.hash_fnv(), key2.hash_fnv());
    }

    #[test]
    fn series_key_canonical_form() {
        let key = SeriesKey::new("cpu", make_tags(&[("dc", "eu"), ("host", "srv1")])).unwrap();
        assert_eq!(key.canonical_form(), "cpu\0dc\x01eu\0host\x01srv1");
    }

    #[test]
    fn series_key_display() {
        let key = SeriesKey::new("cpu", make_tags(&[("dc", "eu"), ("host", "srv1")])).unwrap();
        assert_eq!(format!("{key}"), "cpu,dc=eu,host=srv1");
    }

    #[test]
    fn series_key_serde_roundtrip() {
        let key = SeriesKey::new("cpu", make_tags(&[("host", "srv1")])).unwrap();
        let json = serde_json::to_string(&key).unwrap();
        let back: SeriesKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, back);
    }

    #[test]
    fn segment_id_display() {
        assert_eq!(format!("{}", SegmentId(42)), "seg-42");
    }

    #[test]
    fn shard_id_from_timestamp() {
        let duration = Duration::from_secs(3600);
        let ns_per_hour = 3_600_000_000_000_i64;

        // Exact boundary
        let shard = ShardId::from_timestamp(0, duration);
        assert_eq!(shard, ShardId(0));

        // Mid-shard
        let shard = ShardId::from_timestamp(ns_per_hour / 2, duration);
        assert_eq!(shard, ShardId(0));

        // Next shard
        let shard = ShardId::from_timestamp(ns_per_hour, duration);
        assert_eq!(shard, ShardId(1));

        // Negative timestamp
        let shard = ShardId::from_timestamp(-1, duration);
        assert_eq!(shard, ShardId(-1));
    }

    #[test]
    fn shard_id_start_end_timestamps() {
        let duration = Duration::from_secs(3600);
        let ns_per_hour = 3_600_000_000_000_i64;

        let shard = ShardId(2);
        assert_eq!(shard.start_timestamp(duration), 2 * ns_per_hour);
        assert_eq!(shard.end_timestamp(duration), 3 * ns_per_hour);
    }

    #[test]
    fn point_valid() {
        let key = SeriesKey::new("cpu", make_tags(&[("host", "srv1")])).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(72.5));
        let point = Point::new(key, fields, 1_000_000_000);
        assert!(point.is_ok());
    }

    #[test]
    fn point_empty_fields() {
        let key = SeriesKey::new("cpu", BTreeMap::new()).unwrap();
        let r = Point::new(key, BTreeMap::new(), 0);
        assert!(matches!(r, Err(SchemaError::EmptyFields)));
    }

    #[test]
    fn point_display() {
        let key = SeriesKey::new("cpu", make_tags(&[("host", "srv1")])).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(72.5));
        fields.insert("cores".to_string(), FieldValue::I64(8));
        let point = Point::new(key, fields, 1_000_000_000).unwrap();
        assert_eq!(
            format!("{point}"),
            "cpu,host=srv1 cores=8i,value=72.5 1000000000"
        );
    }

    #[test]
    fn point_serde_roundtrip() {
        let key = SeriesKey::new("cpu", make_tags(&[("host", "srv1")])).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(72.5));
        let point = Point::new(key, fields, 1_000_000_000).unwrap();
        let json = serde_json::to_string(&point).unwrap();
        let back: Point = serde_json::from_str(&json).unwrap();
        assert_eq!(point, back);
    }

    #[test]
    fn point_accessors() {
        let key = SeriesKey::new("cpu", make_tags(&[("host", "srv1")])).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(72.5));
        let point = Point::new(key.clone(), fields, 42).unwrap();
        assert_eq!(point.series_key(), &key);
        assert_eq!(point.field("value"), Some(&FieldValue::F64(72.5)));
        assert_eq!(point.fields().len(), 1);
        assert_eq!(point.timestamp(), 42);
    }

    #[test]
    fn point_field_name_with_null_byte() {
        let key = SeriesKey::new("cpu", BTreeMap::new()).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("val\0ue".to_string(), FieldValue::F64(1.0));
        let r = Point::new(key, fields, 0);
        assert!(matches!(r, Err(SchemaError::NullByte { .. })));
    }

    #[test]
    fn shard_id_display() {
        assert_eq!(format!("{}", ShardId(7)), "shard-7");
        assert_eq!(format!("{}", ShardId(-3)), "shard--3");
    }

    #[test]
    fn segment_id_serde_roundtrip() {
        let id = SegmentId(42);
        let json = serde_json::to_string(&id).unwrap();
        let back: SegmentId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn series_key_null_byte_in_tag_key() {
        let tags = make_tags(&[("ho\0st", "srv1")]);
        let r = SeriesKey::new("cpu", tags);
        assert!(matches!(r, Err(SchemaError::NullByte { .. })));
    }

    #[test]
    fn series_key_null_byte_in_tag_value() {
        let tags = make_tags(&[("host", "srv\x001")]);
        let r = SeriesKey::new("cpu", tags);
        assert!(matches!(r, Err(SchemaError::NullByte { .. })));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn valid_name_strategy() -> impl Strategy<Value = String> {
        "[a-zA-Z_][a-zA-Z0-9_]{0,63}".prop_map(String::from)
    }

    fn tag_map_strategy() -> impl Strategy<Value = BTreeMap<String, String>> {
        proptest::collection::btree_map(valid_name_strategy(), valid_name_strategy(), 0..16)
    }

    proptest! {
        #[test]
        fn series_key_valid_names_always_accepted(
            measurement in valid_name_strategy(),
            tags in tag_map_strategy(),
        ) {
            let result = SeriesKey::new(measurement, tags);
            prop_assert!(result.is_ok());
        }

        #[test]
        fn series_key_hash_is_deterministic(
            measurement in valid_name_strategy(),
            tags in tag_map_strategy(),
        ) {
            let key1 = SeriesKey::new(measurement.clone(), tags.clone()).unwrap();
            let key2 = SeriesKey::new(measurement, tags).unwrap();
            prop_assert_eq!(key1.hash_fnv(), key2.hash_fnv());
        }

        #[test]
        fn shard_id_contains_timestamp(
            ts in -1_000_000_000_000_000_000i64..1_000_000_000_000_000_000i64,
            shard_secs in 1u64..86400u64,
        ) {
            let duration = Duration::from_secs(shard_secs);
            let shard = ShardId::from_timestamp(ts, duration);
            let start = shard.start_timestamp(duration);
            let end = shard.end_timestamp(duration);
            prop_assert!(ts >= start, "ts={ts} < start={start}");
            prop_assert!(ts < end, "ts={ts} >= end={end}");
        }

        #[test]
        fn field_value_serde_roundtrip(v in prop_oneof![
            any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(FieldValue::F64),
            any::<i64>().prop_map(FieldValue::I64),
            any::<u64>().prop_map(FieldValue::U64),
            any::<bool>().prop_map(FieldValue::Bool),
            "[a-zA-Z0-9 ]{0,100}".prop_map(FieldValue::String),
        ]) {
            let json = serde_json::to_string(&v).unwrap();
            let back: FieldValue = serde_json::from_str(&json).unwrap();
            if let (FieldValue::F64(a), FieldValue::F64(b)) = (&v, &back) {
                // JSON (RFC 7159) represents f64 in decimal. Round-trip may
                // introduce up to 1 ULP of error.  We tolerate a relative
                // difference of 2^-50 ≈ 8.9e-16, which is just above the
                // theoretical limit of 2^-52 and accommodates decimal
                // round-trip drift at any magnitude.
                let rel_tol: f64 = 2.0_f64.powi(-50);
                let diff = (a - b).abs();
                let mag = a.abs().max(b.abs()).max(f64::MIN_POSITIVE);
                prop_assert!(
                    diff <= rel_tol * mag,
                    "f64 mismatch: {a} vs {b} (rel err {:.2e})",
                    diff / mag,
                );
            } else { prop_assert_eq!(&v, &back) }
        }
    }

    #[test]
    fn wal_entry_write_roundtrip() {
        let key = SeriesKey::new("cpu", BTreeMap::from([("host".into(), "srv-1".into())])).unwrap();
        let fields = BTreeMap::from([("value".to_string(), FieldValue::F64(42.5))]);
        let point = Point::new(key, fields, 1_000_000_000).unwrap();
        let entry = WalEntry::Write {
            point: point.clone(),
        };

        let json = serde_json::to_vec(&entry).unwrap();
        let back: WalEntry = serde_json::from_slice(&json).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn wal_entry_delete_roundtrip() {
        let entry = WalEntry::Delete {
            tombstones: vec![
                Tombstone::ranged("cpu\0host=srv-1", 100, 200),
                Tombstone::ranged("cpu\0host=srv-2", i64::MIN, 900).with_segments([7, 8]),
            ],
        };
        let json = serde_json::to_vec(&entry).unwrap();
        let back: WalEntry = serde_json::from_slice(&json).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn a_ranged_tombstone_masks_only_its_range() {
        let t = Tombstone::ranged("cpu\0host=a", 100, 200);
        assert!(!t.matches("cpu\0host=a", 99));
        assert!(t.matches("cpu\0host=a", 100));
        assert!(t.matches("cpu\0host=a", 200));
        assert!(!t.matches("cpu\0host=a", 201));
        assert!(
            !t.matches("cpu\0host=b", 150),
            "a different series is untouched"
        );
    }

    #[test]
    fn inserting_the_same_tombstone_twice_stores_it_once() {
        // Retrying a partial delete is the documented recovery, so the same
        // tombstone really does arrive twice.
        let mut set = TombstoneSet::new();
        let t = Tombstone::ranged("cpu\0host=a", 0, 10).with_segments([1, 2]);
        set.insert(t.clone());
        set.insert(t);
        assert_eq!(set.tombstone_count(), 1);

        // A different range for the same series is not a duplicate.
        set.insert(Tombstone::ranged("cpu\0host=a", 20, 30).with_segments([1, 2]));
        assert_eq!(set.tombstone_count(), 2);
    }

    #[test]
    fn tombstone_set_reports_removals_so_they_can_be_persisted() {
        let mut set = TombstoneSet::new();
        set.insert(Tombstone::ranged("cpu\0host=a", 0, 10).with_segments([1]));
        set.insert(Tombstone::ranged("cpu\0host=a", 20, 30).with_segments([2]));
        set.insert(Tombstone::ranged("cpu\0host=b", 0, 10).with_segments([1]));
        assert_eq!(set.tombstone_count(), 3);

        // Keep only tombstones whose segment 2 is still live.
        let removed = set.retain_tombstones(|t| t.segments.contains(&2));
        assert_eq!(removed.len(), 2);
        assert_eq!(set.tombstone_count(), 1);
        assert!(
            !set.contains_series("cpu\0host=b"),
            "empty series entries are dropped"
        );
        assert!(set.is_tombstoned("cpu\0host=a", 25));
        assert!(
            !set.is_tombstoned("cpu\0host=a", 5),
            "the reclaimed range no longer masks"
        );
    }
}

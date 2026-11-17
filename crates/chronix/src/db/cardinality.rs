//! Cardinality statistics: where the series budget goes.
//!
//! Computed on request from the same set of known series the write path
//! admits new series against, so the numbers shown are the numbers a refusal
//! reports. No cache and no second counter.

use std::collections::HashMap;

use chronix_core::{KV_SEPARATOR, NAMESPACE_TAG, TAG_SEPARATOR};

use super::Chronix;
use crate::error::{DbError, Result};

/// The largest `limit` [`Chronix::cardinality`] accepts, as Prometheus does.
pub const MAX_CARDINALITY_LIMIT: usize = 10_000;

/// A name and a count — one row of a cardinality list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameCount {
    /// A measurement, a label key, or a `key=value` pair.
    pub name: String,
    /// The count the list is ranked by.
    pub value: u64,
}

/// Whose series a [`CardinalityStats`] counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardinalityScope {
    /// Every series in the database, against `max_series_cardinality`.
    Database,
    /// The series tagged with one namespace, against its series limit.
    Namespace(String),
}

/// Where the series budget goes, and how much of it is left.
///
/// Lists are ranked by value, highest first, then by name, and hold at most
/// the requested number of entries.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CardinalityStats {
    /// Series in scope. Always equal to [`used`](Self::used).
    pub num_series: u64,
    /// Active segments holding the scope's measurements.
    pub segment_count: u64,
    /// Oldest timestamp held, in milliseconds; `0` when there is none.
    pub min_time_ms: i64,
    /// Newest timestamp held, in milliseconds; `0` when there is none.
    pub max_time_ms: i64,
    /// Series per measurement.
    pub series_by_measurement: Vec<NameCount>,
    /// Distinct values per label key, including `__name__` (measurements).
    pub label_value_counts: Vec<NameCount>,
    /// Total bytes of each label key's distinct values.
    pub label_memory_bytes: Vec<NameCount>,
    /// Series per `key=value` pair.
    pub series_by_label_pair: Vec<NameCount>,
    /// The series limit for the scope; `None` when it has none.
    pub budget: Option<u64>,
    /// Series counted against the budget.
    pub used: u64,
    /// New series the budget still admits; `None` when unlimited.
    pub headroom: Option<u64>,
    /// Whose series these are.
    pub scope: CardinalityScope,
    /// Whether writes are refused once the budget is spent. Always `true`:
    /// both the database budget and namespace limits are enforced at
    /// admission.
    pub enforced: bool,
}

impl Chronix {
    /// Where the series budget goes: series per measurement, label and
    /// label pair, and the headroom left.
    ///
    /// `namespace` scopes everything to the series tagged with it, against
    /// that namespace's limit. `limit` caps each list (`1..=10_000`).
    ///
    /// One pass over the known series; no sample is read. The `__namespace__`
    /// tag is never listed.
    ///
    /// ```no_run
    /// # use chronix::Chronix;
    /// # let db = Chronix::open_small("./data")?;
    /// let stats = db.cardinality(5, None)?;
    /// println!("{} series, headroom {:?}", stats.used, stats.headroom);
    /// for m in &stats.series_by_measurement {
    ///     println!("{:>8}  {}", m.value, m.name);
    /// }
    /// # Ok::<(), chronix::DbError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`DbError::InvalidRequest`] when `limit` is `0` or above `10_000`.
    pub fn cardinality(&self, limit: usize, namespace: Option<&str>) -> Result<CardinalityStats> {
        if limit == 0 || limit > MAX_CARDINALITY_LIMIT {
            return Err(DbError::InvalidRequest(format!(
                "limit must be between 1 and {MAX_CARDINALITY_LIMIT}, got {limit}"
            )));
        }

        // One hash lookup per tag: a pair is already a contiguous
        // `key\x01value` slice of the canonical form, so pairs are counted by
        // that slice and the per-label lists are derived from the pairs.
        let ns_pair = namespace.map(|ns| format!("{NAMESPACE_TAG}{KV_SEPARATOR}{ns}"));
        let ns_prefix = format!("{NAMESPACE_TAG}{KV_SEPARATOR}");
        let mut num_series = 0u64;
        let mut by_measurement: HashMap<String, u64> = HashMap::new();
        let mut by_pair: HashMap<String, u64> = HashMap::new();

        for canonical in self.known_series.iter() {
            let mut parts = canonical.split(TAG_SEPARATOR);
            let measurement = parts.next().unwrap_or_default();
            if let Some(ns_pair) = &ns_pair
                && !parts.clone().any(|pair| pair == ns_pair)
            {
                continue;
            }
            num_series += 1;
            bump(&mut by_measurement, measurement);
            for pair in parts {
                if !pair.starts_with(&ns_prefix) {
                    bump(&mut by_pair, pair);
                }
            }
        }

        // label → (distinct values, bytes of those values)
        let mut labels: HashMap<&str, (u64, u64)> = HashMap::new();
        let mut label_pairs: Vec<(&str, &str, u64)> =
            Vec::with_capacity(by_pair.len() + by_measurement.len());
        for (pair, count) in &by_pair {
            let (key, value) = pair.split_once(KV_SEPARATOR).unwrap_or((pair.as_str(), ""));
            let entry = labels.entry(key).or_default();
            entry.0 += 1;
            entry.1 += value.len() as u64;
            label_pairs.push((key, value, *count));
        }
        if !by_measurement.is_empty() {
            let entry = labels.entry("__name__").or_default();
            for (name, count) in &by_measurement {
                entry.0 += 1;
                entry.1 += name.len() as u64;
                label_pairs.push(("__name__", name.as_str(), *count));
            }
        }
        let (label_values, label_bytes): (Vec<NameCount>, Vec<NameCount>) = labels
            .iter()
            .map(|(key, (values, bytes))| {
                (
                    NameCount {
                        name: (*key).to_string(),
                        value: *values,
                    },
                    NameCount {
                        name: (*key).to_string(),
                        value: *bytes,
                    },
                )
            })
            .unzip();
        let series_by_label_pair = top_pairs(label_pairs, limit);

        let (budget, scope) = match namespace {
            None => (
                Some(self.config.max_series_cardinality as u64),
                CardinalityScope::Database,
            ),
            Some(ns) => (
                self.namespace_limits
                    .get(ns)
                    .map(|l| l.max_series)
                    .filter(|&max| max != u64::MAX),
                CardinalityScope::Namespace(ns.to_string()),
            ),
        };
        let (segment_count, bounds) = self.scope_extent(namespace);
        let (min_time_ms, max_time_ms) = bounds.map_or((0, 0), |(lo, hi)| {
            (lo.div_euclid(1_000_000), hi.div_euclid(1_000_000))
        });

        Ok(CardinalityStats {
            num_series,
            segment_count,
            min_time_ms,
            max_time_ms,
            series_by_measurement: top(
                by_measurement
                    .into_iter()
                    .map(|(name, value)| NameCount { name, value })
                    .collect(),
                limit,
            ),
            label_value_counts: top(label_values, limit),
            label_memory_bytes: top(label_bytes, limit),
            series_by_label_pair,
            budget,
            used: num_series,
            headroom: budget.map(|b| b.saturating_sub(num_series)),
            scope,
            enforced: true,
        })
    }

    /// Segment count and `(min, max)` nanosecond bounds for a scope.
    ///
    /// The database scope covers every segment and the unflushed data. A
    /// namespace covers the segments of the measurements it writes to — its
    /// unflushed data shares memtables with every other namespace, and
    /// reporting their bounds would tell one tenant about another's writes.
    fn scope_extent(&self, namespace: Option<&str>) -> (u64, Option<(i64, i64)>) {
        let measurements = namespace.map(|ns| {
            self.namespace_measurements
                .get(ns)
                .map(|set| set.iter().map(|m| m.key().clone()).collect::<Vec<_>>())
                .unwrap_or_default()
        });
        let catalog = self.catalog.read();
        let mut count = 0u64;
        let mut bounds: Option<(i64, i64)> = None;
        for entry in catalog.all_segments() {
            if let Some(ms) = &measurements
                && !ms.contains(&entry.measurement)
            {
                continue;
            }
            count += 1;
            bounds = chronix_engine::memtable::flush::merge_bounds(
                bounds,
                Some((entry.min_timestamp, entry.max_timestamp)),
            );
        }
        drop(catalog);
        if namespace.is_none() {
            bounds =
                chronix_engine::memtable::flush::merge_bounds(bounds, self.shards.time_bounds());
        }
        (count, bounds)
    }
}

/// Count one occurrence of `name`, allocating only the first time it is seen.
fn bump(counts: &mut HashMap<String, u64>, name: &str) {
    if let Some(n) = counts.get_mut(name) {
        *n += 1;
    } else {
        counts.insert(name.to_string(), 1);
    }
}

/// Highest value first, then name; at most `limit`.
fn top(mut list: Vec<NameCount>, limit: usize) -> Vec<NameCount> {
    let order =
        |a: &NameCount, b: &NameCount| b.value.cmp(&a.value).then_with(|| a.name.cmp(&b.name));
    if list.len() > limit {
        list.select_nth_unstable_by(limit - 1, order);
        list.truncate(limit);
    }
    list.sort_unstable_by(order);
    list
}

/// [`top`] for label pairs, formatting `key=value` only for the survivors.
fn top_pairs(mut pairs: Vec<(&str, &str, u64)>, limit: usize) -> Vec<NameCount> {
    // Ties break on the rendered `key=value`, byte for byte, without
    // rendering it.
    let order = |a: &(&str, &str, u64), b: &(&str, &str, u64)| {
        b.2.cmp(&a.2).then_with(|| {
            a.0.bytes()
                .chain(std::iter::once(b'='))
                .chain(a.1.bytes())
                .cmp(b.0.bytes().chain(std::iter::once(b'=')).chain(b.1.bytes()))
        })
    };
    if pairs.len() > limit {
        pairs.select_nth_unstable_by(limit - 1, order);
        pairs.truncate(limit);
    }
    pairs.sort_unstable_by(order);
    pairs
        .into_iter()
        .map(|(k, v, value)| NameCount {
            name: format!("{k}={v}"),
            value,
        })
        .collect()
}

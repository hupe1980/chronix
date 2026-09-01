//! Materialized forecast views — pre-computed, cached forecast results.
//!
//! Stores the latest forecast for each series so queries can be served
//! from cache without re-fitting.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::forecast::ForecastResult;
use dashmap::DashMap;
use metrics::counter;
use tracing::debug;

/// A materialized (cached) forecast for a series.
#[derive(Debug, Clone)]
pub struct MaterializedForecast {
    /// The forecast result.
    pub result: ForecastResult,
    /// Measurement name.
    pub measurement: String,
    /// Series tags.
    pub tags: BTreeMap<String, String>,
    /// When this forecast was computed.
    pub computed_at: Instant,
    /// Forecast horizon (number of predicted points).
    pub horizon: usize,
}

impl MaterializedForecast {
    /// Check if the forecast is still fresh.
    #[must_use]
    pub fn is_fresh(&self, max_age: std::time::Duration) -> bool {
        self.computed_at.elapsed() < max_age
    }
}

/// Key: `(measurement, canonical_tags_key)`.
type CacheKey = (String, String);

/// Default maximum number of cached forecast models.
pub const DEFAULT_MAX_CACHE_ENTRIES: usize = 10_000;

/// Cache of materialized forecasts.
pub struct ForecastCache {
    cache: DashMap<CacheKey, MaterializedForecast>,
    default_max_age: std::time::Duration,
    /// Maximum number of entries. When exceeded, stale entries are
    /// evicted on the next store. If the cache is still full after
    /// eviction, the oldest entry is removed.
    max_entries: usize,
}

impl ForecastCache {
    /// Create a new forecast cache.
    #[must_use]
    pub fn new(default_max_age: std::time::Duration) -> Self {
        Self {
            cache: DashMap::new(),
            default_max_age,
            max_entries: DEFAULT_MAX_CACHE_ENTRIES,
        }
    }

    /// Create a cache with a custom capacity limit.
    #[must_use]
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max.max(1);
        self
    }

    /// Store a materialized forecast.
    pub fn store(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        result: ForecastResult,
        horizon: usize,
    ) {
        let key = (measurement.to_string(), compute_tags_key(tags));
        self.cache.insert(
            key,
            MaterializedForecast {
                result,
                measurement: measurement.to_string(),
                tags: tags.clone(),
                computed_at: Instant::now(),
                horizon,
            },
        );

        // If the cache exceeds capacity, evict stale entries first,
        // then evict oldest if still over capacity.
        if self.cache.len() > self.max_entries {
            self.evict_stale();
        }
        if self.cache.len() > self.max_entries {
            // Remove the oldest entry
            if let Some(oldest_key) = self
                .cache
                .iter()
                .min_by_key(|entry| entry.value().computed_at)
                .map(|entry| entry.key().clone())
            {
                self.cache.remove(&oldest_key);
            }
        }
        counter!("chronix_forecast_materialized_total", "measurement" => measurement.to_string())
            .increment(1);
        debug!(measurement = %measurement, horizon, "Forecast materialized");
    }

    /// Look up a cached forecast, returning it if fresh.
    #[must_use]
    pub fn get(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
    ) -> Option<MaterializedForecast> {
        let key = (measurement.to_string(), compute_tags_key(tags));
        let entry = self.cache.get(&key)?;
        if entry.is_fresh(self.default_max_age) {
            counter!("chronix_forecast_cache_hit_total").increment(1);
            Some(entry.clone())
        } else {
            counter!("chronix_forecast_cache_miss_total").increment(1);
            None
        }
    }

    /// Get a cached forecast regardless of freshness.
    #[must_use]
    pub fn get_any(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
    ) -> Option<MaterializedForecast> {
        let key = (measurement.to_string(), compute_tags_key(tags));
        self.cache.get(&key).map(|r| r.value().clone())
    }

    /// Number of cached forecasts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Remove all stale entries.
    pub fn evict_stale(&self) -> usize {
        let before = self.cache.len();
        self.cache.retain(|_, v| v.is_fresh(self.default_max_age));
        let removed = before - self.cache.len();
        if removed > 0 {
            debug!(removed, "Evicted stale forecasts");
        }
        removed
    }

    /// Invalidate a specific series.
    pub fn invalidate(&self, measurement: &str, tags: &BTreeMap<String, String>) {
        let key = (measurement.to_string(), compute_tags_key(tags));
        self.cache.remove(&key);
    }

    /// Clear all cached forecasts.
    pub fn clear(&self) {
        self.cache.clear();
    }
}

impl Default for ForecastCache {
    fn default() -> Self {
        Self::new(std::time::Duration::from_secs(3600))
    }
}

use crate::util::compute_tags_key;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_tags(host: &str) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    fn make_result(n: usize) -> ForecastResult {
        ForecastResult {
            values: vec![1.0; n],
            timestamps: (0..n as i64).collect(),
            confidence_lower: vec![0.5; n],
            confidence_upper: vec![1.5; n],
            confidence_level: 0.95,
        }
    }

    #[test]
    fn store_and_retrieve() {
        let cache = ForecastCache::new(Duration::from_secs(3600));
        let tags = make_tags("host1");
        cache.store("cpu", &tags, make_result(5), 5);

        assert_eq!(cache.len(), 1);
        let forecast = cache.get("cpu", &tags).unwrap();
        assert_eq!(forecast.result.values.len(), 5);
        assert_eq!(forecast.horizon, 5);
    }

    #[test]
    fn stale_not_returned_by_get() {
        let cache = ForecastCache::new(Duration::from_millis(1));
        let tags = make_tags("host1");
        cache.store("cpu", &tags, make_result(5), 5);

        // Wait for staleness
        std::thread::sleep(Duration::from_millis(5));
        assert!(cache.get("cpu", &tags).is_none());
    }

    #[test]
    fn get_any_ignores_freshness() {
        let cache = ForecastCache::new(Duration::from_millis(1));
        let tags = make_tags("host1");
        cache.store("cpu", &tags, make_result(5), 5);

        std::thread::sleep(Duration::from_millis(5));
        assert!(cache.get_any("cpu", &tags).is_some());
    }

    #[test]
    fn evict_stale() {
        let cache = ForecastCache::new(Duration::from_millis(1));
        let tags = make_tags("host1");
        cache.store("cpu", &tags, make_result(5), 5);

        std::thread::sleep(Duration::from_millis(5));
        let removed = cache.evict_stale();
        assert_eq!(removed, 1);
        assert!(cache.is_empty());
    }

    #[test]
    fn invalidate() {
        let cache = ForecastCache::new(Duration::from_secs(3600));
        let tags = make_tags("host1");
        cache.store("cpu", &tags, make_result(5), 5);
        assert!(!cache.is_empty());

        cache.invalidate("cpu", &tags);
        assert!(cache.is_empty());
    }

    #[test]
    fn multiple_series() {
        let cache = ForecastCache::new(Duration::from_secs(3600));
        cache.store("cpu", &make_tags("a"), make_result(5), 5);
        cache.store("cpu", &make_tags("b"), make_result(10), 10);
        cache.store("mem", &make_tags("a"), make_result(3), 3);

        assert_eq!(cache.len(), 3);
        let f = cache.get("cpu", &make_tags("b")).unwrap();
        assert_eq!(f.horizon, 10);
    }

    #[test]
    fn clear() {
        let cache = ForecastCache::new(Duration::from_secs(3600));
        cache.store("cpu", &make_tags("a"), make_result(5), 5);
        cache.store("cpu", &make_tags("b"), make_result(5), 5);
        cache.clear();
        assert!(cache.is_empty());
    }
}

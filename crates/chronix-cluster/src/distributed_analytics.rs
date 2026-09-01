//! Distributed analytics — run FORECAST/ANOMALY across a cluster.
//!
//! The [`DistributedAnalytics`] coordinator fetches historical data from
//! `DataNode`s via the [`QueryRouter`] and runs forecast or anomaly
//! detection models locally on the `QueryNode`. This avoids shipping
//! models to DataNodes and keeps the analytics engine close to the
//! consumer.
//!
//! ## Design
//!
//! 1. Client issues FORECAST/ANOMALY query.
//! 2. [`DistributedAnalytics`] constructs a [`DistributedQuery`] to
//!    fetch the required historical training window.
//! 3. [`QueryRouter`] scatters the sub-queries to DataNodes, gathers
//!    the merged result.
//! 4. The analytics engine fits the model and produces predictions or
//!    anomaly scores.
//! 5. Results are returned to the client.

use std::sync::Arc;

use tracing::{debug, instrument};

use chronix_analytics::anomaly::{AnomalyDetector, AnomalyScore};
use chronix_analytics::forecast::{ForecastModel, ForecastResult};

use crate::error::{ClusterError, Result};
use crate::query_router::{DistributedQuery, QueryRouter, ReadConsistency};

// ── Request types ──────────────────────────────────────────────────────

/// Request to run a distributed forecast query.
#[derive(Debug, Clone)]
pub struct DistributedForecastRequest {
    /// Measurement to forecast.
    pub measurement: String,
    /// Field name whose values are used for the time series (e.g. "value").
    pub field: String,
    /// Tag filters to restrict the series.
    pub tag_filters: Vec<(String, String)>,
    /// Start of training window (nanoseconds, inclusive).
    pub training_start_ns: i64,
    /// End of training window (nanoseconds, exclusive).
    pub training_end_ns: i64,
    /// Number of future points to predict.
    pub horizon: usize,
}

/// Request to run distributed anomaly detection.
#[derive(Debug, Clone)]
pub struct DistributedAnomalyRequest {
    /// Measurement to analyze.
    pub measurement: String,
    /// Field name whose values are used for the time series.
    pub field: String,
    /// Tag filters to restrict the series.
    pub tag_filters: Vec<(String, String)>,
    /// Start of detection window (nanoseconds, inclusive).
    pub detection_start_ns: i64,
    /// End of detection window (nanoseconds, exclusive).
    pub detection_end_ns: i64,
}

// ── Result types ───────────────────────────────────────────────────────

/// Result of a distributed forecast.
#[derive(Debug, Clone)]
pub struct DistributedForecastResult {
    /// The forecast predictions.
    pub forecast: ForecastResult,
    /// Number of training points used.
    pub training_points: usize,
    /// Number of cluster regions queried for training data.
    pub regions_queried: usize,
}

/// Result of distributed anomaly detection.
#[derive(Debug, Clone)]
pub struct DistributedAnomalyResult {
    /// Per-point anomaly scores.
    pub scores: Vec<AnomalyScore>,
    /// Number of anomalies detected.
    pub anomaly_count: usize,
    /// Number of cluster regions queried.
    pub regions_queried: usize,
}

// ── Coordinator ────────────────────────────────────────────────────────

/// Distributed analytics coordinator.
///
/// Fetches data from `DataNode`s via the [`QueryRouter`] and runs
/// forecast/anomaly models locally on the `QueryNode`.
pub struct DistributedAnalytics {
    /// Query router for distributed data fetching.
    query_router: Arc<QueryRouter>,
}

impl DistributedAnalytics {
    /// Create a new coordinator backed by the given query router.
    #[must_use]
    pub fn new(query_router: Arc<QueryRouter>) -> Self {
        Self { query_router }
    }

    /// Run a FORECAST query across the cluster.
    ///
    /// Fetches training data via scatter-gather, fits the model locally,
    /// and returns predictions.
    ///
    /// # Errors
    ///
    /// Returns an error if data fetching fails or the model fails to
    /// fit / predict.
    #[instrument(skip(self, model), fields(measurement = %req.measurement, horizon = req.horizon))]
    pub async fn forecast(
        &self,
        req: &DistributedForecastRequest,
        model: &mut dyn ForecastModel,
    ) -> Result<DistributedForecastResult> {
        debug!("fetching training data for forecast");

        let dq = DistributedQuery {
            measurement: req.measurement.clone(),
            start_ns: req.training_start_ns,
            end_ns: req.training_end_ns,
            tag_filters: req.tag_filters.clone(),
            field_columns: vec![req.field.clone()],
            limit: 0,
            consistency: ReadConsistency::Follower,
        };

        let query_result = self.query_router.query(&dq).await?;

        let (timestamps, values) = Self::extract_series(&query_result.points, &req.field);
        let training_points = timestamps.len();

        if training_points == 0 {
            return Err(ClusterError::Analytics(
                "no training data found for forecast".into(),
            ));
        }

        debug!(training_points, "fitting forecast model");

        model
            .fit(&timestamps, &values)
            .map_err(|e| ClusterError::Analytics(format!("forecast fit failed: {e}")))?;

        let forecast = model
            .predict(req.horizon)
            .map_err(|e| ClusterError::Analytics(format!("forecast predict failed: {e}")))?;

        Ok(DistributedForecastResult {
            forecast,
            training_points,
            regions_queried: query_result.regions_queried,
        })
    }

    /// Run an ANOMALY query across the cluster.
    ///
    /// Fetches data via scatter-gather, runs anomaly detection locally,
    /// and returns scored points.
    ///
    /// # Errors
    ///
    /// Returns an error if data fetching fails or the detector fails.
    #[instrument(skip(self, detector), fields(measurement = %req.measurement))]
    pub async fn detect_anomalies(
        &self,
        req: &DistributedAnomalyRequest,
        detector: &mut dyn AnomalyDetector,
    ) -> Result<DistributedAnomalyResult> {
        debug!("fetching data for anomaly detection");

        let dq = DistributedQuery {
            measurement: req.measurement.clone(),
            start_ns: req.detection_start_ns,
            end_ns: req.detection_end_ns,
            tag_filters: req.tag_filters.clone(),
            field_columns: vec![req.field.clone()],
            limit: 0,
            consistency: ReadConsistency::Follower,
        };

        let query_result = self.query_router.query(&dq).await?;

        let (timestamps, values) = Self::extract_series(&query_result.points, &req.field);

        if timestamps.is_empty() {
            return Err(ClusterError::Analytics(
                "no data found for anomaly detection".into(),
            ));
        }

        debug!(data_points = timestamps.len(), "fitting anomaly detector");

        // Fit the detector on the data, then detect anomalies.
        detector
            .fit(&timestamps, &values)
            .map_err(|e| ClusterError::Analytics(format!("anomaly fit failed: {e}")))?;

        let scores = detector
            .detect(&timestamps, &values)
            .map_err(|e| ClusterError::Analytics(format!("anomaly detect failed: {e}")))?;

        let anomaly_count = scores.iter().filter(|s| s.is_anomaly).count();

        Ok(DistributedAnomalyResult {
            scores,
            anomaly_count,
            regions_queried: query_result.regions_queried,
        })
    }

    /// Extract timestamp/value arrays from merged points for a specific
    /// field. Points must already be sorted by timestamp.
    fn extract_series(points: &[chronix_core::Point], field: &str) -> (Vec<i64>, Vec<f64>) {
        let mut timestamps = Vec::with_capacity(points.len());
        let mut values = Vec::with_capacity(points.len());

        for point in points {
            let value = match point.field(field) {
                Some(chronix_core::FieldValue::F64(v)) => Some(*v),
                Some(chronix_core::FieldValue::I64(v)) => Some(*v as f64),
                Some(chronix_core::FieldValue::U64(v)) => Some(*v as f64),
                _ => None,
            };
            if let Some(v) = value {
                timestamps.push(point.timestamp());
                values.push(v);
            }
        }

        (timestamps, values)
    }
}

impl std::fmt::Debug for DistributedAnalytics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedAnalytics")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::MetaClient;
    use crate::data_client::DataGrpcClient;
    use crate::data_service::{RegionQuery, RegionStorage};
    use crate::routing_cache::RoutingCache;
    use async_trait::async_trait;
    use chronix_core::{FieldValue, Point, SeriesKey};
    use chronix_meta::{RouteEntry, RoutingSnapshot};
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    // ── Mock storage ───────────────────────────────────────────────

    #[derive(Debug)]
    struct MockStorage {
        points: Mutex<BTreeMap<u64, Vec<Point>>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                points: Mutex::new(BTreeMap::new()),
            }
        }

        fn seed_region(&self, region_id: u64, pts: Vec<Point>) {
            self.points.lock().insert(region_id, pts);
        }
    }

    #[async_trait]
    impl RegionStorage for MockStorage {
        async fn write_points(
            &self,
            _region_id: u64,
            _points: Vec<Point>,
        ) -> crate::error::Result<u64> {
            Ok(0)
        }

        async fn query_region(
            &self,
            region_id: u64,
            query: RegionQuery,
        ) -> crate::error::Result<Vec<Point>> {
            let binding = self.points.lock();
            let pts = binding.get(&region_id).cloned().unwrap_or_default();
            let filtered: Vec<Point> = pts
                .into_iter()
                .filter(|p| p.timestamp() >= query.start_ns && p.timestamp() < query.end_ns)
                .collect();
            Ok(filtered)
        }

        async fn replicate_wal(
            &self,
            _region_id: u64,
            _entries: Vec<(u64, Vec<u8>)>,
        ) -> crate::error::Result<u64> {
            Ok(0)
        }

        async fn snapshot_data(&self, _region_id: u64) -> crate::error::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore_snapshot(
            &self,
            _region_id: u64,
            _data: &[u8],
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }

    use crate::test_util::MockSnapshotMetaClient;

    // ── Helpers ────────────────────────────────────────────────────

    fn make_ts_point(measurement: &str, ts: i64, value: f64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), "srv1".to_string())]
            .into_iter()
            .collect();
        let sk = SeriesKey::new(measurement, tags).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(value));
        Point::new(sk, fields, ts).unwrap()
    }

    fn make_analytics(measurement: &str, points: Vec<Point>) -> DistributedAnalytics {
        let snapshot = RoutingSnapshot {
            version: 1,
            entries: [(
                measurement.to_string(),
                vec![RouteEntry {
                    region_id: 1,
                    measurement: measurement.to_string(),
                    leader_node_id: 1,
                    leader_addr: "http://n1:5000".into(),
                    replica_addrs: vec![(1, "http://n1:5000".into())],
                    key_range: None,
                    region_state: chronix_meta::RegionState::Active,
                }],
            )]
            .into_iter()
            .collect(),
        };

        let meta_client: Arc<dyn MetaClient> = Arc::new(MockSnapshotMetaClient::new(snapshot));
        let cache = Arc::new(RoutingCache::new(meta_client));
        let storage = Arc::new(MockStorage::new());
        storage.seed_region(1, points);
        let data_client = DataGrpcClient::new();
        let query_router = Arc::new(QueryRouter::new(cache, storage, data_client, 1));
        DistributedAnalytics::new(query_router)
    }

    // ── Tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn forecast_with_linear_regression() {
        // Linear data: y = 2*x + 1
        let points: Vec<Point> = (0..20)
            .map(|i: i64| {
                let ts = (i + 1) * 1_000_000_000; // 1s intervals in ns
                let value = 2.0 * (i as f64) + 1.0;
                make_ts_point("cpu", ts, value)
            })
            .collect();

        let analytics = make_analytics("cpu", points);

        let req = DistributedForecastRequest {
            measurement: "cpu".into(),
            field: "value".into(),
            tag_filters: vec![],
            training_start_ns: 0,
            training_end_ns: i64::MAX,
            horizon: 5,
        };

        let mut model = chronix_analytics::forecast::LinearRegressionModel::new();
        let result = analytics.forecast(&req, &mut model).await.unwrap();

        assert_eq!(result.training_points, 20);
        assert_eq!(result.forecast.values.len(), 5);
        assert_eq!(result.regions_queried, 1);
    }

    #[tokio::test]
    async fn forecast_no_data_returns_error() {
        let analytics = make_analytics("cpu", vec![]);

        let req = DistributedForecastRequest {
            measurement: "cpu".into(),
            field: "value".into(),
            tag_filters: vec![],
            training_start_ns: 0,
            training_end_ns: i64::MAX,
            horizon: 5,
        };

        let mut model = chronix_analytics::forecast::LinearRegressionModel::new();
        let result = analytics.forecast(&req, &mut model).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn detect_anomalies_z_score() {
        // Normal data with one outlier
        let mut points: Vec<Point> = (0..50)
            .map(|i| {
                let ts = i64::from(i) * 1_000_000_000;
                make_ts_point("cpu", ts, 10.0 + f64::from(i % 3))
            })
            .collect();

        // Inject an outlier
        points.push(make_ts_point("cpu", 50_000_000_000, 1000.0));

        let analytics = make_analytics("cpu", points);

        let req = DistributedAnomalyRequest {
            measurement: "cpu".into(),
            field: "value".into(),
            tag_filters: vec![],
            detection_start_ns: 0,
            detection_end_ns: i64::MAX,
        };

        let mut detector = chronix_analytics::anomaly::ZScoreDetector::new(Some(3.0));
        let result = analytics
            .detect_anomalies(&req, &mut detector)
            .await
            .unwrap();

        assert!(!result.scores.is_empty());
        assert!(result.anomaly_count >= 1);
        assert_eq!(result.regions_queried, 1);
    }

    #[tokio::test]
    async fn detect_anomalies_no_data_returns_error() {
        let analytics = make_analytics("cpu", vec![]);

        let req = DistributedAnomalyRequest {
            measurement: "cpu".into(),
            field: "value".into(),
            tag_filters: vec![],
            detection_start_ns: 0,
            detection_end_ns: i64::MAX,
        };

        let mut detector = chronix_analytics::anomaly::ZScoreDetector::new(Some(3.0));
        let result = analytics.detect_anomalies(&req, &mut detector).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn forecast_with_ses_model() {
        // Stationary data
        let points: Vec<Point> = (0..30)
            .map(|i| {
                let ts = i64::from(i) * 1_000_000_000;
                make_ts_point("mem", ts, 50.0 + f64::from(i % 5))
            })
            .collect();

        let analytics = make_analytics("mem", points);

        let req = DistributedForecastRequest {
            measurement: "mem".into(),
            field: "value".into(),
            tag_filters: vec![],
            training_start_ns: 0,
            training_end_ns: i64::MAX,
            horizon: 3,
        };

        let mut model = chronix_analytics::forecast::SesModel::new(Some(0.3));
        let result = analytics.forecast(&req, &mut model).await.unwrap();

        assert_eq!(result.training_points, 30);
        assert_eq!(result.forecast.values.len(), 3);
    }

    #[tokio::test]
    async fn detect_anomalies_iqr() {
        let mut points: Vec<Point> = (0..40)
            .map(|i| {
                let ts = i64::from(i) * 1_000_000_000;
                make_ts_point("disk", ts, 80.0 + f64::from(i % 4))
            })
            .collect();

        // Outlier
        points.push(make_ts_point("disk", 40_000_000_000, 999.0));

        let analytics = make_analytics("disk", points);

        let req = DistributedAnomalyRequest {
            measurement: "disk".into(),
            field: "value".into(),
            tag_filters: vec![],
            detection_start_ns: 0,
            detection_end_ns: i64::MAX,
        };

        let mut detector = chronix_analytics::anomaly::IqrDetector::new(Some(1.5));
        let result = analytics
            .detect_anomalies(&req, &mut detector)
            .await
            .unwrap();

        assert!(result.anomaly_count >= 1);
    }

    #[test]
    fn extract_series_filters_field() {
        let points = vec![
            make_ts_point("cpu", 100, 1.0),
            make_ts_point("cpu", 200, 2.0),
            make_ts_point("cpu", 300, 3.0),
        ];

        let (ts, vals) = DistributedAnalytics::extract_series(&points, "value");
        assert_eq!(ts, vec![100, 200, 300]);
        assert_eq!(vals, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn extract_series_missing_field_returns_empty() {
        let points = vec![make_ts_point("cpu", 100, 1.0)];

        let (ts, vals) = DistributedAnalytics::extract_series(&points, "nonexistent");
        assert!(ts.is_empty());
        assert!(vals.is_empty());
    }

    #[test]
    fn distributed_analytics_debug() {
        let analytics = make_analytics("cpu", vec![]);
        let debug = format!("{analytics:?}");
        assert!(debug.contains("DistributedAnalytics"));
    }

    #[test]
    fn distributed_forecast_request_debug() {
        let req = DistributedForecastRequest {
            measurement: "cpu".into(),
            field: "value".into(),
            tag_filters: vec![],
            training_start_ns: 0,
            training_end_ns: 100,
            horizon: 5,
        };
        let debug = format!("{req:?}");
        assert!(debug.contains("DistributedForecastRequest"));
    }

    #[test]
    fn distributed_anomaly_request_debug() {
        let req = DistributedAnomalyRequest {
            measurement: "cpu".into(),
            field: "value".into(),
            tag_filters: vec![],
            detection_start_ns: 0,
            detection_end_ns: 100,
        };
        let debug = format!("{req:?}");
        assert!(debug.contains("DistributedAnomalyRequest"));
    }
}

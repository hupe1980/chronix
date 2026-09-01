//! Concrete [`RegionStorage`] implementation backed by the Chronix embedded
//! storage engine.
//!
//! Bridges the [`chronix_cluster::RegionStorage`] trait with an
//! [`Arc<Chronix>`] database instance, executing writes and queries against
//! the local embedded engine on behalf of the `DataGrpcServer`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use arrow::array::Array;
use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;

use chronix::Chronix;
use chronix_cluster::data_service::RegionQuery;
use chronix_cluster::{RegionManager, RegionStorage};
use chronix_core::{ColumnRole, FieldValue, Point, SeriesKey};
use chronix_meta::RegionId;

/// [`RegionStorage`] adapter that delegates to the Chronix embedded engine.
///
/// Each `DataNode` has one `Chronix` instance; all local regions share it.
/// The `RegionManager` verifies region ownership before reaching this layer,
/// so the storage adapter trusts that the region is valid.
#[derive(Debug)]
pub struct ChronixRegionStorage {
    db: Arc<Chronix>,
    region_manager: Arc<RegionManager>,
}

impl ChronixRegionStorage {
    /// Create a new `ChronixRegionStorage`.
    pub fn new(db: Arc<Chronix>, region_manager: Arc<RegionManager>) -> Self {
        Self { db, region_manager }
    }
}

#[async_trait]
impl RegionStorage for ChronixRegionStorage {
    async fn write_points(
        &self,
        region_id: RegionId,
        points: Vec<Point>,
    ) -> chronix_cluster::Result<u64> {
        let region = self
            .region_manager
            .get_region(region_id)
            .ok_or_else(|| chronix_cluster::ClusterError::RegionNotFound(region_id))?;

        debug!(
            region_id,
            measurement = %region.measurement,
            count = points.len(),
            "writing points to local storage"
        );

        let count = points.len() as u64;
        let db = self.db.clone();

        // insert_batch is synchronous — run on a blocking thread
        tokio::task::spawn_blocking(move || {
            db.insert_batch(&points)
                .map_err(|e| {
                    chronix_cluster::ClusterError::Internal(format!("storage write failed: {e}"))
                })
                .map(|_| ())
        })
        .await
        .map_err(|e| chronix_cluster::ClusterError::Internal(format!("spawn_blocking: {e}")))??;

        Ok(count)
    }

    async fn query_region(
        &self,
        region_id: RegionId,
        query: RegionQuery,
    ) -> chronix_cluster::Result<Vec<Point>> {
        let _region = self
            .region_manager
            .get_region(region_id)
            .ok_or_else(|| chronix_cluster::ClusterError::RegionNotFound(region_id))?;

        debug!(
            region_id,
            measurement = %query.measurement,
            "querying local storage"
        );

        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            // Build query plan via QueryBuilder
            let mut builder = db.query().measurement(&query.measurement);

            // Apply time range
            if query.start_ns != i64::MIN || query.end_ns != i64::MAX {
                builder = builder.range(query.start_ns, query.end_ns);
            }

            // Apply tag filters
            for (key, value) in &query.tag_filters {
                builder = builder.tag(key, value);
            }

            // Apply field projections
            for field in &query.field_columns {
                builder = builder.field(field);
            }

            let plan = builder.build().map_err(|e| {
                chronix_cluster::ClusterError::Internal(format!("query plan build failed: {e}"))
            })?;

            let batch: RecordBatch = db.execute(&plan).map_err(|e| {
                chronix_cluster::ClusterError::Internal(format!("query execution failed: {e}"))
            })?;

            // Convert RecordBatch → Vec<Point>
            let points = batch_to_points(&batch, &query.measurement, &db)?;

            // Apply client-side limit
            if query.limit > 0 {
                let limit = query.limit as usize;
                Ok(points.into_iter().take(limit).collect())
            } else {
                Ok(points)
            }
        })
        .await
        .map_err(|e| chronix_cluster::ClusterError::Internal(format!("spawn_blocking: {e}")))?
    }

    async fn replicate_wal(
        &self,
        region_id: RegionId,
        entries: Vec<(u64, Vec<u8>)>,
    ) -> chronix_cluster::Result<u64> {
        let _region = self
            .region_manager
            .get_region(region_id)
            .ok_or_else(|| chronix_cluster::ClusterError::RegionNotFound(region_id))?;

        // Deserialize each entry as a batch of points and insert locally
        let count = entries.len() as u64;
        for (_seq, data) in &entries {
            let points: Vec<Point> = postcard::from_bytes(data).map_err(|e| {
                chronix_cluster::ClusterError::ReplicationFailed(format!(
                    "wal entry deserialization: {e}"
                ))
            })?;

            if !points.is_empty() {
                let db = self.db.clone();
                let owned_points = points;
                tokio::task::spawn_blocking(move || {
                    db.insert_batch(&owned_points)
                        .map_err(|e| {
                            chronix_cluster::ClusterError::Internal(format!(
                                "replicate write failed: {e}"
                            ))
                        })
                        .map(|_| ())
                })
                .await
                .map_err(|e| {
                    chronix_cluster::ClusterError::Internal(format!("spawn_blocking: {e}"))
                })??;
            }
        }

        debug!(region_id, count, "replicated WAL entries");
        Ok(count)
    }

    async fn snapshot_data(&self, region_id: RegionId) -> chronix_cluster::Result<Vec<u8>> {
        let region = self
            .region_manager
            .get_region(region_id)
            .ok_or_else(|| chronix_cluster::ClusterError::RegionNotFound(region_id))?;

        let db = self.db.clone();
        let measurement = region.measurement.clone();

        tokio::task::spawn_blocking(move || {
            // Query all points for this region's measurement.
            let plan = db.query().measurement(&measurement).build().map_err(|e| {
                chronix_cluster::ClusterError::Internal(format!("snapshot query build: {e}"))
            })?;

            let batch = db.execute(&plan).map_err(|e| {
                chronix_cluster::ClusterError::Internal(format!("snapshot query exec: {e}"))
            })?;

            let points = batch_to_points(&batch, &measurement, &db)?;

            // Serialize all points as postcard for efficient cross-node transfer.
            postcard::to_stdvec(&points).map_err(|e| {
                chronix_cluster::ClusterError::Internal(format!("snapshot serialization: {e}"))
            })
        })
        .await
        .map_err(|e| chronix_cluster::ClusterError::Internal(format!("spawn_blocking: {e}")))?
    }

    async fn restore_snapshot(
        &self,
        region_id: RegionId,
        data: &[u8],
    ) -> chronix_cluster::Result<()> {
        let _region = self
            .region_manager
            .get_region(region_id)
            .ok_or_else(|| chronix_cluster::ClusterError::RegionNotFound(region_id))?;

        let points: Vec<Point> = postcard::from_bytes(data).map_err(|e| {
            chronix_cluster::ClusterError::Internal(format!("snapshot deserialization: {e}"))
        })?;

        if points.is_empty() {
            return Ok(());
        }

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.insert_batch(&points)
                .map_err(|e| {
                    chronix_cluster::ClusterError::Internal(format!("snapshot restore write: {e}"))
                })
                .map(|_| ())
        })
        .await
        .map_err(|e| chronix_cluster::ClusterError::Internal(format!("spawn_blocking: {e}")))?
    }
}

/// Convert an Arrow [`RecordBatch`] back to [`Point`] objects.
///
/// Uses the measurement schema from the database to distinguish tag columns
/// from field columns.
fn batch_to_points(
    batch: &RecordBatch,
    measurement: &str,
    db: &Chronix,
) -> chronix_cluster::Result<Vec<Point>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    // Determine which columns are tags from the schema
    let tag_columns: Vec<String> = if let Some(schema) = db.schema(measurement) {
        schema
            .columns()
            .iter()
            .filter(|c| c.role == ColumnRole::Tag)
            .map(|c| c.name.clone())
            .collect()
    } else {
        Vec::new()
    };
    let tag_col_refs: Vec<&str> = tag_columns.iter().map(String::as_str).collect();

    let arrow_schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut points = Vec::with_capacity(num_rows);

    // Find timestamp column
    let ts_idx = arrow_schema
        .index_of("time")
        .or_else(|_| arrow_schema.index_of("timestamp"))
        .map_err(|_| {
            chronix_cluster::ClusterError::Internal("no timestamp column in result".into())
        })?;

    let ts_arr = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            chronix_cluster::ClusterError::Internal("timestamp column is not Int64".into())
        })?;

    for row in 0..num_rows {
        let timestamp = ts_arr.value(row);

        // Extract tags
        let mut tags = BTreeMap::new();
        for &tag_name in &tag_col_refs {
            if let Ok(col_idx) = arrow_schema.index_of(tag_name) {
                if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
                    if !arr.is_null(row) {
                        tags.insert(tag_name.to_string(), arr.value(row).to_string());
                    }
                }
            }
        }

        // Extract fields (all non-timestamp, non-tag columns)
        let mut fields = BTreeMap::new();
        for (col_idx, field_ref) in arrow_schema.fields().iter().enumerate() {
            let name = field_ref.name().as_str();
            if name == "time" || name == "timestamp" || tag_col_refs.contains(&name) {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            let val = match field_ref.data_type() {
                arrow::datatypes::DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| FieldValue::F64(a.value(row))),
                arrow::datatypes::DataType::Int64 => col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| FieldValue::I64(a.value(row))),
                arrow::datatypes::DataType::UInt64 => col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .map(|a| FieldValue::U64(a.value(row))),
                arrow::datatypes::DataType::Boolean => col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(|a| FieldValue::Bool(a.value(row))),
                arrow::datatypes::DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| FieldValue::String(a.value(row).to_string())),
                _ => None,
            };
            if let Some(v) = val {
                fields.insert(name.to_string(), v);
            }
        }

        if fields.is_empty() {
            continue; // skip rows with no field data
        }

        let key = SeriesKey::new(measurement, tags).map_err(|e| {
            chronix_cluster::ClusterError::Internal(format!("series key error: {e}"))
        })?;
        let point = Point::new(key, fields, timestamp)
            .map_err(|e| chronix_cluster::ClusterError::Internal(format!("point error: {e}")))?;
        points.push(point);
    }

    Ok(points)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::ChronixConfig;
    use tempfile::TempDir;

    fn open_test_db() -> (TempDir, Arc<Chronix>) {
        let dir = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();
        (dir, Arc::new(db))
    }

    fn make_point(measurement: &str, tag_val: &str, value: f64, ts: i64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), tag_val.to_string())]
            .into_iter()
            .collect();
        let key = SeriesKey::new(measurement, tags).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("usage".to_string(), FieldValue::F64(value));
        Point::new(key, fields, ts).unwrap()
    }

    #[tokio::test]
    async fn write_and_query_roundtrip() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        region_mgr.create_region(10, "cpu").unwrap();

        let storage = ChronixRegionStorage::new(db, region_mgr);

        // Write
        let points = vec![
            make_point("cpu", "srv1", 42.0, 1_000_000),
            make_point("cpu", "srv2", 99.0, 2_000_000),
        ];
        let written = storage.write_points(10, points).await.unwrap();
        assert_eq!(written, 2);

        // Query
        let query = RegionQuery {
            measurement: "cpu".to_string(),
            start_ns: 0,
            end_ns: i64::MAX,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
        };
        let results = storage.query_region(10, query).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn write_to_unknown_region_fails() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        let storage = ChronixRegionStorage::new(db, region_mgr);

        let result = storage
            .write_points(999, vec![make_point("cpu", "srv1", 1.0, 1)])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_unknown_region_fails() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        let storage = ChronixRegionStorage::new(db, region_mgr);

        let query = RegionQuery {
            measurement: "cpu".to_string(),
            start_ns: 0,
            end_ns: i64::MAX,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
        };
        let result = storage.query_region(999, query).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn query_with_tag_filter() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        region_mgr.create_region(10, "cpu").unwrap();

        let storage = ChronixRegionStorage::new(db, region_mgr);

        let points = vec![
            make_point("cpu", "srv1", 42.0, 1_000_000),
            make_point("cpu", "srv2", 99.0, 2_000_000),
        ];
        storage.write_points(10, points).await.unwrap();

        let query = RegionQuery {
            measurement: "cpu".to_string(),
            start_ns: 0,
            end_ns: i64::MAX,
            tag_filters: vec![("host".to_string(), "srv1".to_string())],
            field_columns: vec![],
            limit: 0,
        };
        let results = storage.query_region(10, query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].series_key().tag("host"), Some("srv1"));
    }

    #[tokio::test]
    async fn query_with_limit() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        region_mgr.create_region(10, "cpu").unwrap();

        let storage = ChronixRegionStorage::new(db, region_mgr);

        let points: Vec<Point> = (0..10)
            .map(|i| make_point("cpu", &format!("srv{i}"), i as f64, i * 1_000_000))
            .collect();
        storage.write_points(10, points).await.unwrap();

        let query = RegionQuery {
            measurement: "cpu".to_string(),
            start_ns: 0,
            end_ns: i64::MAX,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 3,
        };
        let results = storage.query_region(10, query).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn replicate_wal_entries() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        region_mgr.create_region(10, "cpu").unwrap();

        let storage = ChronixRegionStorage::new(db.clone(), region_mgr);

        // Serialize some points as a WAL entry
        let points = vec![make_point("cpu", "srv1", 42.0, 1_000_000)];
        let data = postcard::to_stdvec(&points).unwrap();

        let applied = storage.replicate_wal(10, vec![(1, data)]).await.unwrap();
        assert_eq!(applied, 1);

        // Verify the data was actually written
        let query = RegionQuery {
            measurement: "cpu".to_string(),
            start_ns: 0,
            end_ns: i64::MAX,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
        };
        let results = storage.query_region(10, query).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn debug_format() {
        let (_dir, db) = open_test_db();
        let region_mgr = Arc::new(RegionManager::new(1));
        let storage = ChronixRegionStorage::new(db, region_mgr);
        let debug = format!("{storage:?}");
        assert!(debug.contains("ChronixRegionStorage"));
    }
}

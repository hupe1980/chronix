//! DataNode gRPC service — region-level write, query, and replication.
//!
//! The [`DataGrpcServer`] serves the `DataService` gRPC interface
//! RPCs, delegating storage operations to an [`RegionStorage`] implementation.
//! This decouples the gRPC transport from the underlying storage engine so
//! that the same proto contract works against both a real Chronix instance
//! (in `chronixd`) and lightweight test doubles.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tracing::debug;

use chronix_core::{FieldValue, Point, SeriesKey, Timestamp};
use chronix_meta::RegionId;

use crate::error::{ClusterError, Result};
use crate::region::RegionManager;

// ── Generated proto types ──────────────────────────────────────────────────

/// Generated protobuf types for the data service.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod proto {
    tonic::include_proto!("chronix.data.v1");
}

// ── Public query type ──────────────────────────────────────────────────────

/// Parameters for querying a region's local storage.
#[derive(Debug, Clone, Default)]
pub struct RegionQuery {
    /// Measurement name to query.
    pub measurement: String,
    /// Start of the time range (inclusive, nanoseconds).
    pub start_ns: Timestamp,
    /// End of the time range (exclusive, nanoseconds).
    pub end_ns: Timestamp,
    /// Tag equality filters — all must match.
    pub tag_filters: Vec<(String, String)>,
    /// Specific field columns to return. Empty means all fields.
    pub field_columns: Vec<String>,
    /// Maximum points to return. `0` means no limit.
    pub limit: u64,
}

impl RegionQuery {
    /// Create a new query for a measurement within a time range.
    ///
    /// Defaults: no tag filters, all fields, no limit.
    #[must_use]
    pub fn new(measurement: impl Into<String>, start_ns: Timestamp, end_ns: Timestamp) -> Self {
        Self {
            measurement: measurement.into(),
            start_ns,
            end_ns,
            tag_filters: Vec::new(),
            field_columns: Vec::new(),
            limit: 0,
        }
    }

    /// Add a tag equality filter.
    #[must_use]
    pub fn with_tag_filter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_filters.push((key.into(), value.into()));
        self
    }

    /// Restrict output to specific field columns.
    #[must_use]
    pub fn with_field_columns(mut self, columns: Vec<String>) -> Self {
        self.field_columns = columns;
        self
    }

    /// Set the maximum number of points to return.
    #[must_use]
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = limit;
        self
    }
}

// ── RegionStorage trait ────────────────────────────────────────────────────

/// Abstraction over local region storage.
///
/// Implemented by `chronixd` using the Chronix embedded engine, and by
/// test doubles for isolated unit tests.
#[async_trait]
pub trait RegionStorage: Send + Sync + std::fmt::Debug {
    /// Write a batch of points to a region's local storage.
    ///
    /// Returns the number of points successfully written.
    async fn write_points(&self, region_id: RegionId, points: Vec<Point>) -> Result<u64>;

    /// Query a region's local storage.
    async fn query_region(&self, region_id: RegionId, query: RegionQuery) -> Result<Vec<Point>>;

    /// Replay replicated WAL entries from the region leader.
    ///
    /// The `entries` are opaque serialized WAL records; the storage layer
    /// deserializes and applies them in sequence order.
    async fn replicate_wal(&self, region_id: RegionId, entries: Vec<(u64, Vec<u8>)>)
        -> Result<u64>;

    /// Capture a serializable snapshot of the region's data.
    ///
    /// The returned bytes are opaque to the Raft layer — the storage
    /// implementation decides the format (e.g., serialized points,
    /// segment metadata manifest, etc.).
    ///
    /// Called by `RegionSmStore::build_snapshot()` so followers can
    /// catch up after Raft log compaction.
    async fn snapshot_data(&self, region_id: RegionId) -> Result<Vec<u8>>;

    /// Restore a region's state from a snapshot.
    ///
    /// Replaces all existing data for the region with the snapshot
    /// contents. Called by `RegionSmStore::install_snapshot()` on
    /// followers receiving a leader's snapshot.
    async fn restore_snapshot(&self, region_id: RegionId, data: &[u8]) -> Result<()>;
}

// ── Proto ↔ Core conversion ────────────────────────────────────────────────

/// Convert a proto [`DataPoint`](proto::DataPoint) into a core [`Point`].
///
/// # Errors
///
/// Returns [`ClusterError::Validation`] if the series key or fields are invalid.
pub fn proto_to_core_point(dp: &proto::DataPoint) -> Result<Point> {
    let tags: BTreeMap<String, String> = dp.tags.clone().into_iter().collect();

    let series_key = SeriesKey::new(&dp.measurement, tags)
        .map_err(|e| ClusterError::Validation(format!("invalid series key: {e}")))?;

    let mut fields = BTreeMap::new();
    for entry in &dp.fields {
        let value = entry
            .value
            .as_ref()
            .and_then(|v| v.kind.as_ref())
            .ok_or_else(|| ClusterError::Validation("field entry missing value".into()))?;

        let fv = match value {
            proto::field_value::Kind::F64Value(v) => FieldValue::F64(*v),
            proto::field_value::Kind::I64Value(v) => FieldValue::I64(*v),
            proto::field_value::Kind::U64Value(v) => FieldValue::U64(*v),
            proto::field_value::Kind::BoolValue(v) => FieldValue::Bool(*v),
            proto::field_value::Kind::StringValue(v) => FieldValue::String(v.clone()),
            proto::field_value::Kind::DecimalValue(v) => {
                FieldValue::Decimal(v.parse().map_err(|e| {
                    ClusterError::Validation(format!("invalid decimal \"{v}\": {e}"))
                })?)
            }
        };
        fields.insert(entry.name.clone(), fv);
    }

    Point::new(series_key, fields, dp.timestamp_ns)
        .map_err(|e| ClusterError::Validation(format!("invalid point: {e}")))
}

/// Convert a core [`Point`] into a proto [`DataPoint`](proto::DataPoint).
#[must_use]
pub fn core_to_proto_point(p: &Point) -> proto::DataPoint {
    let fields = p
        .fields()
        .iter()
        .map(|(name, value)| {
            let kind = match value {
                FieldValue::F64(v) => proto::field_value::Kind::F64Value(*v),
                FieldValue::I64(v) => proto::field_value::Kind::I64Value(*v),
                FieldValue::U64(v) => proto::field_value::Kind::U64Value(*v),
                FieldValue::Bool(v) => proto::field_value::Kind::BoolValue(*v),
                FieldValue::String(v) => proto::field_value::Kind::StringValue(v.clone()),
                FieldValue::Decimal(d) => proto::field_value::Kind::DecimalValue(d.to_string()),
            };
            proto::FieldEntry {
                name: name.to_string(),
                value: Some(proto::FieldValue { kind: Some(kind) }),
            }
        })
        .collect();

    proto::DataPoint {
        measurement: p.series_key().measurement().to_string(),
        tags: p
            .series_key()
            .tags()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        fields,
        timestamp_ns: p.timestamp(),
    }
}

// ── DataGrpcServer ─────────────────────────────────────────────────────────

/// gRPC server for region-level data operations on a `DataNode`.
///
/// Validates that the target region is hosted locally (via [`RegionManager`]),
/// then delegates the actual storage work to an [`RegionStorage`] backend.
pub struct DataGrpcServer {
    region_manager: Arc<RegionManager>,
    storage: Arc<dyn RegionStorage>,
    /// Optional per-region Raft manager for linearizable read
    /// verification on remote leader queries.
    raft_manager: Option<Arc<crate::region_raft::RegionRaftManager>>,
}

impl DataGrpcServer {
    /// Create a new `DataGrpcServer`.
    pub fn new(region_manager: Arc<RegionManager>, storage: Arc<dyn RegionStorage>) -> Self {
        Self {
            region_manager,
            storage,
            raft_manager: None,
        }
    }

    /// Create a new `DataGrpcServer` with Raft manager for linearizable reads.
    pub fn with_raft_manager(
        region_manager: Arc<RegionManager>,
        storage: Arc<dyn RegionStorage>,
        raft_manager: Arc<crate::region_raft::RegionRaftManager>,
    ) -> Self {
        Self {
            region_manager,
            storage,
            raft_manager: Some(raft_manager),
        }
    }

    /// Convert into a tonic [`DataServiceServer`](proto::data_service_server::DataServiceServer).
    #[must_use]
    pub fn into_service(self) -> proto::data_service_server::DataServiceServer<Self> {
        proto::data_service_server::DataServiceServer::new(self)
    }

    /// Check that a region is hosted on this node.
    #[allow(clippy::result_large_err)]
    fn check_region(&self, region_id: RegionId) -> std::result::Result<(), Status> {
        if self.region_manager.get_region(region_id).is_none() {
            return Err(Status::not_found(format!(
                "region {region_id} not hosted on node {}",
                self.region_manager.node_id()
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl proto::data_service_server::DataService for DataGrpcServer {
    async fn write_region(
        &self,
        request: Request<proto::WriteRegionRequest>,
    ) -> std::result::Result<Response<proto::WriteRegionResponse>, Status> {
        let req = request.into_inner();
        self.check_region(req.region_id)?;

        let points: Vec<Point> = req
            .points
            .iter()
            .map(proto_to_core_point)
            .collect::<Result<Vec<_>>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        debug!(
            region_id = req.region_id,
            count = points.len(),
            "write_region"
        );

        let written = self
            .storage
            .write_points(req.region_id, points)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::WriteRegionResponse { written }))
    }

    async fn query_region(
        &self,
        request: Request<proto::QueryRegionRequest>,
    ) -> std::result::Result<Response<proto::QueryRegionResponse>, Status> {
        let req = request.into_inner();
        self.check_region(req.region_id)?;

        // Verify Raft leadership on the server side when the
        // client requests linearizable reads. This prevents stale data
        // from a deposed leader whose routing cache entry has not
        // expired yet.
        if req.require_linearizable {
            if let Some(ref rm) = self.raft_manager {
                if let Some(raft) = rm.get_raft(req.region_id) {
                    raft.ensure_linearizable().await.map_err(|e| {
                        Status::failed_precondition(format!(
                            "linearizable read failed for region {}: {e}",
                            req.region_id,
                        ))
                    })?;
                }
            }
        }

        let query = RegionQuery {
            measurement: req.measurement,
            start_ns: req.start_ns,
            end_ns: req.end_ns,
            tag_filters: req
                .tag_filters
                .into_iter()
                .map(|f| (f.key, f.value))
                .collect(),
            field_columns: req.field_columns,
            limit: req.limit,
        };

        debug!(
            region_id = req.region_id,
            measurement = %query.measurement,
            "query_region"
        );

        let points = self
            .storage
            .query_region(req.region_id, query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_points = points.iter().map(core_to_proto_point).collect();

        Ok(Response::new(proto::QueryRegionResponse {
            points: proto_points,
        }))
    }

    async fn replicate_wal(
        &self,
        request: Request<proto::ReplicateRequest>,
    ) -> std::result::Result<Response<proto::ReplicateResponse>, Status> {
        let req = request.into_inner();
        self.check_region(req.region_id)?;

        let entries: Vec<(u64, Vec<u8>)> = req
            .entries
            .into_iter()
            .map(|e| (e.sequence, e.data))
            .collect();

        debug!(
            region_id = req.region_id,
            count = entries.len(),
            "replicate_wal"
        );

        let applied = self
            .storage
            .replicate_wal(req.region_id, entries)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ReplicateResponse { applied }))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use proto::data_service_server::DataService;

    // ── Mock storage ───────────────────────────────────────────────

    type ReplicatedEntry = (RegionId, Vec<(u64, Vec<u8>)>);

    #[derive(Debug, Default)]
    struct MockStorage {
        written: Mutex<Vec<(RegionId, Vec<Point>)>>,
        query_results: Mutex<Vec<Point>>,
        replicated: Mutex<Vec<ReplicatedEntry>>,
    }

    #[async_trait]
    impl RegionStorage for MockStorage {
        async fn write_points(&self, region_id: RegionId, points: Vec<Point>) -> Result<u64> {
            let count = points.len() as u64;
            self.written.lock().push((region_id, points));
            Ok(count)
        }

        async fn query_region(
            &self,
            _region_id: RegionId,
            _query: RegionQuery,
        ) -> Result<Vec<Point>> {
            Ok(self.query_results.lock().clone())
        }

        async fn replicate_wal(
            &self,
            region_id: RegionId,
            entries: Vec<(u64, Vec<u8>)>,
        ) -> Result<u64> {
            let count = entries.len() as u64;
            self.replicated.lock().push((region_id, entries));
            Ok(count)
        }

        async fn snapshot_data(&self, _region_id: RegionId) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore_snapshot(&self, _region_id: RegionId, _data: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    fn make_server() -> (DataGrpcServer, Arc<MockStorage>) {
        let mgr = Arc::new(RegionManager::new(1));
        mgr.create_region(10, "cpu").unwrap();
        mgr.create_region(20, "mem").unwrap();

        let storage = Arc::new(MockStorage::default());
        let server = DataGrpcServer::new(mgr, storage.clone());
        (server, storage)
    }

    fn make_proto_point(measurement: &str, ts: i64) -> proto::DataPoint {
        proto::DataPoint {
            measurement: measurement.to_string(),
            tags: [("host".to_string(), "srv1".to_string())]
                .into_iter()
                .collect(),
            fields: vec![proto::FieldEntry {
                name: "usage".to_string(),
                value: Some(proto::FieldValue {
                    kind: Some(proto::field_value::Kind::F64Value(42.5)),
                }),
            }],
            timestamp_ns: ts,
        }
    }

    // ── Proto ↔ Core conversion tests ──────────────────────────────

    #[test]
    fn roundtrip_proto_core_point() {
        let proto_pt = make_proto_point("cpu", 1_000_000);
        let core_pt = proto_to_core_point(&proto_pt).unwrap();

        assert_eq!(core_pt.series_key().measurement(), "cpu");
        assert_eq!(core_pt.series_key().tag("host"), Some("srv1"));
        assert_eq!(core_pt.timestamp(), 1_000_000);

        let back = core_to_proto_point(&core_pt);
        assert_eq!(back.measurement, "cpu");
        assert_eq!(back.timestamp_ns, 1_000_000);
        assert_eq!(back.tags.get("host").unwrap(), "srv1");
        assert_eq!(back.fields.len(), 1);
    }

    #[test]
    fn proto_to_core_all_field_types() {
        let dp = proto::DataPoint {
            measurement: "test".into(),
            tags: Default::default(),
            fields: vec![
                proto::FieldEntry {
                    name: "f".into(),
                    value: Some(proto::FieldValue {
                        kind: Some(proto::field_value::Kind::F64Value(1.5)),
                    }),
                },
                proto::FieldEntry {
                    name: "i".into(),
                    value: Some(proto::FieldValue {
                        kind: Some(proto::field_value::Kind::I64Value(-7)),
                    }),
                },
                proto::FieldEntry {
                    name: "u".into(),
                    value: Some(proto::FieldValue {
                        kind: Some(proto::field_value::Kind::U64Value(99)),
                    }),
                },
                proto::FieldEntry {
                    name: "b".into(),
                    value: Some(proto::FieldValue {
                        kind: Some(proto::field_value::Kind::BoolValue(true)),
                    }),
                },
                proto::FieldEntry {
                    name: "s".into(),
                    value: Some(proto::FieldValue {
                        kind: Some(proto::field_value::Kind::StringValue("hello".into())),
                    }),
                },
            ],
            timestamp_ns: 42,
        };

        let pt = proto_to_core_point(&dp).unwrap();
        assert_eq!(pt.fields().len(), 5);
        assert_eq!(*pt.field("f").unwrap(), FieldValue::F64(1.5));
        assert_eq!(*pt.field("i").unwrap(), FieldValue::I64(-7));
        assert_eq!(*pt.field("u").unwrap(), FieldValue::U64(99));
        assert_eq!(*pt.field("b").unwrap(), FieldValue::Bool(true));
        assert_eq!(*pt.field("s").unwrap(), FieldValue::String("hello".into()));
    }

    #[test]
    fn proto_to_core_empty_fields_fails() {
        let dp = proto::DataPoint {
            measurement: "cpu".into(),
            tags: Default::default(),
            fields: vec![],
            timestamp_ns: 1,
        };
        assert!(proto_to_core_point(&dp).is_err());
    }

    #[test]
    fn proto_to_core_missing_value_fails() {
        let dp = proto::DataPoint {
            measurement: "cpu".into(),
            tags: Default::default(),
            fields: vec![proto::FieldEntry {
                name: "x".into(),
                value: None,
            }],
            timestamp_ns: 1,
        };
        assert!(proto_to_core_point(&dp).is_err());
    }

    // ── gRPC handler tests ─────────────────────────────────────────

    #[tokio::test]
    async fn write_region_success() {
        let (server, storage) = make_server();

        let req = Request::new(proto::WriteRegionRequest {
            region_id: 10,
            points: vec![make_proto_point("cpu", 1), make_proto_point("cpu", 2)],
        });

        let resp = server.write_region(req).await.unwrap();
        assert_eq!(resp.into_inner().written, 2);

        let writes = storage.written.lock();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 10);
        assert_eq!(writes[0].1.len(), 2);
    }

    #[tokio::test]
    async fn write_region_not_found() {
        let (server, _) = make_server();

        let req = Request::new(proto::WriteRegionRequest {
            region_id: 999,
            points: vec![make_proto_point("cpu", 1)],
        });

        let status = server.write_region(req).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn query_region_success() {
        let (server, storage) = make_server();

        // Seed mock with one result point
        let series_key = SeriesKey::new("cpu", BTreeMap::new()).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("usage".to_string(), FieldValue::F64(99.0));
        let pt = Point::new(series_key, fields, 100).unwrap();
        storage.query_results.lock().push(pt);

        let req = Request::new(proto::QueryRegionRequest {
            region_id: 10,
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 200,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            require_linearizable: false,
        });

        let resp = server.query_region(req).await.unwrap();
        let inner = resp.into_inner();
        assert_eq!(inner.points.len(), 1);
        assert_eq!(inner.points[0].measurement, "cpu");
        assert_eq!(inner.points[0].timestamp_ns, 100);
    }

    #[tokio::test]
    async fn query_region_not_found() {
        let (server, _) = make_server();

        let req = Request::new(proto::QueryRegionRequest {
            region_id: 999,
            measurement: "cpu".into(),
            start_ns: 0,
            end_ns: 100,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
            require_linearizable: false,
        });

        let status = server.query_region(req).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn replicate_wal_success() {
        let (server, storage) = make_server();

        let req = Request::new(proto::ReplicateRequest {
            region_id: 10,
            entries: vec![
                proto::WalReplicationEntry {
                    sequence: 1,
                    data: b"entry1".to_vec(),
                },
                proto::WalReplicationEntry {
                    sequence: 2,
                    data: b"entry2".to_vec(),
                },
            ],
        });

        let resp = server.replicate_wal(req).await.unwrap();
        assert_eq!(resp.into_inner().applied, 2);

        let replicated = storage.replicated.lock();
        assert_eq!(replicated.len(), 1);
        assert_eq!(replicated[0].0, 10);
        assert_eq!(replicated[0].1.len(), 2);
    }

    #[tokio::test]
    async fn replicate_wal_not_found() {
        let (server, _) = make_server();

        let req = Request::new(proto::ReplicateRequest {
            region_id: 999,
            entries: vec![],
        });

        let status = server.replicate_wal(req).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn check_region_validates_local() {
        let mgr = Arc::new(RegionManager::new(1));
        mgr.create_region(10, "cpu").unwrap();
        let storage: Arc<dyn RegionStorage> = Arc::new(MockStorage::default());
        let server = DataGrpcServer::new(mgr, storage);

        assert!(server.check_region(10).is_ok());
        assert!(server.check_region(999).is_err());
    }

    // ── RegionQuery builder tests ──────────────────────────────────

    #[test]
    fn region_query_new_defaults() {
        let q = RegionQuery::new("cpu", 100, 200);
        assert_eq!(q.measurement, "cpu");
        assert_eq!(q.start_ns, 100);
        assert_eq!(q.end_ns, 200);
        assert!(q.tag_filters.is_empty());
        assert!(q.field_columns.is_empty());
        assert_eq!(q.limit, 0);
    }

    #[test]
    fn region_query_builder_chain() {
        let q = RegionQuery::new("mem", 0, 1_000_000)
            .with_tag_filter("host", "h1")
            .with_tag_filter("dc", "us-east")
            .with_field_columns(vec!["used".into(), "free".into()])
            .with_limit(100);

        assert_eq!(q.tag_filters.len(), 2);
        assert_eq!(q.tag_filters[0], ("host".to_string(), "h1".to_string()));
        assert_eq!(q.field_columns, vec!["used", "free"]);
        assert_eq!(q.limit, 100);
    }
}

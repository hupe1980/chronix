#![allow(clippy::unwrap_used)] // benches may unwrap
//! Cluster-level performance benchmarks.
//!
//! Benchmarks for distributed write throughput, Raft quorum writes,
//! and series-key hashing. Uses in-process mocks to isolate cluster
//! logic from I/O.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use async_trait::async_trait;
use chronix_core::{FieldValue, Point, SeriesKey};
use chronix_meta::{NodeId, RegionId, RouteEntry, RoutingSnapshot};
use openraft::BasicNode;

use chronix_cluster::error::Result;
use chronix_cluster::region_raft::{RegionRaftManager, RegionRaftRouter};
use chronix_cluster::{
    DataGrpcClient, MetaClient, RegionQuery, RegionStorage, RoutingCache, WriteRouter,
};

// ── Mock infrastructure ────────────────────────────────────────────

#[derive(Debug, Default)]
struct BenchStorage {
    count: std::sync::atomic::AtomicU64,
}

#[async_trait]
impl RegionStorage for BenchStorage {
    async fn write_points(&self, _region_id: RegionId, points: Vec<Point>) -> Result<u64> {
        let n = points.len() as u64;
        self.count
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }

    async fn query_region(&self, _region_id: RegionId, _query: RegionQuery) -> Result<Vec<Point>> {
        Ok(Vec::new())
    }

    async fn replicate_wal(
        &self,
        _region_id: RegionId,
        _entries: Vec<(u64, Vec<u8>)>,
    ) -> Result<u64> {
        Ok(0)
    }

    async fn snapshot_data(&self, _region_id: RegionId) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    async fn restore_snapshot(&self, _region_id: RegionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }
}

struct BenchMetaClient {
    snapshot: RoutingSnapshot,
}

#[async_trait]
impl MetaClient for BenchMetaClient {
    async fn register_node(&self, _info: chronix_meta::DataNodeInfo) -> Result<()> {
        Ok(())
    }
    async fn deregister_node(&self, _node_id: NodeId) -> Result<()> {
        Ok(())
    }
    async fn heartbeat(&self, _node_id: NodeId, _gen: u64) -> Result<()> {
        Ok(())
    }
    async fn create_region(&self, _info: chronix_meta::RegionInfo) -> Result<()> {
        Ok(())
    }
    async fn get_routing_table(&self) -> Result<RoutingSnapshot> {
        Ok(self.snapshot.clone())
    }
    async fn propose(&self, _cmd: chronix_meta::MetaCommand) -> Result<chronix_meta::MetaResponse> {
        Ok(chronix_meta::MetaResponse::Ok)
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn make_points(n: usize) -> Vec<Point> {
    (0..n)
        .map(|i| {
            let tags: BTreeMap<String, String> = [
                ("host".to_string(), format!("host-{}", i % 10)),
                ("region".to_string(), "us-east".to_string()),
            ]
            .into_iter()
            .collect();
            let key = SeriesKey::new("cpu", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> = [
                (
                    "usage_idle".to_string(),
                    FieldValue::F64(42.0 + (i as f64) * 0.01),
                ),
                ("temp".to_string(), FieldValue::I64(60 + (i as i64) % 20)),
            ]
            .into_iter()
            .collect();
            let ts = 1_700_000_000_000_000_000_i64 + (i as i64) * 1_000_000_000;
            Point::new(key, fields, ts).unwrap()
        })
        .collect()
}

fn make_routing_snapshot(num_regions: usize) -> RoutingSnapshot {
    let entries: Vec<RouteEntry> = (0..num_regions)
        .map(|i| RouteEntry {
            region_id: (i + 1) as u64,
            measurement: "cpu".to_string(),
            leader_node_id: 1,
            leader_addr: "http://n1:5000".to_string(),
            replica_addrs: vec![],
            key_range: None,
            region_state: chronix_meta::RegionState::Active,
        })
        .collect();

    RoutingSnapshot {
        version: 1,
        entries: [("cpu".to_string(), entries)].into_iter().collect(),
    }
}

fn make_router(num_regions: usize) -> WriteRouter {
    let snapshot = make_routing_snapshot(num_regions);
    let meta_client: Arc<dyn MetaClient> = Arc::new(BenchMetaClient { snapshot });
    let cache = Arc::new(RoutingCache::new(meta_client));
    let storage: Arc<dyn RegionStorage> = Arc::new(BenchStorage::default());
    let data_client = DataGrpcClient::new();
    WriteRouter::new(cache, storage, data_client, 1)
}

fn new_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ── Benchmarks ─────────────────────────────────────────────────────

/// Write throughput through `WriteRouter` with multiple regions.
fn bench_write_throughput(c: &mut Criterion) {
    let rt = new_runtime();
    let mut group = c.benchmark_group("cluster_write_throughput");

    for batch_size in [100, 1_000, 10_000] {
        let points = make_points(batch_size);
        let router = make_router(3);

        group.bench_with_input(
            BenchmarkId::new("local_write", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = router.write_batch(black_box(&points)).await.unwrap();
                        black_box(result);
                    });
                });
            },
        );
    }

    group.finish();
}

/// Write throughput with a single region (no dispatch overhead).
fn bench_write_single_region(c: &mut Criterion) {
    let rt = new_runtime();
    let mut group = c.benchmark_group("cluster_write_single_region");

    for batch_size in [100, 1_000, 10_000] {
        let points = make_points(batch_size);
        let router = make_router(1);

        group.bench_with_input(
            BenchmarkId::new("write", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = router.write_batch(black_box(&points)).await.unwrap();
                        black_box(result);
                    });
                });
            },
        );
    }

    group.finish();
}

/// Raft quorum write through `RegionRaftManager` (single-node).
fn bench_raft_single_node_write(c: &mut Criterion) {
    let rt = new_runtime();
    let mut group = c.benchmark_group("cluster_raft_write");

    let region_id = 1u64;
    let node_id = 1u64;

    let raft_router = RegionRaftRouter::new();
    let raft_manager = Arc::new(RegionRaftManager::new_in_memory(raft_router, node_id));
    let raft_storage: Arc<dyn RegionStorage> = Arc::new(BenchStorage::default());
    let config = Arc::new(openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    });

    // Bootstrap single-node cluster and wait for leader election.
    rt.block_on(async {
        let r = raft_manager
            .create_raft_group(region_id, node_id, raft_storage.clone(), config)
            .await
            .unwrap();
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new("127.0.0.1:10001"));
        r.initialize(members).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    });

    for batch_size in [1, 10, 100] {
        let points = make_points(batch_size);

        group.bench_with_input(
            BenchmarkId::new("raft_propose", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let written = raft_manager
                            .propose_write(region_id, black_box(points.clone()))
                            .await
                            .unwrap();
                        black_box(written);
                    });
                });
            },
        );
    }

    group.finish();
}

/// Pure FNV hash throughput over `SeriesKey` — zero I/O.
fn bench_routing_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_routing_hash");

    for num_points in [100, 1_000, 10_000] {
        let points = make_points(num_points);

        group.bench_with_input(
            BenchmarkId::new("hash_fnv", num_points),
            &num_points,
            |b, _| {
                b.iter(|| {
                    for p in &points {
                        let hash = p.series_key().hash_fnv();
                        black_box(hash % 3);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_write_single_region,
    bench_raft_single_node_write,
    bench_routing_hash,
);
criterion_main!(benches);

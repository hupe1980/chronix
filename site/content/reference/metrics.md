+++
title = "Metrics"
description = "Every Prometheus metric chronixd exports on /metrics, with its type, labels and what it measures, plus cardinality and tracing settings."
weight = 70
+++

`chronixd` exports Prometheus metrics on `/metrics` (configurable with `metrics_path`). This page lists every metric with its type and what it measures; scrape it with Prometheus and import the bundled dashboards to see them.

## Prometheus Metrics

Chronix exposes Prometheus metrics at `/metrics` (default port 8086).

### Cluster Health

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_cluster_nodes_total` | Gauge | Total registered nodes |
| `chronix_cluster_regions_total` | Gauge | Total regions |
| `chronix_cluster_under_replicated_regions` | Gauge | Regions below replication factor |
| `chronix_cluster_leader_changes_total` | Counter | Leader election events |

### Performance

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_cluster_query_latency_seconds` | Histogram | Query latency |
| `chronix_cluster_write_latency_seconds` | Histogram | Write latency |
| `chronix_cluster_heartbeat_latency_seconds` | Histogram | Heartbeat RTT |
| `chronix_raft_log_replication_lag` | Histogram | Replication lag |

### Object Store

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_objstore_put_duration_seconds` | Histogram | Object put latency |
| `chronix_objstore_get_duration_seconds` | Histogram | Object get latency |
| `chronix_objstore_cache_hits_total` | Counter | Local cache hits |
| `chronix_objstore_cache_misses_total` | Counter | Local cache misses |

### Segment I/O

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_segment_prefetch_bytes_total` | Counter | Bytes successfully prefetched via mmap madvise |
| `chronix_segment_prefetch_errors_total` | Counter | Prefetch madvise failures |

### Streaming Aggregation

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_wire_non_finite_samples_skipped_total` | Counter | NaN/Infinity values dropped from streaming aggregation |
| `chronix_prom_remote_write_samples_total` | Counter | Float samples accepted over Prometheus remote write |
| `chronix_prom_remote_write_histograms_total` | Counter | Native histograms accepted over Prometheus remote write |
| `chronix_prom_remote_read_queries_total` | Counter | Prometheus remote read queries served |

### Self-maintenance

Flushing, compaction, rollup materialisation and retention all run on one
thread per open database. **Alert on `chronix_maintenance_running == 0`**: a
process that has lost it looks healthy until the memtable fills, after which
only a restart recovers. `/ready` answers `503` naming the condition, and a
full memtable with no thread to flush it is `503 OVERLOADED` rather than
retryable back-pressure.

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_maintenance_running` | Gauge | `1` while the maintenance thread is alive, `0` once it has ended. Refreshed every tick, so an absence lasting longer than that means the same as a zero |
| `chronix_maintenance_panics_total` | Counter | Passes that panicked. The thread survives one — the other passes keep running — but a non-zero rate is a defect to report |

## Metrics / Observability

Chronix uses the [`metrics`](https://crates.io/crates/metrics) facade crate
(v0.24) to emit runtime telemetry. The library emits metrics via `counter!`,
`gauge!`, and `histogram!` macros — the embedding application is responsible
for installing a recorder (e.g., `metrics-exporter-prometheus`).

| Metric | Type | Emitted From | Description |
|--------|------|--------------|-------------|
| `chronix_write_errors_total` | Counter | every write path | Writes that failed, labelled `reason` = `storage_full` \| `timeout` \| `rejected` \| `panic`. `storage_full` is the data volume, and the one to alert on |
| `chronix_sql_results_truncated_total` | Counter | the HTTP, gRPC and Flight SQL query paths | Results the `sql_max_rows` ceiling cut short or refused. A non-zero rate means somebody is reading a prefix and calling it an answer |
| `chronix_backfill_points_total` | Counter | every write path with `?backfill=true` | Points written **outside** the out-of-order window — importing history rather than ingesting live |
| `chronix_backups_total` | Counter | `backup()` | Checkpoints taken. Published at zero, so an alert on a nightly backup that stopped running can fire from the first scrape |
| `chronix_backup_failures_total` | Counter | `backup()` | Checkpoints that failed. **The one to alert on** — a backup that silently stops working is what the whole subsystem exists to prevent |
| `chronix_backup_bytes_total` | Counter | `backup()` | Bytes the checkpoints placed. Hard-linked segments count their size, so this measures the database rather than the disk the backup consumed |
| `chronix_backup_duration_seconds` | Histogram | `backup()` | How long a checkpoint takes, flush included |
| `chronix_compaction_backpressure_active` | Gauge | `apply_backpressure()` | 1.0 when write throttling is active, 0.0 otherwise |
| `chronix_catalog_fsync_total` | Counter | the catalog manifest | Manifest fsyncs. Beside `chronix_wal_fsync_total`: on flash the fsync rate is the wear rate. A compaction is **one**, whatever it retires, and so is a delete however many tombstones it produces |
| `chronix_maintenance_running` | Gauge | the maintenance thread, refreshed each tick | `1` while it is alive, `0` once it has ended. Everything a database does for itself happens there, so a `0` — or an absence lasting longer than a tick — means the process will stop accepting writes and only a restart recovers. **The one to alert on** |
| `chronix_maintenance_panics_total` | Counter | the maintenance thread | Passes that panicked. The thread survives one and the other passes keep running, so this is a defect report rather than an outage |
| `chronix_memtable_memory_bytes` | Gauge | `statistics()` | Memtable rows and index entries |
| `chronix_interner_memory_bytes` | Gauge | `statistics()` | String interners — grows with cardinality, not row count |
| `chronix_wal_buffer_bytes` | Gauge | `statistics()` | WAL writer's buffer; fixed size |
| `chronix_catalog_memory_bytes` | Gauge | `statistics()` | Segment catalog, schema registry and tombstones |
| `chronix_retention_shards_dropped_total` | Counter | `enforce_retention()` | Shards retention **removed** — not the ones it considered. A pass that declines an expired shard because a rollup still needs it counts zero here |
| `chronix_series_released_total` | Counter | `enforce_retention()`, `gc()`, `archive_cold_segments()` | Series returned to the cardinality budget because the last of their data was deleted |
| `chronix_gc_segments_deleted_total` | Counter | `gc()` | Segment files unlinked by garbage collection — the ones a retirement pass had to leave because a running read still held them |
| `chronix_segments_retired_awaiting_readers_total` | Counter | every retirement pass | Segments taken out of the database whose files could not be unlinked yet, because a running scan had already been handed the path |
| `chronix_leased_segments` | Gauge | `statistics()` | Segment files a running read is holding. Near zero at rest; a number that only grows is a scan nobody dropped |
| `chronix_rollup_invalidations_total` | Counter | `backfill()`, `execute_delete()` | Rollups whose buckets were marked for recomputation by a late write or a delete |
| `chronix_rollup_ranges_recomputed_total` | Counter | `materialise_rollups()` | Invalidated ranges recomputed |
| `chronix_rollup_failures_total` | Counter | `materialise_rollups()` | Rollups whose materialisation failed this pass (the others still run) |
| `chronix_detector_verdicts_total` | Counter | `advance_detectors()` | Verdict rows a declared detector wrote, labelled `detector` |
| `chronix_detector_late_points_total` | Counter | every write | Points that arrived at or below a declared detector's watermark and will not be judged, labelled `detector`. A steady rate means `settle` is shorter than the source's delivery delay |
| `chronix_detector_failures_total` | Counter | `advance_detectors()` | Declared detectors whose pass failed (the others still run, and the failed one keeps its watermark) |
| `chronix_cold_archive_groups_awaiting_rollup_total` | Counter | `archive_cold_segments()` | `(measurement, shard)` groups kept hot because a rollup has not caught up |
| `chronix_cold_archive_groups_incomplete_total` | Counter | `archive_cold_segments()` | Groups kept hot because another segment or an unflushed row overlaps their range |
| `chronix_cold_archive_objects_total` | Counter | `archive_cold_segments()` | Archive objects uploaded and verified |
| `chronix_cold_archive_rows_total` | Counter | `archive_cold_segments()` | Rows written to the archive, after dedup and tombstones |
| `chronix_rollup_points_written_total` | Counter | `materialise_rollups()` | Rollup points written by materialisation |
| `chronix_retention_segments_awaiting_rollup_total` | Counter | `enforce_retention()` | Expired segments preserved because a rollup they feed is not yet materialised past them |
| `chronix_forecast_fit_duration_seconds` | Histogram | `chronix-analytics::forecast` | Duration of model fit operations |
| `chronix_forecast_predict_duration_seconds` | Histogram | `chronix-analytics::forecast` | Duration of model predict operations |
| `chronix_anomaly_fit_duration_seconds` | Histogram | `chronix-analytics::anomaly` | Duration of anomaly detector fit |
| `chronix_anomaly_detect_duration_seconds` | Histogram | `chronix-analytics::anomaly` | Duration of anomaly detection |
| `chronix_anomaly_detected_total` | Counter | `chronix-analytics::anomaly` | Total anomalies detected |
| `chronix_preprocess_duration_seconds` | Histogram | `chronix-analytics::preprocess` | Duration of preprocessing pipeline |
| `chronix_clock_drift_corrections_total` | Counter | `chronix-analytics::preprocess` | Clock drift corrections applied |
| `chronix_compute_batch_operations_total` | Counter | `chronix-analytics::compute` | Batch compute operations executed |
| `chronix_compute_parallel_fit_duration_seconds` | Histogram | `chronix-analytics::compute` | Duration of parallel model fitting |
| `chronix_segment_prefetch_bytes_total` | Counter | `chronix-engine::segment` | Total bytes successfully prefetched via mmap madvise |
| `chronix_segment_prefetch_errors_total` | Counter | `chronix-engine::segment` | Prefetch madvise failures |
| `chronix_model_drift_detected_total` | Counter | `chronix-analytics::lifecycle` | Drift detection events |
| `chronix_model_drift_score` | Gauge | `chronix-analytics::lifecycle` | Current drift score per model |
| `chronix_model_accuracy_mape` | Gauge | `chronix-analytics::lifecycle` | Model accuracy (MAPE) |
| `chronix_model_accuracy_rmse` | Gauge | `chronix-analytics::lifecycle` | Model accuracy (RMSE) |
| `chronix_model_age_seconds` | Gauge | `chronix-analytics::lifecycle` | Time since last model fit |
| `chronix_model_refit_total` | Counter | `chronix-analytics::lifecycle` | Model re-fit events |
| `chronix_model_ab_test_champion_mape` | Gauge | `chronix-analytics::lifecycle` | Champion model MAPE in A/B test |
| `chronix_model_ab_test_challenger_mape` | Gauge | `chronix-analytics::lifecycle` | Challenger model MAPE in A/B test |
| `chronix_correlation_compute_duration_seconds` | Histogram | `chronix-analytics::multivariate` | Duration of correlation computation |
| `chronix_multivariate_anomaly_detect_duration_seconds` | Histogram | `chronix-analytics::multivariate` | Duration of MV anomaly detection |
| `chronix_derived_series_eval_duration_seconds` | Histogram | `chronix-analytics::multivariate` | Duration of derived series evaluation |
| `chronix_composite_signal_fired_total` | Counter | `chronix-analytics::multivariate` | Composite signals fired |
| `chronix_signal_delivery_queued_total` | Counter | `DeliveryRouter::deliver` | Signals handed to a channel's worker — queued, not yet sent |
| `chronix_signal_delivery_total` | Counter | delivery worker | Signals delivered, labelled `channel` |
| `chronix_signal_delivery_failed_total` | Counter | delivery worker | Failed delivery *attempts*, labelled `channel` — a retried signal counts once per attempt |
| `chronix_signal_delivery_duration_seconds` | Histogram | delivery worker | Queue to successful delivery, **including retries**, labelled `channel` |
| `chronix_signal_delivery_queue_depth` | Gauge | `DeliveryRouter::deliver` | Signals waiting on a channel's worker, labelled `channel` |
| `chronix_signal_delivery_dropped_total` | Counter | `DeliveryRouter::deliver` | Signals discarded on a full queue, labelled `channel`. **Non-zero means alerts are being lost** |
| `chronix_signal_delivery_unrouted_total` | Counter | `DeliveryRouter::deliver` | Signals naming an unregistered channel — fired, delivered nowhere |
| `chronix_signal_evicted_total` | Counter | `SignalStore::store` | Signals evicted from a namespace's own ring to make room |

**Tracing instrumentation** – All forecast `fit()`/`predict()` and anomaly `fit()`/`detect()` operations are annotated with `#[tracing::instrument]` spans at debug level, enabling timing analysis via any `tracing-subscriber` backend.

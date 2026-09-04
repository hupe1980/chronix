//! Object storage backend for Chronix.
//!
//! Provides cloud-compatible segment storage (S3, GCS, Azure Blob Storage)
//! implementing the [`crate::storage::StorageBackend`] trait, with an
//! optional local disk cache for frequently accessed segments.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use chronix_engine::objstore::{ObjectStoreBackend, ObjectStoreConfig};
//! use chronix_engine::storage::StorageBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // S3 backend with 1 GB local cache
//! let config = ObjectStoreConfig {
//!     url: "s3://my-bucket/chronix-data".into(),
//!     cache: Some(crate::objstore::CacheConfig {
//!         cache_dir: "/tmp/chronix-cache".into(),
//!         max_size_bytes: 1024 * 1024 * 1024,
//!     }),
//!     multipart_threshold_bytes: 8 * 1024 * 1024, // 8 MiB
//!     max_concurrent_downloads: 8,
//! };
//!
//! let backend = ObjectStoreBackend::new(config).await?;
//!
//! // Use it like any StorageBackend
//! let path = crate::storage::SegmentPath::new(
//!     chronix_core::NamespaceId::default_namespace(),
//!     chronix_core::ShardId(1),
//!     "seg_001.csx",
//! )?;
//! backend.put_segment(&path, b"segment data").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # URL Schemes
//!
//! | Scheme     | Backend            | Example                              |
//! |------------|--------------------|--------------------------------------|
//! | `s3://`    | Amazon S3          | `s3://bucket/prefix`                 |
//! | `gs://`    | Google Cloud       | `gs://bucket/prefix`                 |
//! | `az://`    | Azure Blob Storage | `az://container/prefix`              |
//! | `file://`  | Local filesystem   | `file:///tmp/data`                   |
//! | `memory://`| In-memory (tests)  | `memory://test`                      |
//! | `/path`    | Local filesystem   | `/tmp/data`                          |

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod backend;
pub mod cache;
pub mod error;
pub mod parquet_tier;
pub mod tiering;

pub use backend::{BackoffConfig, ObjectStoreBackend, ObjectStoreConfig};
pub use cache::{CacheConfig, DiskCache};
pub use error::{ObjStoreError, Result};
pub use parquet_tier::{parquet_to_batches, ParquetArchiveWriter};
pub use tiering::{ArchiveObject, TieringConfig, TieringEngine};

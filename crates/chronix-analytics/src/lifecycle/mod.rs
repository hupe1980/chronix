//! # Chronix Model Lifecycle Management
//!
//! Provides model registry, versioning, champion/challenger A/B testing,
//! drift detection, and accuracy tracking for forecast and anomaly models.
//!
//! ## Components
//!
//! - [`ModelRegistry`] — versioned storage with champion/challenger tagging
//! - `ABTest` — champion/challenger A/B evaluation
//! - [`DriftDetector`] — distribution drift detection (PSI, KS, ADWIN)
//! - [`Adwin`] — streaming ADWIN change detection (Bifet & Gavaldà 2007)
//! - [`AccuracyTracker`] — sliding-window accuracy metrics and staleness

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod ab_test;
pub mod accuracy;
pub mod adwin;
pub mod drift;
pub mod error;
pub mod registry;

pub use ab_test::{ABTestConfig, ABTestEvaluator, ABTestResult, PromotionCriteria};
pub use accuracy::{AccuracyTracker, AccuracyTrackerConfig};
pub use adwin::Adwin;
pub use drift::{DriftAction, DriftCallback, DriftDetector, DriftMonitor, DriftReport};
pub use error::{LifecycleError, Result};
pub use registry::{AccuracyMetrics, ModelRegistry, ModelTag, ModelVersion};

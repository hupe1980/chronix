//! Where a call's parallel work runs: the caller's thread, a pool chronix
//! builds on first use, or a pool the host injected.
//!
//! Chronix never configures rayon's global pool. Every fan-out — the segment
//! scan, the analytics kernels — runs inside [`Executor::run`], so a nested
//! `par_iter` lands on the pool this executor chose, or stays on the caller's
//! thread in serial mode.

use std::sync::{Arc, OnceLock};

use chronix_core::Parallelism;
use rayon::ThreadPool;

/// The parallelism of one open database.
pub(crate) struct Executor {
    kind: Kind,
}

enum Kind {
    Serial,
    /// Built on first parallel use, so a database that never fans out never
    /// starts a thread.
    Lazy {
        threads: usize,
        pool: OnceLock<Option<Arc<ThreadPool>>>,
    },
    Injected(Arc<ThreadPool>),
}

impl Executor {
    pub(crate) fn new(parallelism: Parallelism) -> Self {
        let kind = match parallelism.budget() {
            1 => Kind::Serial,
            threads => Kind::Lazy {
                threads,
                pool: OnceLock::new(),
            },
        };
        Self { kind }
    }

    pub(crate) fn injected(pool: Arc<ThreadPool>) -> Self {
        Self {
            kind: Kind::Injected(pool),
        }
    }

    /// How many threads a call may use. Always ≥ 1.
    pub(crate) fn budget(&self) -> usize {
        match &self.kind {
            Kind::Serial => 1,
            Kind::Lazy { threads, .. } => *threads,
            Kind::Injected(pool) => pool.current_num_threads().max(1),
        }
    }

    /// Runs `f` so that its parallel work uses this executor's budget.
    pub(crate) fn run<R: Send>(&self, f: impl FnOnce() -> R + Send) -> R {
        match self.pool() {
            Some(pool) => pool.install(f),
            None => serial(f),
        }
    }

    fn pool(&self) -> Option<&Arc<ThreadPool>> {
        match &self.kind {
            Kind::Serial => None,
            Kind::Injected(pool) => Some(pool),
            Kind::Lazy { threads, pool } => pool
                .get_or_init(|| {
                    rayon::ThreadPoolBuilder::new()
                        .num_threads(*threads)
                        .thread_name(|i| format!("chronix-par-{i}"))
                        .build()
                        .map(Arc::new)
                        .inspect_err(|e| {
                            tracing::warn!(error = %e, "could not start the query pool; running serially");
                        })
                        .ok()
                })
                .as_ref(),
        }
    }
}

#[cfg(feature = "analytics")]
fn serial<R>(f: impl FnOnce() -> R) -> R {
    chronix_analytics::par::serial(f)
}

#[cfg(not(feature = "analytics"))]
fn serial<R>(f: impl FnOnce() -> R) -> R {
    f()
}

/// A shared [`Executor`] that SQL functions can carry. Compares by identity,
/// so a function registered for one database equals only itself.
#[cfg(feature = "sql")]
#[derive(Clone)]
pub(crate) struct ExecHandle(pub(crate) Arc<Executor>);

#[cfg(feature = "sql")]
impl std::fmt::Debug for ExecHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExecHandle(budget = {})", self.0.budget())
    }
}

#[cfg(feature = "sql")]
impl PartialEq for ExecHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[cfg(feature = "sql")]
impl Eq for ExecHandle {}

#[cfg(feature = "sql")]
impl std::hash::Hash for ExecHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.0), state);
    }
}

/// Records which thread read a segment, for the parallelism tests.
#[inline]
pub(crate) fn probe() {
    #[cfg(feature = "analytics")]
    chronix_analytics::par::probe::record();
}

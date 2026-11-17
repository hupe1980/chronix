//! Data parallelism that respects the caller's thread budget.
//!
//! Every fan-out in this crate goes through [`map`] or [`map_zip_mut`]. Inside
//! a [`serial`] scope they iterate on the calling thread; otherwise they use
//! rayon's *current* pool — the one a caller entered with
//! `ThreadPool::install`, so a host that owns a pool decides how many threads
//! exist. Nothing here builds or configures a pool.

use std::cell::Cell;

use rayon::prelude::*;

thread_local! {
    static SERIAL: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with every helper in this module iterating sequentially on the
/// calling thread. Nests, and restores the previous mode even if `f` panics.
pub fn serial<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            SERIAL.with(|s| s.set(self.0));
        }
    }
    let _restore = Restore(SERIAL.with(|s| s.replace(true)));
    f()
}

/// Whether the current thread is inside a [`serial`] scope.
#[must_use]
pub fn is_serial() -> bool {
    SERIAL.with(Cell::get)
}

/// `items.iter().map(f).collect()`, in parallel unless [`is_serial`].
pub fn map<T, R, F>(items: &[T], f: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(&T) -> R + Sync + Send,
{
    if is_serial() {
        items
            .iter()
            .map(|t| {
                probe::record();
                f(t)
            })
            .collect()
    } else {
        items
            .par_iter()
            .map(|t| {
                probe::record();
                f(t)
            })
            .collect()
    }
}

/// `a.iter_mut().zip(b).map(f).collect()`, in parallel unless [`is_serial`].
pub fn map_zip_mut<A, B, R, F>(a: &mut [A], b: &[B], f: F) -> Vec<R>
where
    A: Send,
    B: Sync,
    R: Send,
    F: Fn(&mut A, &B) -> R + Sync + Send,
{
    if is_serial() {
        a.iter_mut()
            .zip(b)
            .map(|(x, y)| {
                probe::record();
                f(x, y)
            })
            .collect()
    } else {
        a.par_iter_mut()
            .zip(b.par_iter())
            .map(|(x, y)| {
                probe::record();
                f(x, y)
            })
            .collect()
    }
}

/// Test support: which threads ran the work items of a fan-out.
///
/// Off unless a test starts it; when off, recording is one relaxed load.
#[doc(hidden)]
pub mod probe {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    static ON: AtomicBool = AtomicBool::new(false);
    static SEEN: Mutex<Vec<Seen>> = Mutex::new(Vec::new());

    /// One work item: the thread's name and whether it was a rayon worker.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Seen {
        /// `std::thread::current().name()`.
        pub thread: Option<String>,
        /// `rayon::current_thread_index().is_some()`.
        pub rayon_worker: bool,
    }

    /// Starts recording, discarding anything recorded before.
    pub fn start() {
        SEEN.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        ON.store(true, Ordering::SeqCst);
    }

    /// Stops recording and returns what was seen.
    pub fn take() -> Vec<Seen> {
        ON.store(false, Ordering::SeqCst);
        std::mem::take(
            &mut *SEEN
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Records the current thread, if recording is on.
    pub fn record() {
        if ON.load(Ordering::Relaxed) {
            let seen = Seen {
                thread: std::thread::current().name().map(str::to_owned),
                rayon_worker: rayon::current_thread_index().is_some(),
            };
            SEEN.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(seen);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_runs_every_item_on_the_calling_thread() {
        let items: Vec<u32> = (0..64).collect();
        let me = std::thread::current().id();
        let ids = serial(|| map(&items, |_| std::thread::current().id()));
        assert!(ids.iter().all(|id| *id == me));
        let mut a = vec![0u32; 64];
        let on_worker =
            serial(|| map_zip_mut(&mut a, &items, |_, _| rayon::current_thread_index()));
        assert!(on_worker.iter().all(Option::is_none));
    }

    #[test]
    fn outside_serial_the_current_pool_does_the_work() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|i| format!("par-test-{i}"))
            .build()
            .unwrap();
        let items: Vec<u32> = (0..256).collect();
        let names =
            pool.install(|| map(&items, |_| std::thread::current().name().map(str::to_owned)));
        assert!(
            names
                .iter()
                .all(|n| n.as_deref().is_some_and(|n| n.starts_with("par-test-")))
        );
    }

    #[test]
    fn serial_is_restored_after_a_panic() {
        let caught = std::panic::catch_unwind(|| serial(|| panic!("inside")));
        assert!(caught.is_err());
        assert!(!is_serial());
        serial(|| {
            serial(|| assert!(is_serial()));
            assert!(is_serial());
        });
        assert!(!is_serial());
    }
}

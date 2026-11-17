//! A bound on concurrently executing queries.
//!
//! SQL, PromQL and Flight SQL share one set of slots. A query waits for a slot
//! within its own deadline; past it, it is refused with `503` naming
//! `server.max_concurrent_queries`. The slot travels with the execution and
//! is released when the work ends — not when the client is answered — so an
//! abandoned query still counts until its operator stops. Writes, health and
//! discovery never take a slot.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ServerError;

/// The shared slots.
#[derive(Debug, Clone)]
pub struct QuerySlots {
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

/// One held slot. Dropping it frees the slot.
#[derive(Debug)]
pub struct QuerySlot {
    _permit: OwnedSemaphorePermit,
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        metrics::gauge!("chronix_query_slots_in_use").decrement(1.0);
    }
}

impl Default for QuerySlots {
    /// One slot per core but one, leaving a core for writes and health.
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Self::new(cores.saturating_sub(1))
    }
}

impl QuerySlots {
    /// `capacity` slots; `0` is treated as `1`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    /// The slots for a server: `configured`, or — when it is `0` — one less
    /// than the database's parallelism budget, at least one.
    #[must_use]
    pub fn for_server(configured: usize, parallelism_budget: usize) -> Self {
        if configured > 0 {
            Self::new(configured)
        } else {
            Self::new(parallelism_budget.saturating_sub(1))
        }
    }

    /// How many queries may execute at once.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Waits for a slot, for at most `deadline` (`Duration::ZERO` waits as
    /// long as it takes).
    ///
    /// # Errors
    ///
    /// [`ServerError::QuerySlotsBusy`] if every slot stayed busy past the
    /// deadline.
    pub async fn acquire(&self, deadline: Duration) -> Result<QuerySlot, ServerError> {
        let wait = Arc::clone(&self.semaphore).acquire_owned();
        let permit = if deadline.is_zero() {
            wait.await
        } else {
            match tokio::time::timeout(deadline, wait).await {
                Ok(permit) => permit,
                Err(_) => {
                    metrics::counter!("chronix_query_slot_wait_timeouts_total").increment(1);
                    return Err(ServerError::QuerySlotsBusy {
                        waited: deadline,
                        capacity: self.capacity,
                    });
                }
            }
        }
        // The semaphore is never closed.
        .map_err(|_| ServerError::Internal("query slots closed".into()))?;
        metrics::gauge!("chronix_query_slots_in_use").increment(1.0);
        Ok(QuerySlot { _permit: permit })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_query_waits_for_a_slot_and_is_refused_past_its_deadline() {
        let slots = QuerySlots::new(1);
        let held = slots.acquire(Duration::ZERO).await.unwrap();
        let err = slots.acquire(Duration::from_millis(20)).await.unwrap_err();
        assert!(
            err.to_string().contains("server.max_concurrent_queries"),
            "{err}"
        );
        drop(held);
        assert!(slots.acquire(Duration::from_millis(20)).await.is_ok());
    }

    #[test]
    fn zero_derives_from_the_budget_and_never_reaches_zero() {
        assert_eq!(QuerySlots::for_server(0, 8).capacity(), 7);
        assert_eq!(QuerySlots::for_server(0, 1).capacity(), 1);
        assert_eq!(QuerySlots::for_server(3, 8).capacity(), 3);
    }
}

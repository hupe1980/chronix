//! Rate-limiting middleware backed by token-buckets.
//!
//! Two layers of limiting coexist:
//!
//! 1. **Global** — a single shared token bucket applied to all requests.
//!    Configured via `rate_limit_rps` / `rate_limit_burst` in server config.
//! 2. **Per-namespace** — one token bucket per tenant, derived from the
//!    `max_request_rps` / `max_request_burst` fields in `NamespaceQuota`.
//!    Requires the `namespace_layer` middleware to have run first so
//!    [`NamespaceContext`] is available in request extensions.
//!
//! When either layer denies a request, the server returns **HTTP 429 Too
//! Many Requests** with a `Retry-After` header.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use parking_lot::RwLock;
use tracing::warn;

use crate::namespace::NamespaceContext;

// ── Lightweight token-bucket implementation ────────────────────────

/// A thread-safe rate limiter with burst tolerance.
///
/// Implemented as **GCRA** (the generic cell rate algorithm — the same
/// formulation the `governor` crate uses), which keeps the entire state in a
/// single `u64`: the *theoretical arrival time* (TAT) of the next request that
/// would be perfectly conforming, in nanoseconds since `epoch`.
///
/// A request at `now` is admitted when `now >= tat - burst_ns`; admitting it
/// pushes the TAT forward by one emission interval. The bucket therefore
/// starts full (`burst` requests admitted back to back) and refills
/// continuously at `rps`.
///
/// # Why not a token count plus a timestamp
///
/// The obvious two-field form — available tokens and the last refill instant —
/// needs both fields to change together. Updating them with two independent
/// compare-and-swaps is not atomic: a thread can win the timestamp swap and
/// lose the token swap, having advanced the clock without ever crediting the
/// tokens that elapsed. The accrual is then gone for good, so under contention
/// the limiter drifts *below* its configured rate and rejects traffic it
/// should have admitted. Encoding the state as one number makes that class of
/// bug unrepresentable, and needs one CAS instead of two.
#[derive(Debug)]
pub struct TokenBucket {
    /// Nanoseconds between conforming requests (1e9 / rps).
    interval_ns: u64,
    /// How far the TAT may run ahead of now — the burst tolerance.
    ///
    /// `(burst - 1)` intervals, not `burst`: admitting a request always costs
    /// one interval, so a tolerance of `burst` intervals would let `burst + 1`
    /// requests through back to back.
    burst_ns: u64,
    /// Theoretical arrival time, nanos since `epoch`.
    tat_ns: AtomicU64,
    /// A fixed reference point so that `Instant` arithmetic stays small.
    epoch: Instant,
}

impl TokenBucket {
    /// Create a new bucket pre-filled to `burst` tokens.
    fn new(rps: NonZeroU32, burst: NonZeroU32) -> Self {
        let interval_ns = (1_000_000_000u64 / u64::from(rps.get())).max(1);
        Self {
            interval_ns,
            burst_ns: u64::from(burst.get() - 1) * interval_ns,
            tat_ns: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    /// Try to acquire one token.
    ///
    /// Returns `Ok(())` on success, or `Err(wait)` with the minimum
    /// [`Duration`] before a token becomes available.
    fn check(&self) -> Result<(), Duration> {
        let now_ns = self.epoch.elapsed().as_nanos() as u64;
        let mut tat = self.tat_ns.load(Ordering::Acquire);

        loop {
            // A TAT in the past means the bucket has been idle and is full.
            let effective_tat = tat.max(now_ns);
            let earliest = effective_tat.saturating_sub(self.burst_ns);
            if now_ns < earliest {
                return Err(Duration::from_nanos(earliest - now_ns));
            }

            let next_tat = effective_tat.saturating_add(self.interval_ns);
            match self.tat_ns.compare_exchange_weak(
                tat,
                next_tat,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                // The CAS is the admission decision: exactly one caller can
                // move the TAT from `tat` to `next_tat`, so no request is
                // admitted twice and no elapsed time is dropped.
                Ok(_) => return Ok(()),
                Err(observed) => {
                    tat = observed;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// Shared, non-keyed (global) rate limiter.
pub type GlobalLimiter = Arc<TokenBucket>;

/// Per-namespace rate limiter backed by a shared map of individual token buckets.
///
/// Each namespace gets its own independent bucket whose capacity is derived
/// from the namespace's [`NamespaceQuota`](chronix_core::NamespaceQuota).
/// Namespaces without explicit rate limits (`max_request_rps == 0`) are
/// unlimited and do not occupy an entry.
#[derive(Debug, Clone)]
pub struct NamespaceRateLimiter {
    /// Map from namespace name to its dedicated limiter.
    limiters: Arc<dashmap::DashMap<String, GlobalLimiter>>,
}

impl NamespaceRateLimiter {
    /// Create a new empty per-namespace rate limiter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            limiters: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Register or update the rate limit for a namespace.
    ///
    /// Pass `rps == 0` to remove rate limiting for the given namespace.
    pub fn set_limit(&self, namespace: &str, rps: u64, burst: u32) {
        if rps == 0 {
            self.limiters.remove(namespace);
            return;
        }
        if let Some(limiter) = build_limiter(rps, burst) {
            self.limiters.insert(namespace.to_string(), limiter);
        }
    }

    /// Remove the rate limiter for a namespace.
    pub fn remove(&self, namespace: &str) {
        self.limiters.remove(namespace);
    }

    /// Check whether the namespace is allowed to proceed.
    ///
    /// Returns `Ok(())` if allowed (no limit or token available).
    /// Returns `Err(retry_after_secs)` if rate-limited.
    pub fn check(&self, namespace: &str) -> std::result::Result<(), u64> {
        let limiter = match self.limiters.get(namespace) {
            Some(entry) => Arc::clone(entry.value()),
            None => return Ok(()),
        };

        match limiter.check() {
            Ok(_) => Ok(()),
            Err(wait) => Err(wait.as_secs().max(1)),
        }
    }
}

impl Default for NamespaceRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-user (per-authenticated-principal) rate limiter.
///
/// Applies a uniform rate limit to each authenticated user independently.
/// Each user gets their own token bucket with the configured `rps` and
/// `burst` values.  Anonymous / unauthenticated requests bypass this
/// limiter.
///
/// Stale buckets (unused for `bucket_ttl`) are evicted periodically
/// to prevent unbounded memory growth.
///
/// Prevents heavy users from crowding out other tenants' quota.
#[derive(Debug, Clone)]
pub struct UserRateLimiter {
    /// Per-user ceiling (requests per second).
    rps: u64,
    /// Per-user burst size.
    burst: u32,
    /// Map from principal name → (limiter, last_used).
    limiters: Arc<RwLock<HashMap<String, (GlobalLimiter, Instant)>>>,
    /// Time-to-live for idle buckets.
    bucket_ttl: Duration,
    /// Counter tracking total check calls to amortize eviction cost.
    check_counter: Arc<AtomicU64>,
}

/// How many checks between eviction sweeps.
const EVICTION_INTERVAL: u64 = 1024;

impl UserRateLimiter {
    /// Create a new per-user rate limiter with the given ceiling.
    ///
    /// Returns `None` when `rps == 0` (disabled).
    #[must_use]
    pub fn new(rps: u64, burst: u32) -> Option<Self> {
        if rps == 0 {
            return None;
        }
        Some(Self {
            rps,
            burst: if burst == 0 {
                u32::try_from(rps).unwrap_or(u32::MAX)
            } else {
                burst
            },
            limiters: Arc::new(RwLock::new(HashMap::new())),
            bucket_ttl: Duration::from_secs(300), // 5 minutes
            check_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Check whether the user is allowed to proceed.
    ///
    /// Returns `Ok(())` if allowed, `Err(retry_after_secs)` if rate-limited.
    /// Creates a new bucket on first encounter.  Periodically evicts
    /// stale buckets to prevent unbounded memory growth.
    pub fn check(&self, principal: &str) -> std::result::Result<(), u64> {
        // Periodic eviction (amortized O(1) — runs every EVICTION_INTERVAL calls)
        let count = self.check_counter.fetch_add(1, Ordering::Relaxed);
        if count.is_multiple_of(EVICTION_INTERVAL) && count > 0 {
            self.evict_stale();
        }

        // Fast path: bucket already exists.
        {
            let limiters = self.limiters.read();
            if let Some((limiter, _)) = limiters.get(principal) {
                let limiter = Arc::clone(limiter);
                drop(limiters);
                // Update last-used timestamp
                if let Some(entry) = self.limiters.write().get_mut(principal) {
                    entry.1 = Instant::now();
                }
                return Self::try_acquire(&limiter);
            }
        }

        // Slow path: create a new bucket for this user.
        let limiter = match build_limiter(self.rps, self.burst.max(1)) {
            Some(l) => l,
            None => return Ok(()), // should not happen since rps > 0
        };
        let limiter = self
            .limiters
            .write()
            .entry(principal.to_string())
            .or_insert((limiter, Instant::now()))
            .0
            .clone();
        Self::try_acquire(&limiter)
    }

    /// Evict buckets that haven't been used within `bucket_ttl`.
    fn evict_stale(&self) {
        let now = Instant::now();
        let ttl = self.bucket_ttl;
        let mut limiters = self.limiters.write();
        let before = limiters.len();
        limiters.retain(|_, (_, last_used)| now.duration_since(*last_used) < ttl);
        let evicted = before - limiters.len();
        if evicted > 0 {
            tracing::debug!(
                evicted,
                remaining = limiters.len(),
                "evicted stale user rate-limit buckets"
            );
        }
    }

    fn try_acquire(limiter: &GlobalLimiter) -> std::result::Result<(), u64> {
        match limiter.check() {
            Ok(_) => Ok(()),
            Err(wait) => Err(wait.as_secs().max(1)),
        }
    }
}

/// Build a [`GlobalLimiter`] from the configuration.
///
/// Returns `None` when rate limiting is disabled (`rps == 0`).
#[must_use]
pub fn build_limiter(rps: u64, burst: u32) -> Option<GlobalLimiter> {
    // Guard against silent u64→u32 truncation.
    let rps_u32 = u32::try_from(rps).unwrap_or_else(|_| {
        tracing::warn!(rps, "rps value exceeds u32::MAX, clamping to {}", u32::MAX);
        u32::MAX
    });
    let rps = NonZeroU32::new(rps_u32)?;
    let burst = NonZeroU32::new(burst.max(rps.get())).unwrap_or(rps);
    Some(Arc::new(TokenBucket::new(rps, burst)))
}

/// Axum middleware that enforces global and per-namespace rate limits.
///
/// The global limiter is checked first. If it passes, the per-namespace
/// limiter is checked (if configured). Either can return HTTP 429.
pub async fn rate_limit_middleware(request: Request, next: Next) -> Response {
    // --- Global rate limit ---
    let global_limiter = request.extensions().get::<GlobalLimiter>().cloned();
    if let Some(limiter) = global_limiter {
        match limiter.check() {
            Ok(_) => {} // token acquired
            Err(wait) => {
                let retry_after = wait.as_secs().max(1);
                warn!(
                    retry_after_secs = retry_after,
                    "Global rate limit exceeded, returning 429"
                );
                metrics::counter!("chronix_rate_limited_total").increment(1);
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", retry_after.to_string())],
                    Json(crate::error::ErrorResponse {
                        error: "Too Many Requests".into(),
                        code: "RATE_LIMITED",
                    }),
                )
                    .into_response();
            }
        }
    }

    // --- Per-namespace rate limit ---
    let ns_limiter = request.extensions().get::<NamespaceRateLimiter>().cloned();
    if let Some(ns_limiter) = ns_limiter {
        if let Some(ctx) = request.extensions().get::<NamespaceContext>() {
            if let Err(retry_after) = ns_limiter.check(&ctx.namespace) {
                warn!(
                    namespace = %ctx.namespace,
                    retry_after_secs = retry_after,
                    "Per-namespace rate limit exceeded, returning 429"
                );
                metrics::counter!("chronix_rate_limited_total", "namespace" => ctx.namespace.clone())
                    .increment(1);
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", retry_after.to_string())],
                    Json(crate::error::ErrorResponse {
                        error: format!("Too Many Requests (namespace: {})", ctx.namespace),
                        code: "RATE_LIMITED",
                    }),
                )
                    .into_response();
            }
        }
    }

    // --- Per-user (per-principal) rate limit ---
    let user_limiter = request.extensions().get::<UserRateLimiter>().cloned();
    if let Some(user_limiter) = user_limiter {
        if let Some(auth_ctx) = request
            .extensions()
            .get::<chronix_security::auth::AuthContext>()
        {
            if let Err(retry_after) = user_limiter.check(&auth_ctx.principal) {
                warn!(
                    principal = %auth_ctx.principal,
                    retry_after_secs = retry_after,
                    "Per-user rate limit exceeded, returning 429"
                );
                metrics::counter!(
                    "chronix_rate_limited_total",
                    "principal" => auth_ctx.principal.clone()
                )
                .increment(1);
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", retry_after.to_string())],
                    Json(crate::error::ErrorResponse {
                        error: format!("Too Many Requests (user: {})", auth_ctx.principal),
                        code: "RATE_LIMITED",
                    }),
                )
                    .into_response();
            }
        }
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_limiter_disabled_when_zero() {
        assert!(build_limiter(0, 0).is_none());
    }

    #[test]
    fn build_limiter_creates_with_burst() {
        let lim = build_limiter(100, 200);
        assert!(lim.is_some());
    }

    #[test]
    fn build_limiter_burst_defaults_to_rps() {
        let lim = build_limiter(50, 0);
        assert!(lim.is_some());
    }

    #[test]
    fn namespace_limiter_allows_unregistered() {
        let nl = NamespaceRateLimiter::new();
        assert!(nl.check("unknown").is_ok());
    }

    #[test]
    fn namespace_limiter_set_and_check() {
        let nl = NamespaceRateLimiter::new();
        nl.set_limit("tenant_a", 1, 1);
        // First request should succeed
        assert!(nl.check("tenant_a").is_ok());
        // Second request should be rate-limited (burst = 1)
        assert!(nl.check("tenant_a").is_err());
        // Unregistered namespace is still unlimited
        assert!(nl.check("tenant_b").is_ok());
    }

    #[test]
    fn namespace_limiter_remove() {
        let nl = NamespaceRateLimiter::new();
        nl.set_limit("tenant_a", 1, 1);
        assert!(nl.check("tenant_a").is_ok());
        assert!(nl.check("tenant_a").is_err());
        // Remove limit — now unlimited again
        nl.remove("tenant_a");
        assert!(nl.check("tenant_a").is_ok());
    }

    #[test]
    fn namespace_limiter_zero_rps_removes() {
        let nl = NamespaceRateLimiter::new();
        nl.set_limit("tenant_a", 10, 10);
        assert!(nl.check("tenant_a").is_ok());
        // Setting rps to 0 should remove the limit
        nl.set_limit("tenant_a", 0, 0);
        // Now unlimited — should always succeed
        for _ in 0..100 {
            assert!(nl.check("tenant_a").is_ok());
        }
    }

    // ── Per-user rate limiter tests ──────────────────────────

    #[test]
    fn user_limiter_disabled_when_zero() {
        assert!(UserRateLimiter::new(0, 0).is_none());
    }

    #[test]
    fn user_limiter_creates_per_user_buckets() {
        let ul = UserRateLimiter::new(1, 1).unwrap();
        // First request for user_a should succeed
        assert!(ul.check("user_a").is_ok());
        // Second request for user_a should be rate-limited (burst = 1)
        assert!(ul.check("user_a").is_err());
        // user_b has its own bucket — should succeed
        assert!(ul.check("user_b").is_ok());
    }

    #[test]
    fn user_limiter_independent_buckets() {
        let ul = UserRateLimiter::new(1, 1).unwrap();
        // Exhaust user_a
        assert!(ul.check("user_a").is_ok());
        assert!(ul.check("user_a").is_err());
        // user_b is unaffected
        assert!(ul.check("user_b").is_ok());
        assert!(ul.check("user_b").is_err());
        // user_c is also unaffected
        assert!(ul.check("user_c").is_ok());
    }

    /// A burst must be admitted exactly once, no matter how many threads race
    /// for it.
    ///
    /// The previous two-atomic implementation updated the refill timestamp and
    /// the token count with separate compare-and-swaps and answered a lost race
    /// with a rejection, so concurrent callers could consume fewer than `burst`
    /// tokens between them — the limiter silently ran stricter than configured.
    #[test]
    fn concurrent_callers_consume_exactly_the_burst() {
        const BURST: u32 = 100;
        const THREADS: usize = 8;
        const PER_THREAD: usize = 50;

        // 1 rps → a 1 s emission interval, so nothing refills during the test
        // and the admitted count is exactly the burst.
        let bucket = Arc::new(TokenBucket::new(
            NonZeroU32::new(1).unwrap(),
            NonZeroU32::new(BURST).unwrap(),
        ));
        let admitted = Arc::new(AtomicU64::new(0));

        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let bucket = Arc::clone(&bucket);
                let admitted = Arc::clone(&admitted);
                scope.spawn(move || {
                    for _ in 0..PER_THREAD {
                        if bucket.check().is_ok() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        assert_eq!(
            admitted.load(Ordering::Relaxed),
            u64::from(BURST),
            "{THREADS} threads issuing {} requests must consume the burst exactly once",
            THREADS * PER_THREAD
        );
    }

    #[test]
    fn burst_refills_at_the_configured_rate() {
        // 1000 rps → 1 ms interval; burst of 2.
        let bucket = TokenBucket::new(NonZeroU32::new(1000).unwrap(), NonZeroU32::new(2).unwrap());
        assert!(bucket.check().is_ok());
        assert!(bucket.check().is_ok());
        let Err(wait) = bucket.check() else {
            panic!("third request must exceed a burst of 2");
        };
        assert!(
            wait <= Duration::from_millis(1),
            "wait for one token at 1000 rps should be under a millisecond, got {wait:?}"
        );
        std::thread::sleep(Duration::from_millis(3));
        assert!(bucket.check().is_ok(), "bucket must refill after the wait");
    }
}

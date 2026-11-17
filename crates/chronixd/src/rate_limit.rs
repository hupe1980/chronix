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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::RwLock;
use tracing::warn;

use crate::namespace::NamespaceContext;

// ── Lightweight token-bucket implementation ────────────────────────

/// A thread-safe rate limiter with burst tolerance.
///
/// Implemented as **GCRA** (the generic cell rate algorithm, as in the
/// `governor` crate), with the whole state in one `u64`: the theoretical
/// arrival time (TAT) of the next perfectly conforming request, in
/// nanoseconds since `epoch`. A request at `now` is admitted when
/// `now >= tat - burst_ns`, which advances the TAT by one emission interval:
/// the bucket starts full and refills continuously at `rps`. One number means
/// one compare-and-swap, so contention cannot lose accrued tokens the way a
/// separate count and timestamp can.
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

/// Why [`TokenBucket::check_n`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The cost fits the bucket once it refills for this long.
    Wait(Duration),
    /// The cost is larger than the bucket can ever hold — retrying the same
    /// request unchanged can only be refused again.
    Never,
}

impl TokenBucket {
    /// Try to acquire `n` tokens at once: the GCRA with a cost of `n`
    /// intervals and a tolerance of the whole burst.
    ///
    /// All or nothing — a refused request takes no tokens. `n` larger than
    /// the burst is [`Refusal::Never`], which is what lets a caller tell "back
    /// off" from "split this".
    pub fn check_n(&self, n: u64) -> Result<(), Refusal> {
        let tolerance = self.burst_ns.saturating_add(self.interval_ns);
        let cost = n.saturating_mul(self.interval_ns);
        if cost > tolerance {
            return Err(Refusal::Never);
        }
        let now_ns = self.epoch.elapsed().as_nanos() as u64;
        let mut tat = self.tat_ns.load(Ordering::Acquire);
        loop {
            let effective_tat = tat.max(now_ns);
            let next_tat = effective_tat.saturating_add(cost);
            let ahead = next_tat - now_ns;
            if ahead > tolerance {
                return Err(Refusal::Wait(Duration::from_nanos(ahead - tolerance)));
            }
            match self.tat_ns.compare_exchange_weak(
                tat,
                next_tat,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => {
                    tat = observed;
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// Per-namespace **ingestion** limiter: points per second, admitted per
/// batch before the write runs.
///
/// A batch costs one token per point and the bucket holds one second of the
/// namespace's `max_ingestion_rate`, so a steady writer at the rate is never
/// refused, a burst above it waits, and a single batch larger than one
/// second's allowance is refused as unadmittable rather than retried for
/// ever. It is the one check every write path reaches — HTTP, remote write,
/// OTLP, gRPC, Flight SQL and the connectors all go through
/// [`crate::util::insert_batch_with_mode`].
///
/// It also counts what it admits, per namespace, so the usage endpoint can
/// report the observed rate of every namespace, limited or not.
#[derive(Debug, Clone, Default)]
pub struct NamespaceIngestLimiter {
    buckets: Arc<dashmap::DashMap<String, (u64, GlobalLimiter)>>,
    observed: Arc<dashmap::DashMap<String, RateWindow>>,
}

/// Points admitted in the current and the previous whole second.
#[derive(Debug, Default)]
struct RateWindow {
    second: u64,
    current: u64,
    previous: u64,
}

/// What [`NamespaceIngestLimiter::admit`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestRefusal {
    /// Over the rate; the batch fits after this many milliseconds.
    RetryAfterMs(u64),
    /// The batch is larger than one second of the rate.
    BatchTooLarge,
}

impl NamespaceIngestLimiter {
    /// An empty limiter: every namespace unlimited.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a namespace's rate in points per second; `0` removes the limit.
    pub fn set_limit(&self, namespace: &str, points_per_second: u64) {
        if points_per_second == 0 {
            self.buckets.remove(namespace);
            return;
        }
        let burst = u32::try_from(points_per_second).unwrap_or(u32::MAX);
        if let Some(bucket) = build_limiter(points_per_second, burst) {
            self.buckets
                .insert(namespace.to_string(), (points_per_second, bucket));
        }
    }

    /// Remove a namespace's limit and its observed rate.
    pub fn remove(&self, namespace: &str) {
        self.buckets.remove(namespace);
        self.observed.remove(namespace);
    }

    /// The namespace's configured rate, if it has one.
    #[must_use]
    pub fn limit(&self, namespace: &str) -> Option<u64> {
        self.buckets.get(namespace).map(|b| b.0)
    }

    /// Admit a batch of `points` for `namespace`, or say why not.
    ///
    /// # Errors
    ///
    /// [`IngestRefusal`] when the namespace is over its rate.
    pub fn admit(&self, namespace: &str, points: u64) -> Result<(), IngestRefusal> {
        let bucket = self.buckets.get(namespace).map(|b| Arc::clone(&b.1));
        if let Some(bucket) = bucket {
            match bucket.check_n(points) {
                Ok(()) => {}
                Err(Refusal::Never) => return Err(IngestRefusal::BatchTooLarge),
                Err(Refusal::Wait(wait)) => {
                    let ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX).max(1);
                    return Err(IngestRefusal::RetryAfterMs(ms));
                }
            }
        }
        self.record(namespace, points);
        Ok(())
    }

    /// Points admitted for `namespace` over the last whole second.
    #[must_use]
    pub fn observed_rate(&self, namespace: &str) -> u64 {
        let now = unix_second();
        self.observed.get(namespace).map_or(0, |w| {
            if w.second == now {
                w.previous
            } else if w.second + 1 == now {
                w.current
            } else {
                0
            }
        })
    }

    fn record(&self, namespace: &str, points: u64) {
        let now = unix_second();
        let mut window = self.observed.entry(namespace.to_string()).or_default();
        if window.second != now {
            window.previous = if window.second + 1 == now {
                window.current
            } else {
                0
            };
            window.current = 0;
            window.second = now;
        }
        window.current = window.current.saturating_add(points);
    }
}

fn unix_second() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
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

    /// As [`check`](Self::check), with the wait to the nanosecond — what
    /// gRPC's `grpc-retry-pushback-ms` carries.
    ///
    /// # Errors
    ///
    /// The time until a token is available.
    pub fn check_wait(&self, namespace: &str) -> std::result::Result<(), Duration> {
        let limiter = match self.limiters.get(namespace) {
            Some(entry) => Arc::clone(entry.value()),
            None => return Ok(()),
        };
        limiter.check()
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
                        hint: None,
                        span: None,
                    }),
                )
                    .into_response();
            }
        }
    }

    // --- Per-namespace rate limit ---
    let ns_limiter = request.extensions().get::<NamespaceRateLimiter>().cloned();
    if let Some(ns_limiter) = ns_limiter
        && let Some(ctx) = request.extensions().get::<NamespaceContext>()
        && let Err(retry_after) = ns_limiter.check(&ctx.namespace)
    {
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
                hint: None,
                span: None,
            }),
        )
            .into_response();
    }

    // --- Per-user (per-principal) rate limit ---
    let user_limiter = request.extensions().get::<UserRateLimiter>().cloned();
    if let Some(user_limiter) = user_limiter
        && let Some(auth_ctx) = request
            .extensions()
            .get::<chronix_security::auth::AuthContext>()
        && let Err(retry_after) = user_limiter.check(&auth_ctx.principal)
    {
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
                hint: None,
                span: None,
            }),
        )
            .into_response();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(rate: u32) -> TokenBucket {
        let rate = NonZeroU32::new(rate).unwrap();
        TokenBucket::new(rate, rate)
    }

    #[test]
    fn check_n_admits_within_the_burst_and_takes_nothing_when_refused() {
        let b = bucket(1000);
        assert_eq!(b.check_n(600), Ok(()));
        // 400 left; 500 does not fit and must not consume the 400.
        assert!(matches!(b.check_n(500), Err(Refusal::Wait(_))));
        assert_eq!(b.check_n(400), Ok(()));
    }

    #[test]
    fn check_n_reports_the_wait_until_the_cost_fits() {
        let b = bucket(1000);
        assert_eq!(b.check_n(1000), Ok(()));
        let Err(Refusal::Wait(wait)) = b.check_n(100) else {
            panic!("an empty bucket must ask for a wait");
        };
        // 100 points at 1000/s refill in ~100 ms.
        assert!(wait <= Duration::from_millis(101), "{wait:?}");
        assert!(wait >= Duration::from_millis(90), "{wait:?}");
    }

    #[test]
    fn check_n_larger_than_the_burst_is_never_admissible() {
        let b = bucket(1000);
        assert_eq!(b.check_n(1001), Err(Refusal::Never));
        // …and it took nothing.
        assert_eq!(b.check_n(1000), Ok(()));
    }

    #[test]
    fn concurrent_check_n_never_over_admits() {
        let b = Arc::new(bucket(10_000));
        let admitted = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let b = Arc::clone(&b);
                let admitted = Arc::clone(&admitted);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        if b.check_n(50).is_ok() {
                            admitted.fetch_add(50, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // One second's burst plus what refilled during the test, which is
        // milliseconds: never anywhere near the 40 000 attempted.
        assert!(admitted.load(Ordering::Relaxed) <= 12_000);
    }

    #[test]
    fn ingest_limiter_is_per_namespace_and_unlimited_by_default() {
        let l = NamespaceIngestLimiter::new();
        assert_eq!(l.admit("free", 1_000_000), Ok(()));
        l.set_limit("a", 100);
        assert_eq!(l.admit("a", 100), Ok(()));
        assert!(matches!(
            l.admit("a", 10),
            Err(IngestRefusal::RetryAfterMs(_))
        ));
        assert_eq!(l.admit("a", 101), Err(IngestRefusal::BatchTooLarge));
        assert_eq!(l.admit("b", 1000), Ok(()));
        l.set_limit("a", 0);
        assert_eq!(l.admit("a", 1000), Ok(()));
    }

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

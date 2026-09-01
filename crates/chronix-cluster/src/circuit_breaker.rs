//! Per-node circuit breaker for write routing.
//!
//! Tracks consecutive failures per target node and opens the circuit
//! (blocking further attempts) after a configurable threshold.  After a
//! cooldown period the circuit transitions to half-open and allows a
//! single probe request to check whether the node has recovered.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Default number of consecutive failures before opening the circuit.
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// Default cooldown duration before transitioning from Open to HalfOpen.
pub const DEFAULT_COOLDOWN: Duration = Duration::from_secs(30);

/// State of the circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Normal operation — requests are allowed.
    Closed,
    /// Failing — requests are blocked until the cooldown elapses.
    Open,
    /// Testing recovery — one probe request is allowed.
    HalfOpen,
}

/// Per-node circuit breaker.
///
/// Tracks consecutive write failures to a single cluster node and
/// opens the circuit after [`failure_threshold`](Self) consecutive
/// errors.  Once open, the breaker blocks further requests until the
/// [`cooldown`](Self) period elapses, at which point it transitions to
/// [`HalfOpen`](CircuitBreakerState::HalfOpen) and admits a single
/// probe write.
#[derive(Debug)]
pub struct CircuitBreaker {
    state: CircuitBreakerState,
    consecutive_failures: u32,
    failure_threshold: u32,
    last_failure: Option<Instant>,
    cooldown: Duration,
    /// When true, a probe request is already in
    /// flight; further HalfOpen callers are blocked until the probe
    /// resolves. Uses `AtomicBool` for self-synchronizing thread safety
    /// regardless of caller locking strategy.
    probe_in_flight: AtomicBool,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given threshold and cooldown.
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            consecutive_failures: 0,
            failure_threshold,
            last_failure: None,
            cooldown,
            probe_in_flight: AtomicBool::new(false),
        }
    }

    /// Returns `true` if a request should be allowed through.
    ///
    /// - **Closed** → always allows.
    /// - **Open** → blocks unless the cooldown has elapsed, in which
    ///   case it transitions to [`HalfOpen`](CircuitBreakerState::HalfOpen)
    ///   and allows one probe request.
    /// - **HalfOpen** → allows exactly one probe.
    pub fn should_allow(&mut self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::HalfOpen => {
                // Atomic CAS ensures exactly one
                // probe even without exterior synchronisation.
                self.probe_in_flight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            }
            CircuitBreakerState::Open => {
                if let Some(last) = self.last_failure {
                    if last.elapsed() >= self.cooldown {
                        self.state = CircuitBreakerState::HalfOpen;
                        self.probe_in_flight.store(true, Ordering::Release);
                        true
                    } else {
                        false
                    }
                } else {
                    // Defensive: open without a recorded failure — reset.
                    self.state = CircuitBreakerState::Closed;
                    true
                }
            }
        }
    }

    /// Record a successful request — resets to [`Closed`](CircuitBreakerState::Closed).
    pub fn record_success(&mut self) {
        self.state = CircuitBreakerState::Closed;
        self.consecutive_failures = 0;
        self.probe_in_flight.store(false, Ordering::Release);
    }

    /// Record a failed request.
    ///
    /// Increments the consecutive failure counter and transitions to
    /// [`Open`](CircuitBreakerState::Open) if the threshold is reached.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.last_failure = Some(Instant::now());
        self.probe_in_flight.store(false, Ordering::Release);
        if self.consecutive_failures >= self.failure_threshold {
            self.state = CircuitBreakerState::Open;
        }
    }

    /// Current state of the circuit breaker.
    pub fn state(&self) -> CircuitBreakerState {
        self.state
    }

    /// Current consecutive failure count.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let mut cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert_eq!(cb.state(), CircuitBreakerState::Closed);

        for _ in 0..4 {
            cb.record_failure();
            assert_eq!(cb.state(), CircuitBreakerState::Closed);
        }
        // 5th failure → opens the circuit
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Open);
        assert_eq!(cb.consecutive_failures(), 5);
    }

    #[test]
    fn test_circuit_breaker_allows_after_cooldown() {
        let cooldown = Duration::from_millis(10);
        let mut cb = CircuitBreaker::new(2, cooldown);

        // Open the circuit.
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Open);
        assert!(!cb.should_allow());

        // Wait for cooldown to elapse.
        std::thread::sleep(cooldown + Duration::from_millis(5));

        // Should transition to HalfOpen and allow one request.
        assert!(cb.should_allow());
        assert_eq!(cb.state(), CircuitBreakerState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_closes_on_success() {
        let cooldown = Duration::from_millis(10);
        let mut cb = CircuitBreaker::new(2, cooldown);

        // Open → wait → HalfOpen
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Open);

        std::thread::sleep(cooldown + Duration::from_millis(5));
        assert!(cb.should_allow()); // → HalfOpen

        // Success in HalfOpen → Closed
        cb.record_success();
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn test_circuit_breaker_resets_on_success() {
        let mut cb = CircuitBreaker::new(5, Duration::from_secs(30));

        // Accumulate some failures but stay Closed.
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
        assert_eq!(cb.consecutive_failures(), 3);

        // Success resets counter and keeps Closed.
        cb.record_success();
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn half_open_failure_reopens_circuit() {
        let cooldown = Duration::from_millis(10);
        let mut cb = CircuitBreaker::new(2, cooldown);

        // Open → wait → HalfOpen
        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(cooldown + Duration::from_millis(5));
        assert!(cb.should_allow()); // → HalfOpen

        // Failure in HalfOpen → back to Open
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Open);
    }

    #[test]
    fn new_circuit_breaker_is_closed() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn should_allow_when_closed() {
        let mut cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert!(cb.should_allow());
    }

    #[test]
    fn should_block_when_open_and_cooldown_not_elapsed() {
        let mut cb = CircuitBreaker::new(1, Duration::from_secs(300));
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Open);
        assert!(!cb.should_allow());
    }

    #[test]
    fn half_open_allows_only_one_probe() {
        let cooldown = Duration::from_millis(10);
        let mut cb = CircuitBreaker::new(2, cooldown);

        // Open the circuit.
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitBreakerState::Open);

        std::thread::sleep(cooldown + Duration::from_millis(5));

        // First call transitions to HalfOpen and passes.
        assert!(cb.should_allow());
        assert_eq!(cb.state(), CircuitBreakerState::HalfOpen);

        // Subsequent calls while probe is in flight are blocked.
        assert!(!cb.should_allow());
        assert!(!cb.should_allow());

        // After success, circuit closes and allows again.
        cb.record_success();
        assert_eq!(cb.state(), CircuitBreakerState::Closed);
        assert!(cb.should_allow());
    }
}

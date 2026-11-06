//! Fixed service-wide limits. No peer-address or identity-indexed allocation.
use giggles_core::CoreError;
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceLimits {
    pub sessions: usize,
    pub burst: u64,
    pub per_second: u64,
}
impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            sessions: 128,
            burst: 4096,
            per_second: 4096,
        }
    }
}
pub(crate) struct Admission {
    sessions: Arc<Semaphore>,
    api_calls: Arc<Semaphore>,
    bucket: Mutex<Bucket>,
}
struct Bucket {
    limits: ServiceLimits,
    // Nano-token units retain fractional refill without drift.
    credit: u128,
    last: Instant,
}
impl Bucket {
    fn take(&mut self, now: Instant) -> bool {
        // A regressing timestamp grants nothing and never moves the anchor back.
        let Some(elapsed) = now.checked_duration_since(self.last) else {
            return false;
        };
        let cap = u128::from(self.limits.burst) * 1_000_000_000;
        let refill = elapsed
            .as_nanos()
            .checked_mul(u128::from(self.limits.per_second))
            .unwrap_or(cap);
        self.credit = self.credit.saturating_add(refill).min(cap);
        self.last = now;
        if self.credit < 1_000_000_000 {
            return false;
        }
        self.credit -= 1_000_000_000;
        true
    }
}
impl Admission {
    pub(crate) fn new(limits: ServiceLimits) -> Result<Self, CoreError> {
        if limits.sessions == 0
            || limits.sessions > 128
            || limits.burst == 0
            || limits.burst > 1_000_000
            || limits.per_second == 0
            || limits.per_second > 1_000_000
        {
            return Err(CoreError::Limit);
        }
        Ok(Self {
            sessions: Arc::new(Semaphore::new(limits.sessions)),
            api_calls: Arc::new(Semaphore::new(128)),
            bucket: Mutex::new(Bucket {
                limits,
                credit: u128::from(limits.burst) * 1_000_000_000,
                last: Instant::now(),
            }),
        })
    }
    pub(crate) fn request(&self) -> bool {
        self.bucket.lock().is_ok_and(|mut b| b.take(Instant::now()))
    }
    /// Charge every API attempt, including malformed routes and capacity rejects.
    /// One pool belongs to Service, so multiple listeners/runtimes cannot multiply it.
    pub(crate) fn api_call(&self) -> Option<OwnedSemaphorePermit> {
        if !self.request() {
            return None;
        }
        self.api_calls.clone().try_acquire_owned().ok()
    }
    pub(crate) fn connect(&self) -> Option<Arc<OwnedSemaphorePermit>> {
        if !self.request() {
            return None;
        }
        self.sessions.clone().try_acquire_owned().ok().map(Arc::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn exact_burst_fractional_refill_and_regression_cannot_mint_tokens() {
        let base = Instant::now();
        let mut b = Bucket {
            limits: ServiceLimits {
                sessions: 1,
                burst: 2,
                per_second: 2,
            },
            credit: 2_000_000_000,
            last: base,
        };
        assert!(b.take(base));
        assert!(b.take(base));
        assert!(!b.take(base));
        assert!(!b.take(base + Duration::from_millis(499)));
        assert!(!b.take(base));
        assert!(b.take(base + Duration::from_millis(500)));
        assert!(!b.take(base + Duration::from_millis(999)));
        assert!(b.take(base + Duration::from_secs(1)));
        assert!(!b.take(base + Duration::from_secs(1)));
    }
    #[test]
    fn refill_matches_independent_integer_oracle_over_irregular_steps() {
        let base = Instant::now();
        let mut b = Bucket {
            limits: ServiceLimits {
                sessions: 1,
                burst: 7,
                per_second: 13,
            },
            credit: 7_000_000_000,
            last: base,
        };
        let mut oracle = 7_000_000_000u64;
        let mut previous = 0;
        for i in 0..10_000u64 {
            let now = i * i * 101;
            oracle = (oracle + (now - previous) * 13).min(7_000_000_000);
            let expected = oracle >= 1_000_000_000;
            if expected {
                oracle -= 1_000_000_000;
            }
            assert_eq!(b.take(base + Duration::from_nanos(now)), expected);
            previous = now;
        }
    }
    #[test]
    fn all_configuration_maxima_and_zero_limits_checked() {
        for limits in [
            ServiceLimits {
                sessions: 0,
                ..Default::default()
            },
            ServiceLimits {
                sessions: usize::MAX,
                ..Default::default()
            },
            ServiceLimits {
                burst: 0,
                ..Default::default()
            },
            ServiceLimits {
                burst: u64::MAX,
                ..Default::default()
            },
            ServiceLimits {
                per_second: 0,
                ..Default::default()
            },
            ServiceLimits {
                per_second: u64::MAX,
                ..Default::default()
            },
        ] {
            assert!(Admission::new(limits).is_err());
        }
        assert!(
            Admission::new(ServiceLimits {
                sessions: 128,
                burst: 1_000_000,
                per_second: 1_000_000
            })
            .is_ok()
        );
    }
    #[test]
    fn huge_idle_refill_caps_at_burst_and_has_constant_state() {
        let base = Instant::now();
        let mut b = Bucket {
            limits: ServiceLimits {
                sessions: 1,
                burst: 3,
                per_second: 1_000_000,
            },
            credit: 0,
            last: base,
        };
        let later = base + Duration::from_secs(1_000_000_000);
        assert!(b.take(later));
        assert!(b.take(later));
        assert!(b.take(later));
        assert!(!b.take(later));
        assert!(std::mem::size_of::<Bucket>() < 128);
    }
    #[test]
    fn shared_connection_budget_counts_clones_and_rejections_spend_rate() {
        let a = Admission::new(ServiceLimits {
            sessions: 1,
            burst: 4,
            per_second: 1,
        })
        .unwrap();
        let first = a.connect().unwrap();
        let detached = first.clone();
        assert!(a.connect().is_none());
        drop(first);
        assert!(a.connect().is_none());
        drop(detached);
        assert!(a.connect().is_some());
        assert!(!a.request());
        assert_eq!(a.sessions.available_permits(), 1);
    }

    #[test]
    fn api_attempts_and_capacity_rejections_spend_shared_rate() {
        let admission = Admission::new(ServiceLimits {
            sessions: 1,
            burst: 132,
            per_second: 1,
        })
        .unwrap();
        // Freeze refill through this private test seam; assertions never depend
        // on the host finishing in less than a wall-clock second.
        admission.bucket.lock().unwrap().limits.per_second = 0;
        let socket = admission.connect().unwrap();
        // Admission happens before decoding, so even an immediately discarded
        // malformed request consumes its token.
        drop(admission.api_call().unwrap());
        let calls: Vec<_> = (0..128).map(|_| admission.api_call().unwrap()).collect();
        assert!(admission.api_call().is_none());
        assert_eq!(admission.bucket.lock().unwrap().credit, 1_000_000_000);
        drop(calls);
        // Native/pgwire requests consume the same final token.
        assert!(admission.request());
        assert!(admission.api_call().is_none());
        assert!(!admission.request());
        assert_eq!(admission.api_calls.available_permits(), 128);
        drop(socket);
        assert_eq!(admission.sessions.available_permits(), 1);
    }
}

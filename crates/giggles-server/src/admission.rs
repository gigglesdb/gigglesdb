//! Fixed service-wide limits, plus per-role statement pools keyed by role
//! identity in a bounded map (GD-0075 R5-103). No peer-address allocation.
use crate::Role;
use giggles_core::{Cancellation, CoreError};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// At most this many role pools exist; idle pools are dropped to make room.
pub(crate) const MAX_ROLE_POOLS: usize = 4096;

pub(crate) fn control_exempt(command: &crate::pg_transaction::Command) -> bool {
    use crate::pg_transaction::Command::*;
    matches!(command, Commit | Rollback | Savepoint(_) | Release(_) | RollbackTo(_))
}

pub(crate) fn sql_exempt(sql: &str) -> bool {
    crate::pgwire::admission_exempt(sql)
}

pub(crate) fn request_exempt(request: &crate::Request) -> bool {
    match request {
        crate::Request::Deadline { request, .. } => request_exempt(request),
        crate::Request::Sql { sql, .. } => sql_exempt(sql),
        _ => false,
    }
}

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
/// Per-role statement admission, applied to every role separately on a node:
/// `concurrent` admitted executions (including time waiting for a blocking slot),
/// at most `queue` waiting for role admission, each waiting at most
/// `wait_ms` before failing with 53000. Configured beside `ServiceLimits`
/// (whose literal field set is fixed by existing callers).
/// The default is 16 admitted statement executions per role. A single
/// saturated role cannot fill the blocking pool, leaving a slot for another
/// role. This latency isolation requires other roles together to leave capacity;
/// multiple saturated roles can still fill the pool. Ordinary statements take
/// admission inside and outside transactions; idle transactions hold no permit.
/// COPY holds its permit until CopyDone/CopyFail. Transaction completion,
/// savepoint controls and already-aborted transactions are exempt.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct RoleStatementLimits {
    pub concurrent: usize,
    pub queue: usize,
    pub wait_ms: u64,
}
impl RoleStatementLimits {
    fn valid(&self) -> bool {
        (1..crate::blocking::MAX_BLOCKING).contains(&self.concurrent)
            && self.queue <= 65_536
            && (1..=600_000).contains(&self.wait_ms)
    }
}
impl Default for RoleStatementLimits {
    fn default() -> Self {
        Self {
            concurrent: 16,
            queue: 256,
            wait_ms: 30_000,
        }
    }
}
pub(crate) struct Admission {
    sessions: Arc<Semaphore>,
    api_calls: Arc<Semaphore>,
    bucket: Mutex<Bucket>,
    roles: Mutex<RolePools>,
}
struct RolePools {
    limits: RoleStatementLimits,
    pools: HashMap<(u8, String), Arc<RolePool>>,
}
struct RolePool {
    // The limits the pool was created with; a reconfiguration applies to new pools.
    limits: RoleStatementLimits,
    running: Arc<Semaphore>,
    queued: AtomicUsize,
}
/// Held for the whole statement; dropping it (completion, error, panic or
/// cancellation) returns the role's slot.
pub(crate) struct RolePermit {
    _permit: OwnedSemaphorePermit,
    _pool: Arc<RolePool>,
}
impl std::fmt::Debug for RolePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RolePermit")
    }
}
struct Queued(Arc<RolePool>);
impl Drop for Queued {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Debug)]
pub(crate) enum StatementRefusal {
    QueueFull { role: String, queue: usize },
    WaitExceeded { role: String, wait_ms: u64 },
    TooManyRoles,
    Core(CoreError),
}
impl StatementRefusal {
    pub(crate) fn sqlstate(&self) -> &'static str {
        match self {
            Self::Core(error) => giggles_core::sqlstate(error),
            _ => "53000",
        }
    }
    pub(crate) fn message(&self) -> String {
        match self {
            Self::QueueFull { role, queue } => format!(
                "too many statements waiting for role \"{role}\" on this node (queue limit {queue})"
            ),
            Self::WaitExceeded { role, wait_ms } => format!(
                "statement for role \"{role}\" waited {wait_ms} ms for admission on this node"
            ),
            Self::TooManyRoles => "too many roles with active statements on this node".into(),
            Self::Core(_) => "statement admission cancelled".into(),
        }
    }
}
/// Pool key and display name. The kind byte keeps database role names apart
/// from certificate-bound application and task identities.
fn role_key(role: &Role) -> ((u8, String), String) {
    match role {
        Role::Database { name } => ((0, name.clone()), name.clone()),
        Role::Application { domain, .. } => ((1, format!("{role:?}")), domain.clone()),
        Role::Task { domain, .. } => ((2, format!("{role:?}")), domain.clone()),
    }
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
            roles: Mutex::new(RolePools {
                limits: RoleStatementLimits::default(),
                pools: HashMap::new(),
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
    /// Set the per-role statement limits (startup configuration). Pools still
    /// in use keep their old bounds until they go idle; new pools use these.
    pub(crate) fn set_role_limits(&self, limits: RoleStatementLimits) -> Result<(), CoreError> {
        if !limits.valid() {
            return Err(CoreError::Limit);
        }
        let mut roles = self.roles.lock().map_err(|_| CoreError::Limit)?;
        roles.limits = limits;
        roles.pools.clear();
        Ok(())
    }
    fn role_pool(&self, key: (u8, String)) -> Result<Arc<RolePool>, StatementRefusal> {
        let mut roles = self
            .roles
            .lock()
            .map_err(|_| StatementRefusal::Core(CoreError::Busy))?;
        let limits = roles.limits;
        let pools = &mut roles.pools;
        if let Some(pool) = pools.get(&key) {
            return Ok(pool.clone());
        }
        if pools.len() >= MAX_ROLE_POOLS {
            // Only the map holds an idle pool: no permit and no waiter.
            pools.retain(|_, pool| Arc::strong_count(pool) > 1);
            if pools.len() >= MAX_ROLE_POOLS {
                return Err(StatementRefusal::TooManyRoles);
            }
        }
        let pool = Arc::new(RolePool {
            limits,
            running: Arc::new(Semaphore::new(limits.concurrent)),
            queued: AtomicUsize::new(0),
        });
        pools.insert(key, pool.clone());
        Ok(pool)
    }
    /// Per-role statement admission, in front of the service-wide blocking
    /// pool. A full role queue fails at once; a queued statement fails after
    /// `wait_ms`; a cancelled context ends the wait. The caller owns the permit
    /// through execution, never through idle time between statements.
    pub(crate) async fn statement(
        &self,
        role: &Role,
        context: Option<&Cancellation>,
    ) -> Result<RolePermit, StatementRefusal> {
        let check = || {
            context
                .map_or(Ok(()), Cancellation::check)
                .map_err(StatementRefusal::Core)
        };
        let (key, name) = role_key(role);
        let pool = self.role_pool(key)?;
        check()?;
        if let Ok(permit) = pool.running.clone().try_acquire_owned() {
            return Ok(RolePermit {
                _permit: permit,
                _pool: pool,
            });
        }
        let limits = pool.limits;
        let mut queued = pool.queued.load(Ordering::Acquire);
        loop {
            if queued >= limits.queue {
                return Err(StatementRefusal::QueueFull {
                    role: name,
                    queue: limits.queue,
                });
            }
            match pool.queued.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => queued = current,
            }
        }
        let queued = Queued(pool.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_millis(limits.wait_ms);
        let acquire = pool.running.clone().acquire_owned();
        tokio::pin!(acquire);
        loop {
            check()?;
            tokio::select! {
                permit = &mut acquire => {
                    drop(queued);
                    let permit = permit.map_err(|_| StatementRefusal::Core(CoreError::Busy))?;
                    check()?;
                    return Ok(RolePermit { _permit: permit, _pool: pool });
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(StatementRefusal::WaitExceeded { role: name, wait_ms: limits.wait_ms });
                }
                _ = tokio::time::sleep(Duration::from_millis(5)), if context.is_some() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn statement_exemptions_use_complete_transaction_grammar() {
        for sql in ["COMMIT", "END WORK", "ROLLBACK", "ABORT", "SAVEPOINT s",
            "RELEASE SAVEPOINT s", "ROLLBACK TO SAVEPOINT s", "/* control */ END; -- done"] {
            assert!(sql_exempt(sql), "{sql}");
        }
        for sql in ["BEGIN", "START TRANSACTION", "SELECT 1", "COMMIT; SELECT 1",
            "ROLLBACK; BEGIN", "SAVEPOINT", "COMMIT nonsense"] {
            assert!(!sql_exempt(sql), "{sql}");
        }
    }

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
    // GD-0075 R5-103: per-role statement pools.
    fn roles(concurrent: usize, queue: usize, wait_ms: u64) -> Admission {
        let admission = Admission::new(ServiceLimits::default()).unwrap();
        admission
            .set_role_limits(RoleStatementLimits {
                concurrent,
                queue,
                wait_ms,
            })
            .unwrap();
        admission
    }
    fn database(name: &str) -> Role {
        Role::Database { name: name.into() }
    }
    /// Poll once: the statement is now queued (or refused) without a sleep.
    async fn poll_once<F: std::future::Future + Unpin>(f: &mut F) -> std::task::Poll<F::Output> {
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(std::pin::Pin::new(&mut *f), cx))
        })
        .await
    }
    #[tokio::test]
    async fn role_queue_bound_wait_bound_and_release_on_drop() {
        let a = roles(1, 1, 200);
        let role = database("a");
        let running = a.statement(&role, None).await.unwrap();
        let mut queued = Box::pin(a.statement(&role, None));
        assert!(poll_once(&mut queued).await.is_pending());
        // Queue full: the next statement fails at once with 53000.
        let refused = a.statement(&role, None).await.unwrap_err();
        assert!(matches!(
            refused,
            StatementRefusal::QueueFull { queue: 1, .. }
        ));
        assert_eq!(refused.sqlstate(), "53000");
        assert!(refused.message().contains("role \"a\""));
        // Wait bound: the queued statement fails after wait_ms, freeing its slot.
        let started = Instant::now();
        let waited = queued.await.unwrap_err();
        assert!(matches!(
            waited,
            StatementRefusal::WaitExceeded { wait_ms: 200, .. }
        ));
        assert_eq!(waited.sqlstate(), "53000");
        assert!(started.elapsed() < Duration::from_secs(5));
        // A queued statement is admitted when the running one is dropped.
        let mut next = Box::pin(a.statement(&role, None));
        assert!(poll_once(&mut next).await.is_pending());
        drop(running);
        let next = next.await.unwrap();
        drop(next);
        let roles = a.roles.lock().unwrap();
        let pool = roles.pools.get(&(0, "a".to_string())).unwrap();
        assert_eq!(pool.running.available_permits(), 1);
        assert_eq!(pool.queued.load(Ordering::Acquire), 0);
    }
    #[tokio::test]
    async fn roles_are_independent_and_cancellation_ends_the_wait() {
        let a = roles(2, 0, 60_000);
        let held = hold(&a, "a", 2).await;
        assert!(matches!(
            a.statement(&database("a"), None).await,
            Err(StatementRefusal::QueueFull { queue: 0, .. })
        ));
        // Role b is unaffected by a at its limit, including an application
        // identity whose display name collides with a database role.
        let b = a.statement(&database("b"), None).await.unwrap();
        let app = Role::Application {
            domain: "a".into(),
            tables: Default::default(),
        };
        let app_permit = a.statement(&app, None).await.unwrap();
        drop((b, app_permit));
        // A cancelled context ends a queued wait without waiting for wait_ms.
        let c = roles(1, 4, 60_000);
        let role = database("c");
        let _running = c.statement(&role, None).await.unwrap();
        let context = Cancellation::new(None);
        let mut waiting = Box::pin(c.statement(&role, Some(&context)));
        assert!(poll_once(&mut waiting).await.is_pending());
        context.cancel();
        let cancelled = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            cancelled,
            StatementRefusal::Core(CoreError::Cancelled)
        ));
        drop(held);
    }
    async fn hold(a: &Admission, role: &str, n: usize) -> Vec<RolePermit> {
        let mut held = Vec::new();
        for _ in 0..n {
            held.push(a.statement(&database(role), None).await.unwrap());
        }
        held
    }
    #[tokio::test]
    async fn role_pools_are_bounded_and_idle_pools_are_dropped() {
        let a = roles(1, 0, 1_000);
        let mut held = Vec::new();
        for i in 0..MAX_ROLE_POOLS {
            held.push(
                a.statement(&database(&format!("r{i}")), None)
                    .await
                    .unwrap(),
            );
        }
        assert!(matches!(
            a.statement(&database("extra"), None).await,
            Err(StatementRefusal::TooManyRoles)
        ));
        // Releasing one role's last permit makes its pool idle and reclaimable.
        drop(held.pop());
        assert!(a.statement(&database("extra"), None).await.is_ok());
        assert_eq!(a.roles.lock().unwrap().pools.len(), MAX_ROLE_POOLS);
    }
    #[test]
    fn role_limits_are_validated_and_parse_with_defaults() {
        let admission = Admission::new(ServiceLimits::default()).unwrap();
        for limits in [
            RoleStatementLimits {
                concurrent: 0,
                ..Default::default()
            },
            RoleStatementLimits {
                concurrent: crate::blocking::MAX_BLOCKING,
                ..Default::default()
            },
            RoleStatementLimits {
                queue: 65_537,
                ..Default::default()
            },
            RoleStatementLimits {
                wait_ms: 0,
                ..Default::default()
            },
            RoleStatementLimits {
                wait_ms: 600_001,
                ..Default::default()
            },
        ] {
            assert!(admission.set_role_limits(limits).is_err());
        }
        assert_eq!(
            admission.roles.lock().unwrap().limits,
            RoleStatementLimits::default()
        );
        let parsed: RoleStatementLimits =
            serde_json::from_str(r#"{"concurrent":2,"wait_ms":5}"#).unwrap();
        assert_eq!(
            parsed,
            RoleStatementLimits {
                concurrent: 2,
                queue: 256,
                wait_ms: 5
            }
        );
        assert!(admission.set_role_limits(parsed).is_ok());
        assert!(admission.set_role_limits(RoleStatementLimits {
            concurrent: crate::blocking::MAX_BLOCKING - 1,
            ..Default::default()
        }).is_ok());
        assert_eq!(RoleStatementLimits::default().concurrent, 16);
        assert!(serde_json::from_str::<RoleStatementLimits>(r#"{"x":1}"#).is_err());
    }
}

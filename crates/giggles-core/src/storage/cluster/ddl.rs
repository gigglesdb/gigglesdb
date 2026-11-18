//! DDL across gateways (GD-0075 Phase 11, R5-100; design in r5/p11/ddl-design.md).
//!
//! Membership. With a member list (`ClusterConfig::gateways`, this gateway included), a gateway
//! serves only while its liveness is valid. Every `gateway_lease / 4` its probe thread asks for
//! its liveness key `Key(node)` on `GATEWAY_LOCK_TABLE` Shared under a fresh transaction (one
//! slice, no deadlock wait, a constant priority: no oracle call) and releases it at once. A
//! granted probe makes the gateway valid until the probe's send time plus `gateway_lease`.
//! Nobody holds the key between probes, so a DDL that evicts the gateway gets it Exclusive at
//! once and holds it to its end; every probe then fails and the gateway fences when its
//! validity ends. Range locks, never rows: nothing is persisted.
//!
//! Every request that can decide a commit (each single-range commit attempt, each prepare, the
//! decision write) first re-checks the validity (`gate`), so nothing is decided after it ends.
//! Fencing marks every live transaction aborted, refuses new statements (40001, "gateway is
//! fenced; retry") and releases this gateway's proxies; it does not wait. The gateway answers
//! a close without a proxy only after its lock lease drained and in-flight commits returned;
//! it rejoins only once its in-flight commit calls have returned.
//! Joining probes under a transaction holding `DDL_LOCK_TABLE` Shared, so a gateway rejoins only
//! after the DDL that evicted it ended; its later statements take oracle timestamps after the
//! join, so the P7-1 generation-token check reloads any catalog committed meanwhile.
//!
//! A DDL transaction D on gateway G takes `DDL_LOCK_TABLE` Exclusive before its first local
//! `Table(0)` lock (`begin_ddl`), then closes every other member before each DDL statement's
//! final snapshot and again for every table its catalog write locks AccessExclusive
//! (`close_ddl`): the member runs a proxy that takes, in its own lock manager and as D,
//! `Table(0)` and those tables AccessExclusive, so its writers and the readers of those tables
//! drain and no new ones start until D ends. Members that cannot be asked for a second are
//! evicted together. When D ends, `end_ddl` reopens the members before D's range locks go
//! (after a settle time when D's commit outcome is unknown). A proxy's watchdog thread asks G
//! whether D still runs. Without a member list none of this runs.
use super::{ClusterTxnServices, FAILOVER_WINDOW, TxnState};
use crate::range_api::{
    ClusterTxnId, LockRequest, LockTarget, RangeError, RangeId, RangeLockMode, RangeSpace,
    Timestamp, WaitSite, range_key,
};
use crate::storage::engine::{
    locks::{LockError, LockKey, LockManager, LockMode, TxnId},
    txn::{SerializationReason, TxnError},
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak, atomic::Ordering},
    thread,
    time::{Duration, Instant},
};

/// The cluster's DDL lock: `TableSentinel` Exclusive under a DDL transaction (one DDL at a
/// time), Shared while a gateway joins. A lock target only; the table has no rows.
pub(crate) const DDL_LOCK_TABLE: u64 = u64::MAX - 5;
/// Gateway liveness keys: `Key { Rows, GATEWAY_LOCK_TABLE BE8 || node BE8 }`.
pub(crate) const GATEWAY_LOCK_TABLE: u64 = u64::MAX - 6;
/// Priority of probe and join requests: they never register a detector wait, and a constant
/// keeps them independent of the oracle (a system-range failover must not fence everyone).
const MEMBER_PRIORITY: Timestamp = Timestamp(u64::MAX);
/// A member whose close calls keep failing this long is evicted.
const EVICT_AFTER: Duration = Duration::from_secs(1);
/// Extra scheduling margin beyond the gateway validity and range lock lease.
const EVICT_MARGIN: Duration = Duration::from_millis(200);
/// How long one close call waits on the member before answering Pending.
const CLOSE_WAIT: Duration = Duration::from_millis(200);
/// Total time `open_ddl` spends reopening members (the proxies' watchdog covers the rest).
const OPEN_BUDGET: Duration = Duration::from_secs(2);
/// How often the watchdog asks each proxy's DDL gateway whether the DDL still runs.
const WATCHDOG: Duration = Duration::from_secs(1);
/// Longest wait for the catalog refresh before a proxy's locks are released.
const REFRESH_WAIT: Duration = Duration::from_secs(1);
/// How long an opened DDL is remembered, so a late close does not start a new proxy.
const OPENED_MEMORY: Duration = Duration::from_secs(60);

/// The refusal of a fenced gateway (40001: clients retry and keep their sessions).
fn fenced() -> TxnError {
    TxnError::Serialization(SerializationReason::GatewayFenced)
}

/// The DDL lock's target.
pub(crate) fn ddl_lock_target() -> LockTarget {
    LockTarget::TableSentinel {
        table: DDL_LOCK_TABLE,
    }
}

/// Gateway `node`'s liveness key.
pub(crate) fn liveness_target(node: u16) -> LockTarget {
    let mut key = GATEWAY_LOCK_TABLE.to_be_bytes().to_vec();
    key.extend_from_slice(&u64::from(node).to_be_bytes());
    LockTarget::Key {
        space: RangeSpace::Rows,
        key,
    }
}

/// The range key that routes a lock target.
pub(crate) fn route(target: &LockTarget) -> Vec<u8> {
    match target {
        LockTarget::Key { space, key } => range_key(*space, key),
        LockTarget::TableSentinel { table } => range_key(RangeSpace::Rows, &table.to_be_bytes()),
        // Gap locks (Phase 8) route by their start, as the gateway's lock path does.
        LockTarget::Interval { space, start, .. } => match start {
            std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => {
                range_key(*space, key)
            }
            std::ops::Bound::Unbounded => range_key(*space, &[]),
        },
    }
}

/// Where a gateway stands in the cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// No member list: always serving, never fenced, closes nobody and is closed by nobody.
    Single,
    /// Not joined yet: serves nothing.
    Joining,
    /// Joined: serves while its validity lasts.
    Serving,
    /// Lost its liveness: serves nothing until it has rejoined.
    Fenced,
}

/// A DDL transaction's state (in `TxnState`).
#[derive(Default)]
pub(super) struct DdlTxn {
    /// Members evicted (their liveness key is held Exclusive): never asked again.
    evicted: BTreeSet<u16>,
    /// Members reached by a close, including proxies still draining.
    closed: BTreeSet<u16>,
    release_deferred: bool,
    /// The commit's outcome is unknown: reopen only after it settled.
    pub(super) ambiguous: bool,
}

type MemberSource = dyn Fn() -> Result<Vec<u16>, RangeError> + Send + Sync;

pub(super) struct Membership {
    member_source: Mutex<Option<Arc<MemberSource>>>,
    state: Mutex<MemberState>,
    join: Mutex<()>,
    changed: Condvar,
    /// Proxies this gateway runs for other gateways' DDL transactions.
    proxies: Mutex<HashMap<ClusterTxnId, Arc<Proxy>>>,
    /// DDL transactions opened here recently (no new proxy for a late close).
    opened: Mutex<HashMap<ClusterTxnId, Instant>>,
    /// Refreshes this gateway's catalog at a fresh timestamp (set at attach).
    refresh: OnceLock<Box<dyn Fn() + Send + Sync>>,
}

struct MemberState {
    phase: Phase,
    /// The gateway may serve until then (Serving only).
    validity: Instant,
    /// Successful joins so far: a statement admitted under one join never runs in the next.
    joins: u64,
    /// Commit calls running now.
    inflight: usize,
    fenced_at: Option<Instant>,
    fences: u64,
}

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

impl Membership {
    pub(super) fn new(members: bool) -> Self {
        Self {
            state: Mutex::new(MemberState {
                phase: if members {
                    Phase::Joining
                } else {
                    Phase::Single
                },
                validity: Instant::now(),
                joins: 0,
                inflight: 0,
                fenced_at: None,
                fences: 0,
            }),
            join: Mutex::new(()),
            changed: Condvar::new(),
            proxies: Mutex::new(HashMap::new()),
            opened: Mutex::new(HashMap::new()),
            refresh: OnceLock::new(),
            member_source: Mutex::new(None),
        }
    }
    fn lock(&self) -> MutexGuard<'_, MemberState> {
        guard(&self.state)
    }
}

impl MemberState {
    /// The join a statement may run under now; 40001 once the gateway does not serve.
    fn serving(&self) -> Result<u64, TxnError> {
        match self.phase {
            Phase::Single => Ok(self.joins),
            Phase::Serving if Instant::now() < self.validity => Ok(self.joins),
            _ => Err(fenced()),
        }
    }
}

/// A running commit call, counted until dropped.
pub(super) struct InflightCommit<'a>(&'a Membership);
impl Drop for InflightCommit<'_> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.inflight -= 1;
        self.0.changed.notify_all();
    }
}

/// A proxy for another gateway's DDL transaction (its id is the proxy's lock owner):
/// AccessExclusive locks in this gateway's lock manager, `Table(0)` first, then tables in id
/// order.
struct Proxy {
    state: Mutex<ProxyState>,
    changed: Condvar,
}

struct ProxyState {
    wanted: BTreeSet<u64>,
    /// Granted tables; 0 is `Table(0)`.
    granted: BTreeSet<u64>,
    failed: Option<LockError>,
    open: bool,
    /// Watchdog: when the DDL's gateway last answered, and failed asks since.
    answered: Instant,
    misses: u32,
}

impl ProxyState {
    fn next(&self) -> Option<u64> {
        std::iter::once(0)
            .chain(self.wanted.iter().copied())
            .find(|table| !self.granted.contains(table))
    }
}

/// Takes the proxy's locks one at a time until it is opened or a wait fails (a deadlock
/// victim's wait is cancelled by the detector through `cancel_wait`).
fn run_proxy(locks: Arc<LockManager>, txn: TxnId, proxy: Arc<Proxy>) {
    loop {
        let table = {
            let mut state = guard(&proxy.state);
            loop {
                if state.open || state.failed.is_some() {
                    return;
                }
                if let Some(table) = state.next() {
                    break table;
                }
                state = proxy.changed.wait(state).unwrap_or_else(|p| p.into_inner());
            }
        };
        let result = locks.acquire(
            txn,
            LockKey::Table(table),
            LockMode::AccessExclusive,
            None,
            &|| guard(&proxy.state).open,
        );
        let mut state = guard(&proxy.state);
        if state.open {
            // Opened while this grant raced `proxy_open`'s release: give it back.
            drop(state);
            locks.release_all(txn);
            return;
        }
        match result {
            Ok(()) => {
                state.granted.insert(table);
            }
            Err(error) => state.failed = Some(error),
        }
        proxy.changed.notify_all();
    }
}

impl ClusterTxnServices {
    /// This gateway's membership phase.
    pub(crate) fn membership_phase(&self) -> Phase {
        self.membership.lock().phase
    }

    /// Ok (with the current join) while this gateway may serve statements.
    /// Tries one bounded rejoin before refusing a new statement.
    pub(super) fn check_serving(&self) -> Result<u64, TxnError> {
        if let Ok(joined) = self.membership.lock().serving() {
            return Ok(joined);
        }
        self.join_once();
        self.membership.lock().serving()
    }

    pub(crate) fn membership_fences(&self) -> u64 {
        self.membership.lock().fences
    }

    #[cfg(test)]
    pub(crate) fn stop_background_for_test(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Asked before every request that can decide a commit: refused once the validity ended.
    pub(super) fn gate(&self) -> Result<(), RangeError> {
        match self.membership.lock().serving() {
            Ok(_) => Ok(()),
            Err(_) => Err(RangeError::Aborted("gateway is fenced; retry".into())),
        }
    }

    /// Runs `f` on `txn`'s state if the gateway still serves under join `joined` and `txn`
    /// was not live across a fence (40001 otherwise). Ordered against `fence` marking.
    pub(super) fn admit<T>(
        &self,
        txn: TxnId,
        joined: u64,
        f: impl FnOnce(&mut TxnState) -> T,
    ) -> Result<T, TxnError> {
        let membership = self.membership.lock();
        if membership.serving()? != joined {
            return Err(fenced());
        }
        let result = self.state(txn, |state| (!state.fenced).then(|| f(state)));
        drop(membership);
        result.ok_or_else(fenced)
    }

    /// Counts a commit call while the gateway serves; refused otherwise. Taken before the
    /// transaction's own checks, so a fence that marks it aborted sees it in flight.
    pub(super) fn begin_commit(&self) -> Result<InflightCommit<'_>, TxnError> {
        let mut state = self.membership.lock();
        state.serving()?;
        state.inflight += 1;
        Ok(InflightCommit(&self.membership))
    }

    /// Sets how this gateway refreshes its catalog (called at attach).
    pub(crate) fn set_catalog_refresh(&self, refresh: Box<dyn Fn() + Send + Sync>) {
        let _ = self.membership.refresh.set(refresh);
    }

    /// Joins the cluster before the gateway serves (attach), then starts the probe thread and
    /// the proxies' watchdog thread. A no-op without a member list. Fails after
    /// `lock_backstop`.
    pub(crate) fn join_cluster(self: &Arc<Self>) -> Result<(), TxnError> {
        if self.membership_phase() == Phase::Single {
            return Ok(());
        }
        let deadline = Instant::now() + self.config.lock_backstop;
        while !self.join_once() {
            if Instant::now() >= deadline {
                return Err(TxnError::LockTimeout);
            }
            thread::sleep(Duration::from_millis(10));
        }
        let period = self.member_wait();
        let threads: [(&str, Duration, Step); 2] = [
            ("gateway-probe", period, Self::probe_step),
            ("gateway-ddl-watchdog", WATCHDOG, |services| {
                services.watchdog();
                WATCHDOG
            }),
        ];
        for (name, period, step) in threads {
            let weak = Arc::downgrade(self);
            thread::Builder::new()
                .name(name.into())
                .spawn(move || every(weak, period, step))
                .expect("spawn gateway membership thread");
        }
        Ok(())
    }

    /// How long one probe or join request waits: a quarter of the lease.
    fn member_wait(&self) -> Duration {
        (self.config.gateway_lease / 4).max(Duration::from_millis(1))
    }

    /// Retries a membership lock until `deadline`, retaining the successful SEND time:
    /// retry time must never extend the lease that this probe actually proves.
    fn member_lock(
        &self,
        txn: ClusterTxnId,
        target: LockTarget,
        deadline: Instant,
    ) -> Result<(RangeId, Instant), RangeError> {
        let key = route(&target);
        let mut delay = Duration::from_millis(1);
        let resolve = |delay: &mut Duration| loop {
            match self.client.descriptor_for(&key) {
                Ok(descriptor) => return Ok(descriptor),
                Err(error @ (RangeError::NotLeader { .. } | RangeError::Unavailable(_))) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(error);
                    }
                    thread::sleep((*delay).min(left));
                    *delay = (*delay * 2).min(Duration::from_millis(50));
                }
                Err(error) => return Err(error),
            }
        };
        let mut descriptor = resolve(&mut delay)?;
        loop {
            let sent = Instant::now();
            if sent >= deadline {
                return Err(RangeError::LockTimeout);
            }
            let wait = (deadline - sent)
                .as_millis()
                .max(1)
                .try_into()
                .unwrap_or(u64::MAX);
            let error = match self.client.lock(LockRequest {
                range: descriptor.id,
                epoch: descriptor.epoch,
                txn,
                priority: MEMBER_PRIORITY,
                target: target.clone(),
                mode: RangeLockMode::Shared,
                timeout_ms: Some(wait),
            }) {
                Ok(_) => return Ok((descriptor.id, sent)),
                Err(error) => error,
            };
            // A transport failure may have left a lease behind.
            let _ = self.client.release(descriptor.id, txn);
            match error {
                RangeError::StaleEpoch { descriptor: holder } => {
                    self.client.invalidate(&descriptor);
                    descriptor = if holder.contains(&key) {
                        self.client.learn(&holder);
                        holder
                    } else {
                        // The parent id may now name only the left child. Resolve the
                        // key again rather than using a holder that cannot grant it.
                        resolve(&mut delay)?
                    };
                }
                error @ (RangeError::NotLeader { .. } | RangeError::Unavailable(_)) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(error);
                    }
                    thread::sleep(delay.min(left));
                    delay = (delay * 2).min(Duration::from_millis(50));
                    if let Some(current) = self.reroute(&key, &descriptor) {
                        descriptor = current;
                    }
                }
                error => return Err(error),
            }
        }
    }

    /// One join attempt, once no commit call of a fenced past is in flight: under a
    /// join transaction, the DDL lock Shared (no DDL runs) and a probe of this gateway's liveness
    /// key; then Serving, then the transaction ends.
    fn join_once(&self) -> bool {
        let deadline = Instant::now() + self.member_wait();
        // Admission and the probe thread share one bounded join, including mutex wait.
        let _join = loop {
            match self.membership.join.try_lock() {
                Ok(join) => break join,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return false;
                    }
                    thread::sleep(left.min(Duration::from_millis(1)));
                }
            }
        };
        if self.membership.lock().serving().is_ok() {
            return true;
        }
        // A statement may notice expired validity before the next probe does.
        if self.membership_phase() == Phase::Serving {
            self.fence();
        }
        if self.membership.lock().inflight > 0 {
            return false;
        }
        let join = ClusterTxnId(self.next_txn_id().0);
        let Ok((ddl_range, _)) = self.member_lock(join, ddl_lock_target(), deadline) else {
            return false;
        };
        // A retry releases its transaction's range locks. Use a separate probe owner so
        // retrying liveness cannot accidentally release the join's DDL lock.
        let probe_id = ClusterTxnId(self.next_txn_id().0);
        let probe = self.member_lock(probe_id, liveness_target(self.config.node), deadline);
        // A split/leader change may have dropped the earlier DDL grant too. Reconfirm it
        // before Serving, while the liveness probe remains Shared.
        let confirmed = if probe.is_ok() {
            self.member_lock(join, ddl_lock_target(), deadline).ok()
        } else {
            None
        };
        let mut joined = false;
        if let Ok((range, sent)) = probe {
            // Serving before the DDL lock goes: a DDL that starts next closes this gateway.
            let mut state = self.membership.lock();
            if confirmed.is_some() && Instant::now() < sent + self.config.gateway_lease {
                joined = true;
                state.phase = Phase::Serving;
                state.validity = sent + self.config.gateway_lease;
                state.joins += 1;
            }
            self.membership.changed.notify_all();
            drop(state);
            let _ = self.client.release(range, probe_id);
        }
        let _ = self.client.release(ddl_range, join);
        // The reconfirmation may have routed into a new child.
        if let Some((range, _)) = confirmed
            && range != ddl_range
        {
            let _ = self.client.release(range, join);
        }
        joined
    }

    /// One renewal probe. Once the validity has passed (granted or not) the gateway fences:
    /// it may have been evicted meanwhile.
    fn renew(&self) -> bool {
        // An old renewal must not overwrite the validity of a statement's new join.
        let _join = guard(&self.membership.join);
        if self.membership_phase() != Phase::Serving {
            return false;
        }
        let probe = ClusterTxnId(self.next_txn_id().0);
        let deadline = (Instant::now() + self.member_wait()).min(self.membership.lock().validity);
        let granted = self.member_lock(probe, liveness_target(self.config.node), deadline);
        if let Ok((range, _)) = granted {
            let _ = self.client.release(range, probe);
        }
        let mut state = self.membership.lock();
        if state.phase != Phase::Serving {
            return false;
        }
        if Instant::now() >= state.validity {
            drop(state);
            self.fence();
        } else if let Ok((_, sent)) = granted {
            state.validity = sent + self.config.gateway_lease;
            return true;
        }
        false
    }

    /// Stops serving: every live transaction is marked aborted and fenced (each `gate` now
    /// refuses), and the proxies are released. Never waits; rejoining waits for the drain.
    fn fence(&self) {
        {
            let mut state = self.membership.lock();
            if state.phase != Phase::Serving {
                return;
            }
            state.phase = Phase::Fenced;
            state.fences += 1;
            state.fenced_at = Some(Instant::now());
            for txn in guard(&self.txns).values_mut() {
                txn.aborted = true;
                txn.fenced = true;
            }
            self.membership.changed.notify_all();
        }
        self.leave();
    }

    /// Releases every proxy (fence, or the gateway is dropped).
    pub(super) fn leave(&self) {
        let proxies: Vec<ClusterTxnId> = guard(&self.membership.proxies).keys().copied().collect();
        for txn in proxies {
            self.release_proxy(txn, false);
        }
    }

    fn probe_step(&self) -> Duration {
        let granted = match self.membership_phase() {
            Phase::Serving => self.renew(),
            Phase::Joining | Phase::Fenced => self.join_once(),
            Phase::Single => true,
        };
        if granted {
            self.member_wait()
        } else {
            self.member_wait().min(Duration::from_millis(50))
        }
    }

    /// Asks each proxy's DDL gateway whether the DDL still runs: no -> reopen; no answer
    /// twice and for `gateway_lease` -> fence (the DDL may commit unseen).
    fn watchdog(&self) {
        let Some(peer) = &self.peer else {
            return;
        };
        let proxies: Vec<_> = guard(&self.membership.proxies)
            .iter()
            .map(|(txn, proxy)| (*txn, proxy.clone()))
            .collect();
        for (txn, proxy) in proxies {
            let live = peer.ddl_live(u64::from(txn.node()), txn);
            let mut state = guard(&proxy.state);
            match live {
                Ok(true) => {
                    state.answered = Instant::now();
                    state.misses = 0;
                }
                Ok(false) => {
                    drop(state);
                    self.release_proxy(txn, true);
                }
                Err(_) => {
                    state.misses += 1;
                    if state.misses >= 2 && state.answered.elapsed() >= self.config.gateway_lease {
                        drop(state);
                        self.fence();
                        return;
                    }
                }
            }
        }
    }

    // ---- The receiving gateway (`GatewayService::ddl_*`). ----

    /// Closes this gateway for DDL transaction `txn` on `tables` (adds them to its proxy):
    /// true once every lock is granted, false while still waiting (after `CLOSE_WAIT`). A
    /// fenced gateway is closed without a proxy once its lock lease drained and in-flight
    /// commits returned (it rejoins only after the DDL); so is one that never joined,
    /// and one in single mode.
    pub(super) fn proxy_close(
        &self,
        txn: ClusterTxnId,
        tables: &[u64],
    ) -> Result<bool, RangeError> {
        let deadline = Instant::now() + CLOSE_WAIT;
        {
            let mut state = self.membership.lock();
            match state.phase {
                Phase::Serving => {}
                Phase::Joining | Phase::Single => return Ok(true),
                Phase::Fenced => loop {
                    if state.inflight == 0
                        && state
                            .fenced_at
                            .is_some_and(|at| at.elapsed() >= self.config.lock_lease)
                    {
                        return Ok(true);
                    }
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Ok(false);
                    }
                    state = self
                        .membership
                        .changed
                        .wait_timeout(state, left)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                    if state.phase != Phase::Fenced {
                        break;
                    }
                },
            }
        }
        {
            let mut opened = guard(&self.membership.opened);
            opened.retain(|_, at| at.elapsed() < OPENED_MEMORY);
            if opened.contains_key(&txn) {
                // A late close of a DDL that already ended here.
                return Ok(true);
            }
        }
        let (proxy, created) = {
            let mut proxies = guard(&self.membership.proxies);
            match proxies.get(&txn) {
                Some(proxy) => (proxy.clone(), false),
                None => {
                    let proxy = Arc::new(Proxy {
                        state: Mutex::new(ProxyState {
                            wanted: BTreeSet::new(),
                            granted: BTreeSet::new(),
                            failed: None,
                            open: false,
                            answered: Instant::now(),
                            misses: 0,
                        }),
                        changed: Condvar::new(),
                    });
                    proxies.insert(txn, proxy.clone());
                    (proxy, true)
                }
            }
        };
        if created {
            let (locks, runner) = (self.gateway.clone(), proxy.clone());
            let spawned = thread::Builder::new()
                .name("gateway-ddl-proxy".into())
                .spawn(move || run_proxy(locks, TxnId(txn.0), runner));
            if let Err(error) = spawned {
                guard(&self.membership.proxies).remove(&txn);
                return Err(RangeError::Unavailable(format!("no proxy thread: {error}")));
            }
        }
        let mut state = guard(&proxy.state);
        state.wanted.extend(tables);
        proxy.changed.notify_all();
        loop {
            match state.failed {
                Some(LockError::Deadlock) => return Err(RangeError::Deadlock),
                Some(error) => return Err(RangeError::Unavailable(format!("{error:?}"))),
                None => {}
            }
            if state.next().is_none() {
                return Ok(true);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(false);
            }
            state = proxy
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }

    /// The DDL `txn` ended: refreshes the catalog (bounded, best effort), then releases its
    /// proxy's locks. Idempotent.
    pub(super) fn proxy_open(&self, txn: ClusterTxnId) {
        self.release_proxy(txn, true);
    }

    fn release_proxy(&self, txn: ClusterTxnId, refresh: bool) {
        guard(&self.membership.opened).insert(txn, Instant::now());
        let Some(proxy) = guard(&self.membership.proxies).remove(&txn) else {
            return;
        };
        guard(&proxy.state).open = true;
        proxy.changed.notify_all();
        if refresh && self.membership.refresh.get().is_some() {
            // The first statements after the release then find the new catalog loaded. Only
            // an optimization: each statement checks the generation token at its snapshot.
            let (done, wait) = std::sync::mpsc::channel();
            let weak = self.endpoint.services.get().cloned();
            let _ = thread::Builder::new()
                .name("gateway-catalog-refresh".into())
                .spawn(move || {
                    if let Some(services) = weak.as_ref().and_then(Weak::upgrade)
                        && let Some(refresh) = services.membership.refresh.get()
                    {
                        refresh();
                    }
                    let _ = done.send(());
                });
            let _ = wait.recv_timeout(REFRESH_WAIT);
        }
        self.gateway.release_all(TxnId(txn.0));
    }

    /// Whether DDL transaction `txn` still runs here (or its unknown outcome still settles).
    pub(super) fn ddl_live(&self, txn: ClusterTxnId) -> bool {
        guard(&self.txns)
            .get(&TxnId(txn.0))
            .is_some_and(|state| state.ddl.is_some())
    }

    // ---- The DDL's own gateway. ----

    /// First DDL statement of `txn`: the cluster's DDL lock Exclusive (before the local
    /// `Table(0)` when this is the transaction's first write; with earlier DML the transaction
    /// holds `Table(0)` RowExclusive while it waits, and the deadlock detector breaks a cycle).
    pub(super) fn begin_ddl(&self, txn: TxnId, cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        self.check_serving()?;
        if self.membership_phase() == Phase::Single || self.state(txn, |s| s.ddl.is_some()) {
            return Ok(());
        }
        let target = ddl_lock_target();
        self.lock_in_range(
            txn,
            &route(&target),
            target,
            RangeLockMode::Exclusive,
            None,
            cancel,
        )?;
        self.state(txn, |state| state.ddl = Some(DdlTxn::default()));
        Ok(())
    }

    /// Supplies additional gateways to close, read after taking the cluster DDL lock.
    pub(crate) fn set_member_source(&self, source: Box<MemberSource>) {
        *guard(&self.membership.member_source) = Some(Arc::from(source));
    }

    /// The other members a DDL transaction closes (not evicted ones).
    fn others(&self, members: &BTreeSet<u16>, evicted: &BTreeSet<u16>) -> Vec<u16> {
        members
            .iter()
            .copied()
            .filter(|n| *n != self.config.node && !evicted.contains(n))
            .collect()
    }

    /// Closes every other member for `tables` (a DDL statement's AccessExclusive tables).
    /// Waits with a deadlock-detector wait at the pending gateways. A no-op for a transaction
    /// that did not `begin_ddl` (single mode).
    pub(super) fn close_ddl(
        &self,
        txn: TxnId,
        tables: &[u64],
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        if !self.state(txn, |s| s.ddl.is_some()) {
            return Ok(());
        }
        // Snapshot the source outside its mutex and outside transaction state locks. The
        // DDL lock is already held, serializing this read with other DDL and joins.
        let source = guard(&self.membership.member_source).clone();
        let mut members: BTreeSet<_> = self.config.gateways.iter().copied().collect();
        if let Some(source) = source {
            members.extend(source().map_err(|_| fenced())?);
        }
        if self.others(&members, &BTreeSet::new()).is_empty() {
            return Ok(());
        }
        let peer = self.peer.clone().ok_or(TxnError::Unsupported)?;
        let id = ClusterTxnId(txn.0);
        let priority = self.priority(txn)?;
        let deadline = Instant::now() + self.config.lock_backstop;
        let mut failing: HashMap<u16, Instant> = HashMap::new();
        let mut wait: Option<(Vec<u16>, super::super::coordinator::deadlock::WaitGuard)> = None;
        loop {
            self.check_live(txn)?;
            if cancel() {
                return Err(TxnError::Cancelled);
            }
            if wait.as_ref().is_some_and(|(_, guard)| guard.was_victim()) {
                return Err(TxnError::Deadlock);
            }
            let others = self.state(txn, |state| {
                state
                    .ddl
                    .as_ref()
                    .map_or_else(Vec::new, |ddl| self.others(&members, &ddl.evicted))
            });
            let (mut pending, mut evict) = (Vec::new(), Vec::new());
            for node in others {
                match peer.ddl_close(u64::from(node), id, priority, tables) {
                    Ok(closed) => {
                        self.state(txn, |state| {
                            if let Some(ddl) = state.ddl.as_mut() {
                                ddl.closed.insert(node);
                            }
                        });
                        failing.remove(&node);
                        if !closed {
                            pending.push(node);
                        }
                    }
                    Err(RangeError::Deadlock) => return Err(TxnError::Deadlock),
                    Err(_) => {
                        if failing.entry(node).or_insert_with(Instant::now).elapsed() >= EVICT_AFTER
                        {
                            evict.push(node);
                        } else {
                            pending.push(node);
                        }
                    }
                }
            }
            if !evict.is_empty() {
                self.evict(txn, &evict, cancel)?;
            }
            if pending.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TxnError::LockTimeout);
            }
            if let Some(detector) = &self.detector
                && wait.as_ref().is_none_or(|(sites, _)| *sites != pending)
            {
                // A victim mark on the wait being replaced still counts.
                if wait.as_ref().is_some_and(|(_, guard)| guard.was_victim()) {
                    return Err(TxnError::Deadlock);
                }
                let sites = pending
                    .iter()
                    .map(|node| WaitSite::Gateway(u64::from(*node)))
                    .collect();
                wait = Some((pending.clone(), detector.begin_wait(id, priority, sites)));
            }
            if pending.iter().all(|node| failing.contains_key(node)) {
                // Failed calls return at once: do not spin.
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    /// Evicts members that cannot be asked: each one's liveness key Exclusive, held until the
    /// DDL ends, so every probe of it fails; then one wait for their validity (their last
    /// granted probe's send time plus the lease, before these grants) plus one commit
    /// lock lease and a transit margin. Heartbeats stop as soon as validity lapses.
    /// Remaining assumption: a request (including a heartbeat) is processed within
    /// `lock_lease` of its send; requests cannot remain queued indefinitely.
    /// A range leader change that loses one of these locks lets that member renew, but then
    /// the DDL's commit fails the lock-only check (40001): it never commits past a member it
    /// neither closed nor evicted.
    fn evict(&self, txn: TxnId, nodes: &[u16], cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        for node in nodes {
            let target = liveness_target(*node);
            self.lock_in_range(
                txn,
                &route(&target),
                target,
                RangeLockMode::Exclusive,
                None,
                cancel,
            )?;
        }
        let until =
            Instant::now() + self.config.gateway_lease + self.config.lock_lease + EVICT_MARGIN;
        while let Some(left) = until.checked_duration_since(Instant::now()) {
            if cancel() {
                return Err(TxnError::Cancelled);
            }
            thread::sleep(left.min(Duration::from_millis(50)));
        }
        self.state(txn, |state| {
            if let Some(ddl) = state.ddl.as_mut() {
                ddl.evicted.extend(nodes);
            }
        });
        Ok(())
    }

    /// Retain the WHOLE transaction while an ambiguous DDL commit settles: local locks,
    /// range locks, heartbeats, and the txns entry used by `ddl_live`.
    pub(super) fn defer_ddl_release(&self, txn: TxnId) -> bool {
        {
            let mut txns = guard(&self.txns);
            let Some(ddl) = txns.get_mut(&txn).and_then(|state| state.ddl.as_mut()) else {
                return false;
            };
            if !ddl.ambiguous {
                return false;
            }
            if ddl.release_deferred {
                return true;
            }
            ddl.release_deferred = true;
        }
        let services = self.endpoint.services().expect("live gateway services");
        let settle = FAILOVER_WINDOW + self.config.gateway_lease;
        let spawned = thread::Builder::new()
            .name("gateway-ddl-settle".into())
            .spawn(move || {
                thread::sleep(settle);
                services.finish_release(txn);
            });
        if spawned.is_err() {
            // Thread exhaustion must not release the fence early.
            thread::sleep(settle);
            self.finish_release(txn);
        }
        true
    }

    /// Called after the outcome settled, before releasing the DDL's range locks.
    pub(super) fn end_ddl(&self, txn: TxnId, ddl: DdlTxn) {
        self.open_ddl(txn, &ddl.closed);
    }

    /// Open only members reached by close, in parallel. Peer calls have bounded timeouts;
    /// the shared deadline bounds dispatch, and watchdogs cover failed opens.
    fn open_ddl(&self, txn: TxnId, closed: &BTreeSet<u16>) {
        let Some(peer) = &self.peer else {
            return;
        };
        let deadline = Instant::now() + OPEN_BUDGET;
        thread::scope(|scope| {
            for &node in closed {
                scope.spawn(move || {
                    if Instant::now() < deadline {
                        let _ = peer.ddl_open(u64::from(node), ClusterTxnId(txn.0));
                    }
                });
            }
        });
    }
}

/// One periodic step of a membership thread.
type Step = fn(&ClusterTxnServices) -> Duration;

/// Runs `step` every `period` until the gateway is dropped.
fn every(services: Weak<ClusterTxnServices>, mut period: Duration, step: Step) {
    let Some(stop) = services.upgrade().map(|services| services.stop.clone()) else {
        return;
    };
    let mut waited = Duration::ZERO;
    while !stop.load(Ordering::Acquire) {
        let tick = period.min(Duration::from_millis(50));
        thread::sleep(tick);
        waited += tick;
        if waited < period {
            continue;
        }
        waited = Duration::ZERO;
        let Some(services) = services.upgrade() else {
            return;
        };
        period = step(&services);
    }
}

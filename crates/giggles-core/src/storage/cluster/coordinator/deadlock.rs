//! Distributed deadlock detection by edge-chasing probes (GD-0075 R5-56, design.md 3.2).
//!
//! Single-range cycles stay with the range leader's local wait-for graph. This module finds
//! cross-range and gateway-local cycles: a wait older than `deadlock_timeout` sends probes along
//! wait-for edges (Chandy-Misra-Haas, AND model); a probe that returns to its initiator names the
//! youngest member of the cycle as the victim, whose waits are then cancelled (40P01).
//! A prepared transaction never waits (prepare is its last lock step), so it never appears in a
//! probe path and is never a victim.
// Wired into the gateway later (Track C's ClusterTxnServices).
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::storage::range_api::{
    ClusterTxnId, DeadlockProbe, GatewayPeer, GatewayService, RangeClient, Timestamp, WaitSite,
};

/// Probes whose path reaches this length are dropped.
const MAX_PATH: usize = 64;

/// Detector timing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DeadlockConfig {
    /// A wait this old starts probes (PostgreSQL's deadlock_timeout).
    pub deadlock_timeout: Duration,
    /// Longest any lock wait lasts: [`DeadlockDetector::lock_timeout_ms`] caps every
    /// `LockRequest.timeout_ms` with it, and the range returns `LockTimeout` (55P03).
    pub lock_wait_backstop: Duration,
    /// Scan period of the background thread.
    pub tick: Duration,
}
impl Default for DeadlockConfig {
    fn default() -> Self {
        Self {
            deadlock_timeout: Duration::from_secs(1),
            lock_wait_backstop: Duration::from_secs(300),
            tick: Duration::from_millis(100),
        }
    }
}

/// This node's own LockManager (`WaitSite::Gateway(self)`).
pub(crate) trait LocalLocks: Send + Sync {
    /// Holders and conflicting waiters queued ahead of txn's pending request; empty if not waiting.
    fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId>;
    /// Makes txn's pending request fail as a deadlock; no-op if not waiting.
    fn cancel_wait(&self, txn: ClusterTxnId);
}

struct Wait {
    id: u64,
    priority: Timestamp,
    sites: Vec<WaitSite>,
    since: Instant,
    last_probe: Option<Instant>,
    victim: Arc<AtomicBool>,
}

/// Edge-chasing deadlock detector of one gateway.
pub(crate) struct DeadlockDetector {
    node: u16,
    cfg: DeadlockConfig,
    client: Arc<dyn RangeClient>,
    peer: Arc<dyn GatewayPeer>,
    local: Arc<dyn LocalLocks>,
    waits: Mutex<HashMap<ClusterTxnId, Wait>>,
    next_wait: AtomicU64,
}

/// Ends a registered wait on drop.
pub(crate) struct WaitGuard {
    detector: Arc<DeadlockDetector>,
    txn: ClusterTxnId,
    id: u64,
    victim: Arc<AtomicBool>,
}
impl WaitGuard {
    /// True if this wait was cancelled as a deadlock victim; the gateway maps the lock call's
    /// `RangeError::Deadlock` to 40P01 either way.
    pub fn was_victim(&self) -> bool {
        self.victim.load(Ordering::SeqCst)
    }
}
impl Drop for WaitGuard {
    fn drop(&mut self) {
        let mut waits = self.detector.waits();
        if waits.get(&self.txn).is_some_and(|w| w.id == self.id) {
            waits.remove(&self.txn);
        }
    }
}

/// Stops the background scan thread on drop.
pub(crate) struct DetectorHandle {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}
impl DetectorHandle {
    /// Stops and joins the scan thread.
    pub fn stop(self) {}
}
impl Drop for DetectorHandle {
    fn drop(&mut self) {
        let (flag, cv) = &*self.stop;
        *flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
        cv.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The youngest member (largest priority), ties by the largest id: every initiator of one cycle
/// sees the same members and so picks the same victim.
fn victim_of(path: &[(ClusterTxnId, Timestamp)]) -> Option<ClusterTxnId> {
    path.iter()
        .max_by_key(|(txn, prio)| (*prio, *txn))
        .map(|(txn, _)| *txn)
}

impl DeadlockDetector {
    pub fn new(
        node: u16,
        cfg: DeadlockConfig,
        client: Arc<dyn RangeClient>,
        peer: Arc<dyn GatewayPeer>,
        local: Arc<dyn LocalLocks>,
    ) -> Arc<Self> {
        Arc::new(Self {
            node,
            cfg,
            client,
            peer,
            local,
            waits: Mutex::new(HashMap::new()),
            next_wait: AtomicU64::new(0),
        })
    }

    /// The `timeout_ms` of a lock request: the statement's own lock timeout, capped by
    /// `lock_wait_backstop`, so a wait that no probe resolves still ends with 55P03.
    pub fn lock_timeout_ms(&self, requested: Option<u64>) -> Option<u64> {
        let backstop = u64::try_from(self.cfg.lock_wait_backstop.as_millis()).unwrap_or(u64::MAX);
        Some(requested.map_or(backstop, |r| r.min(backstop)))
    }

    fn waits(&self) -> MutexGuard<'_, HashMap<ClusterTxnId, Wait>> {
        self.waits.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers that `txn` (priority = its start ts) is about to block at `sites`; the guard
    /// ends the wait on drop.
    pub fn begin_wait(
        self: &Arc<Self>,
        txn: ClusterTxnId,
        priority: Timestamp,
        sites: Vec<WaitSite>,
    ) -> WaitGuard {
        let id = self.next_wait.fetch_add(1, Ordering::Relaxed);
        let victim = Arc::new(AtomicBool::new(false));
        let wait = Wait {
            id,
            priority,
            sites,
            since: Instant::now(),
            last_probe: None,
            victim: victim.clone(),
        };
        self.waits().insert(txn, wait);
        WaitGuard {
            detector: self.clone(),
            txn,
            id,
            victim,
        }
    }

    /// One scan: every wait older than deadlock_timeout whose last probe is older than
    /// deadlock_timeout starts probes.
    pub fn tick(&self, now: Instant) {
        let timeout = self.cfg.deadlock_timeout;
        let due: Vec<_> = self
            .waits()
            .iter_mut()
            .filter(|(_, w)| {
                now.saturating_duration_since(w.since) >= timeout
                    && w.last_probe
                        .is_none_or(|t| now.saturating_duration_since(t) >= timeout)
            })
            .map(|(txn, w)| {
                w.last_probe = Some(now);
                (*txn, w.priority, w.sites.clone())
            })
            .collect();
        for (txn, priority, sites) in due {
            for site in sites {
                for target in self.site_blockers(site, txn) {
                    if target != txn {
                        self.deliver_probe(DeadlockProbe {
                            initiator: txn,
                            path: vec![(txn, priority)],
                            target,
                        });
                    }
                }
            }
        }
    }

    /// Background thread calling tick every cfg.tick until the handle is dropped or stopped.
    pub fn spawn(self: &Arc<Self>) -> DetectorHandle {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let (weak, period, signal): (Weak<Self>, _, _) =
            (Arc::downgrade(self), self.cfg.tick, stop.clone());
        let thread = std::thread::Builder::new()
            .name(format!("deadlock-detector-{}", self.node))
            .spawn(move || {
                let (flag, cv) = &*signal;
                let mut stopped = flag.lock().unwrap_or_else(PoisonError::into_inner);
                loop {
                    stopped = cv
                        .wait_timeout(stopped, period)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                    if *stopped {
                        return;
                    }
                    let Some(detector) = weak.upgrade() else {
                        return;
                    };
                    drop(stopped);
                    detector.tick(Instant::now());
                    stopped = flag.lock().unwrap_or_else(PoisonError::into_inner);
                }
            })
            .expect("spawn deadlock detector thread");
        DetectorHandle {
            stop,
            thread: Some(thread),
        }
    }

    fn is_local(&self, node: u64) -> bool {
        node == u64::from(self.node)
    }

    /// Who blocks `txn` at `site`; errors read as "not waiting there".
    fn site_blockers(&self, site: WaitSite, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
        match site {
            WaitSite::Range(range) => self.client.blockers(range, txn).unwrap_or_default(),
            WaitSite::Gateway(node) if self.is_local(node) => self.local.blockers(txn),
            WaitSite::Gateway(node) => self.peer.blockers(node, txn).unwrap_or_default(),
        }
    }

    fn deliver_probe(&self, probe: DeadlockProbe) {
        let node = probe.target.node();
        if node == self.node {
            self.handle_probe(probe);
        } else {
            // Probes are fire-and-forget; a lost one is retried by the initiator's next scan.
            let _ = self.peer.probe(u64::from(node), probe);
        }
    }

    fn deliver_abort(&self, victim: ClusterTxnId) {
        let node = victim.node();
        if node == self.node {
            self.abort_local(victim);
        } else {
            let _ = self.peer.abort(u64::from(node), victim);
        }
    }

    fn handle_probe(&self, probe: DeadlockProbe) {
        let DeadlockProbe {
            initiator,
            mut path,
            target,
        } = probe;
        if path.len() >= MAX_PATH {
            return;
        }
        let Some((priority, sites)) = self
            .waits()
            .get(&target)
            .map(|w| (w.priority, w.sites.clone()))
        else {
            return;
        };
        path.push((target, priority));
        let mut cycle = false;
        for site in sites {
            for blocker in self.site_blockers(site, target) {
                if blocker == initiator {
                    cycle = true;
                } else if blocker != target && !path.iter().any(|(txn, _)| *txn == blocker) {
                    self.deliver_probe(DeadlockProbe {
                        initiator,
                        path: path.clone(),
                        target: blocker,
                    });
                }
            }
        }
        if cycle && let Some(victim) = victim_of(&path) {
            self.deliver_abort(victim);
        }
    }

    fn abort_local(&self, victim: ClusterTxnId) {
        // A transaction that is no longer waiting is never failed.
        let Some(sites) = self.waits().get(&victim).map(|w| {
            w.victim.store(true, Ordering::SeqCst);
            w.sites.clone()
        }) else {
            return;
        };
        for site in sites {
            let _ = match site {
                WaitSite::Range(range) => self.client.cancel_wait(range, victim),
                WaitSite::Gateway(node) if self.is_local(node) => {
                    self.local.cancel_wait(victim);
                    Ok(())
                }
                WaitSite::Gateway(node) => self.peer.cancel_wait(node, victim),
            };
        }
    }
}

impl GatewayService for DeadlockDetector {
    fn probe(&self, probe: DeadlockProbe) {
        self.handle_probe(probe);
    }
    fn abort(&self, victim: ClusterTxnId) {
        self.abort_local(victim);
    }
    fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
        self.local.blockers(txn)
    }
    fn cancel_wait(&self, txn: ClusterTxnId) {
        self.local.cancel_wait(txn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::range_api::{
        Epoch, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, LockTarget, Outcome,
        RangeDescriptor, RangeError, RangeId, RangeLockMode, RangeSpace, RangeWrite, ReadPage,
        ReadRequest, TsOracle, range_key,
    };
    use RangeLockMode::{Exclusive, Shared};
    use std::collections::HashSet;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;

    type Entry = (ClusterTxnId, Vec<u8>, RangeLockMode);

    /// One lock table: holders plus a FIFO of waiters, like a range leader's LockManager.
    #[derive(Default)]
    struct Table {
        st: Mutex<TableState>,
        cv: Condvar,
    }
    #[derive(Default)]
    struct TableState {
        holders: Vec<Entry>,
        waiters: Vec<Entry>,
        cancelled: HashSet<ClusterTxnId>,
    }
    impl TableState {
        fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
            let Some(pos) = self.waiters.iter().position(|w| w.0 == txn) else {
                return Vec::new();
            };
            let (_, key, mode) = &self.waiters[pos];
            let mut out = Vec::new();
            for (t, k, m) in self.holders.iter().chain(&self.waiters[..pos]) {
                let conflict = *m == Exclusive || *mode == Exclusive;
                if *t != txn && k == key && conflict && !out.contains(t) {
                    out.push(*t);
                }
            }
            out
        }
        fn reaches(&self, from: ClusterTxnId, goal: ClusterTxnId, seen: &mut HashSet<u64>) -> bool {
            self.blockers(from)
                .into_iter()
                .any(|b| b == goal || (seen.insert(b.0) && self.reaches(b, goal, seen)))
        }
    }
    impl Table {
        fn state(&self) -> MutexGuard<'_, TableState> {
            self.st.lock().unwrap()
        }
        fn lock(
            &self,
            txn: ClusterTxnId,
            key: Vec<u8>,
            mode: RangeLockMode,
        ) -> Result<(), RangeError> {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut st = self.state();
            st.waiters.push((txn, key.clone(), mode));
            // Stand-in for the leader's local wait-for graph check at enqueue.
            let mut result = if st.reaches(txn, txn, &mut HashSet::new()) {
                Err(RangeError::Deadlock)
            } else {
                loop {
                    if st.cancelled.contains(&txn) {
                        break Err(RangeError::Deadlock);
                    }
                    if st.blockers(txn).is_empty() {
                        break Ok(());
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break Err(RangeError::LockTimeout);
                    }
                    st = self.cv.wait_timeout(st, deadline - now).unwrap().0;
                }
            };
            st.waiters.retain(|w| w.0 != txn);
            if st.cancelled.remove(&txn) {
                result = result.and(Err(RangeError::Deadlock));
            }
            if result.is_ok() {
                st.holders.push((txn, key, mode));
            }
            self.cv.notify_all();
            result
        }
        fn cancel(&self, txn: ClusterTxnId) {
            let mut st = self.state();
            if st.waiters.iter().any(|w| w.0 == txn) {
                st.cancelled.insert(txn);
                self.cv.notify_all();
            }
        }
        fn release(&self, txn: ClusterTxnId) {
            self.state().holders.retain(|h| h.0 != txn);
            self.cv.notify_all();
        }
    }
    impl LocalLocks for Table {
        fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
            self.state().blockers(txn)
        }
        fn cancel_wait(&self, txn: ClusterTxnId) {
            self.cancel(txn);
        }
    }

    /// Range r covers user keys starting with byte r.
    struct FakeClient {
        ranges: Vec<(RangeDescriptor, Table)>,
        blocker_calls: AtomicUsize,
    }
    impl FakeClient {
        fn table(&self, range: RangeId) -> Result<&Table, RangeError> {
            let found = self.ranges.iter().find(|(d, _)| d.id == range);
            found
                .map(|(_, t)| t)
                .ok_or(RangeError::Unavailable("no range".into()))
        }
    }
    impl RangeClient for FakeClient {
        fn descriptor_for(&self, key: &[u8]) -> Result<RangeDescriptor, RangeError> {
            let found = self.ranges.iter().find(|(d, _)| d.contains(key));
            found
                .map(|(d, _)| d.clone())
                .ok_or(RangeError::Unavailable("no range".into()))
        }
        fn invalidate(&self, _: &RangeDescriptor) {}
        fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
            unimplemented!()
        }
        fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
            let key = match req.target {
                LockTarget::Key { key, .. } => key,
                LockTarget::TableSentinel { table } => table.to_be_bytes().to_vec(),
            };
            let table = self.table(req.range)?;
            table.lock(req.txn, key, req.mode).map(|()| LockGrant {
                latest: None,
                generation: 0,
            })
        }
        fn blockers(
            &self,
            range: RangeId,
            txn: ClusterTxnId,
        ) -> Result<Vec<ClusterTxnId>, RangeError> {
            self.blocker_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.table(range)?.state().blockers(txn))
        }
        fn cancel_wait(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
            self.table(range)?.cancel(txn);
            Ok(())
        }
        fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
            self.table(range)?.release(txn);
            Ok(())
        }
        fn heartbeat(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
            unimplemented!()
        }
        fn commit(
            &self,
            _: RangeId,
            _: Epoch,
            _: ClusterTxnId,
            _: Vec<RangeWrite>,
            _: Option<u64>,
            _: Option<u64>,
        ) -> Result<Timestamp, RangeError> {
            unimplemented!()
        }
        fn prepare(
            &self,
            _: RangeId,
            _: Epoch,
            _: ClusterTxnId,
            _: RangeId,
            _: Vec<RangeWrite>,
            _: Option<u64>,
        ) -> Result<Timestamp, RangeError> {
            unimplemented!()
        }
        fn finalize(&self, _: RangeId, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
            unimplemented!()
        }
        fn home(&self, _: RangeId, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
            unimplemented!()
        }
        fn oracle(&self) -> &dyn TsOracle {
            unimplemented!()
        }
        fn lease(&self, _: LeaseKind, _: u64, _: u64) -> Result<std::ops::Range<u64>, RangeError> {
            unimplemented!()
        }
    }

    /// Routes gateway calls to the addressed node's detector, in process.
    #[derive(Default)]
    struct FakePeer {
        nodes: Mutex<HashMap<u64, Weak<DeadlockDetector>>>,
    }
    impl FakePeer {
        fn node(&self, node: u64) -> Result<Arc<DeadlockDetector>, RangeError> {
            let found = self
                .nodes
                .lock()
                .unwrap()
                .get(&node)
                .and_then(Weak::upgrade);
            found.ok_or(RangeError::Unavailable("no node".into()))
        }
    }
    impl GatewayPeer for FakePeer {
        fn probe(&self, node: u64, probe: DeadlockProbe) -> Result<(), RangeError> {
            self.node(node)?.probe(probe);
            Ok(())
        }
        fn abort(&self, node: u64, victim: ClusterTxnId) -> Result<(), RangeError> {
            self.node(node)?.abort(victim);
            Ok(())
        }
        fn blockers(&self, node: u64, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
            Ok(GatewayService::blockers(&*self.node(node)?, txn))
        }
        fn cancel_wait(&self, node: u64, txn: ClusterTxnId) -> Result<(), RangeError> {
            GatewayService::cancel_wait(&*self.node(node)?, txn);
            Ok(())
        }
    }

    struct Cluster {
        client: Arc<FakeClient>,
        locals: HashMap<u16, Arc<Table>>,
        nodes: HashMap<u16, Arc<DeadlockDetector>>,
    }
    fn cluster(nodes: u16, ranges: u8, cfg: DeadlockConfig) -> Cluster {
        let descriptor = |r: u8| RangeDescriptor {
            id: RangeId(r.into()),
            epoch: Epoch(1),
            start: range_key(RangeSpace::Rows, &[r]),
            end: Some(range_key(RangeSpace::Rows, &[r + 1])),
            replicas: Vec::new(),
        };
        let client = Arc::new(FakeClient {
            ranges: (1..=ranges)
                .map(|r| (descriptor(r), Table::default()))
                .collect(),
            blocker_calls: AtomicUsize::new(0),
        });
        let peer = Arc::new(FakePeer::default());
        let (mut locals, mut detectors) = (HashMap::new(), HashMap::new());
        for n in 1..=nodes {
            let local = Arc::new(Table::default());
            let det = DeadlockDetector::new(n, cfg, client.clone(), peer.clone(), local.clone());
            peer.nodes
                .lock()
                .unwrap()
                .insert(n.into(), Arc::downgrade(&det));
            locals.insert(n, local);
            detectors.insert(n, det);
        }
        Cluster {
            client,
            locals,
            nodes: detectors,
        }
    }
    fn fast() -> DeadlockConfig {
        DeadlockConfig {
            deadlock_timeout: Duration::from_millis(30),
            tick: Duration::from_millis(5),
            ..DeadlockConfig::default()
        }
    }

    #[derive(Clone, Copy)]
    enum Step {
        /// Key (range, byte) in a range.
        Key(u8, u8, RangeLockMode),
        /// Exclusive key in node n's own LockManager.
        Gate(u16, u8),
        Sync,
        /// Holds the locks taken so far for a moment.
        Hold,
    }
    /// Runs one transaction on txn.node(); Err(was_victim) when a lock returned Deadlock.
    fn run_txn(
        c: &Cluster,
        txn: ClusterTxnId,
        prio: u64,
        steps: &[Step],
        sync: &Barrier,
    ) -> Result<(), bool> {
        let det = &c.nodes[&txn.node()];
        let priority = Timestamp(prio);
        let mut out = Ok(());
        for step in steps {
            let (res, victim) = match *step {
                Step::Sync => {
                    sync.wait();
                    continue;
                }
                Step::Hold => {
                    std::thread::sleep(Duration::from_micros(500));
                    continue;
                }
                Step::Key(r, k, mode) => {
                    let d = c
                        .client
                        .descriptor_for(&range_key(RangeSpace::Rows, &[r, k]))
                        .unwrap();
                    let guard = det.begin_wait(txn, priority, vec![WaitSite::Range(d.id)]);
                    let target = LockTarget::Key {
                        space: RangeSpace::Rows,
                        key: vec![r, k],
                    };
                    let req = LockRequest {
                        range: d.id,
                        epoch: d.epoch,
                        txn,
                        priority,
                        target,
                        mode,
                        timeout_ms: None,
                    };
                    (c.client.lock(req).map(drop), guard.was_victim())
                }
                Step::Gate(n, k) => {
                    let guard = det.begin_wait(txn, priority, vec![WaitSite::Gateway(n.into())]);
                    (
                        c.locals[&n].lock(txn, vec![k], Exclusive),
                        guard.was_victim(),
                    )
                }
            };
            match res {
                Ok(()) => {}
                Err(RangeError::Deadlock) => {
                    out = Err(victim);
                    break;
                }
                Err(e) => panic!("{txn:?}: {e}"),
            }
        }
        for (d, _) in &c.client.ranges {
            c.client.release(d.id, txn).unwrap();
        }
        c.locals.values().for_each(|t| t.release(txn));
        out
    }
    /// Runs the transactions concurrently with detectors scanning; returns their results.
    fn race(c: &Cluster, txns: &[(ClusterTxnId, u64, Vec<Step>)]) -> Vec<Result<(), bool>> {
        let _scans: Vec<_> = c.nodes.values().map(|d| d.spawn()).collect();
        let sync = Barrier::new(txns.len());
        std::thread::scope(|s| {
            let handles: Vec<_> = txns
                .iter()
                .map(|(txn, prio, steps)| s.spawn(|| run_txn(c, *txn, *prio, steps, &sync)))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    }
    fn id(node: u16, n: u64) -> ClusterTxnId {
        ClusterTxnId::new(node, n)
    }
    /// Exclusive key (range, byte).
    fn x(r: u8, k: u8) -> Step {
        Step::Key(r, k, Exclusive)
    }
    /// Take `first`, wait for every transaction to do the same, then take `second`.
    fn cyc(first: Step, second: Step) -> Vec<Step> {
        vec![first, Step::Sync, second]
    }

    #[test]
    fn two_range_cycle_exactly_one_deadlock() {
        let c = cluster(2, 2, fast());
        let (t1, t2) = (id(1, 1), id(2, 1));
        let res = race(
            &c,
            &[
                (t1, 10, cyc(x(1, 0), x(2, 0))),
                (t2, 20, cyc(x(2, 0), x(1, 0))),
            ],
        );
        assert_eq!(res, vec![Ok(()), Err(true)]);
    }

    #[test]
    fn three_range_cycle_exactly_one_deadlock() {
        let c = cluster(3, 3, fast());
        let res = race(
            &c,
            &[
                (id(1, 1), 10, cyc(x(1, 0), x(2, 0))),
                (id(2, 1), 30, cyc(x(2, 0), Step::Key(3, 0, Shared))),
                (id(3, 1), 20, cyc(x(3, 0), x(1, 0))),
            ],
        );
        assert_eq!(res, vec![Ok(()), Err(true), Ok(())]);
    }

    #[test]
    fn gateway_site_cycle_detected() {
        // T1 waits in node 2's own LockManager (a DDL proxy) for T2, which waits at R1 for T1.
        let c = cluster(2, 1, fast());
        let res = race(
            &c,
            &[
                (id(1, 1), 20, cyc(x(1, 0), Step::Gate(2, 7))),
                (id(2, 1), 10, cyc(Step::Gate(2, 7), x(1, 0))),
            ],
        );
        assert_eq!(res, vec![Err(true), Ok(())]);
    }

    #[test]
    fn no_false_positive_without_cycles() {
        let cfg = DeadlockConfig {
            deadlock_timeout: Duration::from_millis(3),
            tick: Duration::from_millis(1),
            ..DeadlockConfig::default()
        };
        let c = cluster(3, 3, cfg);
        let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
        let mut rand = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut txns = Vec::new();
        for thread in 0..8_u64 {
            let mut list = Vec::new();
            for n in 0..30 {
                // Keys in one global order: (range, byte) ascending.
                let mut steps = Vec::new();
                for k in 0..6_u8 {
                    let mode = if rand() % 3 == 0 { Shared } else { Exclusive };
                    if steps.len() < 3 && rand() % 2 == 0 {
                        steps.push(Step::Key(k / 2 + 1, k % 2, mode));
                    }
                }
                steps.push(Step::Hold);
                let node = (thread % 3 + 1) as u16;
                list.push((id(node, thread * 100 + n), rand() % 1000, steps));
            }
            txns.push(list);
        }
        let scans: Vec<_> = c.nodes.values().map(|d| d.spawn()).collect();
        let sync = Barrier::new(1);
        let results: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = txns
                .iter()
                .map(|list| {
                    let (c, sync) = (&c, &sync);
                    s.spawn(move || {
                        list.iter()
                            .map(|(txn, prio, steps)| run_txn(c, *txn, *prio, steps, sync))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        scans.into_iter().for_each(DetectorHandle::stop);
        assert_eq!(results.len(), 240);
        assert!(
            results.iter().all(Result::is_ok),
            "false deadlock: {results:?}"
        );
        assert!(
            c.client.blocker_calls.load(Ordering::SeqCst) > 0,
            "no probe ever ran"
        );
    }

    #[test]
    fn duplicate_initiators_pick_same_victim() {
        let (a, b, c3) = (id(1, 1), id(2, 1), id(3, 1));
        for path in [
            vec![(a, Timestamp(10)), (b, Timestamp(20)), (c3, Timestamp(5))],
            vec![(c3, Timestamp(5)), (a, Timestamp(10)), (b, Timestamp(20))],
        ] {
            assert_eq!(victim_of(&path), Some(b));
        }
        assert_eq!(victim_of(&[(b, Timestamp(7)), (a, Timestamp(7))]), Some(b));
        assert_eq!(victim_of(&[(a, Timestamp(7)), (b, Timestamp(7))]), Some(b));

        // A static two-range cycle, driven by explicit ticks (no scan threads).
        let cfg = DeadlockConfig::default();
        assert_eq!(cfg.lock_wait_backstop, Duration::from_secs(300));
        let c = cluster(2, 2, cfg);
        let backstop = &c.nodes[&1];
        assert_eq!(backstop.lock_timeout_ms(None), Some(300_000));
        assert_eq!(backstop.lock_timeout_ms(Some(50)), Some(50));
        assert_eq!(backstop.lock_timeout_ms(Some(900_000)), Some(300_000));
        let (t1, t2) = (id(1, 1), id(2, 1));
        let key = |r: u8| vec![r, 0];
        {
            let (mut r1, mut r2) = (c.client.ranges[0].1.state(), c.client.ranges[1].1.state());
            r1.holders.push((t1, key(1), Exclusive));
            r2.holders.push((t2, key(2), Exclusive));
            r2.waiters.push((t1, key(2), Exclusive));
            r1.waiters.push((t2, key(1), Exclusive));
        }
        let (d1, d2) = (&c.nodes[&1], &c.nodes[&2]);
        let g1 = d1.begin_wait(t1, Timestamp(10), vec![WaitSite::Range(RangeId(2))]);
        let g2 = d2.begin_wait(t2, Timestamp(20), vec![WaitSite::Range(RangeId(1))]);
        let now = Instant::now();
        d1.tick(now);
        d2.tick(now);
        assert_eq!(
            c.client.blocker_calls.load(Ordering::SeqCst),
            0,
            "probed before deadlock_timeout"
        );
        let later = now + 2 * cfg.deadlock_timeout;
        let set = |r: usize| Vec::from_iter(c.client.ranges[r].1.state().cancelled.drain());
        let cancelled = || (set(0), set(1));
        d1.tick(later);
        assert!(g2.was_victim() && !g1.was_victim());
        assert_eq!(cancelled(), (vec![t2], vec![]));
        d2.tick(later);
        assert!(!g1.was_victim());
        assert_eq!(cancelled(), (vec![t2], vec![]));
        // A probe from the other initiator, delivered directly, aborts the same victim.
        let calls = c.client.blocker_calls.load(Ordering::SeqCst);
        d2.tick(later); // already probed at `later`: no re-probe yet
        assert_eq!(c.client.blocker_calls.load(Ordering::SeqCst), calls);
        d1.probe(DeadlockProbe {
            initiator: t2,
            path: vec![(t2, Timestamp(20))],
            target: t1,
        });
        assert_eq!(cancelled(), (vec![t2], vec![]));
        // abort of a transaction that is no longer waiting (or unknown) is a no-op.
        drop(g2);
        d2.abort(t2);
        d1.abort(id(1, 99));
        assert_eq!(cancelled(), (vec![], vec![]));
        assert!(!g1.was_victim());
    }

    #[test]
    fn single_range_cycle_ends_via_local_graph() {
        let c = cluster(2, 1, fast());
        let res = race(
            &c,
            &[
                (id(1, 1), 10, cyc(x(1, 0), x(1, 1))),
                (id(2, 1), 20, cyc(x(1, 1), x(1, 0))),
            ],
        );
        // Exactly one deadlock, raised by the leader's local check, not by a detector abort.
        assert_eq!(res.iter().filter(|r| r.is_err()).count(), 1, "{res:?}");
        assert!(res.iter().all(|r| *r != Err(true)), "{res:?}");
    }
}

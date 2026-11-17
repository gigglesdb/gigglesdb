//! Routed range client (GD-0075 R5-23): descriptors from the route cache, refreshed from the
//! system range's directory; calls go in process when the leader is local, otherwise over a
//! `RangeTransport` to the leader hint or a replica, at most `MAX_ATTEMPTS` sends per call.
//! A node the transport could not reach (timeout, no connection) is suspect for `SUSPECT_FOR`:
//! calls go to the other replicas first, so one silently cut-off leader does not hold every
//! caller for a call timeout after its range failed over (GD-0075 Phase 11, P11-5).
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use giggles_core::range_api::{
    ClusterTxnId, Epoch, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, Outcome,
    RangeClient, RangeDescriptor, RangeError, RangeId, RangeService, RangeWrite, ReadPage,
    ReadRequest, SYSTEM_RANGE, SequenceOp, SequenceReply, SystemService, Timestamp, TsOracle,
};

use crate::{directory, route::RouteCache};

/// Attempts per call in total (local, hint, replicas) before `Unavailable`.
pub const MAX_ATTEMPTS: usize = 3;
/// How long a node stays suspect after the transport could not reach it.
pub const SUSPECT_FOR: Duration = Duration::from_secs(3);

/// One range call; system calls (`OracleNext`, `Lease`, `Sequence`) target `SYSTEM_RANGE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeCall {
    Descriptor {
        range: RangeId,
    },
    Read(ReadRequest),
    Lock(LockRequest),
    Blockers {
        range: RangeId,
        txn: ClusterTxnId,
    },
    CancelWait {
        range: RangeId,
        txn: ClusterTxnId,
    },
    Release {
        range: RangeId,
        txn: ClusterTxnId,
    },
    Heartbeat {
        range: RangeId,
        txn: ClusterTxnId,
    },
    Commit {
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        /// Generation of the transaction's grants on the range (None = no lock there).
        lock_generation: Option<u64>,
    },
    Prepare {
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    },
    Finalize {
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    },
    Home {
        range: RangeId,
        epoch: Epoch,
        op: HomeOp,
    },
    OracleNext {
        range: RangeId,
        n: u32,
    },
    Lease {
        range: RangeId,
        kind: LeaseKind,
        id: u64,
        count: u64,
    },
    /// A SQL sequence operation on the system range leader (design.md Changes, Track C (a)).
    Sequence {
        range: RangeId,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    },
}

impl RangeCall {
    /// Target range.
    pub fn range(&self) -> RangeId {
        match self {
            Self::Read(req) => req.range,
            Self::Lock(req) => req.range,
            Self::Descriptor { range }
            | Self::Blockers { range, .. }
            | Self::CancelWait { range, .. }
            | Self::Release { range, .. }
            | Self::Heartbeat { range, .. }
            | Self::Commit { range, .. }
            | Self::Prepare { range, .. }
            | Self::Finalize { range, .. }
            | Self::Home { range, .. }
            | Self::OracleNext { range, .. }
            | Self::Lease { range, .. }
            | Self::Sequence { range, .. } => *range,
        }
    }
    /// Calls carrying the caller's epoch: `StaleEpoch` goes back to the caller.
    pub fn carries_epoch(&self) -> bool {
        matches!(
            self,
            Self::Read(_)
                | Self::Lock(_)
                | Self::Commit { .. }
                | Self::Prepare { .. }
                | Self::Home { .. }
        )
    }
    fn is_system(&self) -> bool {
        matches!(
            self,
            Self::OracleNext { .. } | Self::Lease { .. } | Self::Sequence { .. }
        )
    }
}

/// Result of the matching `RangeCall` variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeReply {
    Descriptor(RangeDescriptor),
    Read(ReadPage),
    Lock(LockGrant),
    Blockers(Vec<ClusterTxnId>),
    CancelWait,
    Release,
    Heartbeat,
    Commit(Timestamp),
    Prepare(Timestamp),
    Finalize,
    Home(HomeReply),
    OracleNext(Timestamp),
    Lease(Range<u64>),
    Sequence(SequenceReply),
}

/// Remote path to a node's range leaders (the peer transport, R5-21).
pub trait RangeTransport: Send + Sync {
    fn call(&self, node: u64, call: RangeCall) -> Result<RangeReply, RangeError>;
    /// Whether `error` (from `call` to `node`) says the transport could not reach the node
    /// (a timeout or no connection), as opposed to an answer from it.
    fn unreachable(&self, node: u64, error: &RangeError) -> bool {
        let _ = (node, error);
        false
    }
}

/// The range leaders on this node.
pub trait LocalRanges: Send + Sync {
    fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>>;
    fn system(&self) -> Option<Arc<dyn SystemService>>;
}

/// Serves a call from this node's leaders; `NotLeader { hint: None }` if it has none for it.
pub fn dispatch(local: &dyn LocalRanges, call: RangeCall) -> Result<RangeReply, RangeError> {
    let not_leader = || RangeError::NotLeader { hint: None };
    let service = |range| local.service(range).ok_or_else(not_leader);
    let system = || local.system().ok_or_else(not_leader);
    Ok(match call {
        RangeCall::Descriptor { range } => RangeReply::Descriptor(service(range)?.descriptor()),
        RangeCall::Read(req) => RangeReply::Read(service(req.range)?.read(req)?),
        RangeCall::Lock(req) => RangeReply::Lock(service(req.range)?.lock(req)?),
        RangeCall::Blockers { range, txn } => RangeReply::Blockers(service(range)?.blockers(txn)?),
        RangeCall::CancelWait { range, txn } => {
            service(range)?.cancel_wait(txn)?;
            RangeReply::CancelWait
        }
        RangeCall::Release { range, txn } => {
            service(range)?.release(txn)?;
            RangeReply::Release
        }
        RangeCall::Heartbeat { range, txn } => {
            service(range)?.heartbeat(txn)?;
            RangeReply::Heartbeat
        }
        RangeCall::Commit {
            range,
            epoch,
            txn,
            writes,
            marker_xid,
            lock_generation,
        } => RangeReply::Commit(service(range)?.commit(
            epoch,
            txn,
            writes,
            marker_xid,
            lock_generation,
        )?),
        RangeCall::Prepare {
            range,
            epoch,
            txn,
            home,
            writes,
            lock_generation,
        } => RangeReply::Prepare(service(range)?.prepare(
            epoch,
            txn,
            home,
            writes,
            lock_generation,
        )?),
        RangeCall::Finalize {
            range,
            txn,
            outcome,
        } => {
            service(range)?.finalize(txn, outcome)?;
            RangeReply::Finalize
        }
        RangeCall::Home { range, epoch, op } => RangeReply::Home(service(range)?.home(epoch, op)?),
        RangeCall::OracleNext { n, .. } => RangeReply::OracleNext(system()?.oracle().next(n)?),
        RangeCall::Lease {
            kind, id, count, ..
        } => RangeReply::Lease(system()?.lease(kind, id, count)?),
        RangeCall::Sequence {
            id,
            incarnation,
            op,
            ..
        } => RangeReply::Sequence(system()?.sequence(id, incarnation, op)?),
    })
}

struct Router {
    node: u64,
    bootstrap: RangeDescriptor,
    transport: Arc<dyn RangeTransport>,
    local: Arc<dyn LocalRanges>,
    cache: Mutex<RouteCache>,
    suspects: Suspects,
}

/// Nodes the transport could not reach recently, each until its suspicion ends.
#[derive(Default)]
struct Suspects(Mutex<HashMap<u64, Instant>>);

impl Suspects {
    fn mark(&self, node: u64, now: Instant) {
        let mut map = self.0.lock().unwrap_or_else(|p| p.into_inner());
        map.insert(node, now + SUSPECT_FOR);
    }
    fn clear(&self, node: u64) {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&node);
    }
    /// The nodes suspect at `now` (expired entries are dropped).
    fn at(&self, now: Instant) -> Vec<u64> {
        let mut map = self.0.lock().unwrap_or_else(|p| p.into_inner());
        map.retain(|_, until| *until > now);
        map.keys().copied().collect()
    }
}

/// Where the next attempt goes: the leader hint unless it is suspect, else the first untried
/// replica that is not suspect, else the first untried one, else the first one. `replicas`
/// are in preference order and exclude this node.
fn choose(hint: Option<u64>, replicas: &[u64], tried: &[u64], suspects: &[u64]) -> Option<u64> {
    if let Some(hint) = hint.filter(|hint| !suspects.contains(hint)) {
        return Some(hint);
    }
    let untried = || replicas.iter().copied().filter(|n| !tried.contains(n));
    untried()
        .find(|n| !suspects.contains(n))
        .or_else(|| untried().next())
        .or_else(|| replicas.first().copied())
}

impl Router {
    /// Locks the cache for one statement; never held across a call.
    fn cache(&self) -> MutexGuard<'_, RouteCache> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn system_descriptor(&self) -> RangeDescriptor {
        self.cache()
            .by_id(SYSTEM_RANGE)
            .cloned()
            .unwrap_or_else(|| self.bootstrap.clone())
    }

    fn is_local(&self, call: &RangeCall) -> bool {
        if call.is_system() {
            self.local.system().is_some()
        } else {
            self.local.service(call.range()).is_some()
        }
    }

    /// Leader hint, else the first untried replica (voters first, then by node id); suspect
    /// nodes only when nothing else is left (`choose`).
    fn pick(&self, range: RangeId, tried: &[u64]) -> Option<u64> {
        let suspects = self.suspects.at(Instant::now());
        let cache = self.cache();
        let hint = cache.leader(range).filter(|&hint| hint != self.node);
        let mut replicas = match cache.route_hint(range) {
            Some(desc) => desc.replicas.clone(),
            None if range == SYSTEM_RANGE => self.bootstrap.replicas.clone(),
            None => return hint,
        };
        drop(cache);
        replicas.sort_by_key(|replica| (replica.role, replica.node));
        let nodes: Vec<u64> = replicas
            .into_iter()
            .map(|replica| replica.node)
            .filter(|&n| n != self.node)
            .collect();
        choose(hint, &nodes, tried, &suspects)
    }

    fn call(&self, call: RangeCall) -> Result<RangeReply, RangeError> {
        let range = call.range();
        let (mut try_local, mut tried, mut last) = (true, Vec::new(), None);
        let mut call_hint = None;
        for _ in 0..MAX_ATTEMPTS {
            let (node, result) = if try_local && self.is_local(&call) {
                try_local = false;
                (self.node, dispatch(&*self.local, call.clone()))
            } else {
                let Some(node) = call_hint.take().or_else(|| self.pick(range, &tried)) else {
                    return Err(RangeError::Unavailable(format!(
                        "no route to range {}",
                        range.0
                    )));
                };
                tried.push(node);
                (node, self.transport.call(node, call.clone()))
            };
            let err = match result {
                Ok(reply) => {
                    if node != self.node {
                        self.suspects.clear(node);
                        self.cache().set_leader(range, Some(node));
                    }
                    return Ok(reply);
                }
                Err(err) => err,
            };
            if node != self.node && self.transport.unreachable(node, &err) {
                // Not reached: suspect, and its leader hint no longer leads the next attempt.
                self.suspects.mark(node, Instant::now());
                let mut cache = self.cache();
                if cache.leader(range) == Some(node) {
                    cache.set_leader(range, None);
                }
            }
            match &err {
                RangeError::NotLeader { hint } => {
                    let suspects = self.suspects.at(Instant::now());
                    // A follower may name a recovered leader. Try a suspect hint once in
                    // this call, without changing other callers' suspicion or leader cache.
                    call_hint = hint
                        .filter(|n| *n != self.node && suspects.contains(n) && !tried.contains(n));
                    try_local |= *hint == Some(self.node);
                    self.cache().set_leader(
                        range,
                        hint.filter(|n| *n != self.node && !suspects.contains(n)),
                    );
                }
                RangeError::StaleEpoch { descriptor } => {
                    let mut cache = self.cache();
                    cache.insert(descriptor.clone());
                    cache.set_leader(range, None);
                    drop(cache);
                    if call.carries_epoch() {
                        return Err(err);
                    }
                }
                RangeError::Unavailable(_) => self.cache().set_leader(range, None),
                _ => return Err(err),
            }
            last = Some(err);
        }
        let last = last.map_or_else(String::new, |err| err.to_string());
        Err(RangeError::Unavailable(format!(
            "range {}: no leader after {MAX_ATTEMPTS} attempts ({last})",
            range.0
        )))
    }
}

macro_rules! routed {
    ($router:expr, $call:expr, $pattern:pat => $out:expr) => {
        match $router.call($call)? {
            $pattern => Ok($out),
            _ => Err(RangeError::Corrupt("mismatched range reply".into())),
        }
    };
}

/// Timestamp oracle served by the system range's leader.
struct SystemOracle(Arc<Router>);

impl TsOracle for SystemOracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        routed!(self.0, RangeCall::OracleNext { range: SYSTEM_RANGE, n }, RangeReply::OracleNext(ts) => ts)
    }
}

/// `RangeClient` over the route cache, in-process leaders and the range transport.
pub struct RoutedRangeClient {
    router: Arc<Router>,
    oracle: SystemOracle,
}

impl RoutedRangeClient {
    /// `system` is the system range's bootstrap descriptor (static configuration).
    pub fn new(
        node: u64,
        system: RangeDescriptor,
        transport: Arc<dyn RangeTransport>,
        local: Arc<dyn LocalRanges>,
    ) -> Self {
        let router = Arc::new(Router {
            node,
            bootstrap: system,
            transport,
            local,
            cache: Mutex::new(RouteCache::default()),
            suspects: Suspects::default(),
        });
        Self {
            oracle: SystemOracle(router.clone()),
            router,
        }
    }

    /// Directory lookup at one timestamp; retried while the system range's epoch moves.
    fn refresh(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        let ts = self.oracle.next(1)?;
        let mut attempts = 1;
        loop {
            match self.scan_directory(range_key, ts) {
                Ok(Some(desc)) => {
                    self.router.cache().insert(desc.clone());
                    return Ok(desc);
                }
                Ok(None) => {
                    return Err(RangeError::Unavailable(format!(
                        "no directory record contains key {range_key:02x?}"
                    )));
                }
                Err(RangeError::StaleEpoch { descriptor })
                    if descriptor.id == SYSTEM_RANGE && attempts < MAX_ATTEMPTS =>
                {
                    attempts += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn scan_directory(
        &self,
        range_key: &[u8],
        ts: Timestamp,
    ) -> Result<Option<RangeDescriptor>, RangeError> {
        let mut req = directory::lookup_request(range_key, &self.router.system_descriptor(), ts);
        loop {
            let page = self.read(req.clone())?;
            if let Some(desc) = directory::parse_lookup(&page, range_key)? {
                return Ok(Some(desc));
            }
            match page.resume {
                Some(resume) if directory::all_tombstones(&page) => req.start = resume,
                _ => return Ok(None),
            }
        }
    }
}

impl RangeClient for RoutedRangeClient {
    fn descriptor_for(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        let cached = self.router.cache().lookup(range_key);
        cached.map_or_else(|| self.refresh(range_key), Ok)
    }
    fn invalidate(&self, stale: &RangeDescriptor) {
        self.router.cache().invalidate(stale);
    }
    /// Caches `descriptor` (e.g. a split child from its parent's lineage record) unless the
    /// same range is cached at an epoch >= its own; like a StaleEpoch descriptor, it then drops
    /// every cached descriptor overlapping its span (`RouteCache::insert`).
    fn learn(&self, descriptor: &RangeDescriptor) {
        let mut cache = self.router.cache();
        if cache
            .by_id(descriptor.id)
            .is_none_or(|cached| cached.epoch < descriptor.epoch)
        {
            cache.insert(descriptor.clone());
        }
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        routed!(self.router, RangeCall::Read(req), RangeReply::Read(page) => page)
    }
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        routed!(self.router, RangeCall::Lock(req), RangeReply::Lock(grant) => grant)
    }
    fn blockers(&self, range: RangeId, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        routed!(self.router, RangeCall::Blockers { range, txn }, RangeReply::Blockers(txns) => txns)
    }
    fn cancel_wait(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        routed!(self.router, RangeCall::CancelWait { range, txn }, RangeReply::CancelWait => ())
    }
    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        routed!(self.router, RangeCall::Release { range, txn }, RangeReply::Release => ())
    }
    fn heartbeat(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        routed!(self.router, RangeCall::Heartbeat { range, txn }, RangeReply::Heartbeat => ())
    }
    fn commit(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        let call = RangeCall::Commit {
            range,
            epoch,
            txn,
            writes,
            marker_xid,
            lock_generation,
        };
        routed!(self.router, call, RangeReply::Commit(ts) => ts)
    }
    fn prepare(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        let call = RangeCall::Prepare {
            range,
            epoch,
            txn,
            home,
            writes,
            lock_generation,
        };
        routed!(self.router, call, RangeReply::Prepare(ts) => ts)
    }
    fn finalize(
        &self,
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        routed!(self.router, RangeCall::Finalize { range, txn, outcome }, RangeReply::Finalize => ())
    }
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        routed!(self.router, RangeCall::Home { range, epoch, op }, RangeReply::Home(reply) => reply)
    }
    fn oracle(&self) -> &dyn TsOracle {
        &self.oracle
    }
    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError> {
        let call = RangeCall::Lease {
            range: SYSTEM_RANGE,
            kind,
            id,
            count,
        };
        routed!(self.router, call, RangeReply::Lease(range) => range)
    }
    fn sequence(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        let call = RangeCall::Sequence {
            range: SYSTEM_RANGE,
            id,
            incarnation,
            op,
        };
        routed!(self.router, call, RangeReply::Sequence(reply) => reply)
    }
}

#[cfg(test)]
mod suspect_tests {
    use super::*;
    use giggles_core::range_api::{ReplicaDesc, ReplicaRole};

    #[test]
    fn choose_skips_suspects_until_nothing_else_is_left() {
        // A healthy hint leads; a suspect hint does not.
        assert_eq!(choose(Some(3), &[2, 3], &[], &[]), Some(3));
        assert_eq!(choose(Some(3), &[2, 3], &[], &[3]), Some(2));
        // Replicas in order, suspects last.
        assert_eq!(choose(None, &[2, 3], &[], &[2]), Some(3));
        assert_eq!(choose(None, &[2, 3], &[3], &[2]), Some(2));
        // Every untried replica suspect: the first untried one.
        assert_eq!(choose(None, &[2, 3], &[], &[2, 3]), Some(2));
        assert_eq!(choose(Some(3), &[2, 3], &[2], &[3]), Some(3));
        // Everything tried: the first replica, as before.
        assert_eq!(choose(None, &[2, 3], &[2, 3], &[]), Some(2));
        assert_eq!(choose(None, &[], &[], &[]), None);
    }

    #[test]
    fn suspicion_ends_after_suspect_for() {
        let suspects = Suspects::default();
        let now = Instant::now();
        suspects.mark(3, now);
        assert_eq!(suspects.at(now), [3]);
        assert_eq!(suspects.at(now + SUSPECT_FOR / 2), [3]);
        assert!(suspects.at(now + SUSPECT_FOR).is_empty());
        // Marked again later: suspect again from then.
        suspects.mark(3, now + SUSPECT_FOR);
        assert_eq!(suspects.at(now + SUSPECT_FOR), [3]);
    }

    /// Times out every call to node 3; elsewhere answers NotLeader naming node 3.
    struct CutOff(Mutex<Vec<u64>>);
    impl RangeTransport for CutOff {
        fn call(&self, node: u64, _call: RangeCall) -> Result<RangeReply, RangeError> {
            self.0.lock().unwrap().push(node);
            if node == 3 {
                Err(RangeError::Unavailable("call to node 3 timed out".into()))
            } else {
                Err(RangeError::NotLeader { hint: Some(3) })
            }
        }
        fn unreachable(&self, node: u64, error: &RangeError) -> bool {
            node == 3 && matches!(error, RangeError::Unavailable(_))
        }
    }
    struct NoLocal;
    impl LocalRanges for NoLocal {
        fn service(&self, _range: RangeId) -> Option<Arc<dyn RangeService>> {
            None
        }
        fn system(&self) -> Option<Arc<dyn SystemService>> {
            None
        }
    }

    #[test]
    fn an_unreachable_leader_is_tried_last_while_suspect() {
        let system = RangeDescriptor {
            id: SYSTEM_RANGE,
            epoch: Epoch(1),
            start: Vec::new(),
            end: None,
            replicas: [3, 2, 1]
                .map(|node| ReplicaDesc {
                    node,
                    role: ReplicaRole::Voter,
                })
                .to_vec(),
        };
        let transport = Arc::new(CutOff(Mutex::new(Vec::new())));
        let client = RoutedRangeClient::new(1, system, transport.clone(), Arc::new(NoLocal));
        client.router.cache().set_leader(SYSTEM_RANGE, Some(3));
        // Node 3 becomes suspect. Node 2's hint must not retry it in the same call,
        // clear the shared suspicion, or restore the cached leader.
        assert!(client.oracle().next(1).is_err());
        assert_eq!(*transport.0.lock().unwrap(), [3, 2, 2]);
        assert_eq!(client.router.suspects.at(Instant::now()), [3]);
        assert_eq!(client.router.cache().leader(SYSTEM_RANGE), None);
        // A new call may follow node 2's hint once; failure keeps node 3 suspect.
        transport.0.lock().unwrap().clear();
        assert!(client.oracle().next(1).is_err());
        assert_eq!(*transport.0.lock().unwrap(), [2, 3, 2]);
        assert_eq!(client.router.suspects.at(Instant::now()), [3]);
        assert_eq!(client.router.cache().leader(SYSTEM_RANGE), None);
    }
    #[test]
    fn fresh_follower_hint_routes_to_suspect_leader_with_five_replicas() {
        struct Healthy(Mutex<Vec<u64>>);
        impl RangeTransport for Healthy {
            fn call(&self, node: u64, _: RangeCall) -> Result<RangeReply, RangeError> {
                self.0.lock().unwrap().push(node);
                if node == 5 {
                    Ok(RangeReply::OracleNext(Timestamp(42)))
                } else {
                    Err(RangeError::NotLeader { hint: Some(5) })
                }
            }
        }
        let system = RangeDescriptor {
            id: SYSTEM_RANGE,
            epoch: Epoch(1),
            start: Vec::new(),
            end: None,
            replicas: [2, 3, 4, 5, 6]
                .map(|node| ReplicaDesc {
                    node,
                    role: ReplicaRole::Voter,
                })
                .to_vec(),
        };
        let transport = Arc::new(Healthy(Mutex::new(Vec::new())));
        let client = RoutedRangeClient::new(1, system, transport.clone(), Arc::new(NoLocal));
        client.router.cache().set_leader(SYSTEM_RANGE, Some(5));
        client.router.suspects.mark(5, Instant::now());
        assert_eq!(client.oracle().next(1).unwrap(), Timestamp(42));
        assert_eq!(*transport.0.lock().unwrap(), [2, 5]);
        assert!(client.router.suspects.at(Instant::now()).is_empty());
    }
}

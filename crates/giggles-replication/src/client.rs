//! Routed range client (GD-0075 R5-23): descriptors from the route cache, refreshed from the
//! system range's directory; calls go in process when the leader is local, otherwise over a
//! `RangeTransport` to the leader hint or a replica, at most `MAX_ATTEMPTS` sends per call.
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};

use giggles_core::range_api::{
    ClusterTxnId, Epoch, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, Outcome,
    RangeClient, RangeDescriptor, RangeError, RangeId, RangeService, RangeWrite, ReadPage,
    ReadRequest, SYSTEM_RANGE, SystemService, Timestamp, TsOracle,
};

use crate::{directory, route::RouteCache};

/// Attempts per call in total (local, hint, replicas) before `Unavailable`.
pub const MAX_ATTEMPTS: usize = 3;

/// One range call; system calls (`OracleNext`, `Lease`) target `SYSTEM_RANGE`.
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
    },
    Prepare {
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
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
            | Self::Lease { range, .. } => *range,
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
        matches!(self, Self::OracleNext { .. } | Self::Lease { .. })
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
}

/// Remote path to a node's range leaders (the peer transport, R5-21).
pub trait RangeTransport: Send + Sync {
    fn call(&self, node: u64, call: RangeCall) -> Result<RangeReply, RangeError>;
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
        } => RangeReply::Commit(service(range)?.commit(epoch, txn, writes, marker_xid)?),
        RangeCall::Prepare {
            range,
            epoch,
            txn,
            home,
            writes,
        } => RangeReply::Prepare(service(range)?.prepare(epoch, txn, home, writes)?),
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
    })
}

struct Router {
    node: u64,
    bootstrap: RangeDescriptor,
    transport: Arc<dyn RangeTransport>,
    local: Arc<dyn LocalRanges>,
    cache: Mutex<RouteCache>,
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

    /// Leader hint, else the first untried replica (voters first, then by node id).
    fn pick(&self, range: RangeId, tried: &[u64]) -> Option<u64> {
        let cache = self.cache();
        if let Some(hint) = cache.leader(range).filter(|&hint| hint != self.node) {
            return Some(hint);
        }
        let mut replicas = match cache.by_id(range) {
            Some(desc) => desc.replicas.clone(),
            None if range == SYSTEM_RANGE => self.bootstrap.replicas.clone(),
            None => return None,
        };
        replicas.sort_by_key(|replica| (replica.role, replica.node));
        let mut nodes = replicas
            .into_iter()
            .map(|replica| replica.node)
            .filter(|&n| n != self.node);
        let first = nodes.clone().next();
        nodes.find(|node| !tried.contains(node)).or(first)
    }

    fn call(&self, call: RangeCall) -> Result<RangeReply, RangeError> {
        let range = call.range();
        let (mut try_local, mut tried, mut last) = (true, Vec::new(), None);
        for _ in 0..MAX_ATTEMPTS {
            let (node, result) = if try_local && self.is_local(&call) {
                try_local = false;
                (self.node, dispatch(&*self.local, call.clone()))
            } else {
                let Some(node) = self.pick(range, &tried) else {
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
                        self.cache().set_leader(range, Some(node));
                    }
                    return Ok(reply);
                }
                Err(err) => err,
            };
            match &err {
                RangeError::NotLeader { hint } => {
                    try_local |= *hint == Some(self.node);
                    self.cache()
                        .set_leader(range, hint.filter(|&hint| hint != self.node));
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
    ) -> Result<Timestamp, RangeError> {
        let call = RangeCall::Commit {
            range,
            epoch,
            txn,
            writes,
            marker_xid,
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
    ) -> Result<Timestamp, RangeError> {
        let call = RangeCall::Prepare {
            range,
            epoch,
            txn,
            home,
            writes,
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
}

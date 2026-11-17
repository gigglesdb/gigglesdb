//! Per-range leader engines (GD-0075 R5-16).
//!
//! On a store-backed `RangeHost` every hosted range group gets a `RoleManager`
//! that, once this node leads the group and its read barrier has drained the
//! log, reads the range's applied descriptor and opens an engine through the
//! `RangeEngineOpener` (design.md section 4). The engine's sink is bound to that
//! leadership term and descriptor epoch; leadership loss, a stop, any failed write
//! or a split applied on this node (R5-81) fences it and calls `opener.close(range)`
//! exactly once. Each open gets a fresh lock generation `(term << 20) | k`
//! (design.md Changes 2026-11-16T10:40Z).
use crate::{
    core_store::TermSink,
    host::StateFactory,
    range_state::{EngineGuard, RangeStateMachine, SplitListener, encode_proposal},
    role::{Engine, RoleManager, Serving},
    sink::{ReplicaHandle, ReplicateError},
    state::StateMachine,
};
use giggles_core::{
    range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LeaseKind, LockBatchRequest, LockGrant, LockRequest, Outcome,
        RangeDescriptor, RangeEngineOpener, RangeError, RangeId, RangeService, RangeWrite,
        ReadPage, ReadRequest, SYSTEM_RANGE, SequenceOp, SequenceReply, SystemService, Timestamp,
        TsOracle,
    },
    replicated::{self as gr, RangeStore},
};
use raft::StateRole;
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{
        Arc, Mutex, PoisonError, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

/// A range leader's `BatchSink` for one term: term-fenced like `TermSink`, and
/// every batch is wrapped as `encode_proposal(epoch, batch)` with the epoch of
/// the descriptor the engine was opened with. A batch refused at apply (stale
/// epoch, key outside the range) returns `NotLeader` (definitely not applied);
/// any error fences the sink for good.
pub struct RangeSink {
    inner: TermSink,
    epoch: Epoch,
}
impl RangeSink {
    pub fn new(handle: ReplicaHandle, term: u64, epoch: Epoch) -> Self {
        Self {
            inner: TermSink::new(handle, term),
            epoch,
        }
    }
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    pub fn fence(&self) {
        self.inner.fence();
    }
    pub fn is_fenced(&self) -> bool {
        self.inner.is_fenced()
    }
}
impl gr::BatchSink for RangeSink {
    fn replicate(&self, batch: Vec<u8>) -> Result<(), gr::ReplicateError> {
        match self.inner.replicate_local(encode_proposal(self.epoch, &batch)) {
            Ok(()) => Ok(()),
            // Refused at apply (stale epoch, key outside the range) means definitely
            // not applied, like NotLeader: the engine is fenced and the caller retries
            // against the reopened engine, never treats it as an unknown outcome.
            Err(ReplicateError::NotLeader | ReplicateError::Refused(_)) => {
                Err(gr::ReplicateError::NotLeader)
            }
            Err(e) => Err(gr::ReplicateError::Unavailable(e.to_string())),
        }
    }
}

/// Confirms that `handle`'s replica still leads in `term`: a ReadIndex round
/// (a quorum heard this leader after the call began) that also waits until the
/// read index is applied here. A deposed or cut-off leader fails with NotLeader
/// once check-quorum deposes it, never answers from stale state.
fn confirm_leader(handle: &ReplicaHandle, term: u64) -> Result<(), RangeError> {
    let not_leader = || {
        let role = handle.role();
        RangeError::NotLeader {
            hint: (role.leader != 0 && role.state != StateRole::Leader).then_some(role.leader),
        }
    };
    match handle.read_barrier() {
        Ok(_) => {}
        Err(ReplicateError::NotLeader | ReplicateError::Stopped) => return Err(not_leader()),
        Err(e) => return Err(RangeError::Unavailable(e.to_string())),
    }
    let role = handle.role();
    if role.state != StateRole::Leader || role.term != term {
        return Err(not_leader());
    }
    Ok(())
}

/// A leader engine's service as this node serves it (GD-0075 R5-33, design.md 4a
/// O1/O6): reads and lock grants (a grant returns the newest committed value)
/// first confirm leadership in the engine's term. Writes need no check: their
/// proposals commit only through a quorum of this term.
struct ConfirmedService {
    inner: Arc<dyn RangeService>,
    handle: ReplicaHandle,
    term: u64,
}
impl RangeService for ConfirmedService {
    fn descriptor(&self) -> RangeDescriptor {
        self.inner.descriptor()
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        confirm_leader(&self.handle, self.term)?;
        self.inner.read(req)
    }
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        confirm_leader(&self.handle, self.term)?;
        self.inner.lock(req)
    }
    fn lock_batch(&self, req: LockBatchRequest) -> Result<Vec<LockGrant>, RangeError> {
        // A batch never waits, so one leadership confirmation covers all grants.
        confirm_leader(&self.handle, self.term)?;
        self.inner.lock_batch(req)
    }
    fn blockers(&self, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.inner.blockers(txn)
    }
    fn cancel_wait(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.inner.cancel_wait(txn)
    }
    fn release(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.inner.release(txn)
    }
    fn heartbeat(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.inner.heartbeat(txn)
    }
    fn commit(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.inner
            .commit(epoch, txn, writes, marker_xid, lock_generation)
    }
    fn prepare(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.inner
            .prepare(epoch, txn, home, writes, lock_generation)
    }
    fn finalize(&self, txn: ClusterTxnId, outcome: Outcome) -> Result<(), RangeError> {
        self.inner.finalize(txn, outcome)
    }
    fn home(&self, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.inner.home(epoch, op)
    }
}

/// The system service as the system range's current leader serves it: every
/// timestamp and allocator lease is handed out only after confirming leadership,
/// so a cut-off deposed system leader issues none (design.md 4a O1, R5-31).
struct ConfirmedSystem {
    inner: Arc<dyn SystemService>,
    handle: ReplicaHandle,
    term: u64,
}
impl TsOracle for ConfirmedSystem {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        confirm_leader(&self.handle, self.term)?;
        self.inner.oracle().next(n)
    }
}
impl SystemService for ConfirmedSystem {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError> {
        confirm_leader(&self.handle, self.term)?;
        self.inner.lease(kind, id, count)
    }
    fn sequence(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        confirm_leader(&self.handle, self.term)?;
        self.inner.sequence(id, incarnation, op)
    }
}

/// One range's leader engine for one term.
pub struct RangeEngine {
    range: RangeId,
    service: Arc<dyn RangeService>,
    sink: Arc<RangeSink>,
    handle: ReplicaHandle,
    term: u64,
    opener: Arc<dyn RangeEngineOpener>,
    closed: AtomicBool,
    /// The range's `RangeStateMachine::moved_epoch`: a split applied on this node moved the
    /// descriptor past this engine's epoch (GD-0075 R5-81).
    moved: Arc<AtomicU64>,
    // Keeps snapshot installs of this range refused while the engine lives.
    _guard: EngineGuard,
}
impl RangeEngine {
    pub fn range(&self) -> RangeId {
        self.range
    }
    pub fn service(&self) -> &Arc<dyn RangeService> {
        &self.service
    }
    pub fn epoch(&self) -> Epoch {
        self.sink.epoch()
    }
}
impl Engine for RangeEngine {
    fn fence(&self) {
        self.sink.fence();
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.opener.close(self.range);
        }
    }
    /// Also once a split applied on this node moved the range past the engine's epoch: the
    /// role manager then fences it on its own thread and reopens it in the same term, over
    /// the narrowed descriptor and with a fresh lock generation (`role.rs`).
    fn is_fenced(&self) -> bool {
        self.sink.is_fenced() || self.moved.load(Ordering::SeqCst) > self.sink.epoch().0
    }
}
impl Drop for RangeEngine {
    fn drop(&mut self) {
        self.fence();
    }
}

type States = Arc<Mutex<BTreeMap<RangeId, Arc<RangeStateMachine>>>>;

/// Per range: the term of this node's last engine open and how many opens it made in it.
type Opened = Arc<Mutex<BTreeMap<RangeId, (u64, u64)>>>;

/// Bits of a lock generation below the term (design.md Changes 2026-11-16T10:40Z (d)).
const GENERATION_K_BITS: u32 = 20;

/// The lock generation of the next engine this node opens for `range` in `term`:
/// `(term << 20) | k`, k = engines this node already opened for the range in that
/// term (reset per term). Strictly increasing per range across leaders (terms
/// increase) and same-term reopens; every attempt takes a fresh k, so a failed
/// open is never reused. Fails rather than wrap when k or the term overflows.
pub(crate) fn next_generation(
    opened: &Mutex<BTreeMap<RangeId, (u64, u64)>>,
    range: RangeId,
    term: u64,
) -> Result<u64, String> {
    let mut opened = opened.lock().unwrap_or_else(PoisonError::into_inner);
    let entry = opened.entry(range).or_insert((term, 0));
    if entry.0 != term {
        *entry = (term, 0);
    }
    let k = entry.1;
    if k >= 1 << GENERATION_K_BITS || term >= 1 << (64 - GENERATION_K_BITS) {
        return Err(format!(
            "range {}: no lock generation left in term {term}",
            range.0
        ));
    }
    entry.1 += 1;
    Ok((term << GENERATION_K_BITS) | k)
}

struct Backing {
    store: Arc<RangeStore>,
    opener: Option<Arc<dyn RangeEngineOpener>>,
    states: States,
    /// Lock-generation counters; kept across role-manager restarts of a range.
    opened: Opened,
}

/// The store-backed part of a `RangeHost`: role managers and the system service.
#[derive(Default)]
pub(crate) struct RangeRoles {
    backing: Option<Backing>,
    managers: Mutex<BTreeMap<RangeId, RoleManager<RangeEngine>>>,
    system: RwLock<Option<Arc<dyn SystemService>>>,
}
impl RangeRoles {
    /// Roles over `store`, and the factory building each group's `RangeStateMachine`
    /// (with `split` as its split listener).
    pub(crate) fn backed(
        store: Arc<RangeStore>,
        opener: Option<Arc<dyn RangeEngineOpener>>,
        split: SplitListener,
    ) -> (Self, StateFactory) {
        let states = States::default();
        let made = states.clone();
        let over = store.clone();
        let factory: StateFactory = Arc::new(move |range| {
            let state = Arc::new(RangeStateMachine::new(range, over.clone()));
            state.set_split_listener(split.clone());
            made.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(range, state.clone());
            state as Arc<dyn StateMachine>
        });
        let roles = Self {
            backing: Some(Backing {
                store,
                opener,
                states,
                opened: Opened::default(),
            }),
            ..Self::default()
        };
        (roles, factory)
    }

    pub(crate) fn store(&self) -> Option<&Arc<RangeStore>> {
        self.backing.as_ref().map(|b| &b.store)
    }

    /// Starts the range's role manager when the host has an opener.
    pub(crate) fn start(&self, range: RangeId, handle: ReplicaHandle, poll: Duration) {
        let Some(backing) = &self.backing else {
            return;
        };
        let Some(opener) = backing.opener.clone() else {
            return;
        };
        let state = backing
            .states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&range)
            .cloned();
        let Some(state) = state else {
            return;
        };
        let store = backing.store.clone();
        let opened = backing.opened.clone();
        let barrier = handle.clone();
        let open = Box::new(move |term| {
            // Runs after the leader's read barrier: the descriptor is the applied one.
            let descriptor = store
                .descriptor(range)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("range {} has no descriptor", range.0))?;
            let guard = state.engine_guard();
            let moved = state.moved_epoch();
            let generation = next_generation(&opened, range, term)?;
            let sink = Arc::new(RangeSink::new(handle.clone(), term, descriptor.epoch));
            let service = opener
                .open(descriptor, sink.clone(), generation)
                .map_err(|e| e.to_string())?;
            Ok(RangeEngine {
                range,
                service,
                sink,
                handle: handle.clone(),
                term,
                opener: opener.clone(),
                closed: AtomicBool::new(false),
                moved,
                _guard: guard,
            })
        });
        let manager = RoleManager::start(barrier, open, poll.max(Duration::from_millis(1)));
        let old = self
            .managers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(range, manager);
        if let Some(old) = old {
            old.stop();
        }
    }

    /// Stops every role manager, fencing and closing its engine.
    pub(crate) fn stop_all(&self) {
        let managers =
            std::mem::take(&mut *self.managers.lock().unwrap_or_else(PoisonError::into_inner));
        for manager in managers.into_values() {
            manager.stop();
        }
    }

    pub(crate) fn serving(&self, range: RangeId) -> Option<Serving<RangeEngine>> {
        let managers = self.managers.lock().unwrap_or_else(PoisonError::into_inner);
        managers.get(&range).map(RoleManager::current)
    }

    /// This node's leader engine for `range`, confirming leadership on reads.
    pub(crate) fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>> {
        match self.serving(range)? {
            Serving::Leader { engine, .. } if !engine.is_fenced() => {
                Some(Arc::new(ConfirmedService {
                    inner: engine.service.clone(),
                    handle: engine.handle.clone(),
                    term: engine.term,
                }))
            }
            _ => None,
        }
    }

    /// The registered system service, only while this node leads the system range
    /// (else None: callers route to the system range's leader).
    pub(crate) fn system(&self) -> Option<Arc<dyn SystemService>> {
        let inner = self
            .system
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()?;
        match self.serving(SYSTEM_RANGE)? {
            Serving::Leader { engine, .. } if !engine.is_fenced() => {
                Some(Arc::new(ConfirmedSystem {
                    inner,
                    handle: engine.handle.clone(),
                    term: engine.term,
                }))
            }
            _ => None,
        }
    }

    pub(crate) fn set_system(&self, system: Option<Arc<dyn SystemService>>) {
        *self.system.write().unwrap_or_else(PoisonError::into_inner) = system;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_grow_across_same_term_reopens_and_terms() {
        let opened = Mutex::default();
        let (a, b) = (RangeId(2), RangeId(3));
        let first = next_generation(&opened, a, 5).unwrap();
        assert_eq!(first, 5 << 20);
        // A same-term reopen (after a fenced sink) gets a larger generation.
        let reopen = next_generation(&opened, a, 5).unwrap();
        assert_eq!(reopen, (5 << 20) | 1);
        // Counters are per range; a new term resets k but stays above every earlier one.
        assert_eq!(next_generation(&opened, b, 5).unwrap(), 5 << 20);
        let next_term = next_generation(&opened, a, 6).unwrap();
        assert_eq!(next_term, 6 << 20);
        assert!(first < reopen && reopen < next_term);
        // Overflow fails instead of reusing a generation.
        opened.lock().unwrap().insert(a, (6, 1 << 20));
        assert!(next_generation(&opened, a, 6).is_err());
        assert!(next_generation(&opened, b, 1 << 44).is_err());
    }
}

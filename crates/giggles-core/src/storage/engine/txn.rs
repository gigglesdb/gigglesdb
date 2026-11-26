//! Transaction isolation, write ownership, and lazy read-your-writes scans.
use super::{
    codec::CodecError,
    commit::{CommitRequest, RawOp, Sequencer, SnapshotHandle, valid_counter_key},
    hlc::Ts,
    kv::{Direction, Keyspace, KvError, KvIter, KvRead},
    locks::{LockError, LockKey, LockManager, LockMode, TASK_AUTHORITY_LOCK_TABLE, TxnId},
    mvcc,
    once_guard::{CommitGuard, GuardRejection},
    ssi_validate::{ValidationConfig, Validator},
};
pub(crate) type PrivilegeWrites = BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[cfg(test)]
thread_local! {
    static ENGINE_OPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
/// Test-only count of this thread's engine operations: transactions begun and
/// reads, writes and scans issued (each takes the snapshot first). GD-0061
/// R3-308 shows a no-task release does none.
#[cfg(test)]
pub(crate) fn engine_ops() -> u64 {
    ENGINE_OPS.get()
}
#[cfg(test)]
fn count_engine_op() {
    ENGINE_OPS.set(ENGINE_OPS.get() + 1);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SerializationReason {
    WriteConflict,
    ReadWriteConflict,
    ValidationBudget,
    SnapshotTooOld,
    /// A cluster gateway whose membership lapsed (GD-0075 Phase 11): "gateway is fenced;
    /// retry". The statement or transaction is refused; the session stays usable.
    GatewayFenced,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TxnError {
    Serialization(SerializationReason),
    Deadlock,
    LockTimeout,
    Cancelled,
    Guard(super::once_guard::GuardRejection),
    LockLimit,
    WriteSetLimit,
    /// A write past the limit `set_write_limit` set (a task statement's
    /// `pending_write_bytes`, GD-0061 R3-402); nothing was staged.
    PendingWriteLimit,
    SnapshotTooOld,
    CommitAmbiguous,
    Fenced,
    WriteFenced {
        epoch: u64,
    },
    Misuse,
    /// Not available on this path yet (cluster mode before a later R5 item).
    Unsupported,
    /// A counter reservation targeted a sequence that was dropped or recreated.
    SequenceGone,
    DependentObjects,
    Kv(KvError),
    Codec(CodecError),
}
impl TxnError {
    pub(crate) fn sqlstate(&self) -> &'static str {
        match self {
            Self::Serialization(_) | Self::SnapshotTooOld => "40001",
            Self::Deadlock => "40P01",
            Self::LockTimeout => "55P03",
            Self::Guard(GuardRejection::SnapshotMoved) => "40001",
            Self::Guard(GuardRejection::Cancelled) | Self::Cancelled => "57014",
            Self::Guard(GuardRejection::LeaseExpired) => "XX000",
            Self::LockLimit | Self::WriteSetLimit => "53200",
            Self::PendingWriteLimit => "54T01",
            Self::CommitAmbiguous => "08007",
            Self::SequenceGone => "42P01",
            Self::DependentObjects => "2BP01",
            Self::Fenced | Self::Misuse => "XX000",
            Self::WriteFenced { .. } => "25006",
            Self::Unsupported => "0A000",
            Self::Kv(_) | Self::Codec(_) => "XX000",
        }
    }
}
impl From<KvError> for TxnError {
    fn from(e: KvError) -> Self {
        Self::Kv(e)
    }
}
impl From<CodecError> for TxnError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}
impl From<LockError> for TxnError {
    fn from(e: LockError) -> Self {
        match e {
            LockError::Deadlock => Self::Deadlock,
            LockError::Timeout => Self::LockTimeout,
            LockError::Cancelled => Self::Cancelled,
            LockError::Limit => Self::LockLimit,
            LockError::Misuse => Self::Misuse,
        }
    }
}
impl From<TxnError> for crate::CoreError {
    fn from(e: TxnError) -> Self {
        match e {
            TxnError::Kv(e) => e.into(),
            TxnError::Codec(e) => e.into(),
            TxnError::Guard(GuardRejection::SnapshotMoved) => Self::Sql(crate::relational::SqlError::Conflict),
            TxnError::Guard(GuardRejection::Cancelled) | TxnError::Cancelled => Self::Cancelled,
            // The once layer maps LeaseExpired to ClockUncertain before conversion; no wire tag.
            TxnError::Guard(GuardRejection::LeaseExpired) => Self::Busy,
            TxnError::Fenced => Self::Fenced,
            TxnError::WriteFenced { epoch } => Self::WriteFenced { epoch },
            TxnError::LockLimit | TxnError::WriteSetLimit => Self::Limit,
            TxnError::PendingWriteLimit => Self::Task(crate::task::TaskError::BudgetExceeded(
                crate::task::Budget::PendingWrite,
            )),
            TxnError::Serialization(_) | TxnError::SnapshotTooOld => {
                Self::Sql(crate::relational::SqlError::Conflict)
            }
            TxnError::Deadlock => Self::Deadlock,
            TxnError::LockTimeout if crate::Cancellation::lock_deadline().is_some() => Self::LockNotAvailable,
            TxnError::LockTimeout => Self::Busy,
            // CoreError has no dedicated unknown-outcome variant; Storage follows its unknown-outcome route.
            TxnError::CommitAmbiguous => Self::Storage("engine commit outcome unknown".into()),
            TxnError::Misuse => Self::Corrupt("engine transaction misuse".into()),
            TxnError::Unsupported => Self::Unsupported,
            // The sequence relation no longer exists (42P01, as on legacy).
            TxnError::DependentObjects => Self::Sql(crate::relational::SqlError::DependentObjects),
            TxnError::SequenceGone => Self::Sql(crate::relational::SqlError::UnknownTable),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Isolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadRange {
    pub(crate) ks: Keyspace,
    pub(crate) start: Bound<Vec<u8>>,
    pub(crate) end: Bound<Vec<u8>>,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct ReadSet {
    pub(crate) points: BTreeSet<(Keyspace, Vec<u8>)>,
    pub(crate) ranges: Vec<ReadRange>,
}
#[derive(Debug)]
pub(crate) struct WriteTarget {
    pub(crate) current: Option<Vec<u8>>,
    pub(crate) changed_since_snapshot: bool,
}
/// An opaque mark belonging to one transaction instance, even when TxnId is reused.
#[derive(Clone, Debug)]
pub(crate) struct Savepoint {
    owner: Arc<TxnLifetime>,
    sequence: u64,
    undo_len: usize,
    raw_len: usize,
    lock_undo_len: usize,
    releases_locks: bool,
    features_or: u64,
    minimum_subversion: u32,
}
/// Savepoints and outside users may retain the transaction's generation pin.
#[derive(Debug, Default)]
pub(crate) struct TxnLifetime {
    pub(crate) lease: Option<crate::storage::EngineLease>,
}

struct UndoWrite {
    key: (Keyspace, Vec<u8>),
    // None means absent from the overlay; Some(None) means a buffered tombstone.
    previous: Option<Option<Vec<u8>>>,
    write_bytes: usize,
    bytes: usize,
}
/// What a transaction calls below it (GD-0075 design.md section 4): the single-node
/// sequencer and lock manager, or a gateway's cluster services (R5-42). Enum dispatch;
/// `Local` behaves exactly as before.
#[derive(Clone)]
pub(crate) enum TxnServices {
    Local {
        seq: Arc<Sequencer>,
        locks: Arc<LockManager>,
    },
    Cluster(Arc<dyn ClusterServices>),
}
/// A key's newest committed version: its timestamp and value (None = tombstone).
pub(crate) type LatestVersion = (Ts, Option<Vec<u8>>);
/// Gateway services for cluster transactions, implemented by
/// `storage::cluster::txn::ClusterTxnServices`. Per-transaction state is keyed by TxnId,
/// as in LockManager.
pub(crate) trait ClusterServices: Send + Sync {
    /// A statement timestamp from the oracle (design.md 4a O2).
    fn snapshot(&self, txn: TxnId) -> Result<Ts, TxnError>;
    /// A fresh oracle timestamp not tied to a transaction (catalog loads and pins).
    fn now(&self) -> Result<Ts, TxnError>;
    /// Raw reader serving, for each user key, only its newest version at or below `ts`.
    fn reader(&self, ts: Ts) -> Arc<dyn KvRead>;
    /// This gateway's own lock manager (Table(0) and catalog-name locks).
    fn gateway_locks(&self) -> &LockManager;
    /// Exclusive row or unique lock in the key's range, then its newest committed version.
    fn lock_key(
        &self,
        txn: TxnId,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<LatestVersion>, TxnError>;
    /// Shared FK witness lock in the key's range, without a writer sentinel.
    fn lock_key_shared(
        &self,
        txn: TxnId,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError>;
    /// The per-table writer sentinel (design.md 3.1).
    fn lock_sentinel(
        &self,
        txn: TxnId,
        table: u64,
        mode: LockMode,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError>;
    fn write_set_bytes(&self) -> usize;
    fn commit(&self, txn: TxnId, commit: ClusterCommit) -> Result<Ts, TxnError>;
    /// Releases every lock the transaction holds, in ranges and on the gateway.
    fn release_all(&self, txn: TxnId);
    /// A new transaction id: the gateway's node id in the high 16 bits.
    fn next_txn_id(&self) -> TxnId;
    /// The range client: allocator leases and sequences on the system range.
    fn client(&self) -> Arc<dyn crate::range_api::RangeClient>;
    /// R5-55: commits `XACT/<xid> = Owner{this node, txn}` to the system range; `txn` then
    /// counts as live here until it ends. Must succeed before the xid is shown to anyone.
    fn register_xact(&self, txn: TxnId, xid: u64) -> Result<(), TxnError>;
    /// R5-55: the status of an xid that is not live on this gateway (None: never assigned).
    fn xact_status(&self, xid: u64) -> Result<Option<super::xact::XactStatus>, TxnError>;
    /// Phase 11: a DDL statement of `txn` starts (before its local `Table(0)` lock): takes the
    /// cluster's DDL lock once per transaction.
    fn ddl_begin(&self, txn: TxnId, cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        let _ = (txn, cancel);
        Ok(())
    }
    /// Phase 11: after the DDL statement's local locks, before its final snapshot: closes the
    /// other gateways for `tables` (the ids it locks AccessExclusive).
    fn ddl_close(
        &self,
        txn: TxnId,
        tables: &[u64],
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        let _ = (txn, tables, cancel);
        Ok(())
    }
}
/// A cluster transaction's buffered effects at commit.
pub(crate) struct ClusterCommit {
    pub(crate) isolation: Isolation,
    pub(crate) snapshot: Ts,
    /// SERIALIZABLE only: what the transaction read at `snapshot`, validated under shared
    /// locks at commit (GD-0075 R5-71/R5-72).
    pub(crate) reads: Option<ReadSet>,
    /// The transaction's validation budget for `reads`.
    pub(crate) validation: ValidationConfig,
    pub(crate) writes: BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>,
    pub(crate) features_or: u64,
    pub(crate) raw: Vec<RawOp>,
}
enum Snapshot {
    Local(SnapshotHandle),
    Cluster { ts: Ts, reader: Arc<dyn KvRead> },
}
impl Snapshot {
    fn ts(&self) -> Ts {
        match self {
            Self::Local(handle) => handle.ts(),
            Self::Cluster { ts, .. } => *ts,
        }
    }
}
/// Read view of a cluster transaction whose statement timestamp could not be obtained.
struct NoSnapshot;
impl KvRead for NoSnapshot {
    fn get(&self, _: Keyspace, _: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Err(KvError::Io("no cluster statement timestamp".into()))
    }
    fn range(
        &self,
        _: Keyspace,
        _: Bound<&[u8]>,
        _: Bound<&[u8]>,
        _: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        Err(KvError::Io("no cluster statement timestamp".into()))
    }
}
static NO_SNAPSHOT: NoSnapshot = NoSnapshot;
/// A KV read view: borrowed from the local store, or a routed reader fixed at a timestamp.
pub(crate) enum ReadView<'a> {
    Borrowed(&'a dyn KvRead),
    Owned(Arc<dyn KvRead>),
}
impl<'a> std::ops::Deref for ReadView<'a> {
    type Target = dyn KvRead + 'a;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(kv) => *kv,
            Self::Owned(kv) => kv.as_ref(),
        }
    }
}
/// A timestamp that GC and cache pruning must not pass while held (Local); a plain
/// oracle timestamp in a cluster, where range leaders own GC.
pub(crate) struct Pin {
    ts: Ts,
    _handle: Option<SnapshotHandle>,
}
impl Pin {
    pub(crate) fn ts(&self) -> Ts {
        self.ts
    }
}
impl TxnServices {
    pub(crate) fn pin(&self) -> Result<Pin, TxnError> {
        Ok(match self {
            Self::Local { seq, .. } => {
                let handle = seq.snapshot();
                Pin {
                    ts: handle.ts(),
                    _handle: Some(handle),
                }
            }
            Self::Cluster(cluster) => Pin {
                ts: cluster.now()?,
                _handle: None,
            },
        })
    }
    /// The newest committed timestamp (Local) or a fresh oracle timestamp (cluster).
    pub(crate) fn visible_ts(&self) -> Result<Ts, TxnError> {
        match self {
            Self::Local { seq, .. } => Ok(seq.visible_ts()),
            Self::Cluster(cluster) => cluster.now(),
        }
    }
    pub(crate) fn oldest_active(&self) -> Option<Ts> {
        match self {
            Self::Local { seq, .. } => seq.oldest_active(),
            Self::Cluster(_) => None,
        }
    }
    /// Reads at `ts` (and below). Local reads the whole store; MVCC filters by ts.
    pub(crate) fn read_view(&self, ts: Ts) -> ReadView<'_> {
        match self {
            Self::Local { seq, .. } => ReadView::Borrowed(seq.kv()),
            Self::Cluster(cluster) => ReadView::Owned(cluster.reader(ts)),
        }
    }
    /// A view holding every committed version up to now, for "newest version" checks.
    pub(crate) fn latest_view(&self) -> Result<ReadView<'_>, TxnError> {
        Ok(match self {
            Self::Local { seq, .. } => ReadView::Borrowed(seq.kv()),
            Self::Cluster(cluster) => ReadView::Owned(cluster.reader(cluster.now()?)),
        })
    }
    fn take_snapshot(&self, txn: TxnId) -> Result<Snapshot, TxnError> {
        Ok(match self {
            Self::Local { seq, .. } => Snapshot::Local(seq.snapshot()),
            Self::Cluster(cluster) => {
                let ts = cluster.snapshot(txn)?;
                Snapshot::Cluster {
                    ts,
                    reader: cluster.reader(ts),
                }
            }
        })
    }
    fn write_set_bytes(&self) -> usize {
        match self {
            Self::Local { seq, .. } => seq.write_set_bytes(),
            Self::Cluster(cluster) => cluster.write_set_bytes(),
        }
    }
    fn acquire(
        &self,
        txn: TxnId,
        key: LockKey,
        mode: LockMode,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        match self {
            Self::Local { locks, .. } => locks.acquire(txn, key, mode, deadline, cancel)?,
            Self::Cluster(cluster) => cluster
                .gateway_locks()
                .acquire(txn, key, mode, deadline, cancel)?,
        }
        Ok(())
    }
    fn release_all(&self, txn: TxnId) {
        match self {
            Self::Local { locks, .. } => locks.release_all(txn),
            Self::Cluster(cluster) => cluster.release_all(txn),
        }
    }
}
fn snapshot_view<'a>(services: &'a TxnServices, snapshot: &'a Option<Snapshot>) -> &'a dyn KvRead {
    match (services, snapshot) {
        (TxnServices::Local { seq, .. }, _) => seq.kv(),
        (TxnServices::Cluster(_), Some(Snapshot::Cluster { reader, .. })) => reader.as_ref(),
        (TxnServices::Cluster(_), _) => &NO_SNAPSHOT,
    }
}
/// Learned while holding a local exclusive lock continuously until Drop.
struct LockedWrite {
    latest_ts: Option<Ts>,
    // Outer None means discarded after an own write (or not needed outside RC).
    current: Option<Option<Vec<u8>>>,
}

pub(crate) struct Transaction {
    services: Option<TxnServices>,
    id: TxnId,
    /// Set before acquisition (even failures), or on callback commit paths
    /// that also promise cleanup of locks acquired externally by transaction id.
    lock_cleanup_needed: AtomicBool,
    isolation: Isolation,
    snapshot: Option<Snapshot>,
    /// The last failure to obtain a statement timestamp (cluster only).
    snapshot_error: Option<TxnError>,
    /// Fresh cluster catalog validation timestamp, reused within one statement.
    catalog_check_ts: Option<Ts>,
    writes: Arc<PrivilegeWrites>,
    catalog_generation: u64,
    pub(super) catalog_overlay: std::sync::Mutex<Option<super::catalog::PrivateCatalogSnapshot>>,
    index_predicates: Option<(Ts, u64, Arc<super::catalog::IndexPredicateImage>)>,
    features_or: u64,
    minimum_subversion: u32,
    pub(crate) reclaim_after_commit: bool,
    raw: Vec<RawOp>,
    xact: Option<super::xact::XactGuard>,
    write_bytes: usize,
    /// The most `write_bytes` may reach (`set_write_limit`); `None` = only
    /// the engine's write-set limit.
    write_limit: Option<usize>,
    savepoint_owner: Option<Arc<TxnLifetime>>,
    savepoint_sequence: u64,
    savepoints: Vec<(u64, usize, bool)>,
    lock_savepoints: usize,
    undo: Vec<UndoWrite>,
    undo_bytes: usize,
    lock_undo: Vec<(LockKey, super::locks::HeldModes)>,
    locked: BTreeMap<(Keyspace, Vec<u8>), LockedWrite>,
    reads: ReadSet,
    /// The transaction snapshot, set aside while FK crosscheck reads run at the
    /// latest committed state (GD-0075 R5-76); see `begin_latest`.
    set_aside: Option<Snapshot>,
    validation_config: ValidationConfig,
    /// Set once `restart_snapshot` has replaced the first snapshot (GD-0061 C2/C3).
    restarted: bool,
}
impl Transaction {
    pub(crate) fn begin(
        seq: &Arc<Sequencer>,
        locks: &Arc<LockManager>,
        id: TxnId,
        isolation: Isolation,
    ) -> Self {
        Self::begin_with(
            TxnServices::Local {
                seq: seq.clone(),
                locks: locks.clone(),
            },
            id,
            isolation,
        )
    }
    pub(crate) fn begin_with(services: TxnServices, id: TxnId, isolation: Isolation) -> Self {
        #[cfg(test)]
        count_engine_op();
        let mut txn = Self {
            services: Some(services),
            id,
            lock_cleanup_needed: AtomicBool::new(false),
            isolation,
            snapshot: None,
            snapshot_error: None,
            catalog_check_ts: None,
            writes: Arc::new(BTreeMap::new()),
            features_or: 0,
            minimum_subversion: 0,
            catalog_generation: 0,
            catalog_overlay: std::sync::Mutex::new(None),
            index_predicates: None,
            reclaim_after_commit: false,
            raw: Vec::new(),
            xact: None,
            write_bytes: 0,
            write_limit: None,
            savepoint_owner: Some(Arc::default()),
            savepoint_sequence: 0,
            savepoints: Vec::new(),
            lock_savepoints: 0,
            undo: Vec::new(),
            undo_bytes: 0,
            lock_undo: Vec::new(),
            locked: BTreeMap::new(),
            reads: ReadSet::default(),
            set_aside: None,
            validation_config: ValidationConfig::default(),
            restarted: false,
        };
        if isolation == Isolation::ReadCommitted {
            txn.begin_statement();
        }
        txn
    }
    pub(crate) fn attach_xact(&mut self, guard: super::xact::XactGuard) -> Result<(), TxnError> {
        if self.xact_id().is_some_and(|id| id != guard.id()) {
            return Err(TxnError::Misuse);
        }
        self.xact = Some(guard);
        Ok(())
    }
    pub(crate) fn xact_id(&self) -> Option<u64> {
        self.xact.as_ref().map(super::xact::XactGuard::id)
    }
    pub(crate) fn attach_lease(&mut self, lease: crate::storage::EngineLease) {
        Arc::get_mut(self.savepoint_owner.as_mut().expect("transaction owner")).expect("new transaction").lease = Some(lease);
    }
    pub(crate) fn engine_lifetime(&self) -> Option<&Arc<TxnLifetime>> {
        self.savepoint_owner.as_ref().filter(|owner| owner.lease.is_some())
    }
    pub(crate) fn lifetime(&self) -> std::sync::Weak<TxnLifetime> {
        Arc::downgrade(self.savepoint_owner.as_ref().expect("transaction owner"))
    }
    /// Marks buffered writes; undo records, keys, and prior values count against
    /// the write-set limit while a savepoint remains live.
    pub(crate) fn savepoint(&mut self) -> Savepoint {
        self.make_savepoint(true)
    }
    /// Marks statement writes without enabling lock journaling. Locks are retained
    /// on statement rollback; an enclosing lock-releasing mark still journals them.
    pub(crate) fn statement_savepoint(&mut self) -> Savepoint {
        self.make_savepoint(false)
    }
    fn make_savepoint(&mut self, releases_locks: bool) -> Savepoint {
        // Never wrap and revive a released token, even at an identical undo mark.
        self.savepoint_sequence = self
            .savepoint_sequence
            .checked_add(1)
            .expect("transaction savepoint sequence exhausted");
        let mark = Savepoint {
            owner: Arc::clone(self.savepoint_owner.as_ref().expect("transaction owner")),
            sequence: self.savepoint_sequence,
            undo_len: self.undo.len(),
            raw_len: self.raw.len(),
            lock_undo_len: self.lock_undo.len(),
            releases_locks,
            features_or: self.features_or,
            minimum_subversion: self.minimum_subversion,
        };
        self.savepoints.push((mark.sequence, mark.undo_len, releases_locks));
        self.lock_savepoints += usize::from(releases_locks);
        mark
    }
    fn savepoint_index(&self, mark: &Savepoint) -> Result<usize, TxnError> {
        if !Arc::ptr_eq(self.savepoint_owner.as_ref().expect("transaction owner"), &mark.owner) {
            return Err(TxnError::Misuse);
        }
        self.savepoints
            .iter()
            .position(|entry| *entry == (mark.sequence, mark.undo_len, mark.releases_locks))
            .ok_or(TxnError::Misuse)
    }
    fn truncate_savepoints(&mut self, len: usize) {
        self.lock_savepoints -= self.savepoints[len..].iter().filter(|entry| entry.2).count();
        self.savepoints.truncate(len);
    }
    /// Restores buffered writes and keeps this mark reusable, invalidating later marks.
    /// Locks, the `locked` entries permitting writes, and SERIALIZABLE reads are
    /// deliberately retained until transaction end. Unlike PostgreSQL subtransaction
    /// rollback, this does not release locks acquired after the mark. SQL uses
    /// `rollback_to_releasing_locks` instead.
    pub(crate) fn rollback_to(&mut self, mark: &Savepoint) -> Result<(), TxnError> {
        let index = self.savepoint_index(mark)?;
        while self.undo.len() > mark.undo_len {
            let undo = self.undo.pop().unwrap();
            if undo.key.0 == Keyspace::Catalog {
                self.catalog_generation = self
                    .catalog_generation
                    .checked_add(1)
                    .expect("catalog generation exhausted");
            }
            match undo.previous {
                Some(value) => {
                    Arc::make_mut(&mut self.writes).insert(undo.key, value);
                }
                None => {
                    Arc::make_mut(&mut self.writes).remove(&undo.key);
                }
            }
            self.write_bytes = undo.write_bytes;
            self.undo_bytes -= undo.bytes;
        }
        self.truncate_savepoints(index + 1);
        self.raw.truncate(mark.raw_len);
        self.features_or = mark.features_or;
        self.minimum_subversion = mark.minimum_subversion;
        Ok(())
    }
    /// SQL subtransaction rollback. Native/statement rollback retains locks;
    /// its journal entries still belong to any enclosing SQL savepoint.
    pub(crate) fn rollback_to_releasing_locks(&mut self, mark: &Savepoint) -> Result<(), TxnError> {
        if !mark.releases_locks || mark.lock_undo_len > self.lock_undo.len() {
            return Err(TxnError::Misuse);
        }
        self.rollback_to(mark)?;
        let undo = self.lock_undo.split_off(mark.lock_undo_len);
        if let TxnServices::Local { locks, .. } = self.services() {
            locks.restore_modes(self.id, &undo);
        }
        for (key, _) in undo {
            let (ks, id, suffix) = match key {
                LockKey::Row { table, pk } => (Keyspace::Rows, table, pk),
                LockKey::Unique { index, tuple } => (Keyspace::Index, index, tuple),
                _ => continue,
            };
            let mut physical = id.to_be_bytes().to_vec();
            physical.extend(suffix);
            self.locked.remove(&(ks, physical));
        }
        Ok(())
    }

    fn acquire_tracked(&mut self, requested: &[(LockKey, LockMode)], grouped: bool,
        deadline: Option<Instant>, cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        let TxnServices::Local { locks, .. } = self.services() else { return Ok(()); };
        let tracked: Vec<_> = if self.lock_savepoints == 0 { Vec::new() } else {
            requested.iter().filter(|(key, _)| match key {
                LockKey::Row { pk, .. } => !pk.is_empty(),
                LockKey::RowKey { .. } => true,
                LockKey::Unique { index, .. } => *index != super::locks::CATALOG_NAME_INDEX,
                _ => false,
            }).cloned().collect()
        };
        let before = if tracked.is_empty() { Vec::new() } else { locks.modes_on(self.id, &tracked) };
        let result = if grouped { locks.acquire_points(self.id, requested, deadline, cancel) }
            else { locks.acquire(self.id, requested[0].0.clone(), requested[0].1, deadline, cancel) };
        let after = if tracked.is_empty() { Vec::new() } else { locks.modes_on(self.id, &tracked) };
        self.lock_undo.extend(before.into_iter().zip(after).filter_map(|(before, after)|
            (before.1 != after.1 && before.1.is_subset(after.1)).then_some(before)));
        result.map_err(Into::into)
    }

    /// Keeps writes while invalidating this mark and all later marks.
    pub(crate) fn release(&mut self, mark: Savepoint) -> Result<(), TxnError> {
        let index = self.savepoint_index(&mark)?;
        self.truncate_savepoints(index);
        if self.savepoints.is_empty() {
            self.lock_undo.clear();
            self.undo = Vec::new();
            self.undo_bytes = 0;
        }
        Ok(())
    }
    /// Re-levels a transaction that has written, locked for write and recorded
    /// no reads yet (SET TRANSACTION before the first query, GD-0075 R5-76).
    /// The next read takes a fresh snapshot, as PostgreSQL takes it at the
    /// first statement rather than at BEGIN.
    pub(crate) fn set_isolation(&mut self, isolation: Isolation) -> Result<(), TxnError> {
        if !self.writes.is_empty()
            || !self.raw.is_empty()
            || !self.locked.is_empty()
            || !self.reads.points.is_empty()
            || !self.reads.ranges.is_empty()
        {
            return Err(TxnError::Misuse);
        }
        self.isolation = isolation;
        self.snapshot = None;
        self.begin_statement();
        Ok(())
    }
    /// Forgets the snapshot a statement took when PostgreSQL would run it
    /// without one (LOCK TABLE before the first query, as pg_dump does): the
    /// next statement then snapshots after the lock wait. Only for a
    /// REPEATABLE READ or SERIALIZABLE transaction that has written and
    /// locked no rows; reads recorded at the forgotten snapshot go too.
    pub(crate) fn forget_unused_snapshot(&mut self) {
        if self.isolation != Isolation::ReadCommitted
            && self.writes.is_empty()
            && self.raw.is_empty()
            && self.locked.is_empty()
        {
            self.snapshot = None;
            self.reads = ReadSet::default();
        }
    }
    pub(crate) fn begin_statement(&mut self) {
        self.catalog_check_ts = None;
        if self.isolation == Isolation::ReadCommitted {
            // A failed cluster timestamp leaves no snapshot: reads then fail rather
            // than run at an older timestamp.
            self.snapshot = None;
            let _ = self.try_snapshot_ts();
        }
    }
    pub(crate) fn catalog_check_ts(&mut self) -> Result<Ts, TxnError> {
        if let Some(ts) = self.catalog_check_ts {
            return Ok(ts);
        }
        let ts = self.services().visible_ts()?;
        self.catalog_check_ts = Some(ts);
        Ok(ts)
    }
    fn try_snapshot_ts(&mut self) -> Result<Ts, TxnError> {
        #[cfg(test)]
        count_engine_op();
        if self.snapshot.is_none() {
            match self.services().take_snapshot(self.id) {
                Ok(snapshot) => {
                    self.snapshot = Some(snapshot);
                    self.snapshot_error = None;
                }
                Err(error) => {
                    self.snapshot_error = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(self.snapshot.as_ref().unwrap().ts())
    }
    /// Infallible for Local. In a cluster, an oracle failure returns Ts(0) with no
    /// snapshot; every read then fails and `snapshot_error` reports the cause.
    pub(crate) fn snapshot_ts(&mut self) -> Ts {
        self.try_snapshot_ts().unwrap_or(Ts(0))
    }
    pub(crate) fn snapshot_error(&self) -> Option<&TxnError> {
        self.snapshot_error.as_ref()
    }
    #[allow(dead_code)] // R5-43
    pub(crate) fn append_version_budget(&self) -> u64 {
        // Each appended row has a physical key with an eight-byte table ID
        // and a row payload with an eight-byte version. The write set must
        // reject the statement before it can use more versions than this.
        const MIN_PHYSICAL_ROW_BYTES: usize = 2 * std::mem::size_of::<u64>();
        (self.services().write_set_bytes() / MIN_PHYSICAL_ROW_BYTES) as u64 + 1
    }

    pub(crate) fn services(&self) -> &TxnServices {
        self.services.as_ref().expect("transaction services until drop")
    }
    /// Replaces a non-RC snapshot with a fresh one and forgets every read made
    /// under the old one (GD-0061 C2). Only legal before anything is buffered:
    /// no writes, raw ops or savepoint undo records. Locks, including the
    /// `locked` entries that permit writes, are kept.
    pub(crate) fn restart_snapshot(&mut self) -> Result<Ts, TxnError> {
        if self.isolation == Isolation::ReadCommitted
            || !self.writes.is_empty()
            || !self.raw.is_empty()
            || !self.undo.is_empty()
        {
            return Err(TxnError::Misuse);
        }
        // Unregister the old snapshot before registering the new one.
        self.snapshot = None;
        self.reads = ReadSet::default();
        let ts = self.try_snapshot_ts()?;
        self.restarted = true;
        Ok(ts)
    }
    pub(crate) fn snapshot_restarted(&self) -> bool {
        self.restarted
    }
    /// Whether anything is buffered (writes, raw ops or savepoint undo
    /// records), which makes [`Self::restart_snapshot`] illegal.
    pub(crate) fn has_buffered(&self) -> bool {
        !self.writes.is_empty() || !self.raw.is_empty() || !self.undo.is_empty()
    }
    /// Records `ks[start..end]` as read without reading it, so a later commit
    /// into the range fails SERIALIZABLE validation (GD-0061 C5). Fixes the
    /// snapshot the range is relative to. A no-op below SERIALIZABLE.
    pub(crate) fn track_range(
        &mut self,
        ks: Keyspace,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<(), TxnError> {
        if matches!(ks, Keyspace::Meta | Keyspace::Seq | Keyspace::Gc) {
            return Err(TxnError::Misuse);
        }
        if self.isolation != Isolation::Serializable {
            return Ok(());
        }
        self.snapshot_ts();
        self.reads.ranges.push(ReadRange { ks, start, end });
        Ok(())
    }
    /// Buffered keys of one keyspace, deletes included.
    pub(crate) fn written_keys(&self, ks: Keyspace) -> impl Iterator<Item = &Vec<u8>> {
        self.writes
            .range((ks, Vec::new())..)
            .take_while(move |((space, _), _)| *space == ks)
            .map(|((_, key), _)| key)
    }
    pub(crate) fn set_validation_config(&mut self, config: ValidationConfig) {
        self.validation_config = config;
    }
    pub(crate) fn current_snapshot_ts(&self) -> Option<Ts> {
        self.snapshot.as_ref().map(Snapshot::ts)
    }
    pub(crate) fn read_set(&self) -> &ReadSet {
        &self.reads
    }
    pub(crate) fn isolation(&self) -> Isolation {
        self.isolation
    }
    pub(crate) fn id(&self) -> TxnId {
        self.id
    }
    pub(crate) fn held_table_locks(&self) -> Vec<(u64, &'static str, bool)> {
        let locks = match self.services() {
            TxnServices::Local { locks, .. } => locks.as_ref(),
            TxnServices::Cluster(cluster) => cluster.gateway_locks(),
        };
        locks.held(self.id).into_iter().filter_map(|(key, mode)| {
            use LockMode::*;
            let LockKey::Table(table) = key else { return None; };
            let name = match mode {
                AccessShare => "AccessShareLock", RowShare => "RowShareLock",
                RowExclusive => "RowExclusiveLock", ShareUpdateExclusive => "ShareUpdateExclusiveLock",
                Share => "ShareLock", ShareRowExclusive => "ShareRowExclusiveLock",
                ExclusiveTable => "ExclusiveLock", AccessExclusive => "AccessExclusiveLock",
                Shared | Exclusive => return None,
            };
            Some((table, name, locks.fastpath(self.id, table, mode)))
        }).collect()
    }
    pub(crate) fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }
    pub(super) fn index_predicates(&mut self) -> Result<Arc<super::catalog::IndexPredicateImage>, crate::CoreError> {
        let ts = self.try_snapshot_ts()?;
        if let Some((cached_ts, generation, image)) = &self.index_predicates
            && *cached_ts == ts
            && *generation == self.catalog_generation
        {
            return Ok(image.clone());
        }
        let image = super::catalog::index_predicate_image(self)?;
        self.index_predicates = Some((ts, self.catalog_generation, image.clone()));
        Ok(image)
    }
    pub(crate) fn catalog_own(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.writes.get(&(Keyspace::Catalog, key.to_vec()))
    }
    pub(crate) fn catalog_latest_committed(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        let version = match self.services() {
            TxnServices::Local { seq, .. } => {
                mvcc::latest_committed_at_most(seq.kv(), Keyspace::Catalog, key, seq.visible_ts())?
            }
            TxnServices::Cluster(cluster) => {
                let ts = cluster.snapshot(self.id)?;
                mvcc::latest_committed_at_most(cluster.reader(ts).as_ref(), Keyspace::Catalog, key, ts)?
            }
        };
        Ok(version.and_then(|(_, value)| value))
    }
    /// Limits the bytes this transaction may buffer (`write_bytes`: keys and
    /// values of its pending writes) from now on; `None` lifts the limit. A
    /// `put` past it fails with `PendingWriteLimit` before anything is
    /// staged (GD-0061 R3-402).
    pub(crate) fn set_write_limit(&mut self, limit: Option<usize>) {
        self.write_limit = limit;
    }
    /// Latest published Kernel value, ignoring this transaction's snapshot and
    /// its own buffered writes (GD-0061 C6).
    pub(crate) fn kernel_latest_committed(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        // Protected records are local-only (GD-0061); a cluster gateway has no form yet.
        let TxnServices::Local { seq, .. } = self.services() else {
            return Err(TxnError::Unsupported);
        };
        Ok(mvcc::latest_committed_at_most(seq.kv(), Keyspace::Kernel, key, seq.visible_ts())?
            .and_then(|(_, value)| value))
    }
    pub(crate) fn privilege_writes(&self) -> Arc<PrivilegeWrites> { self.writes.clone() }
    pub(crate) fn catalog_writes(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        // Metadata reads must not walk the growing row/index overlay during COPY.
        self.writes
            .range((Keyspace::Catalog, Vec::new())..)
            .take_while(|((ks, _), _)| *ks == Keyspace::Catalog)
            .map(|((_, key), value)| (key, value))
    }
    /// DDL across gateways (Phase 11); no-op on a single node. See `ClusterServices`.
    pub(crate) fn ddl_begin(&self, cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        match self.services() {
            TxnServices::Cluster(cluster) => cluster.ddl_begin(self.id, cancel),
            TxnServices::Local { .. } => Ok(()),
        }
    }
    pub(crate) fn ddl_close(&self, tables: &[u64], cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        match self.services() {
            TxnServices::Cluster(cluster) => cluster.ddl_close(self.id, tables, cancel),
            TxnServices::Local { .. } => Ok(()),
        }
    }
    pub(crate) fn acquire_lock(
        &self,
        key: LockKey,
        mode: LockMode,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        if !matches!(key, LockKey::Table(_))
            && !matches!(key, LockKey::Unique { index, .. } if index == super::locks::CATALOG_NAME_INDEX)
        {
            return Err(TxnError::Misuse);
        }
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        self.services().acquire(self.id, key, mode, deadline, cancel)
    }
    /// Swap read sets around an FK snapshot crosscheck. RowKey locks protect
    /// this witness; ordinary version validation would reject non-key updates.
    pub(crate) fn replace_reads(&mut self, reads: ReadSet) -> ReadSet {
        std::mem::replace(&mut self.reads, reads)
    }
    /// Local FK key protection, separate from ordinary row writes.
    pub(crate) fn lock_row_key(
        &mut self, table: u64, pk_bytes: &[u8], mode: LockMode,
        deadline: Option<Instant>, cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        if matches!(self.services(), TxnServices::Cluster(_)) {
            return Ok(());
        }
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        self.acquire_tracked(&[(LockKey::RowKey { table, pk: pk_bytes.to_vec() }, mode)],
            false, deadline, cancel)
    }
    /// Reads at the latest committed state until `end_latest`, recording no
    /// SERIALIZABLE reads; returns the transaction snapshot. FK checks at
    /// REPEATABLE READ and SERIALIZABLE use it for PostgreSQL's RI crosscheck:
    /// referencing rows are found in the latest state and fail with 40001 if
    /// newer than the transaction snapshot (GD-0075 R5-76).
    pub(crate) fn begin_latest(&mut self) -> Ts {
        let ts = self.snapshot_ts();
        debug_assert!(self.set_aside.is_none(), "nested latest-state reads");
        match self.services().take_snapshot(self.id) {
            Ok(latest) => self.set_aside = self.snapshot.replace(latest),
            // A cluster oracle failure: no snapshot, so latest-state reads fail.
            Err(error) => {
                self.set_aside = self.snapshot.take();
                self.snapshot_error = Some(error);
            }
        }
        ts
    }
    pub(crate) fn end_latest(&mut self) {
        if let Some(snapshot) = self.set_aside.take() {
            self.snapshot = Some(snapshot);
        }
    }
    /// Use one pinned local RI snapshot across pages, restoring the
    /// statement snapshot before any wait or write crosscheck.
    pub(crate) fn begin_fk_action_page(&mut self, snapshot: Option<SnapshotHandle>) {
        debug_assert!(self.set_aside.is_none());
        let TxnServices::Local { seq, .. } = self.services() else { unreachable!("local FK scan"); };
        self.set_aside = self.snapshot.replace(Snapshot::Local(snapshot.unwrap_or_else(|| seq.snapshot())));
    }

    pub(crate) fn end_fk_action_page(&mut self) -> SnapshotHandle {
        let Snapshot::Local(snapshot) = self.snapshot.take().expect("local FK snapshot")
            else { unreachable!("local FK scan"); };
        self.end_latest();
        snapshot
    }

    /// Whether a committed version of `key` is newer than `ts`.
    pub(crate) fn changed_since(&self, ks: Keyspace, key: &[u8], ts: Ts) -> Result<bool, TxnError> {
        let latest = match self.services() {
            TxnServices::Local { seq, .. } => mvcc::latest_version(seq.kv(), ks, key)?,
            TxnServices::Cluster(cluster) => {
                mvcc::latest_version(&*cluster.reader(cluster.now()?), ks, key)?
            }
        };
        Ok(latest.is_some_and(|(latest, _)| latest > ts))
    }
    pub(crate) fn get(&mut self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        let ts = self.try_snapshot_ts()?;
        if self.isolation == Isolation::Serializable && self.set_aside.is_none() {
            self.reads.points.insert((ks, key.to_vec()));
        }
        if let Some(value) = self.writes.get(&(ks, key.to_vec())) {
            return Ok(value.clone());
        }
        mvcc::read_at(snapshot_view(self.services(), &self.snapshot), ks, key, ts)
    }
    /// SQL destination sentinel; acquire without choosing or inspecting a snapshot.
    pub(crate) fn lock_sql_sentinel(
        &self,
        table: u64,
        mode: LockMode,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        if !matches!(mode, LockMode::Shared | LockMode::Exclusive) {
            return Err(TxnError::Misuse);
        }
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        match self.services() {
            TxnServices::Local { locks, .. } => {
                locks.acquire(self.id, LockKey::Row { table, pk: Vec::new() }, mode, None, cancel)?
            }
            TxnServices::Cluster(cluster) => cluster.lock_sentinel(self.id, table, mode, cancel)?,
        }
        Ok(())
    }
    /// Exclusive engine lock on one Kernel key, held until transaction end and
    /// kept across savepoint rollback (GD-0061 C6, the authority hold).
    /// Acquires without choosing or inspecting a snapshot.
    pub(crate) fn lock_kernel_key(
        &self,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        if key.is_empty() {
            return Err(TxnError::Misuse);
        }
        // The authority hold is local-only (GD-0061); a cluster gateway has no form yet.
        let TxnServices::Local { locks, .. } = self.services() else {
            return Err(TxnError::Unsupported);
        };
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        locks.acquire(
            self.id,
            LockKey::Row {
                table: TASK_AUTHORITY_LOCK_TABLE,
                pk: key.to_vec(),
            },
            LockMode::Exclusive,
            deadline,
            cancel,
        )?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn local_seq(&self) -> &Arc<Sequencer> {
        match self.services() {
            TxnServices::Local { seq, .. } => seq,
            TxnServices::Cluster(_) => panic!("cluster transaction has no local sequencer"),
        }
    }
    #[cfg(test)]
    pub(crate) fn sql_waiting_count(&self) -> usize {
        match self.services() {
            TxnServices::Local { locks, .. } => locks.waiting_count(),
            TxnServices::Cluster(cluster) => cluster.gateway_locks().waiting_count(),
        }
    }
    /// Cluster FK witness lock, held until transaction end. Local FK checks use table locks.
    pub(crate) fn lock_key_share(
        &mut self,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        match self.services() {
            TxnServices::Local { .. } => Ok(()),
            TxnServices::Cluster(cluster) => {
                self.lock_cleanup_needed.store(true, Ordering::Relaxed);
                cluster.lock_key_shared(self.id, ks, key, deadline, cancel)
            }
        }
    }
    /// Writers that need both row keys use the same upgrade protocol as SELECT.
    /// The caller still uses lock_for_write for version and snapshot checks.
    pub(crate) fn lock_row_write_group(
        &mut self, table: u64, pk: &[u8], deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        if matches!(self.services(), TxnServices::Cluster(_)) { return Ok(()); }
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        self.acquire_tracked(&[
            (LockKey::RowKey { table, pk: pk.to_vec() }, LockMode::Exclusive),
            (LockKey::Row { table, pk: pk.to_vec() }, LockMode::Exclusive),
        ], true, deadline, cancel)?;
        Ok(())
    }

    /// Lock a SQL row without writing a version or permitting writes through a
    /// shared row lock. K is GD-0084 A2's local-only RowKey lock.
    pub(crate) fn lock_sql_row(
        &mut self, table: u64, pk: &[u8],
        mode: crate::relational::scan::RowLock, deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<WriteTarget, TxnError> {
        use crate::relational::scan::RowLock;
        let ts = self.try_snapshot_ts()?;
        let TxnServices::Local { seq, .. } = self.services() else { return Err(TxnError::Unsupported); };
        let seq = seq.clone();
        let mut physical = table.to_be_bytes().to_vec();
        physical.extend_from_slice(pk);
        let d = LockKey::Row { table, pk: pk.to_vec() };
        let k = LockKey::RowKey { table, pk: pk.to_vec() };
        let requested = match mode {
            RowLock::KeyShare => vec![(k, LockMode::Shared)],
            RowLock::Share => vec![(k, LockMode::Shared), (d, LockMode::Shared)],
            RowLock::NoKeyUpdate => vec![(d, LockMode::Exclusive)],
            RowLock::Update => vec![(k, LockMode::Exclusive), (d, LockMode::Exclusive)],
        };
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        self.acquire_tracked(&requested, true, deadline, cancel)?;
        // KEY SHARE coexists with non-key updates. Use the physical latest for
        // change detection and cap the READ COMMITTED value at publication.
        let visible = (mode == RowLock::KeyShare && self.isolation == Isolation::ReadCommitted)
            .then(|| seq.visible_ts());
        let latest = mvcc::latest_version(seq.kv(), Keyspace::Rows, &physical)?;
        let changed = latest.as_ref().is_some_and(|(latest, _)| *latest > ts);
        let own = self.writes.get(&(Keyspace::Rows, physical.clone()));
        if self.isolation != Isolation::ReadCommitted && changed && own.is_none() {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict));
        }
        let current = if let Some(own) = own {
            own.clone()
        } else if self.isolation == Isolation::ReadCommitted {
            let version = match visible {
                Some(bound) if latest.as_ref().is_some_and(|(ts, _)| *ts > bound) => {
                    mvcc::latest_committed_at_most(seq.kv(), Keyspace::Rows, &physical, bound)?
                }
                _ => latest,
            };
            version.and_then(|(_, value)| value)
        } else {
            mvcc::read_at(snapshot_view(self.services(), &self.snapshot), Keyspace::Rows, &physical, ts)?
        };
        Ok(WriteTarget { current, changed_since_snapshot: changed })
    }

    /// Selects the snapshot before waiting. Errors keep the transaction open; rollback or drop releases its locks.
    pub(crate) fn lock_for_write(
        &mut self,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<WriteTarget, TxnError> {
        let ts = self.try_snapshot_ts()?;
        if key.len() < 8 {
            return Err(TxnError::Misuse);
        }
        let id = u64::from_be_bytes(key[..8].try_into().unwrap());
        let lock = match ks {
            Keyspace::Rows => LockKey::Row {
                table: id,
                pk: key[8..].to_vec(),
            },
            Keyspace::Index => LockKey::Unique {
                index: id,
                tuple: key[8..].to_vec(),
            },
            _ => return Err(TxnError::Misuse),
        };
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        // Retain acquisition's misuse/poisoning errors even on a cache hit.
        self.acquire_tracked(&[(lock, LockMode::Exclusive)], false, deadline, cancel)?;
        let write_key = (ks, key.to_vec());
        let own = self.writes.get(&write_key);
        let cached = if matches!(self.services(), TxnServices::Local { seq, .. } if seq.kv().is_single_writer() && !seq.kv().is_fenced()) {
            self.locked.get(&write_key).filter(|cached| {
                self.isolation != Isolation::ReadCommitted || own.is_some() || cached.current.is_some()
            })
        } else {
            None
        };
        let (latest_ts, latest_value) = if let Some(cached) = cached {
            (cached.latest_ts, cached.current.clone().flatten())
        } else {
            let latest = match self.services() {
                TxnServices::Local { seq, .. } => mvcc::latest_version(seq.kv(), ks, key)?,
                TxnServices::Cluster(cluster) => cluster.lock_key(self.id, ks, key, deadline, cancel)?,
            };
            match latest {
                Some((ts, value)) => (Some(ts), value),
                None => (None, None),
            }
        };
        let changed = latest_ts.is_some_and(|latest| latest > ts);
        if self.isolation != Isolation::ReadCommitted && changed && own.is_none() {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict));
        }
        let value = if let Some(own) = own {
            own.clone()
        } else if self.isolation == Isolation::ReadCommitted {
            latest_value
        } else {
            mvcc::read_at(snapshot_view(self.services(), &self.snapshot), ks, key, ts)?
        };
        self.locked.insert(write_key, LockedWrite {
            latest_ts,
            current: (self.isolation == Isolation::ReadCommitted && own.is_none())
                .then(|| value.clone()),
        });
        Ok(WriteTarget {
            current: value,
            changed_since_snapshot: changed,
        })
    }
    pub(crate) fn put(
        &mut self,
        ks: Keyspace,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnError> {
        self.write(ks, key, Some(value))
    }
    pub(crate) fn delete(&mut self, ks: Keyspace, key: Vec<u8>) -> Result<(), TxnError> {
        self.write(ks, key, None)
    }
    fn write(
        &mut self,
        ks: Keyspace,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(), TxnError> {
        self.try_snapshot_ts()?;
        // Meta, Seq and Gc are unversioned: only the Sequencer worker writes them
        // (counters via write_counter, which ORs bit 8 in the same batch; DD-0068).
        if matches!(ks, Keyspace::Meta | Keyspace::Seq | Keyspace::Gc) {
            return Err(TxnError::Misuse);
        }
        if if matches!(ks, Keyspace::Catalog | Keyspace::Kernel) {
            key.is_empty()
        } else {
            key.len() < 8
        } {
            return Err(TxnError::Misuse);
        }
        if matches!(ks, Keyspace::Rows | Keyspace::Index)
            && !self.locked.contains_key(&(ks, key.clone()))
        {
            return Err(TxnError::Misuse);
        }
        if ks == Keyspace::Catalog {
            self.catalog_generation = self
                .catalog_generation
                .checked_add(1)
                .expect("catalog generation exhausted");
        }
        let write_key = (ks, key);
        let previous = self.writes.get(&write_key);
        if previous == Some(&value) {
            return Ok(());
        }
        let old = previous.map_or(0, |v| write_key.1.len() + v.as_ref().map_or(0, Vec::len));
        let undo_size = if self.savepoints.is_empty() {
            0
        } else {
            std::mem::size_of::<UndoWrite>()
                .checked_add(write_key.1.len())
                .and_then(|n| n.checked_add(previous.and_then(Option::as_ref).map_or(0, Vec::len)))
                .ok_or(TxnError::WriteSetLimit)?
        };
        let undo_bytes = self
            .undo_bytes
            .checked_add(undo_size)
            .ok_or(TxnError::WriteSetLimit)?;
        let size = write_key
            .1
            .len()
            .checked_add(value.as_ref().map_or(0, Vec::len))
            .and_then(|n| self.write_bytes.checked_sub(old)?.checked_add(n))
            .ok_or(TxnError::WriteSetLimit)?;
        if size
            .checked_add(undo_bytes)
            .is_none_or(|n| n > self.services().write_set_bytes())
        {
            return Err(TxnError::WriteSetLimit);
        }
        if self.write_limit.is_some_and(|limit| size > limit) {
            return Err(TxnError::PendingWriteLimit);
        }
        // Savepoint rollback retains the lock. If it removes this own write,
        // the next RC lock request reloads the discarded value under that lock.
        if let Some(cached) = self.locked.get_mut(&write_key) {
            cached.current = None;
        }
        if undo_size == 0 {
            Arc::make_mut(&mut self.writes).insert(write_key, value);
        } else {
            let previous = Arc::make_mut(&mut self.writes).insert(write_key.clone(), value);
            self.undo.push(UndoWrite {
                key: write_key,
                previous,
                write_bytes: self.write_bytes,
                bytes: undo_size,
            });
        }
        self.undo_bytes = undo_bytes;
        self.write_bytes = size;
        Ok(())
    }
    /// Streams a lazy merge while borrowing the transaction; unbounded scans do not materialize results.
    pub(crate) fn scan<'a>(
        &'a mut self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
        limit: Option<usize>,
    ) -> Result<TxnScan<'a>, TxnError> {
        let ts = self.try_snapshot_ts()?;
        let start = start.map(<[u8]>::to_vec);
        let end = end.map(<[u8]>::to_vec);
        let empty = match (&start, &end) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a >= b
            }
            _ => false,
        };
        let base = mvcc::VersionedRange::new(
            snapshot_view(self.services.as_ref().expect("transaction services"), &self.snapshot),
            ks,
            start.as_ref().map(Vec::as_slice),
            end.as_ref().map(Vec::as_slice),
            dir,
            ts,
        )?
        .peekable();
        // Paged mutations grow the overlay after every page. Seek directly to
        // this range rather than re-filtering all earlier writes on each scan.
        let own_bounds = if empty {
            (Bound::Included((ks, vec![])), Bound::Excluded((ks, vec![])))
        } else {
            (
                start.clone().map(|key| (ks, key)),
                end.clone().map(|key| (ks, key)),
            )
        };
        let own = self
            .writes
            .range(own_bounds)
            .filter(move |((space, _), _)| *space == ks);
        let own: OwnIter<'a> = match dir {
            Direction::Forward => Box::new(own),
            Direction::Reverse => Box::new(own.rev()),
        };
        Ok(TxnScan {
            base,
            own: own.peekable(),
            reads: if self.isolation == Isolation::Serializable && self.set_aside.is_none() {
                Some(&mut self.reads)
            } else {
                None
            },
            ks,
            start,
            end,
            dir,
            remaining: limit,
            record: None,
            done: empty,
        })
    }
    pub(crate) fn writes_keyspace(&self, ks: Keyspace) -> bool {
        self.writes.keys().any(|(space, _)| *space == ks)
    }
    pub(crate) fn require_features(&mut self, bits: u64) {
        self.features_or |= bits;
    }
    pub(crate) fn set_secondary_index_features(&mut self, partial: bool, temporal: bool) {
        use super::meta::{PARTIAL_INDEXES, TEMPORAL_INDEXES};
        // Use the final catalog image so CREATE followed by DROP in the same
        // transaction cannot enable the feature without an index.
        self.features_or &= !(PARTIAL_INDEXES | TEMPORAL_INDEXES);
        self.require_features(
            (if partial { PARTIAL_INDEXES } else { 0 })
                | (if temporal { TEMPORAL_INDEXES } else { 0 }),
        );
    }
    pub(crate) fn require_subversion(&mut self, version: u32) {
        self.minimum_subversion = self.minimum_subversion.max(version);
    }
    /// Existing counter bytes, committed atomically with their new definition.
    pub(crate) fn put_counter(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), TxnError> {
        if !valid_counter_key(&key) || value.len() != 25 {
            return Err(TxnError::Misuse);
        }
        self.require_features(256);
        self.raw.push(RawOp::PutCounter { key, value });
        Ok(())
    }
    pub(crate) fn delete_counter(&mut self, key: Vec<u8>) -> Result<(), TxnError> {
        if !valid_counter_key(&key) {
            return Err(TxnError::Misuse);
        }
        self.raw.push(RawOp::DeleteCounter { key });
        Ok(())
    }
    pub(crate) fn local_durable_tail_eligible(&self) -> bool {
        matches!(self.services(), TxnServices::Local { .. })
            && self.catalog_writes().next().is_none()
            && !self.reclaim_after_commit
    }
    pub(crate) fn commit(self) -> Result<Ts, TxnError> {
        if self.writes_keyspace(Keyspace::Catalog) {
            return Err(TxnError::Misuse);
        }
        self.commit_inner(None, |_| Ok(()), None)
    }
    /// Runs publication after sequencer success, while this transaction still
    /// owns its locks and snapshot. Catalog writes are allowed on this path.
    /// A callback error occurs after a successful durable commit: do not retry
    /// the transaction. Drop releases locks on every return path.
    pub(crate) fn commit_then<F: FnOnce(Ts) -> Result<(), TxnError>>(
        self,
        publish: F,
    ) -> Result<Ts, TxnError> {
        self.commit_then_with_tail(publish, None)
    }
    pub(crate) fn commit_then_with_tail<F: FnOnce(Ts) -> Result<(), TxnError>>(
        self, publish: F, release_admission: Option<&mut dyn FnMut()>,
    ) -> Result<Ts, TxnError> {
        // Publication callers may acquire locks externally by transaction id.
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        self.commit_inner(None, publish, release_admission)
    }
    pub(crate) fn commit_guarded<F: FnOnce(Ts) -> Result<(), TxnError>>(
        self, guard: CommitGuard, publish: F,
    ) -> Result<Ts, TxnError> {
        // Preserve the same external-lock cleanup contract as commit_then.
        self.lock_cleanup_needed.store(true, Ordering::Relaxed);
        self.commit_inner(Some(guard), publish, None)
    }
    fn commit_inner<F: FnOnce(Ts) -> Result<(), TxnError>>(
        mut self, guard: Option<CommitGuard>, publish: F, release_admission: Option<&mut dyn FnMut()>,
    ) -> Result<Ts, TxnError> {
        // Inspect the final write set, after statement/savepoint rollback.
        // The sequencer ORs these bits into metadata in the same durable batch.
        if self.writes.iter().any(|((ks, _), value)| *ks == Keyspace::Rows && value.as_ref().is_some_and(|v| v.len() >= 20 && u32::from_be_bytes(v[12..16].try_into().unwrap()) as usize > crate::relational::MAX_ROW_BYTES)) {
            self.require_features(super::meta::WIDE_ROWS);
        }

        if guard.as_ref().is_some_and(|g| g.is_fenced() && self.current_snapshot_ts() != Some(g.fence())) {
            return Err(TxnError::Misuse);
        }
        if let Some(xid) = self.xact_id() {
            self.raw.push(RawOp::XactCommitted { xid });
        }
        if guard.is_none() && self.writes.is_empty()
            && self.raw.is_empty()
            && self.features_or == 0
            && self.minimum_subversion == 0
            && self.isolation != Isolation::Serializable
        {
            let ts = match self.services() {
                TxnServices::Local { seq, .. } => seq.visible_ts(),
                TxnServices::Cluster(cluster) => match self.current_snapshot_ts() {
                    Some(ts) => ts,
                    None => cluster.snapshot(self.id)?,
                },
            };
            publish(ts)?;
            return Ok(ts);
        }
        let snapshot = self.try_snapshot_ts()?;
        if let TxnServices::Cluster(cluster) = self.services.as_ref().expect("transaction services") {
            // A fenced guard compares against one node's visible timestamp,
            // which a cluster does not have; guarded DDL arrives with Phase 11.
            if let Some(guard) = &guard {
                if guard.is_fenced() {
                    return Err(TxnError::Unsupported);
                }
                guard.decide().map_err(TxnError::Guard)?;
            }
            // Drop releases every range lock after the commit returns.
            let ts = cluster.commit(
                self.id,
                ClusterCommit {
                    isolation: self.isolation,
                    snapshot,
                    reads: (self.isolation == Isolation::Serializable)
                        .then(|| std::mem::take(&mut self.reads)),
                    validation: self.validation_config.clone(),
                    writes: Arc::unwrap_or_clone(std::mem::take(&mut self.writes)),
                    features_or: self.features_or,
                    raw: std::mem::take(&mut self.raw),
                },
            )?;
            publish(ts)?;
            return Ok(ts);
        }
        let validator = (self.isolation == Isolation::Serializable).then(|| {
            Validator::new(
                std::mem::take(&mut self.reads),
                self.validation_config.clone(),
            )
        });
        // The owned validator reads only the worker's supplied log and KV handle.
        // Drop releases locks and unregisters the snapshot on success or error.
        let request = CommitRequest {
            snapshot,
            writes: Arc::unwrap_or_clone(std::mem::take(&mut self.writes)),
            features_or: self.features_or,
            minimum_subversion: self.minimum_subversion,
            raw: std::mem::take(&mut self.raw),
            validation: validator.map(|validator| {
                Box::new(
                    move |log: &super::commit::RecentLog,
                          kv: &dyn super::kv::KvRead,
                          snapshot,
                          _commit| { validator.validate(log, kv, snapshot) },
                ) as _
            }),
        };
        let TxnServices::Local { seq, .. } = self.services() else {
            unreachable!("cluster commits returned above");
        };
        let ts = match guard {
            Some(guard) => seq.submit_guarded(request, guard)?,
            None => seq.submit_with_tail(request, release_admission)?,
        };
        publish(ts)?;
        Ok(ts)
    }
    pub(crate) fn rollback(self) {}
}
impl Drop for Transaction {
    fn drop(&mut self) {
        // Cluster range/prepared holders can acquire by txn id outside this object.
        if self.lock_cleanup_needed.load(Ordering::Relaxed)
            || matches!(self.services(), TxnServices::Cluster(_))
        {
            self.services().release_all(self.id);
        }
        // Release the engine resources before the counted generation pin.
        // Savepoints can independently retain their share of this pin.
        drop(self.snapshot.take());
        drop(self.set_aside.take());
        drop(self.xact.take());
        drop(self.services.take());
        drop(self.savepoint_owner.take());
    }
}
type OwnEntry<'a> = (&'a (Keyspace, Vec<u8>), &'a Option<Vec<u8>>);
type OwnIter<'a> = Box<dyn Iterator<Item = OwnEntry<'a>> + 'a>;
/// Merge of the snapshot base (`VersionedRange`) and the transaction's own
/// writes (a `BTreeMap` range with exact bounds).
///
/// GD-0084 Q40: the merge assumes the base arrives in user-key order and that
/// both sides apply the same bounds, which holds only over prefix-free regions.
/// Where the base holds prefix-related visible keys (Catalog name and role
/// membership keys), an own delete or overwrite of the shorter key can be
/// passed before the base reaches it, so the committed value is emitted again.
/// Such callers must reapply `catalog_writes()` to a keyed result. Inside a DDL
/// rekey transaction (ADD/DROP PRIMARY KEY under the same table id) the base
/// holds the old arity and the overlay the new one plus deletes of the old
/// keys. The base's start bounds and `Included` end are exact for that layout
/// (`VersionedRange` drops proper prefixes of the start key, an excluded start
/// key itself, and extensions of an `Included` end key), so an old key outside
/// the overlay's exact range is never emitted: for example `DROP CONSTRAINT
/// t_pkey; ADD PRIMARY KEY (a)` on `(a, b)` then `WHERE a = 5` no longer counts
/// the old `tid||5||1`. Emission order and end bounds' omission of proper
/// prefixes of the end key stay layout-dependent; in a rekey transaction the
/// omitted base keys are old-arity keys the overlay deleted. See the module
/// docs of `mvcc.rs`.
pub(crate) struct TxnScan<'a> {
    base: std::iter::Peekable<mvcc::VersionedRange<'a>>,
    own: std::iter::Peekable<OwnIter<'a>>,
    reads: Option<&'a mut ReadSet>,
    ks: Keyspace,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    dir: Direction,
    remaining: Option<usize>,
    record: Option<usize>,
    done: bool,
}
impl TxnScan<'_> {
    fn cover(&mut self, key: Option<&[u8]>) {
        let Some(reads) = self.reads.as_mut() else {
            return;
        };
        let mut range = ReadRange {
            ks: self.ks,
            start: self.start.clone(),
            end: self.end.clone(),
        };
        if let Some(key) = key {
            match self.dir {
                Direction::Forward => range.end = Bound::Included(key.to_vec()),
                Direction::Reverse => range.start = Bound::Included(key.to_vec()),
            }
        }
        if let Some(i) = self.record {
            reads.ranges[i] = range;
        } else {
            self.record = Some(reads.ranges.len());
            reads.ranges.push(range);
        }
    }
}
impl Iterator for TxnScan<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>), TxnError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.remaining == Some(0) {
            return None;
        }
        loop {
            if matches!(self.base.peek(), Some(Err(_))) {
                self.done = true;
                return self.base.next();
            }
            let base = self
                .base
                .peek()
                .and_then(|v| v.as_ref().ok())
                .map(|(k, _)| k);
            let own = self.own.peek().map(|((_, k), _)| k);
            let choose_own = match (base, own) {
                (None, None) => {
                    self.cover(None);
                    self.done = true;
                    return None;
                }
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (Some(b), Some(o)) => match self.dir {
                    Direction::Forward => o <= b,
                    Direction::Reverse => o >= b,
                },
            };
            let item = if choose_own {
                let ((_, key), value) = self.own.next().unwrap();
                let key = key.clone();
                if base.is_some_and(|b| *b == key) {
                    self.base.next();
                }
                self.cover(Some(&key));
                value.as_ref().map(|value| (key, value.clone()))
            } else {
                let (key, value) = self.base.next().unwrap().unwrap();
                self.cover(Some(&key));
                Some((key, value))
            };
            if let Some(item) = item {
                if let Some(n) = self.remaining.as_mut() {
                    *n -= 1;
                }
                return Some(Ok(item));
            }
        }
    }
}
#[cfg(test)]
#[path = "savepoint_tests.rs"]
mod savepoint_tests;
#[cfg(test)]
#[path = "txn_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "txn_restart_tests.rs"]
mod restart_tests;
#[cfg(test)]
#[path = "txn_track_tests.rs"]
mod track_tests;
#[cfg(test)]
#[path = "txn_hold_tests.rs"]
mod hold_tests;

#[cfg(test)]
mod f19_mapping_tests {
    #[test]
    fn deadlock_remains_distinct_from_lock_timeout() {
        assert_eq!(crate::CoreError::from(super::TxnError::Deadlock), crate::CoreError::Deadlock);
        assert_eq!(crate::local::failure(crate::CoreError::Deadlock).code, 19);
        assert_eq!(crate::CoreError::from(super::TxnError::LockTimeout), crate::CoreError::Busy);
    }
}

#[cfg(test)]
#[path = "txn_drop_tests.rs"]
mod drop_tests;

#[cfg(test)]
#[path = "locks_savepoint_tests.rs"]
mod locks_savepoint_tests;

#[cfg(test)]
#[path = "statement_savepoint_tests.rs"]
mod statement_savepoint_tests;

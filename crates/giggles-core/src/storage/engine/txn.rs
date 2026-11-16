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
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::Arc,
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SerializationReason {
    WriteConflict,
    ReadWriteConflict,
    ValidationBudget,
    SnapshotTooOld,
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
    SnapshotTooOld,
    CommitAmbiguous,
    Fenced,
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
            Self::CommitAmbiguous => "08007",
            Self::SequenceGone => "42P01",
            Self::DependentObjects => "2BP01",
            Self::Fenced | Self::Misuse => "XX000",
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
            TxnError::LockLimit | TxnError::WriteSetLimit => Self::Limit,
            TxnError::Serialization(_) | TxnError::SnapshotTooOld => {
                Self::Sql(crate::relational::SqlError::Conflict)
            }
            TxnError::Deadlock => Self::Deadlock,
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
    owner: Arc<()>,
    sequence: u64,
    undo_len: usize,
    raw_len: usize,
    features_or: u64,
    minimum_subversion: u32,
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
}
/// A cluster transaction's buffered effects at commit.
pub(crate) struct ClusterCommit {
    pub(crate) isolation: Isolation,
    pub(crate) snapshot: Ts,
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
pub(crate) struct Transaction {
    services: TxnServices,
    id: TxnId,
    isolation: Isolation,
    snapshot: Option<Snapshot>,
    /// The last failure to obtain a statement timestamp (cluster only).
    snapshot_error: Option<TxnError>,
    writes: BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>,
    catalog_generation: u64,
    features_or: u64,
    minimum_subversion: u32,
    pub(crate) reclaim_after_commit: bool,
    raw: Vec<RawOp>,
    xact: Option<super::xact::XactGuard>,
    write_bytes: usize,
    savepoint_owner: Arc<()>,
    savepoint_sequence: u64,
    savepoints: Vec<(u64, usize)>,
    undo: Vec<UndoWrite>,
    undo_bytes: usize,
    locked: BTreeSet<(Keyspace, Vec<u8>)>,
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
        let mut txn = Self {
            services,
            id,
            isolation,
            snapshot: None,
            snapshot_error: None,
            writes: BTreeMap::new(),
            features_or: 0,
            minimum_subversion: 0,
            catalog_generation: 0,
            reclaim_after_commit: false,
            raw: Vec::new(),
            xact: None,
            write_bytes: 0,
            savepoint_owner: Arc::new(()),
            savepoint_sequence: 0,
            savepoints: Vec::new(),
            undo: Vec::new(),
            undo_bytes: 0,
            locked: BTreeSet::new(),
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
    pub(crate) fn lifetime(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.savepoint_owner)
    }
    /// Marks buffered writes; undo records, keys, and prior values count against
    /// the write-set limit while a savepoint remains live.
    pub(crate) fn savepoint(&mut self) -> Savepoint {
        // Never wrap and revive a released token, even at an identical undo mark.
        self.savepoint_sequence = self
            .savepoint_sequence
            .checked_add(1)
            .expect("transaction savepoint sequence exhausted");
        let mark = Savepoint {
            owner: self.savepoint_owner.clone(),
            sequence: self.savepoint_sequence,
            undo_len: self.undo.len(),
            raw_len: self.raw.len(),
            features_or: self.features_or,
            minimum_subversion: self.minimum_subversion,
        };
        self.savepoints.push((mark.sequence, mark.undo_len));
        mark
    }
    fn savepoint_index(&self, mark: &Savepoint) -> Result<usize, TxnError> {
        if !Arc::ptr_eq(&self.savepoint_owner, &mark.owner) {
            return Err(TxnError::Misuse);
        }
        self.savepoints
            .iter()
            .position(|entry| *entry == (mark.sequence, mark.undo_len))
            .ok_or(TxnError::Misuse)
    }
    /// Restores buffered writes and keeps this mark reusable, invalidating later marks.
    /// Locks, the `locked` entries permitting writes, and SERIALIZABLE reads are
    /// deliberately retained until transaction end. Unlike PostgreSQL subtransaction
    /// rollback, this does not release locks acquired after the mark.
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
                    self.writes.insert(undo.key, value);
                }
                None => {
                    self.writes.remove(&undo.key);
                }
            }
            self.write_bytes = undo.write_bytes;
            self.undo_bytes -= undo.bytes;
        }
        self.savepoints.truncate(index + 1);
        self.raw.truncate(mark.raw_len);
        self.features_or = mark.features_or;
        self.minimum_subversion = mark.minimum_subversion;
        Ok(())
    }
    /// Keeps writes while invalidating this mark and all later marks.
    pub(crate) fn release(&mut self, mark: Savepoint) -> Result<(), TxnError> {
        let index = self.savepoint_index(&mark)?;
        self.savepoints.truncate(index);
        if self.savepoints.is_empty() {
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
        if self.isolation == Isolation::ReadCommitted {
            // A failed cluster timestamp leaves no snapshot: reads then fail rather
            // than run at an older timestamp.
            self.snapshot = None;
            let _ = self.try_snapshot_ts();
        }
    }
    fn try_snapshot_ts(&mut self) -> Result<Ts, TxnError> {
        if self.snapshot.is_none() {
            match self.services.take_snapshot(self.id) {
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
    pub(crate) fn services(&self) -> &TxnServices {
        &self.services
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
    pub(crate) fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }
    pub(crate) fn catalog_own(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.writes.get(&(Keyspace::Catalog, key.to_vec()))
    }
    pub(crate) fn catalog_latest_committed(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        let version = match &self.services {
            TxnServices::Local { seq, .. } => {
                mvcc::latest_version(seq.kv(), Keyspace::Catalog, key)?
            }
            TxnServices::Cluster(cluster) => {
                // The reader holds only versions at or below a fresh oracle timestamp.
                let ts = cluster.snapshot(self.id)?;
                mvcc::latest_version(cluster.reader(ts).as_ref(), Keyspace::Catalog, key)?
            }
        };
        Ok(version.and_then(|(_, value)| value))
    }
    /// Latest committed Kernel value, ignoring this transaction's snapshot and
    /// its own buffered writes (GD-0061 C6).
    pub(crate) fn kernel_latest_committed(&self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        // Protected records are local-only (GD-0061); a cluster gateway has no form yet.
        let TxnServices::Local { seq, .. } = &self.services else {
            return Err(TxnError::Unsupported);
        };
        Ok(mvcc::latest_version(seq.kv(), Keyspace::Kernel, key)?.and_then(|(_, value)| value))
    }
    pub(crate) fn catalog_writes(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        // Metadata reads must not walk the growing row/index overlay during COPY.
        self.writes
            .range((Keyspace::Catalog, Vec::new())..)
            .take_while(|((ks, _), _)| *ks == Keyspace::Catalog)
            .map(|((_, key), value)| (key, value))
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
        self.services.acquire(self.id, key, mode, deadline, cancel)
    }
    /// Reads at the latest committed state until `end_latest`, recording no
    /// SERIALIZABLE reads; returns the transaction snapshot. FK checks at
    /// REPEATABLE READ and SERIALIZABLE use it for PostgreSQL's RI crosscheck:
    /// referencing rows are found in the latest state and fail with 40001 if
    /// newer than the transaction snapshot (GD-0075 R5-76).
    pub(crate) fn begin_latest(&mut self) -> Ts {
        let ts = self.snapshot_ts();
        debug_assert!(self.set_aside.is_none(), "nested latest-state reads");
        match self.services.take_snapshot(self.id) {
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
    /// Whether a committed version of `key` is newer than `ts`.
    pub(crate) fn changed_since(&self, ks: Keyspace, key: &[u8], ts: Ts) -> Result<bool, TxnError> {
        let latest = match &self.services {
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
        mvcc::read_at(snapshot_view(&self.services, &self.snapshot), ks, key, ts)
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
        match &self.services {
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
        let TxnServices::Local { locks, .. } = &self.services else {
            return Err(TxnError::Unsupported);
        };
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
        match &self.services {
            TxnServices::Local { seq, .. } => seq,
            TxnServices::Cluster(_) => panic!("cluster transaction has no local sequencer"),
        }
    }
    #[cfg(test)]
    pub(crate) fn sql_waiting_count(&self) -> usize {
        match &self.services {
            TxnServices::Local { locks, .. } => locks.waiting_count(),
            TxnServices::Cluster(cluster) => cluster.gateway_locks().waiting_count(),
        }
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
        let latest = match &self.services {
            TxnServices::Local { seq, locks } => {
                locks.acquire(self.id, lock, LockMode::Exclusive, deadline, cancel)?;
                mvcc::latest_version(seq.kv(), ks, key)?
            }
            TxnServices::Cluster(cluster) => cluster.lock_key(self.id, ks, key, deadline, cancel)?,
        };
        let changed = latest.as_ref().is_some_and(|(latest, _)| *latest > ts);
        let own = self.writes.get(&(ks, key.to_vec()));
        if self.isolation != Isolation::ReadCommitted && changed && own.is_none() {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict));
        }
        let value = if let Some(own) = own {
            own.clone()
        } else if self.isolation == Isolation::ReadCommitted {
            latest.and_then(|(_, v)| v)
        } else {
            mvcc::read_at(snapshot_view(&self.services, &self.snapshot), ks, key, ts)?
        };
        self.locked.insert((ks, key.to_vec()));
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
            && !self.locked.contains(&(ks, key.clone()))
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
            .is_none_or(|n| n > self.services.write_set_bytes())
        {
            return Err(TxnError::WriteSetLimit);
        }
        if undo_size == 0 {
            self.writes.insert(write_key, value);
        } else {
            let previous = self.writes.insert(write_key.clone(), value);
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
            snapshot_view(&self.services, &self.snapshot),
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
    pub(crate) fn commit(self) -> Result<Ts, TxnError> {
        if self.writes_keyspace(Keyspace::Catalog) {
            return Err(TxnError::Misuse);
        }
        self.commit_then(|_| Ok(()))
    }
    /// Runs publication after sequencer success, while this transaction still
    /// owns its locks and snapshot. Catalog writes are allowed on this path.
    /// A callback error occurs after a successful durable commit: do not retry
    /// the transaction. Drop releases locks on every return path.
    pub(crate) fn commit_then<F: FnOnce(Ts) -> Result<(), TxnError>>(
        self,
        publish: F,
    ) -> Result<Ts, TxnError> {
        self.commit_inner(None, publish)
    }
    pub(crate) fn commit_guarded<F: FnOnce(Ts) -> Result<(), TxnError>>(
        self, guard: CommitGuard, publish: F,
    ) -> Result<Ts, TxnError> {
        self.commit_inner(Some(guard), publish)
    }
    fn commit_inner<F: FnOnce(Ts) -> Result<(), TxnError>>(
        mut self, guard: Option<CommitGuard>, publish: F,
    ) -> Result<Ts, TxnError> {
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
            let ts = match &self.services {
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
        if let TxnServices::Cluster(cluster) = &self.services {
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
                    writes: std::mem::take(&mut self.writes),
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
            writes: std::mem::take(&mut self.writes),
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
        let TxnServices::Local { seq, .. } = &self.services else {
            unreachable!("cluster commits returned above");
        };
        let ts = match guard {
            Some(guard) => seq.submit_guarded(request, guard)?,
            None => seq.submit(request)?,
        };
        publish(ts)?;
        Ok(ts)
    }
    pub(crate) fn rollback(self) {}
}
impl Drop for Transaction {
    fn drop(&mut self) {
        self.services.release_all(self.id);
        self.snapshot.take();
    }
}
type OwnEntry<'a> = (&'a (Keyspace, Vec<u8>), &'a Option<Vec<u8>>);
type OwnIter<'a> = Box<dyn Iterator<Item = OwnEntry<'a>> + 'a>;
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

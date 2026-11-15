//! Transaction isolation, write ownership, and lazy read-your-writes scans.
use super::{
    codec::CodecError,
    commit::{CommitRequest, RawOp, Sequencer, SnapshotHandle, valid_counter_key},
    hlc::Ts,
    kv::{Direction, Keyspace, KvError},
    locks::{LockError, LockKey, LockManager, LockMode, TxnId},
    mvcc,
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
    LockLimit,
    WriteSetLimit,
    SnapshotTooOld,
    CommitAmbiguous,
    Fenced,
    Misuse,
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
            Self::Cancelled => "57014",
            Self::LockLimit | Self::WriteSetLimit => "53200",
            Self::CommitAmbiguous => "08007",
            Self::SequenceGone => "42P01",
            Self::DependentObjects => "2BP01",
            Self::Fenced | Self::Misuse => "XX000",
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
            TxnError::Cancelled => Self::Cancelled,
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
}
struct UndoWrite {
    key: (Keyspace, Vec<u8>),
    // None means absent from the overlay; Some(None) means a buffered tombstone.
    previous: Option<Option<Vec<u8>>>,
    write_bytes: usize,
    bytes: usize,
}
pub(crate) struct Transaction {
    seq: Arc<Sequencer>,
    locks: Arc<LockManager>,
    id: TxnId,
    isolation: Isolation,
    snapshot: Option<SnapshotHandle>,
    writes: BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>,
    catalog_generation: u64,
    features_or: u64,
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
    validation_config: ValidationConfig,
}
impl Transaction {
    pub(crate) fn begin(
        seq: &Arc<Sequencer>,
        locks: &Arc<LockManager>,
        id: TxnId,
        isolation: Isolation,
    ) -> Self {
        let mut txn = Self {
            seq: seq.clone(),
            locks: locks.clone(),
            id,
            isolation,
            snapshot: None,
            writes: BTreeMap::new(),
            features_or: 0,
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
            validation_config: ValidationConfig::default(),
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
    pub(crate) fn begin_statement(&mut self) {
        if self.isolation == Isolation::ReadCommitted {
            self.snapshot = Some(self.seq.snapshot());
        }
    }
    pub(crate) fn snapshot_ts(&mut self) -> Ts {
        if self.snapshot.is_none() {
            self.snapshot = Some(self.seq.snapshot());
        }
        self.snapshot.as_ref().unwrap().ts()
    }
    pub(crate) fn set_validation_config(&mut self, config: ValidationConfig) {
        self.validation_config = config;
    }
    pub(crate) fn current_snapshot_ts(&self) -> Option<Ts> {
        self.snapshot.as_ref().map(SnapshotHandle::ts)
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
        Ok(mvcc::latest_version(self.seq.kv(), Keyspace::Catalog, key)?
            .and_then(|(_, value)| value))
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
        self.locks.acquire(self.id, key, mode, deadline, cancel)?;
        Ok(())
    }
    pub(crate) fn get(&mut self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        let ts = self.snapshot_ts();
        if self.isolation == Isolation::Serializable {
            self.reads.points.insert((ks, key.to_vec()));
        }
        if let Some(value) = self.writes.get(&(ks, key.to_vec())) {
            return Ok(value.clone());
        }
        mvcc::read_at(self.seq.kv(), ks, key, ts)
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
        self.locks.acquire(self.id, LockKey::Row { table, pk: Vec::new() }, mode, None, cancel)?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn sql_waiting_count(&self) -> usize {
        self.locks.waiting_count()
    }
    /// Selects the snapshot before waiting. Errors keep the transaction open; rollback or drop releases its locks.
    pub(crate) fn lock_for_write(
        &mut self,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<WriteTarget, TxnError> {
        let ts = self.snapshot_ts();
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
        self.locks
            .acquire(self.id, lock, LockMode::Exclusive, deadline, cancel)?;
        let latest = mvcc::latest_version(self.seq.kv(), ks, key)?;
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
            mvcc::read_at(self.seq.kv(), ks, key, ts)?
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
        self.snapshot_ts();
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
            .is_none_or(|n| n > self.seq.write_set_bytes())
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
        let ts = self.snapshot_ts();
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
            self.seq.kv(),
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
            reads: if self.isolation == Isolation::Serializable {
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
        mut self,
        publish: F,
    ) -> Result<Ts, TxnError> {
        if let Some(xid) = self.xact_id() {
            self.raw.push(RawOp::XactCommitted { xid });
        }
        if self.writes.is_empty()
            && self.raw.is_empty()
            && self.features_or == 0
            && self.isolation != Isolation::Serializable
        {
            let ts = self.seq.visible_ts();
            publish(ts)?;
            return Ok(ts);
        }
        let snapshot = self.snapshot_ts();
        let validator = (self.isolation == Isolation::Serializable).then(|| {
            Validator::new(
                std::mem::take(&mut self.reads),
                self.validation_config.clone(),
            )
        });
        // The owned validator reads only the worker's supplied log and KV handle.
        // Drop releases locks and unregisters the snapshot on success or error.
        let ts = self.seq.submit(CommitRequest {
            snapshot,
            writes: std::mem::take(&mut self.writes),
            features_or: self.features_or,
            raw: std::mem::take(&mut self.raw),
            validation: validator.map(|validator| {
                Box::new(
                    move |log: &super::commit::RecentLog,
                          kv: &dyn super::kv::KvRead,
                          snapshot,
                          _commit| { validator.validate(log, kv, snapshot) },
                ) as _
            }),
        })?;
        publish(ts)?;
        Ok(ts)
    }
    pub(crate) fn rollback(self) {}
}
impl Drop for Transaction {
    fn drop(&mut self) {
        self.locks.release_all(self.id);
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
mod f19_mapping_tests {
    #[test]
    fn deadlock_remains_distinct_from_lock_timeout() {
        assert_eq!(crate::CoreError::from(super::TxnError::Deadlock), crate::CoreError::Deadlock);
        assert_eq!(crate::local::failure(crate::CoreError::Deadlock).code, 19);
        assert_eq!(crate::CoreError::from(super::TxnError::LockTimeout), crate::CoreError::Busy);
    }
}

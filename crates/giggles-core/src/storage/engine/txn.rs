//! Transaction isolation, write ownership, and lazy read-your-writes scans.
use super::{
    codec::CodecError,
    commit::{CommitRequest, Sequencer, SnapshotHandle},
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
            TxnError::Deadlock | TxnError::LockTimeout => Self::Busy,
            // CoreError has no dedicated unknown-outcome variant; Storage follows its unknown-outcome route.
            TxnError::CommitAmbiguous => Self::Storage("engine commit outcome unknown".into()),
            TxnError::Misuse => Self::Corrupt("engine transaction misuse".into()),
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
pub(crate) struct Transaction {
    seq: Arc<Sequencer>,
    locks: Arc<LockManager>,
    id: TxnId,
    isolation: Isolation,
    snapshot: Option<SnapshotHandle>,
    writes: BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>,
    write_bytes: usize,
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
            write_bytes: 0,
            locked: BTreeSet::new(),
            reads: ReadSet::default(),
            validation_config: ValidationConfig::default(),
        };
        if isolation == Isolation::ReadCommitted {
            txn.begin_statement();
        }
        txn
    }
    pub(crate) fn begin_statement(&mut self) {
        if self.isolation == Isolation::ReadCommitted {
            self.snapshot = Some(self.seq.snapshot());
        }
    }
    fn snapshot_ts(&mut self) -> Ts {
        if self.snapshot.is_none() {
            self.snapshot = Some(self.seq.snapshot());
        }
        self.snapshot.as_ref().unwrap().ts()
    }
    pub(crate) fn set_validation_config(&mut self, config: ValidationConfig) {
        self.validation_config = config;
    }
    #[cfg(test)]
    pub(crate) fn current_snapshot_ts(&self) -> Option<Ts> {
        self.snapshot.as_ref().map(SnapshotHandle::ts)
    }
    pub(crate) fn read_set(&self) -> &ReadSet {
        &self.reads
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
        if key.len() < 8 {
            return Err(TxnError::Misuse);
        }
        if matches!(ks, Keyspace::Rows | Keyspace::Index)
            && !self.locked.contains(&(ks, key.clone()))
        {
            return Err(TxnError::Misuse);
        }
        let old = self
            .writes
            .get(&(ks, key.clone()))
            .map_or(0, |v| key.len() + v.as_ref().map_or(0, Vec::len));
        let size = key
            .len()
            .checked_add(value.as_ref().map_or(0, Vec::len))
            .and_then(|n| self.write_bytes.checked_sub(old)?.checked_add(n))
            .ok_or(TxnError::WriteSetLimit)?;
        if size > self.seq.write_set_bytes() {
            return Err(TxnError::WriteSetLimit);
        }
        self.writes.insert((ks, key), value);
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
        let lo = start.clone();
        let hi = end.clone();
        let own = self
            .writes
            .iter()
            .filter(move |((space, key), _)| *space == ks && contains(&lo, &hi, key));
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
    pub(crate) fn commit(mut self) -> Result<Ts, TxnError> {
        if self.writes.is_empty() && self.isolation != Isolation::Serializable {
            return Ok(self.seq.visible_ts());
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
        self.seq.submit(CommitRequest {
            snapshot,
            writes: std::mem::take(&mut self.writes),
            validation: validator.map(|validator| {
                Box::new(
                    move |log: &super::commit::RecentLog,
                          kv: &dyn super::kv::KvRead,
                          snapshot,
                          _commit| { validator.validate(log, kv, snapshot) },
                ) as _
            }),
        })
    }
    pub(crate) fn rollback(self) {}
}
impl Drop for Transaction {
    fn drop(&mut self) {
        self.locks.release_all(self.id);
        self.snapshot.take();
    }
}
fn contains(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>, key: &[u8]) -> bool {
    (match start {
        Bound::Included(k) => key >= k.as_slice(),
        Bound::Excluded(k) => key > k.as_slice(),
        Bound::Unbounded => true,
    }) && (match end {
        Bound::Included(k) => key <= k.as_slice(),
        Bound::Excluded(k) => key < k.as_slice(),
        Bound::Unbounded => true,
    })
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
#[path = "txn_tests.rs"]
mod tests;

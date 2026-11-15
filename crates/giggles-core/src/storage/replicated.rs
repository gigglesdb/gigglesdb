//! Replication seam for shipping committed write batches (GD-0074 R4).
use super::engine::kv::{
    Direction, Durability, Keyspace, KvBackend, KvError, KvIter, KvPair, KvRead, WriteBatch,
};
use super::{
    engine::{
        kv::Op,
        kv_rocksdb::{RocksKv, WriterLock},
        meta,
    },
    v2,
};
use crate::{Admin, CoreError, Limits};
use std::{
    ops::Bound,
    path::Path,
    sync::{Arc, Mutex},
};

/// A replication failure. Any error means the write outcome is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicateError {
    NotLeader,
    Unavailable(String),
}

impl std::fmt::Display for ReplicateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader => f.write_str("not the leader"),
            Self::Unavailable(msg) => write!(f, "replication unavailable: {msg}"),
        }
    }
}
impl std::error::Error for ReplicateError {}

/// Ships encoded write batches through replication.
///
/// Returns `Ok` only after a majority has committed the batch and it has been
/// applied to this node's store. Any error means the outcome is unknown.
pub trait BatchSink: Send + Sync {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError>;
}

pub(crate) struct ReplicatedKv {
    store: Arc<dyn KvBackend>,
    sink: Arc<dyn BatchSink>,
}

impl ReplicatedKv {
    pub(crate) fn new(store: Arc<dyn KvBackend>, sink: Arc<dyn BatchSink>) -> Self {
        Self { store, sink }
    }
}

impl KvRead for ReplicatedKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.store.get(ks, key)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.store.range(ks, start, end, dir)
    }

    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.store.seek_ge(ks, key)
    }
}

impl KvBackend for ReplicatedKv {
    fn write(&self, batch: WriteBatch, _durability: Durability) -> Result<(), KvError> {
        // The Raft log is the durable record; only the sink applies to the store.
        self.sink
            .replicate(batch.encode())
            .map_err(|e| KvError::Io(format!("replication: {e}")))
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.store.snapshot()
    }

    fn engine_name(&self) -> &'static str {
        "replicated"
    }
}

#[cfg(test)]
#[path = "replicated_tests.rs"]
mod tests;

/// The last applied Raft log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppliedPosition {
    pub index: u64,
    pub term: u64,
}

/// Whether a committed entry changed this store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Skipped,
}

/// A replicated v2 directory and its serialized apply boundary.
pub struct ReplicaStore {
    kv: Arc<RocksKv>,
    domain: [u8; 16],
    apply_lock: Mutex<()>,
}

impl ReplicaStore {
    /// Create an empty replicated v2 directory with the supplied domain.
    pub fn create(path: impl AsRef<Path>, domain: [u8; 16]) -> Result<Self, CoreError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(super::io)?;
        let lock = WriterLock::acquire(path)?;
        let mut concrete = None;
        meta::create_with_features(path, domain, meta::REPLICATED, |p| {
            let kv = Arc::new(RocksKv::open_locked(p, 64 << 20, lock)?);
            concrete = Some(kv.clone());
            Ok(kv)
        })
        .map_err(v2::format_error)?;
        Ok(Self {
            kv: concrete.expect("create opened backend"),
            domain,
            apply_lock: Mutex::new(()),
        })
    }

    /// Open an existing replicated v2 directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CoreError> {
        let path = path.as_ref();
        if meta::classify_dir(path).map_err(v2::format_error)? != meta::DirClass::V2 {
            return Err(CoreError::Corrupt("not a v2 directory".into()));
        }
        let kv = Arc::new(RocksKv::open(path, 64 << 20)?);
        let info = meta::validate_meta(kv.as_ref()).map_err(v2::format_error)?;
        if info.features & meta::REPLICATED == 0 {
            return Err(CoreError::Unsupported);
        }
        Ok(Self {
            kv,
            domain: info.domain,
            apply_lock: Mutex::new(()),
        })
    }

    /// Return the replication domain shared by this replica group.
    pub fn domain(&self) -> [u8; 16] {
        self.domain
    }

    /// Read the last atomically applied log position.
    pub fn applied(&self) -> Result<Option<AppliedPosition>, CoreError> {
        self.kv
            .get(Keyspace::Meta, b"raft_applied")?
            .map(|bytes| {
                let bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| CoreError::Corrupt("wrong raft_applied length".into()))?;
                Ok(AppliedPosition {
                    index: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                    term: u64::from_be_bytes(bytes[8..].try_into().unwrap()),
                })
            })
            .transpose()
    }

    /// Apply a committed encoded batch and its position atomically in log order.
    pub fn apply(
        &self,
        encoded_batch: &[u8],
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, CoreError> {
        self.apply_entry(Some(encoded_batch), at)
    }

    /// Advance the applied position for a committed entry without data changes.
    pub fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, CoreError> {
        self.apply_entry(None, at)
    }

    fn apply_entry(
        &self,
        encoded: Option<&[u8]>,
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, CoreError> {
        let _guard = self
            .apply_lock
            .lock()
            .map_err(|_| CoreError::Corrupt("apply lock poisoned".into()))?;
        let cur = self.applied()?;
        if cur.is_some_and(|c| at.index <= c.index) {
            return Ok(ApplyOutcome::Skipped);
        }
        let expected = cur.map_or(1, |c| c.index + 1);
        if at.index != expected {
            return Err(CoreError::Corrupt(format!(
                "replication gap: applied {}, got {}",
                cur.map_or(0, |c| c.index),
                at.index
            )));
        }
        let mut batch = match encoded {
            Some(bytes) => {
                WriteBatch::decode(bytes).map_err(|e| CoreError::Corrupt(e.to_string()))?
            }
            None => WriteBatch::default(),
        };
        let protected = b"raft_applied".as_slice();
        if batch.ops().iter().any(|op| match op {
            Op::Put { ks, key, .. } | Op::Delete { ks, key } => {
                *ks == Keyspace::Meta && key == protected
            }
            Op::DeleteRange { ks, start, end } => {
                *ks == Keyspace::Meta && start.as_slice() <= protected && protected < end.as_slice()
            }
        }) {
            return Err(CoreError::Corrupt("batch touches raft_applied".into()));
        }
        let mut bytes = at.index.to_be_bytes().to_vec();
        bytes.extend_from_slice(&at.term.to_be_bytes());
        batch.put(Keyspace::Meta, protected.to_vec(), bytes);
        self.kv.write(batch, Durability::NoSync)?;
        Ok(ApplyOutcome::Applied)
    }

    /// Flush all applied batches to durable storage.
    pub fn flush(&self) -> Result<(), CoreError> {
        Ok(self.kv.flush_barrier()?)
    }

    /// Capture a reopenable v2 checkpoint and return its sorted file inventory.
    pub fn checkpoint(&self, target: impl AsRef<Path>) -> Result<Vec<(String, u64)>, CoreError> {
        let target = target.as_ref();
        let mut files = self.kv.checkpoint(target)?;
        meta::write_marker(target).map_err(v2::format_error)?;
        files.push((
            "GIGGLES_FORMAT".into(),
            std::fs::metadata(target.join("GIGGLES_FORMAT"))
                .map_err(super::io)?
                .len(),
        ));
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }

    /// Attach the SQL engine using the supplied replication sink for writes.
    pub fn open_admin(
        &self,
        sink: Arc<dyn BatchSink>,
        limits: Limits,
        write_set_bytes: usize,
    ) -> Result<Admin, CoreError> {
        Admin::attach_v2_backend(
            Arc::new(ReplicatedKv::new(self.kv.clone(), sink)),
            limits,
            write_set_bytes,
        )
    }
}

#[cfg(test)]
#[path = "replicated_store_tests.rs"]
mod store_tests;

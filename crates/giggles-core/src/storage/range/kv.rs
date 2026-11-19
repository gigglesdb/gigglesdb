//! Track A owns this module.
//! `RangeKv`: the KV backend of one range's leader engine (GD-0075 R5-06).
//!
//! Writes behave as R4's `ReplicatedKv` (replicated.rs): the encoded batch goes to the range's
//! replication sink, which applies it through `RangeStore::apply` on every replica. Before
//! proposing, every op is checked against the range (store.rs `check_batch`); a batch with any
//! op outside it fails with nothing proposed. Reads go to the shared store.
//!
//! The engine keeps three per-database Meta keys, `last_commit_ts`, `features` (sequencer) and
//! `gc_watermark` (gc.rs). A range engine keeps them per range, so this backend maps exact
//! `get`s and writes of them to `Meta/range/<id>/<same name>`. Meta range scans are not mapped.
//! Commit markers `Seq/[5] || xid` map to `Meta/range/<id>/marker/<xid>` the same way.
//! `subversion` stays node-wide: a batch writing it is refused (format upgrades are not run in
//! range mode).
use std::{ops::Bound, sync::Arc};

use super::store::check_batch;
use crate::storage::engine::{
    kv::{
        Direction, Durability, Keyspace, KvBackend, KvError, KvIter, KvPair, KvRead, Op, WriteBatch,
    },
    meta::{RangeLocalKey, range_local_key},
};
use crate::storage::range_api::{RangeDescriptor, RangeId};
use crate::storage::replicated::{BatchSink, ReplicateError};

/// Node-wide Meta keys the sequencer reads and writes, and their range-local homes.
const MAPPED: [(&[u8], RangeLocalKey); 3] = [
    (b"last_commit_ts", RangeLocalKey::LastCommitTs),
    (b"features", RangeLocalKey::Features),
    (b"gc_watermark", RangeLocalKey::GcWatermark),
];

fn mapped(range: RangeId, ks: Keyspace, key: &[u8]) -> Option<Vec<u8>> {
    match ks {
        Keyspace::Meta => MAPPED
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, sub)| range_local_key(range, sub)),
        // A commit marker `Seq/[5] || xid` (xact.rs marker_key) is range-local in a cluster
        // (design.md section 6, `marker/<xid>`).
        Keyspace::Seq => match key {
            [5, xid @ ..] => <[u8; 8]>::try_from(xid)
                .ok()
                .map(|xid| range_local_key(range, &RangeLocalKey::Marker(u64::from_be_bytes(xid)))),
            _ => None,
        },
        _ => None,
    }
}

/// Maps the sequencer's Meta keys of `inner` reads to the range's own keys.
struct MappedRead<R> {
    range: RangeId,
    inner: R,
}

impl<R: std::ops::Deref<Target = dyn KvRead> + Send + Sync> KvRead for MappedRead<R> {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        match mapped(self.range, ks, key) {
            Some(local) => self.inner.get(Keyspace::Meta, &local),
            None => self.inner.get(ks, key),
        }
    }

    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.inner.first_in(ks, start, end)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.inner.range(ks, start, end, dir)
    }

    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.inner.seek_ge(ks, key)
    }
}

/// KV backend of one range: bounded writes through the range's sink, reads from the store.
#[cfg_attr(not(test), allow(dead_code, reason = "R5-32 RangeEngine opens it"))]
pub(crate) struct RangeKv {
    read: MappedRead<Arc<dyn KvRead>>,
    store: Arc<dyn KvBackend>,
    descriptor: RangeDescriptor,
    sink: Arc<dyn BatchSink>,
}

#[allow(dead_code, reason = "R5-32 RangeEngine opens it")]
impl RangeKv {
    /// `store` is the node's shared store; `descriptor` the range's applied descriptor at the
    /// leader's open (a new descriptor reopens the engine).
    pub(crate) fn new(
        store: Arc<dyn KvBackend>,
        descriptor: RangeDescriptor,
        sink: Arc<dyn BatchSink>,
    ) -> Self {
        let read: Arc<dyn KvRead> = store.clone();
        Self {
            read: MappedRead {
                range: descriptor.id,
                inner: read,
            },
            store,
            descriptor,
            sink,
        }
    }

    pub(crate) fn descriptor(&self) -> &RangeDescriptor {
        &self.descriptor
    }

    /// The batch as the range applies it: mapped Meta keys moved to the range's own keys.
    fn map_batch(&self, batch: WriteBatch) -> WriteBatch {
        let range = self.descriptor.id;
        let mut out = WriteBatch::default();
        for op in batch.into_ops() {
            match op {
                Op::Put { ks, key, value } => match mapped(range, ks, &key) {
                    Some(local) => out.put(Keyspace::Meta, local, value),
                    None => out.put(ks, key, value),
                },
                Op::Delete { ks, key } => match mapped(range, ks, &key) {
                    Some(local) => out.delete(Keyspace::Meta, local),
                    None => out.delete(ks, key),
                },
                Op::DeleteRange { ks, start, end } => out.delete_range(ks, start, end),
            }
        }
        out
    }
}

impl KvRead for RangeKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.read.get(ks, key)
    }

    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.read.first_in(ks, start, end)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.read.range(ks, start, end, dir)
    }

    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.read.seek_ge(ks, key)
    }
}

impl KvBackend for RangeKv {
    fn write(&self, batch: WriteBatch, _durability: Durability) -> Result<(), KvError> {
        let batch = self.map_batch(batch);
        check_batch(&self.descriptor, &batch).map_err(|reason| {
            KvError::Corrupt(format!("write refused before proposal: {reason}"))
        })?;
        // The Raft log is the durable record; only the sink applies to the store.
        self.sink.replicate(batch.encode()).map_err(|e| match e {
            // Definitely not applied (never proposed in this term, or refused at apply):
            // a fenced KV, never an unknown outcome (R5-81).
            ReplicateError::NotLeader => KvError::Fenced,
            ReplicateError::Unavailable(_) => {
                eprintln!(
                    "range {}: write not confirmed, commit outcome unknown: {e}",
                    self.descriptor.id.0
                );
                KvError::Io(format!("replication: {e}"))
            }
        })
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        Ok(Arc::new(MappedRead {
            range: self.descriptor.id,
            inner: self.store.snapshot()?,
        }))
    }

    fn engine_name(&self) -> &'static str {
        "range"
    }
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;

//! Track A owns this module.
//! The system range's services: the timestamp oracle (GD-0075 R5-31) and allocator leases
//! (R5-38). Both run on the system range's leader, write through its replication sink and
//! are fenced with its term.
//!
//! Oracle (design.md 4a O1, section 8 R5-00b): timestamps come from a window reserved ahead by
//! the range-local high-water `Meta/range/1/oracle_hw`, written through Raft before any
//! timestamp below it is handed out. Every timestamp handed out is below the persisted
//! high-water, so a new leader that starts at the high-water it reads after its read barrier
//! is above every earlier one. A refill takes max(now_ms << 16, hw, next) + max(W, n).
//!
//! Leases: `lease(kind, id, count)` advances the allocator high-water `Seq/[kind] || id BE8`
//! (seq.rs layout: exclusive high-water u64 BE, initially 1) by `count` and returns the
//! reserved block; values never repeat across leader changes because the new high-water is
//! durable before the block is returned.
use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use super::kv::RangeKv;
use super::local::decode_u64;
use super::store::RangeStore;
use crate::storage::engine::{
    kv::{Durability, Keyspace, Kv, KvBackend, KvRead, WriteBatch},
    meta::{RangeLocalKey, range_local_key},
};
use crate::storage::range_api::{
    LeaseKind, RangeDescriptor, RangeError, SYSTEM_RANGE, SequenceOp, SequenceReply,
    SystemService, Timestamp, TsOracle,
};
use crate::storage::replicated::BatchSink;

mod sequence;

/// Default window: one second of milliseconds in the `ms << 16 | logical` layout.
pub(crate) const WINDOW: u64 = 1000 << 16;

struct Window {
    next: u64,
    limit: u64,
}

/// The system range leader's oracle and lease service for one term.
pub(crate) struct SystemRange {
    kv: Kv,
    window: Mutex<Window>,
    width: u64,
    /// Serializes lease read-advance-persist.
    leases: Mutex<()>,
    /// SQL sequence counters handed out in this term (sequence.rs).
    sequences: Mutex<sequence::Live>,
    /// The system range engine's sequencer: counter reservations go through it, so the
    /// range's feature bits have one owner (sequence.rs).
    sequencer: std::sync::OnceLock<Arc<crate::storage::engine::commit::Sequencer>>,
    closed: AtomicBool,
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn unknown(error: impl std::fmt::Display) -> RangeError {
    RangeError::Unknown(error.to_string())
}

impl SystemRange {
    /// Opens the services for a new term of the system range, after the leader's read barrier.
    pub(crate) fn open(
        store: &RangeStore,
        descriptor: RangeDescriptor,
        sink: Arc<dyn BatchSink>,
    ) -> Result<Self, RangeError> {
        Self::open_with(store, descriptor, sink, WINDOW, Box::new(wall_ms))
    }

    pub(crate) fn open_with(
        store: &RangeStore,
        descriptor: RangeDescriptor,
        sink: Arc<dyn BatchSink>,
        width: u64,
        now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self, RangeError> {
        if descriptor.id != SYSTEM_RANGE {
            return Err(RangeError::Corrupt(
                "system services outside the system range".into(),
            ));
        }
        match store.descriptor(SYSTEM_RANGE) {
            Ok(Some(stored)) if stored == descriptor => {}
            Ok(Some(stored)) => return Err(RangeError::StaleEpoch { descriptor: stored }),
            Ok(None) => return Err(RangeError::Corrupt("no system range here".into())),
            Err(e) => return Err(RangeError::Corrupt(format!("{e:?}"))),
        }
        let backend: Arc<dyn KvBackend> = store.kv().clone();
        let kv = Kv::new(Arc::new(RangeKv::new(backend, descriptor, sink)));
        // O1/O7: start at the persisted high-water; nothing below it is handed out again.
        let hw = read_u64(&kv, &oracle_hw_key())?.unwrap_or(0);
        Ok(Self {
            kv,
            window: Mutex::new(Window {
                next: hw,
                limit: hw,
            }),
            width: width.max(1),
            leases: Mutex::new(()),
            sequences: Mutex::new(sequence::Live::new()),
            sequencer: std::sync::OnceLock::new(),
            closed: AtomicBool::new(false),
            now_ms,
        })
    }

    /// Attaches the system range engine's sequencer of this term (set once by the opener).
    pub(crate) fn attach_sequencer(&self, sequencer: Arc<crate::storage::engine::commit::Sequencer>) {
        let _ = self.sequencer.set(sequencer);
    }

    /// Fences this term's services.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn check_open(&self) -> Result<(), RangeError> {
        if self.closed.load(Ordering::SeqCst) || self.kv.is_fenced() {
            return Err(RangeError::NotLeader { hint: None });
        }
        Ok(())
    }

    fn write(&self, batch: WriteBatch) -> Result<(), RangeError> {
        self.kv.write(batch, Durability::Sync).map_err(unknown)
    }
}

fn oracle_hw_key() -> Vec<u8> {
    range_local_key(SYSTEM_RANGE, &RangeLocalKey::OracleHw)
}

fn read_u64(kv: &Kv, key: &[u8]) -> Result<Option<u64>, RangeError> {
    KvRead::get(kv, Keyspace::Meta, key)
        .map_err(|e| RangeError::Corrupt(e.to_string()))?
        .map(|v| decode_u64(&v).ok_or_else(|| RangeError::Corrupt("bad oracle_hw".into())))
        .transpose()
}

impl TsOracle for SystemRange {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        self.check_open()?;
        let n = u64::from(n.max(1));
        let mut window = self.window.lock().unwrap_or_else(|p| p.into_inner());
        // R5-101: timestamps follow the leader's clock (never below now_ms << 16), so a
        // timestamp's ms part is a wall time the GC TTL (engine/reclaim.rs) can compare with.
        let now = (self.now_ms)() << 16;
        window.next = window.next.max(now);
        if window.next.saturating_add(n) > window.limit {
            let from = now.max(window.limit).max(window.next);
            let limit = from
                .checked_add(self.width.max(n))
                .ok_or_else(|| RangeError::Corrupt("timestamp space exhausted".into()))?;
            let mut batch = WriteBatch::default();
            batch.put(
                Keyspace::Meta,
                oracle_hw_key(),
                limit.to_be_bytes().to_vec(),
            );
            // The new high-water is durable before any timestamp below it is handed out.
            self.write(batch)?;
            window.next = from;
            window.limit = limit;
        }
        let first = window.next;
        window.next += n;
        Ok(Timestamp(first))
    }
}

impl SystemService for SystemRange {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }

    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError> {
        self.check_open()?;
        if count == 0 {
            return Err(RangeError::Corrupt("empty lease".into()));
        }
        // One lease of a sequence at a time: read, advance and persist its high-water.
        let leases = self.leases.lock().unwrap_or_else(|p| p.into_inner());
        let mut key = vec![kind as u8];
        key.extend_from_slice(&id.to_be_bytes());
        let start = match KvRead::get(&self.kv, Keyspace::Seq, &key)
            .map_err(|e| RangeError::Corrupt(e.to_string()))?
        {
            None => 1,
            Some(value) => decode_u64(&value)
                .filter(|hw| *hw > 0)
                .ok_or_else(|| RangeError::Corrupt("bad allocator high-water".into()))?,
        };
        let end = start
            .checked_add(count)
            .ok_or_else(|| RangeError::Corrupt("allocator exhausted".into()))?;
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Seq, key, end.to_be_bytes().to_vec());
        self.write(batch)?;
        drop(leases);
        Ok(start..end)
    }

    fn sequence(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        self.sequence_op(id, incarnation, op)
    }
}

#[cfg(test)]
#[path = "oracle_tests.rs"]
mod tests;

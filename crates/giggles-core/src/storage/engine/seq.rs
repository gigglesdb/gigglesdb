//! Durable, non-transactional sequence reservations. Gaps after restart are expected.
use super::{
    kv::{Durability, Keyspace, Kv, KvError, WriteBatch},
    txn::TxnError,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

const DEFAULT_BLOCK: u64 = 1024;
// DML reserves 1,000,001 versions per statement. Amortize its Sync over 64
// statements without allocating memory proportional to the reservation.
const ROW_VERSION_CHUNK: u64 = 64 * 1_000_001;
type SequenceKey = [u8; 9];

#[derive(Default)]
struct Block {
    cur: u64,
    end: u64,
}

/// Share one allocator for a KV handle: its per-sequence locks serialize reservation
/// reads and writes. The map lock is released before waiting on a sequence or KV I/O.
/// Kind 0 / id 0 allocates object IDs, kind 1 / table ID allocates row IDs, and
/// kind 2 is reserved. Kind 3 / id 0 allocates global row-version IDs.
/// Keys use `kind || id_be`; values are the exclusive durable
/// highwater, initially 1, rather than the last value handed to a caller.
pub(crate) struct SequenceAllocator {
    kv: Arc<Kv>,
    block: u64,
    blocks: Mutex<BTreeMap<SequenceKey, Arc<Mutex<Block>>>>,
}

impl SequenceAllocator {
    /// A zero block size selects the default of 1024. Values start at 1.
    pub(crate) fn new(kv: Arc<Kv>, block: u64) -> Self {
        Self {
            kv,
            block: if block == 0 { DEFAULT_BLOCK } else { block },
            blocks: Mutex::new(BTreeMap::new()),
        }
    }

    /// Reserves a full block synchronously before returning any value from it.
    /// Exhaustion (including a block that would overflow its exclusive highwater)
    /// returns `Misuse` without changing the sequence. Thus `u64::MAX` cannot be
    /// issued: its exclusive highwater is not representable.
    pub(crate) fn next(&self, kind: u8, id: u64) -> Result<u64, TxnError> {
        let key = sequence_key(kind, id);
        let slot = self.slot(key)?;
        let mut block = slot.lock().map_err(|_| TxnError::Misuse)?;
        self.check_live()?;
        if block.cur == block.end {
            let start = self.highwater(&key)?;
            let end = start.checked_add(self.block).ok_or(TxnError::Misuse)?;
            self.persist_highwater(key, end)?;
            // Do not publish the reservation until Sync succeeds. An ambiguous
            // write error is propagated; Kv fences the shared handle itself.
            block.cur = start;
            block.end = end;
        }
        let value = block.cur;
        block.cur += 1; // cur < end, so this cannot overflow.
        Ok(value)
    }

    /// Reserves `n` contiguous values from a durable cached block when possible.
    /// Otherwise abandon its remainder and synchronously reserve a new chunk.
    /// next() and ranges consume the same cursor under the same sequence lock.
    /// Zero length and exclusive-highwater overflow leave the sequence unchanged.
    pub(crate) fn reserve_range(
        &self,
        kind: u8,
        id: u64,
        n: u64,
    ) -> Result<std::ops::Range<u64>, crate::CoreError> {
        if n == 0 {
            return Err(TxnError::Misuse.into());
        }
        let key = sequence_key(kind, id);
        let slot = self.slot(key)?;
        let mut block = slot.lock().map_err(|_| TxnError::Misuse)?;
        self.check_live()?;
        if n <= block.end - block.cur {
            let start = block.cur;
            block.cur += n;
            return Ok(start..block.cur);
        }
        let start = self.highwater(&key)?;
        let end = start.checked_add(n).ok_or(TxnError::Misuse)?;
        let chunk = if kind == 3 && id == 0 {
            ROW_VERSION_CHUNK
        } else {
            self.block
        };
        // Near exhaustion reserve the representable tail, provided n fits.
        let reserved_end = start.saturating_add(n.max(chunk));
        self.persist_highwater(key, reserved_end)?;
        // Publish only after Sync succeeds, just as next() does.
        block.cur = end;
        block.end = reserved_end;
        Ok(start..end)
    }

    /// Makes every subsequent allocation at least `floor`, without rewinding.
    /// Any cached block containing values below the floor is discarded. Raising
    /// the persisted highwater uses Sync before the cached state is changed.
    pub(crate) fn ensure_at_least(&self, kind: u8, id: u64, floor: u64) -> Result<(), TxnError> {
        let key = sequence_key(kind, id);
        let slot = self.slot(key)?;
        let mut block = slot.lock().map_err(|_| TxnError::Misuse)?;
        self.check_live()?;
        let highwater = self.highwater(&key)?;
        if floor > highwater {
            self.persist_highwater(key, floor)?;
        }
        if floor > block.cur {
            // Refill from the durable highwater on next(), abandoning the old
            // reservation even when the floor falls inside it.
            block.cur = 0;
            block.end = 0;
        }
        Ok(())
    }

    fn slot(&self, key: SequenceKey) -> Result<Arc<Mutex<Block>>, TxnError> {
        let mut blocks = self.blocks.lock().map_err(|_| TxnError::Misuse)?;
        Ok(blocks.entry(key).or_default().clone())
    }

    fn check_live(&self) -> Result<(), TxnError> {
        if self.kv.is_fenced() {
            Err(KvError::Fenced.into())
        } else {
            Ok(())
        }
    }

    fn highwater(&self, key: &SequenceKey) -> Result<u64, TxnError> {
        let Some(value) = self.kv.get(Keyspace::Seq, key)? else {
            return Ok(1);
        };
        let bytes: [u8; 8] = value.try_into().map_err(|_| {
            KvError::Corrupt("sequence highwater must contain exactly eight bytes".into())
        })?;
        let highwater = u64::from_be_bytes(bytes);
        if highwater == 0 {
            return Err(KvError::Corrupt("sequence highwater must be positive".into()).into());
        }
        Ok(highwater)
    }

    fn persist_highwater(&self, key: SequenceKey, highwater: u64) -> Result<(), TxnError> {
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Seq,
            key.to_vec(),
            highwater.to_be_bytes().to_vec(),
        );
        self.kv.write(batch, Durability::Sync)?;
        Ok(())
    }
}

fn sequence_key(kind: u8, id: u64) -> SequenceKey {
    let mut key = [0; 9];
    key[0] = kind;
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

#[cfg(test)]
mod range_tests {
    use super::*;
    use crate::storage::engine::kv_mem::MemKv;
    use std::{collections::BTreeSet, sync::Barrier, thread};

    #[test]
    fn concurrent_next_and_ranges_never_overlap_and_survive_reopen() {
        let mem = Arc::new(MemKv::new());
        let kv = Arc::new(Kv::new(mem.clone()));
        let seq = Arc::new(SequenceAllocator::new(kv.clone(), 7));
        assert_eq!(seq.next(1, 42).unwrap(), 1);
        let barrier = Arc::new(Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let seq = seq.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let mut values = Vec::new();
                    for iteration in 0..100 {
                        if (worker + iteration) % 2 == 0 {
                            values.extend(seq.reserve_range(1, 42, 3).unwrap());
                        } else {
                            values.push(seq.next(1, 42).unwrap());
                        }
                    }
                    values
                })
            })
            .collect();
        let mut seen = BTreeSet::from([1]);
        for worker in workers {
            for value in worker.join().unwrap() {
                assert!(seen.insert(value), "duplicate sequence value {value}");
            }
        }
        assert_eq!(seen.len(), 1601);
        let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 7);
        assert!(reopened.next(1, 42).unwrap() > *seen.last().unwrap());
    }

    #[test]
    fn range_highwater_is_synced_before_return_and_survives_crash() {
        let mem = Arc::new(MemKv::new());
        let seq = SequenceAllocator::new(Arc::new(Kv::new(mem.clone())), 7);
        assert_eq!(seq.next(0, 0).unwrap(), 1);
        let reserved = seq.reserve_range(0, 0, 11).unwrap();
        assert_eq!(reserved, 8..19);
        // No later write or flush may make a NoSync reservation durable.
        let crashed = Arc::new(Kv::new(Arc::new(mem.crash())));
        assert_eq!(
            crashed.get(Keyspace::Seq, &sequence_key(0, 0)).unwrap(),
            Some(19_u64.to_be_bytes().to_vec())
        );
        let reopened = SequenceAllocator::new(crashed, 7);
        assert_eq!(reopened.next(0, 0).unwrap(), reserved.end);
        assert!(seq.next(0, 0).unwrap() >= reserved.end);
    }

    #[test]
    fn invalid_range_preserves_cached_values() {
        let seq = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(MemKv::new()))), 7);
        assert_eq!(seq.next(0, 0).unwrap(), 1);
        assert!(seq.reserve_range(0, 0, 0).is_err());
        assert!(seq.reserve_range(0, 0, u64::MAX).is_err());
        assert_eq!(seq.next(0, 0).unwrap(), 2);
    }

    #[test]
    fn row_version_chunks_serve_64_statements_before_refill() {
        let mem = Arc::new(MemKv::new());
        let seq = SequenceAllocator::new(Arc::new(Kv::new(mem.clone())), 7);
        let first = seq.reserve_range(3, 0, 1_000_001).unwrap();
        assert_eq!(first, 1..1_000_002);
        // Leave a write failure armed: cached ranges must perform no writes.
        mem.fail_next_write(KvError::Io("refill boundary".into()));
        let mut end = first.end;
        for _ in 1..64 {
            let range = seq.reserve_range(3, 0, 1_000_001).unwrap();
            assert_eq!(range.start, end);
            end = range.end;
        }
        assert_eq!(end, 1 + ROW_VERSION_CHUNK);
        let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 7);
        assert!(reopened.reserve_range(3, 0, 1_000_001).unwrap().start >= end);
        assert!(seq.reserve_range(3, 0, 1_000_001).is_err());
        assert!(seq.next(3, 0).is_err());
    }

    #[test]
    fn ranges_share_next_cursor_and_obey_floors_and_overflow() {
        let mem = Arc::new(MemKv::new());
        let kv = Arc::new(Kv::new(mem.clone()));
        let seq = SequenceAllocator::new(kv, 8);
        assert_eq!(seq.reserve_range(0, 0, 3).unwrap(), 1..4);
        assert_eq!(seq.next(0, 0).unwrap(), 4);
        assert_eq!(seq.reserve_range(0, 0, 2).unwrap(), 5..7);
        seq.ensure_at_least(0, 0, 8).unwrap();
        assert!(seq.reserve_range(0, 0, 2).unwrap().start >= 8);
        seq.ensure_at_least(0, 0, u64::MAX - 4).unwrap();
        assert!(seq.reserve_range(0, 0, 5).is_err());
        assert_eq!(seq.reserve_range(0, 0, 4).unwrap(), u64::MAX - 4..u64::MAX);
        assert!(seq.reserve_range(0, 0, 1).is_err());
        let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 8);
        assert!(reopened.reserve_range(0, 0, 1).is_err());
    }
}

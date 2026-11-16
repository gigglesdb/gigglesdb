//! Durable, non-transactional sequence reservations. Gaps after restart are expected.
use super::{
    kv::{Durability, Keyspace, Kv, KvError, WriteBatch},
    txn::TxnError,
};
use crate::range_api::{LeaseKind, RangeClient, RangeError};
use std::{
    collections::BTreeMap,
    ops::Range,
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
/// A leased allocator (a cluster gateway) has no local Seq keyspace: every block is
/// a `RangeClient::lease` on the system range, which is durable server side.
pub(crate) struct SequenceAllocator {
    source: Source,
    block: u64,
    blocks: Mutex<BTreeMap<SequenceKey, Arc<Mutex<Block>>>>,
}

enum Source {
    Kv(Arc<Kv>),
    #[cfg_attr(not(test), allow(dead_code, reason = "R5-43 gateway wiring is pending"))]
    Leases(Arc<dyn RangeClient>),
}

impl SequenceAllocator {
    /// A zero block size selects the default of 1024. Values start at 1.
    pub(crate) fn new(kv: Arc<Kv>, block: u64) -> Self {
        Self::with_source(Source::Kv(kv), block)
    }

    /// Allocates from system-range leases; a zero block size selects 1024.
    #[cfg_attr(not(test), allow(dead_code, reason = "R5-43 gateway wiring is pending"))]
    pub(crate) fn leased(client: Arc<dyn RangeClient>, block: u64) -> Self {
        Self::with_source(Source::Leases(client), block)
    }

    fn with_source(source: Source, block: u64) -> Self {
        Self {
            source,
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
            let (start, end) = match &self.source {
                Source::Kv(kv) => {
                    let start = highwater(kv, &key)?;
                    let end = start.checked_add(self.block).ok_or(TxnError::Misuse)?;
                    persist_highwater(kv, key, end)?;
                    (start, end)
                }
                Source::Leases(client) => {
                    let lease = lease(client.as_ref(), kind, id, self.block)?;
                    (lease.start, lease.end)
                }
            };
            // Do not publish the reservation until Sync succeeds. An ambiguous
            // write error is propagated; Kv fences the shared handle itself.
            block.cur = start;
            block.end = end;
        }
        let value = block.cur;
        block.cur += 1; // cur < end, so this cannot overflow.
        Ok(value)
    }

    /// Returns the next value without reserving or consuming it. A leased
    /// allocator with an empty block leases one first, so `next` returns it.
    pub(crate) fn peek(&self, kind: u8, id: u64) -> Result<u64, TxnError> {
        let key = sequence_key(kind, id);
        let slot = self.slot(key)?;
        let mut block = slot.lock().map_err(|_| TxnError::Misuse)?;
        self.check_live()?;
        if block.cur < block.end {
            return Ok(block.cur);
        }
        match &self.source {
            Source::Kv(kv) => highwater(kv, &key),
            Source::Leases(client) => {
                let lease = lease(client.as_ref(), kind, id, self.block)?;
                block.cur = lease.start;
                block.end = lease.end;
                Ok(lease.start)
            }
        }
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
        let chunk = if kind == 3 && id == 0 {
            ROW_VERSION_CHUNK
        } else {
            self.block
        };
        let (start, end, reserved_end) = match &self.source {
            Source::Kv(kv) => {
                let start = highwater(kv, &key)?;
                let end = start.checked_add(n).ok_or(TxnError::Misuse)?;
                // Near exhaustion reserve the representable tail, provided n fits.
                let reserved_end = start.saturating_add(n.max(chunk));
                persist_highwater(kv, key, reserved_end)?;
                (start, end, reserved_end)
            }
            Source::Leases(client) => {
                // One lease is one contiguous range of exactly n.max(chunk) values.
                let lease = lease(client.as_ref(), kind, id, n.max(chunk))?;
                (lease.start, lease.start + n, lease.end)
            }
        };
        // Publish only after Sync succeeds, just as next() does.
        block.cur = end;
        block.end = reserved_end;
        Ok(start..end)
    }

    /// Makes every subsequent allocation at least `floor`, without rewinding.
    /// Any cached block containing values below the floor is discarded. Raising
    /// the persisted highwater uses Sync before the cached state is changed.
    /// A leased allocator discards its block and leases past the floor, wasting
    /// the values below it.
    pub(crate) fn ensure_at_least(&self, kind: u8, id: u64, floor: u64) -> Result<(), TxnError> {
        let key = sequence_key(kind, id);
        let slot = self.slot(key)?;
        let mut block = slot.lock().map_err(|_| TxnError::Misuse)?;
        self.check_live()?;
        let client = match &self.source {
            Source::Kv(kv) => {
                let highwater = highwater(kv, &key)?;
                if floor > highwater {
                    persist_highwater(kv, key, floor)?;
                }
                if floor > block.cur {
                    // Refill from the durable highwater on next(), abandoning the old
                    // reservation even when the floor falls inside it.
                    block.cur = 0;
                    block.end = 0;
                }
                return Ok(());
            }
            Source::Leases(client) => client.as_ref(),
        };
        if floor <= block.cur && block.cur < block.end {
            // The cached block and every later lease are already at or above the floor.
            return Ok(());
        }
        block.cur = 0;
        block.end = 0;
        // Leases of one sequence never go backwards, so this ends within three leases.
        let mut count = 1;
        let mut previous_end = None;
        loop {
            let lease = lease(client, kind, id, count)?;
            if previous_end.is_some_and(|end| lease.start < end) {
                return Err(KvError::Corrupt(format!(
                    "sequence lease {lease:?} precedes an earlier lease"
                ))
                .into());
            }
            if lease.end > floor {
                block.cur = lease.start.max(floor);
                block.end = lease.end;
                return Ok(());
            }
            previous_end = Some(lease.end);
            count = (floor - lease.end).max(1);
        }
    }

    fn slot(&self, key: SequenceKey) -> Result<Arc<Mutex<Block>>, TxnError> {
        let mut blocks = self.blocks.lock().map_err(|_| TxnError::Misuse)?;
        Ok(blocks.entry(key).or_default().clone())
    }

    fn check_live(&self) -> Result<(), TxnError> {
        match &self.source {
            Source::Kv(kv) if kv.is_fenced() => Err(KvError::Fenced.into()),
            _ => Ok(()),
        }
    }
}

fn highwater(kv: &Kv, key: &SequenceKey) -> Result<u64, TxnError> {
    let Some(value) = kv.get(Keyspace::Seq, key)? else {
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

fn persist_highwater(kv: &Kv, key: SequenceKey, highwater: u64) -> Result<(), TxnError> {
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Seq,
        key.to_vec(),
        highwater.to_be_bytes().to_vec(),
    );
    kv.write(batch, Durability::Sync)?;
    Ok(())
}

/// Leases exactly `count` values; a lease of any other length is corrupt and
/// none of its values are handed out. Retries are the client's business.
fn lease(client: &dyn RangeClient, kind: u8, id: u64, count: u64) -> Result<Range<u64>, TxnError> {
    let lease_kind = match kind {
        0 => LeaseKind::ObjectId,
        1 => LeaseKind::HeapRowId,
        3 => LeaseKind::RowVersion,
        4 => LeaseKind::Xid,
        _ => return Err(TxnError::Unsupported),
    };
    let lease = client
        .lease(lease_kind, id, count)
        .map_err(|error| match error {
            RangeError::Corrupt(message) => KvError::Corrupt(message),
            error => KvError::Io(error.to_string()),
        })?;
    if lease.end.checked_sub(lease.start) != Some(count) {
        return Err(KvError::Corrupt(format!(
            "sequence lease of {count} values returned {lease:?}"
        ))
        .into());
    }
    Ok(lease)
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

#[cfg(test)]
pub(super) mod lease_tests {
    use super::*;
    use crate::range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LockGrant, LockRequest, Outcome,
        RangeDescriptor, RangeId, RangeWrite, ReadPage, ReadRequest, Timestamp, TsOracle,
    };
    use std::{
        collections::{BTreeSet, HashMap},
        sync::{
            Barrier,
            atomic::{AtomicBool, Ordering},
        },
        thread,
    };

    /// System-range stand-in: per-(kind, id) lease counters starting at 1.
    #[derive(Default)]
    pub(crate) struct StubClient {
        next: Mutex<HashMap<(u8, u64), u64>>,
        /// Hands out one value fewer than requested.
        short: AtomicBool,
        /// Fails the next lease with this error.
        fail: Mutex<Option<RangeError>>,
    }

    impl StubClient {
        fn issued(&self, kind: LeaseKind, id: u64) -> u64 {
            let next = self.next.lock().unwrap();
            next.get(&(kind as u8, id)).copied().unwrap_or(1)
        }
    }

    impl RangeClient for StubClient {
        fn descriptor_for(&self, _: &[u8]) -> Result<RangeDescriptor, RangeError> {
            unimplemented!()
        }
        fn invalidate(&self, _: &RangeDescriptor) {
            unimplemented!()
        }
        fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
            unimplemented!()
        }
        fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
            unimplemented!()
        }
        fn blockers(&self, _: RangeId, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
            unimplemented!()
        }
        fn cancel_wait(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
            unimplemented!()
        }
        fn release(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
            unimplemented!()
        }
        fn heartbeat(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
            unimplemented!()
        }
        fn commit(
            &self,
            _: RangeId,
            _: Epoch,
            _: ClusterTxnId,
            _: Vec<RangeWrite>,
            _: Option<u64>,
            _: Option<u64>,
        ) -> Result<Timestamp, RangeError> {
            unimplemented!()
        }
        fn prepare(
            &self,
            _: RangeId,
            _: Epoch,
            _: ClusterTxnId,
            _: RangeId,
            _: Vec<RangeWrite>,
            _: Option<u64>,
        ) -> Result<Timestamp, RangeError> {
            unimplemented!()
        }
        fn finalize(&self, _: RangeId, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
            unimplemented!()
        }
        fn home(&self, _: RangeId, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
            unimplemented!()
        }
        fn oracle(&self) -> &dyn TsOracle {
            unimplemented!()
        }
        fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError> {
            if let Some(error) = self.fail.lock().unwrap().take() {
                return Err(error);
            }
            let mut next = self.next.lock().unwrap();
            let cursor = next.entry((kind as u8, id)).or_insert(1);
            let start = *cursor;
            *cursor = start
                .checked_add(count)
                .ok_or_else(|| RangeError::Unavailable("sequence exhausted".into()))?;
            let short = u64::from(self.short.load(Ordering::SeqCst));
            Ok(start..*cursor - short)
        }
    }

    fn leased(stub: &Arc<StubClient>, block: u64) -> SequenceAllocator {
        SequenceAllocator::leased(stub.clone(), block)
    }

    #[test]
    fn two_leased_allocators_never_overlap_and_ranges_are_contiguous() {
        let stub = Arc::new(StubClient::default());
        let allocators = [Arc::new(leased(&stub, 7)), Arc::new(leased(&stub, 7))];
        let barrier = Arc::new(Barrier::new(16));
        let workers: Vec<_> = (0..16)
            .map(|worker| {
                let seq = allocators[worker % 2].clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    let mut values = Vec::new();
                    for iteration in 0..100 {
                        let n = [1, 3, 10][(worker + iteration) % 3];
                        if n == 1 {
                            values.push(seq.next(1, 42).unwrap());
                        } else {
                            let range = seq.reserve_range(1, 42, n).unwrap();
                            assert_eq!(range.end - range.start, n);
                            values.extend(range);
                        }
                    }
                    values
                })
            })
            .collect();
        let mut seen = BTreeSet::new();
        for worker in workers {
            for value in worker.join().unwrap() {
                assert!(seen.insert(value), "duplicate sequence value {value}");
            }
        }
        let expected: u64 = (0..16)
            .flat_map(|worker| (0..100).map(move |iteration| [1, 3, 10][(worker + iteration) % 3]))
            .sum();
        assert_eq!(seen.len() as u64, expected);
        assert!(*seen.first().unwrap() >= 1);
        assert!(*seen.last().unwrap() < stub.issued(LeaseKind::HeapRowId, 42));
    }

    #[test]
    fn row_version_ranges_lease_one_chunk() {
        let stub = Arc::new(StubClient::default());
        let seq = leased(&stub, 7);
        let first = seq.reserve_range(3, 0, 1_000_001).unwrap();
        assert_eq!(first, 1..1_000_002);
        assert_eq!(stub.issued(LeaseKind::RowVersion, 0), 1 + ROW_VERSION_CHUNK);
        assert_eq!(seq.reserve_range(3, 0, 1_000_001).unwrap().start, first.end);
        assert_eq!(stub.issued(LeaseKind::RowVersion, 0), 1 + ROW_VERSION_CHUNK);
    }

    #[test]
    fn peek_leases_the_value_next_returns() {
        let stub = Arc::new(StubClient::default());
        let seq = leased(&stub, 4);
        let other = leased(&stub, 4);
        let peeked = seq.peek(0, 0).unwrap();
        assert_eq!(other.next(0, 0).unwrap(), 5);
        assert_eq!(seq.next(0, 0).unwrap(), peeked);
        for expected in 2..5 {
            assert_eq!(seq.next(0, 0).unwrap(), expected);
        }
        let peeked = seq.peek(0, 0).unwrap();
        assert_eq!(peeked, 9);
        assert_eq!(seq.peek(0, 0).unwrap(), peeked);
        assert_eq!(seq.next(0, 0).unwrap(), peeked);
    }

    #[test]
    fn ensure_at_least_leases_past_the_floor() {
        let stub = Arc::new(StubClient::default());
        let seq = leased(&stub, 7);
        assert_eq!(seq.next(0, 0).unwrap(), 1);
        seq.ensure_at_least(0, 0, 100).unwrap();
        assert!(seq.peek(0, 0).unwrap() >= 100);
        assert!(seq.next(0, 0).unwrap() >= 100);
        assert!(seq.reserve_range(0, 0, 20).unwrap().start >= 100);
        for _ in 0..20 {
            assert!(seq.next(0, 0).unwrap() >= 100);
        }
        // A floor below the cursor still discards the block but never rewinds.
        let before = seq.next(0, 0).unwrap();
        seq.ensure_at_least(0, 0, 2).unwrap();
        assert!(seq.next(0, 0).unwrap() > before);
    }

    #[test]
    fn unsupported_kinds_are_refused_without_leasing() {
        let stub = Arc::new(StubClient::default());
        let seq = leased(&stub, 7);
        for kind in [2, 5, u8::MAX] {
            assert_eq!(seq.next(kind, 0), Err(TxnError::Unsupported));
            assert_eq!(seq.peek(kind, 0), Err(TxnError::Unsupported));
            assert!(seq.reserve_range(kind, 0, 3).is_err());
            assert_eq!(seq.ensure_at_least(kind, 0, 10), Err(TxnError::Unsupported));
        }
        assert!(stub.next.lock().unwrap().is_empty());
    }

    #[test]
    fn bad_leases_and_client_errors_hand_out_nothing() {
        let stub = Arc::new(StubClient::default());
        let seq = leased(&stub, 7);
        stub.short.store(true, Ordering::SeqCst);
        fn corrupt<T>(result: Result<T, TxnError>) -> bool {
            matches!(result, Err(TxnError::Kv(KvError::Corrupt(_))))
        }
        assert!(corrupt(seq.next(0, 0)));
        assert!(corrupt(seq.peek(0, 0)));
        assert!(seq.reserve_range(0, 0, 3).is_err());
        assert!(corrupt(seq.ensure_at_least(0, 0, 50)));
        // An empty lease (a one-value request shortened to zero) is corrupt too.
        assert!(corrupt(seq.ensure_at_least(0, 0, 1)));
        stub.short.store(false, Ordering::SeqCst);
        // Nothing from the short leases was cached: next takes a fresh lease.
        let value = seq.next(0, 0).unwrap();
        assert_eq!(value, stub.issued(LeaseKind::ObjectId, 0) - 7);
        *stub.fail.lock().unwrap() = Some(RangeError::Unavailable("down".into()));
        assert_eq!(
            seq.next(1, 9),
            Err(TxnError::Kv(KvError::Io("unavailable: down".into())))
        );
        *stub.fail.lock().unwrap() = Some(RangeError::Corrupt("bad".into()));
        assert_eq!(
            seq.next(1, 9),
            Err(TxnError::Kv(KvError::Corrupt("bad".into())))
        );
        assert_eq!(seq.next(1, 9).unwrap(), 1);
    }
}

//! Durable on-demand transaction IDs and committed-marker history.
use super::{
    kv::{Durability, Keyspace, Kv, WriteBatch},
    seq::SequenceAllocator,
    txn::TxnError,
};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
pub(crate) const FIRST_XID: u64 = 3;
pub(crate) const XACT_RETAIN: u64 = 1 << 20;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XactStatus {
    Committed,
    Aborted,
    InProgress,
}
#[derive(Debug)]
pub(crate) enum XactLookupError {
    Future,
    /// A cluster gateway: the xid is not live here, its status is in the system range
    /// (`ClusterServices::xact_status`, R5-55).
    Cluster,
    Txn(TxnError),
}
pub(crate) struct XactIds {
    /// None on a cluster gateway: markers are range-local there.
    kv: Option<Arc<Kv>>,
    seq: Arc<SequenceAllocator>,
    live: Arc<Mutex<BTreeSet<u64>>>,
    pruned: AtomicU64,
}
impl XactIds {
    pub(crate) fn new(kv: Arc<Kv>, seq: Arc<SequenceAllocator>) -> Self {
        Self::with_kv(Some(kv), seq)
    }
    /// Xids from a leased allocator, with no local markers to prune or read.
    #[cfg_attr(not(test), allow(dead_code, reason = "R5-43 gateway wiring is pending"))]
    pub(crate) fn leased(seq: Arc<SequenceAllocator>) -> Self {
        Self::with_kv(None, seq)
    }
    fn with_kv(kv: Option<Arc<Kv>>, seq: Arc<SequenceAllocator>) -> Self {
        Self {
            kv,
            seq,
            live: Arc::new(Mutex::new(BTreeSet::new())),
            pruned: AtomicU64::new(0),
        }
    }
    pub(crate) fn assign(&self) -> Result<XactGuard, TxnError> {
        let mut live = self.live.lock().map_err(|_| TxnError::Misuse)?;
        let id = self
            .seq
            .next(4, 0)?
            .checked_add(2)
            .ok_or(TxnError::Misuse)?;
        live.insert(id);
        drop(live);
        let guard = XactGuard {
            id,
            live: self.live.clone(),
        };
        // Serialize pruning; on error the new guard is dropped after this lock.
        if let Some(kv) = &self.kv {
            let _live = self.live.lock().map_err(|_| TxnError::Misuse)?;
            let next = self
                .seq
                .peek(4, 0)?
                .checked_add(2)
                .ok_or(TxnError::Misuse)?;
            let cutoff = next.saturating_sub(XACT_RETAIN);
            if cutoff.saturating_sub(self.pruned.load(Ordering::Relaxed)) >= 4096 {
                let mut batch = WriteBatch::default();
                batch.delete_range(Keyspace::Seq, marker_key(0), marker_key(cutoff));
                kv.write(batch, Durability::NoSync)?;
                self.pruned.store(cutoff, Ordering::Relaxed);
            }
        }
        Ok(guard)
    }
    pub(crate) fn status(&self, xid: u64) -> Result<Option<XactStatus>, XactLookupError> {
        if xid == 0 {
            return Ok(None);
        }
        if xid < FIRST_XID {
            return Ok(Some(XactStatus::Committed));
        }
        let Some(kv) = &self.kv else {
            let live = self
                .live
                .lock()
                .map_err(|_| XactLookupError::Txn(TxnError::Misuse))?;
            if live.contains(&xid) {
                return Ok(Some(XactStatus::InProgress));
            }
            return Err(XactLookupError::Cluster);
        };
        let next = self
            .seq
            .peek(4, 0)
            .map_err(XactLookupError::Txn)?
            .checked_add(2)
            .ok_or(XactLookupError::Txn(TxnError::Misuse))?;
        if xid >= next {
            return Err(XactLookupError::Future);
        }
        if xid < next.saturating_sub(XACT_RETAIN) {
            return Ok(None);
        }
        let live = self
            .live
            .lock()
            .map_err(|_| XactLookupError::Txn(TxnError::Misuse))?;
        if live.contains(&xid) {
            return Ok(Some(XactStatus::InProgress));
        }
        let marker = kv
            .get(Keyspace::Seq, &marker_key(xid))
            .map_err(|e| XactLookupError::Txn(TxnError::Kv(e)))?;
        Ok(Some(if marker.is_some() {
            XactStatus::Committed
        } else {
            XactStatus::Aborted
        }))
    }
}
pub(crate) fn marker_key(xid: u64) -> Vec<u8> {
    let mut key = vec![5];
    key.extend_from_slice(&xid.to_be_bytes());
    key
}
pub(crate) struct XactGuard {
    id: u64,
    live: Arc<Mutex<BTreeSet<u64>>>,
}
impl XactGuard {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}
impl Drop for XactGuard {
    fn drop(&mut self) {
        self.live
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}
#[cfg(test)]
mod tests {
    use super::super::{
        commit::{Sequencer, SequencerConfig, SystemClock},
        kv_mem::MemKv,
        locks::{LockConfig, LockManager, TxnId},
        txn::{Isolation, Transaction},
    };
    use super::*;
    fn store() -> (Arc<Kv>, Arc<SequenceAllocator>, XactIds) {
        let kv = Arc::new(Kv::new(Arc::new(MemKv::new())));
        let seq = Arc::new(SequenceAllocator::new(kv.clone(), 7));
        let ids = XactIds::new(kv.clone(), seq.clone());
        (kv, seq, ids)
    }
    fn marker(kv: &Kv, xid: u64) {
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Seq,
            marker_key(xid),
            42_u64.to_be_bytes().to_vec(),
        );
        kv.write(batch, Durability::Sync).unwrap();
    }
    #[test]
    fn allocation_peek_and_restart() {
        let mem = Arc::new(MemKv::new());
        let kv = Arc::new(Kv::new(mem.clone()));
        let seq = Arc::new(SequenceAllocator::new(kv.clone(), 7));
        let ids = XactIds::new(kv, seq.clone());
        assert_eq!(seq.peek(4, 0).unwrap(), 1);
        let mut last = 2;
        for expected in 3..13 {
            let guard = ids.assign().unwrap();
            assert_eq!(guard.id(), expected);
            assert!(guard.id() > last);
            last = guard.id();
            assert_eq!(seq.peek(4, 0).unwrap() + 2, last + 1);
        }
        let kv = Arc::new(Kv::new(Arc::new(mem.crash())));
        let seq = Arc::new(SequenceAllocator::new(kv.clone(), 7));
        assert!(seq.peek(4, 0).unwrap() + 2 > last);
        let ids = XactIds::new(kv, seq);
        for _ in 0..10 {
            let guard = ids.assign().unwrap();
            assert!(guard.id() > last);
            last = guard.id();
        }
    }
    #[test]
    fn status_rules_and_marker_liveness() {
        let (kv, _, ids) = store();
        assert_eq!(ids.status(0).unwrap(), None);
        for xid in [1, 2] {
            assert_eq!(ids.status(xid).unwrap(), Some(XactStatus::Committed));
        }
        assert!(matches!(ids.status(3), Err(XactLookupError::Future)));
        let guard = ids.assign().unwrap();
        let xid = guard.id();
        assert_eq!(xid, FIRST_XID);
        assert_eq!(ids.status(xid).unwrap(), Some(XactStatus::InProgress));
        assert!(matches!(ids.status(xid + 1), Err(XactLookupError::Future)));
        drop(guard);
        assert_eq!(ids.status(xid).unwrap(), Some(XactStatus::Aborted));
        let guard = ids.assign().unwrap();
        marker(&kv, guard.id());
        assert_eq!(
            ids.status(guard.id()).unwrap(),
            Some(XactStatus::InProgress)
        );
        let xid = guard.id();
        drop(guard);
        assert_eq!(ids.status(xid).unwrap(), Some(XactStatus::Committed));
    }
    #[test]
    fn leased_ids_are_distinct_and_finished_status_is_refused() {
        let stub = Arc::new(super::super::seq::lease_tests::StubClient::default());
        let a = XactIds::leased(Arc::new(SequenceAllocator::leased(stub.clone(), 4)));
        let b = XactIds::leased(Arc::new(SequenceAllocator::leased(stub, 4)));
        let mut seen = BTreeSet::new();
        let mut guards = Vec::new();
        for _ in 0..10 {
            for ids in [&a, &b] {
                let guard = ids.assign().unwrap();
                assert!(guard.id() >= FIRST_XID);
                assert!(seen.insert(guard.id()), "duplicate xid {}", guard.id());
                guards.push(guard);
            }
        }
        let xid = guards[0].id();
        assert_eq!(a.status(0).unwrap(), None);
        assert_eq!(a.status(2).unwrap(), Some(XactStatus::Committed));
        assert_eq!(a.status(xid).unwrap(), Some(XactStatus::InProgress));
        // b cannot see a's live xid: that is a cluster status lookup (R5-55).
        assert!(matches!(b.status(xid), Err(XactLookupError::Cluster)));
        guards.clear();
        assert!(matches!(a.status(xid), Err(XactLookupError::Cluster)));
    }
    #[test]
    fn retention_and_prune_threshold() {
        let (kv, seq, ids) = store();
        marker(&kv, 3);
        marker(&kv, 4097);
        seq.ensure_at_least(4, 0, XACT_RETAIN + 4094).unwrap();
        let guard = ids.assign().unwrap(); // next = retain + 4097
        assert_eq!(ids.status(3).unwrap(), None);
        assert_eq!(ids.status(4097).unwrap(), Some(XactStatus::Committed));
        assert_eq!(kv.get(Keyspace::Seq, &marker_key(3)).unwrap(), None);
        marker(&kv, 3);
        drop(ids.assign().unwrap());
        assert!(kv.get(Keyspace::Seq, &marker_key(3)).unwrap().is_some());
        seq.ensure_at_least(4, 0, guard.id() + 4096).unwrap();
        drop(ids.assign().unwrap());
        assert_eq!(kv.get(Keyspace::Seq, &marker_key(3)).unwrap(), None);
    }
    #[test]
    fn transaction_commit_abort_and_savepoint() {
        let (kv, _, ids) = store();
        let seq = Sequencer::open(
            kv.clone(),
            Arc::new(SystemClock),
            SequencerConfig::default(),
        )
        .unwrap();
        let locks = Arc::new(LockManager::new(LockConfig::default()));
        for n in 0..3 {
            let mut txn = Transaction::begin(&seq, &locks, TxnId(n + 1), Isolation::ReadCommitted);
            let mark = txn.savepoint();
            let guard = ids.assign().unwrap();
            let xid = guard.id();
            txn.attach_xact(guard).unwrap();
            assert_eq!(txn.xact_id(), Some(xid));
            if n == 1 {
                drop(txn);
                assert_eq!(ids.status(xid).unwrap(), Some(XactStatus::Aborted));
                assert_eq!(kv.get(Keyspace::Seq, &marker_key(xid)).unwrap(), None);
            } else {
                if n == 2 {
                    txn.rollback_to(&mark).unwrap();
                }
                let ts = txn.commit().unwrap();
                assert_eq!(
                    kv.get(Keyspace::Seq, &marker_key(xid)).unwrap(),
                    Some(ts.0.to_be_bytes().to_vec())
                );
                assert_eq!(ids.status(xid).unwrap(), Some(XactStatus::Committed));
            }
        }
    }
}

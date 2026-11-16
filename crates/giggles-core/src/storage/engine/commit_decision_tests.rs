use super::*;
use super::guard_tests::{fixture, request};
use super::super::{
    kv::{Direction, KvError}, kv_mem::MemKv, meta, mvcc,
    locks::{LockConfig, LockManager, TxnId}, txn::{Isolation, Transaction},
    once_guard::GuardRejection,
};
use crate::{Cancellation, DecisionPoint};
use std::ops::Bound;

fn write(id: u64) -> CommitRequest {
    let mut req = request(0);
    req.writes.insert((Keyspace::Rows, id.to_be_bytes().to_vec()), Some(vec![42]));
    req
}
fn guard(ts: Ts) -> CommitGuard {
    CommitGuard::capture(ts, Instant::now() + Duration::from_secs(60))
}
fn submit(seq: &Arc<Sequencer>, req: CommitRequest, guard: Option<CommitGuard>) -> Result<Ts, TxnError> {
    let seq = seq.clone();
    thread::spawn(move || match guard {
        Some(g) => seq.submit_guarded(req, g), None => seq.submit(req),
    }).join().unwrap()
}
fn dump(kv: &dyn KvRead) -> Vec<(Keyspace, Vec<u8>, Vec<u8>)> {
    Keyspace::ALL.into_iter().flat_map(|space| {
        kv.range(space, Bound::Unbounded, Bound::Unbounded, Direction::Forward).unwrap()
            .map(move |r| { let (k, v) = r.unwrap(); (space, k, v) })
    }).collect()
}
fn absent(mem: &MemKv, id: u64) {
    assert_eq!(mvcc::latest_version(mem, Keyspace::Rows, &id.to_be_bytes()).unwrap(), None);
}
fn pending(id: u64, guarded: bool) -> Pending {
    Pending { request: write(id), guard: guarded.then(|| guard(Ts(0))), reply: mpsc::channel().0 }
}
fn id(p: &Pending) -> u64 {
    u64::from_be_bytes(p.request.writes.keys().next().unwrap().1.as_slice().try_into().unwrap())
}

#[test]
fn gather_isolates_guarded_requests_without_draining_followers() {
    let (tx, rx) = mpsc::channel();
    for (n, g) in [(1, false), (2, true), (3, false)] {
        tx.send(Message::Commit(pending(n, g))).unwrap();
    }
    let (group, next, stop) = gather(pending(0, false), &rx, &SequencerConfig::default());
    assert!(!stop);
    assert_eq!(group.iter().map(id).collect::<Vec<_>>(), [0, 1]);
    let Some(Message::Commit(g)) = next else { panic!("guard must be deferred") };
    let (group, next, stop) = gather(g, &rx, &SequencerConfig::default());
    assert_eq!(group.iter().map(id).collect::<Vec<_>>(), [2]);
    assert!(next.is_none() && !stop);
    let Message::Commit(o2) = rx.try_recv().unwrap() else { panic!() };
    assert_eq!(id(&o2), 3);
    tx.send(Message::Commit(pending(1, false))).unwrap();
    let (_, next, stop) = gather(pending(2, true), &rx, &SequencerConfig::default());
    assert!(next.is_none() && !stop);
    let Message::Commit(o1) = rx.try_recv().unwrap() else { panic!() };
    assert_eq!(id(&o1), 1);
}

#[test]
fn expired_guard_changes_neither_state_history_nor_hlc_even_when_empty() {
    for empty in [false, true] {
        let (mem, seq) = fixture();
        let _held = seq.snapshot();
        let previous = submit(&seq, write(1), None).unwrap();
        let before = dump(mem.as_ref());
        let rejected = if empty { request(0) } else { write(2) };
        let g = CommitGuard::capture(previous, Instant::now());
        assert_eq!(submit(&seq, rejected, Some(g)), Err(TxnError::Guard(GuardRejection::LeaseExpired)));
        assert_eq!(seq.visible_ts(), previous);
        assert_eq!(dump(mem.as_ref()), before);
        absent(&mem, 2);
        let mut next = write(3);
        next.validation = Some(Box::new(move |log, _, _, ts| {
            assert_eq!(ts.0, previous.0 + 1);
            let mut seen = Vec::new();
            log.writes_after(Ts(0), |ts, ks, key| seen.push((ts, ks, key.to_vec()))).unwrap();
            assert_eq!(seen, [(previous, Keyspace::Rows, 1u64.to_be_bytes().to_vec())]);
            Ok(())
        }));
        assert_eq!(submit(&seq, next, None).unwrap().0, previous.0 + 1);
    }
}

#[test]
fn cancellation_before_submit_writes_nothing_and_leaves_worker_usable() {
    let (mem, seq) = fixture();
    let c = Cancellation::default();
    let g = c.scope(|| guard(Ts(0)));
    assert!(c.cancel());
    let before = dump(mem.as_ref());
    assert_eq!(submit(&seq, write(1), Some(g)), Err(TxnError::Guard(GuardRejection::Cancelled)));
    assert_eq!(dump(mem.as_ref()), before);
    assert_eq!(seq.visible_ts(), Ts(0));
    assert!(submit(&seq, write(2), None).unwrap() > Ts(0));
}

#[test]
fn cancellation_after_decision_loses_and_commit_is_visible() {
    let (mem, seq) = fixture();
    let c = Cancellation::default();
    let decision_cancellation = c.clone();
    let g = c.scope(|| guard(Ts(0))).with_hook(Arc::new(move |p| {
        if p == DecisionPoint::Decided { assert!(!decision_cancellation.cancel()); }
    }));
    let ts = submit(&seq, write(1), Some(g)).unwrap();
    assert_eq!(seq.visible_ts(), ts);
    assert_eq!(mvcc::read_at(mem.as_ref(), Keyspace::Rows, &1u64.to_be_bytes(), ts).unwrap(), Some(vec![42]));
}

#[test]
fn lease_expiring_at_worker_decision_writes_nothing() {
    let (mem, seq) = fixture();
    let before = dump(mem.as_ref());
    let deadline = Instant::now() + Duration::from_millis(50);
    let g = CommitGuard::capture(Ts(0), deadline).with_hook(Arc::new(move |p| {
        if p == DecisionPoint::BeforeDecision {
            while Instant::now() < deadline { std::hint::spin_loop(); }
        }
    }));
    assert_eq!(submit(&seq, write(1), Some(g)), Err(TxnError::Guard(GuardRejection::LeaseExpired)));
    assert_eq!(dump(mem.as_ref()), before);
    assert_eq!(seq.visible_ts(), Ts(0));
}

#[test]
fn stale_fence_preserves_history_and_serializable_validation() {
    let (mem, seq) = fixture();
    let _held = seq.snapshot();
    let g = guard(seq.visible_ts());
    let previous = submit(&seq, write(1), None).unwrap();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::Serializable);
    assert_eq!(txn.get(Keyspace::Rows, &2u64.to_be_bytes()).unwrap(), None);
    let before = dump(mem.as_ref());
    assert_eq!(submit(&seq, write(2), Some(g)), Err(TxnError::Guard(GuardRejection::SnapshotMoved)));
    assert_eq!(dump(mem.as_ref()), before);
    assert_eq!(seq.visible_ts(), previous);
    assert_eq!(txn.commit().unwrap(), previous);
    let mut req = write(3);
    req.validation = Some(Box::new(move |log, _, _, ts| {
        assert_eq!(log.len(), 1);
        assert_eq!(ts.0, previous.0 + 1);
        Ok(())
    }));
    submit(&seq, req, None).unwrap();
}

#[test]
fn rejected_native_metadata_upgrade_stays_unchanged_after_reopen() {
    let (mem, seq) = fixture();
    let mut req = write(1);
    req.features_or = 2;
    req.minimum_subversion = 2;
    let mut ledger = 1u16.to_be_bytes().to_vec();
    for n in [110u64, 1] { ledger.extend(n.to_be_bytes()); }
    ledger.extend(99u128.to_be_bytes());
    ledger.extend(1u64.to_be_bytes());
    ledger.push(42);
    ledger.extend([1; 32]); ledger.extend([2; 32]);
    ledger.extend(120u64.to_be_bytes()); ledger.push(0);
    for n in [10u64, 100, 110] { ledger.extend(n.to_be_bytes()); }
    let metadata = crate::storage::Metadata {
        format: 4, domain: 0, schema: 0, last_time: 0, next_approval: 0, origin: Some(77),
    };
    req.writes.insert((Keyspace::Kernel, vec![0]), Some(crate::storage::encode(&metadata).unwrap()));
    req.writes.insert((Keyspace::Kernel, vec![7]), Some(ledger));
    let before = dump(mem.as_ref());
    assert_eq!(submit(&seq, req, Some(CommitGuard::capture(Ts(0), Instant::now()))),
        Err(TxnError::Guard(GuardRejection::LeaseExpired)));
    let kv = seq.kv.clone();
    drop(seq);
    let reopened = Sequencer::open(kv, Arc::new(ManualClock::new(10)), SequencerConfig::default()).unwrap();
    assert_eq!(dump(mem.as_ref()), before);
    assert_eq!(reopened.visible_ts(), Ts(0));
    let info = meta::open_checks(&mem.crash()).unwrap();
    assert_eq!((info.features, info.subversion), (0, 1));
}

#[test]
fn sync_failure_after_decision_is_ambiguous_then_fenced() {
    let (mem, seq) = fixture();
    let inject = mem.clone();
    let c = Cancellation::default();
    let g = c.scope(|| guard(Ts(0))).with_hook(Arc::new(move |p| {
        if p == DecisionPoint::Decided { inject.fail_next_write(KvError::Io("post-decision".into())); }
    }));
    assert_eq!(submit(&seq, write(1), Some(g)), Err(TxnError::CommitAmbiguous));
    assert!(!c.cancel());
    assert_eq!(submit(&seq, write(2), None), Err(TxnError::Fenced));
    assert_eq!(seq.visible_ts(), Ts(0));
    absent(&mem, 1);
}

#[test]
fn live_ordinary_guarded_ordinary_timestamps_increase() {
    let (_, seq) = fixture();
    let first = submit(&seq, write(1), None).unwrap();
    let middle = submit(&seq, write(2), Some(guard(first))).unwrap();
    let last = submit(&seq, write(3), None).unwrap();
    assert!(first < middle && middle < last);
    assert_eq!(seq.visible_ts(), last);
}

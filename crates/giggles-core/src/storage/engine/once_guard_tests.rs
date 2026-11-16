use super::*;
use crate::{Cancellation, DecisionPoint::{BeforeDecision, Decided}};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use super::super::{
    commit::{CommitRequest, ManualClock, Sequencer, SequencerConfig},
    kv::{Keyspace, Kv}, kv_mem::MemKv, locks::{LockConfig, LockManager, TxnId},
    mvcc, txn::{Isolation, Transaction, TxnError},
};
fn hooked(c: &Cancellation, deadline: Instant) -> (CommitGuard, Arc<Mutex<Vec<crate::DecisionPoint>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let out = events.clone();
    (c.scope(|| CommitGuard::capture(Ts(7), deadline))
        .with_hook(Arc::new(move |p| out.lock().unwrap().push(p))), events)
}
#[test]
fn exact_fence_and_deadline_accessors() {
    let d = Instant::now();
    let g = CommitGuard::capture(Ts(7), d);
    assert_eq!(g.fence(), Ts(7));
    assert_eq!(g.lease_deadline(), d);
    for ts in [Ts(6), Ts(8)] { assert_eq!(g.check_fence(ts), Err(GuardRejection::SnapshotMoved)); }
    assert_eq!(g.check_fence(Ts(7)), Ok(()));
}
#[test]
fn lease_checks_bracket_cancellation_decision() {
    let deadline = Instant::now();
    for second in [false, true] {
        let c = Cancellation::default();
        let (g, events) = hooked(&c, deadline);
        let mut calls = 0;
        let result = g.decide_with(&mut || {
            calls += 1;
            if second && calls == 1 { deadline - Duration::from_millis(1) } else { deadline }
        });
        assert_eq!(result, Err(GuardRejection::LeaseExpired));
        assert_eq!(calls, if second { 2 } else { 1 });
        assert_eq!(*events.lock().unwrap(), [BeforeDecision]);
        assert_eq!(c.cancel(), !second);
    }
}
#[test]
fn cancellation_and_success_hooks() {
    let now = Instant::now();
    for cancelled in [false, true] {
        let c = Cancellation::default();
        if cancelled { assert!(c.cancel()); }
        let (g, events) = hooked(&c, now + Duration::from_secs(1));
        assert_eq!(g.decide_with(&mut || now), if cancelled { Err(GuardRejection::Cancelled) } else { Ok(()) });
        assert_eq!(*events.lock().unwrap(), if cancelled { vec![BeforeDecision] } else { vec![BeforeDecision, Decided] });
        assert!(!c.cancel());
    }
}
#[test]
fn caller_capture_never_infers_worker_scope() {
    let deadline = Instant::now() + Duration::from_secs(60);
    assert!(Cancellation::current().is_none());
    let g1 = CommitGuard::capture(Ts(0), deadline);
    let y = Cancellation::default();
    let g2 = y.scope(|| CommitGuard::capture(Ts(0), deadline));
    let x = Cancellation::default();
    assert!(x.cancel());
    std::thread::spawn(move || x.scope(|| {
        assert_eq!(g1.decide(), Ok(()));
        assert_eq!(g2.decide(), Ok(()));
        assert_eq!(crate::cancellation::check(), Err(crate::CoreError::Cancelled));
    })).join().unwrap();
    assert!(!y.cancel());
}
fn sequencer() -> Arc<Sequencer> {
    Sequencer::open(Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)), SequencerConfig::default()).unwrap()
}
fn request() -> CommitRequest {
    CommitRequest { snapshot: Ts(0),
        writes: BTreeMap::from([((Keyspace::Rows, 1u64.to_be_bytes().to_vec()), Some(vec![1]))]),
        features_or: 0, minimum_subversion: 0, raw: vec![], validation: None }
}
fn guard(ts: Ts) -> CommitGuard { CommitGuard::capture(ts, Instant::now() + Duration::from_secs(60)) }
#[test]
fn transported_fence_rejects_without_writing_and_accepts_exact_match() {
    let seq = sequencer();
    assert_eq!(seq.submit_guarded(request(), guard(Ts(1))), Err(TxnError::Guard(GuardRejection::SnapshotMoved)));
    assert_eq!(seq.visible_ts(), Ts(0));
    assert_eq!(mvcc::latest_version(seq.kv(), Keyspace::Rows, &1u64.to_be_bytes()).unwrap(), None);
    let ts = seq.submit_guarded(request(), guard(Ts(0))).unwrap();
    assert_eq!(seq.visible_ts(), ts);
    assert_eq!(mvcc::read_at(seq.kv(), Keyspace::Rows, &1u64.to_be_bytes(), ts).unwrap(), Some(vec![1]));
}
#[test]
fn transaction_requires_matching_snapshot_and_empty_commits_are_guarded() {
    let seq = sequencer();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::ReadCommitted);
    assert_eq!(txn.commit_guarded(guard(Ts(1)), |_| panic!("must not publish")), Err(TxnError::Misuse));
    let txn = Transaction::begin(&seq, &locks, TxnId(2), Isolation::ReadCommitted);
    seq.submit(request()).unwrap();
    assert_eq!(txn.commit_guarded(guard(Ts(0)), |_| panic!("must not publish")), Err(TxnError::Guard(GuardRejection::SnapshotMoved)));
}
#[test]
fn rejection_mappings_use_existing_errors() {
    use GuardRejection::*;
    for (reason, state, core) in [
        (SnapshotMoved, "40001", crate::CoreError::Sql(crate::relational::SqlError::Conflict)),
        (Cancelled, "57014", crate::CoreError::Cancelled),
        (LeaseExpired, "XX000", crate::CoreError::Busy),
    ] {
        let error = TxnError::Guard(reason);
        assert_eq!(error.sqlstate(), state);
        assert_eq!(crate::CoreError::from(error), core);
    }
}

#[test]
fn unfenced_guard_accepts_moved_snapshot() {
    let seq = sequencer();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    txn.snapshot_ts();
    seq.submit(request()).unwrap();
    let guard = CommitGuard::capture_unfenced(Instant::now() + Duration::from_secs(60));
    assert_eq!(guard.check_fence(Ts(999)), Ok(()));
    txn.commit_guarded(guard, |_| Ok(())).unwrap();
}

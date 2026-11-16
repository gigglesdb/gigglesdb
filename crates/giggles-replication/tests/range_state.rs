use std::{ops::Bound, sync::Arc};

use giggles_core::range_api::{
    ClusterTxnId, Epoch, LockRequest, LockTarget, RangeDescriptor, RangeError, RangeId,
    RangeLockMode, RangeSpace, RangeWrite, ReadDirection, ReadRequest, ReplicaDesc, ReplicaRole,
    Timestamp, range_key,
};
use giggles_replication::{
    range_guard::{check_epoch, check_lock, check_read, check_writes},
    range_state::{
        MemOp, MemRangeStore, RangeStateMachine, RangeStoreOps, decode_proposal, encode_proposal,
    },
    state::{AppliedPosition as P, ApplyOutcome::*, StateMachine},
};

fn pos(index: u64) -> P {
    P { index, term: 2 }
}
fn rk(key: &[u8]) -> Vec<u8> {
    range_key(RangeSpace::Rows, key)
}
fn desc(id: u64, epoch: u64, start: &[u8], end: Option<&[u8]>) -> RangeDescriptor {
    RangeDescriptor {
        id: RangeId(id),
        epoch: Epoch(epoch),
        start: rk(start),
        end: end.map(rk),
        replicas: vec![ReplicaDesc {
            node: 1,
            role: ReplicaRole::Voter,
        }],
    }
}
/// Range 7 = Rows [b, m) at epoch 1, over a fresh store.
fn machine() -> (Arc<MemRangeStore>, RangeStateMachine) {
    let store = Arc::new(MemRangeStore::new());
    store
        .create_range(desc(7, 1, b"b", Some(&b"m"[..])))
        .unwrap();
    (store.clone(), RangeStateMachine::new(RangeId(7), store))
}
fn put(epoch: u64, key: &[u8]) -> Vec<u8> {
    let op = MemOp::Put {
        key: rk(key),
        value: key.to_vec(),
    };
    encode_proposal(Epoch(epoch), &MemRangeStore::batch(&[op]))
}
fn keys(store: &MemRangeStore, range: u64) -> Vec<Vec<u8>> {
    store.keys(RangeId(range))
}

#[test]
fn apply_skip_gap_and_noop() {
    let (store, s) = machine();
    assert_eq!(s.applied().unwrap(), None);
    assert_eq!(s.apply(&put(1, b"zero"), pos(0)).unwrap(), Skipped);
    assert!(s.apply(&put(1, b"gap"), pos(2)).is_err());
    assert_eq!(s.applied().unwrap(), None);
    assert_eq!(s.apply(&put(1, b"c"), pos(1)).unwrap(), Applied);
    assert_eq!(
        s.apply(&put(1, b"dup"), P { index: 1, term: 9 }).unwrap(),
        Skipped
    );
    assert!(s.apply_noop(pos(3)).is_err());
    assert_eq!(s.apply_noop(pos(2)).unwrap(), Applied);
    assert_eq!(s.apply_noop(pos(1)).unwrap(), Skipped);
    assert_eq!(s.apply(&put(1, b"d"), pos(3)).unwrap(), Applied);
    assert_eq!(s.applied().unwrap(), Some(pos(3)));
    assert_eq!(keys(&store, 7), vec![rk(b"c"), rk(b"d")]);
}

#[test]
fn failures_are_once_and_atomic() {
    let (store, s) = machine();
    store.fail_next_apply();
    let error = s.apply(&put(1, b"c"), pos(1)).unwrap_err();
    assert!(!error.to_string().is_empty());
    assert_eq!(s.applied().unwrap(), None);
    assert!(keys(&store, 7).is_empty());
    assert_eq!(s.apply(&put(1, b"c"), pos(1)).unwrap(), Applied);
    store.fail_next_apply();
    assert!(s.apply_noop(pos(2)).is_err());
    assert_eq!(s.applied().unwrap(), Some(pos(1)));
    assert_eq!(s.apply_noop(pos(2)).unwrap(), Applied);
    assert_eq!(keys(&store, 7).len(), 1);
}

#[test]
fn proposal_envelope_is_strict() {
    let bytes = encode_proposal(Epoch(5), b"batch");
    assert_eq!(&bytes[..9], &[1, 0, 0, 0, 0, 0, 0, 0, 5]);
    assert_eq!(decode_proposal(&bytes).unwrap(), (Epoch(5), &b"batch"[..]));
    assert!(decode_proposal(&bytes[..8]).is_err());
    let mut v2 = bytes.clone();
    v2[0] = 2;
    assert!(decode_proposal(&v2).is_err());
    let (_, s) = machine();
    assert!(s.apply(&bytes[..3], pos(1)).is_err());
    assert_eq!(s.applied().unwrap(), None);
}

#[test]
fn stale_epoch_proposal_never_applies_after_descriptor_change() {
    let (store, s) = machine();
    let split = MemRangeStore::batch(&[MemOp::SetDescriptor(desc(7, 2, b"b", Some(&b"g"[..])))]);
    assert_eq!(
        s.apply(&encode_proposal(Epoch(1), &split), pos(1)).unwrap(),
        Applied
    );
    assert_eq!(
        store.descriptor(RangeId(7)).unwrap().unwrap().epoch,
        Epoch(2)
    );
    // Proposed under epoch 1 before the split, applied after it.
    assert_eq!(s.apply(&put(1, b"c"), pos(2)).unwrap(), Refused);
    assert_eq!(s.applied().unwrap(), Some(pos(2)));
    assert!(keys(&store, 7).is_empty());
    let reason = s.take_refusal(2).unwrap();
    assert!(reason.contains("stale epoch"), "{reason}");
    assert!(
        reason.contains("epoch 1") && reason.contains("epoch 2"),
        "{reason}"
    );
    assert_eq!(s.take_refusal(2), None);
    // Replay of the refused index is a skip, and the new epoch applies.
    assert_eq!(s.apply(&put(1, b"c"), pos(2)).unwrap(), Skipped);
    assert_eq!(s.apply(&put(2, b"c"), pos(3)).unwrap(), Applied);
    assert_eq!(store.get(RangeId(7), &rk(b"c")), Some(b"c".to_vec()));
    // An epoch that does not grow is refused by the store.
    let back = MemRangeStore::batch(&[MemOp::SetDescriptor(desc(7, 2, b"b", None))]);
    assert_eq!(
        s.apply(&encode_proposal(Epoch(2), &back), pos(4)).unwrap(),
        Refused
    );
    assert!(s.take_refusal(4).is_some());
}

#[test]
fn out_of_bounds_batch_is_refused_deterministically() {
    let run = || {
        let (store, s) = machine();
        let batch = MemRangeStore::batch(&[
            MemOp::Put {
                key: rk(b"c"),
                value: b"in".to_vec(),
            },
            MemOp::Put {
                key: rk(b"z"),
                value: b"out".to_vec(),
            },
        ]);
        let outcomes = vec![
            s.apply(&put(1, b"d"), pos(1)).unwrap(),
            s.apply(&encode_proposal(Epoch(1), &batch), pos(2)).unwrap(),
            s.apply(&put(1, b"e"), pos(3)).unwrap(),
        ];
        assert!(s.take_refusal(2).unwrap().contains("outside"));
        (outcomes, s.applied().unwrap(), keys(&store, 7))
    };
    let first = run();
    assert_eq!(first.0, vec![Applied, Refused, Applied]);
    assert_eq!(first.2, vec![rk(b"d"), rk(b"e")]);
    assert_eq!(first, run());
}

#[test]
fn ranges_apply_independently() {
    let (store, a) = machine();
    store.create_range(desc(8, 1, b"m", None)).unwrap();
    assert!(store.create_range(desc(8, 1, b"m", None)).is_err());
    let b = RangeStateMachine::new(RangeId(8), store.clone());
    assert_eq!(a.apply(&put(1, b"c"), pos(1)).unwrap(), Applied);
    assert_eq!(b.apply(&put(1, b"c"), pos(1)).unwrap(), Refused);
    assert_eq!(b.apply(&put(1, b"x"), pos(2)).unwrap(), Applied);
    assert_eq!(a.apply(&put(1, b"x"), pos(2)).unwrap(), Refused);
    assert_eq!(keys(&store, 7), vec![rk(b"c")]);
    assert_eq!(keys(&store, 8), vec![rk(b"x")]);
    assert!(a.take_refusal(1).is_none() && a.take_refusal(2).is_some());
}

#[test]
fn checkpoint_install_round_trip_and_engine_guard() {
    let dir = std::env::temp_dir().join(format!("range-state-{}", std::process::id()));
    let (store, s) = machine();
    s.apply(&put(1, b"c"), pos(1)).unwrap();
    s.apply(&put(1, b"d"), pos(2)).unwrap();
    assert_eq!(s.checkpoint(&dir).unwrap(), pos(2));
    let (target_store, target) = machine();
    assert!(target.can_install());
    let guard = target.engine_guard();
    let second = target.engine_guard();
    assert!(!target.can_install());
    drop(guard);
    assert!(!target.can_install());
    drop(second);
    assert!(target.can_install());
    assert_eq!(target.install(&dir).unwrap(), pos(2));
    assert_eq!(target.applied().unwrap(), Some(pos(2)));
    assert_eq!(keys(&target_store, 7), keys(&store, 7));
    assert_eq!(
        target_store.descriptor(RangeId(7)).unwrap(),
        store.descriptor(RangeId(7)).unwrap()
    );
    assert_eq!(target.apply(&put(1, b"e"), pos(3)).unwrap(), Applied);
    target.flush().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

fn read(epoch: u64, space: RangeSpace, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> ReadRequest {
    ReadRequest {
        range: RangeId(7),
        epoch: Epoch(epoch),
        space,
        start,
        end,
        dir: ReadDirection::Forward,
        ts: Timestamp(1),
        page_bytes: 4096,
    }
}
fn lock(epoch: u64, target: LockTarget) -> LockRequest {
    LockRequest {
        range: RangeId(7),
        epoch: Epoch(epoch),
        txn: ClusterTxnId::new(1, 1),
        priority: Timestamp(1),
        target,
        mode: RangeLockMode::Shared,
        timeout_ms: None,
    }
}
fn is_stale(result: Result<(), RangeError>, expected: &RangeDescriptor) -> bool {
    matches!(result, Err(RangeError::StaleEpoch { descriptor }) if descriptor == *expected)
}

#[test]
fn guard_checks_epoch_and_bounds() {
    use Bound::{Excluded, Included, Unbounded};
    let d = desc(7, 3, b"b", Some(&b"m"[..]));
    assert!(check_epoch(&d, Epoch(3)).is_ok());
    assert!(is_stale(check_epoch(&d, Epoch(2)), &d));
    let w = |key: &[u8]| RangeWrite {
        space: RangeSpace::Rows,
        key: key.to_vec(),
        value: None,
    };
    assert!(check_writes(&d, Epoch(3), &[w(b"b"), w(b"lz")]).is_ok());
    assert!(is_stale(
        check_writes(&d, Epoch(3), &[w(b"c"), w(b"m")]),
        &d
    ));
    assert!(is_stale(check_writes(&d, Epoch(4), &[w(b"c")]), &d));
    let rows = RangeSpace::Rows;
    let ok = |r: ReadRequest| check_read(&d, &r).is_ok();
    assert!(ok(read(
        3,
        rows,
        Included(b"b".to_vec()),
        Excluded(b"m".to_vec())
    )));
    assert!(ok(read(
        3,
        rows,
        Excluded(b"c".to_vec()),
        Included(b"l".to_vec())
    )));
    assert!(!ok(read(
        3,
        rows,
        Included(b"a".to_vec()),
        Excluded(b"c".to_vec())
    )));
    assert!(!ok(read(
        3,
        rows,
        Included(b"c".to_vec()),
        Included(b"m".to_vec())
    )));
    assert!(!ok(read(3, rows, Unbounded, Excluded(b"c".to_vec()))));
    assert!(!ok(read(3, rows, Included(b"c".to_vec()), Unbounded)));
    assert!(!ok(read(
        2,
        rows,
        Included(b"c".to_vec()),
        Excluded(b"d".to_vec())
    )));
    assert!(!ok(read(
        3,
        RangeSpace::Index,
        Included(b"c".to_vec()),
        Excluded(b"d".to_vec())
    )));
    // A range spanning all of Rows may be read unbounded in Rows only.
    let all_rows = RangeDescriptor {
        start: rk(b""),
        end: Some(range_key(RangeSpace::Index, b"")),
        ..d.clone()
    };
    let all = |r: ReadRequest| check_read(&all_rows, &r).is_ok();
    assert!(all(read(3, rows, Unbounded, Unbounded)));
    assert!(!all(read(3, RangeSpace::Catalog, Unbounded, Unbounded)));
    assert!(!all(read(3, RangeSpace::Index, Unbounded, Unbounded)));
    let key = |k: &[u8]| LockTarget::Key {
        space: rows,
        key: k.to_vec(),
    };
    assert!(check_lock(&d, &lock(3, key(b"c"))).is_ok());
    assert!(is_stale(check_lock(&d, &lock(3, key(b"z"))), &d));
    assert!(is_stale(check_lock(&d, &lock(2, key(b"c"))), &d));
    // Table 5 rows sit in [Rows 5 BE8, Rows 6 BE8).
    let table = |t: u64| LockTarget::TableSentinel { table: t };
    let five = 5_u64.to_be_bytes();
    let mid = [&five[..], b"pk"].concat();
    let inside = desc(7, 3, &mid, None);
    assert!(check_lock(&inside, &lock(3, table(5))).is_ok());
    assert!(is_stale(check_lock(&inside, &lock(3, table(4))), &inside));
    assert!(check_lock(&inside, &lock(3, table(u64::MAX))).is_ok());
    let before = desc(7, 3, b"", Some(&five[..]));
    assert!(is_stale(check_lock(&before, &lock(3, table(5))), &before));
    assert!(check_lock(&before, &lock(3, table(4))).is_ok());
}

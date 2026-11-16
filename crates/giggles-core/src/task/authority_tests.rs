#![cfg(feature = "engine-rocksdb")]

use super::*;
use crate::relational::{SqlRow, Value};
use crate::storage::{Isolation, V2Db};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const T: TaskObjectId = TaskObjectId(41);
const TABLE: u64 = 1_000_007;

struct TempDb {
    path: std::path::PathBuf,
}
impl TempDb {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("giggles-authority-{:032x}", u128::from_be_bytes(nonce)));
        Self { path }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn never() -> bool {
    false
}

fn pk(id: i64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn physical(id: i64) -> Vec<u8> {
    let mut key = TABLE.to_be_bytes().to_vec();
    key.extend(pk(id));
    key
}

fn row(id: i64, status: &str, version: u64) -> SqlRow {
    SqlRow { values: vec![Value::Int(id), Value::Text(status.into())], version }
}

/// `(id, old status, new status)`; equal statuses are a no-op.
fn effects(changes: &[(i64, &str, &str)]) -> Effects {
    let mut e = Effects::default();
    for (id, old, new) in changes {
        e.record(TABLE, &physical(*id), Some(&row(*id, old, 1)), Some(&row(*id, new, 2))).unwrap();
    }
    e
}

fn membership(txn: &mut Transaction, id: i64) -> Option<Membership> {
    txn.get(Keyspace::Kernel, &membership_key(T, TABLE, &pk(id)))
        .unwrap()
        .map(|v| Membership::decode(&v).unwrap())
}

/// One protected operation: hold, load head, charge, stage the new head, commit.
fn operate(db: &V2Db, changes: &[(i64, &str, &str)]) -> (Charge, MutationHead) {
    let mut txn = db.begin(Isolation::Serializable);
    hold(&txn, T, None, &never).unwrap();
    let mut head = load_head(&mut txn, T).unwrap();
    let seq = head.next_seq;
    let charge = charge(&mut txn, T, seq, &effects(changes)).unwrap();
    head.distinct_rows += u64::from(charge.distinct_new);
    head.transitions += u64::from(charge.transitions);
    head.next_seq += 1;
    stage_head(&mut txn, T, &head).unwrap();
    txn.commit().unwrap();
    (charge, head)
}

#[test]
fn absent_head_is_zero_with_seq_one() {
    let dir = TempDb::new();
    let db = V2Db::create(&dir.path).unwrap();
    let mut txn = db.begin(Isolation::Serializable);
    hold(&txn, T, None, &never).unwrap();
    let head = load_head(&mut txn, T).unwrap();
    assert_eq!(head.next_seq, 1);
    assert_eq!(head.next_preview, 1);
    assert_eq!(
        MutationHead { next_seq: 0, next_preview: 0, ..head },
        MutationHead::default()
    );
    // Holding after the snapshot is chosen is refused.
    let other = TaskObjectId(T.0 + 1);
    assert!(matches!(hold(&txn, other, None, &never), Err(TxnError::Misuse)));
}

#[test]
fn new_rows_count_distinct_once() {
    let dir = TempDb::new();
    let db = V2Db::create(&dir.path).unwrap();
    let (first, head) = operate(&db, &[(1, "open", "closed"), (2, "open", "closed")]);
    assert_eq!(first, Charge { distinct_new: 2, transitions: 2, noops: 0 });
    assert_eq!((head.distinct_rows, head.transitions, head.next_seq), (2, 2, 2));

    // Row 1 again plus a new row 3: only row 3 is a new distinct row.
    let (second, head) = operate(&db, &[(1, "closed", "open"), (3, "open", "closed")]);
    assert_eq!(second, Charge { distinct_new: 1, transitions: 2, noops: 0 });
    assert_eq!((head.distinct_rows, head.transitions), (3, 4));

    let mut txn = db.begin(Isolation::Serializable);
    assert_eq!(membership(&mut txn, 1), Some(Membership { first_seq: 1, last_seq: 2, transitions: 2 }));
    assert_eq!(membership(&mut txn, 2), Some(Membership { first_seq: 1, last_seq: 1, transitions: 1 }));
    assert_eq!(membership(&mut txn, 3), Some(Membership { first_seq: 2, last_seq: 2, transitions: 1 }));
}

#[test]
fn repeat_row_increments_transitions_only() {
    let dir = TempDb::new();
    let db = V2Db::create(&dir.path).unwrap();
    operate(&db, &[(7, "a", "b")]);
    for (n, (old, new)) in [("b", "c"), ("c", "d"), ("d", "e")].into_iter().enumerate() {
        let (charge, head) = operate(&db, &[(7, old, new)]);
        assert_eq!(charge, Charge { distinct_new: 0, transitions: 1, noops: 0 });
        assert_eq!(head.distinct_rows, 1);
        assert_eq!(head.transitions, n as u64 + 2);
    }
    let mut txn = db.begin(Isolation::Serializable);
    assert_eq!(membership(&mut txn, 7), Some(Membership { first_seq: 1, last_seq: 4, transitions: 4 }));
}

#[test]
fn noop_rows_are_not_charged() {
    let dir = TempDb::new();
    let db = V2Db::create(&dir.path).unwrap();
    let (charge, head) = operate(&db, &[(1, "same", "same"), (2, "open", "closed"), (3, "x", "x")]);
    assert_eq!(charge, Charge { distinct_new: 1, transitions: 1, noops: 2 });
    assert_eq!((head.distinct_rows, head.transitions), (1, 1));
    let mut txn = db.begin(Isolation::Serializable);
    assert_eq!(membership(&mut txn, 1), None);
    assert_eq!(membership(&mut txn, 3), None);
    assert!(membership(&mut txn, 2).is_some());

    // An all-no-op operation stages no membership.
    let mut txn = db.begin(Isolation::Serializable);
    hold(&txn, T, None, &never).unwrap();
    let charge = super::charge(&mut txn, T, 2, &effects(&[(4, "y", "y")])).unwrap();
    assert_eq!(charge, Charge { distinct_new: 0, transitions: 0, noops: 1 });
    assert_eq!(txn.written_keys(Keyspace::Kernel).count(), 0);
}

#[test]
fn two_holders_serialize_and_second_sees_first_counts() {
    let dir = TempDb::new();
    let db = V2Db::create(&dir.path).unwrap();

    // A holds first and has not committed.
    let mut a = db.begin(Isolation::Serializable);
    hold(&a, T, None, &never).unwrap();

    // A bounded wait for the same hold times out while A holds it.
    let timed = db.begin(Isolation::Serializable);
    let err = hold(&timed, T, Some(Instant::now() + Duration::from_millis(50)), &never).unwrap_err();
    assert!(matches!(err, TxnError::LockTimeout));
    drop(timed);

    // Another task's hold is independent.
    let other = db.begin(Isolation::Serializable);
    hold(&other, TaskObjectId(T.0 + 1), Some(Instant::now() + Duration::from_millis(50)), &never).unwrap();
    drop(other);

    let b_held = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let mut b = db.begin(Isolation::Serializable);
            hold(&b, T, Some(Instant::now() + Duration::from_secs(30)), &never).unwrap();
            b_held.store(true, Ordering::SeqCst);
            let head = load_head(&mut b, T).unwrap();
            let charge = charge(&mut b, T, head.next_seq, &effects(&[(1, "closed", "open")])).unwrap();
            (head, charge)
        });

        std::thread::sleep(Duration::from_millis(200));
        assert!(!b_held.load(Ordering::SeqCst), "second holder ran while the first held");

        let mut head = load_head(&mut a, T).unwrap();
        let charge = charge(&mut a, T, head.next_seq, &effects(&[(1, "open", "closed"), (2, "a", "b")])).unwrap();
        head.distinct_rows += u64::from(charge.distinct_new);
        head.transitions += u64::from(charge.transitions);
        head.next_seq += 1;
        stage_head(&mut a, T, &head).unwrap();
        a.commit().unwrap();

        let (seen, second) = waiter.join().unwrap();
        assert!(b_held.load(Ordering::SeqCst));
        assert_eq!((seen.distinct_rows, seen.transitions, seen.next_seq), (2, 2, 2));
        assert_eq!(second, Charge { distinct_new: 0, transitions: 1, noops: 0 });
    });
}

#[test]
fn head_round_trips_through_commit_and_reopen() {
    let dir = TempDb::new();
    let head = MutationHead {
        def_version: 3,
        distinct_rows: 17,
        transitions: 40,
        next_seq: 9,
        next_preview: 5,
        denials: 2,
        last_decided_ms: 1_763_251_200_123,
        stored_previews: 4,
    };
    {
        let db = V2Db::create(&dir.path).unwrap();
        let mut txn = db.begin(Isolation::Serializable);
        hold(&txn, T, None, &never).unwrap();
        stage_head(&mut txn, T, &head).unwrap();
        txn.commit().unwrap();
        let mut txn = db.begin(Isolation::Serializable);
        assert_eq!(load_head(&mut txn, T).unwrap(), head);
    }
    let db = V2Db::open(&dir.path).unwrap();
    let mut txn = db.begin(Isolation::Serializable);
    assert_eq!(load_head(&mut txn, T).unwrap(), head);
    assert_eq!(load_head(&mut txn, TaskObjectId(T.0 + 1)).unwrap().next_seq, 1);
}

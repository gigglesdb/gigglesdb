//! GD-0061 C2: `restart_snapshot`, `snapshot_restarted` and `written_keys`.
use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::Kv,
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;

fn setup() -> (Arc<Sequencer>, Arc<LockManager>) {
    let seq = Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
    )
    .unwrap();
    (seq, Arc::new(LockManager::new(LockConfig::default())))
}
fn key(n: u64) -> Vec<u8> {
    let mut k = 1u64.to_be_bytes().to_vec();
    k.extend(n.to_be_bytes());
    k
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, i: Isolation) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), i)
}
fn store(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, k: &[u8], v: &[u8]) -> Ts {
    let mut t = txn(seq, locks, id, Isolation::ReadCommitted);
    t.lock_for_write(Keyspace::Rows, k, None, &|| false)
        .unwrap();
    t.put(Keyspace::Rows, k.to_vec(), v.to_vec()).unwrap();
    t.commit().unwrap()
}

/// A well-formed protected Kernel write (R3-601 gates the 0x20..=0x2B namespaces).
fn protected_write(t: &mut Transaction, n: u64) {
    use crate::task::records::{self as rec, TaskObjectId};
    t.put(
        Keyspace::Kernel,
        rec::debit_key(TaskObjectId(1), n, 0),
        rec::DebitRecord::default().encode(),
    )
    .unwrap();
    t.require_features(super::super::meta::PROTECTED_RECORDS);
}

#[test]
fn restart_clears_reads_and_moves_snapshot() {
    for i in [Isolation::RepeatableRead, Isolation::Serializable] {
        let (seq, locks) = setup();
        let k = key(1);
        store(&seq, &locks, 1, &k, b"a");
        let mut t = txn(&seq, &locks, 2, i);
        assert!(!t.snapshot_restarted());
        assert_eq!(t.get(Keyspace::Rows, &k).unwrap(), Some(b"a".to_vec()));
        let old = t.current_snapshot_ts().unwrap();
        let committed = store(&seq, &locks, 3, &k, b"b");
        assert_eq!(t.get(Keyspace::Rows, &k).unwrap(), Some(b"a".to_vec()));
        let new = t.restart_snapshot().unwrap();
        assert!(new > old);
        assert!(new >= committed);
        assert_eq!(t.current_snapshot_ts(), Some(new));
        assert!(t.snapshot_restarted());
        assert!(t.read_set().points.is_empty());
        assert!(t.read_set().ranges.is_empty());
        assert_eq!(t.get(Keyspace::Rows, &k).unwrap(), Some(b"b".to_vec()));
        if i == Isolation::Serializable {
            assert_eq!(t.read_set().points.len(), 1);
        }
        t.rollback();
    }
}

#[test]
fn restart_with_writes_is_misuse() {
    let (seq, locks) = setup();
    // READ COMMITTED never restarts: it already takes one snapshot per statement.
    let mut rc = txn(&seq, &locks, 1, Isolation::ReadCommitted);
    assert_eq!(rc.restart_snapshot(), Err(TxnError::Misuse));
    assert!(!rc.snapshot_restarted());
    rc.rollback();

    let mut t = txn(&seq, &locks, 2, Isolation::Serializable);
    t.put(Keyspace::Kernel, vec![0x20, 1], vec![1]).unwrap();
    let before = t.current_snapshot_ts();
    assert_eq!(t.restart_snapshot(), Err(TxnError::Misuse));
    assert_eq!(t.current_snapshot_ts(), before);
    assert!(!t.snapshot_restarted());
    t.rollback();

    let mut t = txn(&seq, &locks, 3, Isolation::Serializable);
    t.delete_counter(super::super::catalog_codec::counter_key(3, 7))
        .unwrap();
    assert_eq!(t.restart_snapshot(), Err(TxnError::Misuse));
    t.rollback();

    // A savepoint rollback that empties the write set makes the restart legal
    // again (the denial path of GD-0061 D-F).
    let mut t = txn(&seq, &locks, 4, Isolation::Serializable);
    let mark = t.savepoint();
    t.put(Keyspace::Kernel, vec![0x20, 1], vec![1]).unwrap();
    assert_eq!(t.restart_snapshot(), Err(TxnError::Misuse));
    t.rollback_to(&mark).unwrap();
    t.release(mark).unwrap();
    t.restart_snapshot().unwrap();
    assert!(t.snapshot_restarted());
    t.rollback();
}

#[test]
fn restart_keeps_locks() {
    let (seq, locks) = setup();
    let k = key(1);
    let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
    t.lock_sql_sentinel(1, LockMode::Shared, &|| false).unwrap();
    t.lock_for_write(Keyspace::Rows, &k, None, &|| false)
        .unwrap();
    let mut held = locks.held(TxnId(1));
    held.sort_by_key(|(key, _)| format!("{key:?}"));
    t.restart_snapshot().unwrap();
    let mut after = locks.held(TxnId(1));
    after.sort_by_key(|(key, _)| format!("{key:?}"));
    assert_eq!(held, after);
    assert_eq!(after.len(), 2);
    // The `locked` entry survives too, so the locked row is still writable.
    t.put(Keyspace::Rows, k.clone(), b"v".to_vec()).unwrap();
    t.commit().unwrap();
    assert!(locks.held(TxnId(1)).is_empty());
    let mut r = txn(&seq, &locks, 2, Isolation::ReadCommitted);
    assert_eq!(r.get(Keyspace::Rows, &k).unwrap(), Some(b"v".to_vec()));
}

#[test]
fn old_snapshot_is_unregistered() {
    let (seq, locks) = setup();
    store(&seq, &locks, 1, &key(1), b"a");
    let mut t = txn(&seq, &locks, 2, Isolation::Serializable);
    assert_eq!(seq.oldest_active(), None);
    let old = t.snapshot_ts();
    assert_eq!(seq.oldest_active(), Some(old));
    store(&seq, &locks, 3, &key(1), b"b");
    let new = t.restart_snapshot().unwrap();
    assert!(new > old);
    assert_eq!(seq.oldest_active(), Some(new));
    drop(t);
    assert_eq!(seq.oldest_active(), None);
}

#[test]
fn conflict_before_restart_is_not_reported_after() {
    for restart in [false, true] {
        let (seq, locks) = setup();
        let k = key(1);
        store(&seq, &locks, 1, &k, b"a");
        let mut t = txn(&seq, &locks, 2, Isolation::Serializable);
        t.get(Keyspace::Rows, &k).unwrap();
        t.scan(
            Keyspace::Rows,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            None,
        )
        .unwrap()
        .for_each(drop);
        store(&seq, &locks, 3, &k, b"b");
        if restart {
            t.restart_snapshot().unwrap();
        }
        protected_write(&mut t, 1);
        let result = t.commit();
        if restart {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert_eq!(
                result,
                Err(TxnError::Serialization(
                    SerializationReason::ReadWriteConflict
                ))
            );
        }
    }
}

#[test]
fn written_keys_lists_only_that_keyspace() {
    let (seq, locks) = setup();
    store(&seq, &locks, 1, &key(3), b"old");
    let mut t = txn(&seq, &locks, 2, Isolation::Serializable);
    for n in [2, 1, 3] {
        t.lock_for_write(Keyspace::Rows, &key(n), None, &|| false)
            .unwrap();
    }
    t.put(Keyspace::Rows, key(2), b"x".to_vec()).unwrap();
    t.put(Keyspace::Rows, key(1), b"y".to_vec()).unwrap();
    t.delete(Keyspace::Rows, key(3)).unwrap();
    t.lock_for_write(Keyspace::Index, &key(7), None, &|| false)
        .unwrap();
    t.put(Keyspace::Index, key(7), vec![]).unwrap();
    t.put(Keyspace::Kernel, vec![0x20, 9], vec![1]).unwrap();
    t.put(Keyspace::Catalog, vec![5], vec![1]).unwrap();
    assert_eq!(
        t.written_keys(Keyspace::Rows).cloned().collect::<Vec<_>>(),
        vec![key(1), key(2), key(3)]
    );
    assert_eq!(
        t.written_keys(Keyspace::Index).cloned().collect::<Vec<_>>(),
        vec![key(7)]
    );
    assert_eq!(
        t.written_keys(Keyspace::Kernel)
            .cloned()
            .collect::<Vec<_>>(),
        vec![vec![0x20, 9]]
    );
    assert_eq!(
        t.written_keys(Keyspace::Catalog)
            .cloned()
            .collect::<Vec<_>>(),
        vec![vec![5]]
    );
    assert_eq!(t.written_keys(Keyspace::Meta).count(), 0);
    assert_eq!(t.written_keys(Keyspace::Gc).count(), 0);
    t.rollback();
}

use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::Kv,
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;

fn setup(budget: usize) -> (Arc<Sequencer>, Arc<LockManager>) {
    let seq = Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            write_set_bytes: budget,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    (seq, Arc::new(LockManager::new(LockConfig::default())))
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), Isolation::ReadCommitted)
}
fn key(n: u64) -> Vec<u8> {
    [1u64.to_be_bytes(), n.to_be_bytes()].concat()
}
fn put_row(t: &mut Transaction, n: u64, value: &[u8]) {
    t.lock_for_write(Keyspace::Rows, &key(n), None, &|| false)
        .unwrap();
    t.put(Keyspace::Rows, key(n), value.to_vec()).unwrap();
}

#[test]
fn rollback_restores_overwrite_insert_and_committed_delete() {
    let (seq, locks) = setup(4096);
    let mut seed = txn(&seq, &locks, 1);
    put_row(&mut seed, 3, b"committed C");
    seed.commit().unwrap();
    let mut t = txn(&seq, &locks, 2);
    put_row(&mut t, 1, b"original A");
    let bytes = t.write_bytes;
    let mark = t.savepoint();
    put_row(&mut t, 1, b"changed A");
    put_row(&mut t, 2, b"inserted B");
    t.lock_for_write(Keyspace::Rows, &key(3), None, &|| false)
        .unwrap();
    t.delete(Keyspace::Rows, key(3)).unwrap();
    t.rollback_to(&mark).unwrap();
    assert_eq!(t.write_bytes, bytes);
    assert_eq!(t.undo_bytes, 0);
    assert!(!t.writes.contains_key(&(Keyspace::Rows, key(2))));
    assert!(!t.writes.contains_key(&(Keyspace::Rows, key(3))));
    assert_eq!(
        t.get(Keyspace::Rows, &key(1)).unwrap(),
        Some(b"original A".to_vec())
    );
    assert_eq!(t.get(Keyspace::Rows, &key(2)).unwrap(), None);
    assert_eq!(
        t.get(Keyspace::Rows, &key(3)).unwrap(),
        Some(b"committed C".to_vec())
    );
    t.commit().unwrap();
    let mut reader = txn(&seq, &locks, 3);
    assert_eq!(
        reader.get(Keyspace::Rows, &key(1)).unwrap(),
        Some(b"original A".to_vec())
    );
    assert_eq!(reader.get(Keyspace::Rows, &key(2)).unwrap(), None);
    assert_eq!(
        reader.get(Keyspace::Rows, &key(3)).unwrap(),
        Some(b"committed C".to_vec())
    );
}

#[test]
fn rollback_distinguishes_absent_overlay_from_tombstone() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    t.lock_for_write(Keyspace::Rows, &key(1), None, &|| false)
        .unwrap();
    t.delete(Keyspace::Rows, key(1)).unwrap();
    let mark = t.savepoint();
    put_row(&mut t, 1, b"temporarily present");
    t.rollback_to(&mark).unwrap();
    assert_eq!(t.writes.get(&(Keyspace::Rows, key(1))), Some(&None));
    assert_eq!(t.write_bytes, key(1).len());
    t.commit().unwrap();
}

#[test]
fn nested_marks_invalidate_by_sequence_and_target_remains_reusable() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    put_row(&mut t, 1, b"before");
    let outer = t.savepoint();
    let same_mark = t.savepoint();
    put_row(&mut t, 1, b"middle");
    let inner = t.savepoint();
    put_row(&mut t, 1, b"after");
    t.rollback_to(&inner).unwrap();
    assert_eq!(
        t.get(Keyspace::Rows, &key(1)).unwrap(),
        Some(b"middle".to_vec())
    );
    t.rollback_to(&outer).unwrap();
    assert_eq!(t.rollback_to(&same_mark), Err(TxnError::Misuse));
    assert_eq!(t.rollback_to(&inner), Err(TxnError::Misuse));
    t.rollback_to(&outer).unwrap();
    let replacement = t.savepoint();
    assert_eq!(t.rollback_to(&same_mark), Err(TxnError::Misuse));
    put_row(&mut t, 1, b"again");
    t.rollback_to(&outer).unwrap();
    assert_eq!(t.rollback_to(&replacement), Err(TxnError::Misuse));
    assert_eq!(
        t.get(Keyspace::Rows, &key(1)).unwrap(),
        Some(b"before".to_vec())
    );
}

#[test]
fn release_keeps_writes_invalidates_descendants_and_discards_unused_undo() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    put_row(&mut t, 1, b"before");
    let outer = t.savepoint();
    put_row(&mut t, 1, b"middle");
    let inner = t.savepoint();
    let later = t.savepoint();
    put_row(&mut t, 1, b"after");
    t.release(inner.clone()).unwrap();
    assert_eq!(t.release(inner), Err(TxnError::Misuse));
    assert_eq!(t.rollback_to(&later), Err(TxnError::Misuse));
    assert_eq!(
        t.get(Keyspace::Rows, &key(1)).unwrap(),
        Some(b"after".to_vec())
    );
    assert!(t.undo_bytes > 0);
    t.rollback_to(&outer).unwrap();
    put_row(&mut t, 1, b"kept");
    t.release(outer.clone()).unwrap();
    assert!(t.undo.is_empty());
    assert_eq!(t.undo_bytes, 0);
    let fresh = t.savepoint();
    assert_eq!(t.release(outer), Err(TxnError::Misuse));
    t.release(fresh).unwrap();
    t.commit().unwrap();
    assert_eq!(
        txn(&seq, &locks, 2).get(Keyspace::Rows, &key(1)).unwrap(),
        Some(b"kept".to_vec())
    );
}

#[test]
fn rollback_recovers_write_budget_and_rejected_write_is_atomic() {
    let (seq, locks) = setup(512);
    let mut t = txn(&seq, &locks, 1);
    put_row(&mut t, 1, &[1; 180]);
    let initial_bytes = t.write_bytes;
    let mark = t.savepoint();
    put_row(&mut t, 1, &[2; 180]);
    let undo_bytes = t.undo_bytes;
    assert_eq!(
        t.put(Keyspace::Rows, key(1), vec![3; 180]),
        Err(TxnError::WriteSetLimit)
    );
    assert_eq!(t.undo_bytes, undo_bytes);
    assert_eq!(t.undo.len(), 1);
    assert_eq!(t.get(Keyspace::Rows, &key(1)).unwrap(), Some(vec![2; 180]));
    t.rollback_to(&mark).unwrap();
    assert_eq!(t.write_bytes, initial_bytes);
    assert_eq!(t.undo_bytes, 0);
    put_row(&mut t, 2, &[4; 100]);
    t.rollback_to(&mark).unwrap();
    assert_eq!(t.write_bytes, initial_bytes);
    assert_eq!(t.get(Keyspace::Rows, &key(2)).unwrap(), None);
    t.release(mark).unwrap();
    t.put(Keyspace::Rows, key(1), vec![5; 496]).unwrap();
    assert_eq!(t.write_bytes, 512);
    assert_eq!(t.undo_bytes, 0);
    t.commit().unwrap();
}

#[test]
fn undo_budget_bounds_repeated_overwrites_and_tombstones() {
    for initial in [Some(vec![0; 100]), None] {
        let (seq, locks) = setup(512);
        let mut t = txn(&seq, &locks, 1);
        t.write(Keyspace::Kernel, vec![1], initial.clone()).unwrap();
        let mark = t.savepoint();
        let mut limited = false;
        for n in 1..100 {
            let value = initial.as_ref().map(|_| vec![n; 100]);
            // Alternate a tombstone and an empty value to exercise nonzero
            // record/key charges even when no previous value bytes are retained.
            let value = if initial.is_none() && n % 2 == 1 {
                Some(vec![])
            } else {
                value
            };
            match t.write(Keyspace::Kernel, vec![1], value) {
                Ok(()) => {}
                Err(TxnError::WriteSetLimit) => {
                    limited = true;
                    break;
                }
                Err(e) => panic!("unexpected write error: {e:?}"),
            }
        }
        assert!(limited);
        t.rollback_to(&mark).unwrap();
        assert_eq!(t.writes.get(&(Keyspace::Kernel, vec![1])), Some(&initial));
        assert_eq!(t.undo_bytes, 0);
        t.release(mark).unwrap();
    }
    let (seq, locks) = setup(512);
    let mut t = txn(&seq, &locks, 1);
    let mark = t.savepoint();
    t.delete(Keyspace::Kernel, vec![1]).unwrap();
    let bytes = t.undo_bytes;
    for _ in 0..1000 {
        t.delete(Keyspace::Kernel, vec![1]).unwrap();
    }
    assert_eq!(t.undo_bytes, bytes);
    assert_eq!(t.undo.len(), 1);
    t.rollback_to(&mark).unwrap();
    assert!(t.writes.is_empty());
}

#[test]
fn rollback_retains_postmark_locks_and_write_permission() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    let mark = t.savepoint();
    put_row(&mut t, 1, b"rolled back");
    t.rollback_to(&mark).unwrap();
    assert_eq!(locks.held(TxnId(1)).len(), 1);
    let mut other = txn(&seq, &locks, 2);
    assert!(matches!(
        other.lock_for_write(Keyspace::Rows, &key(1), Some(Instant::now()), &|| false),
        Err(TxnError::LockTimeout)
    ));
    other.rollback();
    t.put(Keyspace::Rows, key(1), b"still owned".to_vec())
        .unwrap();
    t.commit().unwrap();
    assert!(locks.held(TxnId(1)).is_empty());
}

#[test]
fn rollback_retains_serializable_point_and_range_reads() {
    let (seq, locks) = setup(4096);
    let mut t = Transaction::begin(&seq, &locks, TxnId(1), Isolation::Serializable);
    let mark = t.savepoint();
    t.get(Keyspace::Rows, &key(1)).unwrap();
    t.scan(
        Keyspace::Rows,
        Bound::Unbounded,
        Bound::Unbounded,
        Direction::Forward,
        None,
    )
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    t.rollback_to(&mark).unwrap();
    assert!(t.read_set().points.contains(&(Keyspace::Rows, key(1))));
    assert_eq!(t.read_set().ranges.len(), 1);
    let mut other = txn(&seq, &locks, 2);
    put_row(&mut other, 1, b"conflict");
    other.commit().unwrap();
    assert_eq!(
        t.commit(),
        Err(TxnError::Serialization(
            SerializationReason::ReadWriteConflict
        ))
    );
}

#[test]
fn foreign_and_reused_transaction_ids_cannot_use_tokens() {
    let (seq, locks) = setup(4096);
    let mut first = txn(&seq, &locks, 1);
    let mark = first.savepoint();
    let mut other = txn(&seq, &locks, 2);
    other.savepoint();
    assert_eq!(other.rollback_to(&mark), Err(TxnError::Misuse));
    assert_eq!(other.release(mark.clone()), Err(TxnError::Misuse));
    first.rollback();
    let mut reused = txn(&seq, &locks, 1);
    let own = reused.savepoint();
    assert_eq!(reused.rollback_to(&mark), Err(TxnError::Misuse));
    assert_eq!(reused.release(mark), Err(TxnError::Misuse));
    reused.rollback_to(&own).unwrap();
    reused.release(own).unwrap();
}

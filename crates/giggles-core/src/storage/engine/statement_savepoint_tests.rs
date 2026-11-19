use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::Kv,
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;

fn setup(budget: usize) -> (Arc<Sequencer>, Arc<LockManager>) {
    let seq = Sequencer::open(
        Arc::new(Kv::new_single_writer(Arc::new(MemKv::new()))),
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

#[test]
fn statement_only_marks_do_not_journal_or_release_locks() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    let mark = t.statement_savepoint();
    t.lock_for_write(Keyspace::Rows, &key(1), None, &|| false).unwrap();
    assert!(t.lock_undo.is_empty());
    let held = locks.held(t.id);
    assert_eq!(t.rollback_to_releasing_locks(&mark), Err(TxnError::Misuse));
    t.rollback_to(&mark).unwrap();
    assert_eq!(locks.held(t.id), held);
    t.release(mark).unwrap();
    assert_eq!(t.lock_savepoints, 0);
}

#[test]
fn named_mark_journals_nested_statements_and_tracks_truncation() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    let earlier = t.statement_savepoint();
    t.lock_for_write(Keyspace::Rows, &key(1), None, &|| false).unwrap();
    t.release(earlier).unwrap();
    let held = locks.held(t.id);
    let named = t.savepoint();
    let statement = t.statement_savepoint();
    t.lock_for_write(Keyspace::Rows, &key(2), None, &|| false).unwrap();
    assert!(!t.lock_undo.is_empty());
    t.rollback_to(&statement).unwrap();
    t.release(statement).unwrap();
    assert_eq!(t.lock_savepoints, 1);
    t.rollback_to_releasing_locks(&named).unwrap();
    assert_eq!(locks.held(t.id), held);
    assert!(t.lock_undo.is_empty());
    t.release(named).unwrap();
    assert_eq!(t.lock_savepoints, 0);

    let statement = t.statement_savepoint();
    let _named = t.savepoint();
    assert_eq!(t.lock_savepoints, 1);
    t.rollback_to(&statement).unwrap();
    assert_eq!(t.lock_savepoints, 0);
    t.lock_for_write(Keyspace::Rows, &key(3), None, &|| false).unwrap();
    assert!(t.lock_undo.is_empty());
}

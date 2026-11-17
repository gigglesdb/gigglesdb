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

#[test]
fn local_read_only_drop_skips_lock_manager() {
    let (seq, locks) = setup();
    let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    let key = [1u64.to_be_bytes(), 2u64.to_be_bytes()].concat();
    assert_eq!(txn.get(Keyspace::Rows, &key).unwrap(), None);
    drop(txn);
    assert_eq!(locks.release_all_calls(), 0);
    assert_eq!(seq.oldest_active(), None);
}

#[test]
fn local_read_only_commit_skips_lock_manager() {
    let (seq, locks) = setup();
    let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    let key = [1u64.to_be_bytes(), 2u64.to_be_bytes()].concat();
    assert_eq!(txn.get(Keyspace::Rows, &key).unwrap(), None);
    txn.commit().unwrap();
    assert_eq!(locks.release_all_calls(), 0);
    assert_eq!(seq.oldest_active(), None);
}

#[test]
fn local_locking_drop_releases_each_acquisition_path() {
    for path in 0..4 {
        let (seq, locks) = setup();
        let mut txn = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
        match path {
            0 => txn
                .acquire_lock(LockKey::Table(1), LockMode::AccessExclusive, None, &|| {
                    false
                })
                .unwrap(),
            1 => txn
                .lock_sql_sentinel(1, LockMode::Exclusive, &|| false)
                .unwrap(),
            2 => txn.lock_kernel_key(b"key", None, &|| false).unwrap(),
            3 => {
                let key = [1u64.to_be_bytes(), 2u64.to_be_bytes()].concat();
                txn.lock_for_write(Keyspace::Rows, &key, None, &|| false)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(!locks.held(TxnId(1)).is_empty(), "path {path}");
        drop(txn);
        assert_eq!(locks.release_all_calls(), 1, "path {path}");
        assert!(locks.held(TxnId(1)).is_empty(), "path {path}");
    }
}

#[test]
fn failed_acquisition_still_calls_release_all_on_drop() {
    let (seq, locks) = setup();
    let holder = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    holder
        .acquire_lock(LockKey::Table(1), LockMode::AccessExclusive, None, &|| {
            false
        })
        .unwrap();
    let waiter = Transaction::begin(&seq, &locks, TxnId(2), Isolation::RepeatableRead);
    assert!(matches!(
        waiter.acquire_lock(
            LockKey::Table(1),
            LockMode::AccessShare,
            Some(Instant::now()),
            &|| false
        ),
        Err(TxnError::LockTimeout)
    ));
    drop(waiter);
    assert_eq!(locks.release_all_calls(), 1);
    assert!(!locks.held(TxnId(1)).is_empty());
    drop(holder);
    assert_eq!(locks.release_all_calls(), 2);
    assert!(locks.held(TxnId(1)).is_empty());
}

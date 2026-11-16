//! GD-0061 C6: the authority hold (`lock_kernel_key`) and `kernel_latest_committed`.
use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::Kv,
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;
use super::super::meta::PROTECTED_RECORDS;
use crate::task::records as rec;
use std::{sync::mpsc, thread, time::Duration};

fn setup() -> (Arc<Sequencer>, Arc<LockManager>) {
    let seq = Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
    )
    .unwrap();
    let locks = LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_millis(10),
        ..LockConfig::default()
    });
    (seq, Arc::new(locks))
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, i: Isolation) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), i)
}
fn authority(task: u8) -> Vec<u8> {
    rec::mutation_head_key(rec::TaskObjectId(u64::from(task)))
}
/// A well-formed mutation head; `n` makes distinct values.
fn head(n: u64) -> Vec<u8> {
    rec::MutationHead { next_seq: n, ..Default::default() }.encode()
}
fn store_kernel(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, k: &[u8], v: &[u8]) {
    let mut t = txn(seq, locks, id, Isolation::ReadCommitted);
    t.put(Keyspace::Kernel, k.to_vec(), v.to_vec()).unwrap();
    t.require_features(PROTECTED_RECORDS);
    t.commit().unwrap();
}
fn hold_key(k: &[u8]) -> LockKey {
    LockKey::Row {
        table: TASK_AUTHORITY_LOCK_TABLE,
        pk: k.to_vec(),
    }
}

#[test]
fn second_holder_waits_until_first_drops() {
    let (seq, locks) = setup();
    let k = authority(1);
    let first = txn(&seq, &locks, 1, Isolation::Serializable);
    first.lock_kernel_key(&k, None, &|| false).unwrap();
    assert_eq!(
        locks.held(TxnId(1)),
        vec![(hold_key(&k), LockMode::Exclusive)]
    );
    let (sent, got) = mpsc::channel();
    let waiter = {
        let (seq, locks, k) = (seq.clone(), locks.clone(), k.clone());
        thread::spawn(move || {
            let second = txn(&seq, &locks, 2, Isolation::Serializable);
            let result = second.lock_kernel_key(&k, None, &|| false);
            sent.send(result).unwrap();
            second.rollback();
        })
    };
    assert!(got.recv_timeout(Duration::from_millis(150)).is_err());
    assert_eq!(locks.waiting_count(), 1);
    drop(first);
    assert_eq!(got.recv_timeout(Duration::from_secs(10)).unwrap(), Ok(()));
    waiter.join().unwrap();
    assert!(locks.held(TxnId(2)).is_empty());
}

#[test]
fn different_keys_do_not_contend() {
    let (seq, locks) = setup();
    let first = txn(&seq, &locks, 1, Isolation::Serializable);
    first
        .lock_kernel_key(&authority(1), None, &|| false)
        .unwrap();
    let second = txn(&seq, &locks, 2, Isolation::Serializable);
    // A zero deadline is a nonblocking attempt.
    second
        .lock_kernel_key(&authority(2), Some(Instant::now()), &|| false)
        .unwrap();
    // The disclosure head `20|task|01` is a separate lock key.
    let mut disclosure = authority(1);
    *disclosure.last_mut().unwrap() = 0x01;
    second
        .lock_kernel_key(&disclosure, Some(Instant::now()), &|| false)
        .unwrap();
    // Neither collides with a SQL row lock of an ordinary table.
    let mut row = 1u64.to_be_bytes().to_vec();
    row.extend(authority(1));
    let mut third = txn(&seq, &locks, 3, Isolation::Serializable);
    third
        .lock_for_write(Keyspace::Rows, &row, Some(Instant::now()), &|| false)
        .unwrap();
    assert_eq!(
        second.lock_kernel_key(&[], None, &|| false),
        Err(TxnError::Misuse)
    );
}

#[test]
fn hold_does_not_fix_snapshot() {
    let (seq, locks) = setup();
    let k = authority(1);
    store_kernel(&seq, &locks, 1, &k, &head(1));
    let mut t = txn(&seq, &locks, 2, Isolation::Serializable);
    t.lock_kernel_key(&k, None, &|| false).unwrap();
    assert_eq!(t.current_snapshot_ts(), None);
    assert_eq!(seq.oldest_active(), None);
    store_kernel(&seq, &locks, 3, &k, &head(2));
    // The snapshot is chosen after the hold, so it sees the earlier commit.
    assert_eq!(
        t.get(Keyspace::Kernel, &k).unwrap(),
        Some(head(2))
    );
    t.rollback();
}

#[test]
fn latest_committed_ignores_own_snapshot() {
    let (seq, locks) = setup();
    let k = authority(1);
    let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
    assert_eq!(t.kernel_latest_committed(&k).unwrap(), None);
    store_kernel(&seq, &locks, 2, &k, &head(1));
    assert_eq!(t.get(Keyspace::Kernel, &k).unwrap(), Some(head(1)));
    store_kernel(&seq, &locks, 3, &k, &head(2));
    assert_eq!(t.get(Keyspace::Kernel, &k).unwrap(), Some(head(1)));
    assert_eq!(t.kernel_latest_committed(&k).unwrap(), Some(head(2)));
    // Own buffered writes are not committed.
    t.put(Keyspace::Kernel, k.clone(), head(3)).unwrap();
    assert_eq!(t.kernel_latest_committed(&k).unwrap(), Some(head(2)));
    // It is not a tracked read.
    assert!(
        !t.read_set()
            .points
            .contains(&(Keyspace::Kernel, authority(2)))
    );
    t.kernel_latest_committed(&authority(2)).unwrap();
    assert!(
        !t.read_set()
            .points
            .contains(&(Keyspace::Kernel, authority(2)))
    );
    // A committed delete reads as absent.
    let mut d = txn(&seq, &locks, 4, Isolation::ReadCommitted);
    d.delete(Keyspace::Kernel, k.clone()).unwrap();
    d.require_features(PROTECTED_RECORDS);
    d.commit().unwrap();
    assert_eq!(t.kernel_latest_committed(&k).unwrap(), None);
    t.rollback();
}

#[test]
fn deadline_gives_lock_timeout() {
    let (seq, locks) = setup();
    let k = authority(1);
    let first = txn(&seq, &locks, 1, Isolation::Serializable);
    first.lock_kernel_key(&k, None, &|| false).unwrap();
    let second = txn(&seq, &locks, 2, Isolation::Serializable);
    let start = Instant::now();
    let result = second.lock_kernel_key(&k, Some(start + Duration::from_millis(60)), &|| false);
    assert_eq!(result, Err(TxnError::LockTimeout));
    assert_eq!(result.unwrap_err().sqlstate(), "55P03");
    assert!(start.elapsed() >= Duration::from_millis(60));
    assert_eq!(locks.waiting_count(), 0);
    assert!(locks.held(TxnId(2)).is_empty());
    assert_eq!(
        locks.held(TxnId(1)),
        vec![(hold_key(&k), LockMode::Exclusive)]
    );
}

#[test]
fn cancel_gives_cancelled() {
    let (seq, locks) = setup();
    let k = authority(1);
    let first = txn(&seq, &locks, 1, Isolation::Serializable);
    first.lock_kernel_key(&k, None, &|| false).unwrap();
    let second = txn(&seq, &locks, 2, Isolation::Serializable);
    let result = second.lock_kernel_key(&k, None, &|| true);
    assert_eq!(result, Err(TxnError::Cancelled));
    assert_eq!(result.unwrap_err().sqlstate(), "57014");
    assert_eq!(locks.waiting_count(), 0);
    assert!(locks.held(TxnId(2)).is_empty());
    // An uncontended hold succeeds even when cancellation is pending.
    second
        .lock_kernel_key(&authority(2), None, &|| true)
        .unwrap();
}

#[test]
fn savepoint_rollback_keeps_hold() {
    let (seq, locks) = setup();
    let k = authority(1);
    let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
    let mark = t.savepoint();
    t.lock_kernel_key(&k, None, &|| false).unwrap();
    t.put(Keyspace::Kernel, k.clone(), head(4))
        .unwrap();
    t.rollback_to(&mark).unwrap();
    t.release(mark).unwrap();
    assert_eq!(t.written_keys(Keyspace::Kernel).count(), 0);
    assert_eq!(
        locks.held(TxnId(1)),
        vec![(hold_key(&k), LockMode::Exclusive)]
    );
    let other = txn(&seq, &locks, 2, Isolation::Serializable);
    assert_eq!(
        other.lock_kernel_key(&k, Some(Instant::now()), &|| false),
        Err(TxnError::LockTimeout)
    );
    // The denial path: restart, stage, commit while still holding.
    t.restart_snapshot().unwrap();
    t.put(Keyspace::Kernel, k.clone(), head(5))
        .unwrap();
    t.require_features(PROTECTED_RECORDS);
    t.commit().unwrap();
    other
        .lock_kernel_key(&k, Some(Instant::now()), &|| false)
        .unwrap();
    assert_eq!(
        other.kernel_latest_committed(&k).unwrap(),
        Some(head(5))
    );
}

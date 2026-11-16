//! Cross-range deadlock hooks: `blockers` and `cancel_wait` (GD-0075 R5-34a).

use super::*;
use std::sync::{Arc, mpsc};
use std::thread;

const BOUND: Duration = Duration::from_secs(2);

fn row(pk: u64) -> LockKey {
    LockKey::Row {
        table: 1,
        pk: pk.to_le_bytes().to_vec(),
    }
}

fn manager() -> Arc<LockManager> {
    Arc::new(LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_millis(5),
        ..LockConfig::default()
    }))
}

fn acquire(lm: &LockManager, txn: u64, key: LockKey, mode: LockMode) {
    lm.acquire(TxnId(txn), key, mode, Some(Instant::now() + BOUND), &|| {
        false
    })
    .expect("uncontended acquisition failed");
}

/// Blocks `txn` on `key` from another thread (no deadline) and waits until queued.
fn spawn_waiter(
    lm: &Arc<LockManager>,
    txn: u64,
    key: LockKey,
    mode: LockMode,
) -> mpsc::Receiver<Result<(), LockError>> {
    let before = lm.waiting_count();
    let (send, result) = mpsc::channel();
    let worker = Arc::clone(lm);
    thread::spawn(move || {
        let _ = send.send(worker.acquire(TxnId(txn), key, mode, None, &|| false));
    });
    let deadline = Instant::now() + BOUND;
    while lm.waiting_count() <= before {
        assert!(Instant::now() < deadline, "waiter never queued");
        thread::yield_now();
    }
    result
}

fn ids(txns: &[u64]) -> Vec<TxnId> {
    txns.iter().copied().map(TxnId).collect()
}

#[test]
fn blockers_of_a_waiter_are_all_conflicting_holders() {
    let lm = manager();
    acquire(&lm, 3, row(1), LockMode::Shared);
    acquire(&lm, 1, row(1), LockMode::Shared);
    let waiter = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    assert_eq!(lm.blockers(TxnId(2)), ids(&[1, 3]));
    // Holders and unrelated transactions are not waiting.
    assert!(lm.blockers(TxnId(1)).is_empty());
    assert!(lm.blockers(TxnId(3)).is_empty());
    assert!(lm.blockers(TxnId(9)).is_empty());
    lm.release_all(TxnId(1));
    lm.release_all(TxnId(3));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Ok(())));
    assert!(lm.blockers(TxnId(2)).is_empty());
    lm.release_all(TxnId(2));
}

#[test]
fn blockers_include_conflicting_waiters_queued_ahead() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let first = spawn_waiter(&lm, 2, row(1), LockMode::Shared);
    let second = spawn_waiter(&lm, 3, row(1), LockMode::Exclusive);
    let third = spawn_waiter(&lm, 4, row(1), LockMode::Shared);
    assert_eq!(lm.blockers(TxnId(2)), ids(&[1]));
    assert_eq!(lm.blockers(TxnId(3)), ids(&[1, 2]));
    // Shared conflicts with the queued Exclusive but not the queued Shared.
    assert_eq!(lm.blockers(TxnId(4)), ids(&[1, 3]));
    for txn in 1..=4 {
        lm.release_all(TxnId(txn));
    }
    for result in [first, second, third] {
        assert!(result.recv_timeout(BOUND).is_ok());
    }
}

#[test]
fn cancel_wait_ends_a_blocked_acquire_with_deadlock_and_keeps_the_table_consistent() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let victim = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    let started = Instant::now();
    lm.cancel_wait(TxnId(2));
    assert_eq!(victim.recv_timeout(BOUND), Ok(Err(LockError::Deadlock)));
    assert!(started.elapsed() < BOUND);
    assert_eq!(lm.waiting_count(), 0);
    assert!(lm.blockers(TxnId(2)).is_empty());
    assert_eq!(lm.held(TxnId(1)), vec![(row(1), LockMode::Exclusive)]);
    assert!(lm.held(TxnId(2)).is_empty());

    // A later request queues normally behind the untouched holder.
    let later = spawn_waiter(&lm, 3, row(1), LockMode::Shared);
    assert_eq!(lm.blockers(TxnId(3)), ids(&[1]));
    lm.release_all(TxnId(1));
    assert_eq!(later.recv_timeout(BOUND), Ok(Ok(())));
    assert_eq!(lm.held(TxnId(3)), vec![(row(1), LockMode::Shared)]);
    lm.release_all(TxnId(3));
}

#[test]
fn cancel_wait_unblocks_compatible_followers() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Shared);
    let victim = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    let follower = spawn_waiter(&lm, 3, row(1), LockMode::Shared);
    assert_eq!(lm.blockers(TxnId(3)), ids(&[2]));
    lm.cancel_wait(TxnId(2));
    assert_eq!(victim.recv_timeout(BOUND), Ok(Err(LockError::Deadlock)));
    assert_eq!(follower.recv_timeout(BOUND), Ok(Ok(())));
    lm.release_all(TxnId(1));
    lm.release_all(TxnId(3));
}

#[test]
fn cancelled_waiter_is_not_granted_when_the_holder_releases_first() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let victim = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    // Release in the same critical section order as a racing commit would.
    lm.cancel_wait(TxnId(2));
    lm.release_all(TxnId(1));
    assert_eq!(victim.recv_timeout(BOUND), Ok(Err(LockError::Deadlock)));
    assert!(lm.held(TxnId(2)).is_empty());
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn cancel_wait_on_a_non_waiter_is_a_noop() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    lm.cancel_wait(TxnId(1));
    lm.cancel_wait(TxnId(7));
    assert_eq!(lm.held(TxnId(1)), vec![(row(1), LockMode::Exclusive)]);
    // The flag is per request: a later wait by the same txn is unaffected.
    let waiter = spawn_waiter(&lm, 7, row(1), LockMode::Exclusive);
    assert_eq!(lm.blockers(TxnId(7)), ids(&[1]));
    lm.release_all(TxnId(1));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Ok(())));
    lm.release_all(TxnId(7));
}

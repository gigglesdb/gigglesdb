use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(2);

fn row(pk: u64) -> LockKey {
    LockKey::Row {
        table: 1,
        pk: pk.to_le_bytes().to_vec(),
    }
}

fn unique(tuple: u64) -> LockKey {
    LockKey::Unique {
        index: 1,
        tuple: tuple.to_le_bytes().to_vec(),
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

fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + BOUND;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "bounded condition did not become true"
        );
        thread::yield_now();
    }
}

struct Waiter {
    result: mpsc::Receiver<Result<(), LockError>>,
    release: mpsc::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Waiter {
    fn finish(self) {
        let _ = self.release.send(());
        self.thread.join().expect("waiter thread panicked");
    }

    fn pending(&self) {
        assert!(matches!(
            self.result.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }

    fn granted(&self) {
        self.result
            .recv_timeout(BOUND)
            .expect("grant did not arrive")
            .expect("queued acquisition failed");
    }
}

fn spawn_waiter(lm: &Arc<LockManager>, txn: u64, key: LockKey, mode: LockMode) -> Waiter {
    spawn_waiter_with(
        lm,
        txn,
        key,
        mode,
        Instant::now() + BOUND,
        Arc::new(AtomicBool::new(false)),
    )
}

fn spawn_waiter_with(
    lm: &Arc<LockManager>,
    txn: u64,
    key: LockKey,
    mode: LockMode,
    deadline: Instant,
    cancel: Arc<AtomicBool>,
) -> Waiter {
    let lm = Arc::clone(lm);
    let (send_result, result) = mpsc::channel();
    let (release, receive_release) = mpsc::channel();
    let thread = thread::spawn(move || {
        let outcome = lm.acquire(TxnId(txn), key, mode, Some(deadline), &|| {
            cancel.load(Ordering::SeqCst)
        });
        let _ = send_result.send(outcome);
        let _ = receive_release.recv();
        lm.release_all(TxnId(txn));
    });
    Waiter {
        result,
        release,
        thread,
    }
}

fn compatible(a: &LockMode, b: &LockMode) -> bool {
    use LockMode::*;
    matches!(
        (a, b),
        (AccessShare, AccessShare | RowExclusive | Share)
            | (RowExclusive | Share, AccessShare)
            | (RowExclusive, RowExclusive)
            | (Share, Share)
            | (Shared, Shared)
    )
}

#[test]
fn compatibility_matrices_are_exhaustive() {
    use LockMode::*;
    for (key, modes) in [
        (
            LockKey::Table(1),
            vec![AccessShare, RowExclusive, Share, AccessExclusive],
        ),
        (row(1), vec![Shared, Exclusive]),
        (unique(1), vec![Shared, Exclusive]),
    ] {
        for first in &modes {
            for second in &modes {
                let lm = manager();
                acquire(&lm, 1, key.clone(), *first);
                let result = lm.acquire(
                    TxnId(2),
                    key.clone(),
                    *second,
                    Some(Instant::now()),
                    &|| false,
                );
                if compatible(first, second) {
                    assert!(
                        result.is_ok(),
                        "compatible modes {first:?}/{second:?}: {result:?}"
                    );
                } else {
                    assert!(
                        matches!(result, Err(LockError::Timeout)),
                        "conflicting modes {first:?}/{second:?}: {result:?}"
                    );
                }
                assert_eq!(lm.waiting_count(), 0);
                lm.release_all(TxnId(1));
                lm.release_all(TxnId(2));
            }
        }
    }
}

#[test]
fn same_and_weaker_acquisitions_are_reentrant() {
    use LockMode::*;
    for (key, strong, weak) in [
        (row(1), Exclusive, Shared),
        (unique(1), Exclusive, Shared),
        (LockKey::Table(1), AccessExclusive, AccessShare),
        (LockKey::Table(2), RowExclusive, AccessShare),
        (LockKey::Table(3), Share, AccessShare),
    ] {
        let lm = manager();
        acquire(&lm, 1, key.clone(), strong);
        acquire(&lm, 1, key.clone(), strong);
        acquire(&lm, 1, key.clone(), weak);
        let held = lm.held(TxnId(1));
        assert_eq!(held.len(), 1);
        assert_eq!(held[0], (key.clone(), strong));
        assert_eq!(lm.waiting_count(), 0);
        lm.release_all(TxnId(1));
        assert!(lm.held(TxnId(1)).is_empty());
        acquire(
            &lm,
            2,
            key,
            if matches!(held[0].1, Shared | Exclusive) {
                Exclusive
            } else {
                AccessExclusive
            },
        );
        lm.release_all(TxnId(2));
        lm.release_all(TxnId(1));
    }
}

#[test]
fn sole_holder_upgrades_immediately() {
    let lm = manager();
    for key in [row(1), unique(1)] {
        acquire(&lm, 1, key.clone(), LockMode::Shared);
        acquire(&lm, 1, key.clone(), LockMode::Exclusive);
        assert_eq!(
            lm.held(TxnId(1)),
            vec![(key.clone(), LockMode::Shared), (key, LockMode::Exclusive)]
        );
        lm.release_all(TxnId(1));
    }
}

#[test]
fn covers_respects_incomparable_table_modes_and_mode_families() {
    use LockMode::*;
    let modes = [
        AccessShare,
        RowExclusive,
        Share,
        AccessExclusive,
        Shared,
        Exclusive,
    ];
    for strong in modes {
        for weak in modes {
            let expected = match strong {
                AccessShare => matches!(weak, AccessShare),
                RowExclusive => matches!(weak, AccessShare | RowExclusive),
                Share => matches!(weak, AccessShare | Share),
                AccessExclusive => {
                    matches!(weak, AccessShare | RowExclusive | Share | AccessExclusive)
                }
                Shared => matches!(weak, Shared),
                Exclusive => matches!(weak, Shared | Exclusive),
            };
            assert_eq!(strong.covers(weak), expected, "{strong:?}.covers({weak:?})");
        }
    }
}

#[test]
fn incomparable_table_modes_retain_their_individual_conflicts() {
    use LockMode::*;
    let lm = manager();
    let key = LockKey::Table(1);
    acquire(&lm, 1, key.clone(), RowExclusive);
    assert_eq!(
        lm.acquire(TxnId(1), key.clone(), Share, Some(Instant::now()), &|| {
            false
        }),
        Ok(())
    );
    assert_eq!(
        lm.held(TxnId(1)),
        vec![(key.clone(), RowExclusive), (key.clone(), Share)]
    );
    assert_eq!(
        lm.acquire(
            TxnId(2),
            key.clone(),
            AccessShare,
            Some(Instant::now()),
            &|| false,
        ),
        Ok(())
    );
    for (txn, mode) in [(3, RowExclusive), (4, Share)] {
        assert_eq!(
            lm.acquire(TxnId(txn), key.clone(), mode, Some(Instant::now()), &|| {
                false
            }),
            Err(LockError::Timeout)
        );
    }
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn table_access_exclusive_upgrade_retains_access_share() {
    use LockMode::*;
    let lm = manager();
    let key = LockKey::Table(1);
    acquire(&lm, 1, key.clone(), AccessShare);
    acquire(&lm, 2, key.clone(), AccessShare);
    let upgrade = spawn_waiter(&lm, 1, key.clone(), AccessExclusive);
    until(|| lm.waiting_count() == 1);
    upgrade.pending();
    assert_eq!(lm.held(TxnId(1)), vec![(key.clone(), AccessShare)]);
    lm.release_all(TxnId(2));
    upgrade.granted();
    assert_eq!(
        lm.held(TxnId(1)),
        vec![(key.clone(), AccessShare), (key, AccessExclusive)]
    );
    upgrade.finish();
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn queued_incomparable_upgrade_uses_only_requested_mode_for_blockers() {
    use LockMode::*;
    let lm = manager();
    let key = LockKey::Table(1);
    acquire(&lm, 1, key.clone(), RowExclusive);
    acquire(&lm, 1, row(1), Exclusive);
    acquire(&lm, 2, key.clone(), RowExclusive);
    acquire(&lm, 3, key.clone(), AccessShare);
    let upgrade = spawn_waiter(&lm, 1, key.clone(), Share);
    until(|| lm.waiting_count() == 1);
    // The AccessShare holder waits on txn 1, but does not block its Share
    // request. Synthesizing AccessExclusive would invent a deadlock edge.
    let follower = spawn_waiter(&lm, 3, row(1), Exclusive);
    until(|| lm.waiting_count() == 2);
    upgrade.pending();
    follower.pending();
    lm.release_all(TxnId(2));
    upgrade.granted();
    follower.pending();
    let held = lm.held(TxnId(1));
    assert_eq!(held.len(), 3);
    assert!(held.contains(&(key.clone(), RowExclusive)));
    assert!(held.contains(&(key, Share)));
    upgrade.finish();
    follower.granted();
    follower.finish();
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn upgrade_waits_for_other_shared_holder() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Shared);
    acquire(&lm, 2, row(1), LockMode::Shared);
    let upgrade = spawn_waiter(&lm, 1, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    upgrade.pending();
    assert_eq!(lm.held(TxnId(1)), vec![(row(1), LockMode::Shared)]);
    lm.release_all(TxnId(2));
    upgrade.granted();
    assert_eq!(
        lm.held(TxnId(1)),
        vec![(row(1), LockMode::Shared), (row(1), LockMode::Exclusive)]
    );
    upgrade.finish();
    assert_eq!(lm.waiting_count(), 0);
}

// Read the first completed acquisition without relying on the victim policy.
fn first_result(waiters: &[Option<Waiter>]) -> (usize, Result<(), LockError>) {
    let deadline = Instant::now() + BOUND;
    loop {
        for (i, waiter) in waiters.iter().enumerate() {
            if let Some(waiter) = waiter {
                match waiter.result.try_recv() {
                    Ok(result) => return (i, result),
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => panic!("waiter disconnected"),
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "no deadlock victim or successor completed"
        );
        thread::yield_now();
    }
}

fn resolve_cycle(lm: &LockManager, mut waiters: Vec<Option<Waiter>>) {
    let (victim, result) = first_result(&waiters);
    assert!(
        matches!(result, Err(LockError::Deadlock)),
        "cycle returned {result:?}"
    );
    for (i, waiter) in waiters.iter().enumerate() {
        if i != victim {
            waiter.as_ref().unwrap().pending();
        }
    }
    waiters[victim].take().unwrap().finish();
    while waiters.iter().any(Option::is_some) {
        let (i, result) = first_result(&waiters);
        assert!(
            result.is_ok(),
            "cycle produced more than one victim: {result:?}"
        );
        waiters[i].take().unwrap().finish();
    }
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn dual_upgrade_has_exactly_one_deadlock_victim() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Shared);
    acquire(&lm, 2, row(1), LockMode::Shared);
    let first = spawn_waiter(&lm, 1, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let second = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    resolve_cycle(&lm, vec![Some(first), Some(second)]);
}

#[test]
fn shared_exclusive_shared_queue_is_strict_fifo() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let a = spawn_waiter(&lm, 2, row(1), LockMode::Shared);
    until(|| lm.waiting_count() == 1);
    let b = spawn_waiter(&lm, 3, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 2);
    let c = spawn_waiter(&lm, 4, row(1), LockMode::Shared);
    until(|| lm.waiting_count() == 3);
    lm.release_all(TxnId(1));
    a.granted();
    b.pending();
    c.pending();
    assert_eq!(lm.waiting_count(), 2);
    a.finish();
    b.granted();
    c.pending();
    assert_eq!(lm.waiting_count(), 1);
    b.finish();
    c.granted();
    c.finish();
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn holder_upgrade_precedes_an_earlier_fresh_waiter() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Shared);
    acquire(&lm, 2, row(1), LockMode::Shared);
    let fresh = spawn_waiter(&lm, 3, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let upgrade = spawn_waiter(&lm, 1, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 2);
    upgrade.pending();
    fresh.pending();
    lm.release_all(TxnId(2));
    upgrade.granted();
    fresh.pending();
    upgrade.finish();
    fresh.granted();
    fresh.finish();
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn blocked_upgraders_keep_fifo_order_ahead_of_fresh_waiters() {
    use LockMode::*;
    let lm = manager();
    let key = LockKey::Table(1);
    acquire(&lm, 1, key.clone(), AccessShare);
    acquire(&lm, 2, key.clone(), AccessShare);
    acquire(&lm, 3, key.clone(), Share);
    let fresh = spawn_waiter(&lm, 4, key.clone(), AccessExclusive);
    until(|| lm.waiting_count() == 1);
    let first = spawn_waiter(&lm, 1, key.clone(), RowExclusive);
    until(|| lm.waiting_count() == 2);
    let second = spawn_waiter(&lm, 2, key, AccessExclusive);
    until(|| lm.waiting_count() == 3);
    lm.release_all(TxnId(3));
    first.granted();
    second.pending();
    fresh.pending();
    first.finish();
    second.granted();
    fresh.pending();
    second.finish();
    fresh.granted();
    fresh.finish();
    assert_eq!(lm.waiting_count(), 0);
}

fn deadlock_cycle(size: u64) {
    let lm = manager();
    for txn in 1..=size {
        acquire(&lm, txn, row(txn), LockMode::Exclusive);
    }
    let mut waiters = Vec::new();
    for txn in 1..=size {
        waiters.push(Some(spawn_waiter(
            &lm,
            txn,
            row(txn % size + 1),
            LockMode::Exclusive,
        )));
        if txn < size {
            until(|| lm.waiting_count() == txn as usize);
        }
    }
    resolve_cycle(&lm, waiters);
    for txn in 1..=size {
        assert!(lm.held(TxnId(txn)).is_empty());
    }
}

#[test]
fn two_transaction_deadlock_has_one_victim() {
    deadlock_cycle(2);
}

#[test]
fn three_transaction_deadlock_has_one_victim() {
    deadlock_cycle(3);
}

#[test]
fn acyclic_wait_chain_does_not_report_deadlock() {
    let lm = manager();
    for txn in 1..=3 {
        acquire(&lm, txn, row(txn), LockMode::Exclusive);
    }
    let a = spawn_waiter(&lm, 1, row(2), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let b = spawn_waiter(&lm, 2, row(3), LockMode::Exclusive);
    until(|| lm.waiting_count() == 2);
    a.pending();
    b.pending();
    lm.release_all(TxnId(3));
    b.granted();
    a.pending();
    b.finish();
    a.granted();
    a.finish();
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn timeout_removes_front_waiter_and_unblocks_compatible_follower() {
    // The front deadline is fixed when it starts waiting. Under heavy load the
    // follower may not be queued before it expires, so retry the scenario until
    // the follower is observed queued behind a still-pending front waiter.
    for attempt in 0.. {
        assert!(attempt < 20, "follower never queued before the front deadline");
        let lm = manager();
        acquire(&lm, 1, row(1), LockMode::Shared);
        let front = spawn_waiter_with(
            &lm,
            2,
            row(1),
            LockMode::Exclusive,
            Instant::now() + Duration::from_millis(250),
            Arc::new(AtomicBool::new(false)),
        );
        let mut expired = false;
        until(|| {
            expired = front.result.try_recv().is_ok();
            expired || lm.waiting_count() == 1
        });
        if expired {
            front.finish();
            lm.release_all(TxnId(1));
            continue;
        }
        let back = spawn_waiter(&lm, 3, row(1), LockMode::Shared);
        // FIFO: the follower can only be granted after the front has timed out.
        let mut overtaken = false;
        until(|| {
            overtaken = back.result.try_recv().is_ok();
            overtaken || lm.waiting_count() == 2
        });
        if overtaken {
            front.finish();
            back.finish();
            lm.release_all(TxnId(1));
            continue;
        }
        check_timeout_unblocks_follower(&lm, front, back);
        return;
    }
}

fn check_timeout_unblocks_follower(lm: &Arc<LockManager>, front: Waiter, back: Waiter) {
    back.pending();
    assert!(matches!(
        front.result.recv_timeout(BOUND).unwrap(),
        Err(LockError::Timeout)
    ));
    back.granted();
    assert!(lm.held(TxnId(2)).is_empty());
    assert_eq!(lm.waiting_count(), 0);
    front.finish();
    back.finish();
    lm.release_all(TxnId(1));
}

#[test]
fn cancellation_removes_front_waiter_and_unblocks_compatible_follower() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Shared);
    let cancel = Arc::new(AtomicBool::new(false));
    let front = spawn_waiter_with(
        &lm,
        2,
        row(1),
        LockMode::Exclusive,
        Instant::now() + BOUND,
        Arc::clone(&cancel),
    );
    until(|| lm.waiting_count() == 1);
    let back = spawn_waiter(&lm, 3, row(1), LockMode::Shared);
    until(|| lm.waiting_count() == 2);
    back.pending();
    cancel.store(true, Ordering::SeqCst);
    assert!(matches!(
        front.result.recv_timeout(BOUND).unwrap(),
        Err(LockError::Cancelled)
    ));
    back.granted();
    assert!(lm.held(TxnId(2)).is_empty());
    assert_eq!(lm.waiting_count(), 0);
    front.finish();
    back.finish();
    lm.release_all(TxnId(1));
}

#[test]
fn release_all_cancels_pending_acquisition_and_wakes_follower() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Shared);
    acquire(&lm, 2, row(2), LockMode::Exclusive);
    let front = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let back = spawn_waiter(&lm, 3, row(1), LockMode::Shared);
    until(|| lm.waiting_count() == 2);
    lm.release_all(TxnId(2));
    assert!(matches!(
        front.result.recv_timeout(BOUND).unwrap(),
        Err(LockError::Cancelled)
    ));
    back.granted();
    assert!(lm.held(TxnId(2)).is_empty());
    assert_eq!(lm.waiting_count(), 0);
    front.finish();
    back.finish();
    lm.release_all(TxnId(1));
}

#[test]
fn queued_limit_error_removes_waiter_and_drains_followers() {
    let lm = Arc::new(LockManager::new(LockConfig {
        max_row_locks_per_txn: 1,
        deadlock_recheck: Duration::from_millis(5),
    }));
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    acquire(&lm, 2, row(2), LockMode::Shared);
    let front = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let back = spawn_waiter(&lm, 3, row(1), LockMode::Shared);
    until(|| lm.waiting_count() == 2);
    lm.release_all(TxnId(1));
    assert!(matches!(
        front.result.recv_timeout(BOUND).unwrap(),
        Err(LockError::Limit)
    ));
    back.granted();
    assert_eq!(lm.held(TxnId(2)), vec![(row(2), LockMode::Shared)]);
    assert_eq!(lm.waiting_count(), 0);
    front.finish();
    back.finish();
}

#[test]
fn cancellation_callback_can_inspect_manager_without_locking_itself_out() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let other = Arc::clone(&lm);
    let (send, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = other.acquire(
            TxnId(2),
            row(1),
            LockMode::Shared,
            Some(Instant::now() + BOUND),
            &|| {
                assert_eq!(other.held(TxnId(1)).len(), 1);
                assert_eq!(other.waiting_count(), 1);
                true
            },
        );
        let _ = send.send(result);
        other.release_all(TxnId(2));
    });
    assert!(matches!(
        receive.recv_timeout(BOUND).expect("callback deadlocked"),
        Err(LockError::Cancelled)
    ));
    worker.join().unwrap();
    assert_eq!(lm.waiting_count(), 0);
    lm.release_all(TxnId(1));
}

#[test]
fn row_limit_counts_unique_keys_but_not_tables_or_reentrancy() {
    let lm = LockManager::new(LockConfig {
        max_row_locks_per_txn: 3,
        ..LockConfig::default()
    });
    for table in 1..=5 {
        acquire(&lm, 1, LockKey::Table(table), LockMode::AccessShare);
    }
    acquire(&lm, 1, row(1), LockMode::Shared);
    acquire(&lm, 1, row(2), LockMode::Shared);
    acquire(&lm, 1, unique(1), LockMode::Shared);
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    for key in [row(3), unique(2)] {
        assert!(matches!(
            lm.acquire(TxnId(1), key, LockMode::Shared, None, &|| false),
            Err(LockError::Limit)
        ));
    }
    assert_eq!(lm.held(TxnId(1)).len(), 9);
    assert_eq!(lm.waiting_count(), 0);
    lm.release_all(TxnId(1));
    for key in [row(1), row(2), unique(1)] {
        acquire(&lm, 1, key, LockMode::Exclusive);
    }
    lm.release_all(TxnId(1));
    for key in [row(1), row(2), row(3)] {
        acquire(&lm, 2, key, LockMode::Shared);
    }
    assert!(matches!(
        lm.acquire(TxnId(2), row(4), LockMode::Shared, None, &|| false),
        Err(LockError::Limit)
    ));
    assert_eq!(lm.held(TxnId(2)).len(), 3);
    lm.release_all(TxnId(2));
}

#[test]
fn invalid_mode_families_are_misuse_without_state_changes() {
    use LockMode::*;
    let lm = manager();
    for (key, modes) in [
        (LockKey::Table(1), vec![Shared, Exclusive]),
        (
            row(1),
            vec![AccessShare, RowExclusive, Share, AccessExclusive],
        ),
        (
            unique(1),
            vec![AccessShare, RowExclusive, Share, AccessExclusive],
        ),
    ] {
        acquire(
            &lm,
            1,
            key.clone(),
            if matches!(key, LockKey::Table(_)) {
                AccessExclusive
            } else {
                Exclusive
            },
        );
        let held = lm.held(TxnId(1));
        for mode in modes {
            assert!(matches!(
                lm.acquire(TxnId(1), key.clone(), mode, None, &|| false),
                Err(LockError::Misuse)
            ));
            assert_eq!(lm.held(TxnId(1)), held);
            assert_eq!(lm.waiting_count(), 0);
        }
        lm.release_all(TxnId(1));
    }
}

#[test]
fn defaults_and_error_sqlstates_are_stable() {
    let config = LockConfig::default();
    assert_eq!(config.max_row_locks_per_txn, 1_000_000);
    assert_eq!(config.deadlock_recheck, Duration::from_millis(100));
    assert_eq!(LockError::Deadlock.sqlstate(), "40P01");
    assert_eq!(LockError::Timeout.sqlstate(), "55P03");
    assert_eq!(LockError::Cancelled.sqlstate(), "57014");
    assert_eq!(LockError::Limit.sqlstate(), "53200");
    assert_eq!(LockError::Misuse.sqlstate(), "XX000");
}

// An intentionally small deterministic generator keeps the stress test dependency-free.
fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

type ShadowHolders = HashMap<u64, Vec<(LockKey, LockMode)>>;

#[test]
fn concurrent_random_transactions_preserve_lock_compatibility() {
    const THREADS: u64 = 16;
    const TRANSACTIONS: u64 = 300;
    let lm = manager();
    let shadow: Arc<Mutex<ShadowHolders>> = Arc::new(Mutex::new(HashMap::new()));
    let start = Arc::new(Barrier::new(THREADS as usize));
    let ok = Arc::new(AtomicU64::new(0));
    let deadlocks = Arc::new(AtomicU64::new(0));
    let timeouts = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for worker in 0..THREADS {
        let (lm, shadow, start) = (Arc::clone(&lm), Arc::clone(&shadow), Arc::clone(&start));
        let (ok, deadlocks, timeouts) = (
            Arc::clone(&ok),
            Arc::clone(&deadlocks),
            Arc::clone(&timeouts),
        );
        threads.push(thread::spawn(move || {
            let mut rng = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(worker + 1);
            start.wait();
            for iteration in 0..TRANSACTIONS {
                let txn = worker * TRANSACTIONS + iteration + 1;
                let deadline = Instant::now() + Duration::from_millis(50);
                let count = next_random(&mut rng) % 4 + 1;
                let mut successful = true;
                for _ in 0..count {
                    let key = row(next_random(&mut rng) % 20);
                    let mode = if next_random(&mut rng) & 1 == 0 {
                        LockMode::Shared
                    } else { LockMode::Exclusive };
                    match lm.acquire(TxnId(txn), key.clone(), mode, Some(deadline), &|| false) {
                        Ok(()) => {
                            let mut observed = shadow.lock().unwrap();
                            // Read back all held modes: reacquisition may be weaker.
                            let held = lm.held(TxnId(txn));
                            for (held_key, held_mode) in &held {
                                for (other_txn, locks) in observed.iter() {
                                    if *other_txn != txn {
                                        for (other_key, other_mode) in locks {
                                            if held_key == other_key {
                                                assert!(compatible(held_mode, other_mode),
                                                    "incompatible grants: txn {txn} {held_mode:?}, txn {other_txn} {other_mode:?}, key {held_key:?}");
                                            }
                                        }
                                    }
                                }
                            }
                            observed.insert(txn, held);
                        },
                        Err(LockError::Deadlock) => { deadlocks.fetch_add(1, Ordering::Relaxed); successful = false; break; },
                        Err(LockError::Timeout) => { timeouts.fetch_add(1, Ordering::Relaxed); successful = false; break; },
                        Err(error) => panic!("unexpected stress acquisition error: {error:?}"),
                    }
                }
                if successful { ok.fetch_add(1, Ordering::Relaxed); }
                // Remove shadow ownership before releasing actual ownership, while
                // holding the mutex through release. A new grant can therefore never
                // be compared with a stale shadow holder.
                let mut observed = shadow.lock().unwrap();
                observed.remove(&txn);
                lm.release_all(TxnId(txn));
            }
        }));
    }
    for thread in threads {
        thread.join().expect("stress worker panicked");
    }
    let (ok, deadlocks, timeouts) = (
        ok.load(Ordering::Relaxed),
        deadlocks.load(Ordering::Relaxed),
        timeouts.load(Ordering::Relaxed),
    );
    eprintln!("lock stress: Ok={ok}, Deadlock={deadlocks}, Timeout={timeouts}");
    assert_eq!(ok + deadlocks + timeouts, THREADS * TRANSACTIONS);
    assert!(ok > 0);
    assert!(shadow.lock().unwrap().is_empty());
    assert_eq!(lm.waiting_count(), 0);
    for txn in 1..=THREADS * TRANSACTIONS {
        assert!(lm.held(TxnId(txn)).is_empty());
    }
}

#[test]
fn release_all_visits_only_affected_keys_and_releases_queued_grants() {
    let lm = manager();
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    acquire(&lm, 2, row(2), LockMode::Exclusive);
    let first = spawn_waiter(&lm, 3, row(1), LockMode::Exclusive);
    let second = spawn_waiter(&lm, 4, row(2), LockMode::Exclusive);
    until(|| lm.waiting_count() == 2);
    // A read-only metadata transaction cannot disturb either queue.
    lm.release_all(TxnId(5));
    first.pending();
    second.pending();
    lm.release_all(TxnId(1));
    first.granted();
    second.pending();
    // A grant from drain_queue must be tracked just like an immediate grant.
    lm.release_all(TxnId(3));
    assert!(!lm.state.lock().unwrap().keys.contains_key(&row(1)));
    assert_eq!(lm.held(TxnId(2)), vec![(row(2), LockMode::Exclusive)]);
    lm.release_all(TxnId(2));
    second.granted();
    lm.release_all(TxnId(4));
    let state = lm.state.lock().unwrap();
    assert!(state.keys.is_empty());
    assert!(state.held_keys.is_empty());
    drop(state);
    first.finish();
    second.finish();
}

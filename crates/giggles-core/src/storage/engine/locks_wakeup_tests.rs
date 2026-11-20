//! Targeted wake-ups and optional deferred deadlock detection (GD-0084 A3).

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};
use std::thread::{self, JoinHandle};

const BOUND: Duration = Duration::from_secs(2);

fn row(pk: u8) -> LockKey {
    LockKey::Row {
        table: 1,
        pk: vec![pk],
    }
}

fn rows(start: u8, end: u8) -> LockKey {
    LockKey::Interval {
        space: LockSpace::Row,
        start: Bound::Included(point_bytes(1, &[start])),
        end: Bound::Excluded(point_bytes(1, &[end])),
    }
}

fn manager(deadlock_timeout: Duration) -> Arc<LockManager> {
    Arc::new(LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_secs(10),
        deadlock_timeout,
        ..LockConfig::default()
    }))
}

fn acquire(lm: &LockManager, txn: u64, key: LockKey, mode: LockMode) {
    assert_eq!(
        lm.acquire(TxnId(txn), key, mode, Some(Instant::now() + BOUND), &|| {
            false
        }),
        Ok(())
    );
}

#[track_caller]
fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + BOUND;
    while !condition() {
        assert!(
            Instant::now() < end,
            "waiters did not reach the expected state"
        );
        thread::yield_now();
    }
}

// Observing `sleeping` under the same mutex ensures every request has actually
// entered its wait, rather than merely having queued before the notification.
fn parked(lm: &LockManager, count: usize) {
    until(|| {
        let state = lm.state.lock().unwrap();
        state.waiters.len() == count && state.waiters.values().all(|w| w.sleeping)
    });
}

fn spawn_waiter(
    lm: &Arc<LockManager>,
    txn: u64,
    key: LockKey,
    mode: LockMode,
) -> (JoinHandle<()>, mpsc::Receiver<Result<(), LockError>>) {
    let lm = Arc::clone(lm);
    let (send, receive) = mpsc::channel();
    let handle = thread::spawn(move || {
        send.send(lm.acquire(
            TxnId(txn),
            key,
            mode,
            Some(Instant::now() + Duration::from_secs(5)),
            &|| false,
        ))
        .unwrap();
    });
    (handle, receive)
}

fn assert_clean(lm: &LockManager) {
    assert_eq!(lm.waiting_count(), 0);
    let state = lm.state.lock().unwrap();
    assert!(state.by_txn.is_empty());
    assert!(state.completed.is_empty());
    assert!(state.keys.is_empty());
    assert!(state.held_keys.is_empty());
    assert!(state.row_counts.is_empty());
    assert!(state.intervals.iter().all(HashSet::is_empty));
    assert!(state.points.iter().all(BTreeSet::is_empty));
}

#[test]
fn each_release_wakes_only_the_granted_fifo_waiter() {
    let lm = manager(Duration::ZERO);
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let mut waiters = Vec::new();
    for txn in 2..=11 {
        waiters.push(spawn_waiter(&lm, txn, row(1), LockMode::Exclusive));
        parked(&lm, waiters.len());
    }
    for (index, (handle, result)) in waiters.into_iter().enumerate() {
        lm.release_all(TxnId(index as u64 + 1));
        assert_eq!(result.recv_timeout(BOUND), Ok(Ok(())));
        handle.join().unwrap();
        parked(&lm, 9 - index);
        assert_eq!(lm.waiter_wakeups.load(Ordering::Relaxed), index + 1);
    }
    lm.release_all(TxnId(11));
    assert_eq!(lm.waiter_wakeups.load(Ordering::Relaxed), 10);
    assert_clean(&lm);
}

#[test]
fn uncontended_grant_does_not_wake_an_unrelated_queue() {
    let lm = manager(Duration::ZERO);
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let (handle, result) = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    parked(&lm, 1);
    acquire(&lm, 3, row(2), LockMode::Exclusive);
    lm.release_all(TxnId(3));
    // Give an erroneously notified waiter time to re-enter its wait.
    thread::sleep(Duration::from_millis(20));
    assert_eq!(lm.waiter_wakeups.load(Ordering::Relaxed), 0);
    lm.release_all(TxnId(1));
    assert_eq!(result.recv_timeout(BOUND), Ok(Ok(())));
    handle.join().unwrap();
    assert_eq!(lm.waiter_wakeups.load(Ordering::Relaxed), 1);
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

fn cross_wait_cycle(timeout: Duration) -> Duration {
    let lm = manager(timeout);
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    acquire(&lm, 2, row(2), LockMode::Exclusive);
    let (send, receive) = mpsc::channel();
    let mut handles = Vec::new();
    for (txn, key) in [(1, row(2)), (2, row(1))] {
        let worker = Arc::clone(&lm);
        let send = send.clone();
        handles.push(thread::spawn(move || {
            let start = Instant::now();
            let result = worker.acquire(
                TxnId(txn),
                key,
                LockMode::Exclusive,
                Some(start + Duration::from_secs(5)),
                &|| false,
            );
            let elapsed = start.elapsed();
            worker.release_all(TxnId(txn));
            send.send((result, elapsed)).unwrap();
        }));
        if txn == 1 {
            parked(&lm, 1);
        }
    }
    let results = [
        receive.recv_timeout(BOUND).unwrap(),
        receive.recv_timeout(BOUND).unwrap(),
    ];
    for handle in handles {
        handle.join().unwrap();
    }
    let mut deadlock = None;
    for (result, elapsed) in results {
        match result {
            Err(error @ LockError::Deadlock) => {
                assert_eq!(error.sqlstate(), "40P01");
                assert!(elapsed >= timeout);
                deadlock = Some(elapsed);
            }
            Ok(()) => {}
            other => panic!("unexpected cycle outcome: {other:?}"),
        }
    }
    assert_clean(&lm);
    deadlock.expect("cycle had no deadlock victim")
}

#[test]
fn deferred_deadlock_is_detected_after_200ms_without_waiting_for_poll() {
    assert!(cross_wait_cycle(Duration::from_millis(200)) < BOUND);
}

#[test]
fn zero_deadlock_timeout_detects_cycle_promptly() {
    assert!(cross_wait_cycle(Duration::ZERO) < Duration::from_millis(100));
}

fn grant_closed_cycle(queued_grant: bool) {
    let lm = manager(Duration::ZERO);
    acquire(&lm, 1, row(9), LockMode::Exclusive);
    acquire(&lm, 3, rows(1, 2), LockMode::Shared);
    // T1 waits for T3, and T2 waits for T1. There is no cycle yet.
    let (first_handle, first) = spawn_waiter(&lm, 1, row(1), LockMode::Exclusive);
    parked(&lm, 1);
    let (second_handle, second) = spawn_waiter(&lm, 2, row(9), LockMode::Exclusive);
    parked(&lm, 2);
    let granted = if queued_grant {
        // A distinct overlapping interval of T2 waits for T4's point. Releasing
        // T4 grants it, adding T1 -> T2 while T2 still waits for T1 on row 9.
        acquire(&lm, 4, row(2), LockMode::Exclusive);
        let pending = spawn_waiter(&lm, 2, rows(1, 3), LockMode::Shared);
        parked(&lm, 3);
        lm.release_all(TxnId(4));
        assert_eq!(pending.1.recv_timeout(BOUND), Ok(Ok(())));
        Some(pending.0)
    } else {
        // Compatible Shared holders of the same interval; the new grant adds
        // T1 -> T2 across keys even though that interval has an empty queue.
        acquire(&lm, 2, rows(1, 2), LockMode::Shared);
        None
    };
    // Either side may be the first victim. Both affected sleeping requests must
    // wake even if that first victim removes the cycle before the other checks.
    let mut first_result = None;
    let mut second_result = None;
    until(|| {
        if first_result.is_none() {
            first_result = first.try_recv().ok();
        }
        if second_result.is_none() {
            second_result = second.try_recv().ok();
        }
        first_result == Some(Err(LockError::Deadlock))
            || second_result == Some(Err(LockError::Deadlock))
    });
    until(|| lm.waiter_wakeups.load(Ordering::Relaxed) >= if queued_grant { 3 } else { 2 });
    lm.release_all(TxnId(1));
    lm.release_all(TxnId(2));
    for result in [
        first_result.unwrap_or_else(|| first.recv_timeout(BOUND).unwrap()),
        second_result.unwrap_or_else(|| second.recv_timeout(BOUND).unwrap()),
    ] {
        assert!(matches!(
            result,
            Ok(()) | Err(LockError::Deadlock | LockError::Cancelled)
        ));
    }
    first_handle.join().unwrap();
    second_handle.join().unwrap();
    if let Some(handle) = granted {
        handle.join().unwrap();
    }
    lm.release_all(TxnId(2));
    lm.release_all(TxnId(3));
    assert_clean(&lm);
}

#[test]
fn immediate_interval_grant_closes_cycle_and_wakes_overlapping_queue() {
    grant_closed_cycle(false);
}

#[test]
fn queued_interval_grant_closes_cycle_and_wakes_overlapping_queue() {
    grant_closed_cycle(true);
}

#[test]
fn cancellation_and_release_all_notify_every_request_of_the_transaction() {
    for cancel in [true, false] {
        let lm = manager(Duration::ZERO);
        acquire(&lm, 1, row(1), LockMode::Exclusive);
        acquire(&lm, 1, row(2), LockMode::Exclusive);
        let first = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
        let second = spawn_waiter(&lm, 2, row(2), LockMode::Exclusive);
        parked(&lm, 2);
        assert_eq!(lm.blockers(TxnId(2)), vec![TxnId(1)]);
        if cancel {
            lm.cancel_wait(TxnId(2));
        } else {
            lm.release_all(TxnId(2));
        }
        let error = if cancel {
            LockError::Deadlock
        } else {
            LockError::Cancelled
        };
        for (handle, result) in [first, second] {
            assert_eq!(result.recv_timeout(BOUND), Ok(Err(error)));
            handle.join().unwrap();
        }
        assert_eq!(lm.waiter_wakeups.load(Ordering::Relaxed), 2);
        lm.release_all(TxnId(1));
        assert_clean(&lm);
    }
}

#[test]
fn deferred_detection_preserves_deadline_and_cancellation_polling() {
    let lm = LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_millis(5),
        deadlock_timeout: Duration::from_millis(200),
        ..LockConfig::default()
    });
    acquire(&lm, 1, row(1), LockMode::Exclusive);
    let start = Instant::now();
    assert_eq!(
        lm.acquire(
            TxnId(2),
            row(1),
            LockMode::Exclusive,
            Some(start + Duration::from_millis(20)),
            &|| false
        ),
        Err(LockError::Timeout)
    );
    assert!(start.elapsed() < Duration::from_millis(200));
    let polls = AtomicUsize::new(0);
    assert_eq!(
        lm.acquire(
            TxnId(2),
            row(1),
            LockMode::Exclusive,
            Some(Instant::now() + BOUND),
            &|| polls.fetch_add(1, Ordering::Relaxed) >= 2
        ),
        Err(LockError::Cancelled)
    );
    assert_eq!(polls.load(Ordering::Relaxed), 3);
    lm.release_all(TxnId(1));
    assert_clean(&lm);
}

#[test]
fn stress_32_threads_fifo_no_lost_wakeup() {
    let lm = manager(Duration::ZERO);
    let start = Arc::new(Barrier::new(32));
    let crossing = Arc::new(Barrier::new(2));
    let critical = Arc::new(AtomicUsize::new(0));
    let deadlocks = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for worker in 0..32 {
        let lm = Arc::clone(&lm);
        let start = Arc::clone(&start);
        let crossing = Arc::clone(&crossing);
        let critical = Arc::clone(&critical);
        let deadlocks = Arc::clone(&deadlocks);
        handles.push(thread::spawn(move || {
            let txn = TxnId(worker + 1);
            let attempt = |key, mode| {
                let result = lm.acquire(
                    txn,
                    key,
                    mode,
                    Some(Instant::now() + Duration::from_secs(1)),
                    &|| false,
                );
                match result {
                    Ok(()) => true,
                    Err(LockError::Deadlock) => {
                        deadlocks.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                    other => panic!("worker {worker}: unexpected acquire outcome {other:?}"),
                }
            };
            // Force one interval/point cycle before the random workload, so a
            // successful run must exercise deadlock cleanup as well as grants.
            if worker == 0 {
                assert!(attempt(row(250), LockMode::Exclusive));
            }
            if worker == 1 {
                assert!(attempt(rows(251, 252), LockMode::Shared));
            }
            start.wait();
            if worker < 2 {
                crossing.wait();
                let _ = attempt(
                    row(if worker == 0 { 251 } else { 250 }),
                    LockMode::Exclusive,
                );
                lm.release_all(txn);
            }
            let mut random = worker + 1;
            for iteration in 0..12 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                // Some transactions enter with an interval held, reversing the
                // sentinel/point order and creating additional possible cycles.
                if worker % 8 == 0 && iteration % 3 == 0 {
                    assert!(attempt(rows(1, 5), LockMode::Shared));
                }
                let sentinel = LockKey::Row {
                    table: 1,
                    pk: vec![],
                };
                if attempt(sentinel, LockMode::Exclusive) {
                    assert_eq!(
                        critical.fetch_add(1, Ordering::SeqCst),
                        0,
                        "sentinel granted to two transactions"
                    );
                    let _ = attempt(row((random % 8 + 1) as u8), LockMode::Exclusive);
                    thread::yield_now();
                    assert_eq!(critical.fetch_sub(1, Ordering::SeqCst), 1);
                }
                lm.release_all(txn);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert!(deadlocks.load(Ordering::Relaxed) > 0);
    assert_eq!(critical.load(Ordering::SeqCst), 0);
    assert_clean(&lm);
}

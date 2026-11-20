//! Interval (gap) locks: Shared intervals against Exclusive point keys (GD-0075 R5-72a).

use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::{Arc, mpsc};
use std::thread;

const BOUND: Duration = Duration::from_secs(2);
// How long a test watches a blocked request to confirm it is still waiting.
const STILL: Duration = Duration::from_millis(30);

fn row(pk: u8) -> LockKey {
    LockKey::Row {
        table: 1,
        pk: vec![pk],
    }
}

fn unique(tuple: u8) -> LockKey {
    LockKey::Unique {
        index: 1,
        tuple: vec![tuple],
    }
}

/// Byte form of `row(pk)` and `unique(pk)`.
fn at(pk: u8) -> Vec<u8> {
    point_bytes(1, &[pk])
}

fn interval(space: LockSpace, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> LockKey {
    LockKey::Interval { space, start, end }
}

/// Rows `[start, end)` of table 1.
fn rows(start: u8, end: u8) -> LockKey {
    interval(LockSpace::Row, Included(at(start)), Excluded(at(end)))
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

/// A nonblocking attempt (zero deadline).
fn now(lm: &LockManager, txn: u64, key: LockKey, mode: LockMode) -> Result<(), LockError> {
    lm.acquire(TxnId(txn), key, mode, Some(Instant::now()), &|| false)
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

fn still_waiting(result: &mpsc::Receiver<Result<(), LockError>>) {
    assert_eq!(
        result.recv_timeout(STILL),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
}

/// Every key and index entry is gone once all transactions released.
fn assert_clean(lm: &LockManager) {
    let state = lm.global().unwrap();
    assert!(state.guards.slice().iter().all(|s| s.keys.is_empty()), "keys left: {:?}", state.keys().collect::<Vec<_>>());
    assert!(state.guards.slice().iter().all(|s| s.waiters.is_empty()));
    assert!(state.intervals.iter().all(HashSet::is_empty));
    assert!(state.points.iter().all(BTreeMap::is_empty));
}

#[test]
fn interval_blocks_another_txns_exclusive_point_inside_it_only() {
    let lm = manager();
    acquire(&lm, 1, rows(2, 5), LockMode::Shared);
    assert_eq!(
        now(&lm, 2, row(2), LockMode::Exclusive),
        Err(LockError::Timeout)
    );
    assert_eq!(
        now(&lm, 2, row(4), LockMode::Exclusive),
        Err(LockError::Timeout)
    );
    assert_eq!(now(&lm, 2, row(5), LockMode::Exclusive), Ok(()));
    assert_eq!(now(&lm, 2, row(1), LockMode::Exclusive), Ok(()));
    assert_eq!(now(&lm, 2, row(3), LockMode::Shared), Ok(()));
    // A Shared point holder upgrading inside the interval is blocked too.
    assert_eq!(
        now(&lm, 2, row(3), LockMode::Exclusive),
        Err(LockError::Timeout)
    );
    lm.release_all(TxnId(1));
    assert_eq!(now(&lm, 2, row(3), LockMode::Exclusive), Ok(()));
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn interval_bounds_include_exclude_and_unbounded_ends() {
    let other_table = LockKey::Row {
        table: 9,
        pk: vec![0],
    };
    // (start, end, Exclusive points inside, Exclusive points outside)
    type Case = (Bound<Vec<u8>>, Bound<Vec<u8>>, Vec<LockKey>, Vec<LockKey>);
    let cases: Vec<Case> = vec![
        (
            Included(at(2)),
            Included(at(4)),
            vec![row(2), row(3), row(4)],
            vec![row(1), row(5)],
        ),
        (
            Excluded(at(2)),
            Excluded(at(4)),
            vec![row(3)],
            vec![row(2), row(4)],
        ),
        (
            Unbounded,
            Excluded(at(3)),
            vec![row(0), row(2)],
            vec![row(3), other_table.clone()],
        ),
        (
            Excluded(at(3)),
            Unbounded,
            vec![row(4), row(255), other_table.clone()],
            vec![row(3)],
        ),
        (
            Unbounded,
            Unbounded,
            vec![row(0), row(255), other_table],
            vec![],
        ),
    ];
    for (start, end, inside, outside) in cases {
        let lm = manager();
        let key = interval(LockSpace::Row, start, end);
        acquire(&lm, 1, key.clone(), LockMode::Shared);
        for point in &inside {
            assert_eq!(
                now(&lm, 2, point.clone(), LockMode::Exclusive),
                Err(LockError::Timeout),
                "{point:?} inside {key:?}"
            );
        }
        for point in &outside {
            assert_eq!(
                now(&lm, 2, point.clone(), LockMode::Exclusive),
                Ok(()),
                "{point:?}"
            );
        }
        lm.release_all(TxnId(1));
        lm.release_all(TxnId(2));
        // The reverse order: Exclusive points first, then the interval.
        for point in &inside {
            acquire(&lm, 2, point.clone(), LockMode::Exclusive);
            assert_eq!(
                now(&lm, 1, key.clone(), LockMode::Shared),
                Err(LockError::Timeout)
            );
            lm.release_all(TxnId(2));
        }
        for point in &outside {
            acquire(&lm, 2, point.clone(), LockMode::Exclusive);
        }
        assert_eq!(now(&lm, 1, key.clone(), LockMode::Shared), Ok(()));
        lm.release_all(TxnId(1));
        lm.release_all(TxnId(2));
        assert_clean(&lm);
    }
}

#[test]
fn writer_sentinel_and_table_keys_are_never_inside_an_interval() {
    let lm = manager();
    let sentinel = LockKey::Row {
        table: 1,
        pk: Vec::new(),
    };
    let everything = interval(LockSpace::Row, Unbounded, Unbounded);
    acquire(&lm, 1, everything.clone(), LockMode::Shared);
    assert_eq!(now(&lm, 2, sentinel.clone(), LockMode::Exclusive), Ok(()));
    assert_eq!(
        now(&lm, 2, LockKey::Table(1), LockMode::AccessExclusive),
        Ok(())
    );
    lm.release_all(TxnId(1));
    assert_eq!(now(&lm, 1, everything, LockMode::Shared), Ok(()));
    lm.release_all(TxnId(1));
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn unique_space_is_separate_from_row_space() {
    let lm = manager();
    acquire(
        &lm,
        1,
        interval(LockSpace::Unique, Unbounded, Unbounded),
        LockMode::Shared,
    );
    assert_eq!(now(&lm, 2, row(3), LockMode::Exclusive), Ok(()));
    assert_eq!(
        now(&lm, 2, unique(3), LockMode::Exclusive),
        Err(LockError::Timeout)
    );
    // T2's Row Exclusive does not block a Unique interval over the same bytes.
    assert_eq!(
        now(
            &lm,
            3,
            interval(LockSpace::Unique, Included(at(3)), Included(at(3))),
            LockMode::Shared
        ),
        Ok(())
    );
    assert_eq!(
        now(&lm, 3, rows(0, 9), LockMode::Shared),
        Err(LockError::Timeout)
    );
    for txn in 1..=3 {
        lm.release_all(TxnId(txn));
    }
    assert_clean(&lm);
}

#[test]
fn exclusive_point_blocks_covering_interval_until_released() {
    let lm = manager();
    acquire(&lm, 1, row(3), LockMode::Exclusive);
    assert_eq!(
        now(&lm, 2, rows(2, 5), LockMode::Shared),
        Err(LockError::Timeout)
    );
    assert_eq!(now(&lm, 2, rows(4, 6), LockMode::Shared), Ok(()));
    // A bounded wait times out and leaves no queue entry behind.
    let deadline = Some(Instant::now() + Duration::from_millis(20));
    assert_eq!(
        lm.acquire(TxnId(2), rows(0, 4), LockMode::Shared, deadline, &|| false),
        Err(LockError::Timeout)
    );
    let waiter = spawn_waiter(&lm, 2, rows(2, 5), LockMode::Shared);
    still_waiting(&waiter);
    assert_eq!(lm.blockers(TxnId(2)), vec![TxnId(1)]);
    lm.release_all(TxnId(1));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Ok(())));
    assert!(lm.held(TxnId(2)).contains(&(rows(2, 5), LockMode::Shared)));
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn intervals_are_compatible_with_intervals_and_shared_points() {
    let lm = manager();
    acquire(&lm, 4, row(6), LockMode::Shared);
    acquire(&lm, 1, rows(0, 9), LockMode::Shared);
    assert_eq!(now(&lm, 2, rows(2, 5), LockMode::Shared), Ok(()));
    assert_eq!(now(&lm, 3, row(3), LockMode::Shared), Ok(()));
    assert_eq!(now(&lm, 2, row(4), LockMode::Shared), Ok(()));
    assert_eq!(now(&lm, 3, rows(1, 8), LockMode::Shared), Ok(()));
    for txn in 1..=4 {
        lm.release_all(TxnId(txn));
    }
    assert_clean(&lm);
}

#[test]
fn own_interval_never_blocks_own_exclusive_points() {
    let lm = manager();
    acquire(&lm, 1, rows(0, 9), LockMode::Shared);
    assert_eq!(now(&lm, 1, row(3), LockMode::Exclusive), Ok(()));
    // Reentrant, and a new own interval over an own Exclusive point.
    assert_eq!(now(&lm, 1, rows(0, 9), LockMode::Shared), Ok(()));
    assert_eq!(now(&lm, 1, rows(3, 5), LockMode::Shared), Ok(()));
    assert_eq!(
        now(&lm, 2, row(5), LockMode::Exclusive),
        Err(LockError::Timeout)
    );
    assert_eq!(
        now(&lm, 2, rows(2, 4), LockMode::Shared),
        Err(LockError::Timeout)
    );
    lm.release_all(TxnId(1));
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn releasing_every_covering_interval_grants_a_queued_exclusive_point() {
    let lm = manager();
    acquire(&lm, 1, rows(0, 9), LockMode::Shared);
    acquire(&lm, 3, rows(2, 4), LockMode::Shared);
    let waiter = spawn_waiter(&lm, 2, row(3), LockMode::Exclusive);
    assert_eq!(lm.blockers(TxnId(2)), vec![TxnId(1), TxnId(3)]);
    lm.release_all(TxnId(1));
    still_waiting(&waiter);
    assert_eq!(lm.blockers(TxnId(2)), vec![TxnId(3)]);
    lm.release_all(TxnId(3));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Ok(())));
    assert_eq!(lm.held(TxnId(2)), vec![(row(3), LockMode::Exclusive)]);
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn releasing_an_exclusive_point_grants_a_queued_interval() {
    let lm = manager();
    acquire(&lm, 1, row(3), LockMode::Exclusive);
    acquire(&lm, 1, unique(3), LockMode::Exclusive);
    let waiter = spawn_waiter(&lm, 2, rows(0, 9), LockMode::Shared);
    still_waiting(&waiter);
    lm.release_all(TxnId(1));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Ok(())));
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn deadlock_across_interval_and_point_keys() {
    // T1 holds an interval over row 1 and waits for row 5 (held by T2); T2 then
    // requests row 1 and closes the cycle.
    let lm = manager();
    acquire(&lm, 1, rows(0, 4), LockMode::Shared);
    acquire(&lm, 2, row(5), LockMode::Exclusive);
    let t1 = spawn_waiter(&lm, 1, row(5), LockMode::Exclusive);
    assert_eq!(lm.blockers(TxnId(1)), vec![TxnId(2)]);
    assert_eq!(
        lm.acquire(
            TxnId(2),
            row(1),
            LockMode::Exclusive,
            Some(Instant::now() + BOUND),
            &|| false
        ),
        Err(LockError::Deadlock)
    );
    lm.release_all(TxnId(2));
    assert_eq!(t1.recv_timeout(BOUND), Ok(Ok(())));
    lm.release_all(TxnId(1));
    assert_clean(&lm);
}

#[test]
fn blockers_of_a_point_waiter_name_the_interval_holder() {
    // The other order: T2 waits on the interval holder first, so T1 closes the cycle.
    let lm = manager();
    acquire(&lm, 1, rows(0, 4), LockMode::Shared);
    acquire(&lm, 2, row(5), LockMode::Exclusive);
    let t2 = spawn_waiter(&lm, 2, row(1), LockMode::Exclusive);
    assert_eq!(lm.blockers(TxnId(2)), vec![TxnId(1)]);
    assert_eq!(
        lm.acquire(
            TxnId(1),
            row(5),
            LockMode::Exclusive,
            Some(Instant::now() + BOUND),
            &|| false
        ),
        Err(LockError::Deadlock)
    );
    lm.release_all(TxnId(1));
    assert_eq!(t2.recv_timeout(BOUND), Ok(Ok(())));
    lm.release_all(TxnId(2));
    assert_clean(&lm);
}

#[test]
fn cancel_wait_and_release_all_end_an_interval_waiter() {
    let lm = manager();
    acquire(&lm, 1, row(3), LockMode::Exclusive);
    let waiter = spawn_waiter(&lm, 2, rows(0, 9), LockMode::Shared);
    lm.cancel_wait(TxnId(2));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Err(LockError::Deadlock)));
    assert_eq!(lm.waiting_count(), 0);
    let waiter = spawn_waiter(&lm, 2, rows(0, 9), LockMode::Shared);
    lm.release_all(TxnId(2));
    assert_eq!(waiter.recv_timeout(BOUND), Ok(Err(LockError::Cancelled)));
    lm.release_all(TxnId(1));
    assert_clean(&lm);
}

#[test]
fn interval_misuse_and_empty_intervals() {
    let lm = manager();
    for mode in [
        LockMode::Exclusive,
        LockMode::AccessShare,
        LockMode::RowExclusive,
    ] {
        assert_eq!(now(&lm, 1, rows(0, 9), mode), Err(LockError::Misuse));
    }
    // Empty intervals are granted without being recorded.
    for (start, end) in [
        (Included(at(5)), Excluded(at(5))),
        (Excluded(at(5)), Included(at(5))),
        (Excluded(at(5)), Excluded(at(5))),
        (Included(at(6)), Included(at(5))),
    ] {
        assert_eq!(
            now(
                &lm,
                1,
                interval(LockSpace::Row, start, end),
                LockMode::Shared
            ),
            Ok(())
        );
    }
    assert!(lm.held(TxnId(1)).is_empty());
    assert_clean(&lm);
}

#[test]
fn held_lists_intervals_and_they_count_toward_the_row_limit() {
    let lm = LockManager::new(LockConfig {
        max_row_locks_per_txn: 2,
        ..LockConfig::default()
    });
    acquire(&lm, 1, rows(0, 4), LockMode::Shared);
    acquire(&lm, 1, row(7), LockMode::Exclusive);
    // Reacquiring a held interval takes no new slot.
    acquire(&lm, 1, rows(0, 4), LockMode::Shared);
    assert_eq!(
        now(&lm, 1, rows(5, 6), LockMode::Shared),
        Err(LockError::Limit)
    );
    let mut held = lm.held(TxnId(1));
    held.sort_by_key(|(key, _)| format!("{key:?}"));
    assert_eq!(
        held,
        vec![
            (rows(0, 4), LockMode::Shared),
            (row(7), LockMode::Exclusive)
        ]
    );
    lm.release_all(TxnId(1));
    assert_clean(&lm);
}

/// Review S5: points are indexed only once a space sees an interval. Exclusive points held
/// before the first interval still block it (the index is built then), and a manager that
/// never sees an interval keeps no index.
#[test]
fn points_held_before_the_first_interval_still_block_it() {
    let lm = manager();
    acquire(&lm, 1, row(5), LockMode::Exclusive);
    acquire(&lm, 1, unique(5), LockMode::Exclusive);
    {
        let state = lm.global().unwrap();
        assert_eq!(state.indexed, [false, false]);
        assert!(state.points.iter().all(BTreeMap::is_empty));
    }
    assert_eq!(
        now(&lm, 2, rows(1, 9), LockMode::Shared),
        Err(LockError::Timeout)
    );
    {
        let state = lm.global().unwrap();
        assert_eq!(state.indexed, [true, false]);
    }
    // Later points in the indexed space are indexed as they come.
    acquire(&lm, 3, row(20), LockMode::Exclusive);
    assert_eq!(
        now(&lm, 2, rows(15, 25), LockMode::Shared),
        Err(LockError::Timeout)
    );
    lm.release_all(TxnId(1));
    lm.release_all(TxnId(3));
    now(&lm, 2, rows(1, 30), LockMode::Shared).unwrap();
}

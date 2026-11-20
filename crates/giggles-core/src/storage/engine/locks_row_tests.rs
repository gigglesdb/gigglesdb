use super::*;
fn keys() -> (LockKey, LockKey) {
    (LockKey::Row { table: 1, pk: vec![1] }, LockKey::RowKey { table: 1, pk: vec![1] })
}
#[test]
fn failed_group_acquisition_leaves_no_partial_holding() {
    let lm = LockManager::new(LockConfig::default());
    let (d, k) = keys();
    lm.acquire(TxnId(1), d.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    assert_eq!(lm.acquire_points(TxnId(2), &[(k.clone(), LockMode::Exclusive), (d, LockMode::Exclusive)],
        Some(Instant::now()), &|| false), Err(LockError::Timeout));
    lm.acquire(TxnId(3), k, LockMode::Exclusive, Some(Instant::now()), &|| false).unwrap();
}
#[test]
fn waiting_group_retains_k_but_allows_existing_locker_upgrade() {
    let lm = Arc::new(LockManager::new(LockConfig { deadlock_recheck: Duration::from_millis(1), ..Default::default() }));
    let (d, k) = keys();
    lm.acquire(TxnId(1), d.clone(), LockMode::Shared, None, &|| false).unwrap();
    let worker = lm.clone(); let wd = d.clone();
    let handle = std::thread::spawn(move || worker.acquire_points(TxnId(2), &[(wd, LockMode::Exclusive), (k, LockMode::Exclusive)],
        Some(Instant::now() + Duration::from_secs(3)), &|| false));
    let until = Instant::now() + Duration::from_secs(2);
    while lm.blockers(TxnId(2)).is_empty() {
        assert!(Instant::now() < until); std::thread::yield_now();
    }
    lm.acquire(TxnId(3), keys().1, LockMode::Shared, Some(Instant::now()), &|| false).unwrap();
    lm.release_all(TxnId(3));
    lm.acquire(TxnId(1), keys().1, LockMode::Exclusive, Some(Instant::now()), &|| false).unwrap();
    lm.release_all(TxnId(1));
    handle.join().unwrap().unwrap();
}

#[test]
fn two_group_waiters_make_progress_after_holder_ends() {
    let lm = Arc::new(LockManager::new(LockConfig::default()));
    let (d, k) = keys();
    lm.acquire(TxnId(1), d.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    let mut handles = vec![];
    for id in [TxnId(2), TxnId(3)] {
        let worker = lm.clone(); let d = d.clone(); let k = k.clone();
        handles.push(std::thread::spawn(move || {
            let result = worker.acquire_points(id, &[(k, LockMode::Exclusive), (d, LockMode::Exclusive)],
                Some(Instant::now() + Duration::from_secs(3)), &|| false);
            worker.release_all(id);
            result
        }));
    }
    let until = Instant::now() + Duration::from_secs(2);
    while lm.blockers(TxnId(2)).is_empty() || lm.blockers(TxnId(3)).is_empty() {
        assert!(Instant::now() < until); std::thread::yield_now();
    }
    lm.release_all(TxnId(1));
    for handle in handles { handle.join().unwrap().unwrap(); }
}

// PG18.6 oracle/upgrade-l6b.txt: only KEY SHARE is compatible with A's
// NO KEY UPDATE. Retaining C also exercises B's post-D K revalidation.
#[test]
fn compatible_group_bypasses_provisional_k_and_waiter_revalidates() {
    let lm = Arc::new(LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_millis(1), ..Default::default()
    }));
    let (d, k) = keys();
    lm.acquire(TxnId(1), d.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    let worker = lm.clone(); let wd = d.clone(); let wk = k.clone();
    let handle = std::thread::spawn(move || worker.acquire_points(TxnId(2),
        &[(wk, LockMode::Exclusive), (wd, LockMode::Exclusive)],
        Some(Instant::now() + Duration::from_secs(3)), &|| false));
    let until = Instant::now() + Duration::from_secs(2);
    while lm.blockers(TxnId(2)) != vec![TxnId(1)] {
        assert!(Instant::now() < until); std::thread::yield_now();
    }
    for d_mode in [LockMode::Shared, LockMode::Exclusive] {
        assert_eq!(lm.acquire_points(TxnId(4),
            &[(k.clone(), LockMode::Shared), (d.clone(), d_mode)],
            Some(Instant::now()), &|| false), Err(LockError::Timeout));
        assert!(lm.held(TxnId(4)).is_empty());
    }
    // Both the NOWAIT preflight and the blocking immediate path bypass B.
    lm.acquire_points(TxnId(3), &[(k.clone(), LockMode::Shared)],
        Some(Instant::now()), &|| false).unwrap();
    lm.acquire_points(TxnId(5), &[(k.clone(), LockMode::Shared)],
        None, &|| false).unwrap();
    lm.release_all(TxnId(5));
    lm.release_all(TxnId(1));
    let until = Instant::now() + Duration::from_secs(2);
    while lm.blockers(TxnId(2)) != vec![TxnId(3)] {
        assert!(Instant::now() < until); std::thread::yield_now();
    }
    // PG18.6 oracle/upgrade-l6c.txt: both K and D remain provisional.
    lm.acquire_points(TxnId(4), &[(k.clone(), LockMode::Shared)],
        Some(Instant::now()), &|| false).unwrap();
    lm.release_all(TxnId(4));
    lm.acquire_points(TxnId(3), &[(k, LockMode::Shared), (d, LockMode::Exclusive)],
        Some(Instant::now()), &|| false).unwrap();
    assert_eq!(lm.blockers(TxnId(2)), vec![TxnId(3)]);
    lm.release_all(TxnId(3));
    handle.join().unwrap().unwrap();
    lm.release_all(TxnId(2));
}

#[test]
fn queued_compatible_locker_bypasses_conflicting_waiters() {
    let lm = Arc::new(LockManager::new(LockConfig::default()));
    let (d, k) = keys();
    for id in [TxnId(1), TxnId(4)] {
        lm.acquire(id, d.clone(), LockMode::Shared, None, &|| false).unwrap();
    }
    let spawn = |id, group: Vec<(LockKey, LockMode)>| {
        let lm = lm.clone();
        std::thread::spawn(move || lm.acquire_points(id, &group,
            Some(Instant::now() + Duration::from_secs(5)), &|| false))
    };
    let wait = |id| {
        let until = Instant::now() + Duration::from_secs(2);
        while lm.blockers(id).is_empty() {
            assert!(Instant::now() < until); std::thread::yield_now();
        }
    };
    let b = spawn(TxnId(2), vec![(k.clone(), LockMode::Exclusive), (d.clone(), LockMode::Exclusive)]);
    wait(TxnId(2));
    lm.acquire(TxnId(1), k.clone(), LockMode::Exclusive, Some(Instant::now()), &|| false).unwrap();
    let e = spawn(TxnId(5), vec![(k.clone(), LockMode::Exclusive), (d, LockMode::Exclusive)]);
    wait(TxnId(5));
    let c = spawn(TxnId(3), vec![(k, LockMode::Shared)]);
    wait(TxnId(3));
    lm.release_all(TxnId(1));
    c.join().unwrap().unwrap();
    assert!(lm.blockers(TxnId(3)).is_empty());
    assert!(!lm.blockers(TxnId(5)).is_empty());
    lm.release_all(TxnId(3));
    lm.release_all(TxnId(4));
    b.join().unwrap().unwrap(); lm.release_all(TxnId(2));
    e.join().unwrap().unwrap(); lm.release_all(TxnId(5));
}

// PG18.6 oracle/upgrade-l6c.txt: timeout removes the failed group's additions.
#[test]
fn incomplete_group_restores_previous_modes_on_every_wait_error() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for error in [LockError::Timeout, LockError::Cancelled, LockError::Deadlock] {
        let lm = Arc::new(LockManager::new(LockConfig {
            deadlock_recheck: Duration::from_millis(1), ..Default::default()
        }));
        let (d, k) = keys();
        lm.acquire(TxnId(1), d.clone(), LockMode::Shared, None, &|| false).unwrap();
        lm.acquire_points(TxnId(2), &[(k.clone(), LockMode::Shared), (d.clone(), LockMode::Shared)],
            None, &|| false).unwrap();
        let before = lm.held(TxnId(2)).into_iter().collect::<HashSet<_>>();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker = lm.clone(); let wd = d.clone(); let wk = k.clone(); let cancel = cancelled.clone();
        let handle = std::thread::spawn(move || worker.acquire_points(TxnId(2),
            &[(wk, LockMode::Exclusive), (wd, LockMode::Exclusive)],
            Some(Instant::now() + Duration::from_millis(500)), &|| cancel.load(Ordering::SeqCst)));
        let until = Instant::now() + Duration::from_secs(2);
        while lm.blockers(TxnId(2)) != vec![TxnId(1)] {
            assert!(Instant::now() < until); std::thread::yield_now();
        }
        lm.acquire_points(TxnId(3), &[(k.clone(), LockMode::Shared)],
            Some(Instant::now()), &|| false).unwrap();
        match error {
            LockError::Cancelled => cancelled.store(true, Ordering::SeqCst),
            LockError::Deadlock => lm.cancel_wait(TxnId(2)),
            _ => {}
        }
        assert_eq!(handle.join().unwrap(), Err(error));
        assert_eq!(lm.held(TxnId(2)).into_iter().collect::<HashSet<_>>(), before);
        assert!(!lm.state.lock().unwrap().provisional.keys().any(|(txn, _)| *txn == TxnId(2)));
        lm.release_all(TxnId(1));
        lm.acquire_points(TxnId(4), &[(k.clone(), LockMode::Shared)],
            Some(Instant::now()), &|| false).unwrap();
        lm.release_all(TxnId(4));
        let worker = lm.clone();
        let handle = std::thread::spawn(move || worker.acquire_points(TxnId(2),
            &[(k, LockMode::Exclusive), (d, LockMode::Exclusive)],
            Some(Instant::now() + Duration::from_secs(3)), &|| false));
        let until = Instant::now() + Duration::from_secs(2);
        while lm.blockers(TxnId(2)) != vec![TxnId(3)] {
            assert!(Instant::now() < until); std::thread::yield_now();
        }
        lm.release_all(TxnId(3));
        handle.join().unwrap().unwrap();
        lm.release_all(TxnId(2));
    }
}

fn group_for(kind: usize, row: u8) -> Vec<(LockKey, LockMode)> {
    let k = LockKey::RowKey { table: 1, pk: vec![row] };
    let d = LockKey::Row { table: 1, pk: vec![row] };
    use LockMode::{Exclusive as X, Shared as S};
    match kind {
        0 => vec![(k, S)], // KEY SHARE
        1 => vec![(k, S), (d, S)], // SHARE
        2 | 5 => vec![(d, X)], // NO KEY UPDATE / plain writer
        3 | 4 => vec![(k, X), (d, X)], // UPDATE / DELETE
        _ => unreachable!(),
    }
}

fn wait_for_group(lm: &LockManager, txn: TxnId) {
    let until = Instant::now() + Duration::from_secs(5);
    while lm.blockers(txn).is_empty() {
        assert!(Instant::now() < until, "transaction {txn:?} never queued");
        std::thread::yield_now();
    }
}

#[test]
fn row_group_conflict_matrix_on_immediate_and_waited_grants() {
    // Rows/columns: KEY SHARE, SHARE, NO KEY UPDATE, UPDATE, DELETE, writer.
    let conflicts = [
        [false, false, false, true, true, false],
        [false, false, true, true, true, true],
        [false, true, true, true, true, true],
        [true, true, true, true, true, true],
        [true, true, true, true, true, true],
        [false, true, true, true, true, true],
    ];
    for waited in [false, true] {
        for (holder, row) in conflicts.iter().enumerate() {
            for (requester, conflict) in row.iter().enumerate() {
                let lm = Arc::new(LockManager::new(LockConfig::default()));
                let group = group_for(holder, 1);
                if waited {
                    lm.acquire(TxnId(9), group[0].0.clone(), LockMode::Exclusive, None, &|| false).unwrap();
                    let worker = lm.clone();
                    let handle = std::thread::spawn(move || worker.acquire_points(TxnId(1), &group, None, &|| false));
                    wait_for_group(&lm, TxnId(1));
                    lm.release_all(TxnId(9));
                    handle.join().unwrap().unwrap();
                } else {
                    lm.acquire_points(TxnId(1), &group, None, &|| false).unwrap();
                }
                let result = lm.acquire_points(TxnId(2), &group_for(requester, 1), Some(Instant::now()), &|| false);
                assert_eq!(result, if *conflict { Err(LockError::Timeout) } else { Ok(()) },
                    "holder={holder} requester={requester} waited={waited}");
                if *conflict {
                    assert!(lm.held(TxnId(2)).is_empty());
                    let worker = lm.clone();
                    let group = group_for(requester, 1);
                    let expected = group.clone();
                    let handle = std::thread::spawn(move || worker.acquire_points(TxnId(2), &group,
                        Some(Instant::now() + Duration::from_secs(5)), &|| false));
                    wait_for_group(&lm, TxnId(2));
                    lm.release_all(TxnId(1));
                    handle.join().unwrap().unwrap();
                    assert_eq!(lm.held(TxnId(2)).into_iter().collect::<HashSet<_>>(),
                        expected.into_iter().collect());
                }
            }
        }
    }
}

#[test]
fn nowait_group_preserves_all_previous_modes_when_either_member_blocks() {
    for blocked in 0..2 {
        for previous in [false, true] {
            let lm = LockManager::new(LockConfig::default());
            let group = group_for(3, 1);
            if previous {
                lm.acquire_points(TxnId(2), &group_for(1, 1), None, &|| false).unwrap();
            }
            // Shared blocks the requested upgrade while permitting old holdings.
            lm.acquire(TxnId(1), group[blocked].0.clone(), LockMode::Shared, None, &|| false).unwrap();
            let before = lm.held(TxnId(2)).into_iter().collect::<HashSet<_>>();
            assert_eq!(lm.acquire_points(TxnId(2), &group, Some(Instant::now()), &|| false), Err(LockError::Timeout));
            assert_eq!(lm.held(TxnId(2)).into_iter().collect::<HashSet<_>>(), before);
            assert!(lm.state.lock().unwrap().provisional.is_empty());
        }
    }
}

#[test]
fn grouped_writers_and_key_share_lockers_do_not_lose_wakeups() {
    let lm = Arc::new(LockManager::new(LockConfig::default()));
    let (send, recv) = std::sync::mpsc::channel();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let exclusive = Arc::new(std::array::from_fn::<_, 4, _>(|_| std::sync::atomic::AtomicUsize::new(0)));
    let mut handles = Vec::new();
    for id in 1..=16 {
        let lm = lm.clone(); let send = send.clone(); let barrier = barrier.clone();
        let exclusive = exclusive.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for iteration in 0..64 {
                let group = group_for(if id % 4 == 0 { 0 } else { 3 }, 1 + ((iteration + id) % 4) as u8);
                loop {
                    let result = lm.acquire_points(TxnId(id), &group, None, &|| false);
                    match result {
                        Ok(()) => {
                            if id % 4 != 0 {
                                use std::sync::atomic::Ordering::SeqCst;
                                let counter = &exclusive[((iteration + id) % 4) as usize];
                                assert_eq!(counter.fetch_add(1, SeqCst), 0);
                                std::thread::yield_now();
                                assert_eq!(counter.fetch_sub(1, SeqCst), 1);
                            }
                            lm.release_all(TxnId(id));
                            break;
                        }
                        Err(LockError::Deadlock) => {
                            // Evidence captured under the detection mutex, before
                            // removal destroys the cycle: every retry had a cycle.
                            assert!(lm.state.lock().unwrap().deadlock_victims.remove(&TxnId(id)));
                            lm.release_all(TxnId(id));
                        }
                        error => { lm.release_all(TxnId(id)); send.send(error).unwrap(); return; }
                    }
                }
            }
            send.send(Ok(())).unwrap();
        }));
    }
    for _ in 0..16 { assert_eq!(recv.recv_timeout(Duration::from_secs(20)).unwrap(), Ok(())); }
    for handle in handles { handle.join().unwrap(); }
    assert_eq!(lm.waiting_count(), 0);
    assert!(lm.state.lock().unwrap().provisional.is_empty());
}

#[test]
fn grouped_deadlock_selects_one_victim_at_wait_time() {
    let lm = Arc::new(LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_secs(30), ..Default::default()
    }));
    lm.acquire_points(TxnId(1), &group_for(3, 1), None, &|| false).unwrap();
    lm.acquire_points(TxnId(2), &group_for(3, 2), None, &|| false).unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let mut handles = Vec::new();
    for (id, row) in [(1, 2), (2, 1)] {
        let worker = lm.clone(); let send = send.clone();
        handles.push(std::thread::spawn(move || {
            let result = worker.acquire_points(TxnId(id), &group_for(3, row), None, &|| false);
            worker.release_all(TxnId(id));
            send.send(result).unwrap();
        }));
        if id == 1 { wait_for_group(&lm, TxnId(1)); }
    }
    // Much shorter than deadlock_recheck: detection must happen at queue time.
    let results = [recv.recv_timeout(Duration::from_secs(2)).unwrap(), recv.recv_timeout(Duration::from_secs(2)).unwrap()];
    assert_eq!(results.iter().filter(|r| **r == Err(LockError::Deadlock)).count(), 1);
    assert_eq!(results.iter().filter(|r| **r == Ok(())).count(), 1);
    for handle in handles { handle.join().unwrap(); }
}
#[test]
fn blocking_covered_group_preserves_member_mode_sets_and_skips_drain() {
    let lm = LockManager::new(LockConfig::default());
    let (d, k) = keys();
    let txn = TxnId(1);
    lm.acquire_points(txn, &[(k.clone(), LockMode::Exclusive), (d.clone(), LockMode::Exclusive)], None, &|| false).unwrap();
    // A cancelled waiter at D's head must stay queued: K's finish_group
    // clears provisional, and D's covered shortcut never drains or notifies.
    let id = {
        let mut state = lm.state.lock().unwrap();
        let id = state.next_waiter;
        state.next_waiter += 1;
        state.waiters.insert(id, Waiter {
            txn: TxnId(2), key: d.clone(), mode: LockMode::Exclusive,
            upgrade: false, group: vec![(d.clone(), LockMode::Exclusive)],
            cancelled: true, changed: Arc::new(Condvar::new()), sleeping: false, notified: AtomicBool::new(false),
        });
        state.by_txn.entry(TxnId(2)).or_default().push(id);
        state.keys.get_mut(&d).unwrap().queue.push(id);
        id
    };
    lm.acquire_points(txn, &[(k.clone(), LockMode::Shared), (d.clone(), LockMode::Shared)], None, &|| false).unwrap();
    assert_eq!(lm.held(txn).into_iter().collect::<HashSet<_>>(), HashSet::from([
        (k.clone(), LockMode::Shared), (k, LockMode::Exclusive), (d, LockMode::Exclusive),
    ]));
    assert!(lm.state.lock().unwrap().waiters.contains_key(&id));
}

#[test]
fn blocking_duplicate_members_keep_only_the_old_covered_modes() {
    let lm = LockManager::new(LockConfig::default());
    let (_, k) = keys();
    lm.acquire_points(TxnId(1), &[(k.clone(), LockMode::Exclusive), (k.clone(), LockMode::Shared)], None, &|| false).unwrap();
    assert_eq!(lm.held(TxnId(1)), vec![(k, LockMode::Exclusive)]);
}

#[test]
fn blocking_group_limit_rolls_back_the_provisional_first_member() {
    let lm = LockManager::new(LockConfig { max_row_locks_per_txn: 1, ..Default::default() });
    let (d, k) = keys();
    assert_eq!(lm.acquire_points(TxnId(1), &[(k, LockMode::Exclusive), (d, LockMode::Exclusive)], None, &|| false), Err(LockError::Limit));
    assert!(lm.held(TxnId(1)).is_empty());
    assert!(lm.state.lock().unwrap().provisional.is_empty());
}

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

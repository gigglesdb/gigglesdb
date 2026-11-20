use super::*;
use super::reference::ReferenceLockManager;
use std::sync::{Barrier, mpsc};
use std::thread;

fn placement(key: &LockKey) -> usize {
    match key {
        LockKey::Table(t) => *t as usize,
        LockKey::Row { pk, .. } | LockKey::RowKey { pk, .. } => pk.first().copied().unwrap_or(0) as usize,
        LockKey::Unique { tuple, .. } => tuple.first().copied().unwrap_or(1) as usize + 1,
        LockKey::Interval { .. } => 0,
    }
}
fn row(pk: u8) -> LockKey { LockKey::Row { table: 1, pk: vec![pk] } }
fn config(shards: usize) -> LockConfig {
    LockConfig { shards, placement: Some(placement), deadlock_recheck: Duration::from_secs(3600), ..Default::default() }
}
fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13; *state ^= *state >> 7; *state ^= *state << 17; *state
}
#[test]
fn differential_seeded_model() {
    let mut universe = vec![];
    for table in 1..=2 {
        universe.push(LockKey::Table(table));
        for pk in 1..=3 {
            universe.push(LockKey::Row { table, pk: vec![pk] });
            universe.push(LockKey::RowKey { table, pk: vec![pk] });
            universe.push(LockKey::Unique { index: table, tuple: vec![pk] });
        }
    }
    universe.push(LockKey::Interval { space: LockSpace::Row, start: Bound::Included(point_bytes(1, &[1])), end: Bound::Included(point_bytes(2, &[3])) });
    universe.push(LockKey::Interval { space: LockSpace::Unique, start: Bound::Unbounded, end: Bound::Unbounded });
    let modes = [LockMode::Shared, LockMode::Exclusive];
    let table_modes = [LockMode::AccessShare, LockMode::RowExclusive, LockMode::AccessExclusive, LockMode::Share];
    for shards in [1, 2, 64] {
        for forced in [false, true] {
            for seed in 1..=200 {
                let mut rng = seed;
                let mut cfg = config(shards);
                cfg.max_row_locks_per_txn = 5;
                if !forced { cfg.placement = None; }
                let reference = ReferenceLockManager::new(cfg.clone());
                let actual = LockManager::new(cfg);
                let mut history = vec![Vec::new(); 4];
                for step in 0..200 {
                    let txn = TxnId(random(&mut rng) % 4);
                    let key = &universe[random(&mut rng) as usize % universe.len()];
                    let mode = if matches!(key, LockKey::Table(_)) { table_modes[random(&mut rng) as usize % 4] }
                        else if matches!(key, LockKey::Interval { .. }) { LockMode::Shared }
                        else { modes[random(&mut rng) as usize % 2] };
                    let deadline = Some(Instant::now());
                    match random(&mut rng) % 7 {
                        0 | 1 => assert_eq!(actual.acquire(txn, key.clone(), mode, deadline, &|| false), reference.acquire(txn, key.clone(), mode, deadline, &|| false), "seed {seed} step {step}"),
                        2 => {
                            let pk = vec![(random(&mut rng) % 3 + 1) as u8];
                            let group = [(LockKey::RowKey { table: 1, pk: pk.clone() }, mode), (LockKey::Row { table: 1, pk }, mode), (LockKey::Unique { index: 2, tuple: vec![1] }, LockMode::Exclusive)];
                            assert_eq!(actual.acquire_points(txn, &group, deadline, &|| false), reference.acquire_points(txn, &group, deadline, &|| false), "seed {seed} step {step}");
                        }
                        3 => { actual.release_all(txn); reference.release_all(txn); }
                        4 => {
                            // Always include table and row snapshots, with 2-4
                            // distinct keys, so rollback exercises mixed accounting.
                            let count = 2 + random(&mut rng) as usize % 3;
                            let mut keys = vec![(LockKey::Table(rng % 2 + 1), LockMode::AccessShare),
                                (row((rng % 3 + 1) as u8), LockMode::Shared)];
                            while keys.len() < count {
                                let candidate = universe[random(&mut rng) as usize % universe.len()].clone();
                                if !keys.iter().any(|(k, _)| *k == candidate) {
                                    keys.push((candidate, mode));
                                }
                            }
                            let a = actual.modes_on(txn, &keys); let b = reference.modes_on(txn, &keys);
                            assert_eq!(a, b); history[txn.0 as usize] = a;
                        }
                        5 => { actual.restore_modes(txn, &history[txn.0 as usize]); reference.restore_modes(txn, &history[txn.0 as usize]); }
                        _ => assert_eq!(actual.blockers(txn), reference.blockers(txn)),
                    }
                    let table = rng % 2 + 1;
                    let table_mode = table_modes[(rng / 2) as usize % 4];
                    assert_eq!(actual.fastpath(txn, table, table_mode), reference.fastpath(txn, table, table_mode));
                    for t in 0..4 {
                        let t = TxnId(t);
                        assert_eq!(actual.held(t).into_iter().collect::<HashSet<_>>(), reference.held(t).into_iter().collect::<HashSet<_>>(), "shards {shards} forced {forced} seed {seed} step {step}");

                    }
                }
            }
        }
    }
}
fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !condition() { assert!(Instant::now() < end, "waiter failed to progress"); thread::yield_now(); }
}
#[test]
fn slot_notifications_between_global_and_slot_are_retained() {
    for action in 0..3 {
        let lm = Arc::new(LockManager::new(LockConfig { deadlock_recheck: Duration::from_secs(3600), ..config(2) }));
        lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let reached = Arc::new(Barrier::new(2)); let resume = Arc::new(Barrier::new(2));
        let once = Arc::new(AtomicUsize::new(0));
        *lm.hooks.h2.lock().unwrap() = Some(Arc::new({ let reached = reached.clone(); let resume = resume.clone(); move || {
            if once.fetch_add(1, Ordering::Relaxed) == 0 { reached.wait(); resume.wait(); }
        }}));
        let (send, recv) = mpsc::channel();
        let worker = thread::spawn({ let lm = lm.clone(); move || {
            send.send(lm.acquire(TxnId(2), row(1), LockMode::Exclusive, None, &|| false)).unwrap();
        }});
        reached.wait();
        let expected = match action {
            0 => { lm.release_all(TxnId(1)); Ok(()) }
            1 => { lm.release_all(TxnId(2)); Err(LockError::Cancelled) }
            _ => { lm.cancel_wait(TxnId(2)); Err(LockError::Deadlock) }
        };
        resume.wait(); assert_eq!(recv.recv_timeout(Duration::from_secs(2)), Ok(expected)); worker.join().unwrap();
        lm.release_all(TxnId(1)); lm.release_all(TxnId(2)); assert_eq!(lm.waiting_count(), 0);
    }
}
#[test]
fn callbacks_are_unlocked_and_cancel_precedes_sleep() {
    let lm = LockManager::new(LockConfig { deadlock_recheck: Duration::from_secs(3600), ..config(2) });
    lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
    let calls = AtomicUsize::new(0);
    assert_eq!(lm.acquire(TxnId(2), row(1), LockMode::Exclusive, Some(Instant::now() + Duration::from_secs(1)), &|| {
        calls.fetch_add(1, Ordering::Relaxed); lm.held(TxnId(1)); assert_eq!(lm.waiting_count(), 1); true
    }), Err(LockError::Cancelled));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
#[test]
fn table_only_and_cross_shard_release_and_poison_recovery() {
    for table_only in [true, false] {
        let lm = Arc::new(LockManager::new(config(2)));
        lm.acquire(TxnId(1), LockKey::Table(0), LockMode::AccessShare, None, &|| false).unwrap();
        if !table_only { lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap(); }
        let poisoned = lm.clone();
        assert!(thread::spawn(move || { let _guard = poisoned.shards[1].lock().unwrap(); panic!("intentional poison"); }).join().is_err());
        assert_eq!(lm.acquire(TxnId(2), row(1), LockMode::Shared, None, &|| false), Err(LockError::Misuse));
        lm.release_all(TxnId(1)); assert!(lm.held(TxnId(1)).is_empty());
    }
}
#[test]
fn slot_stress_every_acquire_finishes() {
    let lm = Arc::new(LockManager::new(config(2)));
    let start = Arc::new(Barrier::new(33));
    let (done, receive) = mpsc::channel();
    let watchdog = thread::spawn(move || {
        let end = Instant::now() + Duration::from_secs(25);
        for _ in 0..32 {
            receive.recv_timeout(end.saturating_duration_since(Instant::now()))
                .expect("stress worker failed to finish (possible lost notification)");
        }
    });
    let threads = (0..32).map(|t| {
        let lm = lm.clone(); let start = start.clone(); let done = done.clone();
        thread::spawn(move || {
            let mut rng = t + 1; start.wait();
            let end = Instant::now() + Duration::from_secs(10);
            while Instant::now() < end {
                let result = lm.acquire(TxnId(t), row((random(&mut rng) % 8) as u8),
                    LockMode::Exclusive, None, &|| false);
                assert!(matches!(result, Ok(()) | Err(LockError::Deadlock)));
                lm.release_all(TxnId(t));
            }
            done.send(()).unwrap();
        })
    }).collect::<Vec<_>>();
    drop(done);
    start.wait();
    // Join the watchdog first so a stuck worker cannot hang this test.
    watchdog.join().unwrap();
    for thread in threads { thread.join().unwrap(); }
    assert_eq!(lm.waiting_count(), 0);
    assert!(lm.registry.stripes.iter().all(|s| s.lock().unwrap().is_empty()));
}
#[test]
fn shard_placement_couples_row_and_rowkey() {
    let lm = LockManager::new(LockConfig { shards: 64, ..Default::default() });
    for table in 0..4 { for pk in 0..100 {
        assert_eq!(lm.shard(&LockKey::Row { table, pk: vec![pk] }), lm.shard(&LockKey::RowKey { table, pk: vec![pk] }));
    } }
}

trait ScriptManager: Send + Sync + 'static {
    fn take(&self, txn: TxnId, key: LockKey, mode: LockMode, deadline: Option<Instant>, cancel: &dyn Fn() -> bool) -> Result<(), LockError>;
    fn group(&self, txn: TxnId, keys: &[(LockKey, LockMode)], deadline: Option<Instant>) -> Result<(), LockError>;
    fn release(&self, txn: TxnId);
    fn waiting(&self) -> usize;
    fn victim(&self, txn: TxnId);
}
macro_rules! script_manager {
    ($t:ty) => { impl ScriptManager for $t {
        fn take(&self, txn: TxnId, key: LockKey, mode: LockMode, deadline: Option<Instant>, cancel: &dyn Fn() -> bool) -> Result<(), LockError> { self.acquire(txn, key, mode, deadline, cancel) }
        fn group(&self, txn: TxnId, keys: &[(LockKey, LockMode)], deadline: Option<Instant>) -> Result<(), LockError> { self.acquire_points(txn, keys, deadline, &|| false) }
        fn release(&self, txn: TxnId) { self.release_all(txn); }
        fn waiting(&self) -> usize { self.waiting_count() }
        fn victim(&self, txn: TxnId) { self.cancel_wait(txn); }
    } };
}
script_manager!(LockManager);
script_manager!(ReferenceLockManager);
type Pending = (thread::JoinHandle<()>, mpsc::Receiver<Result<(), LockError>>);
fn spawn<M: ScriptManager>(m: &Arc<M>, txn: u64, key: LockKey, mode: LockMode) -> Pending {
    let (send, recv) = mpsc::channel(); let m = m.clone();
    (thread::spawn(move || { send.send(m.take(TxnId(txn), key, mode, Some(Instant::now() + Duration::from_secs(10)), &|| false)).unwrap(); }), recv)
}
fn finish(p: Pending) -> Result<(), LockError> {
    let result = p.1.recv_timeout(Duration::from_secs(5)).unwrap(); p.0.join().unwrap(); result
}
fn scripted<M: ScriptManager>(m: Arc<M>) -> Vec<Result<(), LockError>> {
    let mut outcomes = vec![];
    let take = |txn, key, mode| m.take(TxnId(txn), key, mode, None, &|| false).unwrap();
    // Three writers, fixed enqueue and release order.
    take(0, row(1), LockMode::Exclusive);
    let mut writers = vec![];
    for txn in 1..=3 { writers.push(spawn(&m, txn, row(1), LockMode::Exclusive)); until(|| m.waiting() == txn as usize); }
    for (i, writer) in writers.into_iter().enumerate() {
        m.release(TxnId(i as u64)); outcomes.push(finish(writer));
    }
    m.release(TxnId(3));
    // Upgrade inserted before a non-upgrade writer.
    take(0, row(1), LockMode::Shared); take(1, row(1), LockMode::Shared);
    let writer = spawn(&m, 2, row(1), LockMode::Exclusive); until(|| m.waiting() == 1);
    let upgrade = spawn(&m, 0, row(1), LockMode::Exclusive); until(|| m.waiting() == 2);
    m.release(TxnId(1)); outcomes.push(finish(upgrade)); m.release(TxnId(0)); outcomes.push(finish(writer)); m.release(TxnId(2));
    // Two- and three-way cycles across forced different shards.
    for length in [2, 3] {
        for txn in 0..length { take(txn, row(txn as u8), LockMode::Exclusive); }
        let mut pending = vec![];
        for txn in 0..length-1 {
            pending.push(spawn(&m, txn, row((txn + 1) as u8), LockMode::Exclusive)); until(|| m.waiting() == (txn+1) as usize);
        }
        outcomes.push(m.take(TxnId(length-1), row(0), LockMode::Exclusive, None, &|| false));
        m.release(TxnId(length-1));
        for (txn, p) in pending.into_iter().enumerate().rev() { outcomes.push(finish(p)); m.release(TxnId(txn as u64)); }
    }
    // A provisional K+D group admits compatible K-only bypassers.
    let k = LockKey::RowKey { table: 1, pk: vec![1] };
    take(0, row(1), LockMode::Exclusive);
    let (send, receive) = mpsc::channel();
    let group_worker = thread::spawn({ let m = m.clone(); let k = k.clone(); move || {
        send.send(m.group(TxnId(1), &[(k, LockMode::Exclusive), (row(1), LockMode::Exclusive)], None)).unwrap();
    }});
    until(|| m.waiting() == 1);
    outcomes.push(m.group(TxnId(2), &[(k.clone(), LockMode::Shared)], Some(Instant::now())));
    outcomes.push(m.group(TxnId(3), &[(k.clone(), LockMode::Shared)], None));
    m.release(TxnId(2)); m.release(TxnId(3)); m.release(TxnId(0));
    outcomes.push(receive.recv_timeout(Duration::from_secs(5)).unwrap()); group_worker.join().unwrap(); m.release(TxnId(1));
    // A group closes a cycle whose first edge spans two distinct rows.
    take(0, row(1), LockMode::Exclusive); take(1, row(2), LockMode::Exclusive);
    let (send, receive) = mpsc::channel();
    let group_worker = thread::spawn({ let m = m.clone(); move || {
        send.send(m.group(TxnId(0), &[(LockKey::RowKey { table: 1, pk: vec![2] }, LockMode::Exclusive), (row(2), LockMode::Exclusive)], None)).unwrap();
    }});
    until(|| m.waiting() == 1);
    outcomes.push(m.group(TxnId(1), &[(k, LockMode::Exclusive), (row(1), LockMode::Exclusive)], None));
    m.release(TxnId(1)); outcomes.push(receive.recv_timeout(Duration::from_secs(5)).unwrap()); group_worker.join().unwrap(); m.release(TxnId(0));
    // NOWAIT/SKIP LOCKED, lock timeout, callback cancel, detector victim, abort.
    take(0, row(1), LockMode::Exclusive);
    for _ in 0..2 { outcomes.push(m.take(TxnId(1), row(1), LockMode::Exclusive, Some(Instant::now()), &|| false)); }
    outcomes.push(m.take(TxnId(1), row(1), LockMode::Exclusive, Some(Instant::now() + Duration::from_millis(5)), &|| false));
    outcomes.push(m.take(TxnId(1), row(1), LockMode::Exclusive, None, &|| true));
    let p = spawn(&m, 1, row(1), LockMode::Exclusive); until(|| m.waiting() == 1); m.victim(TxnId(1)); outcomes.push(finish(p));
    let p = spawn(&m, 1, row(1), LockMode::Exclusive); until(|| m.waiting() == 1); m.release(TxnId(1)); outcomes.push(finish(p)); m.release(TxnId(0));
    // Cross-shard group cannot leave a partial grant on NOWAIT failure.
    take(0, row(1), LockMode::Exclusive);
    let group = [(LockKey::Unique { index: 1, tuple: vec![1] }, LockMode::Exclusive), (row(1), LockMode::Exclusive)];
    outcomes.push(m.group(TxnId(1), &group, Some(Instant::now())));
    m.release(TxnId(0)); outcomes.push(m.group(TxnId(1), &group, None)); m.release(TxnId(1));
    assert_eq!(m.waiting(), 0); outcomes
}
#[test]
fn differential_blocking_scripts() {
    let expected = scripted(Arc::new(ReferenceLockManager::new(config(1))));
    for shards in [1, 2, 64] { assert_eq!(scripted(Arc::new(LockManager::new(config(shards)))), expected); }
}
#[test]
fn concurrent_grants_share_one_exact_row_limit() {
    for _ in 0..32 {
        let lm = Arc::new(LockManager::new(LockConfig { max_row_locks_per_txn: 1, ..config(2) }));
        lm.acquire(TxnId(0), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let waiter = spawn(&lm, 1, row(1), LockMode::Exclusive); until(|| lm.waiting_count() == 1);
        let barrier = Arc::new(Barrier::new(2));
        let direct = thread::spawn({ let lm = lm.clone(); let barrier = barrier.clone(); move || {
            barrier.wait(); lm.acquire(TxnId(1), row(0), LockMode::Exclusive, None, &|| false)
        }});
        barrier.wait(); lm.release_all(TxnId(0)); let a = finish(waiter); let b = direct.join().unwrap();
        assert!(matches!((a,b), (Ok(()), Err(LockError::Limit)) | (Err(LockError::Limit), Ok(()))));
        assert_eq!(lm.held(TxnId(1)).len(), 1); lm.release_all(TxnId(1));
    }
}

#[test]
fn lock_order_assertions_reject_reentry_and_leaf_inversion() {
    let lm = LockManager::new(config(2));
    let leaf = lm.registry.stripe(TxnId(1));
    assert!(std::panic::catch_unwind(|| lm.global()).is_err()); drop(leaf);
    let higher = OrderGuard::shard(1);
    assert!(std::panic::catch_unwind(|| OrderGuard::shard(0)).is_err()); drop(higher);
    assert!(lm.global().is_ok());
}
#[test]
fn deferred_fifty_ms_cycle_does_not_wait_for_hour_poll() {
    let lm = Arc::new(LockManager::new(LockConfig { deadlock_recheck: Duration::from_secs(3600), deadlock_timeout: Duration::from_millis(50), ..config(2) }));
    lm.acquire(TxnId(1), row(0), LockMode::Exclusive, None, &|| false).unwrap();
    lm.acquire(TxnId(2), row(1), LockMode::Exclusive, None, &|| false).unwrap();
    let p = spawn(&lm, 1, row(1), LockMode::Exclusive); until(|| lm.waiting_count() == 1);
    let (send, recv) = mpsc::channel();
    let t = thread::spawn({ let lm = lm.clone(); move || {
        let result = lm.acquire(TxnId(2), row(0), LockMode::Exclusive, None, &|| false);
        send.send(result).unwrap(); lm.release_all(TxnId(2));
    }});
    // One of the two must detect the cycle within the generous bound.
    until(|| lm.waiting_count() < 2);
    if let Ok(result) = p.1.recv_timeout(Duration::from_secs(2)) {
        assert!(matches!(result, Ok(()) | Err(LockError::Deadlock)));
        lm.release_all(TxnId(1)); p.0.join().unwrap();
    } else { panic!("deferred detection missed cycle"); }
    assert!(matches!(recv.recv_timeout(Duration::from_secs(2)), Ok(Ok(())) | Ok(Err(LockError::Deadlock)))); t.join().unwrap();
    lm.release_all(TxnId(1)); assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn cycle_closing_grant_before_slot_lock_sets_recheck() {
    let lm = Arc::new(LockManager::new(LockConfig { deadlock_recheck: Duration::from_secs(3600), ..config(2) }));
    let interval = LockKey::Interval { space: LockSpace::Row, start: Bound::Included(point_bytes(1, &[1])), end: Bound::Included(point_bytes(1, &[1])) };
    lm.acquire(TxnId(1), row(9), LockMode::Exclusive, None, &|| false).unwrap();
    lm.acquire(TxnId(3), interval.clone(), LockMode::Shared, None, &|| false).unwrap();
    let first_at = Arc::new(Barrier::new(2)); let first_resume = Arc::new(Barrier::new(2));
    let second_at = Arc::new(Barrier::new(2)); let second_resume = Arc::new(Barrier::new(2));
    let count = AtomicUsize::new(0);
    *lm.hooks.h2.lock().unwrap() = Some(Arc::new({
        let first_at = first_at.clone(); let first_resume = first_resume.clone();
        let second_at = second_at.clone(); let second_resume = second_resume.clone();
        move || { match count.fetch_add(1, Ordering::Relaxed) {
            0 => { first_at.wait(); first_resume.wait(); }
            1 => { second_at.wait(); second_resume.wait(); }
            _ => {}
        } }
    }));
    let p1 = spawn(&lm, 1, row(1), LockMode::Exclusive); first_at.wait();
    let p2 = spawn(&lm, 2, row(9), LockMode::Exclusive); second_at.wait();
    lm.acquire(TxnId(2), interval, LockMode::Shared, None, &|| false).unwrap();
    first_resume.wait(); assert_eq!(finish(p1), Err(LockError::Deadlock));
    lm.release_all(TxnId(1)); second_resume.wait(); assert_eq!(finish(p2), Ok(()));
    lm.release_all(TxnId(2)); lm.release_all(TxnId(3)); assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn forced_cross_shard_group_is_atomic_to_observers() {
    let lm = Arc::new(LockManager::new(config(2)));
    let group = [(LockKey::Unique { index: 1, tuple: vec![1] }, LockMode::Exclusive), (row(1), LockMode::Exclusive)];
    assert_ne!(lm.shard(&group[0].0), lm.shard(&group[1].0));
    lm.acquire(TxnId(0), row(1), LockMode::Shared, None, &|| false).unwrap();
    assert_eq!(lm.acquire_points(TxnId(1), &group, Some(Instant::now()), &|| false), Err(LockError::Timeout));
    assert!(lm.held(TxnId(1)).is_empty()); lm.release_all(TxnId(0));
    let stop = Arc::new(AtomicBool::new(false)); let start = Arc::new(Barrier::new(2));
    let observer = thread::spawn({ let lm = lm.clone(); let stop = stop.clone(); let start = start.clone(); move || {
        start.wait();
        while !stop.load(Ordering::Relaxed) { assert!(matches!(lm.held(TxnId(1)).len(), 0 | 2)); }
    }});
    start.wait();
    for _ in 0..1000 {
        lm.acquire_points(TxnId(1), &group, Some(Instant::now()), &|| false).unwrap(); lm.release_all(TxnId(1));
    }
    stop.store(true, Ordering::Relaxed); observer.join().unwrap();
}
#[test]
fn exhaustive_pair_conflicts_match_reference() {
    let keys = [LockKey::Table(1), row(1), LockKey::RowKey { table: 1, pk: vec![1] }, LockKey::Unique { index: 1, tuple: vec![1] },
        LockKey::Interval { space: LockSpace::Row, start: Bound::Unbounded, end: Bound::Unbounded }];
    let modes = [LockMode::AccessShare, LockMode::RowShare, LockMode::RowExclusive, LockMode::ShareUpdateExclusive,
        LockMode::Share, LockMode::ShareRowExclusive, LockMode::ExclusiveTable, LockMode::AccessExclusive, LockMode::Shared, LockMode::Exclusive];
    for shards in [1, 2, 64] { for forced in [false, true] {
        for a in &keys { for b in &keys { for am in modes { for bm in modes {
            let mut cfg = config(shards); if !forced { cfg.placement = None; }
            let actual = LockManager::new(cfg.clone()); let reference = ReferenceLockManager::new(cfg);
            assert_eq!(actual.acquire(TxnId(1), a.clone(), am, Some(Instant::now()), &|| false), reference.acquire(TxnId(1), a.clone(), am, Some(Instant::now()), &|| false));
            assert_eq!(actual.acquire(TxnId(2), b.clone(), bm, Some(Instant::now()), &|| false), reference.acquire(TxnId(2), b.clone(), bm, Some(Instant::now()), &|| false));
        } } } }
    } }
}

#[test]
fn expired_deadlock_timeout_stops_bounding_later_sleeps() {
    let cfg = LockConfig { deadlock_timeout: Duration::from_millis(50), ..config(2) };
    let queued = Instant::now();
    let before = Duration::from_millis(25);
    assert_eq!(cfg.sleep_interval(queued, queued + before, before, None), before);
    let expired = Duration::from_millis(51);
    let now = queued + expired;
    assert_eq!(cfg.sleep_interval(queued, now, expired, None), Duration::from_secs(3600));
    assert_eq!(cfg.sleep_interval(queued, now, expired, Some(now + Duration::from_millis(10))), Duration::from_millis(10));
}

#[test]
fn restoring_mixed_holdings_preserves_row_limit() {
    for shards in [1, 2, 64] {
        let cfg = LockConfig { max_row_locks_per_txn: 1, ..config(shards) };
        let actual = LockManager::new(cfg.clone());
        let reference = ReferenceLockManager::new(cfg);
        let txn = TxnId(1);
        let keys = [(row(1), LockMode::Exclusive), (LockKey::Table(1), LockMode::AccessShare)];
        let a = actual.modes_on(txn, &keys);
        let b = reference.modes_on(txn, &keys);
        for (key, mode) in &keys {
            actual.acquire(txn, key.clone(), *mode, None, &|| false).unwrap();
            reference.acquire(txn, key.clone(), *mode, None, &|| false).unwrap();
        }
        actual.restore_modes(txn, &a); reference.restore_modes(txn, &b);
        assert!(actual.held(txn).is_empty()); assert!(reference.held(txn).is_empty());
        assert_eq!(actual.acquire(txn, row(2), LockMode::Exclusive, None, &|| false), Ok(()));
        assert_eq!(reference.acquire(txn, row(2), LockMode::Exclusive, None, &|| false), Ok(()));
        actual.release_all(txn); reference.release_all(txn);
    }
}

#[test]
fn terminal_notifications_before_cancel_callback_are_retained() {
    for shards in [1, 2, 64] { for action in 0..3 {
        let lm = Arc::new(LockManager::new(config(shards)));
        lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let reached = Arc::new(Barrier::new(2)); let resume = Arc::new(Barrier::new(2));
        let (at, arrival) = mpsc::channel();
        *lm.hooks.h3.lock().unwrap() = Some(Arc::new({
            let reached = reached.clone(); let resume = resume.clone();
            move || { at.send(()).unwrap(); reached.wait(); resume.wait(); }
        }));
        let (send, recv) = mpsc::channel();
        let worker = thread::spawn({ let lm = lm.clone(); move || {
            send.send(lm.acquire(TxnId(2), row(1), LockMode::Exclusive, None, &|| false)).unwrap();
        }});
        arrival.recv_timeout(Duration::from_secs(10)).expect("waiter did not reach h3");
        reached.wait();
        let expected = match action {
            0 => { lm.release_all(TxnId(1)); Ok(()) }
            1 => { lm.release_all(TxnId(2)); Err(LockError::Cancelled) }
            _ => { lm.cancel_wait(TxnId(2)); Err(LockError::Deadlock) }
        };
        resume.wait();
        assert_eq!(recv.recv_timeout(Duration::from_secs(10)), Ok(expected));
        worker.join().unwrap();
        lm.release_all(TxnId(1)); lm.release_all(TxnId(2));
        assert_eq!(lm.waiting_count(), 0);
        assert!(lm.registry.stripes.iter().all(|s| s.lock().unwrap().is_empty()));
    } }
}

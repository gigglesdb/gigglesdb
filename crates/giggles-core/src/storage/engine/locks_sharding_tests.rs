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
fn phase3_config(shards: usize) -> LockConfig {
    LockConfig { per_shard_release: true, ..config(shards) }
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
            for gaps in [false, true] {
            let universe = if gaps { &universe[..] } else { &universe[..universe.len()-2] };
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
    for shards in [2, 64] {
    let lm = Arc::new(LockManager::new(config(shards)));
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
            let end = Instant::now() + Duration::from_secs(5);
            while Instant::now() < end {
                let key = row((random(&mut rng) % 8) as u8);
                let deadline = if rng & 1 == 0 { Some(Instant::now()) } else { None };
                let result = if rng & 2 == 0 {
                    let LockKey::Row { table, pk } = key.clone() else { unreachable!() };
                    lm.acquire_points(TxnId(t), &[(LockKey::RowKey { table, pk }, LockMode::Exclusive), (key, LockMode::Exclusive)], deadline, &|| false)
                } else { lm.acquire(TxnId(t), row((random(&mut rng) % 8) as u8),
                    LockMode::Exclusive, deadline, &|| false) };
                assert!(matches!(result, Ok(()) | Err(LockError::Deadlock | LockError::Timeout)));
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
    fn group_cancel(&self, txn: TxnId, keys: &[(LockKey, LockMode)], cancel: &dyn Fn() -> bool) -> Result<(), LockError>;
    fn snapshot(&self, txn: TxnId) -> HashSet<(LockKey, LockMode)>;
    fn release(&self, txn: TxnId);
    fn waiting(&self) -> usize;
    fn victim(&self, txn: TxnId);
    fn modes(&self, txn: TxnId, keys: &[(LockKey, LockMode)]) -> Vec<(LockKey, HeldModes)>;
    fn restore(&self, txn: TxnId, undo: &[(LockKey, HeldModes)]);
}
macro_rules! script_manager {
    ($t:ty) => { impl ScriptManager for $t {
        fn take(&self, txn: TxnId, key: LockKey, mode: LockMode, deadline: Option<Instant>, cancel: &dyn Fn() -> bool) -> Result<(), LockError> { self.acquire(txn, key, mode, deadline, cancel) }
        fn group(&self, txn: TxnId, keys: &[(LockKey, LockMode)], deadline: Option<Instant>) -> Result<(), LockError> { self.acquire_points(txn, keys, deadline, &|| false) }
        fn group_cancel(&self, txn: TxnId, keys: &[(LockKey, LockMode)], cancel: &dyn Fn() -> bool) -> Result<(), LockError> { self.acquire_points(txn, keys, None, cancel) }
        fn snapshot(&self, txn: TxnId) -> HashSet<(LockKey, LockMode)> { self.held(txn).into_iter().collect() }
        fn release(&self, txn: TxnId) { self.release_all(txn); }
        fn waiting(&self) -> usize { self.waiting_count() }
        fn victim(&self, txn: TxnId) { self.cancel_wait(txn); }
        fn modes(&self, txn: TxnId, keys: &[(LockKey, LockMode)]) -> Vec<(LockKey, HeldModes)> { self.modes_on(txn, keys) }
        fn restore(&self, txn: TxnId, undo: &[(LockKey, HeldModes)]) { self.restore_modes(txn, undo); }
    } };
}
script_manager!(LockManager);
script_manager!(ReferenceLockManager);
type Pending<T = ()> = (thread::JoinHandle<()>, mpsc::Receiver<Result<T, LockError>>);
fn spawn<M: ScriptManager>(m: &Arc<M>, txn: u64, key: LockKey, mode: LockMode) -> Pending {
    let (send, recv) = mpsc::channel(); let m = m.clone();
    (thread::spawn(move || { send.send(m.take(TxnId(txn), key, mode, Some(Instant::now() + Duration::from_secs(10)), &|| false)).unwrap(); }), recv)
}
fn finish<T>(p: Pending<T>) -> Result<T, LockError> {
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
    // Release one owner's rows on three independent queues. Keep each
    // grantee live so we can compare the resulting grants with the reference.
    let mut pending = Vec::new();
    for i in 1..=3 {
        take(0, row(i), LockMode::Exclusive);
        pending.push(spawn(&m, u64::from(i), row(i), LockMode::Shared));
        until(|| m.waiting() == usize::from(i));
    }
    m.release(TxnId(0));
    m.release(TxnId(0)); // A second release must preserve every grantee.
    assert!(m.snapshot(TxnId(0)).is_empty());
    for (i, waiter) in (1..=3).zip(pending) {
        outcomes.push(finish(waiter));
        assert_eq!(m.snapshot(TxnId(u64::from(i))), HashSet::from([(row(i), LockMode::Shared)]));
        m.release(TxnId(u64::from(i)));
    }
    assert_eq!(m.waiting(), 0); outcomes
}
#[test]
fn differential_blocking_scripts() {
    let expected = scripted(Arc::new(ReferenceLockManager::new(config(1))));
    for shards in [1, 2, 64] {
        for per_shard_release in [false, true] {
            let cfg = LockConfig { per_shard_release, ..config(shards) };
            assert_eq!(scripted(Arc::new(LockManager::new(cfg))), expected);
        }
    }
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

fn park_passes(lm: &LockManager, count: usize) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (send, at) = mpsc::channel(); let (go, resume) = mpsc::channel();
    let resume = Mutex::new(resume); let calls = AtomicUsize::new(0);
    *lm.hooks.h2.lock().unwrap() = Some(Arc::new(move || {
        if calls.fetch_add(1, Ordering::Relaxed) < count {
            send.send(()).unwrap();
            resume.lock().unwrap().recv_timeout(Duration::from_secs(15)).unwrap();
        }
    }));
    (at, go)
}
fn spawn_without_timer(m: &Arc<LockManager>, txn: u64, key: LockKey, mode: LockMode) -> Pending {
    let (send, recv) = mpsc::channel(); let m = m.clone();
    (thread::spawn(move || { send.send(m.acquire(TxnId(txn), key, mode, None, &|| false)).unwrap(); }), recv)
}

#[test]
fn phase2_f1_pending_grantee_closes_cycle() {
    for (shards, drain, per_shard_release) in [
        (64, false, false), (64, true, false), (1, true, true), (2, true, true), (64, true, true),
    ] {
        let lm = Arc::new(LockManager::new(LockConfig { per_shard_release, ..config(shards) }));
        let t = LockKey::Table(0);
        lm.acquire(TxnId(3), t.clone(), LockMode::RowExclusive, None, &|| false).unwrap();
        if drain { lm.acquire(TxnId(3), t.clone(), LockMode::Share, None, &|| false).unwrap(); }
        lm.acquire(TxnId(2), t.clone(), LockMode::RowShare, None, &|| false).unwrap();
        lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let (at, go) = park_passes(&lm, 2);
        let a = spawn_without_timer(&lm, 1, t.clone(), LockMode::Share);
        at.recv_timeout(Duration::from_secs(5)).unwrap();
        let b = spawn_without_timer(&lm, 2, row(1), LockMode::Exclusive);
        at.recv_timeout(Duration::from_secs(5)).unwrap();
        if drain {
            // Isolate the release-time notification obligation with a synthetic
            // queued upgrade. Normal enqueue would already detect the cycle
            // from this upgrade's FIFO edge, before its eventual grant.
            let slot = Arc::new(Slot::default());
            let key = lm.prepare(t.clone());
            let index = lm.hashed_shard(&key);
            {
                let mut state = lm.global().unwrap();
                let id = lm.next_waiter.fetch_add(1, Ordering::Relaxed);
                lm.registry.pending(TxnId(2), true);
                state.guards.slice_mut()[index].waiters.insert(id, Waiter {
                    txn: TxnId(2), key: key.clone(), mode: LockMode::RowExclusive,
                    upgrade: true, group: vec![(key.clone(), LockMode::RowExclusive)],
                    cancelled: false, slot: slot.clone(), prepared: PreparedKey::new(&key.clone()),
                });
                state.guards.slice_mut()[index].by_txn.entry(TxnId(2)).or_default().push(id);
                state.key_mut(&key.clone()).unwrap().queue.insert(0, id);
                state.assert_registry_covers(TxnId(2));
            }
            let before = SHARD_LOCKS.with(|count| count.get());
            lm.release_all(TxnId(3));
            assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, shards + usize::from(per_shard_release));
            assert_eq!(slot.result(), Some(Ok(())));
        } else {
            lm.acquire(TxnId(2), t, LockMode::RowExclusive, None, &|| false).unwrap();
        }
        go.send(()).unwrap(); go.send(()).unwrap();
        let mut ar = None; let mut br = None;
        until(|| {
            if ar.is_none() { ar = a.1.try_recv().ok(); }
            if br.is_none() { br = b.1.try_recv().ok(); }
            ar == Some(Err(LockError::Deadlock)) || br == Some(Err(LockError::Deadlock))
        });
        if ar == Some(Err(LockError::Deadlock)) { lm.release_all(TxnId(1)); }
        if br == Some(Err(LockError::Deadlock)) { lm.release_all(TxnId(2)); lm.release_all(TxnId(3)); }
        if ar.is_none() { ar = Some(a.1.recv_timeout(Duration::from_secs(5)).unwrap()); }
        if br.is_none() { br = Some(b.1.recv_timeout(Duration::from_secs(5)).unwrap()); }
        assert!(matches!((ar.unwrap(), br.unwrap()),
            (Ok(()), Err(LockError::Deadlock)) | (Err(LockError::Deadlock), Ok(()))));
        a.0.join().unwrap(); b.0.join().unwrap();
        lm.release_all(TxnId(1)); lm.release_all(TxnId(2)); lm.release_all(TxnId(3));
    }
}

#[test]
fn phase2_f4_release_drains_cross_shard_group_before_bypass() {
    fn scenario<M: ScriptManager>(m: Arc<M>) -> Result<(), LockError> {
        let k = LockKey::RowKey { table: 1, pk: vec![1] };
        let u = LockKey::Unique { index: 1, tuple: vec![1] };
        m.take(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        m.take(TxnId(2), k, LockMode::Shared, None, &|| false).unwrap();
        let (send, recv) = mpsc::channel();
        let (at, arrived) = mpsc::channel(); let (go, resume) = mpsc::channel();
        let q = thread::spawn({ let m = m.clone(); move || {
            let once = AtomicBool::new(false);
            send.send(m.group_cancel(TxnId(3), &[(row(1), LockMode::Shared), (u, LockMode::Shared)], &|| {
                if !once.swap(true, Ordering::Relaxed) {
                    at.send(()).unwrap(); resume.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                false
            })).unwrap();
        }});
        arrived.recv_timeout(Duration::from_secs(5)).unwrap();
        m.release(TxnId(1));
        // The release must offer D to Q before any newcomer can see it. Q's
        // thread is paused, so its cross-shard group is still provisional.
        assert!(m.snapshot(TxnId(3)).contains(&(row(1), LockMode::Shared)));
        let bypass = m.take(TxnId(2), row(1), LockMode::Exclusive, Some(Instant::now()), &|| false);
        // The reference permits the K holder to bypass a provisional D grant;
        // finalization, rather than scheduling, makes that grant conflict.
        m.release(TxnId(2)); go.send(()).unwrap();
        assert_eq!(recv.recv_timeout(Duration::from_secs(5)), Ok(Ok(()))); q.join().unwrap();
        assert_eq!(m.take(TxnId(4), row(1), LockMode::Exclusive, Some(Instant::now()), &|| false), Err(LockError::Timeout));
        m.release(TxnId(3)); bypass
    }
    let reference = scenario(Arc::new(ReferenceLockManager::new(config(64))));
    eprintln!("F4 reference provisional K-holder bypass: {reference:?}");
    assert_eq!(scenario(Arc::new(LockManager::new(config(64)))), reference);
}

#[test]
fn phase2_queue_without_conflicting_holder_cannot_be_overtaken() {
    let lm = Arc::new(LockManager::new(config(64)));
    lm.acquire(TxnId(1), row(1), LockMode::Shared, None, &|| false).unwrap();
    let writer = spawn(&lm, 2, row(1), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    assert_eq!(lm.acquire(TxnId(3), row(1), LockMode::Shared, Some(Instant::now()), &|| false), Err(LockError::Timeout));
    assert!(lm.held(TxnId(3)).is_empty());
    lm.release_all(TxnId(1)); assert_eq!(finish(writer), Ok(())); lm.release_all(TxnId(2));
}

#[test]
fn phase2_gap_switch_while_point_shard_is_locked() {
    let lm = Arc::new(LockManager::new(config(64)));
    let (send, at) = mpsc::channel();
    let (go, resume) = mpsc::channel(); let resume = Mutex::new(resume);
    *lm.hooks.s2_false.lock().unwrap() = Some(Arc::new(move || {
        send.send(()).unwrap(); resume.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    let point = spawn(&lm, 1, row(1), LockMode::Exclusive);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    let interval = spawn(&lm, 2, LockKey::Interval { space: LockSpace::Row, start: Bound::Unbounded, end: Bound::Unbounded }, LockMode::Shared);
    until(|| lm.gap_mode[0].load(Ordering::SeqCst));
    *lm.hooks.s2_false.lock().unwrap() = None;
    go.send(()).unwrap();
    until(|| lm.waiting_count() == 1);
    let point_held = !lm.held(TxnId(1)).is_empty();
    let interval_held = !lm.held(TxnId(2)).is_empty();
    assert!(point_held && !interval_held, "interval must see the point committed after the S2 read");
    lm.release_all(if point_held { TxnId(1) } else { TxnId(2) });
    assert_eq!(finish(point), Ok(())); assert_eq!(finish(interval), Ok(()));
    lm.release_all(TxnId(1));
    lm.acquire(TxnId(2), LockKey::Interval { space: LockSpace::Row, start: Bound::Unbounded, end: Bound::Unbounded }, LockMode::Shared, None, &|| false).unwrap();
    assert_eq!(lm.acquire(TxnId(3), row(2), LockMode::Exclusive, Some(Instant::now()), &|| false), Err(LockError::Timeout));
    lm.release_all(TxnId(2));
}

#[test]
fn phase2_release_between_shard_drop_and_global_entry() {
    let lm = Arc::new(LockManager::new(config(64)));
    lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
    let (send, at) = mpsc::channel(); let (go, resume) = mpsc::channel(); let resume = Mutex::new(resume);
    *lm.hooks.h1.lock().unwrap() = Some(Arc::new(move || {
        send.send(()).unwrap(); resume.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    let waiter = spawn(&lm, 2, row(1), LockMode::Exclusive);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    lm.release_all(TxnId(1)); // Slow path must retry the immediate predicate.
    go.send(()).unwrap(); assert_eq!(finish(waiter), Ok(()));
    assert_eq!(lm.waiting_count(), 0); lm.release_all(TxnId(2));
}

#[test]
fn phase2_release_all_covers_all_shards_and_cancels_pending() {
    let lm = Arc::new(LockManager::new(config(64)));
    let keys = [(LockKey::Table(0), LockMode::AccessExclusive), (row(1), LockMode::Exclusive),
        (LockKey::Unique { index: 1, tuple: vec![1] }, LockMode::Exclusive)];
    let mut pending = Vec::new();
    for (i, (key, mode)) in keys.iter().enumerate() {
        lm.acquire(TxnId(1), key.clone(), *mode, None, &|| false).unwrap();
        pending.push(spawn(&lm, i as u64 + 3, key.clone(), *mode));
        until(|| lm.waiting_count() == i+1);
    }
    lm.acquire(TxnId(2), row(3), LockMode::Exclusive, None, &|| false).unwrap();
    let own = spawn(&lm, 1, row(3), LockMode::Exclusive); until(|| lm.waiting_count() == 4);
    lm.release_all(TxnId(1)); assert_eq!(finish(own), Err(LockError::Cancelled));
    for p in pending { assert_eq!(finish(p), Ok(())); }
    assert!(lm.held(TxnId(1)).is_empty());
    for txn in 2..6 { lm.release_all(TxnId(txn)); }
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn phase2_group_reservation_is_exact_across_shards() {
    for _ in 0..50 {
        let lm = Arc::new(LockManager::new(LockConfig { max_row_locks_per_txn: 3, ..config(64) }));
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for pk in [1,2] {
            workers.push(thread::spawn({ let lm = lm.clone(); let barrier = barrier.clone(); move || {
                barrier.wait(); lm.acquire_points(TxnId(1), &[(LockKey::RowKey { table: 1, pk: vec![pk] }, LockMode::Exclusive), (row(pk), LockMode::Exclusive)], Some(Instant::now()), &|| false)
            }}));
        }
        barrier.wait();
        let results = workers.into_iter().map(|w| w.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|r| **r == Err(LockError::Limit)).count(), 1);
        assert_eq!(lm.held(TxnId(1)).len(), 2); lm.release_all(TxnId(1));
    }
}

#[test]
fn phase2_immediate_cycles_span_two_and_three_shards() {
    for length in [2, 3] {
        let lm = Arc::new(LockManager::new(config(64)));
        for txn in 0..length { lm.acquire(TxnId(txn), row(txn as u8), LockMode::Exclusive, None, &|| false).unwrap(); }
        let mut pending = Vec::new();
        for txn in 0..length-1 {
            pending.push(spawn(&lm, txn, row((txn+1) as u8), LockMode::Exclusive));
            until(|| lm.waiting_count() == txn as usize+1);
        }
        let closer = spawn(&lm, length-1, row(0), LockMode::Exclusive);
        assert_eq!(finish(closer), Err(LockError::Deadlock));
        lm.release_all(TxnId(length-1));
        for (txn, p) in pending.into_iter().enumerate().rev() { assert_eq!(finish(p), Ok(())); lm.release_all(TxnId(txn as u64)); }
    }
}

#[test]
fn phase2_table_arbitrates_writers_with_rows_on_other_shards() {
    fn run<M: ScriptManager>(m: Arc<M>) -> Vec<Result<(), LockError>> {
        let t = LockKey::Table(0);
        m.take(TxnId(1), t.clone(), LockMode::RowExclusive, None, &|| false).unwrap();
        m.take(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let mut out = vec![m.take(TxnId(2), t.clone(), LockMode::AccessExclusive, Some(Instant::now()), &|| false)];
        let exclusive = spawn(&m, 2, t.clone(), LockMode::AccessExclusive); until(|| m.waiting() == 1);
        // Compatible with the holder, but must respect the earlier AX waiter.
        out.push(m.take(TxnId(3), t.clone(), LockMode::RowExclusive, Some(Instant::now()), &|| false));
        let writer = spawn(&m, 3, t, LockMode::RowExclusive); until(|| m.waiting() == 2);
        m.release(TxnId(1)); out.push(finish(exclusive));
        assert!(writer.1.try_recv().is_err()); m.release(TxnId(2)); out.push(finish(writer));
        m.release(TxnId(3)); out
    }
    assert_eq!(run(Arc::new(LockManager::new(config(64)))), run(Arc::new(ReferenceLockManager::new(config(1)))));
}

#[test]
fn phase2_immediate_group_cycle_between_k_and_d_holders() {
    let lm = Arc::new(LockManager::new(config(64)));
    let k = LockKey::RowKey { table: 1, pk: vec![1] };
    lm.acquire(TxnId(1), k.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    lm.acquire(TxnId(2), row(1), LockMode::Exclusive, None, &|| false).unwrap();
    let (send, recv) = mpsc::channel();
    let group = thread::spawn({ let lm = lm.clone(); let k = k.clone(); move || {
        send.send(lm.acquire_points(TxnId(1), &[(k, LockMode::Exclusive), (row(1), LockMode::Exclusive)], None, &|| false)).unwrap();
    }});
    until(|| lm.waiting_count() == 1);
    assert_eq!(finish(spawn(&lm, 2, k, LockMode::Exclusive)), Err(LockError::Deadlock));
    lm.release_all(TxnId(2));
    assert_eq!(recv.recv_timeout(Duration::from_secs(5)), Ok(Ok(()))); group.join().unwrap(); lm.release_all(TxnId(1));
}

#[test]
fn phase2_fast_operations_do_not_lock_an_unrelated_shard() {
    let lm = Arc::new(LockManager::new(config(64)));
    let held = lm.shards[63].lock().unwrap();
    let (send, recv) = mpsc::channel();
    let worker = thread::spawn({ let lm = lm.clone(); move || {
        let txn = TxnId(1);
        lm.acquire(txn, row(1), LockMode::Exclusive, None, &|| false).unwrap();
        lm.acquire(txn, row(1), LockMode::Shared, Some(Instant::now()), &|| false).unwrap();
        assert_eq!(lm.acquire(TxnId(2), row(1), LockMode::Exclusive, Some(Instant::now()), &|| false), Err(LockError::Timeout));
        lm.acquire_points(txn, &[(LockKey::RowKey { table: 1, pk: vec![2] }, LockMode::Shared), (row(2), LockMode::Shared)], None, &|| false).unwrap();
        lm.acquire(txn, LockKey::Table(0), LockMode::RowExclusive, None, &|| false).unwrap();
        assert!(lm.fastpath(txn, 0, LockMode::RowExclusive));
        assert!(!lm.modes_on(txn, &[(row(1), LockMode::Shared)])[0].1.is_subset(HeldModes::default()));
        send.send(()).unwrap();
    }});
    let result = recv.recv_timeout(Duration::from_secs(5));
    drop(held); assert_eq!(result, Ok(())); worker.join().unwrap(); lm.release_all(TxnId(1));
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn phase2_prehashed_key_collisions_compare_complete_payloads() {
    let mut keys = KeyMap::default();
    keys.insert(HashedKey { hash: 7, key: row(1) }, 1);
    keys.insert(HashedKey { hash: 7, key: row(2) }, 2);
    assert_eq!(keys.len(), 2);
    assert_eq!(keys.get(&HashedKey { hash: 7, key: row(1) }), Some(&1));
    assert_eq!(keys.get(&HashedKey { hash: 7, key: row(2) }), Some(&2));
}

#[test]
fn phase2_cancel_and_grant_while_another_request_escalates() {
    let lm = Arc::new(LockManager::new(config(64)));
    lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
    lm.acquire(TxnId(3), row(2), LockMode::Exclusive, None, &|| false).unwrap();
    let old = spawn(&lm, 2, row(2), LockMode::Exclusive); until(|| lm.waiting_count() == 1);
    let (send, at) = mpsc::channel(); let (go, resume) = mpsc::channel(); let resume = Mutex::new(resume);
    *lm.hooks.h1.lock().unwrap() = Some(Arc::new(move || {
        send.send(()).unwrap(); resume.lock().unwrap().recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    let new = spawn(&lm, 2, row(1), LockMode::Exclusive);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    lm.cancel_wait(TxnId(2)); assert_eq!(finish(old), Err(LockError::Deadlock));
    lm.release_all(TxnId(1));
    lm.acquire(TxnId(4), row(1), LockMode::Shared, Some(Instant::now()), &|| false).unwrap();
    go.send(()).unwrap(); until(|| lm.waiting_count() == 1);
    lm.release_all(TxnId(4)); assert_eq!(finish(new), Ok(()));
    lm.release_all(TxnId(2)); lm.release_all(TxnId(3));
}

#[test]
fn phase2_s3_escalates_before_a_covered_hit_touches_an_unsafe_queue() {
    for cross_group in [false, true] {
        let lm = Arc::new(LockManager::new(config(64)));
        lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let (send, recv) = mpsc::channel();
        let queued = thread::spawn({ let lm = lm.clone(); move || {
            let mut keys = vec![(row(1), LockMode::Shared)];
            if cross_group { keys.push((LockKey::Unique { index: 1, tuple: vec![1] }, LockMode::Shared)); }
            send.send(lm.acquire_points(TxnId(2), &keys, None, &|| false)).unwrap();
        }});
        until(|| lm.waiting_count() == 1);
        let other = if !cross_group {
            lm.acquire(TxnId(3), row(3), LockMode::Exclusive, None, &|| false).unwrap();
            let pending = spawn(&lm, 2, row(3), LockMode::Shared);
            until(|| lm.waiting_count() == 2); Some(pending)
        } else { None };
        let escalations = Arc::new(AtomicUsize::new(0));
        *lm.hooks.h1.lock().unwrap() = Some(Arc::new({ let escalations = escalations.clone(); move || { escalations.fetch_add(1, Ordering::Relaxed); } }));
        lm.acquire(TxnId(1), row(1), LockMode::Shared, None, &|| false).unwrap();
        assert_eq!(escalations.load(Ordering::Relaxed), 1, "unsafe drain must escalate before changing the shard");
        *lm.hooks.h1.lock().unwrap() = None;
        lm.release_all(TxnId(1));
        assert_eq!(recv.recv_timeout(Duration::from_secs(5)), Ok(Ok(()))); queued.join().unwrap();
        if let Some(other) = other { lm.release_all(TxnId(3)); assert_eq!(finish(other), Ok(())); }
        lm.release_all(TxnId(2));
    }
}

#[test]
fn phase2_shard_drain_cancelled_head_and_pending_follower() {
    for other_pending in [false, true] {
        let lm = Arc::new(LockManager::new(config(64)));
        let t = LockKey::Table(0);
        lm.acquire(TxnId(1), t.clone(), LockMode::RowShare, None, &|| false).unwrap();
        let (at, go) = park_passes(&lm, if other_pending { 3 } else { 2 });
        let c = spawn_without_timer(&lm, 2, t.clone(), LockMode::AccessExclusive);
        at.recv_timeout(Duration::from_secs(5)).unwrap();
        let other = if other_pending {
            lm.acquire(TxnId(4), row(3), LockMode::Exclusive, None, &|| false).unwrap();
            let p = spawn_without_timer(&lm, 3, row(3), LockMode::Exclusive);
            at.recv_timeout(Duration::from_secs(5)).unwrap(); Some(p)
        } else { None };
        let f = spawn_without_timer(&lm, 3, t.clone(), LockMode::RowShare);
        at.recv_timeout(Duration::from_secs(5)).unwrap();
        let slots = {
            let state = lm.global().unwrap();
            [2, 3].into_iter().map(|txn| state.pending(TxnId(txn)).into_iter()
                .map(|id| { let w = state.waiter(&id).unwrap(); (w.key.key.clone(), w.slot.clone()) }).collect::<Vec<_>>()).collect::<Vec<_>>()
        };
        let escalations = Arc::new(AtomicUsize::new(0));
        *lm.hooks.h1.lock().unwrap() = Some(Arc::new({ let escalations = escalations.clone(); move || { escalations.fetch_add(1, Ordering::Relaxed); } }));
        lm.cancel_wait(TxnId(2));
        lm.acquire(TxnId(1), t.clone(), LockMode::RowExclusive, None, &|| false).unwrap();
        assert_eq!(escalations.load(Ordering::Relaxed), usize::from(other_pending));
        assert_eq!(slots[0][0].1.result(), Some(Err(LockError::Deadlock)));
        for (key, slot) in &slots[1] {
            if *key == t { assert_eq!(slot.result(), Some(Ok(()))); }
            else { assert!(slot.m.lock().unwrap().recheck, "other shard was not notified"); }
        }
        for _ in 0..if other_pending { 3 } else { 2 } { go.send(()).unwrap(); }
        assert_eq!(finish(c), Err(LockError::Deadlock)); assert_eq!(finish(f), Ok(()));
        if let Some(other) = other { lm.release_all(TxnId(4)); assert_eq!(finish(other), Ok(())); }
        for txn in 1..=4 { lm.release_all(TxnId(txn)); }
        assert_eq!(lm.waiting_count(), 0);
    }
}

#[test]
fn phase2_nowait_limit_leaves_no_reservation_or_provisional() {
    for shards in [1, 2, 64] {
        let lm = LockManager::new(LockConfig { max_row_locks_per_txn: 2, ..config(shards) });
        lm.acquire(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let group = [(row(2), LockMode::Exclusive), (row(3), LockMode::Exclusive)];
        assert_eq!(lm.acquire_points(TxnId(1), &group, Some(Instant::now()), &|| false), Err(LockError::Limit));
        assert_eq!(lm.waiting_count(), 0);
        assert_eq!(lm.held(TxnId(1)), vec![(row(1), LockMode::Exclusive)]);
        assert_eq!(lm.registry.stripe(TxnId(1))[&TxnId(1)].rows, 1);
        assert_eq!(lm.registry.stripe(TxnId(1))[&TxnId(1)].pending, 0);
        assert!(lm.shards.iter().all(|s| s.lock().unwrap().provisional.is_empty()));
        lm.acquire_points(TxnId(1), &group[..1], Some(Instant::now()), &|| false).unwrap();
        assert_eq!(lm.registry.stripe(TxnId(1))[&TxnId(1)].rows, 2);
        lm.release_all(TxnId(1));
    }
}

#[test]
fn phase2_payload_hash_once_outside_shards() {
    for shards in [1, 2, 64] {
        let lm = LockManager::new(config(shards));
        for key in [row(1), LockKey::RowKey { table: 1, pk: vec![2] }, LockKey::Unique { index: 1, tuple: vec![3] }, LockKey::Table(0)] {
            let mode = if matches!(key, LockKey::Table(_)) { LockMode::RowExclusive } else { LockMode::Exclusive };
            PAYLOAD_HASHES.with(|count| count.set(0));
            lm.acquire(TxnId(1), key, mode, None, &|| false).unwrap();
            assert_eq!(PAYLOAD_HASHES.with(|count| count.get()), 1);
        }
        lm.release_all(TxnId(1));
    }
}

#[test]
fn phase2_restore_downgrade_drains_overlapping_interval_and_points() {
    fn scenario<M: ScriptManager>(m: Arc<M>) -> Vec<Result<(), LockError>> {
        let interval = LockKey::Interval { space: LockSpace::Row, start: Bound::Included(point_bytes(1, &[1])), end: Bound::Included(point_bytes(1, &[1])) };
        m.take(TxnId(1), row(1), LockMode::Shared, None, &|| false).unwrap();
        let undo = m.modes(TxnId(1), &[(row(1), LockMode::Shared)]);
        m.take(TxnId(1), row(1), LockMode::Exclusive, None, &|| false).unwrap();
        let gap = spawn(&m, 2, interval.clone(), LockMode::Shared); until(|| m.waiting() == 1);
        let reader = spawn(&m, 3, row(1), LockMode::Shared); until(|| m.waiting() == 2);
        let writer = spawn(&m, 4, row(1), LockMode::Exclusive); until(|| m.waiting() == 3);
        m.restore(TxnId(1), &undo);
        let mut results = vec![finish(gap), finish(reader)];
        assert!(m.snapshot(TxnId(2)).contains(&(interval, LockMode::Shared)));
        assert!(m.snapshot(TxnId(3)).contains(&(row(1), LockMode::Shared)));
        assert!(writer.1.try_recv().is_err());
        m.release(TxnId(1)); m.release(TxnId(3));
        assert!(writer.1.try_recv().is_err(), "interval must still block the writer");
        m.release(TxnId(2)); results.push(finish(writer)); m.release(TxnId(4)); results
    }
    let expected = scenario(Arc::new(ReferenceLockManager::new(config(1))));
    assert_eq!(expected, vec![Ok(()); 3]);
    for shards in [1, 2, 64] { assert_eq!(scenario(Arc::new(LockManager::new(config(shards)))), expected); }
}

fn pause_release(
    lm: &LockManager, before_shard: usize, also_global: bool,
) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (parked, at) = mpsc::channel();
    let (go, resume) = mpsc::channel();
    let pause = Mutex::new(Some((parked, resume)));
    let hook: RaceHook = Arc::new(move || {
        if let Some((parked, resume)) = pause.lock().unwrap().take() {
            parked.send(()).unwrap();
            resume.recv_timeout(Duration::from_secs(10)).unwrap();
        }
    });
    if also_global {
        *lm.hooks.release_global.lock().unwrap() = Some(hook.clone());
    }
    let visits = AtomicUsize::new(0);
    *lm.hooks.h4.lock().unwrap() = Some(Arc::new(move || {
        if visits.fetch_add(1, Ordering::Relaxed) == before_shard {
            hook();
        }
    }));
    (at, go)
}

fn spawn_release(lm: &Arc<LockManager>, txn: TxnId) -> thread::JoinHandle<usize> {
    let lm = lm.clone();
    thread::spawn(move || {
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(txn);
        SHARD_LOCKS.with(|count| count.get()) - before
    })
}

fn test_gap_switch_during_release(shards: usize, first_point: bool) {
    let lm = Arc::new(LockManager::new(phase3_config(shards)));
    let holder = TxnId(1);
    lm.acquire(holder, row(1), LockMode::Exclusive, None, &|| false).unwrap();
    let local_waiter = (shards > 1).then(|| {
        let (key, held, requested) = if first_point {
            (row(0), LockMode::Exclusive, LockMode::Shared)
        } else {
            (LockKey::Table(0), LockMode::AccessExclusive, LockMode::AccessShare)
        };
        lm.acquire(holder, key.clone(), held, None, &|| false).unwrap();
        let waiter = spawn_without_timer(&lm, 2, key, requested);
        until(|| lm.waiting_count() == 1);
        waiter
    });
    if shards == 64 {
        lm.acquire(holder, LockKey::Table(2), LockMode::AccessExclusive, None, &|| false).unwrap();
    }

    let point_shard = usize::from(shards > 1);
    let (at, go) = pause_release(&lm, point_shard, false);
    let releaser = spawn_release(&lm, holder);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    if let Some(waiter) = local_waiter {
        assert_eq!(finish(waiter), Ok(()));
        lm.global().unwrap().assert_registry_covers(TxnId(2));
    }
    let interval = LockKey::Interval {
        space: LockSpace::Row,
        start: Bound::Included(point_bytes(1, &[0])),
        end: Bound::Included(point_bytes(1, &[1])),
    };
    let interval_waiter = spawn_without_timer(&lm, 3, interval, LockMode::Shared);
    until(|| lm.blockers(TxnId(3)) == vec![holder]);
    go.send(()).unwrap();
    assert_eq!(releaser.join().unwrap(), point_shard + 1 + shards);
    assert_eq!(finish(interval_waiter), Ok(()));
    assert!(lm.held(holder).is_empty());
    assert_eq!(lm.waiting_count(), 0);
    lm.release_all(TxnId(2));
    lm.release_all(TxnId(3));
}

#[test]
fn phase3_f3_gap_switch_during_release() {
    for shards in [1, 2, 64] {
        test_gap_switch_during_release(shards, false);
    }
}

#[test]
fn phase3_f3_gap_switch_after_a_point_drain() {
    for shards in [2, 64] {
        test_gap_switch_during_release(shards, true);
    }
}

fn test_release_drains_row_key_twin(cfg: LockConfig, escalate_first: bool, expected_locks: usize) {
    let lm = Arc::new(LockManager::new(cfg));
    if escalate_first {
        // The Unique point is on shard zero. Indexing that space forces it
        // global while leaving the later Row/RowKey queues locally drainable.
        lm.acquire(TxnId(1), LockKey::Unique { index: 1, tuple: vec![255] },
            LockMode::Exclusive, None, &|| false).unwrap();
        lm.acquire(TxnId(5), LockKey::Interval {
            space: LockSpace::Unique,
            start: Bound::Included(point_bytes(2, &[0])),
            end: Bound::Unbounded,
        }, LockMode::Shared, None, &|| false).unwrap();
    }
    let d = row(1);
    let k = LockKey::RowKey { table: 1, pk: vec![1] };
    lm.acquire(TxnId(1), d.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    lm.acquire(TxnId(2), k.clone(), LockMode::Shared, None, &|| false).unwrap();
    let head = spawn_without_timer(&lm, 3, k.clone(), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let (send, recv) = mpsc::channel();
    let bypass = thread::spawn({
        let lm = lm.clone();
        move || {
            let group = [(k, LockMode::Shared), (d, LockMode::Shared)];
            send.send(lm.acquire_points(TxnId(4), &group, None, &|| false)).unwrap();
        }
    });
    until(|| lm.waiting_count() == 2);
    let before = SHARD_LOCKS.with(|count| count.get());
    lm.release_all(TxnId(1));
    assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, expected_locks);
    assert_eq!(finish((bypass, recv)), Ok(()));
    lm.global().unwrap().assert_registry_covers(TxnId(4));
    assert_eq!(lm.blockers(TxnId(3)), vec![TxnId(2), TxnId(4)]);
    lm.release_all(TxnId(2));
    lm.release_all(TxnId(4));
    assert_eq!(finish(head), Ok(()));
    lm.release_all(TxnId(3));
    lm.release_all(TxnId(5));
}

#[test]
fn phase3_release_drains_row_key_twin_with_compatible_bypass() {
    for shards in [1, 2, 64] {
        test_release_drains_row_key_twin(phase3_config(shards), false, 1);
    }
}

#[test]
fn phase3_release_drains_a_row_key_twin_after_an_earlier_shard_escalates() {
    for shards in [2, 64] {
        test_release_drains_row_key_twin(phase3_config(shards), true, 1 + shards);
    }
}

#[test]
fn phase3_release_escalates_for_a_twin_on_another_shard() {
    let cfg = LockConfig {
        placement: Some(|key| match key {
            LockKey::RowKey { .. } => 0,
            _ => placement(key),
        }),
        ..phase3_config(2)
    };
    test_release_drains_row_key_twin(cfg, false, 3);
}

#[test]
fn phase3_s3_cross_shard_group_escalates_after_first_shard() {
    for shards in [2, 64] {
        let lm = Arc::new(LockManager::new(phase3_config(shards)));
        let held = [row(0), row(1)];
        for key in &held {
            lm.acquire(TxnId(1), key.clone(), LockMode::Exclusive, None, &|| false).unwrap();
        }
        let first = spawn_without_timer(&lm, 2, row(0), LockMode::Exclusive);
        until(|| lm.waiting_count() == 1);
        let (parked, at) = mpsc::channel();
        let (go, resume) = mpsc::channel();
        let (send, recv) = mpsc::channel();
        let group = thread::spawn({
            let lm = lm.clone();
            move || {
                let group = [(row(1), LockMode::Exclusive), (row(0), LockMode::Exclusive)];
                let announced = AtomicBool::new(false);
                let result = lm.acquire_points(TxnId(3), &group, None, &|| {
                    if !announced.swap(true, Ordering::Relaxed) {
                        parked.send(()).unwrap();
                        resume.recv_timeout(Duration::from_secs(10)).unwrap();
                    }
                    false
                });
                send.send(result).unwrap();
            }
        });
        at.recv_timeout(Duration::from_secs(5)).unwrap();
        let behind = spawn_without_timer(&lm, 4, row(1), LockMode::Exclusive);
        until(|| lm.waiting_count() == 3);
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(TxnId(1));
        assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, 2 + shards);
        assert_eq!(finish(first), Ok(()));
        lm.global().unwrap().assert_registry_covers(TxnId(2));
        assert_eq!(lm.held(TxnId(3)), vec![(row(1), LockMode::Exclusive)]);
        assert_eq!(lm.blockers(TxnId(4)), vec![TxnId(3)]);
        go.send(()).unwrap();
        lm.release_all(TxnId(2));
        assert_eq!(finish((group, recv)), Ok(()));
        lm.global().unwrap().assert_registry_covers(TxnId(3));
        lm.release_all(TxnId(3));
        assert_eq!(finish(behind), Ok(()));
        lm.global().unwrap().assert_registry_covers(TxnId(4));
        lm.release_all(TxnId(4));
        assert_eq!(lm.waiting_count(), 0);
    }
}

#[test]
fn phase3_release_many_shards_and_table_only() {
    let lm = LockManager::new(phase3_config(64));
    let txn = TxnId(1);
    for index in 0..=24 {
        let (key, mode) = if index % 2 == 0 {
            (LockKey::Table(index), LockMode::AccessExclusive)
        } else {
            (row(index as u8), LockMode::Exclusive)
        };
        lm.acquire(txn, key, mode, None, &|| false).unwrap();
    }
    lm.global().unwrap().assert_registry_covers(txn);
    let before = SHARD_LOCKS.with(|count| count.get());
    lm.release_all(txn);
    assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, 25);
    assert!(lm.held(txn).is_empty());

    lm.acquire(txn, LockKey::Table(63), LockMode::AccessExclusive, None, &|| false).unwrap();
    let before = SHARD_LOCKS.with(|count| count.get());
    lm.release_all(txn);
    assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, 1);
    assert!(lm.held(txn).is_empty());
}

#[test]
fn phase3_release_absent_txn_and_provisional_without_grant() {
    for shards in [1, 2, 64] {
        let lm = LockManager::new(phase3_config(shards));
        let txn = TxnId(1);
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(txn);
        assert_eq!(SHARD_LOCKS.with(|count| count.get()), before);

        let key = lm.prepare(row(1));
        {
            let mut state = lm.global().unwrap();
            // Model the provisional snapshot taken before the first grant or
            // enqueue. Replacing a duplicate member must still count once.
            state.provisional_insert(txn, key.clone(), HeldModes::default());
            state.provisional_insert(txn, key.clone(), HeldModes::default());
            state.assert_registry_covers(txn);
            assert_eq!(lm.registry.stripe(txn)[&txn].provisional, 1);
            assert!(state.key(&key).is_none());
        }
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(txn);
        assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, shards);
        assert!(lm.global().unwrap().provisional_modes(txn, &key).is_none());
        lm.global().unwrap().assert_registry_covers(txn);
        assert!(!lm.registry.stripe(txn).contains_key(&txn));
    }
}

#[test]
fn phase3_release_recovers_one_poisoned_shard() {
    for shards in [1, 2, 64] {
        let lm = LockManager::new(phase3_config(shards));
        let txn = TxnId(1);
        let key = row(1);
        let shard = lm.shard(&key);
        lm.acquire(txn, key, LockMode::Exclusive, None, &|| false).unwrap();
        assert!(std::panic::catch_unwind(|| {
            let _guard = lm.shards[shard].lock().unwrap();
            panic!("poison the holder's shard");
        }).is_err());
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(txn);
        assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, 1);
        assert!(lm.held(txn).is_empty());
    }
}

#[test]
fn phase3_release_wakes_waiters_on_each_held_shard() {
    for shards in [1, 2, 64] {
        let lm = Arc::new(LockManager::new(phase3_config(shards)));
        let mut waiters = Vec::new();
        for pk in 0..3 {
            let key = row(pk);
            lm.acquire(TxnId(1), key.clone(), LockMode::Exclusive, None, &|| false).unwrap();
            waiters.push(spawn_without_timer(&lm, u64::from(pk) + 2, key, LockMode::Shared));
            until(|| lm.waiting_count() == waiters.len());
        }
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(TxnId(1));
        assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, shards.min(3));
        for (txn, waiter) in (2..).zip(waiters) {
            assert_eq!(finish(waiter), Ok(()));
            lm.global().unwrap().assert_registry_covers(TxnId(txn));
            lm.release_all(TxnId(txn));
        }
        assert_eq!(lm.waiting_count(), 0);
    }
}

#[test]
fn phase3_opt_in_release_exposes_partial_cross_shard_group() {
    let lm = Arc::new(LockManager::new(phase3_config(2)));
    let txn = TxnId(1);
    let a = LockKey::Unique { index: 1, tuple: vec![1] };
    let b = row(1);
    let group = [(a.clone(), LockMode::Exclusive), (b.clone(), LockMode::Exclusive)];
    lm.acquire_points(txn, &group, None, &|| false).unwrap();

    let (at, go) = pause_release(&lm, 1, false);
    let releaser = spawn_release(&lm, txn);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(lm.held(txn), vec![(b.clone(), LockMode::Exclusive)]);
    let observer = TxnId(2);
    assert_eq!(lm.acquire(observer, a, LockMode::Exclusive, Some(Instant::now()), &|| false), Ok(()));
    assert_eq!(lm.acquire(observer, b, LockMode::Exclusive, Some(Instant::now()), &|| false), Err(LockError::Timeout));
    go.send(()).unwrap();
    assert_eq!(releaser.join().unwrap(), 2);
    assert!(lm.held(txn).is_empty());
    lm.release_all(observer);
}

#[test]
fn default_release_preserves_a_same_id_grant_after_the_first_shard_is_free() {
    let lm = Arc::new(LockManager::new(config(2)));
    let txn = TxnId(1);
    for pk in [0, 1] {
        lm.acquire(txn, row(pk), LockMode::Exclusive, None, &|| false).unwrap();
    }

    let (at, go) = pause_release(&lm, 1, true);
    let releaser = spawn_release(&lm, txn);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    let both_held = lm.shards.iter().all(|shard| {
        matches!(shard.try_lock(), Err(std::sync::TryLockError::WouldBlock))
    });
    let (started, attempt) = mpsc::channel();
    let (granted, result) = mpsc::channel();
    let late_request = thread::spawn({
        let lm = lm.clone();
        move || {
            started.send(()).unwrap();
            // The same-ID request is issued only after another owner takes A.
            lm.acquire(TxnId(2), row(0), LockMode::Exclusive, Some(Instant::now()), &|| false)?;
            let grant = lm.acquire(txn, row(1), LockMode::Exclusive, Some(Instant::now()), &|| false);
            granted.send(grant).unwrap();
            grant
        }
    });
    attempt.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = result.recv_timeout(Duration::from_millis(100));
    go.send(()).unwrap();
    releaser.join().unwrap();
    assert_eq!(late_request.join().unwrap(), Ok(()));
    assert!(both_held);
    assert_eq!(early, Err(mpsc::RecvTimeoutError::Timeout));
    assert_eq!(lm.held(txn), vec![(row(1), LockMode::Exclusive)]);
    lm.global().unwrap().assert_registry_covers(txn);
    assert_eq!(lm.acquire(TxnId(3), row(1), LockMode::Exclusive, Some(Instant::now()), &|| false), Err(LockError::Timeout));
    lm.release_all(txn);
    lm.release_all(TxnId(2));
}

#[test]
fn default_release_keeps_the_row_limit_authoritative_during_a_same_id_acquire() {
    let lm = Arc::new(LockManager::new(LockConfig { max_row_locks_per_txn: 1, ..config(2) }));
    let txn = TxnId(1);
    lm.acquire(txn, row(0), LockMode::Exclusive, None, &|| false).unwrap();

    let (at, go) = pause_release(&lm, 0, true);
    let releaser = spawn_release(&lm, txn);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    let registered_rows = lm.registry.rows(txn);
    let (started, attempt) = mpsc::channel();
    let (send, recv) = mpsc::channel();
    let acquire = thread::spawn({
        let lm = lm.clone();
        move || {
            started.send(()).unwrap();
            let result = lm.acquire(txn, row(1), LockMode::Exclusive, Some(Instant::now()), &|| false);
            send.send(result).unwrap();
        }
    });
    attempt.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = recv.recv_timeout(Duration::from_millis(100));
    go.send(()).unwrap();
    releaser.join().unwrap();
    acquire.join().unwrap();
    assert_eq!(registered_rows, 1);
    assert_eq!(early, Err(mpsc::RecvTimeoutError::Timeout));
    assert_eq!(recv.try_recv().unwrap(), Ok(()));
    assert_eq!(lm.held(txn), vec![(row(1), LockMode::Exclusive)]);
    lm.global().unwrap().assert_registry_covers(txn);
    lm.release_all(txn);
}

#[test]
fn default_duplicate_release_returns_only_after_the_holding_is_gone() {
    let lm = Arc::new(LockManager::new(config(2)));
    let txn = TxnId(1);
    lm.acquire(txn, row(1), LockMode::Exclusive, None, &|| false).unwrap();

    let (at, go) = pause_release(&lm, 0, true);
    let first = spawn_release(&lm, txn);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    let holding_guarded = matches!(lm.shards[1].try_lock(), Err(std::sync::TryLockError::WouldBlock));
    let (started, attempt) = mpsc::channel();
    let (returned, released) = mpsc::channel();
    let second = thread::spawn({
        let lm = lm.clone();
        move || {
            started.send(()).unwrap();
            lm.release_all(txn);
            returned.send(()).unwrap();
            lm.acquire(TxnId(2), row(1), LockMode::Exclusive, Some(Instant::now()), &|| false)
        }
    });
    attempt.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = released.recv_timeout(Duration::from_millis(100));
    go.send(()).unwrap();
    first.join().unwrap();
    assert_eq!(second.join().unwrap(), Ok(()));
    assert!(holding_guarded);
    assert_eq!(early, Err(mpsc::RecvTimeoutError::Timeout));
    assert!(lm.held(txn).is_empty());
    lm.release_all(TxnId(2));
}

#[test]
fn default_duplicate_release_takes_all_shards_even_after_registry_removal() {
    let lm = Arc::new(LockManager::new(LockConfig::default()));
    assert!(!lm.per_shard_release());
    let owner = TxnId(1);
    let waiter_txn = TxnId(2);
    let key = row(1);
    lm.acquire(owner, key.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    let waiter = spawn(&lm, waiter_txn.0, key.clone(), LockMode::Exclusive);
    until(|| lm.waiting_count() == 1);
    let slot = {
        let state = lm.global().unwrap();
        state.waiter(&state.pending(waiter_txn)[0]).unwrap().slot.clone()
    };

    let (parked, at) = mpsc::channel();
    let (go, resume) = mpsc::channel();
    let resume = Mutex::new(resume);
    *lm.hooks.before_release_drain.lock().unwrap() = Some(Arc::new(move || {
        parked.send(()).unwrap();
        resume.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
    }));
    let first = spawn_release(&lm, owner);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(!lm.registry.stripe(owner).contains_key(&owner));
    assert_eq!(slot.result(), None);

    let second = spawn_release(&lm, owner);
    until(|| lm.release_all_calls() == 2);
    go.send(()).unwrap();
    first.join().unwrap();
    let second_shards = second.join().unwrap();
    *lm.hooks.before_release_drain.lock().unwrap() = None;
    assert_eq!(finish(waiter), Ok(()));
    assert_eq!(lm.held(waiter_txn), vec![(key, LockMode::Exclusive)]);
    lm.release_all(waiter_txn);

    assert_eq!(second_shards, lm.shards.len(), "duplicate release skipped shard guards");
}

#[test]
fn phase3_fallback_cancels_waiters_and_cleans_up_a_recreated_registry_entry() {
    for queue_waiter in [false, true] {
        let lm = Arc::new(LockManager::new(phase3_config(2)));
        let txn = TxnId(1);
        for pk in [0, 1] {
            lm.acquire(txn, row(pk), LockMode::Exclusive, None, &|| false).unwrap();
        }
        let table = LockKey::Table(1);
        lm.acquire(TxnId(2), table.clone(), LockMode::AccessExclusive, None, &|| false).unwrap();

        let (at, go) = pause_release(&lm, 1, false);
        let releaser = spawn_release(&lm, txn);
        at.recv_timeout(Duration::from_secs(5)).unwrap();
        // Deliberately violate the opt-in serialization contract. The new grant
        // records a bit on the finished shard, outside the detached remainder.
        lm.acquire(txn, row(0), LockMode::Exclusive, None, &|| false).unwrap();
        let key = lm.prepare(row(1));
        let waiter = if queue_waiter {
            let waiter = spawn_without_timer(&lm, txn.0, table, LockMode::AccessShare);
            until(|| lm.waiting_count() == 1);
            Some(waiter)
        } else {
            lm.global().unwrap().provisional_insert(txn, key.clone(), HeldModes::default());
            None
        };

        go.send(()).unwrap();
        assert_eq!(releaser.join().unwrap(), 4);
        if let Some(waiter) = waiter {
            assert_eq!(finish(waiter), Err(LockError::Cancelled));
        }
        assert!(lm.held(txn).is_empty());
        let state = lm.global().unwrap();
        assert!(state.provisional_modes(txn, &key).is_none());
        state.assert_registry_covers(txn);
        assert!(!lm.registry.stripe(txn).contains_key(&txn));
        drop(state);
        lm.release_all(TxnId(2));
    }
}

#[test]
fn phase3_shard_drain_and_global_drain_enforce_the_same_row_limit() {
    for per_shard_release in [false, true] {
        let lm = Arc::new(LockManager::new(LockConfig {
            per_shard_release,
            max_row_locks_per_txn: 1,
            ..config(2)
        }));
        for (txn, pk) in [(1, 1), (2, 0)] {
            lm.acquire(TxnId(txn), row(pk), LockMode::Exclusive, None, &|| false).unwrap();
        }
        let at_limit = spawn_without_timer(&lm, 2, row(1), LockMode::Exclusive);
        until(|| lm.waiting_count() == 1);
        let follower = spawn_without_timer(&lm, 3, row(1), LockMode::Exclusive);
        until(|| lm.waiting_count() == 2);
        let before = SHARD_LOCKS.with(|count| count.get());
        lm.release_all(TxnId(1));
        assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, if per_shard_release { 1 } else { 2 });
        assert_eq!(finish(at_limit), Err(LockError::Limit));
        assert_eq!(finish(follower), Ok(()));
        assert_eq!(lm.held(TxnId(2)), vec![(row(0), LockMode::Exclusive)]);
        for id in [TxnId(2), TxnId(3)] {
            lm.global().unwrap().assert_registry_covers(id);
            lm.release_all(id);
        }
        assert_eq!(lm.waiting_count(), 0);
    }
}

#[test]
fn phase3_release_recovers_a_poisoned_shard_during_fallback() {
    let lm = Arc::new(LockManager::new(phase3_config(2)));
    let txn = TxnId(1);
    lm.acquire(txn, row(1), LockMode::Exclusive, None, &|| false).unwrap();
    let bound = point_bytes(1, &[1]);
    let interval = LockKey::Interval {
        space: LockSpace::Row, start: Bound::Included(bound.clone()), end: Bound::Included(bound),
    };
    let (parked, at) = mpsc::channel();
    let (go, resume) = mpsc::channel();
    let resume = Mutex::new(resume);
    *lm.hooks.h2.lock().unwrap() = Some(Arc::new(move || {
        parked.send(()).unwrap();
        resume.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
    }));
    let waiter = spawn_without_timer(&lm, 2, interval, LockMode::Shared);
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(lm.blockers(TxnId(2)), vec![txn]);
    assert!(std::panic::catch_unwind(|| {
        let _guard = lm.shards[1].lock().unwrap();
        panic!("poison the fallback shard");
    }).is_err());

    let before = SHARD_LOCKS.with(|count| count.get());
    lm.release_all(txn);
    assert_eq!(SHARD_LOCKS.with(|count| count.get()) - before, 3);
    go.send(()).unwrap();
    assert_eq!(finish(waiter), Ok(()));
    assert!(lm.held(txn).is_empty());
    lm.global().unwrap_or_else(|poison| poison.into_inner()).assert_registry_covers(TxnId(2));
    lm.release_all(TxnId(2));
    assert_eq!(lm.waiting_count(), 0);
}

#[test]
fn phase3_blocking_mixed_key_stress_cleans_up_all_shards_and_stripes() {
    const WORKERS: u64 = 8;
    for shards in [2, 64] {
        for forced in [false, true] {
            let mut cfg = phase3_config(shards);
            if !forced {
                cfg.placement = None;
            }
            let lm = Arc::new(LockManager::new(cfg));
            let table = LockKey::Table(1);
            lm.acquire(TxnId(0), table.clone(), LockMode::AccessExclusive, None, &|| false).unwrap();
            // Force an interval fallback while the workers are queued on the table.
            let interval = LockKey::Interval {
                space: LockSpace::Unique,
                start: Bound::Unbounded,
                end: Bound::Unbounded,
            };
            lm.acquire(TxnId(0), interval, LockMode::Shared, None, &|| false).unwrap();
            let mut workers = Vec::new();
            for worker in 1..=WORKERS {
                let lm = lm.clone();
                let table = table.clone();
                let (send, recv) = mpsc::channel();
                let handle = thread::spawn(move || {
                    let txn = TxnId(worker);
                    let mut rng = worker;
                    let releases_before = SHARD_RELEASES.get();
                    for _ in 0..32 {
                        let group_pk = (random(&mut rng) % 4) as u8;
                        let row_pk = 4 + (random(&mut rng) % 4) as u8;
                        let row_key_pk = 8 + (random(&mut rng) % 4) as u8;
                        let tuple = vec![(random(&mut rng) % 4) as u8];
                        let group = [
                            (LockKey::RowKey { table: 1, pk: vec![group_pk] }, LockMode::Exclusive),
                            (row(group_pk), LockMode::Exclusive),
                        ];
                        let result = (|| {
                            lm.acquire(txn, table.clone(), LockMode::RowExclusive, None, &|| false)?;
                            lm.acquire_points(txn, &group, None, &|| false)?;
                            lm.acquire(txn, row(row_pk), LockMode::Exclusive, None, &|| false)?;
                            lm.acquire(txn, LockKey::RowKey { table: 1, pk: vec![row_key_pk] },
                                LockMode::Exclusive, None, &|| false)?;
                            lm.acquire(txn, LockKey::Unique { index: 1, tuple },
                                LockMode::Exclusive, None, &|| false)
                        })();
                        lm.release_all(txn);
                        if let Err(error) = result {
                            send.send(Err(error)).unwrap();
                            return;
                        }
                    }
                    send.send(Ok(SHARD_RELEASES.get() - releases_before)).unwrap();
                });
                workers.push((handle, recv));
            }
            until(|| lm.waiting_count() == WORKERS as usize);
            let locks_before = SHARD_LOCKS.get();
            lm.release_all(TxnId(0));
            assert!(SHARD_LOCKS.get() - locks_before > shards, "interval release did not escalate");
            let local_releases: usize = workers.into_iter().map(|worker| finish(worker).unwrap()).sum();
            assert!(local_releases > 0, "no worker released a shard locally (shards={shards}, forced={forced})");
            assert_eq!(lm.waiting_count(), 0);
            let state = lm.global().unwrap();
            for txn in 0..=WORKERS {
                state.assert_registry_covers(TxnId(txn));
            }
            for shard in state.guards.slice() {
                assert!(shard.keys.is_empty());
                assert!(shard.held.is_empty());
                assert!(shard.by_txn.is_empty());
                assert!(shard.waiters.is_empty());
                assert!(shard.provisional.is_empty());
            }
            assert!(lm.registry.stripes.iter().all(|stripe| stripe.lock().unwrap().is_empty()));
        }
    }
}

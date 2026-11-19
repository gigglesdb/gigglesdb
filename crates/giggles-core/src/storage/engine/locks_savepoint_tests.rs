use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::Kv,
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;
use crate::relational::scan::RowLock;

fn setup(budget: usize) -> (Arc<Sequencer>, Arc<LockManager>) {
    let seq = Sequencer::open(
        Arc::new(Kv::new_single_writer(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            write_set_bytes: budget,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    (seq, Arc::new(LockManager::new(LockConfig::default())))
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), Isolation::ReadCommitted)
}
fn key(n: u64) -> Vec<u8> {
    [1u64.to_be_bytes(), n.to_be_bytes()].concat()
}

#[test]
fn restores_modes_and_nested_marks_without_losing_outer_journal() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::KeyShare, None, &|| false)
        .unwrap();
    let original = locks.held(t.id);
    let outer = t.savepoint();
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::Update, None, &|| false)
        .unwrap();
    let inner = t.savepoint();
    t.lock_for_write(Keyspace::Rows, &key(2), None, &|| false)
        .unwrap();
    let retained = locks.held(t.id);
    t.rollback_to(&inner).unwrap(); // native rollback retains grants and journal
    assert_eq!(locks.held(t.id), retained);
    t.release(inner).unwrap();
    t.rollback_to_releasing_locks(&outer).unwrap();
    assert_eq!(locks.held(t.id), original);
    assert!(!t.locked.contains_key(&(Keyspace::Rows, key(2))));
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::KeyShare, None, &|| false)
        .unwrap();
    t.rollback_to_releasing_locks(&outer).unwrap();
    assert_eq!(locks.held(t.id), original); // covered re-lock survives
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::Update, None, &|| false)
        .unwrap();
    t.rollback_to_releasing_locks(&outer).unwrap();
    assert_eq!(locks.held(t.id), original); // mark reusable
}

#[test]
fn inner_rollback_frees_only_inner_locks_and_clears_upgrade_cache() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    let outer = t.savepoint();
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::Share, None, &|| false)
        .unwrap();
    let inner = t.savepoint();
    let before = locks.held(t.id);
    t.lock_row_write_group(1, &1u64.to_be_bytes(), None, &|| false)
        .unwrap();
    t.lock_for_write(Keyspace::Rows, &key(1), None, &|| false)
        .unwrap();
    t.rollback_to_releasing_locks(&inner).unwrap();
    let after = locks.held(t.id);
    assert_eq!(after.len(), before.len());
    assert!(before.iter().all(|entry| after.contains(entry)));
    assert!(!t.locked.contains_key(&(Keyspace::Rows, key(1))));
    t.rollback_to_releasing_locks(&outer).unwrap();
    assert!(locks.held(t.id).is_empty());
}

#[test]
fn excluded_holds_survive_and_row_budget_is_reusable() {
    use super::super::locks::LockConfig;
    let (seq, _) = setup(4096);
    let locks = Arc::new(LockManager::new(LockConfig {
        max_row_locks_per_txn: 4,
        ..Default::default()
    }));
    let mut t = txn(&seq, &locks, 1);
    let mark = t.savepoint();
    t.lock_sql_sentinel(1, LockMode::Shared, &|| false).unwrap();
    t.lock_kernel_key(b"authority", None, &|| false).unwrap();
    t.acquire_lock(LockKey::Table(1), LockMode::RowExclusive, None, &|| false)
        .unwrap();
    t.lock_for_write(Keyspace::Index, &key(4), None, &|| false)
        .unwrap();
    t.lock_for_write(Keyspace::Rows, &key(1), None, &|| false)
        .unwrap();
    t.rollback_to_releasing_locks(&mark).unwrap();
    assert_eq!(locks.held(t.id).len(), 3);
    assert!(t.locked.is_empty());
    t.lock_for_write(Keyspace::Rows, &key(2), None, &|| false)
        .unwrap();
    t.lock_for_write(Keyspace::Index, &key(5), None, &|| false)
        .unwrap();
}

#[test]
fn rollback_wakes_queued_waiter_but_retains_outer_conflicts() {
    use std::{
        thread,
        time::{Duration, Instant},
    };
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    t.lock_row_key(1, &1u64.to_be_bytes(), LockMode::Shared, None, &|| false)
        .unwrap();
    let mark = t.savepoint();
    t.lock_row_write_group(1, &1u64.to_be_bytes(), None, &|| false)
        .unwrap();
    let l = locks.clone();
    let waiter = thread::spawn(move || {
        l.acquire(
            TxnId(2),
            LockKey::Row {
                table: 1,
                pk: 1u64.to_be_bytes().to_vec(),
            },
            LockMode::Exclusive,
            Some(Instant::now() + Duration::from_secs(3)),
            &|| false,
        )
    });
    let until = Instant::now() + Duration::from_secs(2);
    while locks.waiting_count() == 0 {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    t.rollback_to_releasing_locks(&mark).unwrap();
    waiter.join().unwrap().unwrap();
    assert_eq!(
        locks.acquire(
            TxnId(3),
            LockKey::RowKey {
                table: 1,
                pk: 1u64.to_be_bytes().to_vec()
            },
            LockMode::Exclusive,
            Some(Instant::now()),
            &|| false
        ),
        Err(super::super::locks::LockError::Timeout)
    );
    locks.release_all(TxnId(2));
}

#[test]
fn all_four_modes_and_fk_key_lock_are_released() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    for mode in [
        RowLock::Update,
        RowLock::NoKeyUpdate,
        RowLock::Share,
        RowLock::KeyShare,
    ] {
        let mark = t.savepoint();
        t.lock_sql_row(1, &1u64.to_be_bytes(), mode, None, &|| false)
            .unwrap();
        t.rollback_to_releasing_locks(&mark).unwrap();
        assert!(locks.held(t.id).is_empty());
        t.release(mark).unwrap();
    }
    let mark = t.savepoint();
    t.lock_row_key(1, &1u64.to_be_bytes(), LockMode::Shared, None, &|| false)
        .unwrap();
    t.rollback_to_releasing_locks(&mark).unwrap();
    assert!(locks.held(t.id).is_empty());
}

#[test]
fn downgrade_and_release_refresh_point_interval_index() {
    use super::super::locks::{LockError, LockSpace};
    use std::{ops::Bound, time::Instant};
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    let row = LockKey::Row {
        table: 1,
        pk: 1u64.to_be_bytes().to_vec(),
    };
    let interval = LockKey::Interval {
        space: LockSpace::Row,
        start: Bound::Included(key(1)),
        end: Bound::Included(key(1)),
    };
    // Initialize the lazy index before the upgrade, then remove its holder.
    locks
        .acquire(
            TxnId(2),
            interval.clone(),
            LockMode::Shared,
            Some(Instant::now()),
            &|| false,
        )
        .unwrap();
    locks.release_all(TxnId(2));
    let outer = t.savepoint();
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::Share, None, &|| false)
        .unwrap();
    let inner = t.savepoint();
    t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::NoKeyUpdate, None, &|| {
        false
    })
    .unwrap();
    t.rollback_to_releasing_locks(&inner).unwrap();
    locks
        .acquire(
            TxnId(2),
            row.clone(),
            LockMode::Shared,
            Some(Instant::now()),
            &|| false,
        )
        .unwrap();
    assert_eq!(
        locks.acquire(
            TxnId(3),
            row.clone(),
            LockMode::Exclusive,
            Some(Instant::now()),
            &|| false
        ),
        Err(LockError::Timeout)
    );
    locks
        .acquire(
            TxnId(3),
            interval.clone(),
            LockMode::Shared,
            Some(Instant::now()),
            &|| false,
        )
        .unwrap();
    locks.release_all(TxnId(2));
    locks.release_all(TxnId(3));
    t.rollback_to_releasing_locks(&outer).unwrap();
    locks
        .acquire(
            TxnId(2),
            interval,
            LockMode::Shared,
            Some(Instant::now()),
            &|| false,
        )
        .unwrap();
    assert_eq!(
        locks.acquire(
            TxnId(3),
            row.clone(),
            LockMode::Exclusive,
            Some(Instant::now()),
            &|| false
        ),
        Err(LockError::Timeout)
    );
    locks.release_all(TxnId(2));
    locks
        .acquire(
            TxnId(3),
            row,
            LockMode::Exclusive,
            Some(Instant::now()),
            &|| false,
        )
        .unwrap();
    locks.release_all(TxnId(3));
}

#[test]
fn invalid_lock_journal_offset_is_misuse_before_mutation() {
    let (seq, locks) = setup(4096);
    let mut t = txn(&seq, &locks, 1);
    let mut mark = t.savepoint();
    mark.lock_undo_len = 1;
    assert!(matches!(
        t.rollback_to_releasing_locks(&mark),
        Err(TxnError::Misuse)
    ));
    mark.lock_undo_len = 0;
    t.rollback_to_releasing_locks(&mark).unwrap();
}

#[test]
fn cancelled_upgrade_cleanup_is_not_undone_by_savepoint_rollback() {
    use std::{cell::Cell, time::Instant};
    let (seq, locks) = setup(4096);
    let mut t1 = txn(&seq, &locks, 1);
    let mut t2 = txn(&seq, &locks, 2);
    for t in [&mut t1, &mut t2] {
        t.lock_sql_row(1, &1u64.to_be_bytes(), RowLock::Share, None, &|| false)
            .unwrap();
    }
    let mark = t1.savepoint();
    let id = t1.id;
    let cancelled = Cell::new(false);
    let result = t1.lock_for_write(Keyspace::Rows, &key(1), None, &|| {
        cancelled.set(true);
        locks.release_all(id);
        false
    });
    assert!(cancelled.get());
    assert!(matches!(result, Err(TxnError::Cancelled)));
    assert!(t1.lock_undo.is_empty());
    t1.rollback_to_releasing_locks(&mark).unwrap();
    assert!(locks.held(id).is_empty());
    locks.release_all(t2.id);
    locks.acquire(
        TxnId(3),
        LockKey::Row { table: 1, pk: 1u64.to_be_bytes().to_vec() },
        LockMode::Exclusive,
        Some(Instant::now()),
        &|| false,
    ).unwrap();
    locks.release_all(TxnId(3));
}

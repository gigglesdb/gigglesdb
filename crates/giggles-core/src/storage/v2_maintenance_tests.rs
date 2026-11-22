use super::{DEFAULT_WRITE_SET_BYTES, V2Db, engine, maintenance::TICK};
use crate::{Admin, Limits, storage::Backend};
use engine::{
    codec,
    hlc::Ts,
    kv::{Direction, Keyspace},
    kv_mem::MemKv,
    mvcc,
    txn::Isolation,
};
use std::{
    ops::Bound,
    sync::Arc,
    time::{Duration, Instant},
};

const STAMP: &[u8] = &[0xFF];

fn backend(features: u64) -> Arc<MemKv> {
    let backend = Arc::new(MemKv::new());
    let mut domain = [0; 16];
    getrandom::fill(&mut domain).unwrap();
    engine::meta::write_initial_meta(backend.as_ref(), domain, features).unwrap();
    backend
}

fn attach(backend: &Arc<MemKv>) -> Admin {
    Admin::attach_v2_backend(backend.clone(), Limits::default(), DEFAULT_WRITE_SET_BYTES).unwrap()
}

fn db(admin: &Admin) -> crate::storage::EngineLease {
    match &admin.inner.backend {
        Backend::V2(_) => admin.test_v2(),
        Backend::Legacy(_) => unreachable!("v2 backend expected"),
    }
}

/// With zero retention, W is `visible` truncated to whole milliseconds. A
/// commit made first in a fresh millisecond has logical part 0, so W reaches it.
fn next_millisecond() {
    std::thread::sleep(Duration::from_millis(2));
}

/// Commits one Kernel mutation stamp: a valid, superseding Kernel version.
fn stamp(db: &V2Db) -> Ts {
    next_millisecond();
    let mut txn = db.begin(Isolation::ReadCommitted);
    txn.put(Keyspace::Kernel, STAMP.to_vec(), Vec::new())
        .unwrap();
    txn.commit().unwrap()
}

/// Raw version counts per user key, in key order.
fn groups(db: &V2Db, ks: Keyspace) -> Vec<(Vec<u8>, usize)> {
    let mut groups: Vec<(Vec<u8>, usize)> = Vec::new();
    for pair in db
        .kv
        .range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
        .unwrap()
    {
        let (raw, _) = pair.unwrap();
        let user = codec::split_versioned(&raw).unwrap().0.to_vec();
        match groups.last_mut() {
            Some((last, count)) if *last == user => *count += 1,
            _ => groups.push((user, 1)),
        }
    }
    groups
}

fn stamp_versions(db: &V2Db) -> usize {
    groups(db, Keyspace::Kernel)
        .into_iter()
        .find(|(user, _)| user == STAMP)
        .map_or(0, |(_, count)| count)
}

#[test]
fn superseded_kernel_versions_are_removed() {
    let backend = backend(0);
    let admin = attach(&backend);
    let db = db(&admin);
    for _ in 0..5 {
        stamp(&db);
    }
    assert_eq!(stamp_versions(&db), 5);
    admin.set_gc_retention(Duration::ZERO).unwrap();
    let stats = admin.maintenance_step().unwrap();
    assert!(stats.deleted >= 4, "{stats:?}");
    assert_eq!(stamp_versions(&db), 1, "only the live anchor remains");
    assert_eq!(stats.watermark, db.sequencer.visible_ts().0);
    assert_eq!(stats.background_errors, 0);
}

#[test]
fn open_snapshot_holds_watermark() {
    let backend = backend(0);
    let admin = attach(&backend);
    let db = db(&admin);
    admin.set_gc_retention(Duration::ZERO).unwrap();
    let first = stamp(&db);
    let held = db.sequencer.snapshot();
    assert_eq!(held.ts(), first);
    for _ in 0..3 {
        stamp(&db);
    }
    let stats = admin.maintenance_step().unwrap();
    assert_eq!(stats.watermark, first.0, "the open snapshot bounds W");
    assert_eq!(
        stamp_versions(&db),
        4,
        "the snapshot's anchor and every newer version survive"
    );
    assert!(
        mvcc::read_at(db.kv.as_ref(), Keyspace::Kernel, STAMP, held.ts())
            .unwrap()
            .is_some()
    );
    drop(held);
    let stats = admin.maintenance_step().unwrap();
    assert!(stats.watermark > first.0);
    assert_eq!(stamp_versions(&db), 1);
}

#[test]
fn retention_window_respected() {
    let backend = backend(0);
    let admin = attach(&backend);
    let db = db(&admin);
    let first = stamp(&db);
    stamp(&db);
    stamp(&db);
    let stats = admin.maintenance_step().unwrap();
    assert!(
        stats.watermark < first.0,
        "the default 60 s window keeps W behind recent commits"
    );
    assert_eq!(stamp_versions(&db), 3);
    let visible_ms = db.sequencer.visible_ts().0 >> 16;
    assert!((stats.watermark >> 16) + 60_000 <= visible_ms);
    admin.set_gc_retention(Duration::ZERO).unwrap();
    admin.maintenance_step().unwrap();
    assert_eq!(stamp_versions(&db), 1);
}

#[test]
fn thread_stops_on_drop() {
    let backend = backend(0);
    let admin = attach(&backend);
    let db = db(&admin);
    assert!(db.maintenance.running());
    let gc = Arc::downgrade(&db.gc);
    drop(db);
    let started = Instant::now();
    drop(admin);
    assert!(
        started.elapsed() < TICK,
        "drop wakes the thread rather than waiting out a tick"
    );
    assert!(
        gc.upgrade().is_none(),
        "the joined thread released its collector"
    );
    // The released KV can be attached again.
    let reopened = attach(&backend);
    assert!(db_running(&reopened));
}

fn db_running(admin: &Admin) -> bool {
    db(admin).maintenance.running()
}

#[test]
fn replicated_database_starts_no_thread() {
    let backend = backend(engine::meta::REPLICATED);
    let admin = attach(&backend);
    assert!(!db_running(&admin));
    admin.set_gc_retention(Duration::ZERO).unwrap();
    let stats = admin.maintenance_step().unwrap();
    assert_eq!((stats.examined, stats.deleted), (0, 0));
}

#[test]
fn reopen_resumes_from_cursor() {
    let backend = backend(0);
    let admin = attach(&backend);
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id int PRIMARY KEY, v int)", &[], None)
        .unwrap();
    {
        let db = db(&admin);
        assert!(
            groups(&db, Keyspace::Rows).is_empty(),
            "no rows before ours"
        );
    }
    session
        .execute("INSERT INTO t VALUES (1, 0), (2, 0), (3, 0)", &[], None)
        .unwrap();
    for _ in 0..3 {
        next_millisecond();
        session
            .execute("UPDATE t SET v = v + 1", &[], None)
            .unwrap();
    }
    drop(session);
    let counts = |db: &V2Db| {
        groups(db, Keyspace::Rows)
            .into_iter()
            .map(|(_, count)| count)
            .collect::<Vec<_>>()
    };
    {
        let db = db(&admin);
        assert_eq!(counts(&db), [4, 4, 4]);
        admin.set_gc_retention(Duration::ZERO).unwrap();
        db.maintenance.set_budget(1);
        admin.maintenance_step().unwrap();
        assert_eq!(counts(&db), [1, 4, 4], "budget 1 collects one row group");
    }
    drop(admin);

    let admin = attach(&backend);
    let db = db(&admin);
    assert_eq!(counts(&db), [1, 4, 4]);
    let cursor = db
        .kv
        .get(Keyspace::Gc, &[Keyspace::Rows as u8])
        .unwrap()
        .unwrap();
    assert!(!cursor.is_empty(), "the Rows cursor survived the reopen");
    admin.set_gc_retention(Duration::ZERO).unwrap();
    db.maintenance.set_budget(1);
    admin.maintenance_step().unwrap();
    assert_eq!(
        counts(&db),
        [1, 1, 4],
        "the reopened collector resumes at the second row"
    );
    admin.maintenance_step().unwrap();
    assert_eq!(counts(&db), [1, 1, 1]);
}

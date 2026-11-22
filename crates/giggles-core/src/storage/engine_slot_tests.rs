use super::*;
use crate::{
    Admin, Engine, Limits, SqlSession,
    operations::{self, Bindings, Request, ValueExpr},
    relational::{SqlResult, Value::Int},
    storage::{
        engine::kv::{Direction, Keyspace},
        native_once::{
            NativeClockLease, NativeOnceAdmission, NativeOnceError, NativeOnceKey, NativeOnceOwner,
            NativePublishMode,
        },
    },
};
use std::{
    collections::BTreeSet,
    ops::Bound::Unbounded,
    path::PathBuf,
    sync::{Barrier, atomic::AtomicBool, mpsc},
    thread,
};

const WAIT: Duration = Duration::from_secs(10);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let mut id = [0; 16];
        getrandom::fill(&mut id).unwrap();
        Self(std::env::temp_dir().join(format!("engine-slot-{:032x}", u128::from_be_bytes(id))))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn fixture() -> (Directory, Admin) {
    let dir = Directory::new();
    let admin = Admin::create_engine(
        &dir.0,
        Limits {
            admitted: 64,
            selected_keys: 65_536,
        },
        Engine::V2,
    )
    .unwrap();
    sql(
        &mut admin.sql_session(),
        "CREATE TABLE t(id BIGINT PRIMARY KEY)",
    )
    .unwrap();
    (dir, admin)
}

fn slot(admin: &Admin) -> &EngineSlot {
    match &admin.inner.backend {
        super::super::Backend::V2(slot) => slot,
        _ => panic!("expected V2"),
    }
}

fn sql(session: &mut SqlSession, input: &str) -> Result<SqlResult, CoreError> {
    session.execute(input, &[], None)
}

fn close_and_reopen(slot: &EngineSlot, generation: Arc<Generation>) -> RecoveryCandidate {
    generation.drain(WAIT).unwrap();
    let proof = generation.close_drained().unwrap().wait(WAIT).unwrap();
    slot.reopen_candidate(&proof).unwrap()
}

fn swap(admin: &Admin) {
    let candidate = close_and_reopen(slot(admin), slot(admin).stop_admission(1).unwrap());
    slot(admin).install(candidate).unwrap();
}

fn read_ids(admin: &Admin) -> BTreeSet<i64> {
    sql(&mut admin.sql_session(), "SELECT id FROM t")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.values.as_slice() {
            [Int(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

fn allowed_sql(result: Result<(), CoreError>) -> bool {
    match result {
        Ok(()) => true,
        Err(
            CoreError::Fenced
            | CoreError::Cancelled
            | CoreError::Sql(crate::relational::SqlError::Aborted),
        ) => false,
        Err(error) => panic!("unexpected result {error:?}"),
    }
}

#[test]
fn s1_five_swaps_under_mixed_load_preserve_acknowledged_rows() {
    let (_dir, admin) = fixture();
    let workers = 16;
    let start = Arc::new(Barrier::new(workers + 1));
    let done = Arc::new(AtomicBool::new(false));
    let (ack, acks) = mpsc::channel();
    let (refuse, refusals) = mpsc::channel();
    let mut acknowledged = BTreeSet::new();
    let mut threads = Vec::new();
    for worker in 0..workers {
        let admin = admin.clone();
        let start = Arc::clone(&start);
        let done = Arc::clone(&done);
        let ack = ack.clone();
        let refuse = refuse.clone();
        threads.push(thread::spawn(move || {
            let mut session = admin.sql_session();
            start.wait();
            for attempt in 1.. {
                if done.load(Ordering::Acquire) {
                    break;
                }
                let id = attempt * workers as i64 + worker as i64;
                let insert = format!("INSERT INTO t VALUES ({id})");
                let (success, wrote) = match worker % 4 {
                    0 => (allowed_sql(sql(&mut session, &insert).map(drop)), true),
                    1 => {
                        let result = (|| {
                            sql(&mut session, "BEGIN")?;
                            sql(&mut session, &insert)?;
                            sql(&mut session, "SELECT id FROM t LIMIT 1")?;
                            sql(&mut session, "COMMIT")?;
                            Ok(())
                        })();
                        if result.is_err() {
                            session.rollback();
                        }
                        (allowed_sql(result), true)
                    }
                    2 => {
                        let result = (|| {
                            let p = session.prepare("SELECT id FROM t LIMIT 8")?;
                            let mut cursor = session.open_cursor(&p, &[])?;
                            while !cursor.fetch(2, 64)?.is_empty() {}
                            sql(&mut session, "SELECT id FROM t LIMIT 1").map(drop)
                        })();
                        (allowed_sql(result), false)
                    }
                    _ => {
                        let request = Request::InsertMany {
                            table: "t".into(),
                            columns: vec!["id".into()],
                            rows: vec![vec![operations::Expr::Value(ValueExpr::Literal(Int(id)))]],
                            cap: ValueExpr::Literal(Int(1)),
                        };
                        let success = match session.execute_native(&request, &Bindings::default()) {
                            Ok(_) => true,
                            Err(operations::Error::Core(error))
                                if error == CoreError::Fenced.to_string()
                                    || error == CoreError::Cancelled.to_string() =>
                            {
                                false
                            }
                            Err(error) => panic!("unexpected native error {error:?}"),
                        };
                        (success, true)
                    }
                };
                if success {
                    ack.send((worker, wrote.then_some(id))).unwrap();
                } else {
                    if wrote {
                        refuse.send(id).unwrap();
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }));
    }
    drop((ack, refuse));
    // Stop the workers even if a check panics so the fixture cannot keep
    // retrying against a closed slot.
    struct Stop(Arc<AtomicBool>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let stop = Stop(done);
    start.wait();
    for id in 1..=5 {
        let mut successful_workers = BTreeSet::new();
        let deadline = Instant::now() + WAIT;
        while successful_workers.len() < workers {
            let (worker, row) = acks
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            successful_workers.insert(worker);
            acknowledged.extend(row);
        }
        let generation = slot(&admin).stop_admission(id).unwrap();
        let candidate = close_and_reopen(slot(&admin), generation);
        acknowledged.extend(acks.try_iter().filter_map(|(_, id)| id));
        let check = acknowledged.clone();
        slot(&admin).install(candidate).unwrap();
        assert!(check.is_subset(&read_ids(&admin)));
        sql(
            &mut admin.sql_session(),
            &format!("INSERT INTO t VALUES (-{id})"),
        )
        .unwrap();
    }
    drop(stop);
    for worker in threads {
        worker.join().unwrap();
    }
    acknowledged.extend(acks.try_iter().filter_map(|(_, id)| id));
    let rows = read_ids(&admin);
    assert!(acknowledged.is_subset(&rows));
    assert!(refusals.try_iter().all(|id| !rows.contains(&id)));
    assert!((1..=5).all(|id| rows.contains(&-id)));
}

fn owner(admin: &Admin) -> NativeOnceOwner {
    let key = NativeOnceKey {
        database_domain: admin.authority_metadata().unwrap().0,
        identity: b"generation-owner".to_vec(),
    };
    match admin.admit_native_once_engine(key, [1; 32]).unwrap() {
        NativeOnceAdmission::Owner(owner) => owner,
        _ => panic!("expected new owner"),
    }
}

fn stamp(
    admin: &Admin,
) -> (
    super::super::engine::hlc::Ts,
    Vec<super::super::engine::kv::KvPair>,
) {
    let db = admin.test_v2();
    let seq = db
        .kv
        .range(Keyspace::Seq, Unbounded, Unbounded, Direction::Forward)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    (db.sequencer.visible_ts(), seq)
}

#[test]
fn s2_live_stale_objects_cannot_use_replacement_engine() {
    use crate::task::{
        StatementClass,
        context::TaskContext,
        definition::{TaskName, TaskObjectId},
    };
    let (old_dir, admin) = fixture();
    let mut session = admin.sql_session();
    sql(&mut session, "CREATE SEQUENCE seq").unwrap();
    let lease = session.native_sequence_lease().unwrap();
    let (_, sequence) = session.native_sequence_callback(lease.as_deref()).unwrap();
    drop(lease);
    let mut txn = admin.sql_session();
    sql(&mut txn, "BEGIN").unwrap();
    sql(&mut txn, "INSERT INTO t VALUES (20)").unwrap();
    let p = session.prepare("SELECT id FROM t").unwrap();
    let mut cursor = session.open_cursor(&p, &[]).unwrap();
    let owner = owner(&admin);
    let mut once = admin.sql_session();
    let setup = once.begin_once_engine(Some(&owner)).unwrap();
    let tasks = admin.tasks();
    session
        .bind_task(TaskContext {
            task: TaskObjectId(1),
            name: TaskName::new("fixture").unwrap(),
            executor: "fixture".into(),
            bound_def_version: 1,
            credential_fingerprint: [0; 32],
        })
        .unwrap();
    let readlog = session
        .start_task_statement(StatementClass::Select, 0)
        .unwrap();
    let role_image = admin.roles().image().unwrap();
    let old = slot(&admin).stop_admission(1).unwrap();
    assert!(matches!(
        old.drain(Duration::ZERO),
        Err(DrainError::TimedOut { .. })
    ));

    // A real same-directory reopen can occur only after these objects drop.
    // Use another real engine and bypass the drain guard here to exercise the
    // otherwise unreachable stale-object case without closing beneath a lease.
    let (new_dir, replacement) = fixture();
    drop(replacement);
    let replacement = V2Db::open(&new_dir.0).unwrap();
    slot(&admin).publish(
        &mut slot(&admin).state.lock().unwrap(),
        Generation::new(2, replacement, Some(old.registry.prepared.slot)),
    );

    let before = stamp(&admin);
    assert_eq!(sql(&mut txn, "SELECT id FROM t"), Err(CoreError::Fenced));
    assert!(matches!(cursor.fetch(1, 64), Err(CoreError::Fenced)));
    assert!(matches!(owner.lease(), Err(CoreError::Fenced)));
    assert!(matches!(
        once.publish_native_once_engine(
            Some(setup),
            owner,
            [2; 32],
            vec![],
            60,
            NativePublishMode::Success,
            &mut || panic!("stale owner called clock")
        ),
        Err(NativeOnceError::Core(CoreError::Fenced)),
    ));
    assert!(tasks.lookup("fixture").unwrap().is_none());
    assert!(matches!(
        session.tasks().lookup("fixture"),
        Err(CoreError::Fenced)
    ));
    assert_eq!(
        sequence("nextval", "seq", None, false, crate::relational::clock::SequenceOrigin::Call),
        Err(CoreError::Fenced)
    );
    assert_eq!(readlog.finish(), Err(CoreError::Fenced));
    assert!(matches!(
        role_image.get("fixture"),
        Err(crate::roles::RoleError::Core(CoreError::Fenced))
    ));
    sql(&mut txn, "ROLLBACK").unwrap();
    drop((txn, cursor, session, sequence, readlog, role_image, tasks));
    old.drain(WAIT).unwrap();
    assert_eq!(before, stamp(&admin));
    assert!(read_ids(&admin).is_empty());
    drop((old, admin, new_dir, old_dir));
}

#[test]
fn s3_fresh_registration_linearizes_with_stop_admission() {
    for close_first in [false, true] {
        let (_dir, admin) = fixture();
        let before_cas = Arc::new(Barrier::new(2));
        let allow_cas = Arc::new(Barrier::new(2));
        let (registered, acquired) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let registrar = thread::spawn({
            let (admin, before_cas, allow_cas) =
                (admin.clone(), before_cas.clone(), allow_cas.clone());
            move || {
                let lease = slot(&admin).register(Holder::Sql, || {
                    before_cas.wait();
                    allow_cas.wait();
                });
                registered.send(lease.is_ok()).unwrap();
                released.recv_timeout(WAIT).unwrap();
                drop(lease);
            }
        });
        before_cas.wait();
        let generation = if close_first {
            let generation = slot(&admin).stop_admission(1).unwrap();
            assert_eq!(generation.drain(Duration::ZERO), Ok(()));
            allow_cas.wait();
            assert!(!acquired.recv_timeout(WAIT).unwrap());
            generation
        } else {
            allow_cas.wait();
            assert!(acquired.recv_timeout(WAIT).unwrap());
            let generation = slot(&admin).stop_admission(1).unwrap();
            assert_eq!(
                generation.drain(Duration::ZERO),
                Err(DrainError::TimedOut {
                    active: 1,
                    holders: vec![]
                })
            );
            generation
        };
        assert!(matches!(
            slot(&admin).fresh(Holder::Sql),
            Err(CoreError::Fenced)
        ));
        release.send(()).unwrap();
        registrar.join().unwrap();
        generation.drain(WAIT).unwrap();
    }
}

#[test]
fn tag_registration_cas_has_both_close_outcomes() {
    for close_first in [false, true] {
        let (_dir, admin) = fixture();
        let tag = admin.test_v2().tag();
        let before_cas = Arc::new(Barrier::new(2));
        let allow_cas = Arc::new(Barrier::new(2));
        let (registered, acquired) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let registrar = thread::spawn({
            let (before_cas, allow_cas) = (before_cas.clone(), allow_cas.clone());
            move || {
                let lease = tag.registry.lease(Holder::Cursor, || {
                    before_cas.wait();
                    allow_cas.wait();
                });
                registered.send(lease.is_ok()).unwrap();
                released.recv_timeout(WAIT).unwrap();
                drop(lease);
            }
        });
        before_cas.wait();
        let generation = if close_first {
            let generation = slot(&admin).stop_admission(1).unwrap();
            allow_cas.wait();
            generation
        } else {
            allow_cas.wait();
            assert!(acquired.recv_timeout(WAIT).unwrap());
            slot(&admin).stop_admission(1).unwrap()
        };
        if close_first {
            assert!(!acquired.recv_timeout(WAIT).unwrap());
            generation.drain(Duration::ZERO).unwrap();
        } else {
            assert_eq!(
                generation.drain(Duration::ZERO),
                Err(DrainError::TimedOut {
                    active: 1,
                    holders: vec![(Holder::Cursor, 1)],
                })
            );
        }
        release.send(()).unwrap();
        registrar.join().unwrap();
        generation.drain(WAIT).unwrap();
    }
}

#[test]
fn s4_continuation_does_not_wait_for_slot_mutex() {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    let p = session.prepare("SELECT id FROM t").unwrap();
    sql(&mut session, "BEGIN").unwrap();
    let _generation = slot(&admin).stop_admission(1).unwrap();
    let lock = slot(&admin).state.lock().unwrap();
    let ready = Arc::new(Barrier::new(2));
    let (sent, received) = mpsc::channel();
    let worker = thread::spawn({
        let ready = ready.clone();
        move || {
            ready.wait();
            let start = Instant::now();
            let result = session.execute_prepared(&p, &[], None);
            sent.send((result, start.elapsed())).unwrap();
        }
    });
    ready.wait();
    let result = received.recv_timeout(WAIT);
    drop(lock);
    let (result, elapsed) = result.unwrap();
    assert_eq!(result, Err(CoreError::Fenced));
    assert!(
        elapsed < Duration::from_millis(100),
        "continuation took {elapsed:?}"
    );
    worker.join().unwrap();
}

#[test]
fn s5_native_publication_with_locks_does_not_deadlock_drain() {
    let (_dir, admin) = fixture();
    let owner = owner(&admin);
    let (clock_entered, entered) = mpsc::channel();
    let (release_clock, released) = mpsc::channel();
    let worker_admin = admin.clone();
    let worker = thread::spawn(move || {
        use crate::operations::once::{OnceDescription, OnceOutcome, OnceSchema, outcome};
        let payload = outcome::encode(
            &OnceDescription {
                catalog_epoch: 1,
                output: OnceSchema::Operation(vec![]),
            },
            &Ok(OnceOutcome::Operation(operations::Outcome::default())),
        )
        .unwrap();
        let mut session = worker_admin.sql_session();
        let setup = session.begin_once_engine(Some(&owner)).unwrap();
        sql(&mut session, "INSERT INTO t VALUES (1)").unwrap();
        session.publish_native_once_engine(
            Some(setup),
            owner,
            [2; 32],
            payload,
            60,
            NativePublishMode::Success,
            &mut || {
                clock_entered.send(()).unwrap();
                released.recv_timeout(WAIT).unwrap();
                Ok(NativeClockLease {
                    lower_unix_seconds: 100,
                    upper_unix_seconds: 110,
                    valid_until: Instant::now() + WAIT,
                })
            },
        )
    });
    entered.recv_timeout(WAIT).unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    assert!(matches!(
        generation.drain(Duration::ZERO),
        Err(DrainError::TimedOut { .. })
    ));
    release_clock.send(()).unwrap();
    let output = worker.join().unwrap();
    assert!(
        output.is_ok() || matches!(output, Err(NativeOnceError::Core(CoreError::Fenced))),
        "{output:?}"
    );
    let candidate = close_and_reopen(slot(&admin), generation);
    slot(&admin).install(candidate).unwrap();
}

#[test]
fn s6_drain_names_held_cursor_wakes_on_last_drop_and_refuses_install() {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    let p = session.prepare("SELECT id FROM t").unwrap();
    let cursor = session.open_cursor(&p, &[]).unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    assert!(matches!(
        admin.storage_status(),
        StorageStatus::Recovering { generation: 1, holders, .. }
            if holders == vec![(Holder::Cursor, 1)]
    ));
    assert_eq!(
        generation.drain(Duration::ZERO),
        Err(DrainError::TimedOut {
            active: 1,
            holders: vec![(Holder::Cursor, 1)],
        })
    );
    let (ready, drain) = waiting_drain(&generation);
    ready.recv_timeout(WAIT).unwrap();
    drop(cursor);
    let (result, elapsed) = drain.join().unwrap();
    assert_eq!(result, Ok(()));
    assert!(elapsed < WAIT / 2);
    let candidate = close_and_reopen(slot(&admin), generation);
    let latch = candidate.db.as_ref().unwrap().close_latch().unwrap();
    slot(&admin).begin_stopping();
    assert_eq!(slot(&admin).install(candidate), Err(CoreError::Fenced));
    latch.wait(WAIT).unwrap();
    assert_eq!(admin.storage_status(), StorageStatus::Stopping);
}

type DrainThread = thread::JoinHandle<(Result<(), DrainError>, Duration)>;
fn waiting_drain(generation: &Arc<Generation>) -> (mpsc::Receiver<()>, DrainThread) {
    let (ready, wait) = mpsc::sync_channel(1);
    let generation = Arc::clone(generation);
    let thread = thread::spawn(move || {
        let start = Instant::now();
        let result = generation.drain_with(WAIT, || {
            let _ = ready.try_send(());
        });
        (result, start.elapsed())
    });
    (wait, thread)
}

#[test]
fn stopping_interrupts_drain_with_a_live_cursor() {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    let p = session.prepare("SELECT id FROM t").unwrap();
    let cursor = session.open_cursor(&p, &[]).unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    let (ready, drain) = waiting_drain(&generation);
    ready.recv_timeout(WAIT).unwrap();
    slot(&admin).begin_stopping();
    let (result, elapsed) = drain.join().unwrap();
    assert_eq!(result, Err(DrainError::Stopping));
    assert!(elapsed < WAIT / 2);
    drop(cursor);
}

#[test]
fn lock_wait_cancels_on_flip_without_rewriting_the_error() {
    let (_dir, admin) = fixture();
    let mut owner = admin.sql_session();
    sql(&mut owner, "INSERT INTO t VALUES (1)").unwrap();
    sql(&mut owner, "BEGIN").unwrap();
    sql(&mut owner, "UPDATE t SET id = 2 WHERE id = 1").unwrap();
    let waiter = thread::spawn({
        let admin = admin.clone();
        move || sql(&mut admin.sql_session(), "UPDATE t SET id = 3 WHERE id = 1")
    });
    let start = Instant::now();
    while admin.waiting_lock_count() == 0 {
        assert!(start.elapsed() < WAIT, "no lock waiter queued");
        thread::yield_now();
    }
    let generation = slot(&admin).stop_admission(1).unwrap();
    let (sent, received) = mpsc::channel();
    let reporter = thread::spawn(move || sent.send(waiter.join().unwrap()).unwrap());
    assert_eq!(
        received.recv_timeout(WAIT).unwrap(),
        Err(CoreError::Cancelled)
    );
    reporter.join().unwrap();
    owner.rollback();
    generation.drain(WAIT).unwrap();
}

#[test]
fn buffered_cursor_refuses_a_closed_continuation() {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    let prepared = session.prepare("SELECT id + 1 FROM t").unwrap();
    let lease = session.v2_lease(Holder::Cursor).unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    assert!(matches!(
        session.buffer_cursor(&prepared, &[], None, 1024, Some(lease)),
        Err(CoreError::Fenced)
    ));
    generation.drain(WAIT).unwrap();
}

#[test]
fn stopped_rollback_checks_control_arguments() {
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "BEGIN").unwrap();
    sql(&mut s, "INSERT INTO t VALUES (5)").unwrap();
    let rollback = s.prepare("ROLLBACK").unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    let bad = Err(CoreError::Sql(crate::relational::SqlError::Parameters));
    assert_eq!(s.execute_prepared(&rollback, &[Int(1)], None), bad);
    assert_eq!(s.execute_prepared(&rollback, &[], Some(1)), bad);
    assert!(s.in_transaction());
    s.execute_prepared(&rollback, &[], None).unwrap();
    assert!(!s.in_transaction());
    generation.drain(WAIT).unwrap();
}

#[test]
fn foreign_prepared_plans_use_normal_statement_error_cleanup() {
    use crate::relational::SqlError;

    let (_dir_a, a) = fixture();
    let (_dir_b, b) = fixture();
    let mut source = a.sql_session();
    let select = source.prepare("SELECT id FROM t").unwrap();
    let (_, copy) = source.prepare_copy("COPY t(id) FROM STDIN").unwrap();
    assert_ne!(select.domain, b.authority_metadata().unwrap().0);

    let stale = CoreError::Sql(SqlError::StaleSchema);
    let mut target = b.sql_session();
    for explicit in [false, true] {
        for path in ["execute", "cursor", "copy"] {
            if explicit {
                sql(&mut target, "BEGIN").unwrap();
                sql(&mut target, "INSERT INTO t VALUES (2)").unwrap();
            }
            let result = match path {
                "execute" => target.execute_prepared(&select, &[], None).map(|_| ()),
                "cursor" => target.open_cursor(&select, &[]).map(|_| ()),
                "copy" => target.normalize_copy_rows_at(&copy, vec![vec![Int(1)]], &clock()).map(|_| ()),
                _ => unreachable!(),
            };
            let error = result.unwrap_err();
            assert_eq!(crate::sqlstate(&error), "0A000");
            assert_eq!(error, stale);
            assert_eq!(target.in_transaction(), explicit);
            assert_eq!(target.is_aborted(), explicit);
            if explicit {
                assert_eq!(sql(&mut target, "SELECT id FROM t"), Err(CoreError::Sql(SqlError::Aborted)));
                sql(&mut target, "ROLLBACK").unwrap();
            }
            assert!(sql(&mut target, "SELECT id FROM t").unwrap().rows.is_empty());
        }
    }
}

#[test]
fn prepared_plan_can_cross_independent_slots_with_the_same_domain() {
    let (dir, admin) = fixture();
    let plan = admin.sql_session().prepare("SELECT id FROM t").unwrap();
    drop(admin);

    let reopened = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(plan.domain, reopened.authority_metadata().unwrap().0);
    swap(&reopened);
    let mut target = reopened.sql_session();
    assert!(target.execute_prepared(&plan, &[], None).unwrap().rows.is_empty());
}

#[test]
fn idle_prepared_insert_and_select_cannot_cross_a_same_directory_reopen() {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    let insert = session.prepare("INSERT INTO t VALUES (42)").unwrap();
    let select = session.prepare_unnamed("SELECT id FROM t", &[]).unwrap();
    let mut task = admin.sql_session();
    task.bind_task(crate::task::context::TaskContext {
        task: crate::task::definition::TaskObjectId(1),
        name: crate::task::definition::TaskName::new("fixture").unwrap(),
        executor: "fixture".into(),
        bound_def_version: 1,
        credential_fingerprint: [0; 32],
    })
    .unwrap();
    let entry = task
        .start_task_statement(crate::task::StatementClass::Select, 0)
        .unwrap();
    swap(&admin);
    assert_eq!(entry.finish(), Err(CoreError::Fenced));
    let before = stamp(&admin);
    assert_eq!(
        session.execute_prepared(&insert, &[], None),
        Err(CoreError::Fenced)
    );
    assert_eq!(
        session.execute_prepared(&select, &[], None),
        Err(CoreError::Fenced)
    );
    assert!(matches!(
        session.open_cursor(&select, &[]),
        Err(CoreError::Fenced)
    ));
    assert!(read_ids(&admin).is_empty());
    assert_eq!(before, stamp(&admin));
    let select = session.prepare_unnamed("SELECT id FROM t", &[]).unwrap();
    assert!(
        session
            .execute_prepared(&select, &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}

fn clock() -> crate::relational::StatementClock {
    crate::relational::StatementClock {
        transaction_us: 100,
        statement_us: 100,
        unavailable: None,
        zone: None,
        wall: None,
    }
}

#[test]
fn session_sequence_and_xid_state_are_rebound_after_reopen() {
    use crate::relational::Value::Null;
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "CREATE SEQUENCE seq").unwrap();
    let clock = clock();
    let run = |s: &mut SqlSession, sql| s.execute_at(sql, &[], None, &clock);
    let scalar = |s: &mut SqlSession, sql| run(s, sql).unwrap().rows[0].values[0].clone();
    let Int(value) = scalar(&mut s, "SELECT nextval('seq')") else {
        panic!("sequence value");
    };
    assert_eq!(scalar(&mut s, "SELECT currval('seq')"), Int(value));
    let Int(xid) = scalar(&mut s, "SELECT pg_current_xact_id()") else {
        panic!("xid");
    };
    // Inject an outstanding reservation using the normal callback, but skip
    // the statement exit where it would otherwise be settled.
    let old_xact = Arc::clone(&s.xact);
    let (exposed, last) = leave_unsettled(&admin, &mut s);
    swap(&admin);
    let before = stamp(&admin);
    assert_eq!(s.settle_sequences(), Err(CoreError::Fenced));
    assert_eq!(before, stamp(&admin));
    assert!(run(&mut s, "SELECT currval('seq')").is_err());
    assert!(s.sequence_unsettled.lock().unwrap().is_empty());
    assert!(!Arc::ptr_eq(&old_xact, &s.xact));
    assert_eq!(
        scalar(&mut s, "SELECT pg_current_xact_id_if_assigned()"),
        Null
    );
    let Int(next) = scalar(&mut s, "SELECT nextval('seq')") else {
        panic!("sequence value");
    };
    let Int(next_xid) = scalar(&mut s, "SELECT pg_current_xact_id()") else {
        panic!("xid");
    };
    assert!(next > last);
    assert!(next_xid > xid);
    assert_eq!(
        exposed("nextval", "seq", None, false, crate::relational::clock::SequenceOrigin::Call),
        Err(CoreError::Fenced)
    );
}

fn leave_unsettled(
    admin: &Admin,
    s: &mut SqlSession,
) -> (crate::relational::clock::SequenceCallback, i64) {
    let lease = s.native_sequence_lease().unwrap();
    let (_, callback) = s.native_sequence_callback(lease.as_deref()).unwrap();
    drop(lease);
    let lease = admin.test_v2();
    lease.sequencer.gate_counter_acks(true);
    let result = (|| {
        for _ in 0..32 {
            let Int(value) = callback("nextval", "seq", None, false, crate::relational::clock::SequenceOrigin::Call).unwrap() else {
                panic!("sequence value");
            };
            if !s.sequence_unsettled.lock().unwrap().is_empty() {
                return Some(value);
            }
        }
        None
    })();
    lease.sequencer.gate_counter_acks(false);
    (
        callback,
        result.expect("sequence must refill with a gated acknowledgement"),
    )
}

#[test]
fn standalone_native_sequence_callback_binds_before_issuing_obligations() {
    let (_dir, admin) = fixture();
    sql(&mut admin.sql_session(), "CREATE SEQUENCE seq").unwrap();
    admin
        .sql_session()
        .execute_at("SELECT nextval('seq')", &[], None, &clock())
        .unwrap();
    let mut s = admin.sql_session();
    leave_unsettled(&admin, &mut s);
    assert!(s.sequence_generation.is_some());
    assert_eq!(s.settle_sequences(), Ok(()));
}

#[test]
fn install_carries_engine_settings_and_sync_hook_can_be_cleared() {
    let (_dir, admin) = fixture();
    let gc = Duration::from_secs(7 * 24 * 60 * 60);
    let task = crate::task::RetentionConfig {
        batch: 19,
        ..Default::default()
    };
    admin.set_gc_retention(gc).unwrap();
    admin.set_task_retention(task).unwrap();
    admin.set_task_wall_clock(Some(12345)).unwrap();
    let hook = crate::CommitSyncHook::new();
    admin.set_commit_sync_hook(Some(hook.clone()));
    let candidate = close_and_reopen(slot(&admin), slot(&admin).stop_admission(1).unwrap());
    assert_eq!(candidate.db.as_ref().unwrap().gc.test_retention_ms(), gc.as_millis() as u64);
    slot(&admin).install(candidate).unwrap();
    let db = admin.test_v2();
    assert_eq!(db.gc.test_retention_ms(), gc.as_millis() as u64);
    assert_eq!(db.test_task_retention(), task);
    assert_eq!(db.wall_now_ms(), 12345);
    drop(db);
    let worker = thread::spawn({
        let admin = admin.clone();
        move || sql(&mut admin.sql_session(), "INSERT INTO t VALUES (1)")
    });
    let entered = hook.wait_entered(WAIT);
    admin.set_commit_sync_hook(None);
    hook.release(false);
    assert!(entered);
    worker.join().unwrap().unwrap();
    assert!(!admin.test_v2().sequencer.test_has_sync_hook());
    assert_eq!(read_ids(&admin), BTreeSet::from([1]));
}

#[test]
fn concurrent_terminal_stop_has_already_stopped_admitted_writes() {
    let (_dir, admin) = fixture();
    let mut writer = admin.sql_session();
    sql(&mut writer, "BEGIN").unwrap();
    sql(&mut writer, "INSERT INTO t VALUES (1)").unwrap();
    let db = writer.v2_lease(Holder::Sql).unwrap();
    let (published, stopped) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let first = thread::spawn({
        let admin = admin.clone();
        move || {
            slot(&admin).stop_with(|| {
                published.send(()).unwrap();
                resume.recv_timeout(WAIT).unwrap();
            })
        }
    });
    stopped.recv_timeout(WAIT).unwrap();
    slot(&admin).begin_stopping();
    assert!(!db.sequencer.is_accepting());
    // This publication already holds a g lease. Reach the sequencer rather
    // than getting an early refusal from the statement admission check.
    let inner = admin.inner.clone();
    let mut ticket = inner.enter_sql().unwrap();
    let result = writer.commit_v2_transaction(&db, &mut ticket);
    drop(ticket);
    release.send(()).unwrap();
    first.join().unwrap();
    assert_eq!(result, Err(CoreError::Fenced));
}

#[test]
fn close_proof_waits_for_rocksdb_snapshot_and_can_be_retried() {
    let (_dir, admin) = fixture();
    let snapshot = admin.test_v2().kv.snapshot().unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    generation.drain(WAIT).unwrap();
    let closing = generation.close_drained().unwrap();
    assert!(closing.wait(Duration::ZERO).is_err());
    drop(snapshot);
    let proof = closing.wait(WAIT).unwrap();
    let candidate = slot(&admin).reopen_candidate(&proof).unwrap();
    slot(&admin).install(candidate).unwrap();
}

#[test]
fn native_fence_preserves_write_fence_status_and_inner_refusals() {
    let (_dir, admin) = fixture();
    let db = admin.test_v2();
    db.fence_native_commit(&admin.inner.fenced);
    assert!(!db.kv.is_fenced());
    assert!(admin.write_fence_status().is_ok());
    assert_eq!(
        sql(&mut admin.sql_session(), "INSERT INTO t VALUES (1)"),
        Err(CoreError::Fenced)
    );
}

#[test]
fn production_stop_leaves_reads_cursors_and_rollback_available() {
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "INSERT INTO t VALUES (1)").unwrap();
    let p = s.prepare("SELECT id FROM t").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    sql(&mut s, "BEGIN").unwrap();
    admin.stop_submissions();
    assert_eq!(sql(&mut s, "SELECT id FROM t").unwrap().rows.len(), 1);
    assert_eq!(cursor.fetch(1, 64).unwrap().len(), 1);
    sql(&mut s, "ROLLBACK").unwrap();
    assert_eq!(
        sql(&mut s, "INSERT INTO t VALUES (2)"),
        Err(CoreError::Fenced)
    );
}

fn copy_directory(dir: &Directory) -> Directory {
    let copy = Directory::new();
    std::fs::create_dir(&copy.0).unwrap();
    for entry in std::fs::read_dir(&dir.0).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), copy.0.join(entry.file_name())).unwrap();
        }
    }
    copy
}

fn directory_files(dir: &Directory) -> std::collections::BTreeMap<std::ffi::OsString, Vec<u8>> {
    std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| (entry.file_name(), std::fs::read(entry.path()).unwrap()))
        .collect()
}

#[test]
fn create_and_open_keep_the_supplied_recovery_path() {
    let dir = Directory::new();
    std::fs::create_dir(&dir.0).unwrap();
    let path = dir.0.join(".").join("db");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let check_path = |admin: &Admin| {
        let source = slot(admin).recovery_source.as_ref().unwrap();
        assert_eq!(source.path.as_os_str(), path.as_os_str());
        assert!(source.identity.matches(&path));
    };
    check_path(&admin);
    let latch = admin.test_v2().close_latch().unwrap();
    drop(admin);
    latch.wait(WAIT).unwrap();
    check_path(&Admin::open(&path, Limits::default()).unwrap());
}

#[test]
fn slot_drop_allows_directory_reuse_after_close_latch() {
    let (dir, admin) = fixture();
    let latch = admin.test_v2().close_latch().unwrap();
    thread::scope(|scope| {
        scope.spawn(move || drop(admin));
        latch.wait(WAIT).unwrap();
        std::fs::remove_dir_all(&dir.0).unwrap();
        Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    });
}

#[test]
fn original_close_proof_cannot_install_an_older_same_domain_copy() {
    let (dir, admin) = fixture();
    let db = admin.test_v2();
    let domain = db.meta.domain;
    let latch = db.close_latch().unwrap();
    drop(db);
    drop(admin);
    latch.wait(WAIT).unwrap();

    let older = copy_directory(&dir);

    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    sql(&mut admin.sql_session(), "INSERT INTO t VALUES (31)").unwrap();
    let older_admin = Admin::open(&older.0, Limits::default()).unwrap();
    assert_eq!(older_admin.test_v2().meta.domain, domain);
    assert!(read_ids(&older_admin).is_empty());

    let generation = slot(&admin).stop_admission(1).unwrap();
    generation.drain(WAIT).unwrap();
    let proof = generation.close_drained().unwrap().wait(WAIT).unwrap();
    let older_slot = slot(&older_admin);
    assert!(
        matches!(older_slot.reopen_candidate(&proof), Err(error) if error.error == CoreError::Fenced)
    );
    // A normal open cannot construct a candidate. Even the older store's
    // separately validated recovery candidate cannot be installed here.
    let wrong = close_and_reopen(older_slot, older_slot.stop_admission(1).unwrap());
    assert!(wrong.db.as_ref().unwrap().sequencer.visible_ts() < proof.ack_hw);
    let latch = wrong.db.as_ref().unwrap().close_latch().unwrap();
    assert_eq!(slot(&admin).install(wrong), Err(CoreError::Fenced));
    latch.wait(WAIT).unwrap();

    let displaced = Directory::new();
    std::fs::rename(&dir.0, &displaced.0).unwrap();
    std::fs::rename(&older.0, &dir.0).unwrap();
    assert!(matches!(slot(&admin).reopen_candidate(&proof),
        Err(error) if error.error == CoreError::Fenced));
    std::fs::rename(&dir.0, &older.0).unwrap();
    std::fs::rename(&displaced.0, &dir.0).unwrap();

    let candidate = slot(&admin).reopen_candidate(&proof).unwrap();
    slot(&admin).install(candidate).unwrap();
    assert_eq!(read_ids(&admin), BTreeSet::from([31]));
}

#[test]
fn advanced_same_domain_copy_is_refused_before_recovery_writes() {
    let (dir, admin) = fixture();
    let db = admin.test_v2();
    let domain = db.meta.domain;
    let latch = db.close_latch().unwrap();
    drop(db);
    drop(admin);
    latch.wait(WAIT).unwrap();
    let copy = copy_directory(&dir);

    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    sql(&mut admin.sql_session(), "INSERT INTO t VALUES (31)").unwrap();
    let copy_admin = Admin::open(&copy.0, Limits::default()).unwrap();
    assert_eq!(copy_admin.test_v2().meta.domain, domain);
    let copy_db = copy_admin.test_v2();
    let copy_latch = copy_db.close_latch().unwrap();
    let advanced = (32..100).any(|id| {
        sql(&mut copy_admin.sql_session(), &format!("INSERT INTO t VALUES ({id})")).unwrap();
        copy_db.sequencer.visible_ts() > admin.test_v2().sequencer.visible_ts()
    });
    assert!(advanced);
    assert!(!read_ids(&copy_admin).contains(&31));
    drop(copy_db);
    drop(copy_admin);
    copy_latch.wait(WAIT).unwrap();

    let generation = slot(&admin).stop_admission(1).unwrap();
    generation.drain(WAIT).unwrap();
    let proof = generation.close_drained().unwrap().wait(WAIT).unwrap();
    let displaced = Directory::new();
    std::fs::rename(&dir.0, &displaced.0).unwrap();
    std::fs::rename(&copy.0, &dir.0).unwrap();
    let before = directory_files(&dir);
    assert!(matches!(slot(&admin).reopen_candidate(&proof),
        Err(error) if error.error == CoreError::Fenced));
    assert_eq!(directory_files(&dir), before);
    assert!(matches!(slot(&admin).fresh(Holder::Sql), Err(CoreError::Fenced)));

    std::fs::rename(&dir.0, &copy.0).unwrap();
    std::fs::rename(&displaced.0, &dir.0).unwrap();
    let candidate = slot(&admin).reopen_candidate(&proof).unwrap();
    slot(&admin).install(candidate).unwrap();
    assert_eq!(read_ids(&admin), BTreeSet::from([31]));
}

#[test]
fn authorization_versions_refuse_a_registry_closed_during_the_observation() {
    let (_dir, admin) = fixture();
    let slot = slot(&admin);
    let versions = slot.authorization_versions(|_, catalog| catalog.read());
    assert!(versions.is_some());

    let raced = slot.authorization_versions(|_, catalog| {
        let candidate = close_and_reopen(slot, slot.stop_admission(1).unwrap());
        slot.install(candidate).unwrap();
        catalog.read()
    });
    assert_eq!(raced, None);
    assert!(slot.authorization_versions(|_, catalog| catalog.read()).is_some());
}

#[test]
fn recovery_maintenance_starts_after_publication() {
    let (_dir, admin) = fixture();
    let slot = slot(&admin);
    let mut candidate = close_and_reopen(slot, slot.stop_admission(1).unwrap());
    assert!(!candidate.db.as_ref().unwrap().test_maintenance_running());
    slot.install_with(
        &mut candidate,
        |db| {
            // The final health check passed, but this generation is still private.
            assert!(!db.test_maintenance_running());
            assert!(matches!(slot.fresh(Holder::Sql), Err(CoreError::Fenced)));
        },
        || {
            let db = slot.fresh(Holder::Sql).unwrap();
            assert_eq!(db.id(), 2);
            assert!(!db.test_maintenance_running());
        },
    )
    .unwrap();
    let db = slot.fresh(Holder::Sql).unwrap();
    assert_eq!(db.id(), 2);
    assert!(db.test_maintenance_running());
}

#[test]
fn fenced_candidate_is_refused_and_shutdown_before_install_returns() {
    let (_dir, admin) = fixture();
    let candidate = close_and_reopen(slot(&admin), slot(&admin).stop_admission(1).unwrap());
    let db = candidate.db.as_ref().unwrap();
    let sequencer = Arc::clone(&db.sequencer);
    let latch = db.close_latch().unwrap();
    assert!(sequencer.is_accepting());
    assert!(!db.test_maintenance_running());
    db.kv.fence();
    assert_eq!(slot(&admin).install(candidate), Err(CoreError::Fenced));
    assert!(matches!(slot(&admin).fresh(Holder::Sql), Err(CoreError::Fenced)));
    assert!(sequencer.test_worker_joined());
    drop(sequencer);
    latch.wait(WAIT).unwrap();
}

#[test]
fn uninstalled_candidate_joins_its_worker_on_drop() {
    let (_dir, admin) = fixture();
    let candidate = close_and_reopen(slot(&admin), slot(&admin).stop_admission(1).unwrap());
    let db = candidate.db.as_ref().unwrap();
    let sequencer = Arc::clone(&db.sequencer);
    let latch = db.close_latch().unwrap();
    assert!(sequencer.is_accepting());
    assert!(!db.test_maintenance_running());
    drop(candidate);
    assert!(sequencer.test_worker_joined());
    drop(sequencer);
    latch.wait(WAIT).unwrap();
}

#[test]
fn stale_copy_plan_aborts_an_explicit_transaction() {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    sql(&mut session, "CREATE SEQUENCE seq").unwrap();
    let (_, stale) = session.prepare_copy("COPY t(id) FROM STDIN").unwrap();
    swap(&admin);

    sql(&mut session, "BEGIN").unwrap();
    sql(&mut session, "INSERT INTO t VALUES (1)").unwrap();
    session.execute_at("SELECT nextval('seq')", &[], None, &clock()).unwrap();
    leave_unsettled(&admin, &mut session);
    let pending = session.sequence_unsettled.lock().unwrap().len();
    assert!(pending > 0);
    assert!(matches!(
        session.normalize_copy_rows_at(&stale, vec![vec![Int(2)]], &clock()),
        Err(CoreError::Fenced)
    ));
    assert_eq!(session.sequence_unsettled.lock().unwrap().len(), pending);
    assert!(session.in_transaction() && session.is_aborted());
    assert_eq!(
        sql(&mut session, "SELECT id FROM t"),
        Err(CoreError::Sql(crate::relational::SqlError::Aborted))
    );
    assert_eq!(session.sequence_unsettled.lock().unwrap().len(), pending);
    sql(&mut session, "ROLLBACK").unwrap();
    assert!(session.sequence_unsettled.lock().unwrap().is_empty());
    assert!(read_ids(&admin).is_empty());
}

#[test]
fn standalone_copy_rollback_settles_a_never_bound_session() {
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut admin.sql_session(), "CREATE SEQUENCE seq").unwrap();
    sql(
        &mut admin.sql_session(),
        "CREATE TABLE c(id BIGINT PRIMARY KEY DEFAULT nextval('seq'), n BIGINT)",
    )
    .unwrap();
    let (_, copy) = s.prepare_copy("COPY c(n) FROM STDIN").unwrap();
    assert!(s.sequence_generation.is_none());
    let clock = clock();
    s.normalize_copy_rows_at(&copy, vec![vec![Int(1)]], &clock)
        .unwrap();
    let db = admin.test_v2();
    db.sequencer.gate_counter_writes(true);
    for _ in 0..32 {
        s.normalize_copy_rows_at(&copy, vec![vec![Int(1)]], &clock)
            .unwrap();
        if !s.sequence_unsettled.lock().unwrap().is_empty() {
            break;
        }
    }
    db.sequencer.gate_counter_writes(false);
    assert!(!s.sequence_unsettled.lock().unwrap().is_empty());
    s.rollback();
    assert!(s.sequence_unsettled.lock().unwrap().is_empty());
}

#[test]
fn copy_with_failed_old_reservation_cannot_regress_coverage_after_reopen() {
    use crate::storage::v2_sql::sql_sequences;
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "CREATE SEQUENCE seq").unwrap();
    sql(
        &mut s,
        "CREATE TABLE c(id BIGINT PRIMARY KEY DEFAULT nextval('seq'), n BIGINT)",
    )
    .unwrap();
    let (_, old_copy) = s.prepare_copy("COPY c(n) FROM STDIN").unwrap();
    let clock = clock();
    let normalize = |session: &mut SqlSession, p: &_| {
        session.normalize_copy_rows_at(p, vec![vec![Int(1)]], &clock)
    };
    normalize(&mut s, &old_copy).unwrap();
    let db = admin.test_v2();
    let sql_seq = db.catalog().current_local_view().unwrap();
    let (&id, def) = sql_seq
        .sequences
        .iter()
        .find(|(_, def)| def.name == "seq")
        .unwrap();
    let key = super::super::engine::catalog_codec::counter_key(id, def.incarnation);
    let before_reservation = db.kv.get(Keyspace::Seq, &key).unwrap().unwrap();

    db.sequencer.gate_counter_writes(true);
    for _ in 0..40 {
        normalize(&mut s, &old_copy).unwrap();
    }
    // Admit recovery first. Fence the held counter write and its settlement
    // on the existing lease, without attempting a reopen after a slot stop.
    let generation = slot(&admin).stop_admission(1).unwrap();
    db.sequencer.stop_submissions();
    db.sequencer.gate_counter_writes(false);
    assert_eq!(
        sql_sequences::settle(&db, &s.sequence_unsettled),
        Err(CoreError::Fenced)
    );
    assert_eq!(
        db.kv.get(Keyspace::Seq, &key).unwrap().unwrap(),
        before_reservation
    );
    let pending_on_old = s.sequence_unsettled.lock().unwrap().len();
    assert!(pending_on_old > 0);
    let old_obligations = Arc::clone(&s.sequence_unsettled);
    let old_values = Arc::clone(&s.sequence_values);
    let old_xact = Arc::clone(&s.xact);
    drop(db);

    let candidate = close_and_reopen(slot(&admin), generation);
    slot(&admin).install(candidate).unwrap();
    let mut advance = admin.sql_session();
    advance
        .execute_at("SELECT setval('seq', 1000)", &[], None, &clock)
        .unwrap();
    let db = admin.test_v2();
    let covered = db.kv.get(Keyspace::Seq, &key).unwrap().unwrap();
    assert_ne!(covered, before_reservation);
    assert_eq!(
        sql_sequences::settle(&db, &old_obligations),
        Err(CoreError::Fenced)
    );
    assert!(matches!(
        normalize(&mut s, &old_copy),
        Err(CoreError::Fenced)
    ));
    assert_eq!(db.kv.get(Keyspace::Seq, &key).unwrap().unwrap(), covered);
    assert!(Arc::ptr_eq(&s.sequence_unsettled, &old_obligations));
    assert_eq!(old_obligations.lock().unwrap().len(), pending_on_old);

    let (_, fresh_copy) = s.prepare_copy("COPY c(n) FROM STDIN").unwrap();
    let (_, rows) = normalize(&mut s, &fresh_copy).unwrap();
    assert!(!Arc::ptr_eq(&s.sequence_unsettled, &old_obligations));
    assert!(!Arc::ptr_eq(&s.sequence_values, &old_values));
    assert!(!Arc::ptr_eq(&s.xact, &old_xact));
    s.rollback();
    assert!(matches!(rows[0][0], Int(value) if value > 1000));
    let coverage = db.kv.get(Keyspace::Seq, &key).unwrap().unwrap();
    assert!(
        super::super::engine::catalog_codec::decode_counter(&key, &coverage, def)
            .unwrap()
            .log_hw
            > 1000
    );
}

#[test]
fn prepared_transaction_controls_work_after_reopen() {
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    let controls = [
        "BEGIN",
        "SAVEPOINT a",
        "ROLLBACK TO a",
        "RELEASE a",
        "COMMIT",
        "ROLLBACK",
    ]
    .map(|text| s.prepare(text).unwrap());
    swap(&admin);
    let mut control = |index: usize| s.execute_prepared(&controls[index], &[], None).unwrap();
    for index in [0, 1, 2, 3, 4, 0, 5] {
        control(index);
    }
    assert!(!s.in_transaction());
}

#[test]
fn clocked_privilege_lookup_refuses_a_closed_statement_generation() {
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "INSERT INTO t VALUES (1)").unwrap();
    let (sent, get) = mpsc::channel();
    let callback = Arc::new({
        let admin = admin.clone();
        move |_: &str, _: &[crate::relational::Value], metadata: &crate::PrivilegeMetadata| {
            let old = slot(&admin).stop_admission(1).unwrap();
            sent.send(old).unwrap();
            let roles = metadata.role_image().map_err(|error| match error {
                crate::roles::RoleError::Core(error) => error,
                other => panic!("unexpected role error: {other:?}"),
            })?;
            Ok(Some(roles.get("pg_monitor").unwrap().is_some()))
        }
    });
    let start = Instant::now();
    assert_eq!(
        s.with_privilege_callback(callback, |s| {
            s.execute_at(
                "SELECT pg_has_role('pg_monitor','MEMBER') FROM t",
                &[],
                None,
                &clock(),
            )
        }),
        Err(CoreError::Fenced)
    );
    assert!(start.elapsed() < WAIT / 2);
    get.recv_timeout(WAIT).unwrap().drain(WAIT).unwrap();
}

#[test]
fn native_once_setup_preserves_client_cancel_during_closure() {
    let (_dir, admin) = fixture();
    let mut owner = admin.sql_session();
    let setup = owner.begin_once_engine(None).unwrap();
    let cancel = crate::cancellation::Cancellation::default();
    let (lock_returned, wait_for_error) = mpsc::channel();
    let (closed, wait_for_close) = mpsc::channel();
    let worker = thread::spawn({
        let admin = admin.clone();
        let cancel = cancel.clone();
        move || {
            super::super::once_session::AFTER_ONCE_LOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    lock_returned.send(()).unwrap();
                    wait_for_close.recv_timeout(WAIT).unwrap();
                }));
            });
            let mut waiter = admin.sql_session();
            cancel.scope(|| waiter.begin_once_engine(None)).map(|_| ())
        }
    });
    let start = Instant::now();
    while admin.waiting_lock_count() == 0 {
        assert!(start.elapsed() < WAIT, "native lock wait did not queue");
        thread::yield_now();
    }
    assert!(cancel.cancel());
    wait_for_error.recv_timeout(WAIT).unwrap();
    let generation = slot(&admin).stop_admission(1).unwrap();
    closed.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), Err(CoreError::Cancelled));
    drop(setup);
    owner.rollback();
    generation.drain(WAIT).unwrap();
}

#[test]
fn task_catalog_preserves_the_native_only_fence_refusal_set() {
    use crate::{
        roles::NewRole,
        task::definition::{
            DisclosureCaps, MutationCaps, NewTask, PredicateText, TaskPrivileges, WallClockMs,
        },
    };

    let (_dir, admin) = fixture();
    let roles = admin.roles();
    let issuer = roles
        .create(NewRole {
            name: "issuer".into(),
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    roles
        .create(NewRole {
            name: "runner".into(),
            ..Default::default()
        })
        .unwrap();
    roles.set_task_only("runner", true).unwrap();
    roles
        .create(NewRole {
            name: "grantee".into(),
            ..Default::default()
        })
        .unwrap();

    let new_task = |name: &str| NewTask {
        name: name.into(),
        executor: "runner".into(),
        approver: None,
        schema: None,
        table: "t".into(),
        visible_rows: PredicateText("id > 0".into()),
        readable: vec!["id".into()],
        masked: Vec::new(),
        untrusted_text: Vec::new(),
        allowed_write: None,
        mutation_caps: MutationCaps {
            distinct_rows: 0,
            transitions: 0,
        },
        disclosure_caps: DisclosureCaps {
            rows_read: 10,
            rows_returned: 10,
            bytes_returned: 4096,
        },
        limits: Default::default(),
        approval_threshold: 0,
        expires: "2035-01-01 00:00:00+00".into(),
    };
    let tasks = admin.tasks();
    let now = WallClockMs(0);
    let task = tasks.create(&issuer, new_task("existing"), now).unwrap();
    let for_purge = tasks.create(&issuer, new_task("for_purge"), now).unwrap();

    use crate::operations::once::{OnceDescription, OnceOutcome, OnceSchema, outcome};
    let payload = outcome::encode(
        &OnceDescription {
            catalog_epoch: 1,
            output: OnceSchema::Operation(vec![]),
        },
        &Ok(OnceOutcome::Operation(operations::Outcome::default())),
    )
    .unwrap();
    crate::task::catalog::TEST_BEFORE_TASK_DRAFT_ACL.set(Some(Box::new({
        let admin = admin.clone();
        move || {
            let owner = owner(&admin);
            let mut session = admin.sql_session();
            let setup = session.begin_once_engine(Some(&owner)).unwrap();
            admin.set_crash_point(super::super::CrashPoint::ErrorAfterCommit).unwrap();
            let result = session.publish_native_once_engine(
                Some(setup),
                owner,
                [2; 32],
                payload,
                60,
                NativePublishMode::Success,
                &mut || {
                    Ok(NativeClockLease {
                        lower_unix_seconds: 100,
                        upper_unix_seconds: 110,
                        valid_until: Instant::now() + WAIT,
                    })
                },
            );
            assert!(
                matches!(result, Err(NativeOnceError::Core(CoreError::Storage(message)))
                if message == "injected lost commit response")
            );
        }
    })));
    let fence_state = crate::sqlstate(&CoreError::Fenced);
    assert_eq!(
        tasks.draft(&issuer, &new_task("draft"), now).unwrap_err().sqlstate,
        fence_state,
    );
    assert!(!admin.test_v2().kv.is_fenced());
    let before = stamp(&admin);
    assert_eq!(
        tasks
            .grant(
                &issuer,
                "existing",
                "grantee",
                TaskPrivileges::INSPECT,
                true
            )
            .unwrap_err()
            .sqlstate,
        fence_state,
    );
    assert_eq!(
        tasks
            .create(&issuer, new_task("new"), now)
            .unwrap_err()
            .sqlstate,
        fence_state
    );
    assert!(admin.write_fence_status().is_ok());
    assert_eq!(stamp(&admin), before);
    assert_eq!(tasks.get("existing").unwrap().unwrap().as_ref(), &task);
    assert!(tasks.get("new").unwrap().is_none());

    let credential = tasks.issue_credential(&issuer, "existing", None, now).unwrap();
    tasks.revoke_credential(&issuer, credential.id).unwrap();
    assert!(!tasks.view().unwrap().task_credentials.contains_key(&credential.id));
    assert!(tasks.revoke(&issuer, "existing").unwrap().revoked);
    tasks.purge(for_purge.id).unwrap();
    assert!(tasks.get("for_purge").unwrap().is_none());
}

#[test]
fn embedded_streaming_cursor_outlives_the_session_and_admin() {
    let (dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "INSERT INTO t VALUES (1), (2)").unwrap();
    let p = s.prepare("SELECT id FROM t").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    drop(s);
    drop(admin);
    assert_eq!(cursor.fetch(1, 64).unwrap()[0].values, [Int(1)]);
    assert_eq!(cursor.fetch(1, 64).unwrap()[0].values, [Int(2)]);
    drop(cursor);
    Admin::open(dir.0.as_path(), Limits::default()).unwrap();
}

#[test]
fn embedded_materialized_cursor_releases_the_directory_and_keeps_its_rows() {
    let (dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "INSERT INTO t VALUES (1)").unwrap();
    let p = s.prepare("SELECT count(*) FROM t").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    drop(s);
    drop(admin);
    let reopened = Admin::open(dir.0.as_path(), Limits::default()).unwrap();
    sql(&mut reopened.sql_session(), "INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(cursor.fetch(1, 64).unwrap()[0].values, [Int(1)]);
}

#[test]
fn a_materialized_cursor_is_invalidated_by_recovery_without_blocking_drain() {
    let (_dir, admin) = fixture();
    let mut s = admin.sql_session();
    sql(&mut s, "INSERT INTO t VALUES (1)").unwrap();
    let p = s.prepare("SELECT count(*) FROM t").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    swap(&admin);
    assert!(matches!(cursor.fetch(1, 64), Err(CoreError::Fenced)));
}

/// Exercises the same core COMMIT entry point pgwire calls on Sync when
/// Execute has opened an implicit transaction block.
fn parked_commit_holds_the_generation(implicit_sync: bool, fail_sync: bool) {
    let (_dir, admin) = fixture();
    let mut session = admin.sql_session();
    let clock = clock();
    if implicit_sync {
        session.control_block_at(false, &clock).unwrap();
        sql(&mut session, "INSERT INTO t VALUES (31)").unwrap();
    }
    let hook = crate::CommitSyncHook::new();
    admin.set_commit_sync_hook(Some(hook.clone()));
    let (released, release_seen) = mpsc::channel();
    let client = thread::spawn(move || {
        let context = crate::Cancellation::default();
        let _registration = context.register_durability_wait(move |_, release_admission| {
            release_admission();
            released.send(()).unwrap();
            Some(Box::new(()))
        });
        context.scope(|| {
            if implicit_sync {
                session.control_block_at(true, &clock)
            } else {
                sql(&mut session, "INSERT INTO t VALUES (31)").map(drop)
            }
        })
    });
    // Release the hook even if the commit did not take the expected path.
    let at_sync = hook.wait_entered(WAIT);
    let ticket_released = release_seen.recv_timeout(WAIT);
    let sql_operations = admin.inner.sql_operations.load(Ordering::Acquire);
    let old = slot(&admin).stop_admission(1).unwrap();
    let parked = old.drain(Duration::from_millis(20));
    hook.release(fail_sync);
    let reply = client.join().unwrap();

    assert!(at_sync);
    ticket_released.unwrap();
    assert_eq!(sql_operations, 0);
    assert!(matches!(parked, Err(DrainError::TimedOut { active: 1, .. })));
    old.drain(WAIT).unwrap();
    admin.set_commit_sync_hook(None);
    let candidate = close_and_reopen(slot(&admin), old);
    slot(&admin).install(candidate).unwrap();
    if fail_sync {
        assert_eq!(reply, Err(CoreError::Storage("engine commit outcome unknown".into())));
        assert!(read_ids(&admin).is_empty());
    } else {
        reply.unwrap();
        assert_eq!(read_ids(&admin), BTreeSet::from([31]));
    }
}

#[test]
fn parked_autocommit_pins_the_generation_through_successful_and_failed_sync() {
    for fail_sync in [false, true] {
        parked_commit_holds_the_generation(false, fail_sync);
    }
}

#[test]
fn parked_implicit_commit_pins_the_generation_through_successful_and_failed_sync() {
    for fail_sync in [false, true] {
        parked_commit_holds_the_generation(true, fail_sync);
    }
}

#[test]
fn typed_view_create_returns_its_definition_if_the_slot_closes_during_commit() {
    use crate::relational::{ReadExpr, ReadProjection, SingleSourceRead};
    let (_dir, admin) = fixture();
    let hook = crate::CommitSyncHook::new();
    admin.set_commit_sync_hook(Some(hook.clone()));
    let ddl = thread::spawn({
        let admin = admin.clone();
        move || {
            admin.sql_session().execute_typed_view(
                "v".into(),
                Some(
                    SingleSourceRead {
                        table: "t".into(),
                        projection: vec![ReadProjection {
                            expression: ReadExpr::Column("id".into()),
                            alias: None,
                        }],
                        filter: None,
                    }
                    .into(),
                ),
                false,
            )
        }
    });
    let entered = hook.wait_entered(WAIT);
    if !entered {
        hook.release(false);
    }
    assert!(entered);
    let old = slot(&admin).stop_admission(1).unwrap();
    assert!(matches!(
        old.drain(Duration::ZERO),
        Err(DrainError::TimedOut { .. })
    ));
    hook.release(false);
    let table = ddl.join().unwrap().unwrap();
    assert_eq!(table.name, "v");
    assert_eq!(table.columns[0].name, "id");
    admin.set_commit_sync_hook(None);
    let candidate = close_and_reopen(slot(&admin), old);
    slot(&admin).install(candidate).unwrap();
    let preimage = admin
        .sql_session()
        .execute_typed_view("v".into(), None, false)
        .unwrap();
    assert_eq!(preimage, table);
}

#[test]
fn close_drained_joins_workers_when_terminal_stop_wins() {
    let (_dir, admin) = fixture();
    let generation = slot(&admin).stop_admission(1).unwrap();
    generation.drain(WAIT).unwrap();
    let latch = generation.db.close_latch().unwrap();
    let observer = Arc::clone(&generation);
    slot(&admin).begin_stopping();
    assert!(matches!(generation.close_drained(), Err(CoreError::Fenced)));
    // This owner prevents implicit V2Db drop from hiding a missed shutdown.
    assert!(!observer.db.test_maintenance_running());
    drop(observer);
    assert!(latch.wait(Duration::ZERO).is_err());
    drop(admin);
    latch.wait(WAIT).unwrap();
}

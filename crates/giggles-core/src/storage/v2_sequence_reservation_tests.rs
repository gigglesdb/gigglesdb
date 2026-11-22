//! F2 durable reservation coverage. Kept separate from the SQL oracle tripwires.
use super::*;
use crate::relational::{SqlResult, StatementClock, Value};
use std::{collections::BTreeSet, path::PathBuf, process::Command, thread};

fn engines() -> Vec<Engine> {
    let engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    let engines = [engines, vec![Engine::V2]].concat();
    engines
}
fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}
fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute_at(sql, &[], None, &clock())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}
fn scalar(session: &mut SqlSession, sql: &str) -> i64 {
    let result = run(session, sql);
    match result.rows[0].values[0] {
        Value::Int(value) => value,
        ref value => panic!("expected integer from {sql}: {value:?}"),
    }
}
struct Fixture {
    path: PathBuf,
    admin: Option<Admin>,
}
impl Fixture {
    fn new(engine: Engine) -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("f2-seq-{:032x}", u128::from_be_bytes(nonce)));
        Self {
            path: path.clone(),
            admin: Some(Admin::create_engine(path, Limits::default(), engine).unwrap()),
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
    fn reopen(&mut self) {
        self.admin.take();
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

#[test]
fn bounds_setval_restart_and_pending_across_reopen() {
    for engine in engines() {
        let mut f = Fixture::new(engine);
        let mut s = f.session();
        run(
            &mut s,
            "CREATE SEQUENCE descending INCREMENT -2 START -1 MINVALUE -999 MAXVALUE -1",
        );
        for expected in [-1, -3, -5] {
            assert_eq!(scalar(&mut s, "SELECT nextval('descending')"), expected);
        }
        run(
            &mut s,
            "CREATE SEQUENCE cycling MINVALUE 1 MAXVALUE 3 CYCLE",
        );
        run(
            &mut s,
            "CREATE SEQUENCE negcycle INCREMENT -2 MINVALUE -5 MAXVALUE -1 CYCLE",
        );
        for expected in [1, 2, 3, 1, 2, 3] {
            assert_eq!(scalar(&mut s, "SELECT nextval('cycling')"), expected);
        }
        for expected in [-1, -3, -5, -1, -3, -5] {
            assert_eq!(scalar(&mut s, "SELECT nextval('negcycle')"), expected);
        }
        run(
            &mut s,
            "CREATE SEQUENCE bounded MINVALUE 1 MAXVALUE 3 NO CYCLE",
        );
        for expected in [1, 2, 3] {
            assert_eq!(scalar(&mut s, "SELECT nextval('bounded')"), expected);
        }
        run(&mut s, "CREATE SEQUENCE set_called");
        assert_eq!(scalar(&mut s, "SELECT setval('set_called',500,true)"), 500);
        run(&mut s, "CREATE SEQUENCE set_uncalled");
        assert_eq!(
            scalar(&mut s, "SELECT setval('set_uncalled',600,false)"),
            600
        );
        run(&mut s, "CREATE SEQUENCE restarted START 700");
        run(&mut s, "ALTER SEQUENCE restarted RESTART");
        run(&mut s, "BEGIN");
        run(&mut s, "CREATE SEQUENCE pending START 20 INCREMENT 2");
        assert_eq!(scalar(&mut s, "SELECT nextval('pending')"), 20);
        assert_eq!(scalar(&mut s, "SELECT nextval('pending')"), 22);
        run(&mut s, "COMMIT");
        assert_eq!(scalar(&mut s, "SELECT nextval('pending')"), 24);
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE set_called RESTART");
        assert_eq!(scalar(&mut s, "SELECT nextval('set_called')"), 1);
        run(&mut s, "ROLLBACK");
        drop(s);
        f.reopen();
        let mut s = f.session();
        assert!(scalar(&mut s, "SELECT nextval('descending')") < -5);
        assert_eq!(scalar(&mut s, "SELECT nextval('cycling')"), 1);
        assert_eq!(scalar(&mut s, "SELECT nextval('negcycle')"), -1);
        assert_eq!(
            s.execute_at("SELECT nextval('bounded')", &[], None, &clock()),
            Err(CoreError::Sql(SqlError::SequenceLimit))
        );
        assert_eq!(scalar(&mut s, "SELECT nextval('set_called')"), 501);
        assert_eq!(scalar(&mut s, "SELECT nextval('set_uncalled')"), 600);
        assert_eq!(scalar(&mut s, "SELECT nextval('restarted')"), 700);
        assert!(scalar(&mut s, "SELECT nextval('pending')") > 24);
        assert_eq!(scalar(&mut s, "SELECT currval('restarted')"), 700);
        assert_eq!(
            s.execute_at("SELECT lastval()", &[], None, &clock()),
            Err(CoreError::Sql(SqlError::Unsupported))
        );
    }
}

#[test]
fn rollback_and_concurrent_nextval_do_not_reuse_values() {
    for engine in engines() {
        let f = Fixture::new(engine);
        let mut s = f.session();
        run(&mut s, "CREATE SEQUENCE shared_ids");
        run(&mut s, "BEGIN");
        let rolled_back = scalar(&mut s, "SELECT nextval('shared_ids')");
        run(&mut s, "ROLLBACK");
        assert_eq!(
            scalar(&mut s, "SELECT nextval('shared_ids')"),
            rolled_back + 1
        );
        let workers: Vec<_> = (0..6)
            .map(|_| {
                let admin = f.admin.as_ref().unwrap().clone();
                thread::spawn(move || {
                    let mut s = admin.sql_session();
                    (0..40)
                        .map(|_| scalar(&mut s, "SELECT nextval('shared_ids')"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let values: Vec<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(values.len(), 240);
        assert_eq!(values.into_iter().collect::<BTreeSet<_>>().len(), 240);
    }
}

#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("F2_SEQUENCE_CRASH_PATH") else {
        return;
    };
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for expected in 1..=70 {
        assert_eq!(scalar(&mut s, "SELECT nextval('direct_ids')"), expected);
        assert_eq!(
            scalar(
                &mut s,
                "INSERT INTO serial_rows(payload) VALUES (1) RETURNING id"
            ),
            expected
        );
    }
    run(&mut s, "BEGIN");
    assert_eq!(scalar(&mut s, "SELECT nextval('direct_ids')"), 71);
    assert_eq!(
        scalar(
            &mut s,
            "INSERT INTO serial_rows(payload) VALUES (2) RETURNING id"
        ),
        71
    );
    run(&mut s, "ROLLBACK");
    // Exercise many row-version reservations without growing the fixture.
    for n in 1..=70 {
        run(
            &mut s,
            &format!("UPDATE serial_rows SET payload={n} WHERE id=1"),
        );
    }
    crate::storage::crash_now();
}

#[test]
fn abrupt_crash_preserves_serial_direct_values_and_row_visibility() {
    for engine in engines() {
        let mut f = Fixture::new(engine);
        let mut s = f.session();
        run(&mut s, "CREATE SEQUENCE direct_ids");
        run(
            &mut s,
            "CREATE TABLE serial_rows(id bigserial PRIMARY KEY, payload integer)",
        );
        drop(s);
        f.admin.take();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::v2_sql::sql_sequences::reservation_tests::crash_worker",
                "--nocapture",
            ])
            .env("F2_SEQUENCE_CRASH_PATH", &f.path)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "{engine:?}: {output:?}");
        f.reopen();
        let mut s = f.session();
        assert!(scalar(&mut s, "SELECT nextval('direct_ids')") > 71);
        assert_eq!(scalar(&mut s, "SELECT count(*) FROM serial_rows"), 70);
        assert_eq!(
            scalar(&mut s, "SELECT payload FROM serial_rows WHERE id=1"),
            70
        );
        let first = scalar(
            &mut s,
            "INSERT INTO serial_rows(payload) VALUES (3) RETURNING id",
        );
        assert!(first > 71);
        let second = scalar(
            &mut s,
            "INSERT INTO serial_rows(payload) VALUES (4) RETURNING id",
        );
        assert!(second > first);
        assert_eq!(scalar(&mut s, "SELECT count(*) FROM serial_rows"), 72);
        assert_eq!(
            scalar(
                &mut s,
                &format!("SELECT payload FROM serial_rows WHERE id={first}")
            ),
            3
        );
        assert_eq!(
            scalar(
                &mut s,
                &format!("SELECT payload FROM serial_rows WHERE id={second}")
            ),
            4
        );
        drop(s);
        f.reopen();
        assert_eq!(
            scalar(&mut f.session(), "SELECT count(*) FROM serial_rows"),
            72
        );
    }
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn committed_lookahead_writes_only_on_refill_setval_and_cycle_wrap() {
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE cached");
    run(&mut s, "CREATE SEQUENCE tiny MINVALUE 1 MAXVALUE 2 CYCLE");
    let db = f.admin.as_ref().unwrap().test_v2();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let parts = db.catalog().parts(&mut txn).unwrap();
    let key = |name: &str| {
        let (&id, def) = parts
            .ext
            .sequences
            .iter()
            .find(|(_, def)| def.name == name)
            .unwrap();
        engine::catalog_codec::counter_key(id, def.incarnation)
    };
    let cached = key("cached");
    let tiny = key("tiny");
    assert_eq!(scalar(&mut s, "SELECT nextval('cached')"), 1);
    let first = db.kv.get(Keyspace::Seq, &cached).unwrap();
    for expected in 2..=8 {
        assert_eq!(scalar(&mut s, "SELECT nextval('cached')"), expected);
    }
    assert_eq!(db.kv.get(Keyspace::Seq, &cached).unwrap(), first);
    assert_eq!(scalar(&mut s, "SELECT nextval('cached')"), 9);
    drain_sequence(&db, "cached");
    assert_ne!(db.kv.get(Keyspace::Seq, &cached).unwrap(), first);
    assert_eq!(scalar(&mut s, "SELECT setval('cached',500,false)"), 500);
    let set = db.kv.get(Keyspace::Seq, &cached).unwrap();
    assert_eq!(scalar(&mut s, "SELECT nextval('cached')"), 500);
    assert_ne!(db.kv.get(Keyspace::Seq, &cached).unwrap(), set);
    assert_eq!(scalar(&mut s, "SELECT nextval('tiny')"), 1);
    let first = db.kv.get(Keyspace::Seq, &tiny).unwrap();
    assert_eq!(scalar(&mut s, "SELECT nextval('tiny')"), 2);
    assert_eq!(db.kv.get(Keyspace::Seq, &tiny).unwrap(), first);
    assert_eq!(scalar(&mut s, "SELECT nextval('tiny')"), 1);
    assert_ne!(db.kv.get(Keyspace::Seq, &tiny).unwrap(), first);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn concurrent_explicit_commits_with_nextval_do_not_reuse_values() {
    let f = Fixture::new(Engine::V2);
    run(&mut f.session(), "CREATE SEQUENCE explicit_ids");
    let start = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let admin = f.admin.as_ref().unwrap().clone();
            let start = start.clone();
            thread::spawn(move || {
                let mut s = admin.sql_session();
                start.wait();
                let values: Vec<_> = (0..50)
                    .map(|_| {
                        run(&mut s, "BEGIN");
                        let value = scalar(&mut s, "SELECT nextval('explicit_ids')");
                        run(&mut s, "COMMIT");
                        value
                    })
                    .collect();
                assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
                values
            })
        })
        .collect();
    let values: Vec<_> = workers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();
    assert_eq!(values.len(), 400);
    assert_eq!(values.into_iter().collect::<BTreeSet<_>>().len(), 400);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn concurrent_explicit_commits_publish_new_sequence_positions() {
    let f = Fixture::new(Engine::V2);
    let start = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|id| {
            let admin = f.admin.as_ref().unwrap().clone();
            let start = start.clone();
            thread::spawn(move || {
                let mut s = admin.sql_session();
                start.wait();
                run(&mut s, "BEGIN");
                run(&mut s, &format!("CREATE SEQUENCE s_{id}"));
                let sql = format!("SELECT nextval('s_{id}')");
                for expected in 1..=5 {
                    assert_eq!(scalar(&mut s, &sql), expected);
                }
                run(&mut s, "COMMIT");
                let mut other = admin.sql_session();
                assert!(scalar(&mut other, &sql) > 5);
                // The creator must also observe any advancement by the other session.
                let last = scalar(&mut other, &sql);
                assert!(scalar(&mut s, &sql) > last);
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn explicit_commit_crash_worker() {
    let Some(path) = std::env::var_os("F2_EXPLICIT_CRASH_PATH") else {
        return;
    };
    let point = match std::env::var("F2_EXPLICIT_CRASH_POINT").unwrap().as_str() {
        "before" => CrashPoint::BeforeCommit,
        "after" => CrashPoint::AfterCommit,
        "before_ack" => CrashPoint::BeforeAck,
        "error_before" => CrashPoint::ErrorBeforeCommit,
        "error_after" => CrashPoint::ErrorAfterCommit,
        "acknowledged" => CrashPoint::None,
        other => panic!("unknown crash point {other}"),
    };
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "BEGIN");
    run(&mut s, "CREATE SEQUENCE crash_pending");
    for expected in 1..=5 {
        assert_eq!(scalar(&mut s, "SELECT nextval('crash_pending')"), expected);
    }
    admin.set_crash_point(point).unwrap();
    let committed = s.execute_at("COMMIT", &[], None, &clock());
    match point {
        CrashPoint::None => assert!(committed.is_ok()),
        CrashPoint::ErrorBeforeCommit | CrashPoint::ErrorAfterCommit => {
            assert!(matches!(committed, Err(CoreError::Storage(_))));
        }
        _ => panic!("crash point did not terminate the worker"),
    }
    crate::storage::crash_now();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn abrupt_explicit_commit_preserves_pending_sequence_reservations() {
    for (point, durable) in [
        ("before", false),
        ("error_before", false),
        ("after", true),
        ("before_ack", true),
        ("error_after", true),
        ("acknowledged", true),
    ] {
        let mut f = Fixture::new(Engine::V2);
        f.admin.take();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::v2_sql::sql_sequences::reservation_tests::explicit_commit_crash_worker",
                "--nocapture",
            ])
            .env("F2_EXPLICIT_CRASH_PATH", &f.path)
            .env("F2_EXPLICIT_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "{point}: {output:?}");
        f.reopen();
        let mut s = f.session();
        if durable {
            let first = scalar(&mut s, "SELECT nextval('crash_pending')");
            assert!(first > 5, "{point}: reused {first}");
            assert!(scalar(&mut s, "SELECT nextval('crash_pending')") > first);
        } else {
            assert_eq!(
                s.execute_at("SELECT nextval('crash_pending')", &[], None, &clock()),
                Err(CoreError::Sql(SqlError::UnknownTable)),
                "{point}",
            );
        }
    }
}

#[test]
fn counter_golden_bytes() {
    let def = relational::catalog_ext::SequenceDef {
        name: "golden".into(),
        data_type: relational::DataType::BigInt,
        start: 1,
        increment: 1,
        min: 1,
        max: i64::MAX,
        cache: 1,
        cycle: false,
        owned_by: None,
        identity: false,
        incarnation: 42,
    };
    let counter = sequences::SeqCounter {
        incarnation: 42,
        last_value: 7,
        is_called: true,
        log_hw: 33,
        stamp: 7,
    };
    let bytes = engine::catalog_codec::encode_counter(&counter, &def).unwrap();
    // Captured by running encode_counter on HEAD 51059fda before the slot change.
    assert_eq!(
        bytes,
        vec![
            0, 0, 0, 0, 0, 0, 0, 33, 1, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0, 0, 7
        ]
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn four_sequence_contention() {
    let f = Fixture::new(Engine::V2);
    for id in 0..4 {
        run(&mut f.session(), &format!("CREATE SEQUENCE contended_{id}"));
    }
    let start = Arc::new(std::sync::Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|worker| {
            let admin = f.admin.as_ref().unwrap().clone();
            let start = start.clone();
            thread::spawn(move || {
                let mut s = admin.sql_session();
                let mut values: [Vec<i64>; 4] = Default::default();
                start.wait();
                for n in 0..200 {
                    let id = (worker + n) % 4;
                    values[id].push(scalar(&mut s, &format!("SELECT nextval('contended_{id}')")));
                }
                for sequence in &values {
                    assert!(sequence.windows(2).all(|w| w[0] < w[1]));
                }
                values
            })
        })
        .collect();
    let mut values: [BTreeSet<i64>; 4] = Default::default();
    for worker in workers {
        for (id, sequence) in worker.join().unwrap().into_iter().enumerate() {
            for value in sequence {
                assert!(values[id].insert(value));
            }
        }
    }
    assert!(values.iter().all(|sequence| sequence.len() == 800));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn cached_callback_observes_drop_and_restart() {
    let f = Fixture::new(Engine::V2);
    let mut s1 = f.session();
    let mut s2 = f.session();
    let db = f.admin.as_ref().unwrap().test_v2();
    for ddl in [
        "DROP SEQUENCE freshness",
        "ALTER SEQUENCE freshness RESTART WITH 700",
    ] {
        run(&mut s1, "CREATE SEQUENCE freshness");
        let image = db.metadata_state(None).unwrap().sql;
        let baseline = image
            .catalog_ext()
            .sequences
            .iter()
            .map(|(id, def)| (*id, def.incarnation))
            .collect();
        let cb = callback(
            db.child(Holder::Sequence).unwrap(),
            Arc::new(image),
            baseline,
            Default::default(),
            Default::default(),
            vec!["public".into()],
        );
        assert_eq!(
            cb("nextval", "freshness", None, false).unwrap(),
            Value::Int(1)
        );
        run(&mut s2, ddl);
        assert_eq!(
            cb("nextval", "freshness", None, false),
            Err(CoreError::Sql(SqlError::UnknownTable))
        );
        if ddl.starts_with("ALTER") {
            assert_eq!(scalar(&mut s1, "SELECT nextval('freshness')"), 700);
            run(&mut s2, "DROP SEQUENCE freshness");
        } else {
            assert_eq!(
                s1.execute_at("SELECT nextval('freshness')", &[], None, &clock()),
                Err(CoreError::Sql(SqlError::UnknownTable))
            );
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn durable_counter_bytes_follow_original_lookahead() {
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE durable_bytes");
    let db = f.admin.as_ref().unwrap().test_v2();
    let view = db.catalog().current_local_view().unwrap();
    let (&id, def) = view
        .sequences
        .iter()
        .find(|(_, d)| d.name == "durable_bytes")
        .unwrap();
    let key = engine::catalog_codec::counter_key(id, def.incarnation);
    for n in 1..=70 {
        assert_eq!(scalar(&mut s, "SELECT nextval('durable_bytes')"), n);
        // Reservations are written on calls 1, 9, 17, ... with 32 steps ahead.
        drain_sequence(&db, "durable_bytes");
        let refill = 1 + ((n - 1) / 8) * 8;
        let expected = sequences::SeqCounter {
            incarnation: def.incarnation,
            last_value: refill,
            is_called: true,
            log_hw: refill + 32,
            stamp: refill as u64,
        };
        assert_eq!(
            db.kv.get(Keyspace::Seq, &key).unwrap().unwrap(),
            engine::catalog_codec::encode_counter(&expected, def).unwrap()
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn blocked_sequence_does_not_block_another() {
    use std::{sync::mpsc, time::Duration};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    for name in ["blocked_a", "independent_b"] {
        run(&mut s, &format!("CREATE SEQUENCE {name}"));
    }
    // A's first allocation must refill. Hold its slot while B refills independently.
    let db = f.admin.as_ref().unwrap().test_v2();
    let view = db.catalog().current_local_view().unwrap();
    let (&id, _) = view
        .sequences
        .iter()
        .find(|(_, d)| d.name == "blocked_a")
        .unwrap();
    let slot = db
        .sql_counters
        .lock()
        .unwrap()
        .entry(id)
        .or_default()
        .clone();
    let guard = slot.counter.lock().unwrap();
    let (started, waiting) = mpsc::channel();
    let admin = f.admin.as_ref().unwrap().clone();
    let a = thread::spawn(move || {
        started.send(()).unwrap();
        scalar(&mut admin.sql_session(), "SELECT nextval('blocked_a')")
    });
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    let (tx, rx) = mpsc::channel();
    let admin = f.admin.as_ref().unwrap().clone();
    let b = thread::spawn(move || {
        let value = scalar(&mut admin.sql_session(), "SELECT nextval('independent_b')");
        tx.send(value).unwrap();
    });
    let result = rx.recv_timeout(Duration::from_secs(2));
    drop(guard);
    assert_eq!(a.join().unwrap(), 1);
    b.join().unwrap();
    assert_eq!(result.unwrap(), 1);
}

#[cfg(feature = "engine-rocksdb")]
fn drain_sequence(db: &v2::V2Db, name: &str) {
    let view = db.catalog().current_local_view().unwrap();
    let id = *view
        .sequences
        .iter()
        .find(|(_, d)| d.name == name)
        .unwrap()
        .0;
    let slot = db.sql_counters.lock().unwrap().get(&id).unwrap().clone();
    let mut guard = slot.counter.lock_cancellable().unwrap();
    while guard.receiving || !guard.inflight.is_empty() {
        guard = receive_one(&slot, guard).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn pipelined_acks_release_slot_and_cover_returns() {
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE pipeline");
    assert_eq!(scalar(&mut s, "SELECT nextval('pipeline')"), 1);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    let (tx, rx) = mpsc::channel();
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let admin = f.admin.as_ref().unwrap().clone();
            let tx = tx.clone();
            thread::spawn(move || {
                let mut s = admin.sql_session();
                for _ in 0..4 {
                    tx.send(scalar(&mut s, "SELECT nextval('pipeline')"))
                        .unwrap();
                }
            })
        })
        .collect();
    let mut values = BTreeSet::from([1]);
    for _ in 0..32 {
        assert!(values.insert(rx.recv_timeout(Duration::from_secs(10)).unwrap()));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(values.iter().next_back(), Some(&33));
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, done) = mpsc::channel();
    let waiter = thread::spawn(move || {
        tx.send(scalar(
            &mut admin.sql_session(),
            "SELECT nextval('pipeline')",
        ))
        .unwrap()
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.sequencer.held_counter_acks() == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    let slot = db
        .sql_counters
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    loop {
        let cancel = crate::cancellation::Cancellation::new(Some(
            Instant::now() + Duration::from_millis(100),
        ));
        let receiving = cancel.scope(|| slot.counter.lock_cancellable().unwrap().receiving);
        if receiving {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert!(done.recv_timeout(Duration::from_millis(200)).is_err());
    db.sequencer.gate_counter_acks(false);
    assert!(values.insert(done.recv_timeout(Duration::from_secs(10)).unwrap()));
    waiter.join().unwrap();
    assert_eq!(values, (1..=34).collect());
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn nextval_waiting_on_ack_survives_concurrent_restart() {
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE restart_pipeline");
    let mut values = BTreeSet::from([scalar(&mut s, "SELECT nextval('restart_pipeline')")]);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    // The first durable reservation covers 1..=33. Refills are sent but their
    // acks stay gated; value 34 must enter await_coverage.
    for expected in 2..=33 {
        assert_eq!(
            scalar(&mut s, "SELECT nextval('restart_pipeline')"),
            expected
        );
        assert!(values.insert(expected));
    }
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, done) = mpsc::channel();
    let waiter = thread::spawn(move || {
        tx.send(scalar(
            &mut admin.sql_session(),
            "SELECT nextval('restart_pipeline')",
        ))
        .unwrap();
    });
    let slot = db
        .sql_counters
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let guard = slot.counter.lock().unwrap();
        let waiting = guard.waiting_returns == 1 && guard.receiving;
        drop(guard);
        if waiting && db.sequencer.held_counter_acks() > 0 {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert!(done.try_recv().is_err());
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, restarted) = mpsc::channel();
    let restart = thread::spawn(move || {
        let mut s = admin.sql_session();
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE restart_pipeline RESTART WITH 1000");
        run(&mut s, "COMMIT");
        tx.send(()).unwrap();
    });
    let outcome = restarted.recv_timeout(Duration::from_secs(10));
    // Always release the ack gate before propagating a failure.
    db.sequencer.gate_counter_acks(false);
    outcome.unwrap();
    restart.join().unwrap();
    let value = done.recv_timeout(Duration::from_secs(10)).unwrap();
    waiter.join().unwrap();
    assert_eq!(value, 34);
    assert!(values.insert(value));
    for expected in 1000..1010 {
        assert_eq!(
            scalar(&mut s, "SELECT nextval('restart_pipeline')"),
            expected
        );
        assert!(values.insert(expected));
    }
    assert_eq!(slot.counter.lock().unwrap().waiting_returns, 0);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn nextval_waiting_on_rejected_refill_survives_concurrent_restart() {
    use std::{sync::mpsc, time::{Duration, Instant}};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE rejected_refill");
    assert_eq!(scalar(&mut s, "SELECT nextval('rejected_refill')"), 1);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_writes(true);
    for expected in 2..=33 {
        assert_eq!(scalar(&mut s, "SELECT nextval('rejected_refill')"), expected);
    }
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, done) = mpsc::channel();
    let waiter = thread::spawn(move || {
        tx.send(admin.sql_session().execute_at(
            "SELECT nextval('rejected_refill')", &[], None, &clock(),
        )).unwrap();
    });
    let slot = db.sql_counters.lock().unwrap().values().next().unwrap().clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let guard = slot.counter.lock().unwrap();
        if guard.waiting_returns == 1 && guard.receiving { break; }
        drop(guard);
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    // All refills are still unsubmitted. RESTART is accepted and committed first;
    // releasing the submissions makes real counter validation return SequenceGone.
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE rejected_refill RESTART WITH 1000");
    run(&mut s, "COMMIT");
    db.sequencer.gate_counter_writes(false);
    let outcome = done.recv_timeout(Duration::from_secs(10)).unwrap();
    waiter.join().unwrap();
    assert_eq!(outcome.unwrap().rows[0].values, vec![Value::Int(34)]);
    for expected in 1000..1010 {
        assert_eq!(scalar(&mut s, "SELECT nextval('rejected_refill')"), expected);
    }
    let guard = slot.counter.lock().unwrap();
    assert_eq!(guard.waiting_returns, 0);
    assert!(!guard.receiving);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn stale_image_nextval_does_not_clobber_restarted_slot() {
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE stale_slot");
    let db = f.admin.as_ref().unwrap().test_v2();
    let image = db.metadata_state(None).unwrap().sql;
    let (&id, def) = image.catalog_ext().sequences.iter()
        .find(|(_, def)| def.name == "stale_slot").unwrap();
    assert_eq!(scalar(&mut s, "SELECT nextval('stale_slot')"), 1);
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER SEQUENCE stale_slot RESTART WITH 1000");
    run(&mut s, "COMMIT");
    let incarnation = db.catalog().current_local_view().unwrap().sequences[&id].incarnation;
    assert_ne!(incarnation, def.incarnation);
    let slot = db.sql_counters.lock().unwrap().get(&id).unwrap().clone();
    assert_eq!(slot.counter.lock().unwrap().counter.as_ref().unwrap().incarnation, incarnation);
    // Bypass the callback's freshness check to exercise the install guard itself.
    assert_eq!(
        committed_value(&db, &slot, id, def, "nextval", None, false),
        Err(CoreError::Sql(SqlError::UnknownTable))
    );
    assert_eq!(slot.counter.lock().unwrap().counter.as_ref().unwrap().incarnation, incarnation);
    assert_eq!(scalar(&mut s, "SELECT nextval('stale_slot')"), 1000);
    assert_eq!(scalar(&mut s, "SELECT nextval('stale_slot')"), 1001);
}

#[cfg(feature = "engine-rocksdb")]
fn stress_reservations(iterations: usize) {
    let f = Fixture::new(Engine::V2);
    run(&mut f.session(), "CREATE SEQUENCE stress_pipeline");
    for _ in 0..iterations {
        let workers: Vec<_> = (0..32)
            .map(|_| {
                let admin = f.admin.as_ref().unwrap().clone();
                thread::spawn(move || {
                    let mut s = admin.sql_session();
                    (0..8)
                        .map(|_| scalar(&mut s, "SELECT nextval('stress_pipeline')"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let values: Vec<_> = workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect();
        let unique: BTreeSet<_> = values.iter().copied().collect();
        assert_eq!(unique.len(), values.len());
        assert!(*unique.last().unwrap() - *unique.first().unwrap() + 1 - values.len() as i64 <= 32);
    }
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn pipelined_stress() {
    stress_reservations(1);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
#[ignore]
fn pipelined_stress_1000() {
    stress_reservations(1000);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn pipeline_crash_worker() {
    use std::io::Write;
    let Some(path) = std::env::var_os("PIPELINE_CRASH_PATH") else {
        return;
    };
    let count: i64 = std::env::var("PIPELINE_CRASH_N").unwrap().parse().unwrap();
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let start = Arc::new(std::sync::Barrier::new(16));
    let workers: Vec<_> = (0..16).map(|lane| {
        let admin = admin.clone();
        let start = start.clone();
        let tx = tx.clone();
        thread::spawn(move || {
            let mut s = admin.sql_session();
            start.wait();
            for _ in (lane..count).step_by(16) {
                let value = scalar(&mut s, "SELECT nextval('pipeline_crash')");
                tx.send(value).unwrap();
            }
        })
    }).collect();
    let mut seen = BTreeSet::new();
    for _ in 0..count {
        let value = rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        assert!(seen.insert(value));
        println!("RETURNED={value}");
        std::io::stdout().flush().unwrap();
    }
    for worker in workers { worker.join().unwrap(); }
    crate::storage::crash_now();
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn pipeline_crash_skips_at_most_32() {
    for count in [50, 77] {
        let mut f = Fixture::new(Engine::V2);
        run(&mut f.session(), "CREATE SEQUENCE pipeline_crash");
        f.admin.take();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::v2_sql::sql_sequences::reservation_tests::pipeline_crash_worker",
                "--nocapture",
            ])
            .env("PIPELINE_CRASH_PATH", &f.path)
            .env("PIPELINE_CRASH_N", count.to_string())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86));
        let printed: Vec<i64> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix("RETURNED="))
            .map(|n| n.parse().unwrap())
            .collect();
        assert_eq!(printed.len(), count);
        let max = *printed.iter().max().unwrap();
        f.reopen();
        let next = scalar(&mut f.session(), "SELECT nextval('pipeline_crash')");
        assert!(next > max && next - max <= 33, "{max} -> {next}");
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn cancelled_receiver_restores_oldest_ack() {
    use std::time::{Duration, Instant};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE cancelled_ack");
    assert_eq!(scalar(&mut s, "SELECT nextval('cancelled_ack')"), 1);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    for expected in 2..=33 {
        assert_eq!(scalar(&mut s, "SELECT nextval('cancelled_ack')"), expected);
    }
    let c =
        crate::cancellation::Cancellation::new(Some(Instant::now() + Duration::from_millis(100)));
    assert_eq!(
        c.scope(|| s.execute_at("SELECT nextval('cancelled_ack')", &[], None, &clock())),
        Err(CoreError::Cancelled)
    );
    let slot = db
        .sql_counters
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    {
        let guard = slot.counter.lock().unwrap();
        assert!(!guard.receiving);
        assert_eq!(guard.waiting_returns, 0);
        assert_eq!(guard.inflight.front().unwrap().hw, 41);
    }
    db.sequencer.gate_counter_acks(false);
    assert_eq!(scalar(&mut s, "SELECT nextval('cancelled_ack')"), 35);
    // A backwards move must drain all reservations and leave no stale coverage.
    assert_eq!(scalar(&mut s, "SELECT setval('cancelled_ack',5,false)"), 5);
    assert_eq!(scalar(&mut s, "SELECT nextval('cancelled_ack')"), 5);
}

// Each mode is also run alone, so one durable writer cannot mask another mode.
#[cfg(feature = "engine-rocksdb")]
#[test]
fn unsettled_crash_worker() {
    let Some(path) = std::env::var_os("UNSETTLED_CRASH_PATH") else { return };
    let path = PathBuf::from(path);
    let mode: usize = std::env::var("UNSETTLED_CRASH_MODE").unwrap().parse().unwrap();
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let lanes = if mode == 4 { 4 } else { 1 };
    // Each exposure parks its session (alive, often mid-transaction) until the
    // main thread resumes it, so the final crash finds every session still
    // holding its last observed value: no later statement, rollback or drop
    // can settle it first.
    let (seen_tx, seen_rx) = std::sync::mpsc::channel::<(usize, i64)>();
    let mut resume = Vec::new();
    let mut workers = Vec::new();
    for lane in 0..lanes {
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        resume.push(go_tx);
        let admin = admin.clone();
        let seen_tx = seen_tx.clone();
        workers.push(thread::spawn(move || {
            let mut s = admin.sql_session();
            let expose = |value: i64| {
                seen_tx.send((lane, value)).unwrap();
                // A closed channel means the process is crashing; park forever.
                if go_rx.recv().is_err() { loop { thread::park(); } }
            };
            for n in 0..80 {
                let which = if mode == 4 { (n + lane) % 4 } else { mode };
                match which {
                    0 => expose(scalar(&mut s, "INSERT INTO exposed(payload) VALUES (1) RETURNING id")),
                    1 => expose(scalar(&mut s, "SELECT nextval('exposed_id_seq')")),
                    2 => {
                        run(&mut s, "BEGIN");
                        expose(scalar(&mut s, "SELECT nextval('exposed_id_seq')"));
                        expose(scalar(&mut s, "INSERT INTO exposed(payload) VALUES (2) RETURNING id"));
                        run(&mut s, "ROLLBACK");
                    }
                    _ => {
                        run(&mut s, "BEGIN");
                        let payload = 1000 + lane * 100 + n;
                        run(&mut s, &format!("INSERT INTO exposed(payload) VALUES ({payload})"));
                        expose(scalar(&mut s, &format!("SELECT id FROM exposed WHERE payload={payload}")));
                        run(&mut s, "ROLLBACK");
                    }
                }
            }
            // Finished lanes stay alive with their sessions open.
            loop { thread::park(); }
        }));
    }
    drop(seen_tx);
    let mut seen = 0;
    for _ in 0..lanes * 60 {
        let (lane, value) = seen_rx.recv().unwrap();
        seen = seen.max(value);
        resume[lane].send(()).unwrap();
    }
    // Collect each lane's next exposure and crash while all of them are parked.
    for _ in 0..lanes {
        let (_, value) = seen_rx.recv().unwrap();
        seen = seen.max(value);
    }
    std::fs::write(path.join("client-seen"), seen.to_string()).unwrap();
    let _ = workers;
    crate::storage::crash_now();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn abrupt_crash_never_reissues_exposed_unsettled_values() {
    for mode in 0..5 {
        let mut f = Fixture::new(Engine::V2);
        run(&mut f.session(), "CREATE TABLE exposed(id bigserial PRIMARY KEY, payload integer)");
        f.admin.take();
        let output = Command::new(std::env::current_exe().unwrap()).args([
            "--exact", "storage::v2_sql::sql_sequences::reservation_tests::unsettled_crash_worker",
            "--nocapture",
        ]).env("UNSETTLED_CRASH_PATH", &f.path)
            .env("UNSETTLED_CRASH_MODE", mode.to_string()).output().unwrap();
        assert_eq!(output.status.code(), Some(86), "mode {mode}: {output:?}");
        let seen: i64 = std::fs::read_to_string(f.path.join("client-seen")).unwrap().parse().unwrap();
        f.reopen();
        let mut s = f.session();
        for _ in 0..80 {
            assert!(scalar(&mut s, "SELECT nextval('exposed_id_seq')") > seen, "mode {mode}");
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn deferred_insert_avoids_coverage_wait_but_exposed_select_waits() {
    use std::{sync::mpsc, time::{Duration, Instant}};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE TABLE fast(id bigserial PRIMARY KEY, payload integer)");
    scalar(&mut s, "SELECT nextval('fast_id_seq')");
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, rx) = mpsc::channel();
    let (release, proceed) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut s = admin.sql_session();
        run(&mut s, "BEGIN");
        // Cross durable coverage (33) and reuse the queued reservation at 35.
        for _ in 0..34 { run(&mut s, "INSERT INTO fast(payload) VALUES (1)"); }
        tx.send(()).unwrap();
        proceed.recv().unwrap();
        run(&mut s, "COMMIT");
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.sequencer.held_counter_acks() == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    let completed = rx.recv_timeout(Duration::from_secs(10));
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, done) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut s = admin.sql_session();
        run(&mut s, "BEGIN");
        tx.send(scalar(&mut s, "SELECT nextval('fast_id_seq')")).unwrap();
        run(&mut s, "ROLLBACK");
    });
    // Synchronise on the reader's actual coverage wait, not on a timeout.
    let slot = db.sql_counters.lock().unwrap().values().next().unwrap().clone();
    loop {
        let cancel = crate::cancellation::Cancellation::new(Some(Instant::now() + Duration::from_millis(100)));
        let waiting = cancel.scope(|| {
            let guard = slot.counter.lock_cancellable().unwrap();
            guard.waiting_returns > 0 || guard.receiving
        });
        if waiting { break; }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert!(done.recv_timeout(Duration::from_millis(200)).is_err());
    assert!(db.sequencer.held_counter_acks() > 0);
    db.sequencer.gate_counter_acks(false);
    release.send(()).unwrap();
    worker.join().unwrap();
    reader.join().unwrap();
    completed.unwrap();
    assert_eq!(done.recv_timeout(Duration::from_secs(10)).unwrap(), 36);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn deferred_coverage_survives_a_durable_backwards_install() {
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE TABLE backwards(id bigserial PRIMARY KEY, payload integer)");
    scalar(&mut s, "SELECT nextval('backwards_id_seq')");
    run(&mut s, "BEGIN");
    for _ in 0..40 { run(&mut s, "INSERT INTO backwards(payload) VALUES (1)"); }
    assert_eq!(scalar(&mut s, "SELECT setval('backwards_id_seq',1,false)"), 1);
    run(&mut s, "ROLLBACK");
    // setval is non-transactional: the durable backwards install stands, and
    // settling the earlier deferred values did not move the counter forward.
    assert_eq!(scalar(&mut s, "SELECT nextval('backwards_id_seq')"), 1);
}

// Preserve the original direct-helper tests as synchronous callers. Sessions use
// the obligation-aware signatures; these callers always settle before returning.
#[cfg(feature = "engine-rocksdb")]
fn callback(
    db: EngineLease, image: Arc<relational::Relational>,
    baseline: BTreeMap<u64, u128>, pending: sequences::Pending,
    values: sequences::SessionValues, search_path: Vec<String>,
) -> relational::clock::SequenceCallback {
    let unsettled = Unsettled::default();
    let callback = super::callback(db.child(Holder::Sequence).unwrap(), image, baseline, pending, values, unsettled.clone(), search_path);
    Arc::new(move |operation, name, value, called| {
        let result = callback(operation, name, value, called);
        settle(&db, &unsettled)?;
        result
    })
}

#[cfg(feature = "engine-rocksdb")]
fn committed_value(
    db: &EngineLease, slot: &Arc<v2::SeqSlot>, id: u64,
    def: &relational::catalog_ext::SequenceDef, operation: &str,
    value: Option<i64>, called: bool,
) -> Result<i64, CoreError> {
    let unsettled = Unsettled::default();
    let result = super::committed_value(db, slot, id, def, operation, value, called, &unsettled);
    settle(db, &unsettled)?;
    result
}

#[cfg(feature = "engine-rocksdb")]
struct ReleaseGate<F: FnMut()>(F);
#[cfg(feature = "engine-rocksdb")]
impl<F: FnMut()> Drop for ReleaseGate<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

// Gate a real SQL coverage ack so every caller contends with one receiver.
#[cfg(feature = "engine-rocksdb")]
#[test]
fn coverage_waiters_sleep_and_resume() {
    use std::{sync::mpsc, time::{Duration, Instant}};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE sleeping");
    assert_eq!(scalar(&mut s, "SELECT nextval('sleeping')"), 1);
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    // Release gated acks even if an assertion unwinds, so workers finish
    // before fixture teardown instead of hanging on a gated receiver.
    let _release = ReleaseGate(|| db.sequencer.gate_counter_acks(false));
    for expected in 2..=33 { assert_eq!(scalar(&mut s, "SELECT nextval('sleeping')"), expected); }
    let slot = db.sql_counters.lock().unwrap().values().next().unwrap().clone();
    let (tx, rx) = mpsc::channel();
    let start = Arc::new(std::sync::Barrier::new(32));
    let workers: Vec<_> = (0..32).map(|_| {
        let admin = f.admin.as_ref().unwrap().clone();
        let tx = tx.clone();
        let start = start.clone();
        thread::spawn(move || {
            let mut s = admin.sql_session();
            start.wait();
            // Report each value as it returns: one uncovered return must be seen
            // by the negative check below, even if a later call then blocks.
            for _ in 0..32 { tx.send(scalar(&mut s, "SELECT nextval('sleeping')")).unwrap(); }
        })
    }).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while slot.counter.lock().unwrap().waiting_returns < 32 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    // All callers have entered coverage waits. Keep only this short negative
    // observation window: no positive event can prove they stay blocked.
    assert!(rx.recv_timeout(Duration::from_millis(20)).is_err(), "returned without durable coverage");
    db.sequencer.gate_counter_acks(false);
    let resumed = Instant::now();
    let mut values = BTreeSet::new();
    for _ in 0..1024 {
        assert!(values.insert(rx.recv_timeout(Duration::from_secs(10)).unwrap()));
    }
    assert!(resumed.elapsed() < Duration::from_secs(10));
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(values.len(), 1024);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn coverage_nonreceiver_cancellation_and_deadline() {
    use std::{sync::mpsc, time::{Duration, Instant}};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE cancel_waiter");
    scalar(&mut s, "SELECT nextval('cancel_waiter')");
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    // Release gated acks even if an assertion unwinds, so workers finish
    // before fixture teardown instead of hanging on a gated receiver.
    let _release = ReleaseGate(|| db.sequencer.gate_counter_acks(false));
    for _ in 2..=33 { scalar(&mut s, "SELECT nextval('cancel_waiter')"); }
    let slot = db.sql_counters.lock().unwrap().values().next().unwrap().clone();
    let admin = f.admin.as_ref().unwrap().clone();
    let receiver = thread::spawn(move || scalar(&mut admin.sql_session(), "SELECT nextval('cancel_waiter')"));
    let deadline = Instant::now() + Duration::from_secs(10);
    while !slot.counter.lock().unwrap().receiving {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    for timed in [false, true] {
        let cancel = crate::cancellation::Cancellation::new(None);
        let c = cancel.clone();
        let admin = f.admin.as_ref().unwrap().clone();
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            tx.send(c.scope(|| admin.sql_session().execute_at("SELECT nextval('cancel_waiter')", &[], None, &clock()))).unwrap();
        });
        while slot.counter.lock().unwrap().waiting_returns < 2 {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let began = Instant::now();
        if timed {
            cancel.deadline(Instant::now() + Duration::from_millis(80));
        } else {
            assert!(cancel.cancel());
        }
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), Err(CoreError::Cancelled));
        assert!(began.elapsed() < Duration::from_secs(2));
        worker.join().unwrap();
    }
    db.sequencer.gate_counter_acks(false);
    assert_eq!(receiver.join().unwrap(), 34);
    assert_eq!(scalar(&mut s, "SELECT nextval('cancel_waiter')"), 37);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn coverage_nonreceiver_survives_ddl_restart() {
    use std::{sync::mpsc, time::{Duration, Instant}};
    let f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE restart_waiters");
    scalar(&mut s, "SELECT nextval('restart_waiters')");
    let db = f.admin.as_ref().unwrap().test_v2();
    db.sequencer.gate_counter_acks(true);
    // Release gated acks even if an assertion unwinds, so workers finish
    // before fixture teardown instead of hanging on a gated receiver.
    let _release = ReleaseGate(|| db.sequencer.gate_counter_acks(false));
    for _ in 2..=33 { scalar(&mut s, "SELECT nextval('restart_waiters')"); }
    let slot = db.sql_counters.lock().unwrap().values().next().unwrap().clone();
    let (tx, rx) = mpsc::channel();
    let mut workers = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    for count in 1..=2 {
        let admin = f.admin.as_ref().unwrap().clone();
        let tx = tx.clone();
        workers.push(thread::spawn(move || {
            tx.send(scalar(&mut admin.sql_session(), "SELECT nextval('restart_waiters')")).unwrap();
        }));
        loop {
            let guard = slot.counter.lock().unwrap();
            if guard.receiving && guard.waiting_returns == count { break; }
            drop(guard);
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
    }
    let before = *slot.ack_gen.lock().unwrap();
    let admin = f.admin.as_ref().unwrap().clone();
    let (tx, restarted) = mpsc::channel();
    let restart = thread::spawn(move || {
        let mut s = admin.sql_session();
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE restart_waiters RESTART WITH 1000");
        run(&mut s, "COMMIT");
        tx.send(()).unwrap();
    });
    let outcome = restarted.recv_timeout(Duration::from_secs(10));
    // Release even on failure, so fixture teardown cannot hang on gated acks.
    db.sequencer.gate_counter_acks(false);
    outcome.unwrap();
    restart.join().unwrap();
    assert_ne!(*slot.ack_gen.lock().unwrap(), before);
    let values: BTreeSet<_> = (0..2).map(|_| rx.recv_timeout(Duration::from_secs(10)).unwrap()).collect();
    assert_eq!(values, BTreeSet::from([34, 35]));
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(slot.counter.lock().unwrap().waiting_returns, 0);
    assert_eq!(scalar(&mut s, "SELECT nextval('restart_waiters')"), 1000);
}

// GD-0084 F1: native request boundaries must cover sequence values even when
// rows remain provisional or the request fails after allocating some defaults.
fn native_many_request(count: usize, fail: bool) -> crate::operations::Request {
    use crate::operations::{Expr, Projection, Request, ValueExpr};
    let request = Request::InsertMany {
        table: "native_rows".into(),
        columns: vec!["payload".into()],
        rows: (0..count).map(|i| vec![Expr::Value(ValueExpr::Literal(
            if fail && i == 4 { Value::Null } else { Value::Int(i as i64) }
        ))]).collect(),
        cap: ValueExpr::Literal(Value::Int(count as i64)),
    };
    Request::Returning {
        request: Box::new(request),
        projection: vec![Projection { expression: Expr::Field("id".into()), alias: None }],
    }
}
fn native_many(s: &mut SqlSession, count: usize) -> Vec<i64> {
    let out = s.execute_native(&native_many_request(count, false), &Default::default()).unwrap();
    assert_eq!(out.rows.len(), count);
    assert!(out.rows.iter().all(|r| r.version > 0));
    assert!(s.sequence_unsettled.lock().unwrap().is_empty());
    if s.admin.engine() == crate::Engine::V2 {
        let db = s.admin.test_v2();
        let view = db.catalog().current_local_view().unwrap();
        let id = *view.sequences.iter().find(|(_, def)| def.name == "native_ids").unwrap().0;
        let slot = db.sql_counters.lock().unwrap().get(&id).unwrap().clone();
        let guard = slot.counter.lock_cancellable().unwrap();
        let max = out.rows.iter().map(|r| match r.values[0] {
            Value::Int(id) => id,
            _ => unreachable!(),
        }).max().unwrap();
        assert!(guard.durable_hw.is_some_and(|hw| hw >= max));
    }
    out.rows.into_iter().map(|r| match r.values[0] {
        Value::Int(id) => id,
        ref other => panic!("unexpected id {other:?}"),
    }).collect()
}
fn native_schema(s: &mut SqlSession) {
    run(s, "CREATE SEQUENCE native_ids");
    run(s, "CREATE TABLE native_rows(id BIGINT DEFAULT nextval('native_ids') PRIMARY KEY, payload BIGINT NOT NULL)");
}
#[test]
fn native_many_settles_before_return_and_rollback_keeps_sequence() {
    for engine in engines() {
        let f = Fixture::new(engine);
        let mut s = f.session();
        native_schema(&mut s);
        run(&mut s, "BEGIN");
        let ids = native_many(&mut s, 40);
        assert_eq!(ids, (1..=40).collect::<Vec<_>>());
        assert_eq!(scalar(&mut s, "SELECT currval('native_ids')"), 40);
        run(&mut s, "ROLLBACK");
        assert_eq!(scalar(&mut s, "SELECT count(*) FROM native_rows"), 0);
        assert_eq!(scalar(&mut s, "SELECT nextval('native_ids')"), 41);
    }
}
#[test]
fn native_many_partial_error_is_atomic_and_settled() {
    for engine in engines() {
        let f = Fixture::new(engine);
        let mut s = f.session();
        native_schema(&mut s);
        assert_eq!(s.execute_native(&native_many_request(10, true), &Default::default()),
            Err(crate::operations::Error::Sql { sqlstate: "23502".into() }));
        assert!(s.sequence_unsettled.lock().unwrap().is_empty());
        assert_eq!(scalar(&mut s, "SELECT count(*) FROM native_rows"), 0);
        assert_eq!(scalar(&mut s, "SELECT nextval('native_ids')"), 6);
    }
}
#[test]
fn native_many_crash_worker() {
    let Some(path) = std::env::var_os("F1_NATIVE_MANY_CRASH_PATH") else { return; };
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let mode = std::env::var("F1_NATIVE_MANY_CRASH_MODE").unwrap();
    if mode == "uncommitted" { run(&mut s, "BEGIN"); }
    if mode == "error" {
        assert_eq!(s.execute_native(&native_many_request(10, true), &Default::default()),
            Err(crate::operations::Error::Sql { sqlstate: "23502".into() }));
        assert!(s.sequence_unsettled.lock().unwrap().is_empty());
    } else {
        assert_eq!(native_many(&mut s, 40), (1..=40).collect::<Vec<_>>());
    }
    crate::storage::crash_now();
}
#[test]
fn native_many_crash_preserves_committed_uncommitted_and_failed_values() {
    for engine in engines() {
        for mode in ["committed", "uncommitted", "error"] {
            let mut f = Fixture::new(engine);
            native_schema(&mut f.session());
            f.admin.take();
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "storage::v2_sql::sql_sequences::reservation_tests::native_many_crash_worker", "--nocapture"])
                .env("F1_NATIVE_MANY_CRASH_PATH", &f.path)
                .env("F1_NATIVE_MANY_CRASH_MODE", mode).output().unwrap();
            assert_eq!(output.status.code(), Some(86), "{engine:?}/{mode}: {output:?}");
            f.reopen();
            let mut s = f.session();
            assert_eq!(scalar(&mut s, "SELECT count(*) FROM native_rows"), if mode == "committed" { 40 } else { 0 });
            let high = if mode == "error" { 5 } else { 40 };
            assert!(scalar(&mut s, "SELECT nextval('native_ids')") > high);
            assert!(native_many(&mut s, 40).iter().all(|id| *id > high));
        }
    }
}
#[test]
fn native_many_concurrent_ids_are_unique() {
    use std::time::{Duration, Instant};
    for engine in engines() {
        let mut f = Fixture::new(engine);
        native_schema(&mut f.session());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let workers: Vec<_> = (0..4).map(|writer| {
            let admin = f.admin.as_ref().unwrap().clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut ids = Vec::new();
                barrier.wait();
                for _ in 0..5 {
                    let deadline = Instant::now() + Duration::from_secs(60);
                    let mut conflicts = 0u32;
                    loop {
                        let mut s = admin.sql_session();
                        // Retry only definite conflicts; reservations may leave safe gaps.
                        match s.execute_native(&native_many_request(40, false), &Default::default()) {
                            Ok(out) => {
                                assert!(s.sequence_unsettled.lock().unwrap().is_empty());
                                ids.extend(out.rows.into_iter().map(|r| match r.values[0] {
                                    Value::Int(id) => id,
                                    _ => unreachable!(),
                                }));
                                break;
                            }
                            Err(crate::operations::Error::Conflict) => {
                                assert!(Instant::now() < deadline, "{engine:?}: native insert conflicts did not converge");
                                // Legacy defaults persist outside the optimistic row
                                // transaction. Let another writer finish rather than
                                // continuously invalidating every writer's snapshot.
                                conflicts += 1;
                                thread::sleep(Duration::from_millis(
                                    10 * (writer + 1) * (1u64 << conflicts.min(6))));
                            }
                            Err(error) => panic!("concurrent native insert: {error:?}"),
                        }
                    }
                }
                ids
            })
        }).collect();
        let ids: Vec<_> = workers.into_iter().flat_map(|w| w.join().unwrap()).collect();
        assert_eq!(ids.len(), 800);
        let high = *ids.iter().max().unwrap();
        assert_eq!(ids.into_iter().collect::<BTreeSet<_>>().len(), 800);
        assert_eq!(scalar(&mut f.session(), "SELECT count(*) FROM native_rows"), 800);
        f.reopen();
        assert!(scalar(&mut f.session(), "SELECT nextval('native_ids')") > high);
    }
}

#[test]
fn native_many_expression_error_settles_prior_defaults() {
    use crate::operations::{Arithmetic, Error, Expr, Request, ValueExpr};
    for engine in engines() {
        let f = Fixture::new(engine);
        let mut s = f.session();
        native_schema(&mut s);
        let mut request = native_many_request(20, false);
        let Request::Returning { request: inner, .. } = &mut request else { unreachable!() };
        let Request::InsertMany { rows, .. } = inner.as_mut() else { unreachable!() };
        // Cross the first refill threshold so defaults create obligations.
        let one = Expr::Value(ValueExpr::Literal(Value::Int(1)));
        let zero = Expr::Value(ValueExpr::Literal(Value::Int(0)));
        rows[12][0] = Expr::Arithmetic(Box::new(one), Arithmetic::Divide, Box::new(zero));
        run(&mut s, "BEGIN");
        // Observe the wrapper exit before native_scope's error cleanup calls
        // abort(), which would otherwise mask a missing final settlement.
        crate::operations::NATIVE_MANY_ON_EXIT.with(|observer| observer.set(Some(|session| {
            assert!(session.sequence_unsettled.lock().unwrap().is_empty(),
                "insert_many returned unsettled defaults before outer abort");
        })));
        let result = s.execute_native(&request, &Default::default());
        crate::operations::NATIVE_MANY_ON_EXIT.with(|observer| observer.set(None));
        assert_eq!(result, Err(Error::DivisionByZero));
        assert!(s.sequence_unsettled.lock().unwrap().is_empty(), "{engine:?}: request returned unsettled defaults");
        run(&mut s, "ROLLBACK");
        assert_eq!(scalar(&mut s, "SELECT count(*) FROM native_rows"), 0);
        assert_eq!(scalar(&mut s, "SELECT nextval('native_ids')"), 13);
    }
}

#[test]
fn native_many_between_rows_crash_worker() {
    let Some(path) = std::env::var_os("F1_NATIVE_MID_CRASH_PATH") else { return; };
    let admin = Admin::open(PathBuf::from(path), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let explicit = std::env::var("F1_NATIVE_MID_CRASH_EXPLICIT").unwrap() == "true";
    // Earlier committed request returned these ids to its client.
    assert_eq!(native_many(&mut s, 40), (1..=40).collect::<Vec<_>>());
    if explicit {
        run(&mut s, "BEGIN");
        assert_eq!(native_many(&mut s, 40), (41..=80).collect::<Vec<_>>());
    }
    crate::operations::NATIVE_MANY_CRASH_AFTER.with(|after| after.set(Some(20)));
    let _ = s.execute_native(&native_many_request(40, false), &Default::default());
    panic!("mid-normalization crash hook was not reached");
}

#[test]
fn native_many_between_rows_crash_preserves_returned_ids() {
    for engine in engines() {
        for explicit in [false, true] {
            let mut f = Fixture::new(engine);
            native_schema(&mut f.session());
            f.admin.take();
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "storage::v2_sql::sql_sequences::reservation_tests::native_many_between_rows_crash_worker", "--nocapture"])
                .env("F1_NATIVE_MID_CRASH_PATH", &f.path)
                .env("F1_NATIVE_MID_CRASH_EXPLICIT", explicit.to_string()).output().unwrap();
            assert_eq!(output.status.code(), Some(86), "{engine:?}/{explicit}: {output:?}");
            f.reopen();
            let mut s = f.session();
            assert_eq!(scalar(&mut s, "SELECT count(*) FROM native_rows"), 40);
            assert_eq!(scalar(&mut s, "SELECT max(id) FROM native_rows"), 40);
            let returned_high = if explicit { 80 } else { 40 };
            assert!(scalar(&mut s, "SELECT nextval('native_ids')") > returned_high);
            assert!(native_many(&mut s, 40).iter().all(|id| *id > returned_high));
        }
    }
}

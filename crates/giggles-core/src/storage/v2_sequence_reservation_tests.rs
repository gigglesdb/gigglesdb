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
    let Backend::V2(db) = &f.admin.as_ref().unwrap().inner.backend else {
        unreachable!()
    };
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
    for expected in 2..=33 {
        assert_eq!(scalar(&mut s, "SELECT nextval('cached')"), expected);
    }
    assert_eq!(db.kv.get(Keyspace::Seq, &cached).unwrap(), first);
    assert_eq!(scalar(&mut s, "SELECT nextval('cached')"), 34);
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

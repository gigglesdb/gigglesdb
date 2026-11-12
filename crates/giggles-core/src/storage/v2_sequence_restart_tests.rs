//! Transactional RESTART coverage, with Legacy as the reference engine.
use super::*;
use crate::relational::{SqlResult, StatementClock, Value};
use std::path::PathBuf;

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
    match run(session, sql).rows[0].values[0] {
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
        let path = std::env::temp_dir().join(format!("f4-seq-{:032x}", u128::from_be_bytes(nonce)));
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
fn restart_with_seeds_uncalled_sequence_in_transaction_and_after_commit() {
    for engine in engines() {
        let f = Fixture::new(engine);
        let mut s = f.session();
        run(&mut s, "CREATE SEQUENCE ids");
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE ids RESTART WITH 10");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 10, "{engine:?}");
        run(&mut s, "COMMIT");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 11, "{engine:?}");
    }
}

#[test]
fn schema_qualified_restart_uses_bound_sequence_name() {
    for engine in engines() {
        let mut f = Fixture::new(engine);
        let mut s = f.session();
        run(&mut s, "CREATE SCHEMA app");
        run(&mut s, "CREATE SEQUENCE app.ids");
        scalar(&mut s, "SELECT nextval('app.ids')");
        run(&mut s, "ALTER SEQUENCE app.ids RESTART WITH 10");
        drop(s);
        f.reopen();
        assert_eq!(
            scalar(&mut f.session(), "SELECT nextval('app.ids')"),
            10,
            "{engine:?}"
        );
    }
}

#[test]
fn called_sequence_restart_without_nextval_survives_reopen() {
    for engine in engines() {
        for (restart, expected) in [("RESTART", 7), ("RESTART WITH 10", 10)] {
            for explicit in [false, true] {
                let mut f = Fixture::new(engine);
                let mut s = f.session();
                run(&mut s, "CREATE SEQUENCE ids START 7");
                assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 7);
                if explicit {
                    run(&mut s, "BEGIN");
                }
                run(&mut s, &format!("ALTER SEQUENCE ids {restart}"));
                if explicit {
                    run(&mut s, "COMMIT");
                }
                drop(s);
                f.reopen();
                assert_eq!(
                    scalar(&mut f.session(), "SELECT nextval('ids')"),
                    expected,
                    "{engine:?}, {restart}, explicit={explicit}"
                );
            }
        }
    }
}

#[test]
fn rollback_restart_preserves_old_live_and_durable_counters() {
    for engine in engines() {
        for restart in ["RESTART", "RESTART WITH 10"] {
            let mut f = Fixture::new(engine);
            let mut s = f.session();
            run(&mut s, "CREATE SEQUENCE ids START 7");
            assert_eq!(scalar(&mut s, "SELECT setval('ids',50,false)"), 50);
            run(&mut s, "BEGIN");
            run(&mut s, &format!("ALTER SEQUENCE ids {restart}"));
            scalar(&mut s, "SELECT nextval('ids')");
            run(&mut s, "ROLLBACK");
            drop(s);
            f.reopen();
            let mut s = f.session();
            assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 50, "{engine:?}");
            run(&mut s, "BEGIN");
            run(&mut s, &format!("ALTER SEQUENCE ids {restart}"));
            scalar(&mut s, "SELECT nextval('ids')");
            run(&mut s, "ROLLBACK");
            assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 51, "{engine:?}");
        }
    }
}

#[test]
fn repeated_restarts_and_start_changes_use_latest_seed() {
    for engine in engines() {
        let mut f = Fixture::new(engine);
        let mut s = f.session();
        run(&mut s, "CREATE SEQUENCE ids INCREMENT -2 START -3");
        scalar(&mut s, "SELECT nextval('ids')");
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE ids RESTART WITH -11");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), -11, "{engine:?}");
        run(&mut s, "ALTER SEQUENCE ids START WITH -7 RESTART");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), -7, "{engine:?}");
        run(&mut s, "COMMIT");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), -9, "{engine:?}");
        drop(s);
        f.reopen();
        assert!(scalar(&mut f.session(), "SELECT nextval('ids')") < -9);
    }
}

#[test]
fn called_restart_returns_seed_and_preserves_currval_across_commit() {
    for engine in engines() {
        for (restart, expected) in [("RESTART", 7), ("RESTART WITH 10", 10)] {
            let mut f = Fixture::new(engine);
            let mut s = f.session();
            run(&mut s, "CREATE SEQUENCE ids START 7");
            assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 7);
            assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 8);
            run(&mut s, "BEGIN");
            run(&mut s, &format!("ALTER SEQUENCE ids {restart}"));
            assert_eq!(scalar(&mut s, "SELECT currval('ids')"), 8, "{engine:?}");
            assert_eq!(
                scalar(&mut s, "SELECT nextval('ids')"),
                expected,
                "{engine:?}"
            );
            run(&mut s, "COMMIT");
            assert_eq!(
                scalar(&mut s, "SELECT nextval('ids')"),
                expected + 1,
                "{engine:?}"
            );
            drop(s);
            f.reopen();
            assert!(scalar(&mut f.session(), "SELECT nextval('ids')") > expected + 1);
        }
    }
}

#[test]
fn savepoint_rollback_of_restart_does_not_publish_stale_pending_counter() {
    for engine in engines() {
        let mut f = Fixture::new(engine);
        let mut s = f.session();
        run(&mut s, "CREATE SEQUENCE ids");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 1);
        run(&mut s, "BEGIN");
        run(&mut s, "SAVEPOINT before_restart");
        run(&mut s, "ALTER SEQUENCE ids RESTART WITH 10");
        scalar(&mut s, "SELECT nextval('ids')");
        run(&mut s, "ROLLBACK TO before_restart");
        run(&mut s, "COMMIT");
        assert_eq!(scalar(&mut s, "SELECT nextval('ids')"), 2, "{engine:?}");
        // An uncalled durable counter must also survive the same rollback.
        scalar(&mut s, "SELECT setval('ids',50,false)");
        run(&mut s, "BEGIN");
        run(&mut s, "SAVEPOINT before_restart");
        run(&mut s, "ALTER SEQUENCE ids RESTART WITH 10");
        run(&mut s, "ROLLBACK TO before_restart");
        run(&mut s, "COMMIT");
        drop(s);
        f.reopen();
        assert_eq!(
            scalar(&mut f.session(), "SELECT nextval('ids')"),
            50,
            "{engine:?}"
        );
    }
}

#[test]
fn identity_owned_sequence_restart_uses_same_counter_path() {
    for engine in engines() {
        let mut f = Fixture::new(engine);
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE items(id bigint GENERATED BY DEFAULT AS IDENTITY, payload integer)",
        );
        assert_eq!(
            scalar(&mut s, "INSERT INTO items(payload) VALUES (1) RETURNING id"),
            1
        );
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE items_id_seq RESTART WITH 10");
        assert_eq!(
            scalar(&mut s, "INSERT INTO items(payload) VALUES (2) RETURNING id"),
            10,
            "{engine:?}"
        );
        run(&mut s, "ROLLBACK");
        assert_eq!(
            scalar(&mut s, "INSERT INTO items(payload) VALUES (3) RETURNING id"),
            2
        );
        run(&mut s, "ALTER SEQUENCE items_id_seq RESTART WITH 20");
        drop(s);
        f.reopen();
        assert_eq!(
            scalar(
                &mut f.session(),
                "INSERT INTO items(payload) VALUES (4) RETURNING id"
            ),
            20,
            "{engine:?}"
        );
    }
}

#[test]
fn identity_column_restart_syntax_is_currently_rejected_without_mutation() {
    for engine in engines() {
        let f = Fixture::new(engine);
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE items(id bigint GENERATED BY DEFAULT AS IDENTITY, payload integer)",
        );
        assert_eq!(
            s.execute_at(
                "ALTER TABLE items ALTER COLUMN id RESTART WITH 10",
                &[],
                None,
                &clock()
            ),
            Err(CoreError::Sql(SqlError::Syntax)),
            "{engine:?}"
        );
        assert_eq!(
            scalar(&mut s, "INSERT INTO items(payload) VALUES (1) RETURNING id"),
            1
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn old_orphan_counter_remains_a_validation_error_pending_pre_release() {
    let mut f = Fixture::new(Engine::V2);
    let mut s = f.session();
    run(&mut s, "CREATE SEQUENCE ids");
    scalar(&mut s, "SELECT nextval('ids')");
    {
        let Backend::V2(db) = &f.admin.as_ref().unwrap().inner.backend else {
            unreachable!()
        };
        let mut txn = db.begin(Isolation::ReadCommitted);
        let parts = db.catalog().parts(&mut txn).unwrap();
        let (&id, def) = parts.ext.sequences.iter().next().unwrap();
        let key = engine::catalog_codec::counter_key(id, def.incarnation);
        let bytes = db.kv.get(Keyspace::Seq, &key).unwrap().unwrap();
        run(&mut s, "ALTER SEQUENCE ids RESTART WITH 10");
        // Recreate exactly the old writer's orphan: a valid old reservation
        // beside a replacement definition. Do not relax the on-disk validator.
        let mut batch = engine::kv::WriteBatch::default();
        batch.put(Keyspace::Seq, key, bytes);
        db.kv.write(batch, engine::kv::Durability::Sync).unwrap();
    }
    drop(s);
    f.admin.take();
    assert!(matches!(Admin::open(&f.path, Limits::default()),
        Err(CoreError::Corrupt(message)) if message.contains("sequence counter invariant")));
}

mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, relational::Value, sqlstate};

fn backends() -> Vec<Engine> {
    let mut engines = vec![Engine::Legacy];
    if cfg!(feature = "engine-rocksdb") {
        engines.push(Engine::V2);
    }
    engines
}
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn fixture(engine: Engine, suffix: &str) -> SqlSession {
    let admin =
        Admin::create_engine(common::fresh("replica-role-fk"), Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE p(id INT PRIMARY KEY)");
    run(
        &mut s,
        &format!(
            "CREATE TABLE c(id INT PRIMARY KEY, pid INT NOT NULL DEFAULT 1 REFERENCES p(id) {suffix}, n INT UNIQUE CHECK(n > 0))"
        ),
    );
    s
}
fn error(s: &mut SqlSession, sql: &str, code: &str) {
    assert_eq!(
        sqlstate(&s.execute(sql, &[], None).unwrap_err()),
        code,
        "{sql}"
    );
}
#[test]
fn child_insert_update_upsert_and_mode_reset() {
    for engine in backends() {
        let mut s = fixture(engine, "");
        error(&mut s, "INSERT INTO c VALUES(1, 99, 1)", "23503");
        run(&mut s, "BEGIN");
        s.set_replication_role_replica(true).unwrap();
        run(&mut s, "INSERT INTO c VALUES(1, 99, 1)");
        run(&mut s, "UPDATE c SET pid = 98 WHERE id = 1");
        run(
            &mut s,
            "INSERT INTO c VALUES(1, 97, 2) ON CONFLICT(id) DO UPDATE SET pid = excluded.pid, n = excluded.n",
        );
        run(
            &mut s,
            "INSERT INTO c VALUES(2, 96, 3) ON CONFLICT(id) DO UPDATE SET pid = excluded.pid",
        );
        run(&mut s, "COMMIT");
        error(&mut s, "INSERT INTO c VALUES(3, 95, 4)", "23503");
        run(&mut s, "BEGIN");
        s.set_replication_role_replica(true).unwrap();
        s.set_replication_role_replica(false).unwrap();
        error(&mut s, "UPDATE c SET pid = 94 WHERE id = 1", "23503");
        run(&mut s, "ROLLBACK");
        run(&mut s, "BEGIN");
        s.set_replication_role_replica(true).unwrap();
        run(&mut s, "ROLLBACK");
        run(&mut s, "BEGIN");
        error(&mut s, "INSERT INTO c VALUES(3, 95, 4)", "23503");
        run(&mut s, "ROLLBACK");
    }
}
#[test]
fn parent_actions_are_suspended() {
    for engine in backends() {
        for action in [
            "CASCADE",
            "SET NULL",
            "SET DEFAULT",
            "RESTRICT",
            "NO ACTION",
        ] {
            let mut s = fixture(engine, &format!("ON DELETE {action} ON UPDATE {action}"));
            run(&mut s, "INSERT INTO p VALUES(1)");
            run(&mut s, "INSERT INTO c VALUES(1, 1, 1)");
            run(&mut s, "BEGIN");
            s.set_replication_role_replica(true).unwrap();
            run(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
            run(&mut s, "DELETE FROM p WHERE id = 2");
            run(&mut s, "INSERT INTO p VALUES(1)");
            run(&mut s, "DELETE FROM p WHERE id = 1");
            run(&mut s, "COMMIT");
            let rows = s
                .execute("SELECT pid FROM c LIMIT 10", &[], None)
                .unwrap()
                .rows;
            assert_eq!(rows.len(), 1, "{action}");
            assert_eq!(rows[0].values, vec![Value::Int(1)], "{action}");
        }
    }
}
#[test]
fn non_trigger_constraints_still_fail() {
    for engine in backends() {
        for (sql, code) in [
            ("INSERT INTO c VALUES(2, 99, -1)", "23514"),
            ("INSERT INTO c VALUES(2, 99, 1)", "23505"),
            ("INSERT INTO c VALUES(1, 99, 2)", "23505"),
            ("INSERT INTO c VALUES(2, NULL, 2)", "23502"),
        ] {
            let mut s = fixture(engine, "");
            run(&mut s, "BEGIN");
            s.set_replication_role_replica(true).unwrap();
            run(&mut s, "INSERT INTO c VALUES(1, 99, 1)");
            error(&mut s, sql, code);
            run(&mut s, "ROLLBACK");
        }
    }
}
#[test]
fn deferred_checks_predating_replica_still_run() {
    for engine in backends() {
        for suffix in [
            "DEFERRABLE INITIALLY DEFERRED",
            "DEFERRABLE INITIALLY IMMEDIATE",
        ] {
            let mut s = fixture(engine, suffix);
            run(&mut s, "BEGIN");
            run(&mut s, "SET CONSTRAINTS ALL DEFERRED");
            run(&mut s, "INSERT INTO c VALUES(1, 99, 1)");
            s.set_replication_role_replica(true).unwrap();
            run(&mut s, "INSERT INTO c VALUES(2, 98, 2)");
            error(&mut s, "COMMIT", "23503");
            if s.in_transaction() {
                run(&mut s, "ROLLBACK");
            }
            run(&mut s, "BEGIN");
            s.set_replication_role_replica(true).unwrap();
            run(&mut s, "INSERT INTO c VALUES(3, 97, 3)");
            s.set_replication_role_replica(false).unwrap();
            run(&mut s, "COMMIT");
        }
    }
}
#[test]
fn setter_requires_active_transaction() {
    for engine in backends() {
        let mut s = fixture(engine, "");
        assert!(matches!(
            s.set_replication_role_replica(true),
            Err(CoreError::Sql(
                giggles_core::relational::SqlError::NoActiveTransaction
            ))
        ));
    }
}
#[test]
fn paged_insert_and_copy_skip_foreign_keys() {
    for engine in backends() {
        let mut s = fixture(engine, "DEFERRABLE INITIALLY DEFERRED");
        run(&mut s, "BEGIN");
        s.set_replication_role_replica(true).unwrap();
        let values = (1..=205)
            .map(|n| format!("({n}, 99, {n})"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO c VALUES {values}"));
        let (_, prepared) = s.prepare_copy("COPY c FROM STDIN").unwrap();
        s.ingest_copy(
            &prepared,
            vec![vec![Value::Int(206), Value::Int(99), Value::Int(206)]],
        )
        .unwrap();
        s.set_replication_role_replica(false).unwrap();
        run(&mut s, "COMMIT");
    }
}
#[test]
fn streaming_copy_skips_foreign_keys() {
    if !cfg!(feature = "engine-rocksdb") {
        return;
    }
    let mut s = fixture(Engine::V2, "DEFERRABLE INITIALLY DEFERRED");
    run(&mut s, "BEGIN");
    s.set_replication_role_replica(true).unwrap();
    let (_, prepared) = s.prepare_copy("COPY c FROM STDIN").unwrap();
    assert!(s.begin_copy_input().unwrap());
    let clock = giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: None,
        wall: None,
    };
    for n in 1..=2 {
        s.feed_copy_input_at(
            &prepared,
            vec![vec![Value::Int(n), Value::Int(99), Value::Int(n)]],
            &clock,
        )
        .unwrap();
    }
    s.finish_copy_input_at(&prepared, &clock).unwrap();
    s.set_replication_role_replica(false).unwrap();
    run(&mut s, "COMMIT");
}
#[test]
fn deferred_parent_check_predating_replica_still_runs() {
    for engine in backends() {
        let mut s = fixture(engine, "DEFERRABLE INITIALLY DEFERRED");
        run(&mut s, "INSERT INTO p VALUES(1)");
        run(&mut s, "INSERT INTO c VALUES(1, 1, 1)");
        run(&mut s, "BEGIN");
        run(&mut s, "DELETE FROM p WHERE id = 1");
        s.set_replication_role_replica(true).unwrap();
        error(&mut s, "COMMIT", "23503");
        if s.in_transaction() {
            run(&mut s, "ROLLBACK");
        }
    }
}

mod common;

use giggles_core::relational::Value;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlIsolation, SqlSession, sqlstate};

fn refuse(s: &mut SqlSession, sql: &str, command: &str) {
    let error = s.execute(sql, &[], None).unwrap_err();
    assert_refusal(error, sql, command);
}

fn assert_refusal(error: CoreError, sql: &str, command: &str) {
    assert_eq!(sqlstate(&error), "0A000", "{sql}: {error:?}");
    let CoreError::RoutineDdl(diagnostic) = error else {
        panic!("{sql}: {error:?}");
    };
    assert_eq!(
        diagnostic.message,
        format!("{command} on a table with triggers is not supported"),
    );
}

fn run(session: &mut SqlSession, sql: &str) -> giggles_core::relational::SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    run(session, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

const FUNCTION: &str =
    "CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$";

#[test]
fn trigger_admission_for_conflicts_merge_and_writable_ctes() {
    let path = common::fresh("trigger-write-plans");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE t (id int PRIMARY KEY, v int)",
        "CREATE TABLE src (id int PRIMARY KEY, v int)",
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO src VALUES (1, 20)",
        FUNCTION,
        "CREATE TRIGGER on_update BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION f()",
    ] {
        run(&mut session, sql);
    }
    let query = "SELECT id, v FROM t ORDER BY id";
    let before = rows(&mut session, query);
    for (sql, command) in [
        (
            "INSERT INTO public.t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
            "UPDATE",
        ),
        (
            "MERGE INTO t USING src ON t.id = src.id WHEN MATCHED THEN UPDATE SET v = src.v",
            "MERGE",
        ),
        (
            "WITH x AS (UPDATE t SET v = 30 RETURNING id) SELECT * FROM x",
            "UPDATE",
        ),
        (
            "UPDATE t SET v = src.v FROM src WHERE t.id = src.id",
            "UPDATE",
        ),
    ] {
        refuse(&mut session, sql, command);
        assert_eq!(rows(&mut session, query), before, "{sql}");
    }
    // A statement-level trigger applies even when the conflict would skip the row.
    run(
        &mut session,
        "CREATE TRIGGER on_insert BEFORE INSERT ON t FOR EACH STATEMENT EXECUTE FUNCTION f()",
    );
    run(
        &mut session,
        "CREATE TRIGGER on_delete BEFORE DELETE ON t FOR EACH ROW EXECUTE FUNCTION f()",
    );
    for (sql, command) in [
        (
            "INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO NOTHING",
            "INSERT",
        ),
        (
            "WITH x AS (INSERT INTO t VALUES (2, 20) RETURNING id) SELECT * FROM x",
            "INSERT",
        ),
        (
            "WITH x AS (DELETE FROM t RETURNING id) SELECT * FROM x",
            "DELETE",
        ),
        ("DELETE FROM t USING src WHERE t.id = src.id", "DELETE"),
    ] {
        refuse(&mut session, sql, command);
        assert_eq!(rows(&mut session, query), before, "{sql}");
    }
    // This base also refuses top-level WITH INSERT during planning.
    let sql = "WITH x AS (SELECT id, v FROM src) INSERT INTO t SELECT id + 1, v FROM x";
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), "0A000", "{sql}: {error:?}");
    assert_eq!(rows(&mut session, query), before, "{sql}");
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn trigger_admission_for_fk_actions_and_replica_skip() {
    let path = common::fresh("trigger-write-fk");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    run(&mut session, FUNCTION);
    for (action, sql, command) in [
        (
            "ON DELETE SET NULL",
            "DELETE FROM parent WHERE id = 1",
            "DELETE",
        ),
        (
            "ON DELETE SET DEFAULT",
            "DELETE FROM parent WHERE id = 1",
            "DELETE",
        ),
        (
            "ON UPDATE CASCADE",
            "UPDATE parent SET id = 3 WHERE id = 1",
            "UPDATE",
        ),
    ] {
        run(&mut session, "CREATE TABLE parent (id int PRIMARY KEY)");
        run(
            &mut session,
            &format!(
                "CREATE TABLE child (id int PRIMARY KEY, p int DEFAULT 2 REFERENCES parent(id) {action})",
            ),
        );
        run(&mut session, "INSERT INTO parent VALUES (1), (2)");
        run(&mut session, "INSERT INTO child VALUES (10, 1)");
        run(
            &mut session,
            "CREATE TRIGGER child_update BEFORE UPDATE OF p ON child FOR EACH ROW EXECUTE FUNCTION f()",
        );
        let parent = "SELECT id FROM parent ORDER BY id";
        let child = "SELECT id, p FROM child";
        let parents = rows(&mut session, parent);
        let children = rows(&mut session, child);
        refuse(&mut session, sql, command);
        assert_eq!(rows(&mut session, parent), parents, "{action}");
        assert_eq!(rows(&mut session, child), children, "{action}");

        run(&mut session, "BEGIN");
        session.set_replication_role_replica(true).unwrap();
        assert_eq!(run(&mut session, sql).affected, 1);
        assert_eq!(rows(&mut session, child), children, "{action}");
        run(&mut session, "ROLLBACK");
        run(&mut session, "DROP TABLE child");
        run(&mut session, "DROP TABLE parent");
    }
    for sql in [
        "CREATE TABLE parent (id int PRIMARY KEY)",
        "CREATE TABLE child (id int PRIMARY KEY REFERENCES parent(id) ON DELETE CASCADE)",
        "CREATE TABLE grandchild (id int PRIMARY KEY, p int REFERENCES child(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1)",
        "INSERT INTO child VALUES (1)",
        "INSERT INTO grandchild VALUES (2, 1)",
        "CREATE TRIGGER grandchild_delete BEFORE DELETE ON grandchild FOR EACH ROW EXECUTE FUNCTION f()",
    ] {
        run(&mut session, sql);
    }
    refuse(&mut session, "DELETE FROM parent WHERE id = 1", "DELETE");
    assert_eq!(
        rows(&mut session, "SELECT id FROM parent"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT id FROM child"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT id, p FROM grandchild"),
        vec![vec![Value::Int(2), Value::Int(1)]]
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn trigger_admission_uses_replication_role_and_update_columns() {
    let path = common::fresh("trigger-write-role");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE t (id int PRIMARY KEY, v int, unlisted int)",
        "INSERT INTO t VALUES (1, 10, 20)",
        FUNCTION,
        "CREATE TRIGGER tr BEFORE UPDATE OF v ON t FOR EACH ROW EXECUTE FUNCTION f()",
    ] {
        run(&mut session, sql);
    }
    let update = "UPDATE t SET v = 11";
    let query = "SELECT * FROM t";
    run(&mut session, "UPDATE t SET unlisted = 21");
    let before = rows(&mut session, query);
    run(&mut session, "BEGIN");
    session.set_replication_role_replica(true).unwrap();
    assert_eq!(run(&mut session, update).affected, 1);
    run(&mut session, "ROLLBACK");

    run(&mut session, "ALTER TABLE t ENABLE REPLICA TRIGGER tr");
    run(&mut session, "BEGIN");
    assert_eq!(run(&mut session, update).affected, 1);
    run(&mut session, "ROLLBACK");
    for state in ["REPLICA", "ALWAYS"] {
        run(
            &mut session,
            &format!("ALTER TABLE t ENABLE {state} TRIGGER tr"),
        );
        run(&mut session, "BEGIN");
        session.set_replication_role_replica(true).unwrap();
        refuse(&mut session, update, "UPDATE");
        run(&mut session, "ROLLBACK");
        assert_eq!(rows(&mut session, query), before);
    }
    run(&mut session, "ALTER TABLE t DISABLE TRIGGER tr");
    assert_eq!(run(&mut session, update).affected, 1);
    assert_eq!(
        rows(&mut session, query),
        vec![vec![Value::Int(1), Value::Int(11), Value::Int(21)]]
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn trigger_admission_rechecks_prepared_writes_and_own_ddl() {
    let path = common::fresh("trigger-write-prepare");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut writer = admin.sql_session();
    let mut ddl = admin.sql_session();
    run(&mut ddl, "CREATE TABLE t (id int PRIMARY KEY)");
    run(&mut ddl, FUNCTION);
    let prepared = writer.prepare("INSERT INTO t VALUES ($1)").unwrap();
    writer
        .execute_prepared(&prepared, &[Value::Int(1)], None)
        .unwrap();
    let create = "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()";
    run(&mut ddl, create);
    assert_refusal(
        writer
            .execute_prepared(&prepared, &[Value::Int(2)], None)
            .unwrap_err(),
        "prepared INSERT",
        "INSERT",
    );
    assert_eq!(
        rows(&mut writer, "SELECT id FROM t"),
        vec![vec![Value::Int(1)]]
    );

    run(&mut ddl, "DROP TRIGGER tr ON t");
    run(&mut writer, "BEGIN");
    run(&mut writer, create);
    refuse(&mut writer, "INSERT INTO t VALUES (2)", "INSERT");
    run(&mut writer, "ROLLBACK");
    assert_eq!(
        rows(&mut writer, "SELECT id FROM t"),
        vec![vec![Value::Int(1)]]
    );
    drop(writer);
    drop(ddl);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn trigger_admission_checks_committed_create_and_enable_after_snapshot() {
    let path = common::fresh("trigger-write-snapshots");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut writer = admin.sql_session();
    let mut ddl = admin.sql_session();
    for sql in [
        "CREATE TABLE t (id int PRIMARY KEY, v int)",
        "CREATE TABLE witness (id int PRIMARY KEY)",
        "CREATE TABLE child (id int PRIMARY KEY, p int REFERENCES t(id) ON DELETE SET NULL)",
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO child VALUES (2, 1)",
        FUNCTION,
    ] {
        run(&mut ddl, sql);
    }
    for isolation in [
        SqlIsolation::ReadCommitted,
        SqlIsolation::RepeatableRead,
        SqlIsolation::Serializable,
    ] {
        writer.set_isolation(isolation);
        for (create, before_snapshot, after_snapshot, write, command, table) in [
            (
                "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
                None,
                None,
                "INSERT INTO t VALUES (2, 20)",
                "INSERT",
                "t",
            ),
            (
                "CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION f()",
                Some("ALTER TABLE t DISABLE TRIGGER tr"),
                Some("ALTER TABLE t ENABLE TRIGGER tr"),
                "UPDATE t SET v = 20 WHERE id = 1",
                "UPDATE",
                "t",
            ),
            (
                "CREATE TRIGGER tr BEFORE DELETE ON t FOR EACH ROW EXECUTE FUNCTION f()",
                Some("ALTER TABLE t ENABLE REPLICA TRIGGER tr"),
                Some("ALTER TABLE t ENABLE ALWAYS TRIGGER tr"),
                "DELETE FROM t WHERE id = 1",
                "DELETE",
                "t",
            ),
            (
                "CREATE TRIGGER tr BEFORE UPDATE OF p ON child FOR EACH ROW EXECUTE FUNCTION f()",
                None,
                None,
                "DELETE FROM t WHERE id = 1",
                "DELETE",
                "child",
            ),
        ] {
            if let Some(change) = before_snapshot {
                run(&mut ddl, create);
                run(&mut ddl, change);
            }
            run(&mut writer, "BEGIN");
            rows(&mut writer, "SELECT id FROM witness");
            run(&mut ddl, after_snapshot.unwrap_or(create));
            refuse(&mut writer, write, command);
            run(&mut writer, "ROLLBACK");
            assert_eq!(
                rows(&mut ddl, "SELECT id, v FROM t"),
                vec![vec![Value::Int(1), Value::Int(10)]],
                "{isolation:?}: {write}"
            );
            assert_eq!(
                rows(&mut ddl, "SELECT id, p FROM child"),
                vec![vec![Value::Int(2), Value::Int(1)]],
                "{isolation:?}: {write}"
            );
            run(&mut ddl, &format!("DROP TRIGGER tr ON {table}"));
        }
    }
    drop(writer);
    drop(ddl);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn streaming_copy_rechecks_triggers_before_normalizing_and_before_ingesting() {
    use giggles_core::relational::{CopyInput, StatementClock};
    let path = common::fresh("trigger-copy-snapshot");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut writer = admin.sql_session();
    let mut ddl = admin.sql_session();
    for sql in [
        "CREATE TABLE target (id int PRIMARY KEY)",
        "CREATE TABLE witness (id int PRIMARY KEY)",
        "INSERT INTO target VALUES (1)",
        FUNCTION,
    ] {
        run(&mut ddl, sql);
    }
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: None,
        wall: None,
    };
    let create = "CREATE TRIGGER tr BEFORE INSERT ON target FOR EACH ROW EXECUTE FUNCTION f()";
    for normalize in [true, false] {
        writer.set_isolation(SqlIsolation::RepeatableRead);
        run(&mut writer, "BEGIN");
        rows(&mut writer, "SELECT id FROM witness");
        let (copy, prepared) = writer.prepare_copy("COPY target FROM STDIN").unwrap();
        writer.check_copy_input_triggers(&prepared).unwrap();
        assert!(writer.begin_copy_input().unwrap());
        run(&mut ddl, create);
        let error = if normalize {
            let input = CopyInput {
                rows: vec![vec![Some(Value::Int(2))]],
                options: copy.options,
                first_line: 1,
                skipped: 0,
            };
            writer
                .normalize_copy_cells_at(&prepared, input, &clock)
                .unwrap_err()
        } else {
            writer
                .feed_copy_input_at(&prepared, vec![vec![Value::Int(2)]], &clock)
                .unwrap_err()
        };
        assert_refusal(error, "COPY target FROM STDIN", "COPY FROM");
        run(&mut writer, "ROLLBACK");
        assert_eq!(
            rows(&mut ddl, "SELECT id FROM target"),
            vec![vec![Value::Int(1)]]
        );
        run(&mut ddl, "DROP TRIGGER tr ON target");
    }
    drop(writer);
    drop(ddl);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn trigger_admission_with_generated_text_search_column() {
    let path = common::fresh("trigger-generated-tsvector");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE docs (id int PRIMARY KEY, body text, tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED)",
    );
    run(&mut session, "INSERT INTO docs (id, body) VALUES (1, 'cats')");
    run(&mut session, FUNCTION);
    run(
        &mut session,
        "CREATE TRIGGER on_insert BEFORE INSERT ON docs FOR EACH ROW EXECUTE FUNCTION f()",
    );
    let query = "SELECT id, tsv::text FROM docs ORDER BY id";
    let before = vec![vec![Value::Int(1), Value::Text("'cat':1".into())]];
    assert_eq!(rows(&mut session, query), before);
    let write = "INSERT INTO docs (id, body) VALUES (2, 'dogs')";
    refuse(&mut session, write, "INSERT");
    assert_eq!(rows(&mut session, query), before);

    run(&mut session, "ALTER TABLE docs DISABLE TRIGGER on_insert");
    assert_eq!(run(&mut session, write).affected, 1);
    assert_eq!(
        rows(&mut session, query),
        [before[0].clone(), vec![Value::Int(2), Value::Text("'dog':1".into())]],
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn insert_select_uses_the_same_trigger_guard_as_values() {
    let path = common::fresh("trigger-insert-select");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE dst (id int PRIMARY KEY)",
        "INSERT INTO dst VALUES (3)",
        FUNCTION,
        "CREATE TRIGGER on_insert BEFORE INSERT ON dst FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TABLE source AS SELECT id FROM dst",
    ] {
        run(&mut session, sql);
    }
    for sql in [
        "INSERT INTO dst VALUES (4)",
        "INSERT INTO dst SELECT id + 1 FROM source",
    ] {
        refuse(&mut session, sql, "INSERT");
    }
    assert_eq!(rows(&mut session, "SELECT id FROM dst"), [vec![Value::Int(3)]]);
    run(&mut session, "ALTER TABLE dst DISABLE TRIGGER on_insert");
    assert_eq!(
        run(&mut session, "INSERT INTO dst SELECT id + 1 FROM source").affected,
        1,
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

mod common;

use giggles_core::{Admin, Engine, Limits, SqlSession, relational::take_sql_notices, sqlstate};

const FUNCTION: &str =
    "CREATE FUNCTION f1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$";
const TRIGGER: &str = "CREATE TRIGGER tr1 BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f1()";

fn run(session: &mut SqlSession, sql: &str) {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

#[cfg(feature = "engine-rocksdb")]
fn failure(
    session: &mut SqlSession,
    sql: &str,
    code: &str,
    message: &str,
) -> giggles_core::relational::RoutineDiagnostic {
    use giggles_core::CoreError;
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), code, "{sql}: {error:?}");
    let CoreError::RoutineDdl(diagnostic) = error else {
        panic!("{sql}: {error:?}");
    };
    assert_eq!(diagnostic.message, message, "{sql}");
    *diagnostic
}

#[test]
fn legacy_refuses_every_definition_without_publishing() {
    let path = common::fresh("triggers-legacy");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    let mut session = admin.sql_session();
    run(&mut session, "CREATE TABLE t (id int PRIMARY KEY)");
    let before = admin.catalog().unwrap();
    for sql in [
        FUNCTION,
        TRIGGER,
        "DROP TRIGGER IF EXISTS tr1 ON t",
        "DROP FUNCTION IF EXISTS f1()",
        "ALTER TRIGGER tr1 ON t RENAME TO tr2",
        "ALTER TABLE t ENABLE REPLICA TRIGGER ALL",
        "ALTER TABLE t DISABLE TRIGGER tr1",
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(sqlstate(&error), "0A000", "{sql}: {error:?}");
        assert_eq!(admin.catalog().unwrap(), before);
    }
    assert!(take_sql_notices().is_empty());
    drop(session);
    drop(admin);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(reopened.catalog().unwrap(), before);
    let metadata = reopened.catalog_presentation().unwrap();
    assert!(metadata.routines.is_empty());
    assert!(metadata.triggers.is_empty());
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn postgres_oracle_ddl_and_dependencies() {
    let path = common::fresh("triggers-ddl");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    run(&mut s, "CREATE VIEW vw AS SELECT * FROM t");
    run(&mut s, FUNCTION);
    let first = *s
        .catalog_metadata()
        .unwrap()
        .presentation
        .routines
        .keys()
        .next()
        .unwrap();
    failure(
        &mut s,
        FUNCTION,
        "42723",
        "function \"f1\" already exists with same argument types",
    );
    run(
        &mut s,
        "CREATE OR REPLACE FUNCTION f1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END $$",
    );
    let snapshot = s.catalog_metadata().unwrap().presentation;
    assert_eq!(snapshot.routines[&first].1.body, " BEGIN RETURN OLD; END ");
    run(
        &mut s,
        "CREATE FUNCTION public.f2() RETURNS trigger AS $body$ BEGIN RETURN NEW; END; $body$ LANGUAGE plpgsql",
    );
    run(
        &mut s,
        "CREATE FUNCTION f3() RETURNS TRIGGER LANGUAGE 'plpgsql' AS 'BEGIN RETURN NEW; END'",
    );
    run(
        &mut s,
        "CREATE FUNCTION f4() RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER AS $$ BEGIN RETURN NEW; END $$",
    );
    run(
        &mut s,
        "CREATE FUNCTION options() RETURNS trigger AS 'BEGIN RETURN NEW; END' IMMUTABLE LEAKPROOF RETURNS NULL ON NULL INPUT SECURITY DEFINER PARALLEL SAFE COST 23 LANGUAGE plpgsql",
    );
    let snapshot = s.catalog_metadata().unwrap().presentation;
    let (_, options) = snapshot
        .routines
        .values()
        .find(|(_, r)| r.name == "options")
        .unwrap();
    assert_eq!(options.volatility, b'i');
    assert!(options.leakproof && options.strict && options.security_definer);
    assert_eq!(options.parallel, b's');
    assert_eq!(f32::from_bits(options.cost_bits), 23.0);
    run(&mut s, "DROP FUNCTION options() RESTRICT");
    let d = failure(
        &mut s,
        "CREATE FUNCTION bad(a int) RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        "42P13",
        "trigger functions cannot have declared arguments",
    );
    assert_eq!(
        d.hint.as_deref(),
        Some("The arguments of the trigger can be accessed through TG_NARGS and TG_ARGV instead.")
    );
    assert_eq!(
        d.context.as_deref(),
        Some("compilation of PL/pgSQL function \"bad\" near line 1")
    );
    let d = failure(
        &mut s,
        "SELECT f1()",
        "0A000",
        "trigger functions can only be called as triggers",
    );
    assert_eq!(
        d.context.as_deref(),
        Some("compilation of PL/pgSQL function \"f1\" near line 1")
    );
    for (sql, code, message) in [
        (
            "CREATE FUNCTION bad() RETURNS trigger LANGUAGE nosuchlang AS $$ BEGIN RETURN NEW; END $$",
            "42704",
            "language \"nosuchlang\" does not exist",
        ),
        (
            "CREATE FUNCTION notrig() RETURNS int LANGUAGE sql AS 'SELECT 1'",
            "0A000",
            "LANGUAGE sql is not supported",
        ),
        (
            "CREATE TRIGGER tr1 BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION nosuch()",
            "42883",
            "function nosuch() does not exist",
        ),
        (
            "CREATE TRIGGER tr1 BEFORE INSERT ON nosuch FOR EACH ROW EXECUTE FUNCTION f1()",
            "42P01",
            "relation \"nosuch\" does not exist",
        ),
        (
            "CREATE TRIGGER tr1 BEFORE INSERT ON t EXECUTE FUNCTION random()",
            "42P17",
            "function random must return type trigger",
        ),
    ] {
        failure(&mut s, sql, code, message);
    }
    run(&mut s, TRIGGER);
    failure(
        &mut s,
        TRIGGER,
        "42710",
        "trigger \"tr1\" for relation \"t\" already exists",
    );
    let trigger_id = s.catalog_metadata().unwrap().presentation.triggers[0].id;
    run(
        &mut s,
        "CREATE OR REPLACE TRIGGER tr1 BEFORE UPDATE ON t FOR EACH ROW EXECUTE PROCEDURE f1()",
    );
    assert_eq!(
        s.catalog_metadata().unwrap().presentation.triggers[0].id,
        trigger_id
    );
    failure(
        &mut s,
        "CREATE TRIGGER tr2 BEFORE UPDATE OF nosuchcol ON t EXECUTE FUNCTION f1()",
        "42703",
        "column \"nosuchcol\" of relation \"t\" does not exist",
    );
    failure(
        &mut s,
        "CREATE TRIGGER tr2 BEFORE UPDATE OF v, v ON t EXECUTE FUNCTION f1()",
        "42701",
        "column \"v\" specified more than once",
    );
    let d = failure(
        &mut s,
        "CREATE TRIGGER tr2 BEFORE INSERT ON vw FOR EACH ROW EXECUTE FUNCTION f1()",
        "42809",
        "\"vw\" is a view",
    );
    assert_eq!(
        d.detail.as_deref(),
        Some("Views cannot have row-level BEFORE or AFTER triggers.")
    );
    let duplicate =
        "CREATE TRIGGER tr2 BEFORE INSERT OR INSERT ON t FOR EACH ROW EXECUTE FUNCTION f1()";
    let d = failure(
        &mut s,
        duplicate,
        "42601",
        "duplicate trigger events specified at or near \"INSERT\"",
    );
    assert_eq!(d.position, duplicate.find("OR INSERT").map(|at| at + 4));
    run(
        &mut s,
        "CREATE TRIGGER tr3 AFTER UPDATE OF v OR DELETE ON t FOR EACH ROW EXECUTE FUNCTION f1('a b', 'it''s', 7, 1.5, ident)",
    );
    run(
        &mut s,
        "CREATE TRIGGER tr4 BEFORE INSERT ON t EXECUTE FUNCTION f1()",
    );
    run(
        &mut s,
        "CREATE TRIGGER \"Tr Five\" AFTER INSERT OR UPDATE OR DELETE ON public.t FOR EACH STATEMENT EXECUTE FUNCTION public.f1()",
    );
    failure(
        &mut s,
        "DROP TRIGGER nosuch ON t",
        "42704",
        "trigger \"nosuch\" for table \"t\" does not exist",
    );
    run(&mut s, "DROP TRIGGER IF EXISTS nosuch ON t");
    assert_eq!(
        take_sql_notices()[0].0,
        "trigger \"nosuch\" for relation \"t\" does not exist, skipping"
    );
    failure(
        &mut s,
        "DROP TRIGGER tr1 ON nosuch",
        "42P01",
        "relation \"nosuch\" does not exist",
    );
    run(&mut s, "DROP TRIGGER IF EXISTS tr1 ON nosuch");
    assert_eq!(
        take_sql_notices()[0].0,
        "relation \"nosuch\" does not exist, skipping"
    );
    let d = failure(
        &mut s,
        "DROP FUNCTION f1()",
        "2BP01",
        "cannot drop function f1() because other objects depend on it",
    );
    assert_eq!(
        d.detail.as_deref(),
        Some(
            "trigger tr1 on table t depends on function f1()\ntrigger tr3 on table t depends on function f1()\ntrigger tr4 on table t depends on function f1()\ntrigger Tr Five on table t depends on function f1()"
        )
    );
    assert_eq!(
        d.hint.as_deref(),
        Some("Use DROP ... CASCADE to drop the dependent objects too.")
    );
    failure(
        &mut s,
        "DROP FUNCTION nosuch()",
        "42883",
        "function nosuch() does not exist",
    );
    run(&mut s, "DROP FUNCTION IF EXISTS nosuch()");
    assert_eq!(
        take_sql_notices()[0].0,
        "function nosuch() does not exist, skipping"
    );
    run(&mut s, "DROP FUNCTION f2");
    run(&mut s, "DROP FUNCTION f3(), f4()");
    run(&mut s, "DROP TRIGGER tr1 ON t");
    run(&mut s, "DROP FUNCTION f1() CASCADE");
    assert_eq!(take_sql_notices(), vec![("drop cascades to 3 other objects".into(),
        Some("drop cascades to trigger tr3 on table t\ndrop cascades to trigger tr4 on table t\ndrop cascades to trigger Tr Five on table t".into()), Some("00000"))]);
    assert!(
        s.catalog_metadata()
            .unwrap()
            .presentation
            .triggers
            .is_empty()
    );
    run(&mut s, FUNCTION);
    run(&mut s, TRIGGER);
    run(&mut s, "DROP TABLE t CASCADE");
    let metadata = s.catalog_metadata().unwrap().presentation;
    assert_eq!(metadata.routines.len(), 1);
    assert!(metadata.triggers.is_empty());
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
fn features(path: &std::path::Path) -> u64 {
    let options = rocksdb::Options::default();
    let names = rocksdb::DB::list_cf(&options, path).unwrap();
    let db = rocksdb::DB::open_cf_for_read_only(&options, path, names, false).unwrap();
    let value = db
        .get_cf(&db.cf_handle("meta").unwrap(), b"features")
        .unwrap()
        .unwrap();
    u64::from_be_bytes(value.as_slice().try_into().unwrap())
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_rollback_feature_bit_reopen_and_column_identity() {
    let path = common::fresh("triggers-reopen");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    run(&mut s, "BEGIN");
    run(&mut s, FUNCTION);
    run(&mut s, TRIGGER);
    assert_eq!(s.catalog_metadata().unwrap().presentation.triggers.len(), 1);
    run(&mut s, "ROLLBACK");
    assert!(
        s.catalog_metadata()
            .unwrap()
            .presentation
            .routines
            .is_empty()
    );
    drop(s);
    drop(admin);
    assert_eq!(features(&path) & (1 << 39), 0);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE SCHEMA ops");
    s.set_search_path("ops, public", "giggles");
    run(&mut s, FUNCTION);
    run(&mut s, TRIGGER);
    run(&mut s, "ALTER TRIGGER tr1 ON t RENAME TO tr7");
    for (action, flag) in [
        ("DISABLE TRIGGER tr7", b'D'),
        ("ENABLE REPLICA TRIGGER USER", b'R'),
        ("ENABLE ALWAYS TRIGGER tr7", b'A'),
        ("ENABLE TRIGGER ALL", b'O'),
    ] {
        run(&mut s, &format!("ALTER TABLE t {action}"));
        assert_eq!(
            s.catalog_metadata().unwrap().presentation.triggers[0].enabled,
            flag
        );
    }
    run(
        &mut s,
        "CREATE TRIGGER tr8 BEFORE UPDATE OF v ON t FOR EACH ROW WHEN (NEW.v > OLD.v) EXECUTE FUNCTION f1()",
    );
    run(&mut s, "ALTER TABLE t RENAME COLUMN v TO vv");
    let d = failure(
        &mut s,
        "ALTER TABLE t DROP COLUMN vv",
        "2BP01",
        "cannot drop column vv of table t because other objects depend on it",
    );
    assert_eq!(
        d.detail.as_deref(),
        Some("trigger tr8 on table t depends on column vv of table t")
    );
    assert_eq!(
        d.hint.as_deref(),
        Some("Use DROP ... CASCADE to drop the dependent objects too.")
    );
    let before = s.catalog_metadata().unwrap();
    assert_eq!(
        before.presentation.routines.values().next().unwrap().0,
        "ops"
    );
    assert_eq!(
        before.presentation.triggers[1].update_columns,
        vec![before.tables[0].columns[1].id]
    );
    let id = before.presentation.triggers[1].id;
    drop(s);
    drop(admin);
    assert_ne!(features(&path) & (1 << 39), 0);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    let mut s = reopened.sql_session();
    let after = s.catalog_metadata().unwrap();
    assert_eq!(after.presentation.routines, before.presentation.routines);
    assert_eq!(after.presentation.triggers, before.presentation.triggers);
    assert_eq!(after.presentation.triggers[1].id, id);
    run(&mut s, "ALTER TABLE t DROP COLUMN vv CASCADE");
    assert_eq!(s.catalog_metadata().unwrap().presentation.triggers.len(), 1);
    assert_eq!(
        take_sql_notices()[0].0,
        "drop cascades to trigger tr8 on table t"
    );
    drop(s);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn refused_variants_and_when_references() {
    let path = common::fresh("triggers-refused");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, v int)");
    run(&mut s, FUNCTION);
    for (sql, code, message) in [
        (
            "CREATE CONSTRAINT TRIGGER b AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f1()",
            "0A000",
            "CONSTRAINT triggers are not supported",
        ),
        (
            "CREATE TRIGGER b INSTEAD OF INSERT ON t FOR EACH ROW EXECUTE FUNCTION f1()",
            "0A000",
            "INSTEAD OF triggers are not supported",
        ),
        (
            "CREATE TRIGGER b AFTER TRUNCATE ON t EXECUTE FUNCTION f1()",
            "0A000",
            "TRUNCATE triggers are not supported",
        ),
        (
            "CREATE TRIGGER b AFTER UPDATE ON t REFERENCING NEW TABLE AS n EXECUTE FUNCTION f1()",
            "0A000",
            "REFERENCING transition tables are not supported",
        ),
        (
            "CREATE TRIGGER b BEFORE DELETE ON t FOR EACH ROW WHEN (NEW.v > 0) EXECUTE FUNCTION f1()",
            "42P17",
            "DELETE trigger's WHEN condition cannot reference NEW values",
        ),
        (
            "CREATE TRIGGER b BEFORE INSERT ON t FOR EACH ROW WHEN (OLD.v > 0) EXECUTE FUNCTION f1()",
            "42P17",
            "INSERT trigger's WHEN condition cannot reference OLD values",
        ),
        (
            "CREATE TRIGGER b BEFORE INSERT ON t WHEN (NEW.v > 0) EXECUTE FUNCTION f1()",
            "42P17",
            "statement trigger's WHEN condition cannot reference column values",
        ),
        (
            "ALTER TABLE t ENABLE TRIGGER b",
            "42704",
            "trigger \"b\" for table \"t\" does not exist",
        ),
        (
            "ALTER TRIGGER b ON t RENAME TO c",
            "42704",
            "trigger \"b\" for table \"t\" does not exist",
        ),
    ] {
        failure(&mut s, sql, code, message);
    }
    assert!(
        s.catalog_metadata()
            .unwrap()
            .presentation
            .triggers
            .is_empty()
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn postgres_oracle_function_body_compile_errors() {
    let path = common::fresh("triggers-compile");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for (name, body, token, message) in [
        (
            "bad1",
            " BEGIN RETURN NEW END ",
            "",
            "syntax error at end of input",
        ),
        (
            "bad2",
            " BEGIN IF TG_OP = 'X' THEN RETURN NEW; END ",
            "",
            "syntax error at end of input",
        ),
        (
            "bad3",
            " BEGIN /* caf\u{e9} */ DELETE FRM t; RETURN NEW; END ",
            "FRM",
            "syntax error at or near \"FRM\"",
        ),
        (
            "bad4",
            " RETURN NEW; ",
            "RETURN",
            "syntax error at or near \"RETURN\"",
        ),
    ] {
        let sql = format!(
            "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $body${body}$body$"
        );
        let diagnostic = failure(&mut s, &sql, "42601", message);
        let offset = if token.is_empty() {
            sql.rfind("$body$").unwrap()
        } else {
            sql.find("$body$").unwrap() + "$body$".len() + body.find(token).unwrap()
        };
        assert_eq!(diagnostic.position, Some(sql[..offset].chars().count() + 1));
        assert_eq!(diagnostic.context, None);
        assert_eq!(diagnostic.hint, None);
    }
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

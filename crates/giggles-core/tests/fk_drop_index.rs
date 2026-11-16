mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, sqlstate};
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn snapshot(s: &SqlSession) -> String {
    let m = s.catalog_metadata().unwrap();
    format!("{:?}", (m.tables, m.indexes, m.presentation))
}
fn exercise(engine: Engine) {
    let path = common::fresh("fk-drop-index");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE p(id int NOT NULL)", "CREATE UNIQUE INDEX chosen ON p(id)",
        "INSERT INTO p VALUES(1)",
        "CREATE TABLE c(id int CONSTRAINT chosen_fk REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE d(id int)",
        "ALTER TABLE d ADD CONSTRAINT nv_fk FOREIGN KEY(id) REFERENCES p(id) NOT VALID",
        "ALTER TABLE c ADD CONSTRAINT second_fk FOREIGN KEY(id) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED",
        "INSERT INTO c VALUES(1)", "INSERT INTO d VALUES(1)",
        "ALTER TABLE p ADD CONSTRAINT p_pk PRIMARY KEY(id)",
        "CREATE UNIQUE INDEX equivalent ON p(id)",
        "CREATE TABLE kept(id int CONSTRAINT kept_fk REFERENCES p)",
        "INSERT INTO kept VALUES(1)",
    ] { run(&mut s, sql); }
    let targets = s.foreign_key_targets().unwrap();
    assert_eq!(targets["chosen_fk"], "index:chosen");
    assert_eq!(targets["nv_fk"], "index:chosen");
    assert_eq!(targets["second_fk"], "index:chosen");
    assert_eq!(targets["kept_fk"], "pk:p_pk");
    for suffix in ["", " RESTRICT", " CASCADE"] {
        run(&mut s, "CREATE UNIQUE INDEX unused ON p(id)");
        run(&mut s, &format!("DROP INDEX unused{suffix}"));
    }
    let before = snapshot(&s);
    for sql in ["DROP INDEX chosen", "DROP INDEX chosen RESTRICT", "DROP INDEX IF EXISTS chosen"] {
        assert_eq!(sqlstate(&s.execute(sql, &[], None).unwrap_err()), "2BP01");
        assert_eq!(snapshot(&s), before);
        assert_eq!(s.foreign_key_targets().unwrap(), targets);
    }
    run(&mut s, "BEGIN");
    run(&mut s, "DROP INDEX chosen CASCADE");
    run(&mut s, "ROLLBACK");
    assert_eq!(s.foreign_key_targets().unwrap(), targets);
    assert_eq!(snapshot(&s), before);
    run(&mut s, "BEGIN");
    // Leave a deferred violation queued, then remove its constraint.
    run(&mut s, "INSERT INTO c VALUES(999)");
    run(&mut s, "DROP INDEX chosen CASCADE");
    run(&mut s, "COMMIT");
    let remaining = s.foreign_key_targets().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining["kept_fk"], targets["kept_fk"]);
    assert!(!s.catalog_metadata().unwrap().indexes.iter().any(|i| i.name == "chosen"));
    for table in ["c", "d"] {
        assert!(!s.execute(&format!("SELECT * FROM {table}"), &[], None).unwrap().rows.is_empty());
        run(&mut s, &format!("INSERT INTO {table} VALUES(998)"));
    }
    assert_eq!(sqlstate(&s.execute("INSERT INTO kept VALUES(999)", &[], None).unwrap_err()), "23503");
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(s.foreign_key_targets().unwrap(), remaining);
    assert_eq!(sqlstate(&s.execute("DROP INDEX chosen", &[], None).unwrap_err()), "42704");
    run(&mut s, "INSERT INTO c VALUES(997)");
    drop(s); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_cascade() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_cascade() { exercise(Engine::V2); }

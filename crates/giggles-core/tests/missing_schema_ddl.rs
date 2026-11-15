//! Missing-schema SQLSTATEs from the PG18.6 oracle in sql-fixes/pg/bug2{,b}.out.
mod common;
use giggles_core::{Admin, Engine, Limits};

fn cases() -> Vec<(String, &'static str)> {
    let mut rows = Vec::new();
    for action in ["ADD CONSTRAINT c CHECK(id>0)", "DROP CONSTRAINT c", "ADD COLUMN b int",
        "DROP COLUMN a", "RENAME TO u", "RENAME COLUMN a TO b", "ALTER COLUMN a SET NOT NULL",
        "ALTER COLUMN a SET DEFAULT 1"] {
        rows.push((format!("ALTER TABLE nope.t {action}"), "3F000"));
    }
    for sql in [
        "ALTER TABLE app.k ADD CONSTRAINT bad FOREIGN KEY(pid) REFERENCES nope.t(id)",
        "CREATE TABLE app.k2(pid int REFERENCES nope.t(id))",
        "CREATE VIEW nope.v AS SELECT id FROM t", "CREATE OR REPLACE VIEW nope.v AS SELECT id FROM t",
        "DROP VIEW nope.v",
        "CREATE INDEX i1 ON nope.t(id)", "CREATE INDEX IF NOT EXISTS i1 ON nope.t(id)",
        "DROP INDEX nope.i", "DROP TABLE nope.t",
        "CREATE TABLE nope.t(id int)", "CREATE TABLE IF NOT EXISTS nope.t(id int)",
        "COMMENT ON TABLE nope.t IS 'x'", "COMMENT ON COLUMN nope.t.a IS 'x'",
        "COMMENT ON VIEW nope.v IS 'x'",
        "TRUNCATE nope.t",
    ] { rows.push((sql.into(), "3F000")); }
    for sql in ["DROP VIEW IF EXISTS nope.v", "DROP VIEW IF EXISTS nope.v, public.zz",
        "DROP INDEX IF EXISTS nope.i",
        "DROP TABLE IF EXISTS nope.t",
        "DROP TABLE IF EXISTS nope.t, app.t"] { rows.push((sql.into(), "OK")); }
    for sql in ["SELECT * FROM nope.t", "INSERT INTO nope.t VALUES(1)",
        "UPDATE nope.t SET a=1", "DELETE FROM nope.t", "CREATE VIEW app.v2 AS SELECT * FROM nope.t",
        "ALTER TABLE app.k ADD CONSTRAINT bad FOREIGN KEY(pid) REFERENCES app.nope(id)",
        "ALTER TABLE public.nope ADD CONSTRAINT c CHECK(true)", "DROP VIEW public.nope",
        "COMMENT ON TABLE public.nope IS 'x'"] {
        rows.push((sql.into(), "42P01"));
    }
    rows.push(("DROP INDEX public.nope".into(), "42704"));
    rows
}

#[test]
fn ddl_schema_errors() {
    let mut engines = vec![Engine::Legacy];
    if cfg!(feature = "engine-rocksdb") { engines.push(Engine::V2); }
    for engine in engines {
        let path = common::fresh("missing-schema-ddl");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        for sql in ["CREATE SCHEMA app", "CREATE TABLE t(id int PRIMARY KEY, a int)",
            "CREATE TABLE app.t(id int PRIMARY KEY)", "CREATE TABLE app.k(id int PRIMARY KEY, pid int)"] {
            session.execute(sql, &[], None).unwrap();
        }
        let mut failures = Vec::new();
        for (sql, expected) in cases() {
            // Legacy has no view/comment executor, even with an existing schema.
            if engine == Engine::Legacy && (sql.starts_with("DROP VIEW IF EXISTS")
                || sql == "DROP VIEW public.nope" || sql == "COMMENT ON TABLE public.nope IS 'x'") { continue; }
            let actual = session.execute(&sql, &[], None).map(|_| "OK".to_owned())
                .unwrap_or_else(|err| giggles_core::sqlstate(&err).to_owned());
            println!("{engine:?} | {sql} | {actual} | {expected}");
            if sql == "DROP TABLE IF EXISTS nope.t, app.t" {
                assert!(session.execute("SELECT * FROM app.t", &[], None).is_err());
            }
            if actual != expected { failures.push(format!("{sql}: {actual} != {expected}")); }
        }
        drop(session); drop(admin);
        std::fs::remove_dir_all(path).unwrap();
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}

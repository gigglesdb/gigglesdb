mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, sqlstate};
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn exercise(engine: Engine) {
    let path = common::fresh("fk-drop-constraint");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in [
        "CREATE TABLE p(id int NOT NULL CONSTRAINT u UNIQUE)",
        "INSERT INTO p VALUES(1)",
        "CREATE TABLE c(id int CONSTRAINT chosen_fk REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE d(id int)",
        "ALTER TABLE d ADD CONSTRAINT nv_fk FOREIGN KEY(id) REFERENCES p(id) NOT VALID",
        "ALTER TABLE p ADD CONSTRAINT pk PRIMARY KEY(id)",
        "CREATE UNIQUE INDEX equivalent ON p(id)",
        "CREATE TABLE kept(id int CONSTRAINT kept_fk REFERENCES p)",
    ] { run(&mut s, sql); }
    let targets = s.foreign_key_targets().unwrap();
    assert_eq!(targets["chosen_fk"], "index:u");
    assert_eq!(targets["nv_fk"], "index:u");
    assert_eq!(targets["kept_fk"], "pk:pk");
    for (sql, code) in [
        ("ALTER TABLE p DROP CONSTRAINT u", "2BP01"),
        ("ALTER TABLE p DROP CONSTRAINT u RESTRICT", "2BP01"),
        ("ALTER TABLE p DROP CONSTRAINT u CASCADE RESTRICT", "42601"),
        ("ALTER TABLE p DROP CONSTRAINT u RESTRICT CASCADE", "42601"),
        ("ALTER TABLE p DROP CONSTRAINT u CASCADE, DROP CONSTRAINT missing", "42704"),
        ("ALTER TABLE p DROP CONSTRAINT pk", "0A000"),
        ("ALTER TABLE p DROP CONSTRAINT pk CASCADE", "0A000"),
        ("ALTER TABLE p DROP CONSTRAINT pk RESTRICT", "0A000"),
    ] {
        assert_eq!(sqlstate(&s.execute(sql, &[], None).unwrap_err()), code, "{sql}");
        assert_eq!(s.foreign_key_targets().unwrap(), targets);
    }
    for suffix in ["CASCADE", "RESTRICT"] {
        run(&mut s, &format!("ALTER TABLE p DROP CONSTRAINT IF EXISTS missing {suffix}"));
        assert_eq!(s.foreign_key_targets().unwrap(), targets);
    }
    run(&mut s, "BEGIN");
    run(&mut s, "ALTER TABLE p DROP CONSTRAINT u CASCADE");
    assert_eq!(s.foreign_key_targets().unwrap().len(), 1);
    run(&mut s, "ROLLBACK");
    assert_eq!(s.foreign_key_targets().unwrap(), targets);
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO c VALUES(999)");
    run(&mut s, "ALTER TABLE p DROP CONSTRAINT u CASCADE");
    run(&mut s, "COMMIT");
    let remaining = s.foreign_key_targets().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining["kept_fk"], targets["kept_fk"]);
    assert!(!s.catalog_metadata().unwrap().indexes.iter().any(|i| i.name == "u"));
    // Same-column FK bound to the PK must not block an unrelated UNIQUE.
    for suffix in ["", " RESTRICT", " CASCADE"] {
        run(&mut s, "ALTER TABLE p ADD CONSTRAINT u UNIQUE(id)");
        run(&mut s, &format!("ALTER TABLE p DROP CONSTRAINT u{suffix}"));
        assert_eq!(s.foreign_key_targets().unwrap(), remaining);
    }
    // A binding to a different secondary index also leaves UNIQUE free to drop.
    run(&mut s, "CREATE TABLE q(id int)");
    run(&mut s, "CREATE UNIQUE INDEX first_key ON q(id)");
    run(&mut s, "CREATE TABLE qc(id int CONSTRAINT other_fk REFERENCES q(id))");
    for suffix in ["", " RESTRICT", " CASCADE"] {
        run(&mut s, "ALTER TABLE q ADD CONSTRAINT qu UNIQUE(id)");
        run(&mut s, &format!("ALTER TABLE q DROP CONSTRAINT qu{suffix}"));
        assert_eq!(s.foreign_key_targets().unwrap()["other_fk"], "index:first_key");
    }
    run(&mut s, "ALTER TABLE qc DROP CONSTRAINT other_fk RESTRICT");
    run(&mut s, "CREATE TABLE extra(id int CONSTRAINT ef REFERENCES p, CONSTRAINT ck CHECK(id>0))");
    run(&mut s, "ALTER TABLE extra DROP CONSTRAINT ef CASCADE, DROP CONSTRAINT ck CASCADE");
    assert_eq!(s.foreign_key_targets().unwrap(), remaining);
    run(&mut s, "INSERT INTO extra VALUES(-1)");
    assert_eq!(sqlstate(&s.execute("INSERT INTO kept VALUES(999)", &[], None).unwrap_err()), "23503");
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(s.foreign_key_targets().unwrap(), remaining);
    run(&mut s, "INSERT INTO c VALUES(998)");
    drop(s); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_cascade() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_cascade() { exercise(Engine::V2); }

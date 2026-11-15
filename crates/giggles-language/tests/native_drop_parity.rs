//! Paired DROP observations use identical SQL fixtures; native execution forbids SQL parsing.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::{Numeric, with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, Value,
    capability_manifest_for, parse_catalog_with_version, parse_input_with_version, profile_for};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};

const VERSION: LanguageVersion = LanguageVersion::Embedded017;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-drop-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn storages() -> Vec<Storage> {
    vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2]
}
fn snapshot(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap();
    m.presentation.epoch = 0; // V2 epochs are per-database HLCs, not logical catalog state.
    // Fixture IDs are deterministic on both paths; include FK/view definitions and indexes.
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.presentation));
    for table in m.tables {
        let rows = session.execute(&format!("select * from {} order by id", table.name), &[], None)
            .unwrap_or_else(|e| panic!("snapshot table {}: {e:?}", table.name)).rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        out.push_str(&format!("{rows:?}"));
    }
    out
}
fn equal(left: &Admin, right: &Admin) -> String {
    let expected = snapshot(&mut left.sql_session());
    assert_eq!(snapshot(&mut right.sql_session()), expected);
    expected
}
fn step(left: &Admin, right: &Admin, text: &str, succeeds: bool) {
    let before = equal(left, right);
    let expected = left.sql_session().execute(text, &[], None);
    let actual = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION)
        .execute_catalog_text(text));
    assert_eq!(actual.is_ok(), succeeds, "{text}: {actual:?}");
    match expected {
        Ok(out) => { actual.unwrap(); assert_eq!(out.affected, 0); }
        Err(error) => {
            let sqlstate = match text {
                "drop table missing" => "42P01",
                "drop index missing" => "42704",
                "drop index parent_unique_idx cascade" => "0A000",
                _ => "2BP01",
            };
            assert_eq!(giggles_core::sqlstate(&error), sqlstate, "{text}");
            assert_eq!(actual.unwrap_err().sqlstate(), Some(giggles_core::sqlstate(&error)), "{text}");
            assert_eq!(equal(left, right), before, "failed DROP is atomic");
        }
    }
    equal(left, right);
}
fn reopen(left_db: &Database, left: Admin, right_db: &Database, right: Admin) {
    let before = equal(&left, &right);
    drop(left); drop(right);
    let left = Admin::open(&left_db.0, Limits::default()).unwrap();
    let right = Admin::open(&right_db.0, Limits::default()).unwrap();
    assert_eq!(equal(&left, &right), before);
}
#[test]
fn drop_matrix_matches_sql() {
    for storage in storages() {
        let (left_db, left) = Database::fresh(storage);
        let (right_db, right) = Database::fresh(storage);
        for ddl in ["create table parent(id bigint primary key, u bigint constraint parent_uq unique)",
            "create table child(id bigint primary key,pid bigint references parent(id))",
            "create table scratch(id bigint primary key)",
            "create index scratch_idx on scratch(id)", "create index parent_idx on parent(id)",
            "create index parent_cascade_idx on parent(id)",
            "create unique index parent_unique_idx on parent(id)"] {
            for admin in [&left, &right] { admin.sql_session().execute(ddl, &[], None).unwrap(); }
            equal(&left, &right);
        }
        if storage == Storage::V2 {
            for text in ["insert into parent(id) values(1)", "insert into child values(1,1)",
                "insert into scratch values(1)"] {
                for admin in [&left, &right] { admin.sql_session().execute(text, &[], None).unwrap(); }
                equal(&left, &right);
            }
        }
        step(&left, &right, "drop table scratch", true);
        let before = equal(&left, &right);
        let mut sql = left.sql_session();
        let mut native = right.sql_session();
        sql.execute("begin", &[], None).unwrap();
        native.execute("begin", &[], None).unwrap();
        // Native catalog calls commit standalone. Public SQL transactions on both
        // resulting catalogs must restore dropped tables, indexes, FK constraints and rows.
        for text in ["drop index parent_idx", "drop table parent cascade"] {
            sql.execute(text, &[], None).unwrap();
            native.execute(text, &[], None).unwrap();
            // Legacy metadata remains committed-only during a transaction.
            if storage == Storage::V2 { assert_eq!(snapshot(&mut native), snapshot(&mut sql)); }
            else { assert_eq!(equal(&left, &right), before); }
        }
        sql.execute("rollback", &[], None).unwrap();
        native.rollback();
        assert_eq!(equal(&left, &right), before);
        drop(sql); drop(native);
        for (text, succeeds) in [("drop table parent restrict", false), ("drop table parent", false),
            ("drop table missing", false), ("drop index missing", false),
            ("drop index parent_pkey", false), ("drop index parent_pkey restrict", false),
            ("drop index parent_pkey cascade", false), ("drop index parent_uq", false),
            ("drop index parent_uq restrict", false), ("drop index parent_uq cascade", false),
            ("drop index parent_unique_idx cascade", false), // Standalone UNIQUE CASCADE remains 0A000.
            ("drop index parent_unique_idx restrict", true),
            ("drop index parent_cascade_idx cascade", true),
            ("drop index parent_idx restrict", true), ("drop table parent cascade", true)] {
            step(&left, &right, text, succeeds);
        }
        assert_eq!(left.catalog().unwrap().1.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["child"]);
        assert!(left.sql_session().catalog_metadata().unwrap().presentation.constraints.is_empty());
        assert!(left.secondary_index_catalog().unwrap().1.is_empty());
        for admin in [&left, &right] {
            admin.sql_session().execute("insert into child values(2,999)", &[], None).unwrap();
        }
        reopen(&left_db, left, &right_db, right);
    }
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn dependent_views_restrict_cascade_and_reopen() {
    let (left_db, left) = Database::fresh(Storage::V2);
    let (right_db, right) = Database::fresh(Storage::V2);
    for text in ["create table items(id bigint primary key)", "insert into items values(1)",
        "create view visible as select id from items", "create view nested as select id from visible"] {
        for admin in [&left, &right] { admin.sql_session().execute(text, &[], None).unwrap(); }
        equal(&left, &right);
    }
    step(&left, &right, "drop table items restrict", false);
    assert_eq!(left.sql_session().catalog_metadata().unwrap().presentation.views.len(), 2);
    step(&left, &right, "drop table items cascade", true);
    assert!(left.sql_session().catalog_metadata().unwrap().presentation.views.is_empty());
    reopen(&left_db, left, &right_db, right);
}
#[test]
fn drop_profile_is_opt_in_and_inherits_numeric() {
    for text in ["drop table items", "drop index items_idx cascade", "drop table items restrict"] {
        let tree = parse_catalog_with_version(text, VERSION).unwrap();
        assert!(matches!(parse_input_with_version(text, VERSION), Ok(giggles_language::ParsedInput::Catalog(_))));
        let (_db, admin) = Database::fresh(Storage::Legacy);
        for minor in 1..=16 {
            let old = serde_json::from_str(&format!("\"0.{minor}\"")).unwrap();
            assert!(parse_catalog_with_version(text, old).is_err());
            assert!(parse_input_with_version(text, old).is_err());
            assert_eq!(Engine::with_version(admin.clone(), old).execute_catalog(&tree), Err(Error::Unsupported));
        }
    }
    for text in ["drop table items cascade restrict", "drop table if exists items",
        "drop index concurrently idx", "drop table public.items", "drop table a,b"] {
        assert!(parse_catalog_with_version(text, VERSION).is_err(), "{text}");
    }
    assert_eq!(serde_json::to_string(&VERSION).unwrap(), "\"0.17\"");
    assert_eq!(profile_for(VERSION).scalar_types, profile_for(LanguageVersion::Embedded016).scalar_types);
    assert!(capability_manifest_for(VERSION).capabilities.iter().any(|c| c.feature == "catalog_drop"));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(["capability-manifest", "--language-version", "0.17"]).output().unwrap();
    assert!(output.status.success());
    let manifest: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(manifest["language_version"], "0.17");
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let engine = Engine::with_version(admin.clone(), VERSION);
        with_sql_parser_forbidden(|| {
            engine.execute_catalog_text("create table amounts(id int64,n numeric,primary key(id))").unwrap();
            let bindings = Bindings::from([("n".into(), Value::Numeric(Numeric::parse("1.25").unwrap()))]);
            assert_eq!(engine.execute_text("insert amounts set id=1,n=$n", &bindings).unwrap().affected, 1);
            engine.execute_transaction_text("transaction { update amounts where id=1 set n=$n; }", &bindings).unwrap();
        });
        let rows = admin.sql_session().execute("select n from amounts", &[], None).unwrap().rows;
        assert_eq!(rows[0].values, [Value::Numeric(Numeric::parse("1.25").unwrap())]);
    }
}

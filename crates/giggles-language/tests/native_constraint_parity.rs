use giggles_core::{Admin, Engine as Storage, Limits, SqlSession, relational::with_sql_parser_forbidden};
use giggles_language::{Bindings, AlterTableAction, CatalogStatement, Engine, Error, LanguageVersion,
    capability_manifest_for, parse_catalog_with_version};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-constraint-{}-{}",
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
        let rows = session.execute(&format!("select * from {}.{}", m.schemas.tables.get(&table.id).map(String::as_str).unwrap_or("public"), table.name), &[], None)
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
fn step(left: &Admin, right: &Admin, native: &str, sql: &str, state: Option<&str>) {
    let before = equal(left, right);
    let expected = left.sql_session().execute(sql, &[], None);
    let actual = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_catalog_text(native));
    if let Some(state) = state {
        assert_eq!(giggles_core::sqlstate(&expected.unwrap_err()), state);
        assert_eq!(actual.unwrap_err().sqlstate(), Some(state));
        assert_eq!(equal(left, right), before);
    } else { assert_eq!(expected.unwrap().affected, 0); actual.unwrap(); }
    equal(left, right);
}
#[test]
fn constraint_matrix_matches_sql() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        for a in [&left, &right] {
            for sql in ["create table parent(id bigint primary key)", "create table child(id bigint primary key,parent_id bigint,qty bigint,code text)"] {
                a.sql_session().execute(sql, &[], None).unwrap();
            }
            if storage == Storage::V2 { for sql in ["insert into parent values(1)", "insert into child values(9,1,1,'seed')"] { a.sql_session().execute(sql, &[], None).unwrap(); } }
        }
        for text in ["alter table child add constraint uq unique(code)", "alter table child add check(qty > 0)",
            "alter table child add constraint fk foreign key(parent_id) references parent(id) on delete cascade on update restrict"] {
            step(&left, &right, text, text, None);
        }
        if storage == Storage::Legacy { for a in [&left, &right] { a.sql_session().execute("insert into parent values(1)", &[], None).unwrap(); } }
        for (native, sql, state) in [("insert child set id=1,parent_id=1,qty=1,code=\"a\"", "insert into child values(1,1,1,'a')", None),
            ("insert child set id=2,parent_id=1,qty=1,code=\"a\"", "insert into child values(2,1,1,'a')", Some("23505")),
            ("insert child set id=2,parent_id=1,qty=0,code=\"b\"", "insert into child values(2,1,0,'b')", Some("23514")),
            ("insert child set id=2,parent_id=99,qty=1,code=\"b\"", "insert into child values(2,99,1,'b')", Some("23503")),
            ("delete parent where id=1", "delete from parent where id=1", None)] {
            let l = left.sql_session().execute(sql, &[], None);
            let r = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_text(native, &Bindings::new()));
            if let Some(state) = state {
                let expected = l.unwrap_err(); let actual = r.unwrap_err();
                assert_eq!(giggles_core::sqlstate(&expected), state);
                assert_eq!(actual.sqlstate(), Some(giggles_core::sqlstate(&expected)));
                // Native DML preserves its existing Duplicate identity for SQLSTATE 23505.
                assert_eq!(actual, if state == "23505" { Error::Duplicate } else { Error::Sql { sqlstate: state.into() } });
            }
            else { assert_eq!(l.unwrap().affected, r.unwrap().affected as u64); }
            equal(&left, &right);
        }
        assert!(right.sql_session().execute("select * from child", &[], None).unwrap().rows.is_empty());
        step(&left, &right, "alter table child drop constraint uq", "alter table child drop constraint uq", None);
        step(&left, &right, "alter table child drop constraint nope", "alter table child drop constraint nope", Some("42704"));
        let engine = Engine::with_version(right.clone(), VERSION);
        let optional = parse_catalog_with_version("alter table child drop constraint if exists nope", VERSION).unwrap();
        assert_eq!(engine.execute_catalog(&optional), Err(Error::Unsupported));
        assert!(matches!(engine.prepare_catalog_bound(&optional), Err(Error::Unsupported)));
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog_optional(&optional)).unwrap(), None);
        left.sql_session().execute("alter table child drop constraint if exists nope", &[], None).unwrap();
        let missing = "alter table absent drop constraint if exists nope";
        assert_eq!(giggles_core::sqlstate(&left.sql_session().execute(missing, &[], None).unwrap_err()), "42P01");
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog_optional_text(missing)).unwrap_err(), Error::Sql { sqlstate: "42P01".into() });
        step(&left, &right, "alter table child drop constraint fk cascade", "alter table child drop constraint fk cascade", Some("42601"));
        for a in [&left, &right] { for sql in ["create schema app", "create table app.t(id bigint primary key,v bigint)"] { a.sql_session().execute(sql, &[], None).unwrap(); } }
        step(&left, &right, "alter table app.t add constraint u unique(v)", "alter table app.t add constraint u unique(v)", None);
        for a in [&left, &right] { a.sql_session().execute("insert into app.t values(1,2),(2,2)", &[], None).unwrap_err(); }
        if storage == Storage::V2 {
            for a in [&left, &right] { for sql in ["create table dup(id bigint primary key,v bigint)", "insert into dup values(1,2),(2,2)"] { a.sql_session().execute(sql, &[], None).unwrap(); } }
            step(&left, &right, "alter table dup add unique(v)", "alter table dup add unique(v)", Some("23505"));
        } else {
            for a in [&left, &right] { a.sql_session().execute("insert into app.t values(1,2)", &[], None).unwrap(); }
            let before = snapshot(&mut right.sql_session());
            assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog_text("alter table app.t add check(v > 0)")), Err(Error::PlanRequired));
            assert_eq!(snapshot(&mut right.sql_session()), before);
            left.sql_session().execute("alter table app.t add check(v > 0)", &[], None).unwrap();
            assert_ne!(snapshot(&mut left.sql_session()), before);
        }
    }
}
#[test]
fn constraint_syntax_is_opt_in() {
    let old = parse_catalog_with_version("alter table t add constraint uq unique(code)", LanguageVersion::Embedded019);
    assert_eq!(old, Err(Error::Unsupported));
    assert!(matches!(parse_catalog_with_version("alter table t add column unique int64", VERSION).unwrap(), CatalogStatement::AlterTable { action: AlterTableAction::AddColumn(_), .. }));
    for action in ["no action", "restrict", "cascade", "set null", "set default"] {
        for suffix in [format!("on delete {action} on update restrict"), format!("on update {action} on delete restrict")] {
            parse_catalog_with_version(&format!("alter table t add foreign key(x) references p {suffix}"), VERSION).unwrap();
        }
    }
    assert!(parse_catalog_with_version("alter table t add foreign key(x) references p on delete cascade on delete restrict", VERSION).is_err());
    let (_db, admin) = Database::fresh(Storage::Legacy);
    admin.sql_session().execute("create table t(id bigint primary key,qty bigint)", &[], None).unwrap();
    assert_eq!(with_sql_parser_forbidden(|| Engine::with_version(admin.clone(), VERSION).execute_catalog_text("alter table t add check(qty > id)")), Err(Error::Unsupported));
    admin.sql_session().execute("alter table t add check(qty > id)", &[], None).unwrap();
    assert_eq!(capability_manifest_for(VERSION).capabilities.iter().filter(|c| c.feature == "catalog_constraints").count(), 3);
    assert_eq!(capability_manifest_for(LanguageVersion::Embedded019).capabilities.iter().filter(|c| c.feature == "catalog_constraints").count(), 0);
}

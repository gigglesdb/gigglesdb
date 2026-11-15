use giggles_core::{Admin, Engine as StorageEngine, Limits, relational::with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, LanguageVersion};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Database(PathBuf);
impl Drop for Database {
    fn drop(&mut self) {
        if self.0.is_dir() {
            std::fs::remove_dir_all(&self.0).unwrap();
        } else {
            std::fs::remove_file(&self.0).unwrap();
        }
    }
}
fn database(storage: StorageEngine) -> (Database, Admin) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "native-sqlstate-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
    (Database(path), admin)
}

#[test]
fn native_constraint_and_read_sqlstates_match_sql_on_both_engines() {
    for storage in [
        StorageEngine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        StorageEngine::V2,
    ] {
        let (_db, admin) = database(storage);
        for sql in [
            "create table parent(id bigint primary key)",
            "create table child(id bigint primary key, pid bigint references parent(id) on delete restrict)",
            "create table checked(id bigint primary key, n bigint check(n > 0))",
            "insert into parent values(1)",
            "insert into child values(1,1)",
        ] {
            admin.sql_session().execute(sql, &[], None).unwrap();
        }
        let engine = Engine::with_version(admin.clone(), LanguageVersion::Embedded016);
        for (native, sql, code) in [
            (
                "parent select missing",
                "select missing from parent",
                "42703",
            ),
            (
                "insert child set id=2,pid=99",
                "insert into child values(2,99)",
                "23503",
            ),
            (
                "delete parent where id=1",
                "delete from parent where id=1",
                "23001",
            ),
            (
                "insert checked set id=1,n=-1",
                "insert into checked values(1,-1)",
                "23514",
            ),
        ] {
            let actual =
                with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new()))
                    .unwrap_err();
            let expected = admin.sql_session().execute(sql, &[], None).unwrap_err();
            assert_eq!(
                actual.sqlstate(),
                Some(code),
                "{storage:?}: {native}: {actual:?}"
            );
            assert_eq!(
                actual.sqlstate(),
                Some(giggles_core::sqlstate(&expected)),
                "{storage:?}: {native}"
            );
        }
    }
}

#[test]
fn duplicate_native_create_table_matches_sql_sqlstate() {
    let (_db, admin) = database(StorageEngine::Legacy);
    let engine = Engine::with_version(admin.clone(), LanguageVersion::Embedded016);
    let native = "create table items(id int64,primary key(id))";
    with_sql_parser_forbidden(|| engine.execute_catalog_text(native)).unwrap();
    let actual = with_sql_parser_forbidden(|| engine.execute_catalog_text(native)).unwrap_err();
    let expected = admin
        .sql_session()
        .execute("create table items(id bigint primary key)", &[], None)
        .unwrap_err();
    assert_eq!(actual.sqlstate(), Some("42P07"));
    assert_eq!(actual.sqlstate(), Some(giggles_core::sqlstate(&expected)));
}

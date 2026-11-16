use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::with_sql_parser_forbidden;
use giggles_language::{Engine, LanguageVersion};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};

struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("fk-native-drop-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn snapshot(s: &mut SqlSession) -> String {
    let mut m = s.catalog_metadata().unwrap();
    m.presentation.epoch = 0;
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.presentation,
        s.foreign_key_targets().unwrap()));
    for table in m.tables {
        let rows = s.execute(&format!("select * from {} order by id", table.name), &[], None)
            .unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        out.push_str(&format!("{rows:?}"));
    }
    out
}
fn equal(left: &Admin, right: &Admin) -> String {
    let before = snapshot(&mut left.sql_session());
    assert_eq!(snapshot(&mut right.sql_session()), before);
    before
}
#[test]
fn selected_index_cascade_matches_sql_and_reopens() {
    for storage in [Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] {
        let (ld, left) = Database::fresh(storage);
        let (rd, right) = Database::fresh(storage);
        for ddl in ["create table p(id bigint not null)", "create unique index s on p(id)",
            "alter table p add constraint pk primary key(id)",
            "create table c1(id bigint constraint f1 references p(id))",
            "create table c2(id bigint constraint f2 references p)"] {
            for a in [&left, &right] { a.sql_session().execute(ddl, &[], None).unwrap(); }
        }
        if storage == Storage::V2 {
            for sql in ["insert into p values(1)", "insert into c1 values(1)", "insert into c2 values(1)"] {
                for a in [&left, &right] { a.sql_session().execute(sql, &[], None).unwrap(); }
            }
        }
        let targets = left.sql_session().foreign_key_targets().unwrap();
        assert_eq!(targets["f1"], "index:s");
        assert_eq!(targets["f2"], "pk:pk");
        for text in ["drop index s", "drop index s restrict", "drop index s cascade"] {
            let before = equal(&left, &right);
            let sql = left.sql_session().execute(text, &[], None);
            let native = with_sql_parser_forbidden(|| Engine::with_version(right.clone(),
                LanguageVersion::Embedded017).execute_catalog_text(text));
            if text.ends_with("cascade") {
                assert_eq!(sql.unwrap().affected, 0);
                native.unwrap();
                let targets = left.sql_session().foreign_key_targets().unwrap();
                assert_eq!(targets.len(), 1);
                assert_eq!(targets["f2"], "pk:pk");
            } else {
                assert_eq!(giggles_core::sqlstate(&sql.unwrap_err()), "2BP01");
                assert_eq!(native.unwrap_err().sqlstate(), Some("2BP01"));
                assert_eq!(equal(&left, &right), before);
            }
            equal(&left, &right);
        }
        if storage == Storage::V2 {
            for a in [&left, &right] {
                a.sql_session().execute("insert into c1 values(999)", &[], None).unwrap();
                assert_eq!(giggles_core::sqlstate(&a.sql_session().execute(
                    "insert into c2 values(999)", &[], None).unwrap_err()), "23503");
            }
        }
        let before = equal(&left, &right);
        drop(left); drop(right);
        let left = Admin::open(&ld.0, Limits::default()).unwrap();
        let right = Admin::open(&rd.0, Limits::default()).unwrap();
        assert_eq!(equal(&left, &right), before);
    }
}

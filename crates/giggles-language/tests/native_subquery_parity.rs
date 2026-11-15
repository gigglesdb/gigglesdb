//! Native subqueries and insert-select execute without parsing SQL text.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::with_sql_parser_forbidden;
use giggles_language::{Bindings, Engine, Error, LanguageVersion, lower_text, parse_with_version};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const V: LanguageVersion = LanguageVersion::Embedded019;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-subquery-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        for sql in ["create table t(id int primary key,n int)", "create table u(n int)",
            "insert into t values(1,1),(2,2),(3,null)", "insert into u values(2),(null)",
            "create table src(id bigint primary key,v bigint)", "insert into src values(1,1),(2,2),(3,2)",
            "create table dst(id bigint primary key,v bigint,d bigint default 7)"] {
            admin.sql_session().execute(sql, &[], None).unwrap();
        }
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> { vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] }
fn snapshot(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap();
    m.presentation.epoch = 0; // V2 epochs are per-database HLCs, not logical catalog state.
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.presentation));
    for table in ["t", "u", "src", "dst"] {
        let order = if table == "u" { "n" } else { "id" };
        let rows = session.execute(&format!("select * from {table} order by {order}"), &[], None).unwrap().rows;
        out.push_str(&format!("{:?}", rows.iter().map(|r| &r.values).collect::<Vec<_>>()));
    }
    out
}
#[test]
fn subquery_predicates_match_sql() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let (_other, sqladmin) = Database::fresh(storage);
        let native = Engine::with_version(admin.clone(), V);
        let before = snapshot(&mut admin.sql_session());
        for (pred, sqlpred) in [
            ("n in (u select n)", "n in (select n from u)"),
            ("n not in (u select n)", "n not in (select n from u)"),
            ("exists (u)", "exists (select 1 from u)"),
            ("not exists (u select n)", "not exists (select n from u)"),
            ("n not in (u where n=99 select n)", "n not in (select n from u where n=99)"),
            ("not exists (u where n=99)", "not exists (select 1 from u where n=99)"),
            ("n not in (u where n is not null select n)", "n not in (select n from u where n is not null)"),
            ("n in (u where exists (t select n) select n)", "n in (select n from u where exists (select n from t))"),
        ] {
            let text = format!("t where {pred} order by id select id");
            let actual = with_sql_parser_forbidden(|| native.execute_text(&text, &Bindings::new())).unwrap();
            let expected = sqladmin.sql_session().execute(&format!("select id from t where {sqlpred} order by id"), &[], None).unwrap();
            assert_eq!((actual.matched,actual.affected,actual.changed,actual.complete), (expected.rows.len(),0,0,true));
            assert_eq!((actual.columns, actual.rows), (expected.columns, expected.rows), "{text}");
        }
        for (text, sql) in [
            ("t a join u b on a.n=b.n and a.n in (u select n) select a.id", "select a.id from t a join u b on a.n=b.n and a.n in (select n from u)"),
            ("t group by n having n in (u select n) select n,count(*)", "select n,count(*) from t group by n having n in (select n from u)"),
        ] {
            let actual = with_sql_parser_forbidden(|| native.execute_text(text, &Bindings::new())).unwrap();
            let expected = sqladmin.sql_session().execute(sql, &[], None).unwrap();
            assert_eq!((actual.matched,actual.affected,actual.changed,actual.complete), (expected.rows.len(),0,0,true));
            assert_eq!((actual.columns, actual.rows), (expected.columns, expected.rows), "{text}");
        }
        let error = with_sql_parser_forbidden(|| native.execute_text("t where exists (missing) select id", &Bindings::new())).unwrap_err();
        assert_eq!(error, Error::Sql { sqlstate: "42P01".into() });
        assert_eq!(giggles_core::sqlstate(&sqladmin.sql_session().execute("select id from t where exists(select 1 from missing)", &[], None).unwrap_err()), "42P01");
        assert_eq!(snapshot(&mut admin.sql_session()), before);
        assert_eq!(snapshot(&mut sqladmin.sql_session()), before);
    }
}
#[test]
fn insert_select_matches_sql() {
    for storage in storages() {
        for (text, sql, count) in [
            ("insert into dst(id,v) from src where v>1 select id,v", "insert into dst(id,v) select id,v from src where v>1", 2),
            ("insert into dst(id,v) from src group by v select count(*) as id,v", "insert into dst(id,v) select count(*) as id,v from src group by v", 2),
            ("insert into dst(id,v) from src a join src b on a.id=b.id select a.id,a.v", "insert into dst(id,v) select a.id,a.v from src a join src b on a.id=b.id", 3),
            ("insert into dst from src select id,v,9", "insert into dst select id,v,9 from src", 3),
        ] {
            let (_left, left) = Database::fresh(storage);
            let (_right, right) = Database::fresh(storage);
            let native = Engine::with_version(left.clone(), V);
            let actual = with_sql_parser_forbidden(|| native.execute_text(text, &Bindings::new())).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            let expected = right.sql_session().execute(sql, &[], None).unwrap();
            assert_eq!(expected.affected, count as u64);
            assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete), (count,count,count,true));
            assert!(actual.rows.is_empty() && actual.columns.is_empty());
            assert_eq!(snapshot(&mut left.sql_session()), snapshot(&mut right.sql_session()));
        }
    }
}
#[test]
fn insert_select_errors_match_sql() {
    for storage in storages() {
        for (text, sql, state) in [
            ("insert into dst(id) from src select v", "insert into dst(id) select v from src", "23505"),
            ("insert into dst(id) from src select id,v", "insert into dst(id) select id,v from src", "42601"),
        ] {
            let (_left, left) = Database::fresh(storage);
            let (_right, right) = Database::fresh(storage);
            let before = snapshot(&mut left.sql_session());
            let error = with_sql_parser_forbidden(|| Engine::with_version(left.clone(), V).execute_text(text, &Bindings::new())).unwrap_err();
            assert_eq!(error, Error::Sql { sqlstate: state.into() });
            assert_eq!(giggles_core::sqlstate(&right.sql_session().execute(sql, &[], None).unwrap_err()), state);
            assert_eq!(snapshot(&mut left.sql_session()), before);
            assert_eq!(snapshot(&mut left.sql_session()), snapshot(&mut right.sql_session()));
        }
    }
}
#[test]
fn subqueries_are_opt_in() {
    let (_db, admin) = Database::fresh(Storage::Legacy);
    let native = Engine::with_version(admin, V);
    for text in ["t where n in (u select n) select id", "t where exists (u) select id",
        "insert into dst(id) from src select id"] {
        assert!(parse_with_version(text, LanguageVersion::Embedded018).is_err());
        assert!(matches!(native.prepare_bound_text(text, &Bindings::new()), Err(Error::Unsupported)));
        assert!(matches!(lower_text(text, &Bindings::new(), V), Err(Error::Unsupported)));
    }
    for text in ["t where exists (u x) select id", "t where exists (u order by n) select id",
        "t where exists (u limit 1) select id", "t where exists (u group by n) select id"] {
        assert!(matches!(parse_with_version(text, V), Err(Error::Syntax { .. })), "{text}");
    }
    assert_eq!(parse_with_version("t where n in (u select n,n) select id", V), Err(Error::Unsupported));
    for text in ["t where exists (public.u) select id", "insert into public.dst(id) from src select id"] {
        assert_eq!(native.execute_text(text, &Bindings::new()), Err(Error::Unsupported));
    }
}

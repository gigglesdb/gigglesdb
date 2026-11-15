use giggles_core::{Admin, Engine as Storage, Limits, SqlSession, relational::with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, PageStatement, Statement, Value, ValueExpr,
    capability_manifest_for, parse_page_with_version};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-paging-{}-{}",
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

fn setup(a: &Admin) {
    let mut sql = a.sql_session();
    sql.execute("create table t(id bigint primary key,grp bigint,v text)", &[], None).unwrap();
    let rows = (0..30).map(|i| format!("({i},{},{})", i % 3, if i % 2 == 0 { "NULL" } else { "'value'" })).collect::<Vec<_>>().join(",");
    assert_eq!(sql.execute(&format!("insert into t values {rows}"), &[], None).unwrap().affected, 30);
}
#[test]
fn paging_round_trips_match_sql() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        setup(&left); setup(&right);
        let before = equal(&left, &right);
        // Core SQL does not yet admit row-value comparisons; PG18.6 does.
        assert_eq!(giggles_core::sqlstate(&left.sql_session().execute("select id,v from t where (grp,id)>($1,$2)", &[Value::Int(0),Value::Int(0)], None).unwrap_err()), "0A000");
        let engine = Engine::with_version(right.clone(), VERSION);
        let mut bindings = Bindings::new(); let mut after = None; let mut all = vec![];
        for (index, length) in [7,7,7,7,2].into_iter().enumerate() {
            let suffix = if let Some(key) = &after {
                let key: &Vec<Value> = key;
                bindings.insert("g".into(), key[0].clone()); bindings.insert("i".into(), key[1].clone());
                " after ($g,$i)"
            } else { "" };
            let page = with_sql_parser_forbidden(|| engine.read_page_text(
                &format!("page t order by grp,id limit 7{suffix} select id,v"), &bindings)).unwrap();
            let predicate = if after.is_some() { " where (grp>$1 or (grp=$1 and id>$2))" } else { "" };
            let args = after.clone().unwrap_or_default();
            let expected = left.sql_session().execute(&format!("select id,v from t{predicate} order by grp,id limit 7"), &args, None).unwrap();
            assert_eq!(page.outcome.rows, expected.rows); assert_eq!(page.outcome.columns, expected.columns);
            assert_eq!(page.outcome.rows.len(), length);
            // matched counts all eligible rows before the page limit, unlike SQL's returned row count.
            assert_eq!(page.outcome.matched, 30 - index * 7);
            assert_eq!((page.outcome.affected, page.outcome.changed), (0,0));
            assert_eq!(page.outcome.complete, index == 4); assert_eq!(page.next.is_none(), index == 4);
            all.extend(page.outcome.rows); after = page.next;
        }
        assert_eq!(all, left.sql_session().execute("select id,v from t order by grp,id", &[], None).unwrap().rows);
        let mut key = None; let mut descending = vec![];
        loop {
            let bindings = key.as_ref().map(|v: &Vec<Value>| Bindings::from([("i".into(),v[0].clone())])).unwrap_or_default();
            let suffix = if key.is_some() { " after ($i)" } else { "" };
            let predicate = if key.is_some() { " and id<$1" } else { "" };
            let expected = left.sql_session().execute(&format!("select id from t where grp=1{predicate} order by id desc limit 3"), &key.clone().unwrap_or_default(), None).unwrap();
            let page = with_sql_parser_forbidden(|| engine.read_page_text(&format!("page t where grp=1 order by id desc limit 3{suffix} select id"), &bindings)).unwrap();
            assert_eq!(page.outcome.rows, expected.rows); descending.extend(page.outcome.rows);
            key = page.next; if key.is_none() { break; }
        }
        assert_eq!(descending, left.sql_session().execute("select id from t where grp=1 order by id desc", &[], None).unwrap().rows);
        assert_eq!(giggles_core::sqlstate(&left.sql_session().execute("select id from t where (grp,id)>(2,10) order by grp,id limit 7", &[], None).unwrap_err()), "0A000");
        let literal = with_sql_parser_forbidden(|| engine.read_page_text("page t order by grp,id limit 7 after (2,10) select id", &Bindings::new())).unwrap();
        assert_eq!(literal.outcome.rows, left.sql_session().execute("select id from t where grp>2 or (grp=2 and id>10) order by grp,id limit 7", &[], None).unwrap().rows);
        let zero = with_sql_parser_forbidden(|| engine.read_page_text("page t order by id limit 0", &Bindings::new())).unwrap();
        assert!(zero.outcome.rows.is_empty() && zero.next.is_none());
        assert_eq!(equal(&left, &right), before);
    }
}
#[test]
fn paging_errors_and_opt_in() {
    for storage in storages() {
        let (_db, a) = Database::fresh(storage); let (_other, left) = Database::fresh(storage);
        setup(&a); setup(&left); let before = equal(&left, &a);
        let e = Engine::with_version(a.clone(), VERSION); let b = Bindings::new();
        let state = giggles_core::sqlstate(&left.sql_session().execute("select id from nope order by id limit 7", &[], None).unwrap_err()).to_owned();
        assert_eq!(state, "42P01");
        assert_eq!(with_sql_parser_forbidden(|| e.read_page_text("page nope order by id limit 7", &b)), Err(Error::Sql { sqlstate: state }));
        for text in ["page t order by grp", "page t order by grp,id after (1)", "page t order by id after ($missing)"] {
            assert!(matches!(with_sql_parser_forbidden(|| e.read_page_text(text, &b)), Err(Error::Bind(_))));
        }
        assert_eq!(parse_page_with_version("page t order by id", LanguageVersion::Embedded019), Err(Error::Unsupported));
        let tree = parse_page_with_version("page t order by id", VERSION).unwrap();
        assert_eq!(with_sql_parser_forbidden(|| Engine::with_version(a.clone(), LanguageVersion::Embedded019).read_page(&tree, &b)), Err(Error::Unsupported));
        assert_eq!(parse_page_with_version("page t order by id select id+1 as n", VERSION), Err(Error::Unsupported));
        assert!(matches!(with_sql_parser_forbidden(|| e.execute_text("t order by id limit 7 after (1) select id", &b)), Err(Error::Syntax { .. })));
        let mut large = tree.clone(); large.after = Some(vec![ValueExpr::Literal(Value::Text("x".repeat(giggles_language::MAX_INPUT)))]);
        assert_eq!(with_sql_parser_forbidden(|| e.read_page(&large, &b)), Err(Error::Limit));
        let mutation = PageStatement { read: Statement::Insert { table: "t".into(), values: vec![] }, after: None };
        assert_eq!(with_sql_parser_forbidden(|| e.read_page(&mutation, &b)), Err(Error::Unsupported));
        let many = (0..=giggles_language::MAX_BINDINGS).map(|i| (format!("p{i}"),Value::Int(1))).collect();
        assert_eq!(with_sql_parser_forbidden(|| e.read_page(&tree, &many)), Err(Error::Limit));
        let shared = Bindings::from([("i".into(),Value::Int(7))]);
        assert!(with_sql_parser_forbidden(|| e.read_page_text("page t where id>$i order by id after ($i)", &shared)).is_ok());
        assert_eq!(with_sql_parser_forbidden(|| e.read_page_text(r#"page t order by id after ("bad")"#, &b)), Err(Error::Type));
        let numeric = Bindings::from([("i".into(), Value::Numeric(giggles_core::relational::Numeric::parse("1").unwrap()))]);
        assert_eq!(with_sql_parser_forbidden(|| e.read_page_text("page t order by id after ($i)", &numeric)), Err(Error::Unsupported));
        let extra = Bindings::from([("extra".into(),Value::Int(1))]);
        assert!(matches!(with_sql_parser_forbidden(|| e.read_page(&tree, &extra)), Err(Error::Bind(_))));
        assert_eq!(equal(&left, &a), before);
    }
    for version in [LanguageVersion::Embedded019, VERSION] {
        let capabilities = capability_manifest_for(version);
        let entries = capabilities.capabilities.iter().filter(|c| c.feature == "keyset_paging").count();
        assert_eq!(entries, if version == VERSION { 2 } else { 0 });
    }
}

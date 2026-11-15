mod common;
use giggles_core::{Admin, Engine, Limits, relational::*};
use ReadExpr::{Column as C, Literal as L};
use ReadPredicate::*;
fn read(table: &str, filter: Option<ReadPredicate>) -> SingleSourceRead {
    SingleSourceRead { table: table.into(), projection: vec![ReadProjection {
        expression: C("n".into()), alias: None }], filter }
}
fn trap<T>(f: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")] { with_sql_parser_forbidden(f) }
    #[cfg(not(feature = "test-sql-parser-trap"))] { f() }
}
#[test]
fn subquery_rows_nulls_metadata_and_dependencies_match_sql() {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")] engines.push(Engine::V2);
    for engine in engines {
        let path = common::fresh("typed-subquery");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in ["CREATE TABLE t(id INT PRIMARY KEY,n INT)", "CREATE TABLE u(n INT)",
            "INSERT INTO t VALUES(1,1),(2,2),(3,NULL)", "INSERT INTO u VALUES(2),(NULL)"] {
            s.execute(sql, &[], None).unwrap();
        }
        let sub = || Box::new(read("u", None));
        let empty = || Box::new(read("u", Some(Compare(C("n".into()), ReadCompare::Eq, L(Value::Int(99))))));
        let cases = vec![
            ("n IN(SELECT n FROM u)", In { expr: C("n".into()), subquery: sub() }, 1),
            ("n NOT IN(SELECT n FROM u)", NotIn { expr: C("n".into()), subquery: sub() }, 0),
            ("EXISTS(SELECT n FROM u)", Exists(sub()), 3),
            ("NOT EXISTS(SELECT n FROM u)", NotExists(sub()), 0),
            ("n NOT IN(SELECT n FROM u WHERE n=99)", NotIn { expr: C("n".into()), subquery: empty() }, 3),
            ("NOT EXISTS(SELECT n FROM u WHERE n=99)", NotExists(empty()), 3),
            ("n NOT IN(SELECT n FROM u WHERE n IS NOT NULL)", NotIn { expr: C("n".into()),
                subquery: Box::new(read("u", Some(IsNull(C("n".into()), true)))) }, 1),
            ("n IN(SELECT n FROM u WHERE EXISTS(SELECT n FROM t))", In { expr: C("n".into()),
                subquery: Box::new(read("u", Some(Exists(Box::new(read("t", None)))))) }, 1),
        ];
        for (filter, predicate, count) in cases {
            let sql = format!("SELECT n FROM t WHERE {filter} ORDER BY n");
            let typed = read("t", Some(predicate)).with_clauses(ReadClauses { order_by: vec![ReadOrder {
                expression: C("n".into()), descending: false, nulls_first: None }], ..ReadClauses::default() });
            let plan = s.prepare(&sql).unwrap();
            let expected = s.execute_prepared(&plan, &[], None).unwrap();
            let description = s.describe(&plan).unwrap();
            let actual = trap(|| {
                let plan = Prepared::from_single_source_read(0, 0, typed.clone()).unwrap();
                assert_eq!(plan.authorization_tables().unwrap(), ["t", "u"]);
                let d = s.describe_typed_read(typed.clone()).unwrap();
                assert_eq!(d.columns, description.columns);
                assert_eq!(d.type_oids, description.type_oids);
                s.execute_typed_read(typed, None).unwrap()
            });
            assert_eq!(actual, expected, "{sql}");
            assert_eq!(actual.rows.len(), count);
        }
        let q = |source: &str| ReadExpr::QualifiedColumn { source: source.into(), column: "n".into() };
        for in_join in [false, true] {
            let base = Compare(q("a"), ReadCompare::Eq, q("b"));
            let exists = Exists(Box::new(read("t", None)));
            let typed = TwoSourceRead { left: ReadSource { table: "t".into(), alias: "a".into() },
                right: ReadSource { table: "u".into(), alias: "b".into() }, kind: ReadJoinKind::Inner,
                on: if in_join { And(Box::new(base.clone()), Box::new(exists.clone())) } else { base },
                group_by: vec![], projection: vec![ReadProjection { expression: q("a"), alias: None }],
                filter: if in_join { None } else { Some(exists) } };
            let sql = if in_join { "SELECT a.n FROM t a JOIN u b ON a.n=b.n AND EXISTS(SELECT n FROM t)" }
                else { "SELECT a.n FROM t a JOIN u b ON a.n=b.n WHERE EXISTS(SELECT n FROM t)" };
            let actual = trap(|| s.execute_typed_join(typed, None)).unwrap();
            assert_eq!(actual, s.execute(sql, &[], None).unwrap());
            assert_eq!(actual.rows.len(), 1);
        }
        let invalid = read("t", Some(Exists(Box::new(read("missing", None)))));
        let sql = s.execute("SELECT n FROM t WHERE EXISTS(SELECT n FROM missing)", &[], None);
        assert_eq!(trap(|| s.execute_typed_read(invalid, None)), sql);
        #[cfg(feature = "engine-rocksdb")]
        if engine == Engine::V2 {
            s.execute("CREATE VIEW v AS SELECT n FROM u", &[], None).unwrap();
            // Typed reads see views like SQL (W2-E); PG18 allows views in subqueries.
            let through_view = read("t", Some(Exists(Box::new(read("v", None)))));
            assert_eq!(trap(|| s.execute_typed_read(through_view, None)),
                s.execute("SELECT n FROM t WHERE EXISTS(SELECT n FROM v)", &[], None));
        }
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn nested_subqueries_share_depth_and_byte_budgets() {
    let mut nested = read("u", None);
    for _ in 0..34 { nested = read("u", Some(Exists(Box::new(nested)))); }
    assert!(matches!(Prepared::from_single_source_read(0, 0, nested), Err(SqlError::Limit)));
    let mut child = read("u", None);
    child.projection[0].expression = L(Value::Text("x".repeat(MAX_SQL / 2 + 1)));
    let filter = And(Box::new(Exists(Box::new(child.clone()))), Box::new(Exists(Box::new(child))));
    assert!(matches!(Prepared::from_single_source_read(0, 0, read("t", Some(filter))), Err(SqlError::Limit)));
}

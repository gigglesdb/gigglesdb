mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, relational::*};
use ReadExpr::{Aggregate as A, Column as C, Literal as L};
fn read(table: &str, expressions: Vec<ReadExpr>) -> SingleSourceRead {
    SingleSourceRead { table: table.into(), projection: expressions.into_iter()
        .map(|expression| ReadProjection { expression, alias: None }).collect(), filter: None }
}
fn q(source: &str, column: &str) -> ReadExpr {
    ReadExpr::QualifiedColumn { source: source.into(), column: column.into() }
}
fn order(expression: ReadExpr) -> ReadOrder {
    ReadOrder { expression, descending: false, nulls_first: None }
}
fn join() -> TwoSourceRead {
    TwoSourceRead { left: ReadSource { table: "app.items".into(), alias: "i".into() },
        right: ReadSource { table: "app.orders".into(), alias: "o".into() }, kind: ReadJoinKind::Left,
        on: ReadPredicate::Compare(q("i", "id"), ReadCompare::Eq, q("o", "item")),
        group_by: vec![q("i", "id")], projection: vec![
            ReadProjection { expression: q("i", "id"), alias: None },
            ReadProjection { expression: A(ReadAggregate::Qualified { source: "o".into(),
                aggregate: Box::new(ReadAggregate::Count("id".into())) }), alias: None }], filter: None }
        .with_clauses(ReadClauses { order_by: vec![order(q("i", "id"))], ..ReadClauses::default() })
}
fn trap<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")] { with_sql_parser_forbidden(work) }
    #[cfg(not(feature = "test-sql-parser-trap"))] { work() }
}
fn parity(s: &mut SqlSession, input: SingleSourceRead, sql: &str) -> SqlResult {
    let plan = s.prepare(sql).unwrap();
    let expected = s.describe(&plan).unwrap();
    let actual = trap(|| s.describe_typed_read(input.clone())).unwrap();
    assert_eq!((actual.columns, actual.type_oids, actual.typmods),
        (expected.columns, expected.type_oids, expected.typmods));
    let expected = s.execute_prepared(&plan, &[], None).unwrap();
    let actual = trap(|| s.execute_typed_read(input, None)).unwrap();
    assert_eq!(actual, expected, "{sql}");
    actual
}
#[test]
fn qualified_reads_match_sql_rows_descriptions_search_path_and_errors() {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")] engines.push(Engine::V2);
    for engine in engines {
        let path = common::fresh("typed-read-qualified");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in ["CREATE SCHEMA app", "CREATE TABLE app.orders(id INT PRIMARY KEY,item TEXT,qty INT)",
            "INSERT INTO app.orders VALUES(1,'a',1),(2,'a',2),(3,'b',5)",
            "CREATE TABLE app.items(id TEXT PRIMARY KEY)", "INSERT INTO app.items VALUES('a'),('d')",
            "CREATE TABLE public.t(id INT)", "INSERT INTO public.t VALUES(9)",
            "CREATE TABLE app.t(id INT)", "INSERT INTO app.t VALUES(7)"] {
            s.execute(sql, &[], None).unwrap();
        }
        let count = A(ReadAggregate::CountStar);
        let aggregate = read("app.orders", vec![C("item".into()), count.clone()]).with_clauses(ReadClauses {
            group_by: vec!["item".into()], having: Some(ReadPredicate::Compare(count, ReadCompare::Gt, L(Value::Int(1)))),
            order_by: vec![order(C("item".into()))], limit: Some(1) });
        let result = parity(&mut s, aggregate,
            "SELECT item,count(*) FROM app.orders GROUP BY item HAVING count(*)>1 ORDER BY item LIMIT 1");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, vec![Value::Text("a".into()), Value::Int(2)]);
        let sql = "SELECT i.id,count(o.id) FROM app.items i LEFT JOIN app.orders o ON i.id=o.item GROUP BY i.id ORDER BY i.id";
        let plan = s.prepare(sql).unwrap();
        let a = trap(|| s.describe_typed_join(join())).unwrap();
        let b = s.describe(&plan).unwrap();
        assert_eq!((a.columns, a.type_oids, a.typmods), (b.columns, b.type_oids, b.typmods));
        let expected = s.execute_prepared(&plan, &[], None).unwrap();
        let actual = trap(|| s.execute_typed_join(join(), None)).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
            vec![vec![Value::Text("a".into()), Value::Int(2)], vec![Value::Text("d".into()), Value::Int(0)]]);
        s.set_search_path("app, public", "postgres");
        assert_eq!(parity(&mut s, read("orders", vec![C("id".into())]), "SELECT id FROM orders").rows.len(), 3);
        assert_eq!(parity(&mut s, read("public.t", vec![C("id".into())]), "SELECT id FROM public.t").rows[0].values, vec![Value::Int(9)]);
        s.set_search_path("public", "postgres");
        // The single-source implicit alias is the unqualified table component.
        parity(&mut s, read("app.orders", vec![q("orders", "id")]), "SELECT orders.id FROM app.orders");
        for exists in [false, true] {
            let subquery = Box::new(read("app.items", vec![C("id".into())]));
            let mut input = read("app.orders", vec![C("id".into())]);
            input.filter = Some(if exists { ReadPredicate::Exists(subquery) } else {
                ReadPredicate::In { expr: C("item".into()), subquery } });
            parity(&mut s, input, if exists { "SELECT id FROM app.orders WHERE EXISTS(SELECT id FROM app.items)" }
                else { "SELECT id FROM app.orders WHERE item IN(SELECT id FROM app.items)" });
        }
        for table in ["nosuch.orders", "app.nosuch"] {
            let expected = s.execute(&format!("SELECT id FROM {table}"), &[], None).unwrap_err();
            assert_eq!(expected, CoreError::Sql(SqlError::UnknownTable));
            assert_eq!(giggles_core::sqlstate(&expected), "42P01");
            let input = read(table, vec![C("id".into())]);
            assert_eq!(trap(|| s.describe_typed_read(input.clone())).unwrap_err(), expected);
            assert_eq!(trap(|| s.execute_typed_read(input, None)).unwrap_err(), expected);
            for left in [false, true] {
                let mut input = join();
                if left { input.left.table = table.into(); } else { input.right.table = table.into(); }
                let (l, r) = (&input.left.table, &input.right.table);
                let expected = s.execute(&format!("SELECT i.id FROM {l} i LEFT JOIN {r} o ON i.id=o.item"), &[], None).unwrap_err();
                assert_eq!(giggles_core::sqlstate(&expected), "42P01");
                assert_eq!(trap(|| s.describe_typed_join(input.clone())).unwrap_err(), expected);
                assert_eq!(trap(|| s.execute_typed_join(input, None)).unwrap_err(), expected);
            }
        }
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn qualified_table_components_are_canonical_and_other_names_stay_strict() {
    for table in ["app.orders.extra", ".orders", "app.", "App.orders", "app.Orders", "1app.orders", "app.1orders", "app.or ders"] {
        let input = read(table, vec![C("id".into())]);
        assert!(matches!(Prepared::from_single_source_read(0, 0, input), Err(SqlError::Syntax)), "{table}");
        for left in [false, true] {
            let mut input = join();
            if left { input.left.table = table.into(); } else { input.right.table = table.into(); }
            assert!(matches!(Prepared::from_two_source_read(0, 0, input), Err(SqlError::Syntax)), "{table}");
        }
    }
    for expression in [C("orders.id".into()), q("app.orders", "id"), q("orders", "orders.id")] {
        assert!(matches!(Prepared::from_single_source_read(0, 0, read("app.orders", vec![expression])), Err(SqlError::Syntax)));
    }
    let mut input = read("app.orders", vec![C("id".into())]);
    input.projection[0].alias = Some("orders.id".into());
    assert!(matches!(Prepared::from_single_source_read(0, 0, input), Err(SqlError::Syntax)));
    let mut input = join(); input.left.alias = "app.i".into();
    assert!(matches!(Prepared::from_two_source_read(0, 0, input), Err(SqlError::Syntax)));
}

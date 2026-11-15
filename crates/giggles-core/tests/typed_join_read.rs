mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, relational::*};
use ReadExpr::{Aggregate as A, Column as C, Literal as L};
use ReadPredicate::{And, Compare, IsNull, Not, Or};
fn q(source: &str, column: &str) -> ReadExpr {
    ReadExpr::QualifiedColumn { source: source.into(), column: column.into() }
}
fn eq(a: ReadExpr, b: ReadExpr) -> ReadPredicate { Compare(a, ReadCompare::Eq, b) }
fn read(kind: ReadJoinKind, projection: Vec<ReadExpr>) -> TwoSourceRead {
    TwoSourceRead {
        left: ReadSource { table: "t".into(), alias: "a".into() },
        right: ReadSource { table: "u".into(), alias: "b".into() },
        kind, on: eq(q("a", "id"), q("b", "tid")), filter: None, group_by: vec![],
        projection: projection.into_iter().map(|expression| ReadProjection { expression, alias: None }).collect(),
    }
}
fn trap<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    { with_sql_parser_forbidden(work) }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    { work() }
}
fn parity(s: &mut SqlSession, sql: &str, typed: TwoSourceRead) -> Result<SqlResult, CoreError> {
    let plan = s.prepare(sql).unwrap();
    match (trap(|| s.describe_typed_join(typed.clone())), s.describe(&plan)) {
        (Ok(a), Ok(b)) => {
            assert_eq!(a.columns, b.columns, "{sql}");
            assert_eq!(a.type_oids, b.type_oids, "{sql}");
            assert_eq!(a.typmods, b.typmods, "{sql}");
            assert_eq!(a.parameters, b.parameters, "{sql}");
        }
        (Err(a), Err(b)) => assert_eq!(a, b, "{sql}"),
        (a, b) => panic!("{sql}: typed {a:?}, SQL {b:?}"),
    }
    let expected = s.execute_prepared(&plan, &[], None);
    let actual = trap(|| s.execute_typed_join(typed, None));
    assert_eq!(actual, expected, "{sql}");
    actual
}
#[test]
fn joins_clauses_nulls_self_join_and_errors_match_sql() {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    engines.push(Engine::V2);
    for engine in engines {
        let path = common::fresh("typed-join");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id INT PRIMARY KEY,g INT)", &[], None).unwrap();
        s.execute("CREATE TABLE u(id INT PRIMARY KEY,tid INT,n INT)", &[], None).unwrap();
        s.execute("INSERT INTO t VALUES(1,1),(2,1),(3,2)", &[], None).unwrap();
        s.execute("INSERT INTO u VALUES(1,1,8),(2,1,NULL),(3,2,4),(4,NULL,9)", &[], None).unwrap();
        for (kind, text, count) in [(ReadJoinKind::Inner, "INNER", 3), (ReadJoinKind::Left, "LEFT", 4)] {
            let mut typed = read(kind, vec![q("a", "id"), q("b", "n")]);
            typed.projection[1].alias = Some("value".into());
            let order = ReadClauses { order_by: vec![ReadOrder { expression: q("a", "id"), descending: false, nulls_first: None },
                ReadOrder { expression: q("b", "id"), descending: false, nulls_first: None }], ..ReadClauses::default() };
            let sql = format!("SELECT a.id,b.n AS value FROM t a {text} JOIN u b ON a.id=b.tid");
            let result = parity(&mut s, &format!("{sql} ORDER BY a.id,b.id"), typed.clone().with_clauses(order.clone())).unwrap();
            assert_eq!(result.rows.len(), count);
            if matches!(kind, ReadJoinKind::Left) {
                assert_eq!(result.rows.last().unwrap().values, vec![Value::Int(3), Value::Null]);
            }
            typed.filter = Some(IsNull(q("b", "id"), false));
            let result = parity(&mut s, &format!("{sql} WHERE b.id IS NULL ORDER BY a.id,b.id"), typed.with_clauses(order)).unwrap();
            assert_eq!(result.rows.len(), count - 3);
            let sum = ReadAggregate::Qualified { source: "b".into(), aggregate: Box::new(ReadAggregate::Sum("n".into())) };
            let grouped = read(kind, vec![q("a", "g"), A(ReadAggregate::CountStar), A(sum.clone())]).with_clauses(ReadClauses {
                group_by: vec!["g".into()], having: Some(Compare(A(ReadAggregate::CountStar), ReadCompare::Gt, L(Value::Int(0)))),
                order_by: vec![ReadOrder { expression: C("g".into()), descending: true, nulls_first: Some(false) }],
                limit: Some(2),
            });
            parity(&mut s, &format!("SELECT a.g,count(*),sum(b.n) FROM t a {text} JOIN u b ON a.id=b.tid GROUP BY g HAVING count(*)>0 ORDER BY g DESC NULLS LAST LIMIT 2"), grouped).unwrap();
        }
        let count = ReadAggregate::Qualified { source: "b".into(), aggregate: Box::new(ReadAggregate::Count("id".into())) };
        let mut qualified_group = read(ReadJoinKind::Left, vec![q("a", "id"), A(count)]);
        qualified_group.group_by = vec![q("a", "id")];
        qualified_group = qualified_group.with_clauses(ReadClauses {
            order_by: vec![ReadOrder { expression: q("a", "id"), descending: false, nulls_first: None }], ..ReadClauses::default()
        });
        parity(&mut s, "SELECT a.id,count(b.id) FROM t a LEFT JOIN u b ON a.id=b.tid GROUP BY a.id ORDER BY a.id", qualified_group).unwrap();
        let mut logical = read(ReadJoinKind::Left, vec![q("a", "id"), q("b", "id")]);
        logical.on = And(Box::new(logical.on), Box::new(Not(Box::new(Or(
            Box::new(IsNull(q("b", "n"), false)), Box::new(Compare(q("b", "n"), ReadCompare::Lt, L(Value::Int(5)))))))));
        logical = logical.with_clauses(ReadClauses { order_by: vec![ReadOrder { expression: q("a", "id"), descending: false, nulls_first: None }], ..ReadClauses::default() });
        parity(&mut s, "SELECT a.id,b.id FROM t a LEFT JOIN u b ON a.id=b.tid AND NOT (b.n IS NULL OR b.n<5) ORDER BY a.id", logical).unwrap();
        let mut self_join = read(ReadJoinKind::Inner, vec![q("a", "id"), q("b", "id")]);
        self_join.right.table = "t".into();
        self_join.on = eq(q("a", "id"), q("b", "id"));
        self_join = self_join.with_clauses(ReadClauses { order_by: vec![ReadOrder { expression: q("a", "id"), descending: false, nulls_first: None }], ..ReadClauses::default() });
        parity(&mut s, "SELECT a.id,b.id FROM t a INNER JOIN t b ON a.id=b.id ORDER BY a.id", self_join).unwrap();
        for (column, sql, error) in [(C("id".into()), "id", SqlError::Duplicate),
            (q("missing", "id"), "missing.id", SqlError::UnknownTable),
            (q("b", "id,secret"), "b.\"id,secret\"", SqlError::UnknownColumn), (q("b", "missing"), "b.missing", SqlError::UnknownColumn)] {
            // Identical CoreError variants preserve the SQLSTATE mapping.
            assert_eq!(parity(&mut s, &format!("SELECT {sql} FROM t a JOIN u b ON a.id=b.tid"),
                read(ReadJoinKind::Inner, vec![column])), Err(CoreError::Sql(error)));
        }
        let valid = read(ReadJoinKind::Inner, vec![q("a", "id")]);
        let p = trap(|| Prepared::from_two_source_read(0, 0, valid.clone())).unwrap();
        assert_eq!(p.authorization_tables().unwrap(), ["t", "u"]);
        for missing_left in [false, true] {
            let mut invalid = valid.clone();
            if missing_left { invalid.left.table = "missing".into(); } else { invalid.right.table = "missing".into(); }
            assert_eq!(trap(|| s.describe_typed_join(invalid.clone())).unwrap_err(), CoreError::Sql(SqlError::UnknownTable));
            assert_eq!(trap(|| s.execute_typed_join(invalid, None)), Err(CoreError::Sql(SqlError::UnknownTable)));
        }
        #[cfg(feature = "engine-rocksdb")]
        if engine == Engine::V2 {
            s.execute("CREATE VIEW v AS SELECT id,tid,n FROM u", &[], None).unwrap();
            let mut view = valid.clone(); view.right.table = "v".into();
            parity(&mut s, "SELECT a.id FROM t a JOIN v b ON a.id=b.tid", view).unwrap();
        }
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO u VALUES(5,3,6)", &[], None).unwrap();
        assert_eq!(trap(|| s.execute_typed_join(valid.clone(), None)).unwrap().rows.len(), 4);
        let mut invalid = valid.clone(); invalid.on = eq(q("a", "missing"), q("b", "tid"));
        assert_eq!(trap(|| s.describe_typed_join(invalid)).unwrap_err(), CoreError::Sql(SqlError::UnknownColumn));
        assert_eq!(trap(|| s.execute_typed_join(valid.clone(), None)), Err(CoreError::Sql(SqlError::Aborted)));
        s.execute("ROLLBACK", &[], None).unwrap();
        assert_eq!(trap(|| s.execute_typed_join(valid, None)).unwrap().rows.len(), 3);
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn join_admission_uses_the_shared_name_and_work_budgets() {
    let valid = read(ReadJoinKind::Inner, vec![q("a", "id")]);
    for (source, column) in [("a.secret", "id"), ("a", "id.secret")] {
        let mut invalid = valid.clone(); invalid.projection[0].expression = q(source, column);
        assert!(Prepared::from_two_source_read(0, 0, invalid).is_err());
    }
    let mut invalid = valid.clone(); invalid.right.alias = "b.secret".into();
    assert!(Prepared::from_two_source_read(0, 0, invalid).is_err());
    let mut invalid = valid.clone();
    for _ in 0..34 { invalid.on = Not(Box::new(invalid.on)); }
    assert!(matches!(Prepared::from_two_source_read(0, 0, invalid), Err(SqlError::Limit)));
    let mut invalid = valid.clone(); invalid.group_by = vec![q("a", "id"); MAX_COLUMNS + 1];
    assert!(matches!(Prepared::from_two_source_read(0, 0, invalid), Err(SqlError::Limit)));
    let mut invalid = valid;
    invalid.projection[0].expression = A(ReadAggregate::Qualified {
        source: "b".into(), aggregate: Box::new(ReadAggregate::CountStar),
    });
    assert!(matches!(Prepared::from_two_source_read(0, 0, invalid), Err(SqlError::Syntax)));
}

mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, relational::*};
use ReadAggregate::*;
use ReadExpr::{Aggregate as A, Column as C, Literal as L};

fn engines() -> Vec<Engine> {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    engines.push(Engine::V2);
    engines
}
fn trap<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    { with_sql_parser_forbidden(work) }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    { work() }
}
fn read(expressions: Vec<ReadExpr>, clauses: ReadClauses) -> SingleSourceRead {
    SingleSourceRead { table: "t".into(), filter: None, projection: expressions.into_iter()
        .map(|expression| ReadProjection { expression, alias: None }).collect() }.with_clauses(clauses)
}
fn parity(s: &mut SqlSession, sql: &str, typed: SingleSourceRead, ordered: bool) -> Result<SqlResult, CoreError> {
    let plan = s.prepare(sql).unwrap();
    let description = s.describe(&plan);
    match (trap(|| s.describe_typed_read(typed.clone())), description) {
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
    let actual = trap(|| s.execute_typed_read(typed, None));
    match (&actual, &expected) {
        (Ok(a), Ok(b)) if !ordered => {
            let sorted = |r: &SqlResult| {
                let mut rows: Vec<_> = r.rows.iter().map(|r| format!("{:?}", r.values)).collect();
                rows.sort(); rows
            };
            assert_eq!(a.columns, b.columns, "{sql}");
            assert_eq!(sorted(a), sorted(b), "{sql}");
        }
        _ => assert_eq!(actual, expected, "{sql}"),
    }
    actual
}
#[test]
fn aggregate_rows_types_nulls_empty_groups_having_and_paging_match_sql() {
    for engine in engines() {
        let path = common::fresh("typed-aggregate");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id INT PRIMARY KEY,g INT,n INT,b BIGINT,d NUMERIC,f REAL)", &[], None).unwrap();
        for populated in [false, true] {
            if populated {
                s.execute("INSERT INTO t VALUES(1,1,2,2,2.50,2),(2,1,NULL,NULL,NULL,NULL),(3,2,4,4,4.25,4),(4,NULL,NULL,NULL,NULL,NULL)", &[], None).unwrap();
            }
            for column in ["n", "b", "d", "f"] {
                for (text, aggregate) in [("count(*)", CountStar),
                    ("count(col)", Count(column.into())), ("sum(col)", Sum(column.into())),
                    ("min(col)", Min(column.into())), ("max(col)", Max(column.into())), ("avg(col)", Avg(column.into()))] {
                    let text = text.replace("col", column);
                    for grouped in [false, true] {
                        let (prefix, suffix, group_by, mut expressions) = if grouped {
                            ("g,", " GROUP BY g", vec!["g".into()], vec![C("g".into())])
                        } else { ("", "", vec![], vec![]) };
                        expressions.push(A(aggregate.clone()));
                        let base = ReadClauses { group_by, ..ReadClauses::default() };
                        let sql = format!("SELECT {prefix}{text} FROM t{suffix}");
                        parity(&mut s, &sql, read(expressions.clone(), base.clone()), false).unwrap();
                        for (op, spelling, threshold) in [(ReadCompare::Gt, ">", 0), (ReadCompare::Le, "<=", 1)] {
                            let having = ReadPredicate::Compare(A(aggregate.clone()), op, L(Value::Int(threshold)));
                            parity(&mut s, &format!("{sql} HAVING {text}{spelling}{threshold}"),
                                read(expressions.clone(), ReadClauses { having: Some(having), ..base.clone() }), false).unwrap();
                        }
                    }
                }
            }
            let mut typed = read(vec![C("g".into()), A(CountStar), A(Sum("n".into()))], ReadClauses {
                group_by: vec!["g".into(), "b".into()],
                order_by: vec![ReadOrder { expression: C("g".into()), descending: true, nulls_first: Some(false) },
                    ReadOrder { expression: A(CountStar), descending: false, nulls_first: None }],
                limit: Some(2), ..ReadClauses::default()
            });
            typed.projection[2].alias = Some("total".into());
            // The builder preserves the pre-group WHERE predicate.
            let mut filtered = SingleSourceRead { filter: Some(ReadPredicate::Compare(C("id".into()), ReadCompare::Gt, L(Value::Int(0)))),
                ..read(vec![A(CountStar)], ReadClauses::default()) };
            filtered = filtered.with_clauses(ReadClauses { limit: Some(1), ..ReadClauses::default() });
            parity(&mut s, "SELECT count(*) FROM t WHERE id>0 LIMIT 1", filtered, true).unwrap();
            parity(&mut s, "SELECT g,count(*),sum(n) AS total FROM t GROUP BY g,b ORDER BY g DESC NULLS LAST,count(*) LIMIT 2", typed, true).unwrap();
            for limit in [0, -1] {
                let result = parity(&mut s, &format!("SELECT count(*) FROM t LIMIT {limit}"),
                    read(vec![A(CountStar)], ReadClauses { limit: Some(limit), ..ReadClauses::default() }), true);
                assert_eq!(result.is_ok(), limit == 0);
            }
        }
        for (sql, expressions, error) in [
            ("SELECT sum(missing) FROM t", vec![A(Sum("missing".into()))], SqlError::UnknownColumn),
            ("SELECT g,count(*) FROM t", vec![C("g".into()), A(CountStar)], SqlError::GroupingError),
        ] {
            // Equal CoreError variants preserve the SQLSTATE mapping (42703/42803).
            assert_eq!(parity(&mut s, sql, read(expressions, ReadClauses::default()), false), Err(CoreError::Sql(error)));
        }
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn integer_sum_widening_and_sum_overflow_match_sql() {
    for engine in engines() {
        let path = common::fresh("typed-aggregate-overflow");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id INT PRIMARY KEY,g INT,b BIGINT,n DOUBLE PRECISION)", &[], None).unwrap();
        s.execute("INSERT INTO t VALUES(1,1,9223372036854775807,1e308::float8),(2,1,1,1e308::float8)", &[], None).unwrap();
        for grouped in [false, true] {
            let suffix = if grouped { " GROUP BY g" } else { "" };
            let clauses = ReadClauses { group_by: if grouped { vec!["g".into()] } else { vec![] }, ..ReadClauses::default() };
            let result = parity(&mut s, &format!("SELECT sum(b) FROM t{suffix}"),
                read(vec![A(Sum("b".into()))], clauses.clone()), false).unwrap();
            assert_eq!(result.rows[0].values, vec![Value::Numeric(Numeric::parse("9223372036854775808").unwrap())]);
            // BIGINT SUM widens above i64::MAX; finite FLOAT8 SUM can overflow (22003).
            assert_eq!(parity(&mut s, &format!("SELECT sum(n) FROM t{suffix}"),
                read(vec![A(Sum("n".into()))], clauses), false), Err(CoreError::Sql(SqlError::Overflow)));
        }
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn aggregate_clause_admission_remains_bounded() {
    for clauses in [ReadClauses { group_by: vec!["g".into(); MAX_COLUMNS + 1], ..ReadClauses::default() },
        ReadClauses { order_by: vec![ReadOrder { expression: A(CountStar), descending: false, nulls_first: None }; MAX_COLUMNS + 1], ..ReadClauses::default() }] {
        assert!(matches!(Prepared::from_single_source_read(0, 0, read(vec![A(CountStar)], clauses)), Err(SqlError::Limit)));
    }
    for aggregate in [Sum("secret.n".into()), Count("x".repeat(MAX_SQL + 1))] {
        assert!(Prepared::from_single_source_read(0, 0, read(vec![A(aggregate)], ReadClauses::default())).is_err());
    }
    let nested = read(vec![A(CountStar)], ReadClauses::default()).with_clauses(ReadClauses::default());
    assert!(matches!(Prepared::from_single_source_read(0, 0, nested), Err(SqlError::Syntax)));
}

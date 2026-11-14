mod common;
use giggles_core::{Admin, CoreError, Engine, Error, Limits, relational::*};
use ReadCompare::*;
use ReadExpr::{Column as C, Literal as L};
use ReadPredicate::*;

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
fn read(filter: Option<ReadPredicate>) -> SingleSourceRead {
    SingleSourceRead { table: "t".into(), projection: vec![
        ReadProjection { expression: C("label".into()), alias: Some("renamed".into()) },
        ReadProjection { expression: C("id".into()), alias: None },
        ReadProjection { expression: L(Value::Int(7)), alias: Some("fixed".into()) },
    ], filter }
}
fn cmp(op: ReadCompare) -> ReadPredicate { Compare(C("n".into()), op, L(Value::Int(2))) }
fn rows(result: SqlResult) -> Vec<String> {
    let mut rows: Vec<_> = result.rows.into_iter().map(|r| format!("{:?}", r.values)).collect();
    rows.sort(); rows
}
#[test]
fn sql_and_typed_leaf_reads_share_metadata_rows_and_transactions() {
    for engine in engines() {
        let path = common::fresh("typed-leaf");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,label TEXT)", &[], None).unwrap();
        for populated in [false, true] {
            if populated { s.execute("INSERT INTO t VALUES(1,NULL,'a'),(2,2,NULL),(3,3,'c')", &[], None).unwrap(); }
            let mut cases = vec![("".to_string(), None), ("n IS NULL".into(), Some(IsNull(C("n".into()), false))),
                ("n IS NOT NULL".into(), Some(IsNull(C("n".into()), true))),
                ("NOT (n=2 OR n IS NULL) AND id>0".into(), Some(And(Box::new(Not(Box::new(Or(Box::new(cmp(Eq)),
                    Box::new(IsNull(C("n".into()), false)))))), Box::new(Compare(C("id".into()), Gt, L(Value::Int(0)))))))];
            for (op, text) in [(Eq,"="),(Ne,"<>"),(Lt,"<"),(Le,"<="),(Gt,">"),(Ge,">=")] {
                cases.push((format!("n{text}2"), Some(cmp(op))));
            }
            for (predicate, filter) in cases {
                let sql = format!("SELECT label AS renamed,id,7 AS fixed FROM t{}", if predicate.is_empty() { String::new() } else { format!(" WHERE {predicate}") });
                let sql_plan = s.prepare(&sql).unwrap();
                let description = s.describe(&sql_plan).unwrap();
                let expected = rows(s.execute_prepared(&sql_plan, &[], None).unwrap());
                trap(|| {
                    let typed = read(filter);
                    let p = Prepared::from_single_source_read(0, 0, typed.clone()).unwrap();
                    assert_eq!(p.authorization_tables().unwrap(), ["t"]);
                    let actual = s.describe_typed_read(typed.clone()).unwrap();
                    assert_eq!(actual.columns, description.columns);
                    assert_eq!(actual.type_oids, description.type_oids);
                    assert_eq!(actual.typmods, description.typmods);
                    assert_eq!(rows(s.execute_typed_read(typed, None).unwrap()), expected);
                });
            }
        }
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO t VALUES(4,4,'d')", &[], None).unwrap();
        assert_eq!(trap(|| s.execute_typed_read(read(None), None)).unwrap().rows.len(), 4);
        s.execute("ROLLBACK", &[], None).unwrap();
        assert_eq!(trap(|| s.execute_typed_read(read(None), None)).unwrap().rows.len(), 3);
        s.execute("BEGIN", &[], None).unwrap();
        let mut invalid = read(None); invalid.projection[0].expression = C("missing".into());
        assert!(trap(|| s.describe_typed_read(invalid)).is_err());
        assert_eq!(s.execute_typed_read(read(None), None), Err(CoreError::Sql(SqlError::Aborted)));
        s.execute("ROLLBACK", &[], None).unwrap();
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn semantic_errors_and_authorization_first_are_preserved() {
    for engine in engines() {
        let path = common::fresh("typed-leaf-errors");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,label TEXT)", &[], None).unwrap();
        for (sql, typed, error) in [
            ("SELECT missing FROM t", SingleSourceRead { projection: vec![ReadProjection { expression: C("missing".into()), alias: None }], ..read(None) }, SqlError::UnknownColumn),
            ("SELECT label AS renamed,id,7 AS fixed FROM t WHERE n=true", read(Some(Compare(C("n".into()), Eq, L(Value::Bool(true))))), SqlError::UndefinedFunction),
        ] {
            let p = s.prepare(sql).unwrap();
            let expected = CoreError::Sql(error);
            // Identical CoreError variants use the identical server SQLSTATE mapping (42703/42883).
            assert_eq!(s.describe(&p).unwrap_err(), expected);
            assert_eq!(s.execute_prepared(&p, &[], None).unwrap_err(), expected);
            trap(|| {
                let p = Prepared::from_single_source_read(0, 0, typed.clone()).unwrap();
                // Gateway contract: authorize every dependency before describe/execute (E939).
                let authorize = || -> Result<(), CoreError> {
                    for table in p.authorization_tables().map_err(CoreError::Sql)? {
                        if table != "permitted" { return Err(CoreError::Policy(Error::Scope)); }
                    }
                    Ok(())
                };
                assert_eq!(authorize().and_then(|()| s.describe_typed_read(typed.clone())).unwrap_err(), CoreError::Policy(Error::Scope));
                assert_eq!(s.describe_typed_read(typed.clone()).unwrap_err(), expected);
                assert_eq!(s.execute_typed_read(typed, None).unwrap_err(), expected);
            });
        }
        #[cfg(feature = "engine-rocksdb")]
        if engine == Engine::V2 {
            s.execute("CREATE VIEW v AS SELECT id,n,label FROM t", &[], None).unwrap();
            let mut view = read(None); view.table = "v".into();
            assert_eq!(trap(|| s.describe_typed_read(view.clone())).unwrap_err(), CoreError::Sql(SqlError::UnknownTable));
            assert_eq!(trap(|| s.execute_typed_read(view, None)).unwrap_err(), CoreError::Sql(SqlError::UnknownTable));
        }
        let mut extra = read(None); extra.table = "t,secret".into();
        assert!(trap(|| s.execute_typed_read(extra, None)).is_err());
        drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn typed_input_admission_is_bounded() {
    let mut input = read(None); input.projection = vec![];
    assert!(matches!(Prepared::from_single_source_read(0, 0, input), Err(SqlError::Limit)));
    let mut input = read(None); input.projection = vec![input.projection[0].clone(); MAX_COLUMNS + 1];
    assert!(matches!(Prepared::from_single_source_read(0, 0, input), Err(SqlError::Limit)));
    let mut input = read(None); input.projection[0].expression = L(Value::Text("x".repeat(MAX_SQL + 1)));
    assert!(matches!(Prepared::from_single_source_read(0, 0, input), Err(SqlError::Limit)));
    let mut predicate = cmp(Eq);
    for _ in 0..34 { predicate = Not(Box::new(predicate)); }
    assert!(matches!(Prepared::from_single_source_read(0, 0, read(Some(predicate))), Err(SqlError::Limit)));
    let mut wide = cmp(Eq);
    for _ in 0..12 { wide = And(Box::new(wide.clone()), Box::new(wide)); }
    assert!(matches!(Prepared::from_single_source_read(0, 0, read(Some(wide))), Err(SqlError::Limit)));
    let mut input = read(None); input.projection[0].expression = C("secret.n".into());
    assert!(Prepared::from_single_source_read(0, 0, input).is_err());
}

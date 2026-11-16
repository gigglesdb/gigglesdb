mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, operations::{self, Bindings, Expr, Request, ValueExpr}, relational::*};

fn exercise(engine: Engine) {
    let path = common::fresh("parity-insert-duplicate-column");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let native = operations::Engine::new(admin.clone());
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE t(id int PRIMARY KEY,v smallint)", &[], None).unwrap();
    sql.execute("CREATE TABLE empty_source(id int)", &[], None).unwrap();
    sql.execute("INSERT INTO t VALUES(1,9)", &[], None).unwrap();
    // PostgreSQL 18.6: checkInsertTargets reports 42701 before input evaluation,
    // including an empty SELECT source and an incompatible input value.
    for statement in [
        "INSERT INTO t(id,id) VALUES(2,3)",
        "INSERT INTO t(id,v,id) VALUES(2,7,3) RETURNING *",
        "INSERT INTO t(id,id) VALUES(2+0,3)",
        "INSERT INTO t(id,id) VALUES(2,3),(4,5)",
        "INSERT INTO t(id,id) SELECT 2,3",
        "INSERT INTO t(id,id) SELECT id,id FROM empty_source",
        "INSERT INTO t(id,id) SELECT 2,3 WHERE false",
        "INSERT INTO t(id,id) VALUES(2,3) ON CONFLICT DO NOTHING",
        "INSERT INTO t(id,id) VALUES(1,3) ON CONFLICT(id) DO UPDATE SET v=4",
        "INSERT INTO t(id,id) VALUES(2,3) RETURNING id",
        "INSERT INTO t(id,id) VALUES(2,3),(4,5) ON CONFLICT DO NOTHING RETURNING id",
        "INSERT INTO t(id,v,v) VALUES(2,true::boolean,1)",
        "INSERT INTO t(id,ID) VALUES(2,3)",
        "INSERT INTO t(id,\"id\") VALUES(2,3)",
    ] {
        let error = sql.execute(statement, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "42701", "{statement}");
        assert_eq!(error, CoreError::Sql(SqlError::DuplicateColumn), "{statement}");
        assert_eq!(sql.execute("SELECT * FROM t", &[], None).unwrap().rows[0].values,
            vec![Value::Int(1), Value::Int(9)], "{statement}");
        assert_eq!(sql.execute("SELECT * FROM t", &[], None).unwrap().rows.len(), 1);
    }
    match sql.prepare("INSERT INTO t(id,id) VALUES($1,$2)") {
        Ok(prepared) => assert_eq!(sql.describe(&prepared).unwrap_err(), CoreError::Sql(SqlError::DuplicateColumn)),
        Err(error) => assert_eq!(error, CoreError::Sql(SqlError::DuplicateColumn)),
    }
    sql.execute("CREATE TABLE distinct_names(id int, \"ID\" int)", &[], None).unwrap();
    assert_eq!(sql.execute("INSERT INTO distinct_names(id,\"ID\") VALUES(1,2)", &[], None).unwrap().affected, 1);
    let literal = |value| ValueExpr::Literal(value);
    for bad_value in [Value::Int(2), Value::Bool(true)] {
        let values = vec![("id".into(), literal(Value::Int(2))),
            ("v".into(), literal(bad_value)), ("v".into(), literal(Value::Int(3)))];
        for request in [
            Request::Insert { table: "t".into(), values: values.clone() },
            Request::InsertExpressions { table: "t".into(), values: values.clone().into_iter()
                .map(|(name, value)| (name, Expr::Value(value))).collect() },
            Request::InsertMany { table: "t".into(), columns: values.iter().map(|(name, _)| name.clone()).collect(),
                rows: vec![values.into_iter().map(|(_, value)| Expr::Value(value)).collect()], cap: literal(Value::Int(1)) },
        ] {
            assert_eq!(native.execute(&request, &Bindings::new()).unwrap_err().sqlstate(), Some("42701"));
        }
    }
    for table in ["empty_source", "t"] {
        let input = TypedInsertSelect { table: "t".into(), columns: vec!["id".into(), "id".into()],
            source: TypedRead::Single(Box::new(SingleSourceRead { table: table.into(),
                projection: vec![ReadProjection { expression: ReadExpr::Column("id".into()), alias: None }; 2], filter: None })) };
        assert_eq!(sql.execute_typed_insert_select(input, None), Err(CoreError::Sql(SqlError::DuplicateColumn)));
    }
    assert_eq!(sql.execute("SELECT * FROM t", &[], None).unwrap().rows.len(), 1);
    drop(sql); drop(native); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy() { exercise(Engine::Legacy); }

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() { exercise(Engine::V2); }

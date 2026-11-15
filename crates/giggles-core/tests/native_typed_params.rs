mod common;
use giggles_core::{
    Admin, CoreError, Engine as StorageEngine, Limits, SqlSession,
    operations::{Bindings, Compare, Engine, Expr, Intent, Request, ScalarFunction, Transaction, TransactionStep, ValueExpr},
    relational::Value,
};

fn sql(session: &mut SqlSession, text: &str) -> Vec<Vec<Value>> {
    session.execute(text, &[], None).unwrap_or_else(|e| panic!("{text}: {e:?}"))
        .rows.into_iter().map(|r| r.values).collect()
}
fn literal(value: Value) -> ValueExpr { ValueExpr::Literal(value) }
fn eq(field: &str, value: ValueExpr, reverse: bool) -> Expr {
    let field = Box::new(Expr::Field(field.into()));
    let value = Box::new(Expr::Value(value));
    if reverse { Expr::Compare(value, Compare::Eq, field) }
    else { Expr::Compare(field, Compare::Eq, value) }
}
fn read(filter: Expr) -> Request {
    Request::Read {
        table: "t".into(), filter: Some(filter), order: vec![], limit: None,
        projection: vec!["v".into()],
    }
}
fn roundtrip(storage: StorageEngine) {
    let path = common::fresh("native-typed-params");
    let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
    let engine = Engine::new(admin.clone());
    let mut session = admin.sql_session();
    // Values and precision expectations checked with PostgreSQL 18.6 Docker.
    for (ty, input) in [
        ("date", "'2024-02-29'::date"),
        ("time(3)", "'12:34:56.7896'::time"),
        ("timetz(3)", "'12:34:56.7896+02'::timetz"),
        ("timestamp(3)", "'2024-02-29 12:34:56.7896'::timestamp"),
        ("timestamptz(3)", "'2024-02-29 12:34:56.7896+02'::timestamptz"),
        ("interval", "'1 mon 2 days 03:04:05.6789'::interval"),
        ("real", "'0.1'::real"),
        ("double precision", "'0.1'::double precision"),
        ("real", "'NaN'::real"),
        ("double precision", "'Infinity'::double precision"),
        ("uuid", "'{550E8400E29B41D4A716446655440000}'::uuid"),
        ("jsonb", "'{\"b\":2,\"a\":1}'::jsonb"),
    ] {
        sql(&mut session, &format!("CREATE TABLE t(id int PRIMARY KEY, v {ty})"));
        let typed = sql(&mut session, &format!("SELECT {input}"))[0][0].clone();
        let value = match ty {
            "uuid" => Value::Text("{550E8400E29B41D4A716446655440000}".into()),
            "jsonb" => Value::Text("{ \"b\": 2, \"a\": 1 }".into()),
            _ => typed,
        };
        let bindings = Bindings::from([("v".into(), value.clone())]);
        let insert = Request::Insert { table: "t".into(), values: vec![
            ("id".into(), literal(Value::Int(1))),
            ("v".into(), ValueExpr::Parameter("v".into())),
        ] };
        assert_eq!(engine.execute(&insert, &bindings).unwrap().affected, 1, "{ty}");
        sql(&mut session, &format!("INSERT INTO t VALUES (2, {input})"));
        let expected = sql(&mut session, "SELECT v FROM t WHERE id=2")[0].clone();
        assert_eq!(sql(&mut session, "SELECT v FROM t WHERE id=1")[0], expected, "{ty}");
        // A parameter already typed at higher precision is compared without rounding.
        if ty.ends_with("(3)") {
            assert!(engine.execute(&read(eq("v", ValueExpr::Parameter("v".into()), false)),
                &bindings).unwrap().rows.is_empty(), "{ty}");
        }
        let probe = if matches!(ty, "uuid" | "jsonb") { value.clone() } else { expected[0].clone() };
        for reverse in [false, true] {
            let outcome = engine.execute(&read(eq("v", ValueExpr::Parameter("v".into()), reverse)),
                &Bindings::from([("v".into(), probe.clone())])).unwrap();
            assert_eq!(outcome.rows.len(), 2, "{ty}");
            assert!(outcome.rows.iter().all(|r| r.values == expected), "{ty}");
        }
        if matches!(ty, "uuid" | "jsonb") {
            let predicate = eq("v", ValueExpr::Parameter("v".into()), false);
            for filter in [
                Expr::Call { function: ScalarFunction::Coalesce, arguments: vec![
                    predicate.clone(), Expr::Value(literal(Value::Bool(false))),
                ] },
                Expr::Case { operand: None, arms: vec![(predicate,
                    Expr::Value(literal(Value::Bool(true))))],
                    else_expr: Some(Box::new(Expr::Value(literal(Value::Bool(false))))) },
            ] {
                assert_eq!(engine.execute(&read(filter),
                    &Bindings::from([("v".into(), probe.clone())])).unwrap().rows.len(), 2);
            }
        }
        let transaction = Transaction { read_only: true, emit: None, steps: vec![
            TransactionStep::Execute(read(eq("v", ValueExpr::Parameter("v".into()), false))),
        ] };
        let result = engine.execute_transaction(&transaction,
            &Bindings::from([("v".into(), probe)])).unwrap();
        assert_eq!(result.summaries[0].matched, 2, "{ty}");
        sql(&mut session, "UPDATE t SET v=NULL WHERE id=1");
        let update = Request::Update {
            table: "t".into(), filter: eq("id", literal(Value::Int(1)), false),
            guard: None, values: vec![("v".into(), ValueExpr::Parameter("v".into()))],
            intent: Intent::One,
        };
        let outcome = engine.execute(&update, &bindings).unwrap();
        assert_eq!((outcome.matched, outcome.affected, outcome.changed), (1, 1, 1), "{ty}");
        assert_eq!(sql(&mut session, "SELECT v FROM t WHERE id=1")[0], expected, "{ty}");
        sql(&mut session, "DROP TABLE t");
    }
    // Interval equality uses PostgreSQL's 30-day month ordering, not field identity.
    sql(&mut session, "CREATE TABLE t(id int PRIMARY KEY, v interval)");
    sql(&mut session, "INSERT INTO t VALUES (1, '1 mon'::interval)");
    let days = sql(&mut session, "SELECT '30 days'::interval")[0][0].clone();
    assert_eq!(engine.execute(&read(eq("v", literal(days), false)), &Bindings::new())
        .unwrap().rows.len(), 1);
    drop(session); drop(engine); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
fn invalid(storage: StorageEngine) {
    let path = common::fresh("native-typed-invalid");
    let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
    let engine = Engine::new(admin.clone());
    let mut session = admin.sql_session();
    for (ty, bad, good) in [
        ("uuid", "bad", "'550e8400-e29b-41d4-a716-446655440000'"),
        ("jsonb", "{", "'{}'"),
    ] {
        sql(&mut session, &format!("CREATE TABLE t(id int PRIMARY KEY, v {ty})"));
        sql(&mut session, &format!("INSERT INTO t VALUES (1, {good})"));
        let Err(CoreError::Sql(error)) = session.execute(
            &format!("INSERT INTO t VALUES (2, '{bad}')"), &[], None) else { panic!("{ty}") };
        let state = giggles_core::sqlstate(&CoreError::Sql(error));
        assert_eq!(state, "22P02"); // PG18.6 INSERT errors, pg.sql / pg.txt.
        let Err(CoreError::Sql(error)) = session.execute(
            &format!("UPDATE t SET v='{bad}' WHERE id=999"), &[], None) else { panic!("{ty}") };
        assert_eq!(giggles_core::sqlstate(&CoreError::Sql(error)), state);
        let bindings = Bindings::from([("v".into(), Value::Text(bad.into()))]);
        let requests = [
            Request::Insert { table: "t".into(), values: vec![
                ("id".into(), literal(Value::Int(2))),
                ("v".into(), ValueExpr::Parameter("v".into())),
            ] },
            Request::Update { table: "t".into(), filter: eq("id", literal(Value::Int(1)), false),
                guard: None, values: vec![("v".into(), ValueExpr::Parameter("v".into()))], intent: Intent::One },
            Request::Update { table: "t".into(), filter: eq("id", literal(Value::Int(999)), false),
                guard: None, values: vec![("v".into(), ValueExpr::Parameter("v".into()))], intent: Intent::One },
            read(eq("v", ValueExpr::Parameter("v".into()), false)),
        ];
        for request in requests {
            let err = engine.execute(&request, &bindings).unwrap_err();
            assert_eq!(err.sqlstate(), Some(state), "{ty}: {request:?}");
        }
        assert_eq!(sql(&mut session, "SELECT count(*) FROM t"), [vec![Value::Int(1)]]);
        sql(&mut session, "DROP TABLE t");
    }
    drop(session); drop(engine); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_roundtrip() { roundtrip(StorageEngine::Legacy); }
#[test]
fn legacy_invalid_sqlstate() { invalid(StorageEngine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_roundtrip() { roundtrip(StorageEngine::V2); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_invalid_sqlstate() { invalid(StorageEngine::V2); }

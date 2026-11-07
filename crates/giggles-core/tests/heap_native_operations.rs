mod common;

use giggles_core::{Admin, Limits, operations::*, relational::Value};
use giggles_core::operations::once::*;
use std::time::{Duration, Instant};

fn read(table: &str) -> Request {
    Request::Read {
        table: table.into(), filter: None, order: vec![], limit: None, projection: vec![],
    }
}
fn yes() -> Expr { Expr::Value(ValueExpr::Literal(Value::Bool(true))) }
fn transaction(table: &str) -> Transaction {
    Transaction { read_only: true, steps: vec![TransactionStep::Let { name: "rows".into(), request: read(table) }], emit: Some(vec![EmitExpr::Rows("rows".into())]) }
}
fn write(table: &str) -> Request {
    Request::Update {
        table: table.into(),
        filter: Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Eq, Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1))))),
        guard: None,
        values: vec![("n".into(), ValueExpr::Literal(Value::Int(20)))],
        intent: Intent::One,
    }
}
fn write_transaction(table: &str) -> Transaction {
    Transaction { read_only: false, steps: vec![TransactionStep::Execute(write(table))], emit: None }
}
fn clock() -> std::result::Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: 1000, upper_unix_seconds: 1001, valid_until: Instant::now() + Duration::from_secs(10) })
}
fn once(admin: &Admin, key: u8, body: OnceBody) -> OnceRequest {
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity { database_domain: admin.snapshot().unwrap().domain, tenant: 1, namespace: "heap-tests".into(), key: vec![key] },
        retention_seconds: 60,
        body,
    }
}

#[test]
fn heap_native_bind_transaction_once_and_inspection_boundaries() {
    let path = common::fresh("b3n-native-operations");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    for text in [
        "CREATE TABLE heap (id BIGINT, n BIGINT)",
        "CREATE TABLE keyed (id BIGINT PRIMARY KEY, n BIGINT)",
        "INSERT INTO heap VALUES (1, 10)",
        "INSERT INTO keyed VALUES (1, 10)",
    ] { sql.execute(text, &[], None).unwrap(); }
    drop(sql);
    let engine = Engine::new(admin.clone());
    let bindings = Bindings::new();
    let insert = Request::Insert { table: "heap".into(), values: vec![("id".into(), ValueExpr::Literal(Value::Int(2)))] };
    let update = |intent| Request::Update { table: "heap".into(), filter: yes(), guard: None, values: vec![("n".into(), ValueExpr::Literal(Value::Int(20)))], intent };
    let requests = vec![
        read("heap"),
        insert.clone(),
        update(Intent::One),
        update(Intent::Many { cap: ValueExpr::Literal(Value::Int(1)) }),
        update(Intent::Claim { order: vec![Sort { field: "id".into(), descending: false }] }),
        Request::Delete { table: "heap".into(), filter: yes(), guard: None, cap: None },
        Request::ReadExpressions { table: "heap".into(), filter: None, order: vec![], limit: None, projection: vec![] },
        Request::InsertExpressions { table: "heap".into(), values: vec![("id".into(), Expr::Value(ValueExpr::Literal(Value::Int(2))))] },
        Request::UpdateExpressions { table: "heap".into(), filter: yes(), guard: None, values: vec![("n".into(), Expr::Value(ValueExpr::Literal(Value::Int(20))))], intent: Intent::One },
        Request::InsertMany { table: "heap".into(), columns: vec!["id".into()], rows: vec![vec![Expr::Value(ValueExpr::Literal(Value::Int(2)))]], cap: ValueExpr::Literal(Value::Int(1)) },
        Request::Returning { request: Box::new(insert.clone()), projection: vec![] },
        Request::OnConflict { request: Box::new(insert), target: vec!["id".into()], action: ConflictAction::Nothing },
    ];
    for request in requests {
        assert_eq!(engine.prepare_bound(&request, &bindings).err(), Some(Error::Unsupported), "{request:?}");
        assert_eq!(engine.execute(&request, &bindings), Err(Error::Unsupported), "{request:?}");
    }
    assert_eq!(engine.explain_read(&read("heap"), &bindings).err(), Some(Error::Unsupported));
    assert!(engine.explain_read(&read("keyed"), &bindings).is_ok());
    let prepared = engine.prepare_bound(&read("keyed"), &bindings).unwrap();
    assert_eq!(prepared.description().columns.len(), 2);
    assert_eq!(engine.execute_bound(&prepared).unwrap().rows[0].values, [Value::Int(1), Value::Int(10)]);
    assert_eq!(engine.prepare_transaction_bound(&transaction("heap"), &bindings).err(), Some(Error::Unsupported));
    assert_eq!(engine.execute_transaction(&transaction("heap"), &bindings), Err(Error::Unsupported));
    assert!(engine.execute_transaction(&transaction("keyed"), &bindings).is_ok());
    for (key, body) in [
        (1, OnceBody::Operation(write("heap"))),
        (2, OnceBody::Transaction(write_transaction("heap"))),
    ] {
        let request = once(&admin, key, body);
        assert_eq!(engine.execute_once(&request, &bindings, &clock), Err(OnceError::Operation(Error::Unsupported)));
        assert_eq!(engine.outcome(&request.identity).unwrap(), OnceStatus::Absent);
    }
    for (key, body) in [
        (3, OnceBody::Operation(write("keyed"))),
        (4, OnceBody::Transaction(write_transaction("keyed"))),
    ] {
        let request = once(&admin, key, body);
        assert!(engine.execute_once(&request, &bindings, &clock).unwrap().outcome.is_ok());
        assert!(engine.execute_once(&request, &bindings, &clock).unwrap().replayed);
    }
    let summaries = engine.inspect(&InspectionRequest::Tables).unwrap();
    let InspectionResult::Tables(summaries) = summaries.result else { unreachable!() };
    for (name, keys) in [("heap", 0), ("keyed", 1)] {
        let summary = summaries.iter().find(|summary| summary.name == name).unwrap();
        assert_eq!((summary.column_count, summary.primary_key_columns), (2, keys));
        let result = engine.inspect(&InspectionRequest::Table { table: name.into() }).unwrap();
        let InspectionResult::Table(table) = result.result else { unreachable!() };
        assert_eq!(table.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(), ["id", "n"]);
        assert_eq!(table.primary_key.len(), keys);
        if name == "keyed" { assert_eq!(admin.catalog().unwrap().1.iter().find(|t| t.name == name), Some(&table)); }
    }
    let rows = admin.sql_session().execute("SELECT * FROM heap", &[], None).unwrap();
    assert_eq!(rows.rows[0].values, [Value::Int(1), Value::Int(10)]);
    drop(engine);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

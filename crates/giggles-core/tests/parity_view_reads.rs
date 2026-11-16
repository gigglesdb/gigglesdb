//! Native typed reads of views match SQL `SELECT ... FROM <view>` (GD-0001 R2).
//! PostgreSQL 18.6 reads a view like a table in FROM; native reads use SQL's
//! own view expansion, then native filter, order, limit and projection.
mod common;
use giggles_core::operations::{
    self, Arithmetic, Bindings, Compare, Expr, Projection, Request, Sort, ValueExpr,
};
use giggles_core::{Admin, CoreError, Engine as Backend, Limits, relational::*};

fn int(n: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(n)))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn math(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
}
fn item(expression: Expr, alias: Option<&str>) -> Projection {
    Projection { expression, alias: alias.map(Into::into) }
}
fn read(table: &str, projection: &[&str]) -> Request {
    Request::Read { table: table.into(), filter: None, order: vec![], limit: None,
        projection: projection.iter().map(|c| (*c).into()).collect() }
}
fn expressions(table: &str, projection: Vec<Projection>) -> Request {
    Request::ReadExpressions { table: table.into(), filter: None, order: vec![], limit: None, projection }
}
fn values(rows: &[SqlRow]) -> Vec<Vec<Value>> {
    rows.iter().map(|row| row.values.clone()).collect()
}
fn backends() -> Vec<Backend> {
    let mut backends = vec![Backend::Legacy];
    if cfg!(feature = "engine-rocksdb") { backends.push(Backend::V2); }
    backends
}
/// Native and SQL agree on columns and rows, or on the SQLSTATE.
fn same(native: operations::Result<operations::Outcome>, sql: Result<SqlResult, CoreError>, what: &str) {
    match (native, sql) {
        (Ok(native), Ok(sql)) => {
            assert_eq!(native.columns, sql.columns, "{what}");
            assert_eq!(values(&native.rows), values(&sql.rows), "{what}");
        }
        (Err(native), Err(sql)) => {
            assert_eq!(native.sqlstate(), Some(giggles_core::sqlstate(&sql)), "{what}: {native:?} vs {sql:?}");
        }
        (native, sql) => panic!("{what}: native {native:?}, SQL {sql:?}"),
    }
}
fn cases() -> Vec<(Request, &'static str)> {
    let filtered = |table: &str, projection, filter, order, limit: Option<i64>| Request::ReadExpressions {
        table: table.into(), filter: Some(filter), order, limit: limit.map(|n| ValueExpr::Literal(Value::Int(n))), projection,
    };
    vec![
        (read("v", &[]), "SELECT * FROM v"),
        (read("v", &["qty", "id"]), "SELECT qty, id FROM v"),
        (expressions("v", vec![item(math(field("qty"), Arithmetic::Multiply, int(2)), Some("d"))]),
            "SELECT qty*2 AS d FROM v"),
        (expressions("v", vec![item(field("id"), None), item(math(field("qty"), Arithmetic::Add, field("id")), Some("s"))]),
            "SELECT id, qty+id AS s FROM v"),
        (filtered("v", vec![item(field("id"), None), item(math(field("qty"), Arithmetic::Add, int(1)), Some("e"))],
            Expr::Compare(Box::new(field("qty")), Compare::Gt, Box::new(int(4))),
            vec![Sort { field: "id".into(), descending: true }], Some(1)),
            "SELECT id, qty+1 AS e FROM v WHERE qty > 4 ORDER BY id DESC LIMIT 1"),
        (Request::Read { table: "v".into(), filter: Some(Expr::IsNull(Box::new(field("qty")), true)),
            order: vec![Sort { field: "qty".into(), descending: true }], limit: None, projection: vec![] },
            "SELECT * FROM v WHERE qty IS NOT NULL ORDER BY qty DESC"),
        (Request::Read { table: "v".into(), filter: None,
            order: vec![Sort { field: "name".into(), descending: false }], limit: Some(ValueExpr::Literal(Value::Int(2))),
            projection: vec!["name".into()] },
            "SELECT name FROM v ORDER BY name LIMIT 2"),
        // A renamed view, a view over a view, and a schema-qualified view.
        (expressions("r", vec![item(math(field("x"), Arithmetic::Subtract, field("y")), Some("diff"))]),
            "SELECT x-y AS diff FROM r"),
        (read("n", &[]), "SELECT * FROM n"),
        (read("app.v", &[]), "SELECT * FROM app.v"),
        // Errors keep SQL's SQLSTATE.
        (expressions("v", vec![item(math(field("qty"), Arithmetic::Divide, int(0)), Some("z"))]),
            "SELECT qty/0 AS z FROM v"),
        (read("missing", &[]), "SELECT * FROM missing"),
    ]
}

#[test]
fn native_view_reads_match_sql() {
    for backend in backends() {
        let path = common::fresh("parity-view-reads");
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let engine = operations::Engine::new(admin.clone());
        let mut s = admin.sql_session();
        for statement in ["CREATE SCHEMA app", "CREATE TABLE t(id INT PRIMARY KEY, qty INT, name TEXT)",
            "INSERT INTO t VALUES(1,5,'c'),(2,NULL,'a'),(3,7,'b'),(4,3,NULL)"] {
            s.execute(statement, &[], None).unwrap();
        }
        let views = s.execute("CREATE VIEW v AS SELECT id, qty, name FROM t", &[], None);
        if matches!(backend, Backend::Legacy) {
            // Legacy has no views; every read is then SQL's own error (42P01).
            assert_eq!(views.unwrap_err(), CoreError::Sql(SqlError::Unsupported));
        } else {
            views.unwrap();
            for statement in ["CREATE VIEW r(x, y) AS SELECT qty, id FROM t", "CREATE VIEW n AS SELECT y FROM r",
                "CREATE VIEW app.v AS SELECT name, id FROM t"] {
                s.execute(statement, &[], None).unwrap();
            }
        }
        for (request, sql) in cases() {
            same(engine.execute(&request, &Bindings::new()), s.execute(sql, &[], None), sql);
            same(s.execute_native(&request, &Bindings::new()), s.execute(sql, &[], None), sql);
        }
        drop(s); drop(engine); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn view_reads_share_snapshots_descriptions_and_fences_with_sql() {
    use operations::{EmitExpr, EmittedValue, Transaction, TransactionStep};
    let path = common::fresh("parity-view-snapshots");
    let admin = Admin::create_engine(&path, Limits::default(), Backend::V2).unwrap();
    let engine = operations::Engine::new(admin.clone());
    let mut s = admin.sql_session();
    for statement in ["CREATE TABLE t(id INT PRIMARY KEY, qty INT, name TEXT)",
        "INSERT INTO t VALUES(1,5,'c'),(2,NULL,'a')", "CREATE VIEW v AS SELECT id, qty FROM t"] {
        s.execute(statement, &[], None).unwrap();
    }
    let doubled = expressions("v", vec![item(field("id"), None), item(math(field("qty"), Arithmetic::Multiply, int(2)), Some("d"))]);
    let sql = "SELECT id, qty*2 AS d FROM v";

    // Uncommitted base rows are visible through the view in the same transaction.
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO t VALUES(3,10,'b')", &[], None).unwrap();
    same(s.execute_native(&doubled, &Bindings::new()), s.execute(sql, &[], None), "in transaction");
    assert_eq!(s.execute_native(&doubled, &Bindings::new()).unwrap().rows.len(), 3);
    s.execute("ROLLBACK", &[], None).unwrap();

    // Prepared reads describe SQL's column names and types, and fence on the view.
    let prepared = engine.prepare_bound(&doubled, &Bindings::new()).unwrap();
    let statement = s.prepare(sql).unwrap();
    let described = s.describe(&statement).unwrap();
    let shape = |columns: &[ColumnDef]| columns.iter().map(|c| (c.name.clone(), c.data_type.clone())).collect::<Vec<_>>();
    let names = |columns: &[ColumnDef]| columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>();
    assert_eq!(names(&prepared.description().columns), names(&described.columns));
    assert_eq!(shape(&prepared.description().columns)[0], shape(&described.columns)[0]);
    // Native types an expression the same way for a view as for its base table.
    let base = expressions("t", vec![item(field("id"), None), item(math(field("qty"), Arithmetic::Multiply, int(2)), Some("d"))]);
    assert_eq!(shape(&prepared.description().columns), shape(&engine.prepare_bound(&base, &Bindings::new()).unwrap().description().columns));
    same(engine.execute_bound(&prepared), s.execute(sql, &[], None), "bound");

    // Transaction blocks read views too, as locals and emitted rows.
    let block = Transaction { read_only: true, steps: vec![TransactionStep::Let { name: "rows".into(), request: doubled.clone() }],
        emit: Some(vec![EmitExpr::Rows("rows".into())]) };
    let outcome = engine.execute_transaction(&block, &Bindings::new()).unwrap();
    let EmittedValue::Rows { rows, columns, .. } = &outcome.emitted[0] else { panic!("{outcome:?}") };
    let expected = s.execute(sql, &[], None).unwrap();
    assert_eq!((columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(), values(rows)), (expected.columns, values(&expected.rows)));
    let bound = engine.prepare_transaction_bound(&block, &Bindings::new()).unwrap();
    assert_eq!(engine.execute_transaction_bound(&bound).unwrap(), outcome);

    // Writes through a view and keyset pages over one stay unsupported (42P01).
    let insert = Request::Insert { table: "v".into(), values: vec![("id".into(), ValueExpr::Literal(Value::Int(9)))] };
    assert_eq!(engine.execute(&insert, &Bindings::new()).unwrap_err().sqlstate(), Some("42P01"));
    let page = Request::Read { table: "v".into(), filter: None, order: vec![Sort { field: "id".into(), descending: false }],
        limit: None, projection: vec![] };
    assert_eq!(engine.read_page(&page, None, &Bindings::new()).unwrap_err().sqlstate(), Some("42P01"));

    // A replaced view invalidates the prepared fences, like a changed table.
    s.execute("DROP VIEW v", &[], None).unwrap();
    s.execute("CREATE VIEW v AS SELECT qty, id FROM t", &[], None).unwrap();
    assert!(engine.execute_bound(&prepared).is_err());
    assert!(engine.execute_transaction_bound(&bound).is_err());
    same(engine.execute(&doubled, &Bindings::new()), s.execute(sql, &[], None), "replaced");
    drop(s); drop(engine); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

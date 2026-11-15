mod common;

use giggles_core::{
    Admin, Limits,
    operations::{
        Bindings, Compare, Engine, Error, Expr, MAX_BINDING_BYTES, MAX_BINDINGS,
        MAX_BOUND_VALUE_BYTES, Request, ValueExpr,
    },
    relational::{MAX_TRANSACTIONS, SqlRow, Value as V},
};

fn fixture() -> (Admin, Engine) {
    let admin = Admin::create(common::fresh("binding-limits"), Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
    (admin.clone(), Engine::new(admin))
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn literal(value: V) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn read(filter: Option<Expr>) -> Request {
    Request::Read {
        table: "items".into(),
        filter,
        order: vec![],
        // An explicit limit would itself consume nine resolved-value bytes.
        limit: None,
        projection: vec![],
    }
}
fn equal(left: Expr, right: Expr) -> Expr {
    Expr::Compare(Box::new(left), Compare::Eq, Box::new(right))
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin
        .export(common::fresh("binding-limits-export"))
        .unwrap()
}
fn rejects_without_effects(
    admin: &Admin,
    engine: &Engine,
    request: &Request,
    bindings: &Bindings,
    expected: Error,
) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    let existing = rows(admin);
    assert_eq!(
        engine.prepare_bound(request, bindings).unwrap_err(),
        expected
    );
    assert_eq!(
        digest(admin),
        before,
        "failed preparation changed durable state"
    );
    assert_eq!(engine.execute(request, bindings), Err(expected));
    assert_eq!(
        digest(admin),
        before,
        "failed execution changed durable state"
    );
    assert_eq!(admin.catalog().unwrap(), catalog);
    assert_eq!(rows(admin), existing);
}
fn accepts_empty(engine: &Engine, request: &Request, bindings: &Bindings) {
    let prepared = engine.prepare_bound(request, bindings).unwrap();
    assert!(engine.execute_bound(&prepared).unwrap().rows.is_empty());
    assert!(engine.execute(request, bindings).unwrap().rows.is_empty());
}
fn balanced_and(mut expressions: Vec<Expr>) -> Expr {
    while expressions.len() > 1 {
        let mut next = Vec::with_capacity(expressions.len().div_ceil(2));
        let mut current = expressions.into_iter();
        while let Some(left) = current.next() {
            next.push(match current.next() {
                Some(right) => Expr::And(Box::new(left), Box::new(right)),
                None => left,
            });
        }
        expressions = next;
    }
    expressions.pop().unwrap()
}
fn named_boolean_filter(count: usize) -> (Request, Bindings) {
    let names: Vec<_> = (0..count).map(|index| format!("p{index}")).collect();
    let filter = balanced_and(names.iter().map(|name| parameter(name)).collect());
    let bindings = names
        .into_iter()
        .map(|name| (name, V::Bool(true)))
        .collect();
    (read(Some(filter)), bindings)
}

#[test]
fn exact_text_and_bytes_payload_limits_are_admitted_and_frozen() {
    for payload in [
        V::Text("é".repeat(MAX_BINDING_BYTES / 2)),
        V::Bytes(vec![255; MAX_BINDING_BYTES]),
    ] {
        let (admin, engine) = fixture();
        let request = read(Some(Expr::IsNull(Box::new(parameter("p")), false)));
        let mut bindings = Bindings::from([("p".into(), payload)]);
        let before = digest(&admin);
        let prepared = engine.prepare_bound(&request, &bindings).unwrap();
        assert_eq!(digest(&admin), before);
        assert!(engine.execute_bound(&prepared).unwrap().rows.is_empty());
        assert!(engine.execute(&request, &bindings).unwrap().rows.is_empty());

        bindings.insert("p".into(), V::Null);
        admin
            .sql_session()
            .execute("INSERT INTO items VALUES (1,'new')", &[], None)
            .unwrap();
        // The accepted handle still holds the original non-null 64 KiB value.
        assert_eq!(engine.execute_bound(&prepared).unwrap().matched, 0);
        assert_eq!(engine.execute(&request, &bindings).unwrap().matched, 1);
    }
}

#[test]
fn oversized_supplied_values_reject_even_when_unused() {
    let (admin, engine) = fixture();
    for value in [
        V::Text(format!("{}x", "é".repeat(MAX_BINDING_BYTES / 2))),
        V::Bytes(vec![0; MAX_BINDING_BYTES + 1]),
    ] {
        let bindings = Bindings::from([("unused".into(), value)]);
        rejects_without_effects(&admin, &engine, &read(None), &bindings, Error::Limit);
    }

    // Scalar and NULL values cost nine bytes in the aggregate supplied budget.
    let request = read(Some(Expr::Or(
        Box::new(Expr::IsNull(Box::new(parameter("p")), false)),
        Box::new(Expr::IsNull(Box::new(parameter("q")), false)),
    )));
    let bindings = Bindings::from([
        ("p".into(), V::Bytes(vec![0; MAX_BINDING_BYTES - 9])),
        ("q".into(), V::Null),
    ]);
    accepts_empty(&engine, &request, &bindings);
    let oversized = Bindings::from([
        ("p".into(), V::Bytes(vec![0; MAX_BINDING_BYTES - 8])),
        ("q".into(), V::Null),
    ]);
    rejects_without_effects(&admin, &engine, &request, &oversized, Error::Limit);
}

#[test]
fn each_repeated_parameter_occurrence_counts_before_any_row_is_read() {
    let (admin, engine) = fixture();
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(MAX_BOUND_VALUE_BYTES / 2)))]);
    let twice = equal(parameter("p"), parameter("p"));
    accepts_empty(&engine, &read(Some(twice.clone())), &bindings);
    let thrice = read(Some(Expr::And(
        Box::new(twice),
        Box::new(Expr::IsNull(Box::new(parameter("p")), true)),
    )));
    rejects_without_effects(&admin, &engine, &thrice, &bindings, Error::Limit);
    assert!(rows(&admin).is_empty());
}

#[test]
fn literal_parameter_and_explicit_limit_values_share_one_resolved_budget() {
    let (admin, engine) = fixture();
    let half = MAX_BOUND_VALUE_BYTES / 2;
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(half)))]);
    let exact = read(Some(equal(
        literal(V::Text("x".repeat(half))),
        parameter("p"),
    )));
    accepts_empty(&engine, &exact, &bindings);
    let oversized = read(Some(equal(
        literal(V::Text("x".repeat(half + 1))),
        parameter("p"),
    )));
    rejects_without_effects(&admin, &engine, &oversized, &bindings, Error::Limit);

    let mut limited = read(Some(Expr::IsNull(Box::new(parameter("p")), false)));
    if let Request::Read { limit, .. } = &mut limited {
        *limit = Some(ValueExpr::Literal(V::Int(0)));
    }
    let exact = Bindings::from([("p".into(), V::Bytes(vec![0; MAX_BOUND_VALUE_BYTES - 9]))]);
    accepts_empty(&engine, &limited, &exact);
    let oversized = Bindings::from([("p".into(), V::Bytes(vec![0; MAX_BOUND_VALUE_BYTES - 8]))]);
    rejects_without_effects(&admin, &engine, &limited, &oversized, Error::Limit);
}

#[test]
fn balanced_boolean_filters_accept_256_names_and_reject_257_before_catalog_lookup() {
    let (admin, engine) = fixture();
    let (exact_request, exact_bindings) = named_boolean_filter(MAX_BINDINGS);
    accepts_empty(&engine, &exact_request, &exact_bindings);

    let (mut request, bindings) = named_boolean_filter(MAX_BINDINGS + 1);
    rejects_without_effects(&admin, &engine, &request, &bindings, Error::Limit);
    if let Request::Read { table, .. } = &mut request {
        *table = "missing_table".into();
    }
    rejects_without_effects(&admin, &engine, &request, &bindings, Error::Limit);
}

#[test]
fn supplied_resource_limits_are_checked_before_transaction_admission() {
    let (admin, engine) = fixture();
    let mut sessions: Vec<_> = (0..MAX_TRANSACTIONS).map(|_| admin.sql_session()).collect();
    for session in &mut sessions {
        session.execute("BEGIN", &[], None).unwrap();
    }
    let request = read(None);
    assert_eq!(
        engine
            .prepare_bound(&request, &Bindings::new())
            .unwrap_err(),
        Error::Core("Busy".into())
    );
    assert_eq!(
        engine.execute(&request, &Bindings::new()),
        Err(Error::Core("Busy".into()))
    );

    let oversized = Bindings::from([("unused".into(), V::Bytes(vec![0; MAX_BINDING_BYTES + 1]))]);
    rejects_without_effects(&admin, &engine, &request, &oversized, Error::Limit);
    let (_, overcount) = named_boolean_filter(MAX_BINDINGS + 1);
    rejects_without_effects(&admin, &engine, &request, &overcount, Error::Limit);
}

#[test]
fn under_budget_missing_extra_and_type_errors_keep_their_existing_meaning() {
    let (admin, engine) = fixture();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES (1,'unchanged')", &[], None)
        .unwrap();
    let request = read(Some(equal(Expr::Field("id".into()), parameter("id"))));
    for (bindings, expected) in [
        (Bindings::new(), Error::Bind("parameter missing".into())),
        (
            Bindings::from([("id".into(), V::Text("wrong type".into()))]),
            Error::Sql { sqlstate: "42883".into() },
        ),
        (
            Bindings::from([("id".into(), V::Int(1)), ("extra".into(), V::Bool(true))]),
            Error::Bind("parameters".into()),
        ),
    ] {
        rejects_without_effects(&admin, &engine, &request, &bindings, expected);
    }

    // Supplied unused names are detected by the used-name subset's length,
    // without needing to copy a potentially large caller-owned key.
    let large_unused_key = Bindings::from([("unused".repeat(64 * 1024), V::Bool(true))]);
    rejects_without_effects(
        &admin,
        &engine,
        &read(None),
        &large_unused_key,
        Error::Bind("parameters".into()),
    );
}

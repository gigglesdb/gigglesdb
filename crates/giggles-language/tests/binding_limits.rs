use giggles_core::{Admin, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
enum EntryPoint {
    ExecuteText,
    ExecuteTree,
    PrepareText,
    PrepareTree,
}

const ENTRY_POINTS: [EntryPoint; 4] = [
    EntryPoint::ExecuteText,
    EntryPoint::ExecuteTree,
    EntryPoint::PrepareText,
    EntryPoint::PrepareTree,
];

fn fixture() -> (Admin, Engine) {
    let path = std::env::temp_dir().join(format!(
        "giggles-bind-limits-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "create table items (id bigint primary key, state text not null)",
        &[],
        None,
    )
    .unwrap();
    sql.execute("insert into items values (1,'ready')", &[], None)
        .unwrap();
    let engine = Engine::new(admin.clone());
    (admin, engine)
}

fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("select * from items limit 1024", &[], None)
        .unwrap()
        .rows
}

// Preparation failures must happen during preparation itself, not execution.
fn admit(entry: EntryPoint, engine: &Engine, text: &str, bindings: &Bindings) -> Result<()> {
    match entry {
        EntryPoint::ExecuteText => engine.execute_text(text, bindings).map(|_| ()),
        EntryPoint::ExecuteTree => engine.execute(&parse(text)?, bindings).map(|_| ()),
        EntryPoint::PrepareText => engine.prepare_bound_text(text, bindings).map(|_| ()),
        EntryPoint::PrepareTree => engine.prepare_bound(&parse(text)?, bindings).map(|_| ()),
    }
}

fn evaluate(
    entry: EntryPoint,
    engine: &Engine,
    text: &str,
    bindings: &Bindings,
) -> Result<Outcome> {
    match entry {
        EntryPoint::ExecuteText => engine.execute_text(text, bindings),
        EntryPoint::ExecuteTree => engine.execute(&parse(text)?, bindings),
        EntryPoint::PrepareText => {
            engine.execute_bound(&engine.prepare_bound_text(text, bindings)?)
        }
        EntryPoint::PrepareTree => {
            engine.execute_bound(&engine.prepare_bound(&parse(text)?, bindings)?)
        }
    }
}

fn assert_refusal(engine: &Engine, text: &str, bindings: &Bindings, expected: Error) {
    for entry in ENTRY_POINTS {
        assert_eq!(
            admit(entry, engine, text, bindings),
            Err(expected.clone()),
            "{entry:?}: {text}"
        );
    }
}

#[test]
fn oversized_supplied_bindings_fail_across_all_native_entry_points() {
    let (admin, engine) = fixture();
    let before = rows(&admin);
    with_sql_parser_forbidden(|| {
        let too_many = (0..=MAX_BINDINGS)
            .map(|i| (format!("p{i}"), Value::Null))
            .collect();
        // Map admission runs even before a missing-table bind, so these must
        // report Limit rather than the ordinary table/unused-parameter errors.
        assert_refusal(&engine, "missing", &too_many, Error::Limit);
        assert_refusal(
            &engine,
            "delete many items where true at most 1",
            &too_many,
            Error::Limit,
        );
        for payload in [
            Value::Text("x".repeat(MAX_BINDING_BYTES + 1)),
            Value::Bytes(vec![0; MAX_BINDING_BYTES + 1]),
            Value::Text("é".repeat(MAX_BINDING_BYTES / 2 + 1)),
        ] {
            let bindings = Bindings::from([("payload".into(), payload)]);
            assert_refusal(
                &engine,
                "missing where $payload is null",
                &bindings,
                Error::Limit,
            );
            assert_refusal(
                &engine,
                "delete many items where true at most 1",
                &bindings,
                Error::Limit,
            );
        }
        let combined = Bindings::from([
            ("a".into(), Value::Text("a".repeat(MAX_BINDING_BYTES / 2))),
            ("b".into(), Value::Bytes(vec![0; MAX_BINDING_BYTES / 2 + 1])),
        ]);
        assert_refusal(
            &engine,
            "items where $a is null or $b is null",
            &combined,
            Error::Limit,
        );
    });
    assert_eq!(rows(&admin), before);
}

#[test]
fn repeated_parameters_and_explicit_literals_share_the_expanded_budget() {
    let (admin, engine) = fixture();
    let before = rows(&admin);
    with_sql_parser_forbidden(|| {
        for payload in [
            Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES / 2 + 1)),
            Value::Bytes(vec![0; MAX_BOUND_VALUE_BYTES / 2 + 1]),
        ] {
            let bindings = Bindings::from([("payload".into(), payload)]);
            assert_refusal(
                &engine,
                "items where $payload=$payload",
                &bindings,
                Error::Limit,
            );
        }
        let exact_map = Bindings::from([(
            "payload".into(),
            Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES)),
        )]);
        assert_refusal(
            &engine,
            "items where $payload=\"x\"",
            &exact_map,
            Error::Limit,
        );
        assert_refusal(
            &engine,
            "items where $payload is null limit 1",
            &exact_map,
            Error::Limit,
        );
        assert_refusal(
            &engine,
            "delete many items where $payload is null at most 0",
            &exact_map,
            Error::Limit,
        );
        let scalar_overflow = Bindings::from([(
            "payload".into(),
            Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES - 8)),
        )]);
        assert_refusal(
            &engine,
            "items where $payload is null limit 1",
            &scalar_overflow,
            Error::Limit,
        );
        assert_refusal(
            &engine,
            "items where $payload is null or null",
            &scalar_overflow,
            Error::Limit,
        );
        assert_refusal(
            &engine,
            "items where $payload is null or false",
            &scalar_overflow,
            Error::Limit,
        );
    });
    assert_eq!(rows(&admin), before);
}

fn conjunction(terms: &[String]) -> String {
    if terms.len() == 1 {
        return terms[0].clone();
    }
    let middle = terms.len() / 2;
    format!(
        "({} and {})",
        conjunction(&terms[..middle]),
        conjunction(&terms[middle..])
    )
}

#[test]
fn exact_binding_boundaries_succeed_without_charging_implicit_defaults() {
    let (admin, engine) = fixture();
    let before = rows(&admin);
    let manifest = capability_manifest();
    let limits = manifest.limits;
    with_sql_parser_forbidden(|| {
        for entry in ENTRY_POINTS {
            for payload in [
                Value::Text("x".repeat(limits.binding_payload_bytes)),
                Value::Bytes(vec![0; limits.binding_payload_bytes]),
            ] {
                let bindings = Bindings::from([("payload".into(), payload)]);
                // No explicit LIMIT: the implicit default page must not be
                // charged as another nine-byte source value.
                assert_eq!(
                    evaluate(entry, &engine, "items where $payload is null", &bindings)
                        .unwrap()
                        .matched,
                    0
                );
            }
            let repeated = Bindings::from([(
                "payload".into(),
                Value::Text("x".repeat(limits.bound_value_bytes / 2)),
            )]);
            assert_eq!(
                evaluate(entry, &engine, "items where $payload=$payload", &repeated)
                    .unwrap()
                    .matched,
                1
            );
            let explicit_limit = Bindings::from([(
                "payload".into(),
                Value::Text("x".repeat(limits.bound_value_bytes - 9)),
            )]);
            assert_eq!(
                evaluate(
                    entry,
                    &engine,
                    "items where $payload is null limit 1",
                    &explicit_limit
                )
                .unwrap()
                .matched,
                0
            );
            let two_values = Bindings::from([
                (
                    "a".into(),
                    Value::Text("x".repeat(limits.binding_payload_bytes / 2)),
                ),
                (
                    "b".into(),
                    Value::Bytes(vec![0; limits.binding_payload_bytes / 2]),
                ),
            ]);
            assert_eq!(
                evaluate(
                    entry,
                    &engine,
                    "items where $a is null or $b is null",
                    &two_values
                )
                .unwrap()
                .matched,
                0
            );
            let bindings: Bindings = (0..limits.named_bindings)
                .map(|i| (format!("p{i}"), Value::Null))
                .collect();
            let terms: Vec<_> = bindings
                .keys()
                .map(|name| format!("${name} is null"))
                .collect();
            let text = format!("items where {}", conjunction(&terms));
            assert_eq!(
                evaluate(entry, &engine, &text, &bindings).unwrap().matched,
                1
            );
        }
    });
    assert_eq!(rows(&admin), before);
}

#[test]
fn supplied_binding_limits_precede_transaction_admission() {
    let (admin, engine) = fixture();
    let before = rows(&admin);
    let mut sessions = Vec::new();
    for _ in 0..giggles_core::relational::MAX_TRANSACTIONS {
        let mut session = admin.sql_session();
        session.execute("begin", &[], None).unwrap();
        sessions.push(session);
    }
    with_sql_parser_forbidden(|| {
        let oversized = Bindings::from([(
            "payload".into(),
            Value::Bytes(vec![0; MAX_BINDING_BYTES + 1]),
        )]);
        assert_refusal(
            &engine,
            "items where $payload is null",
            &oversized,
            Error::Limit,
        );
        assert_refusal(
            &engine,
            "items",
            &Bindings::new(),
            Error::Core("Busy".into()),
        );
    });
    drop(sessions);
    assert_eq!(rows(&admin), before);
}

#[test]
fn under_budget_exact_binding_and_type_errors_are_preserved() {
    let (admin, engine) = fixture();
    let before = rows(&admin);
    with_sql_parser_forbidden(|| {
        assert_refusal(
            &engine,
            "items where id=$id",
            &Bindings::new(),
            Error::Bind("parameter missing".into()),
        );
        let unused = Bindings::from([("unused".into(), Value::Null)]);
        assert_refusal(&engine, "items", &unused, Error::Bind("parameters".into()));
        let wrong_type = Bindings::from([("id".into(), Value::Text("one".into()))]);
        assert_refusal(&engine, "items where id=$id", &wrong_type, Error::Sql { sqlstate: "42883".into() });
        assert_refusal(
            &engine,
            "missing",
            &Bindings::new(),
            // PG18: undefined table is 42P01; native errors carry SQL's SQLSTATE (R2 parity).
            Error::Sql { sqlstate: "42P01".into() },
        );
        let null = Bindings::from([("payload".into(), Value::Null)]);
        for entry in ENTRY_POINTS {
            assert_eq!(
                evaluate(entry, &engine, "items where $payload is null", &null)
                    .unwrap()
                    .matched,
                1
            );
        }
    });
    assert_eq!(rows(&admin), before);
}

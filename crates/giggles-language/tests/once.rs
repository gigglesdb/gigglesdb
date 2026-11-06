use giggles_core::{
    Admin, CrashPoint, Limits,
    operations::{Error as CoreError, once as core_once},
    relational::{DataType, with_sql_parser_forbidden},
};
use giggles_language::{
    once::{OnceClockUnavailable, OnceOutcome, OnceReceipt, OnceSchema, OnceStatus, OnceTime},
    *,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const VERSION: LanguageVersion = LanguageVersion::Embedded011;
const INCREMENT: &str = "update items where id=$id set n=n+1 once $key for 1m return id,n,small";

fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-once-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn fixture() -> (PathBuf, Admin, Engine, OnceScope) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT NOT NULL,small SMALLINT,tag TEXT)",
        &[],
        None,
    )
    .unwrap();
    sql.execute(
        "INSERT INTO items(id,n,tag) VALUES(1,10,'ready')",
        &[],
        None,
    )
    .unwrap();
    drop(sql);
    // Applications capture and persist this scope BEFORE the first attempt.
    let scope = OnceScope {
        database_domain: admin.snapshot().unwrap().domain,
        tenant: 42,
        namespace: "native.jobs".into(),
    };
    let engine = Engine::with_version(admin.clone(), VERSION);
    (path, admin, engine, scope)
}

fn clock() -> std::result::Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: 1000,
        upper_unix_seconds: 1001,
        valid_until: Instant::now() + Duration::from_secs(10),
    })
}

fn no_clock() -> std::result::Result<OnceTime, OnceClockUnavailable> {
    panic!("retained replay must not ask for a new clock certificate")
}

fn args(key: &str) -> Bindings {
    Bindings::from([
        ("id".into(), Value::Int(1)),
        ("key".into(), Value::Text(key.into())),
    ])
}

fn count(admin: &Admin) -> i64 {
    let result = admin
        .sql_session()
        .execute("SELECT n FROM items WHERE id=1", &[], None)
        .unwrap();
    match result.rows[0].values[0] {
        Value::Int(n) => n,
        _ => panic!("integer expected"),
    }
}

fn assert_absent(engine: &Engine, scope: &OnceScope, key: &str) {
    assert_eq!(
        engine
            .outcome_text(
                "outcome $key",
                &Bindings::from([("key".into(), Value::Text(key.into()))]),
                scope,
            )
            .unwrap(),
        OnceStatus::Absent
    );
}

fn assert_same_receipt(original: &OnceReceipt, replay: &OnceReceipt) {
    assert!(replay.replayed);
    assert_eq!(original.accepted_fingerprint, replay.accepted_fingerprint);
    assert_eq!(original.description, replay.description);
    assert_eq!(original.retry_until_seconds, replay.retry_until_seconds);
    assert_eq!(original.outcome, replay.outcome);
}

#[test]
fn once_text_tree_replay_and_schema_metadata_match_core() {
    let (_, admin, engine, scope) = fixture();
    with_sql_parser_forbidden(|| {
        let receipt = engine
            .execute_once_text(INCREMENT, &args("replay"), &scope, &clock)
            .unwrap();
        assert!(!receipt.replayed);
        let OnceSchema::Operation(columns) = &receipt.description.output else {
            panic!("operation schema expected")
        };
        assert_eq!(columns[2].data_type, DataType::SmallInt);
        assert!(!columns[2].not_null);
        let tree = parse_once_with_version(
            "-- retry\n update items where id=1 set n=n+1 once \"replay\" for duration(\"60s\") return id,n,small;",
            VERSION,
        )
        .unwrap();
        let replay = engine
            .execute_once(&tree, &Bindings::new(), &scope, &no_clock)
            .unwrap();
        assert_same_receipt(&receipt, &replay);
        let query = parse_outcome_with_version("outcome $k", VERSION).unwrap();
        let lookup = engine
            .outcome(
                &query,
                &Bindings::from([("k".into(), Value::Bytes(b"replay".to_vec()))]),
                &scope,
            )
            .unwrap();
        let OnceStatus::Retained(found) = lookup else {
            panic!("retained receipt expected")
        };
        assert_same_receipt(&receipt, &found);
        assert_eq!(
            engine.execute_once_text(
                &INCREMENT.replace("n=n+1", "n=n+2"),
                &args("replay"),
                &scope,
                &no_clock,
            ),
            Err(OnceError::Engine(core_once::OnceError::IdempotencyConflict))
        );
    });
    assert_eq!(count(&admin), 11);
}

#[test]
fn once_header_binding_admission_is_global_and_exact() {
    let (_, admin, engine, scope) = fixture();
    let source = "update items where id=1 set tag=$same once $same for 1s return tag";
    with_sql_parser_forbidden(|| {
        let bindings = Bindings::from([("same".into(), Value::Text("shared".into()))]);
        let first = engine
            .execute_once_text(source, &bindings, &scope, &clock)
            .unwrap();
        let replay = engine
            .execute_once_text(source, &bindings, &scope, &no_clock)
            .unwrap();
        assert_same_receipt(&first, &replay);
        for mut bindings in [
            Bindings::from([("key".into(), Value::Text("bad".into()))]),
            Bindings::from([
                ("id".into(), Value::Int(1)),
                ("key".into(), Value::Text("bad".into())),
                ("unused".into(), Value::Null),
            ]),
        ] {
            assert!(
                engine
                    .execute_once_text(INCREMENT, &bindings, &scope, &clock)
                    .is_err()
            );
            bindings.insert(
                "unused".into(),
                Value::Text("x".repeat(MAX_BINDING_BYTES + 1)),
            );
            assert_eq!(
                engine.execute_once_text(INCREMENT, &bindings, &scope, &clock),
                Err(OnceError::Frontend(Error::Limit))
            );
        }
        let mut too_many = args("bad");
        for i in 0..MAX_BINDINGS {
            too_many.insert(format!("unused{i}"), Value::Null);
        }
        assert_eq!(
            engine.execute_once_text(INCREMENT, &too_many, &scope, &clock),
            Err(OnceError::Frontend(Error::Limit))
        );
        assert_absent(&engine, &scope, "bad");
        let excess_lookup = Bindings::from([
            ("key".into(), Value::Text("shared".into())),
            ("unused".into(), Value::Null),
        ]);
        assert!(
            engine
                .outcome_text("outcome $key", &excess_lookup, &scope)
                .is_err()
        );
    });
    assert_eq!(count(&admin), 10);
}

#[test]
fn once_selected_version_and_context_are_required() {
    let (_, admin, engine, scope) = fixture();
    let mut tree = parse_once_with_version(INCREMENT, VERSION).unwrap();
    tree.language_version = LanguageVersion::Embedded010;
    assert_eq!(
        engine.execute_once(&tree, &args("version"), &scope, &clock),
        Err(OnceError::Frontend(Error::Unsupported))
    );
    let old_engine = Engine::with_version(admin.clone(), LanguageVersion::Embedded010);
    assert!(
        old_engine
            .execute_once_text(INCREMENT, &args("version"), &scope, &clock)
            .is_err()
    );
    assert!(engine.execute_text(INCREMENT, &args("version")).is_err());
    assert!(
        engine
            .prepare_bound_text(INCREMENT, &args("version"))
            .is_err()
    );
    let mut invalid_scope = scope.clone();
    invalid_scope.namespace.clear();
    assert!(
        engine
            .execute_once_text(INCREMENT, &args("version"), &invalid_scope, &clock)
            .is_err()
    );
    invalid_scope.namespace = "x".repeat(64);
    assert_eq!(
        engine.execute_once_text(INCREMENT, &args("version"), &invalid_scope, &clock),
        Err(OnceError::Frontend(Error::Limit))
    );
    assert_absent(&engine, &scope, "version");
    assert_eq!(count(&admin), 10);
    assert_eq!(LanguageVersion::Embedded01.as_str(), LANGUAGE_VERSION);
}

#[test]
fn once_atomic_block_and_terminal_receipts_survive_later_changes() {
    let (_, admin, engine, scope) = fixture();
    let text = "transaction once $key for 1m { let changed=(update items where id=1 set n=n+1 return n,small); emit @changed.row.n,@changed.row.small }";
    let bindings = Bindings::from([("key".into(), Value::Text("block".into()))]);
    let first = with_sql_parser_forbidden(|| {
        engine
            .execute_once_text(text, &bindings, &scope, &clock)
            .unwrap()
    });
    let OnceOutcome::Transaction(outcome) = first.outcome.as_ref().unwrap() else {
        panic!("transaction result expected")
    };
    assert_eq!(outcome.emitted.len(), 2);
    admin
        .sql_session()
        .execute("CREATE TABLE later(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let replay = engine
        .execute_once_text(text, &bindings, &scope, &no_clock)
        .unwrap();
    assert_same_receipt(&first, &replay);
    let missing = "update items where id=2 set n=20 once \"missing\" for 1s return small";
    let terminal = engine
        .execute_once_text(missing, &Bindings::new(), &scope, &clock)
        .unwrap();
    assert_eq!(terminal.outcome, Err(CoreError::NotFound));
    admin
        .sql_session()
        .execute("INSERT INTO items(id,n) VALUES(2,5)", &[], None)
        .unwrap();
    let replay = engine
        .execute_once_text(missing, &Bindings::new(), &scope, &no_clock)
        .unwrap();
    assert_same_receipt(&terminal, &replay);
    assert_eq!(count(&admin), 11);
}

#[test]
fn once_header_has_independent_bounds_without_reducing_body_allowance() {
    let (_, _, engine, scope) = fixture();
    let text = "transaction once \"full\" for 1s { emit $payload }";
    let full = Bindings::from([(
        "payload".into(),
        Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES)),
    )]);
    let receipt = engine
        .execute_once_text(text, &full, &scope, &clock)
        .unwrap();
    assert!(receipt.outcome.is_ok());
    let mut over = full;
    over.insert("key".into(), Value::Text("different".into()));
    assert_eq!(
        engine.execute_once_text(
            "transaction once $key for 1s { emit $payload }",
            &over,
            &scope,
            &clock,
        ),
        Err(OnceError::Frontend(Error::Limit))
    );
    let repeated = Bindings::from([(
        "payload".into(),
        Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES / 2 + 1)),
    )]);
    assert_eq!(
        engine.execute_once_text(
            "transaction once \"repeat\" for 1s { emit $payload,$payload }",
            &repeated,
            &scope,
            &clock,
        ),
        Err(OnceError::Engine(core_once::OnceError::Operation(
            CoreError::Limit
        )))
    );
    assert_absent(&engine, &scope, "repeat");
}

#[test]
fn once_key_types_and_retention_are_validated_before_any_effect() {
    let (_, admin, engine, scope) = fixture();
    for value in [
        Value::Null,
        Value::Bool(true),
        Value::Int(1),
        Value::Text(String::new()),
        Value::Bytes(vec![0; 257]),
    ] {
        let mut bindings = args("bad");
        bindings.insert("key".into(), value);
        assert!(
            engine
                .execute_once_text(INCREMENT, &bindings, &scope, &clock)
                .is_err()
        );
    }
    let mut tree = parse_once_with_version(INCREMENT, VERSION).unwrap();
    for micros in [0, -1, 1, 1_000_001, 604_801_000_000, i64::MAX] {
        tree.retention.microseconds = micros;
        assert!(
            engine
                .execute_once(&tree, &args("bad"), &scope, &clock)
                .is_err()
        );
    }
    assert_absent(&engine, &scope, "bad");
    assert_eq!(count(&admin), 10);
}

#[test]
fn once_scope_partitions_keys_without_rebinding_original_identity() {
    let (_, admin, engine, scope) = fixture();
    let mut tenant = scope.clone();
    tenant.tenant += 1;
    let mut namespace = scope.clone();
    namespace.namespace.push_str(".other");
    for selected in [&scope, &tenant, &namespace] {
        let first = engine
            .execute_once_text(INCREMENT, &args("scope"), selected, &clock)
            .unwrap();
        assert!(!first.replayed);
        let replay = engine
            .execute_once_text(INCREMENT, &args("scope"), selected, &no_clock)
            .unwrap();
        assert_same_receipt(&first, &replay);
    }
    assert_eq!(count(&admin), 13);
}

#[test]
fn once_original_scope_rejects_restored_domain() {
    let (_, admin, engine, scope) = fixture();
    engine
        .execute_once_text(INCREMENT, &args("restore"), &scope, &clock)
        .unwrap();
    let archive = path();
    admin.export(&archive).unwrap();
    let restored = Admin::restore(&archive, path(), Limits::default()).unwrap();
    let engine = Engine::with_version(restored.clone(), VERSION);
    let persisted: OnceScope =
        serde_json::from_str(&serde_json::to_string(&scope).unwrap()).unwrap();
    assert_eq!(persisted, scope);
    assert_eq!(
        engine.execute_once_text(INCREMENT, &args("restore"), &persisted, &no_clock),
        Err(OnceError::Engine(core_once::OnceError::DomainMismatch))
    );
    assert_eq!(
        engine.outcome_text("outcome \"restore\"", &Bindings::new(), &persisted),
        Err(OnceError::Engine(core_once::OnceError::DomainMismatch))
    );
    assert_eq!(count(&restored), 11);
}

#[test]
fn once_unknown_is_resolved_with_persisted_scope_after_reopen() {
    for (point, committed) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterDecisionCommit, true),
    ] {
        let (file, admin, engine, scope) = fixture();
        admin.set_crash_point(point).unwrap();
        assert_eq!(
            engine.execute_once_text(INCREMENT, &args("unknown"), &scope, &clock),
            Err(OnceError::Engine(core_once::OnceError::Unknown))
        );
        drop(engine);
        drop(admin);
        let reopened = Admin::open(file, Limits::default()).unwrap();
        let engine = Engine::with_version(reopened.clone(), VERSION);
        let found = engine
            .outcome_text("outcome \"unknown\"", &Bindings::new(), &scope)
            .unwrap();
        assert_eq!(matches!(found, OnceStatus::Retained(_)), committed);
        let receipt = engine
            .execute_once_text(INCREMENT, &args("unknown"), &scope, &clock)
            .unwrap();
        assert_eq!(receipt.replayed, committed);
        assert_eq!(count(&reopened), 11);
    }
}

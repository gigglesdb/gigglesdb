use giggles_core::{
    Admin, Limits,
    relational::{DataType, with_sql_parser_forbidden},
};
use giggles_language::*;
use std::{
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

const V6: LanguageVersion = LanguageVersion::Embedded06;
const EARLIER: [LanguageVersion; 5] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
];
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-insert-many-native-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn database(schema: &str) -> (PathBuf, Admin, Engine) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    (path, admin.clone(), Engine::with_version(admin, V6))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    database("CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,tag TEXT)")
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(path()).unwrap()
}
fn int(n: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(n)))
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn literal(value: Value) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
}
fn many(rows: Vec<Vec<Expr>>, cap: i64) -> Statement {
    Statement::InsertMany {
        table: "items".into(),
        columns: vec!["id".into()],
        rows,
        cap: ValueExpr::Literal(Value::Int(cap)),
    }
}
fn returning(statement: Statement, projection: Vec<Projection>) -> Statement {
    Statement::Returning {
        statement: Box::new(statement),
        projection,
    }
}
fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(args)
        .output()
        .unwrap()
}
fn json(out: &Output) -> serde_json::Value {
    assert!(out.status.success(), "{out:?}");
    assert!(out.stderr.is_empty());
    serde_json::from_slice(&out.stdout).unwrap()
}
fn error(out: &Output) -> Error {
    assert!(!out.status.success(), "{out:?}");
    assert!(out.stdout.is_empty());
    let v: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    serde_json::from_value(v["error"].clone()).unwrap()
}

#[test]
fn insert_many_version_gates_and_empty_tree_contract() {
    let (database, admin, engine) = fixture();
    let source = "insert many into items(id) values(1),(2) at most 2 return *";
    let tree = returning(many(vec![vec![int(1)], vec![int(2)]], 2), vec![]);
    assert_eq!(parse_with_version(source, V6).unwrap(), tree);
    assert_eq!(
        parse_input_with_version(source, V6).unwrap(),
        ParsedInput::Statement(tree.clone())
    );
    let decoded: Statement = serde_json::from_value(serde_json::to_value(&tree).unwrap()).unwrap();
    assert_eq!(decoded, tree);
    let bound = engine.prepare_bound(&tree, &Bindings::new()).unwrap();
    assert_eq!(bound.language_version(), "0.6");
    assert_eq!(bound.profile(), "embedded-trusted-native-0.6");
    let block = Transaction {
        read_only: false,
        steps: vec![TransactionStep::Execute(tree.clone())],
        emit: None,
    };
    let bound_block = engine
        .prepare_transaction_bound(&block, &Bindings::new())
        .unwrap();
    assert_eq!(bound_block.language_version(), "0.6");
    let before = digest(&admin);
    for version in EARLIER {
        let older = Engine::with_version(admin.clone(), version);
        for tree in [&decoded, &many(vec![], 0)] {
            assert_eq!(
                older.execute(tree, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                older.prepare_bound(tree, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
        }
        assert!(parse_with_version(source, version).is_err());
        assert!(older.execute_text(source, &Bindings::new()).is_err());
        assert!(older.prepare_bound_text(source, &Bindings::new()).is_err());
        assert_eq!(older.execute_bound(&bound), Err(Error::Unsupported));
        assert_eq!(
            older.execute_transaction(&block, &Bindings::new()),
            Err(Error::Unsupported)
        );
        assert_eq!(
            older
                .prepare_transaction_bound(&block, &Bindings::new())
                .unwrap_err(),
            Error::Unsupported
        );
        assert_eq!(
            older.execute_transaction_bound(&bound_block),
            Err(Error::Unsupported)
        );
    }
    let older = Engine::with_version(admin.clone(), LanguageVersion::Embedded05);
    let old_statement = older
        .prepare_bound_text("items where id=1", &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_bound(&old_statement),
        Err(Error::Unsupported)
    );
    let old_block = older
        .prepare_transaction_bound_text("transaction read only { require true }", &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&old_block),
        Err(Error::Unsupported)
    );
    assert_eq!(digest(&admin), before);
    for source in [
        "insert into items(id,n) values(1,2)",
        "insert items set id=1,n=-9223372036854775808",
        "update many items where true when true set n=1 at most 2 return id",
    ] {
        assert_eq!(
            parse_with_version(source, V6).unwrap(),
            parse_with_version(source, LanguageVersion::Embedded05).unwrap()
        );
    }
    for source in [
        "insert many items(id) values(1) at most 1",
        "insert many into items(id) values(1)",
        "insert many into items(id) values at most 0",
        "insert many into items(id) values() at most 0",
        "insert many into items(id,n) values(1),(2,3) at most 2",
        "insert into items(id) values(1),(2)",
    ] {
        assert!(parse_with_version(source, V6).is_err(), "{source}");
    }
    let empty = returning(
        Statement::InsertMany {
            table: "items".into(),
            columns: vec!["n".into()],
            rows: vec![],
            cap: ValueExpr::Literal(Value::Int(0)),
        },
        vec![Projection {
            expression: binary(int(1), Arithmetic::Divide, int(0)),
            alias: Some("quotient".into()),
        }],
    );
    let bound = engine.prepare_bound(&empty, &Bindings::new()).unwrap();
    assert_eq!(bound.description().columns[0].data_type, DataType::BigInt);
    let result = with_sql_parser_forbidden(|| engine.execute_bound(&bound)).unwrap();
    assert_eq!((result.matched, result.affected, result.changed), (0, 0, 0));
    assert_eq!(result.columns, ["quotient"]);
    assert!(result.complete && result.rows.is_empty());
    let maximum = many(
        (0..MAX_INSERT_ROWS).map(|i| vec![int(i as i64)]).collect(),
        MAX_INSERT_ROWS as i64,
    );
    let bound = engine.prepare_bound(&maximum, &Bindings::new()).unwrap();
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&bound))
            .unwrap()
            .changed,
        1024
    );
    let mut over = maximum;
    if let Statement::InsertMany { rows, .. } = &mut over {
        rows.push(vec![int(2000)]);
    }
    assert_eq!(
        engine.prepare_bound(&over, &Bindings::new()).err(),
        Some(Error::Limit)
    );
    let text_maximum = format!(
        "insert many into items(id) values {} at most 1024",
        (0..1024)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",")
    );
    assert_eq!(parse_with_version(&text_maximum, V6), Err(Error::Limit));
    assert_eq!(serde_json::to_value(V6).unwrap(), "0.6");
    assert_eq!(
        serde_json::from_value::<Error>(serde_json::to_value(Error::DuplicateTarget).unwrap())
            .unwrap(),
        Error::DuplicateTarget
    );
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    assert_eq!(profile_for(V6).name, "embedded-trusted-native-0.6");
    let before = digest(&admin);
    drop(engine);
    drop(older);
    drop(admin);
    for version in EARLIER {
        for text in [
            source,
            "transaction { insert many into items(id) values(1),(2) at most 2 return * }",
        ] {
            error(&invoke(&[
                "run",
                "--language-version",
                version.as_str(),
                database.to_str().unwrap(),
                text,
            ]));
        }
    }
    error(&invoke(&["run", database.to_str().unwrap(), source]));
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

#[test]
fn insert_many_text_tree_and_bound_execution_match_without_sql() {
    let source = "insert many into items(id,n,tag) values($a,1+2,\"first\"),($b,8/2,\"second\") at most $cap return n as value,id,tag";
    let tree = returning(
        Statement::InsertMany {
            table: "items".into(),
            columns: vec!["id".into(), "n".into(), "tag".into()],
            rows: vec![
                vec![
                    parameter("a"),
                    binary(int(1), Arithmetic::Add, int(2)),
                    literal(Value::Text("first".into())),
                ],
                vec![
                    parameter("b"),
                    binary(int(8), Arithmetic::Divide, int(2)),
                    literal(Value::Text("second".into())),
                ],
            ],
            cap: ValueExpr::Parameter("cap".into()),
        },
        vec![
            Projection {
                expression: field("n"),
                alias: Some("value".into()),
            },
            Projection {
                expression: field("id"),
                alias: None,
            },
            Projection {
                expression: field("tag"),
                alias: None,
            },
        ],
    );
    assert_eq!(parse_with_version(source, V6).unwrap(), tree);
    let mut baseline = None;
    for mode in 0..4 {
        let (_, admin, engine) = fixture();
        let mut bindings = Bindings::from([
            ("a".into(), Value::Int(2)),
            ("b".into(), Value::Int(1)),
            ("cap".into(), Value::Int(2)),
        ]);
        let result = with_sql_parser_forbidden(|| match mode {
            0 => engine.execute_text(source, &bindings),
            1 => engine.execute(&tree, &bindings),
            _ => {
                let prepared = if mode == 2 {
                    engine.prepare_bound_text(source, &bindings)
                } else {
                    engine.prepare_bound(&tree, &bindings)
                }
                .unwrap();
                assert_eq!(
                    prepared.description().columns[0].id,
                    prepared.description().table.columns[1].id
                );
                bindings.insert("a".into(), Value::Int(99));
                bindings.insert("cap".into(), Value::Int(0));
                engine.execute_bound(&prepared)
            }
        })
        .unwrap();
        assert_eq!((result.matched, result.affected, result.changed), (2, 2, 2));
        assert_eq!(result.columns, ["value", "id", "tag"]);
        assert!(result.complete);
        assert_eq!(
            result.rows[0].values,
            [Value::Int(4), Value::Int(1), Value::Text("second".into())]
        );
        assert_eq!(
            result.rows[1].values,
            [Value::Int(3), Value::Int(2), Value::Text("first".into())]
        );
        let stored = rows(&admin);
        for (returned, stored) in result.rows.iter().zip(stored) {
            assert_eq!(returned.version, stored.version);
        }
        if let Some(baseline) = &baseline {
            assert_eq!(&result, baseline);
        } else {
            baseline = Some(result);
        }
    }
    let (_, admin, engine) = fixture();
    let bindings = Bindings::from([
        ("a".into(), Value::Int(2)),
        ("b".into(), Value::Int(1)),
        ("cap".into(), Value::Int(2)),
    ]);
    let before = digest(&admin);
    let bound = engine.prepare_bound_text(source, &bindings).unwrap();
    assert_eq!(digest(&admin), before);
    admin
        .sql_session()
        .execute("INSERT INTO items(id) VALUES(1)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&bound)),
        Err(Error::Core("Sql(Duplicate)".into()))
    );
    assert_eq!(digest(&admin), before);
    admin
        .sql_session()
        .execute("DELETE FROM items WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&bound))
            .unwrap()
            .changed,
        2
    );
    admin
        .sql_session()
        .execute("CREATE TABLE newer (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&bound),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );

    let (_, admin, engine) =
        database("CREATE TABLE items (tenant BIGINT,id BIGINT,n BIGINT,PRIMARY KEY(tenant,id))");
    let source = format!(
        "insert many into items(tenant,id,n) values {} at most 110 return *",
        (0..110)
            .rev()
            .map(|i| format!("({},{},{i})", i / 11, i % 11))
            .collect::<Vec<_>>()
            .join(",")
    );
    let result =
        with_sql_parser_forbidden(|| engine.execute_text(&source, &Bindings::new())).unwrap();
    assert_eq!(result.rows.len(), 110);
    assert!(result.complete);
    assert_eq!(result.rows, rows(&admin));
    for (i, row) in result.rows.iter().enumerate() {
        assert_eq!(
            row.values,
            [
                Value::Int(i as i64 / 11),
                Value::Int(i as i64 % 11),
                Value::Int(i as i64)
            ]
        );
    }
}

#[test]
fn insert_many_caps_duplicates_and_returning_are_atomic() {
    let (_, admin, engine) = fixture();
    for (source, expected) in [
        (
            "insert many into items(id,n) values(1,1/0) at most 0",
            Error::TooManyMatches,
        ),
        (
            "insert many into items(id,n) values(1,2),(1,3) at most 2",
            Error::DuplicateTarget,
        ),
        (
            "insert many into items(id) values(1+1),(2) at most 2",
            Error::DuplicateTarget,
        ),
        (
            "insert many into items(id) values(1),(2) at most 2 return 1/(2-id) as quotient",
            Error::DivisionByZero,
        ),
    ] {
        let before = digest(&admin);
        let tree = parse_with_version(source, V6).unwrap();
        with_sql_parser_forbidden(|| {
            let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
            assert_eq!(engine.execute_bound(&bound), Err(expected.clone()));
            assert_eq!(
                engine.execute(&tree, &Bindings::new()),
                Err(expected.clone())
            );
            assert_eq!(engine.execute_text(source, &Bindings::new()), Err(expected));
        });
        assert_eq!(digest(&admin), before);
    }
    for source in [
        "insert many into items(id,n) values(1,1/0),(2,true) at most 0",
        "insert many into items(id,n) values(1,1/0),(2,$missing) at most 0",
        "insert many into items(id,n) values(1,n) at most 0",
        "insert many into items(id) values(1) at most 0 return absent",
    ] {
        let before = digest(&admin);
        assert!(
            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
        let error = engine.execute_text(source, &Bindings::new()).unwrap_err();
        assert_ne!(error, Error::TooManyMatches);
        assert_ne!(error, Error::DivisionByZero);
        assert_eq!(digest(&admin), before);
    }
    for tail in [
        "emit @batch.row",
        "emit @batch.rows",
        "emit @batch.count",
        "emit @batch.row.id",
    ] {
        let source = format!(
            "transaction {{ let batch=(insert many into items(id) values(1) at most 1 return *); {tail} }}"
        );
        let before = digest(&admin);
        assert!(
            engine
                .prepare_transaction_bound_text(&source, &Bindings::new())
                .is_err()
        );
        assert!(
            engine
                .execute_transaction_text(&source, &Bindings::new())
                .is_err()
        );
        assert_eq!(digest(&admin), before);
    }
    for tail in ["require false", "require null"] {
        let source = format!(
            "transaction {{ let batch=(insert many into items(id) values(1),(2) at most 2 return *); {tail}; emit @batch.status,@batch.changed }}"
        );
        let before = digest(&admin);
        let bound = engine
            .prepare_transaction_bound_text(&source, &Bindings::new())
            .unwrap();
        assert_eq!(
            engine.execute_transaction_bound(&bound),
            Err(Error::RequirementFailed)
        );
        assert_eq!(
            engine.execute_transaction_text(&source, &Bindings::new()),
            Err(Error::RequirementFailed)
        );
        assert_eq!(digest(&admin), before);
    }
    let source = "transaction { let batch=(insert many into items(id) values(1),(2) at most 2 return *); require @batch.changed=2; emit @batch.status,@batch.changed }";
    let result =
        with_sql_parser_forbidden(|| engine.execute_transaction_text(source, &Bindings::new()))
            .unwrap();
    assert_eq!(
        result.emitted,
        [
            EmittedValue::Scalar(Value::Text("success".into())),
            EmittedValue::Scalar(Value::Int(2))
        ]
    );
    assert_eq!(rows(&admin).len(), 2);

    let (_, admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,src TEXT,tail TEXT)");
    let projection = (0..63)
        .map(|i| format!("src as p{i}"))
        .chain(["tail as last".into()])
        .collect::<Vec<_>>()
        .join(",");
    let insert = format!(
        "insert many into items(id,src,tail) values(1,$src,$tail) at most 1 return {projection}"
    );
    let bindings = Bindings::from([
        ("src".into(), Value::Text("x".repeat(16384))),
        ("tail".into(), Value::Text("y".repeat(16384))),
    ]);
    let retained = format!(
        "transaction {{ let a=({insert}); let b=(insert many into items(id) values(2) at most 1 return \"z\" as z) }}"
    );
    let before = digest(&admin);
    let bound = engine
        .prepare_transaction_bound_text(&retained, &bindings)
        .unwrap();
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_transaction_bound(&bound)),
        Err(Error::Limit)
    );
    assert_eq!(
        engine.execute_transaction_text(&retained, &bindings),
        Err(Error::Limit)
    );
    assert_eq!(digest(&admin), before);
    let mut over = bindings.clone();
    over.insert("tail".into(), Value::Text("y".repeat(16385)));
    let bound = engine.prepare_bound_text(&insert, &over).unwrap();
    assert_eq!(engine.execute_bound(&bound), Err(Error::Limit));
    assert_eq!(digest(&admin), before);
    let result = with_sql_parser_forbidden(|| engine.execute_text(&insert, &bindings)).unwrap();
    assert_eq!(
        result.rows[0]
            .values
            .iter()
            .map(|v| if let Value::Text(s) = v {
                s.len()
            } else {
                panic!("text")
            })
            .sum::<usize>(),
        MAX_EMIT_BYTES
    );
    assert_eq!(result.rows[0].version, rows(&admin)[0].version);
}

#[test]
fn insert_many_cli_and_selected_manifest_match_version_six() {
    let (database, admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,tag TEXT,state TEXT)");
    let manifest = capability_manifest_for(V6);
    let metadata = serde_json::to_value(&manifest).unwrap();
    assert_eq!(metadata["language_version"], "0.6");
    assert_eq!(metadata["limits"]["insert_input_rows"], MAX_INSERT_ROWS);
    assert_eq!(
        metadata["limits"]["insert_proposed_bytes"],
        MAX_INSERT_BYTES
    );
    assert_eq!(metadata["limits"]["text_tokens"], MAX_TOKENS);
    for version in EARLIER {
        let metadata = serde_json::to_value(capability_manifest_for(version)).unwrap();
        assert!(metadata["limits"].get("insert_input_rows").is_none());
        assert!(metadata["limits"].get("insert_proposed_bytes").is_none());
    }
    let before = digest(&admin);
    let mut rejected = 0;
    let mut blocks = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            rejected += 1;
            with_sql_parser_forbidden(|| match parse_input_with_version(source, V6) {
                Ok(ParsedInput::Statement(tree)) => {
                    assert!(
                        engine.execute_text(source, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    // Runtime DuplicateTarget is deliberately not a prepare error.
                    if capability.feature == "duplicate_insert_many_targets" {
                        let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
                        assert_eq!(engine.execute_bound(&bound), Err(Error::DuplicateTarget));
                    } else {
                        assert!(
                            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
                            "{}: {source}",
                            capability.feature
                        );
                    }
                    assert!(
                        engine.execute(&tree, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                }
                Ok(ParsedInput::Transaction(tree)) => {
                    blocks += 1;
                    assert!(
                        engine
                            .execute_transaction_text(source, &Bindings::new())
                            .is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine
                            .prepare_transaction_bound_text(source, &Bindings::new())
                            .is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine.execute_transaction(&tree, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                }
                Ok(ParsedInput::Catalog(_)) => {
                    panic!("profile 0.6 accepted catalog syntax: {source}");
                }
                Ok(
                    ParsedInput::Once(_)
                    | ParsedInput::Outcome(_)
                    | ParsedInput::Inspection(_)
                    | ParsedInput::Explain(_),
                ) => {
                    panic!("earlier profile admitted a retry envelope: {source}")
                }
                Err(_) => {}
            });
        }
    }
    assert!(rejected > 0 && blocks > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let db = database.to_str().unwrap();
    let result: Outcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.6",
        db,
        "insert many into items(id,n) values($id,2+3),(1,7) at most 2 return id,n,tag",
        r#"{"id":{"Int":2}}"#,
    ])))
    .unwrap();
    assert_eq!(result.changed, 2);
    assert_eq!(result.rows.len(), 2);
    assert!(result.complete);
    assert_eq!(
        result.rows[0].values,
        [Value::Int(1), Value::Int(7), Value::Null]
    );
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.6",
            db,
            "insert many into items(id) values(3),(3) at most 2 return *"
        ])),
        Error::DuplicateTarget
    );
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.6",
            db,
            "insert many into items(id,n) values(3,1/0) at most 0"
        ])),
        Error::TooManyMatches
    );
    let result:TransactionOutcome=serde_json::from_value(json(&invoke(&["run","--language-version","0.6",db,"transaction { let batch=(insert many into items(id) values(3),(4) at most 2); emit @batch.status,@batch.changed }"]))).unwrap();
    assert_eq!(
        result.emitted,
        [
            EmittedValue::Scalar(Value::Text("success".into())),
            EmittedValue::Scalar(Value::Int(2))
        ]
    );
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.6"]))["name"],
        "embedded-trusted-native-0.6"
    );
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.6"
        ])),
        metadata
    );
    let admin = Admin::open(&database, Limits::default()).unwrap();
    let before = digest(&admin);
    drop(admin);
    let mut count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::Cli)
    {
        for source in &capability.rejection_examples {
            count += 1;
            error(&invoke(&["run", "--language-version", "0.6", db, source]));
        }
    }
    assert!(count > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
    drop(admin);
    let absent = database.with_extension("absent");
    assert!(!absent.exists());
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "9.9",
            absent.to_str().unwrap(),
            "items"
        ])),
        Error::Unsupported
    );
    assert!(!absent.exists());
}

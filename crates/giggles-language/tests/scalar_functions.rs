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

const V8: LanguageVersion = LanguageVersion::Embedded08;
const EARLIER: [LanguageVersion; 7] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
    LanguageVersion::Embedded06,
    LanguageVersion::Embedded07,
];
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-scalar-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,txt TEXT,raw BYTES,state TEXT NOT NULL)",&[],None).unwrap();
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,-5,$1,$2,'ready')",
            &[Value::Text("\u{e9}".into()), Value::Bytes(vec![0, 255])],
            None,
        )
        .unwrap();
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(2,NULL,NULL,NULL,'ready')",
            &[],
            None,
        )
        .unwrap();
    (path, admin.clone(), Engine::with_version(admin, V8))
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(path()).unwrap()
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn value(value: Value) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn int(n: i64) -> Expr {
    value(Value::Int(n))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn call(function: ScalarFunction, arguments: Vec<Expr>) -> Expr {
    Expr::Call {
        function,
        arguments,
    }
}
fn abs(e: Expr) -> Expr {
    call(ScalarFunction::Abs, vec![e])
}
fn length(e: Expr) -> Expr {
    call(ScalarFunction::Length, vec![e])
}
fn coalesce(args: Vec<Expr>) -> Expr {
    call(ScalarFunction::Coalesce, args)
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn read(e: Expr) -> Statement {
    Statement::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![projection(e, Some("out"))],
    }
}
fn update(e: Expr) -> Statement {
    Statement::UpdateExpressions {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        values: vec![("n".into(), e)],
        intent: Intent::One,
    }
}
fn returning(statement: Statement, e: Expr) -> Statement {
    Statement::Returning {
        statement: Box::new(statement),
        projection: vec![projection(e, Some("out"))],
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
fn scalar_function_version_gates_cover_every_expression_root() {
    let (database, admin, engine) = fixture();
    let predicate = Statement::Read {
        table: "items".into(),
        filter: Some(equal(abs(field("n")), int(5))),
        order: vec![],
        limit: None,
        projection: vec![],
    };
    let statement_roots = vec![
        predicate.clone(),
        read(length(field("txt"))),
        update(abs(field("n"))),
        Statement::InsertExpressions {
            table: "items".into(),
            values: vec![
                ("id".into(), int(3)),
                ("n".into(), abs(int(-1))),
                ("state".into(), value(Value::Text("ready".into()))),
            ],
        },
        Statement::InsertMany {
            table: "items".into(),
            columns: vec!["id".into(), "n".into(), "state".into()],
            rows: vec![vec![
                int(3),
                abs(int(-1)),
                value(Value::Text("ready".into())),
            ]],
            cap: ValueExpr::Literal(Value::Int(1)),
        },
        returning(update(int(1)), coalesce(vec![field("n"), int(0)])),
        Statement::Delete {
            table: "items".into(),
            filter: equal(field("id"), int(1)),
            guard: Some(equal(abs(field("n")), int(5))),
            cap: None,
        },
    ];
    let calls_in_blocks = vec![
        Transaction {
            read_only: true,
            steps: vec![TransactionStep::Require(coalesce(vec![
                value(Value::Bool(true)),
                value(Value::Bool(false)),
            ]))],
            emit: None,
        },
        Transaction {
            read_only: true,
            steps: vec![TransactionStep::Require(Expr::And(
                Box::new(value(Value::Bool(true))),
                Box::new(Expr::IsNull(Box::new(length(value(Value::Null))), false)),
            ))],
            emit: None,
        },
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![EmitExpr::Scalar(abs(int(-1)))]),
        },
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![EmitExpr::Scalar(Expr::Arithmetic(
                Box::new(int(1)),
                Arithmetic::Add,
                Box::new(abs(int(-1))),
            ))]),
        },
        Transaction {
            read_only: true,
            steps: vec![TransactionStep::Execute(predicate.clone())],
            emit: None,
        },
    ];
    let source = "items where abs(n)=5";
    assert_eq!(parse_with_version(source, V8).unwrap(), predicate);
    let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
    assert_eq!(bound.language_version(), "0.8");
    let bound_block = engine
        .prepare_transaction_bound(&calls_in_blocks[0], &Bindings::new())
        .unwrap();
    let before = digest(&admin);
    for version in EARLIER {
        let older = Engine::with_version(admin.clone(), version);
        for statement in &statement_roots {
            let tree: Statement =
                serde_json::from_value(serde_json::to_value(statement).unwrap()).unwrap();
            assert_eq!(
                older.execute(&tree, &Bindings::new()),
                Err(Error::Unsupported),
                "{version:?}: {tree:?}"
            );
            assert_eq!(
                older.prepare_bound(&tree, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
        }
        for transaction in &calls_in_blocks {
            let tree: Transaction =
                serde_json::from_value(serde_json::to_value(transaction).unwrap()).unwrap();
            assert_eq!(
                older.execute_transaction(&tree, &Bindings::new()),
                Err(Error::Unsupported),
                "{version:?}: {tree:?}"
            );
            assert_eq!(
                older
                    .prepare_transaction_bound(&tree, &Bindings::new())
                    .err(),
                Some(Error::Unsupported)
            );
        }
        assert!(parse_with_version(source, version).is_err());
        assert!(older.execute_text(source, &Bindings::new()).is_err());
        assert!(older.prepare_bound_text(source, &Bindings::new()).is_err());
        assert_eq!(older.execute_bound(&bound), Err(Error::Unsupported));
        assert_eq!(
            older.execute_transaction_bound(&bound_block),
            Err(Error::Unsupported)
        );
    }
    assert_eq!(digest(&admin), before);
    let old = Engine::with_version(admin.clone(), LanguageVersion::Embedded07);
    let old_bound = old
        .prepare_bound_text("items where id=1", &Bindings::new())
        .unwrap();
    assert_eq!(engine.execute_bound(&old_bound), Err(Error::Unsupported));
    let old_block = old
        .prepare_transaction_bound_text("transaction read only { require true }", &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&old_block),
        Err(Error::Unsupported)
    );
    for name in ["abs", "length", "coalesce"] {
        admin
            .sql_session()
            .execute(
                &format!("CREATE TABLE {name}(id BIGINT PRIMARY KEY,{name} BIGINT)"),
                &[],
                None,
            )
            .unwrap();
        admin
            .sql_session()
            .execute(&format!("INSERT INTO {name} VALUES(1,-7)"), &[], None)
            .unwrap();
        let text = format!("{name} where id=1 select {name}");
        for version in EARLIER.into_iter().chain([V8]) {
            assert_eq!(
                parse_with_version(&text, version).unwrap(),
                parse(&text).unwrap()
            );
            assert_eq!(
                Engine::with_version(admin.clone(), version)
                    .execute_text(&text, &Bindings::new())
                    .unwrap()
                    .rows[0]
                    .values,
                [Value::Int(-7)]
            );
        }
    }
    assert_eq!(
        engine
            .execute_text("abs select ABS(abs) as positive", &Bindings::new())
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(7)]
    );
    assert_eq!(serde_json::to_value(V8).unwrap(), "0.8");
    assert_eq!(profile_for(V8).name, "embedded-trusted-native-0.8");
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    let before = digest(&admin);
    drop(engine);
    drop(old);
    drop(admin);
    for version in EARLIER {
        for source in [
            source,
            "transaction { update items where id=1 set n=99; emit abs(1) }",
        ] {
            error(&invoke(&[
                "run",
                "--language-version",
                version.as_str(),
                database.to_str().unwrap(),
                source,
            ]));
        }
    }
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

#[test]
fn scalar_function_text_tree_and_bound_execution_match_without_sql() {
    let source = "update many items where true set n=abs(coalesce(n,$fallback)) at most 2 return id,n,length(coalesce(txt,\"\")) as chars,length(raw) as bytes";
    let tree = Statement::Returning {
        statement: Box::new(Statement::UpdateExpressions {
            table: "items".into(),
            filter: value(Value::Bool(true)),
            guard: None,
            values: vec![(
                "n".into(),
                abs(coalesce(vec![field("n"), parameter("fallback")])),
            )],
            intent: Intent::Many {
                cap: ValueExpr::Literal(Value::Int(2)),
            },
        }),
        projection: vec![
            projection(field("id"), None),
            projection(field("n"), None),
            projection(
                length(coalesce(vec![field("txt"), value(Value::Text("".into()))])),
                Some("chars"),
            ),
            projection(length(field("raw")), Some("bytes")),
        ],
    };
    assert_eq!(parse_with_version(source, V8).unwrap(), tree);
    let mut baseline = None;
    for mode in 0..4 {
        let (_, admin, engine) = fixture();
        let mut bindings = Bindings::from([("fallback".into(), Value::Int(-7))]);
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
                for column in &prepared.description().columns[2..] {
                    assert_eq!(column.id, 0);
                    assert_eq!(column.data_type, DataType::BigInt);
                    assert!(!column.not_null);
                }
                bindings.insert("fallback".into(), Value::Int(100));
                engine.execute_bound(&prepared)
            }
        })
        .unwrap();
        assert_eq!(
            result.rows[0].values,
            [Value::Int(1), Value::Int(5), Value::Int(1), Value::Int(2)]
        );
        assert_eq!(
            result.rows[1].values,
            [Value::Int(2), Value::Int(7), Value::Int(0), Value::Null]
        );
        assert_eq!(result.changed, 2);
        assert!(result.complete);
        for (returned, stored) in result.rows.iter().zip(rows(&admin)) {
            assert_eq!(returned.version, stored.version);
        }
        if let Some(baseline) = &baseline {
            assert_eq!(&result, baseline);
        } else {
            baseline = Some(result);
        }
    }
    let (_, admin, engine) = fixture();
    let bound = engine
        .prepare_bound_text(
            "items where id=1 select abs(n) as magnitude",
            &Bindings::new(),
        )
        .unwrap();
    admin
        .sql_session()
        .execute("UPDATE items SET n=-10 WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&bound))
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(10)]
    );
    let inserted=with_sql_parser_forbidden(||engine.execute_text("insert many into items(id,n,state) values(3,abs(-3),\"ready\"),(4,length(\"e\\u0301\"),\"ready\") at most 2 return id,n",&Bindings::new())).unwrap();
    assert_eq!(
        inserted
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        [
            vec![Value::Int(3), Value::Int(3)],
            vec![Value::Int(4), Value::Int(2)]
        ]
    );
    let source = "transaction { let saved=(update items where id=1 set n=abs(n) return n); require coalesce(null,@saved.changed=1); emit abs(@saved.row.n),length(@saved.status),coalesce(null,null) }";
    let bound = with_sql_parser_forbidden(|| {
        engine.prepare_transaction_bound_text(source, &Bindings::new())
    })
    .unwrap();
    assert_eq!(
        bound.description().emitted[0],
        EmitDescription::Scalar {
            data_type: Some(DataType::BigInt),
            nullable: true
        }
    );
    assert_eq!(
        bound.description().emitted[2],
        EmitDescription::Scalar {
            data_type: None,
            nullable: true
        }
    );
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_transaction_bound(&bound))
            .unwrap()
            .emitted,
        [
            EmittedValue::Scalar(Value::Int(10)),
            EmittedValue::Scalar(Value::Int(7)),
            EmittedValue::Scalar(Value::Null)
        ]
    );
}

#[test]
fn scalar_function_nulls_laziness_and_shared_limits_are_explicit() {
    let (_, admin, engine) = fixture();
    for (expression, expected) in [
        ("coalesce(7,1/0)", Value::Int(7)),
        ("coalesce(null,7,abs(-9223372036854775808))", Value::Int(7)),
        ("abs(null)", Value::Null),
        ("length(null)", Value::Null),
        ("coalesce(abs(null),length(null))", Value::Null),
        ("length(\"\\u00e9\")", Value::Int(1)),
        ("length(\"e\\u0301\")", Value::Int(2)),
        (
            "length(\"\\ud83d\\udc69\\u200d\\ud83d\\udcbb\")",
            Value::Int(3),
        ),
        ("coalesce(false,true)", Value::Bool(false)),
    ] {
        let source = format!("items where id=1 select {expression} as out");
        assert_eq!(
            with_sql_parser_forbidden(|| engine.execute_text(&source, &Bindings::new()))
                .unwrap()
                .rows[0]
                .values,
            [expected]
        );
    }
    let typed_null = engine
        .prepare_bound_text(
            "items select coalesce(abs(null),length(null)) as out",
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(
        typed_null.description().columns[0].data_type,
        DataType::BigInt
    );
    assert_eq!(typed_null.description().columns[0].id, 0);
    assert!(!typed_null.description().columns[0].not_null);
    for (expression, expected) in [
        ("coalesce(null,1/0,7)", Error::DivisionByZero),
        ("abs(-9223372036854775808)", Error::Overflow),
        ("false and abs(-9223372036854775808)=1", Error::Overflow),
    ] {
        let source = format!("items select {expression} as out");
        let before = digest(&admin);
        let bound = engine
            .prepare_bound_text(&source, &Bindings::new())
            .unwrap();
        assert_eq!(engine.execute_bound(&bound), Err(expected.clone()));
        assert_eq!(
            engine.execute_text(&source, &Bindings::new()),
            Err(expected)
        );
        assert_eq!(digest(&admin), before);
    }
    for expression in [
        "coalesce()",
        "coalesce(1)",
        "abs()",
        "abs(1,2)",
        "length()",
        "length(1)",
        "length(true)",
        "coalesce(1,\"x\")",
        "coalesce(null,null)",
    ] {
        let source = format!("items where false select {expression} as out");
        let before = digest(&admin);
        assert_eq!(
            engine.prepare_bound_text(&source, &Bindings::new()).err(),
            Some(Error::Type),
            "{source}"
        );
        assert_eq!(
            engine.execute_text(&source, &Bindings::new()),
            Err(Error::Type)
        );
        assert_eq!(digest(&admin), before);
    }
    for expression in ["coalesce(1,absent)", "coalesce(1,$missing)"] {
        let source = format!("items where false select {expression} as out");
        assert!(
            engine
                .prepare_bound_text(&source, &Bindings::new())
                .is_err()
        );
        assert!(engine.execute_text(&source, &Bindings::new()).is_err());
    }
    let exact = format!(
        "items select coalesce({}) as out",
        vec!["$p"; MAX_FUNCTION_ARGUMENTS].join(",")
    );
    let bindings = Bindings::from([("p".into(), Value::Text("x".repeat(1024)))]);
    let prepared = engine.prepare_bound_text(&exact, &bindings).unwrap();
    assert_eq!(
        engine.execute_bound(&prepared).unwrap().rows[0].values,
        [Value::Text("x".repeat(1024))]
    );
    let over = Bindings::from([("p".into(), Value::Text("x".repeat(1025)))]);
    assert_eq!(
        engine.prepare_bound_text(&exact, &over).err(),
        Some(Error::Limit)
    );
    let over = format!(
        "items select coalesce({}) as out",
        vec!["1"; MAX_FUNCTION_ARGUMENTS + 1].join(",")
    );
    assert_eq!(parse_with_version(&over, V8), Err(Error::Limit));
    let mut deep = int(1);
    for _ in 0..MAX_DEPTH {
        deep = abs(deep);
    }
    assert_eq!(
        engine
            .execute(&read(deep.clone()), &Bindings::new())
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(1)]
    );
    assert_eq!(
        engine
            .prepare_bound(&read(abs(deep)), &Bindings::new())
            .err(),
        Some(Error::Limit)
    );
    admin
        .sql_session()
        .execute(
            "UPDATE items SET n=$1 WHERE id=2",
            &[Value::Int(i64::MIN)],
            None,
        )
        .unwrap();
    for limit in [0, 1] {
        let source = format!("items limit {limit} select abs(n) as magnitude");
        let before = digest(&admin);
        assert_eq!(
            engine.execute_text(&source, &Bindings::new()),
            Err(Error::Overflow)
        );
        assert_eq!(digest(&admin), before);
    }
    let before = digest(&admin);
    assert_eq!(
        engine.execute_text(
            "update many items where true set n=abs(n) at most 2 return n",
            &Bindings::new()
        ),
        Err(Error::Overflow)
    );
    assert_eq!(digest(&admin), before);
    admin
        .sql_session()
        .execute(
            "UPDATE items SET txt=$1 WHERE id=1",
            &[Value::Text("x".repeat(32758))],
            None,
        )
        .unwrap();
    let make = |tag: &str| {
        format!(
            "transaction {{ let saved=(update items where id=1 set n=n+1 return txt); require coalesce(@saved.row.txt,@saved.row.txt) is not null; emit \"{tag}\" }}"
        )
    };
    with_sql_parser_forbidden(|| engine.execute_transaction_text(&make("zz"), &Bindings::new()))
        .unwrap();
    let source = make("zzz");
    let before = digest(&admin);
    let bound = engine
        .prepare_transaction_bound_text(&source, &Bindings::new())
        .unwrap();
    assert_eq!(engine.execute_transaction_bound(&bound), Err(Error::Limit));
    assert_eq!(
        engine.execute_transaction_text(&source, &Bindings::new()),
        Err(Error::Limit)
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn scalar_function_cli_and_selected_manifest_match_version_eight() {
    let (database, admin, engine) = fixture();
    let manifest = capability_manifest_for(V8);
    let metadata = serde_json::to_value(&manifest).unwrap();
    assert_eq!(metadata["language_version"], "0.8");
    assert_eq!(
        metadata["limits"]["function_arguments"],
        MAX_FUNCTION_ARGUMENTS
    );
    for version in EARLIER {
        assert!(
            serde_json::to_value(capability_manifest_for(version)).unwrap()["limits"]
                .get("function_arguments")
                .is_none()
        );
    }
    let before = digest(&admin);
    let mut count = 0;
    let mut calls = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            count += 1;
            if source.contains("coalesce(") || source.contains("abs(") || source.contains("length(")
            {
                calls += 1;
            }
            with_sql_parser_forbidden(|| match parse_input_with_version(source, V8) {
                Ok(ParsedInput::Statement(tree)) => {
                    assert!(
                        engine.execute_text(source, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(engine.execute(&tree, &Bindings::new()).is_err());
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
                }
                Ok(ParsedInput::Transaction(tree)) => {
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
                    assert!(engine.execute_transaction(&tree, &Bindings::new()).is_err());
                }
                Ok(ParsedInput::Catalog(tree)) => {
                    assert!(
                        engine.execute_catalog_text(source).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine.prepare_catalog_bound_text(source).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(engine.execute_catalog(&tree).is_err());
                }
                Ok(ParsedInput::Once(_) | ParsedInput::Outcome(_) | ParsedInput::Inspection(_)) => {
                    panic!("earlier profile admitted a retry envelope: {source}")
                }
                Err(_) => {}
            });
        }
    }
    assert!(count > 0 && calls > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let db = database.to_str().unwrap();
    let result: Outcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.8",
        db,
        "items where id=1 select abs(n) as magnitude,length(txt) as chars,length(raw) as bytes",
    ])))
    .unwrap();
    assert_eq!(
        result.rows[0].values,
        [Value::Int(5), Value::Int(1), Value::Int(2)]
    );
    let result:TransactionOutcome=serde_json::from_value(json(&invoke(&["run","--language-version","0.8",db,"transaction read only { require coalesce(null,true); emit abs($n),length(\"e\\u0301\"),coalesce(null,null) }",r#"{"n":{"Int":-7}}"#]))).unwrap();
    assert_eq!(
        result.emitted,
        [
            EmittedValue::Scalar(Value::Int(7)),
            EmittedValue::Scalar(Value::Int(2)),
            EmittedValue::Scalar(Value::Null)
        ]
    );
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.8",
            db,
            "update items where id=1 set n=1 return abs(-9223372036854775808) as bad"
        ])),
        Error::Overflow
    );
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.8"]))["name"],
        "embedded-trusted-native-0.8"
    );
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.8"
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
            error(&invoke(&["run", "--language-version", "0.8", db, source]));
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

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
const V10: LanguageVersion = LanguageVersion::Embedded010;
const EARLIER: [LanguageVersion; 9] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
    LanguageVersion::Embedded06,
    LanguageVersion::Embedded07,
    LanguageVersion::Embedded08,
    LanguageVersion::Embedded09,
];
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-upsert-{}-{}-{}.db",
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
    (path, admin.clone(), Engine::with_version(admin, V10))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let result = database("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,txt TEXT)");
    result
        .1
        .sql_session()
        .execute("INSERT INTO items VALUES(1,10,'old')", &[], None)
        .unwrap();
    result
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
fn value(v: Value) -> Expr {
    Expr::Value(ValueExpr::Literal(v))
}
fn int(n: i64) -> Expr {
    value(Value::Int(n))
}
fn yes() -> Expr {
    value(Value::Bool(true))
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn existing(name: &str) -> Expr {
    Expr::ConflictField {
        source: ConflictSource::Existing,
        field: name.into(),
    }
}
fn incoming(name: &str) -> Expr {
    Expr::ConflictField {
        source: ConflictSource::Incoming,
        field: name.into(),
    }
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn insert(id: i64, n: Expr) -> Statement {
    Statement::InsertExpressions {
        table: "items".into(),
        values: vec![("id".into(), int(id)), ("n".into(), n)],
    }
}
fn conflict(statement: Statement, action: ConflictAction) -> Statement {
    Statement::OnConflict {
        statement: Box::new(statement),
        target: vec!["id".into()],
        action,
    }
}
fn nothing(statement: Statement) -> Statement {
    conflict(statement, ConflictAction::Nothing)
}
fn returning(statement: Statement) -> Statement {
    Statement::Returning {
        statement: Box::new(statement),
        projection: vec![],
    }
}
fn read(expression: Expr) -> Statement {
    Statement::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![Projection {
            expression,
            alias: Some("out".into()),
        }],
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
fn runtime_failure(admin: &Admin, engine: &Engine, source: &str, expected: Error) {
    let before = digest(admin);
    with_sql_parser_forbidden(|| {
        let tree = parse_with_version(source, V10).unwrap();
        let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
        assert_eq!(engine.execute_bound(&bound), Err(expected.clone()));
        assert_eq!(
            engine.execute_text(source, &Bindings::new()),
            Err(expected.clone())
        );
        assert_eq!(engine.execute(&tree, &Bindings::new()), Err(expected));
    });
    assert_eq!(digest(admin), before);
}

#[test]
fn upsert_version_gates_and_contextual_names_preserve_legacy() {
    let (database, admin, engine) = fixture();
    let source = "insert into items(id,n) values(1,99) on conflict(id) do nothing return *";
    let tree = returning(nothing(insert(1, int(99))));
    assert_eq!(parse_with_version(source, V10).unwrap(), tree);
    assert_eq!(
        parse_input_with_version(source, V10).unwrap(),
        ParsedInput::Statement(tree.clone())
    );
    let decoded: Statement = serde_json::from_value(serde_json::to_value(&tree).unwrap()).unwrap();
    assert_eq!(decoded, tree);
    let bound = engine.prepare_bound(&tree, &Bindings::new()).unwrap();
    assert_eq!(bound.language_version(), "0.10");
    assert_eq!(bound.profile(), "embedded-trusted-native-0.10");
    let block = Transaction {
        read_only: false,
        steps: vec![TransactionStep::Execute(tree.clone())],
        emit: None,
    };
    let bound_block = engine
        .prepare_transaction_bound(&block, &Bindings::new())
        .unwrap();
    let bare_conflict = existing("n");
    let nested_conflict = Expr::Case {
        operand: None,
        arms: vec![(yes(), int(1))],
        else_expr: Some(Box::new(incoming("n"))),
    };
    let roots = [
        read(bare_conflict.clone()),
        read(nested_conflict.clone()),
        insert(2, bare_conflict.clone()),
    ];
    let scalar_blocks = [
        Transaction {
            read_only: true,
            steps: vec![TransactionStep::Require(equal(bare_conflict, int(1)))],
            emit: None,
        },
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![EmitExpr::Scalar(nested_conflict)]),
        },
    ];
    let before = digest(&admin);
    for version in EARLIER {
        let old = Engine::with_version(admin.clone(), version);
        for statement in std::iter::once(&tree).chain(roots.iter()) {
            assert_eq!(
                old.execute(statement, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                old.prepare_bound(statement, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
        }
        assert!(parse_with_version(source, version).is_err());
        assert!(old.execute_text(source, &Bindings::new()).is_err());
        assert!(old.prepare_bound_text(source, &Bindings::new()).is_err());
        assert_eq!(old.execute_bound(&bound), Err(Error::Unsupported));
        for transaction in std::iter::once(&block).chain(scalar_blocks.iter()) {
            assert_eq!(
                old.execute_transaction(transaction, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                old.prepare_transaction_bound(transaction, &Bindings::new())
                    .unwrap_err(),
                Error::Unsupported
            );
        }
        assert_eq!(
            old.execute_transaction_bound(&bound_block),
            Err(Error::Unsupported)
        );
    }
    let old = Engine::with_version(admin.clone(), LanguageVersion::Embedded09);
    let old_statement = old
        .prepare_bound_text("items where id=1", &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_bound(&old_statement),
        Err(Error::Unsupported)
    );
    let old_block = old
        .prepare_transaction_bound_text("transaction read only { require true }", &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&old_block),
        Err(Error::Unsupported)
    );
    assert_eq!(digest(&admin), before);
    // All new keywords are contextual. Existing bare names retain their ASTs.
    for name in ["on", "conflict", "do", "nothing", "existing", "incoming"] {
        for source in [
            format!("{name} select {name}"),
            format!("insert {name} set id=1,{name}=2"),
        ] {
            for version in EARLIER {
                assert_eq!(
                    parse_with_version(&source, V10).unwrap(),
                    parse_with_version(&source, version).unwrap()
                );
            }
        }
    }
    for source in [
        "insert items set id=2,n=-9223372036854775808",
        "insert into items(id,n) values(2,7)",
        "update items where id=1 set n=case when true then abs(n) else 0 end",
    ] {
        assert_eq!(
            parse_with_version(source, V10).unwrap(),
            parse_with_version(source, LanguageVersion::Embedded09).unwrap()
        );
    }
    // Quoted prefixes are ordinary fields; they do not become conflict qualifiers.
    assert_eq!(
        parse_with_version("items select `existing`,`incoming`", V10).unwrap(),
        parse_with_version(
            "items select `existing`,`incoming`",
            LanguageVersion::Embedded09
        )
        .unwrap()
    );
    for source in [
        "insert into items(id) values(1) on conflict(id) do update set n=`existing`.n",
        "insert into items(id) values(1) on conflict(id) do update set n=`incoming`.n",
    ] {
        assert!(parse_with_version(source, V10).is_err());
    }
    drop(old);
    drop(engine);
    drop(admin);
    for version in EARLIER {
        error(&invoke(&[
            "run",
            "--language-version",
            version.as_str(),
            database.to_str().unwrap(),
            source,
        ]));
        error(&invoke(&[
            "run",
            "--language-version",
            version.as_str(),
            database.to_str().unwrap(),
            "transaction { insert into items(id) values(2); insert into items(id) values(1) on conflict(id) do nothing }",
        ]));
    }
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

#[test]
fn upsert_text_tree_prepared_and_local_outcomes_match_without_sql() {
    let source = "insert many into items(id,n) values(3,$new),(1,$change),(2,20) on conflict(id) do update when existing.n <= incoming.n set n=incoming.n at most 3 return *";
    let tree = returning(conflict(
        Statement::InsertMany {
            table: "items".into(),
            columns: vec!["id".into(), "n".into()],
            rows: vec![
                vec![int(3), parameter("new")],
                vec![int(1), parameter("change")],
                vec![int(2), int(20)],
            ],
            cap: ValueExpr::Literal(Value::Int(3)),
        },
        ConflictAction::Update {
            guard: Some(Expr::Compare(
                Box::new(existing("n")),
                Compare::Le,
                Box::new(incoming("n")),
            )),
            values: vec![("n".into(), incoming("n"))],
        },
    ));
    assert_eq!(parse_with_version(source, V10).unwrap(), tree);
    let mut baseline = None;
    for mode in 0..4 {
        let (_, admin, engine) = fixture();
        admin
            .sql_session()
            .execute("INSERT INTO items VALUES(2,20,'same')", &[], None)
            .unwrap();
        let unchanged = rows(&admin)[1].clone();
        let mut bindings = Bindings::from([
            ("new".into(), Value::Int(30)),
            ("change".into(), Value::Int(11)),
        ]);
        let out = with_sql_parser_forbidden(|| match mode {
            0 => engine.execute_text(source, &bindings),
            1 => engine.execute(&tree, &bindings),
            _ => {
                let bound = if mode == 2 {
                    engine.prepare_bound_text(source, &bindings)
                } else {
                    engine.prepare_bound(&tree, &bindings)
                }
                .unwrap();
                assert_eq!(bound.description().columns[1].data_type, DataType::BigInt);
                bindings.insert("new".into(), Value::Int(999));
                bindings.insert("change".into(), Value::Int(999));
                engine.execute_bound(&bound)
            }
        })
        .unwrap();
        assert_eq!((out.matched, out.affected, out.changed), (3, 3, 2));
        assert_eq!(out.rows, rows(&admin));
        assert_eq!(out.rows[1], unchanged);
        let results = out.conflict_results.as_ref().unwrap();
        assert_eq!(results.len(), 3);
        for (index, (row, changed)) in [(2, true), (0, true), (1, false)].into_iter().enumerate() {
            assert_eq!(results[index].input_index, index);
            assert_eq!(results[index].returned_row, Some(row));
            assert_eq!(results[index].changed, changed);
            assert_eq!(results[index].status, ConflictStatus::Success);
        }
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["conflict_results"][0]["status"], "success");
        if let Some(baseline) = &baseline {
            assert_eq!(&out, baseline);
        } else {
            baseline = Some(out);
        }
    }
    let (_, admin, engine) = fixture();
    let source = "insert into items(id,n) values(2,$n) on conflict(id) do update set n=existing.n+incoming.n return n";
    let bound = engine
        .prepare_bound_text(source, &Bindings::from([("n".into(), Value::Int(7))]))
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items(id,n) VALUES(2,100)", &[], None)
        .unwrap();
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&bound))
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(107)]
    );
    admin
        .sql_session()
        .execute("DELETE FROM items WHERE id=2", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [Value::Int(7)]
    );
    let block = "transaction { let skipped=(insert into items(id,n) values(1,99) on conflict(id) do nothing return id); emit @skipped.status,@skipped.changed }";
    let parsed = parse_transaction_with_version(block, V10).unwrap();
    let expected =
        with_sql_parser_forbidden(|| engine.execute_transaction_text(block, &Bindings::new()))
            .unwrap();
    assert_eq!(
        expected.emitted,
        [
            EmittedValue::Scalar(Value::Text("skipped".into())),
            EmittedValue::Scalar(Value::Int(0))
        ]
    );
    assert_eq!(
        expected.summaries[0].conflict_results.as_ref().unwrap()[0].status,
        ConflictStatus::Skipped
    );
    let summary_json = serde_json::to_value(&expected).unwrap();
    assert_eq!(
        summary_json["summaries"][0]["conflict_results"][0]["status"],
        "skipped"
    );
    with_sql_parser_forbidden(|| {
        assert_eq!(
            engine
                .execute_transaction(&parsed, &Bindings::new())
                .unwrap(),
            expected
        );
        for bound in [
            engine
                .prepare_transaction_bound(&parsed, &Bindings::new())
                .unwrap(),
            engine
                .prepare_transaction_bound_text(block, &Bindings::new())
                .unwrap(),
        ] {
            assert_eq!(engine.execute_transaction_bound(&bound).unwrap(), expected);
        }
    });
    for (source, expected) in [
        (
            "transaction { insert into items(id) values(3); let saved=(insert into items(id) values(1) on conflict(id) do nothing return id); emit @saved.row.id }",
            Error::Bind("local row".into()),
        ),
        (
            "transaction { let saved=(insert into items(id) values(1) on conflict(id) do nothing return id); emit case when @saved.status=\"skipped\" then 0 else @saved.row.id end }",
            Error::Bind("local row".into()),
        ),
        (
            "transaction { let saved=(insert into items(id) values(1) on conflict(id) do nothing return id); require @saved.status=\"success\"; emit @saved.row.id }",
            Error::RequirementFailed,
        ),
    ] {
        let before = digest(&admin);
        with_sql_parser_forbidden(|| {
            let bound = engine
                .prepare_transaction_bound_text(source, &Bindings::new())
                .unwrap();
            assert_eq!(
                engine.execute_transaction_bound(&bound),
                Err(expected.clone())
            );
            assert_eq!(
                engine.execute_transaction_text(source, &Bindings::new()),
                Err(expected)
            );
        });
        assert_eq!(digest(&admin), before);
    }
    let source = "transaction { let first=(insert into items(id,n) values(3,30) on conflict(id) do nothing return id); let second=(insert into items(id,n) values(3,99) on conflict(id) do nothing return id); emit @second.status,@first.row.id,@first.status }";
    let out =
        with_sql_parser_forbidden(|| engine.execute_transaction_text(source, &Bindings::new()))
            .unwrap();
    assert_eq!(
        out.emitted,
        [
            EmittedValue::Scalar(Value::Text("skipped".into())),
            EmittedValue::Scalar(Value::Int(3)),
            EmittedValue::Scalar(Value::Text("success".into()))
        ]
    );
    assert_eq!(
        out.summaries[0].conflict_results.as_ref().unwrap()[0].returned_row,
        Some(0)
    );
    assert_eq!(
        out.summaries[1].conflict_results.as_ref().unwrap()[0].returned_row,
        None
    );
    let plain = engine
        .execute_text("items where id=1", &Bindings::new())
        .unwrap();
    let json = serde_json::to_value(&plain).unwrap();
    assert!(json.get("conflict_results").is_none());
    let decoded: Outcome = serde_json::from_value(json).unwrap();
    assert!(decoded.conflict_results.is_none());
    let plain = engine
        .execute_transaction_text(
            "transaction read only { items where id=1 }",
            &Bindings::new(),
        )
        .unwrap();
    let json = serde_json::to_value(&plain).unwrap();
    assert!(json["summaries"][0].get("conflict_results").is_none());
    let decoded: TransactionOutcome = serde_json::from_value(json).unwrap();
    assert!(decoded.summaries[0].conflict_results.is_none());
}

#[test]
fn upsert_scope_caps_and_atomic_refusals_match_common_engine() {
    let (_, admin, engine) = fixture();
    admin
        .sql_session()
        .execute("INSERT INTO items(id,n) VALUES(2,20)", &[], None)
        .unwrap();
    let update = Statement::Update {
        table: "items".into(),
        filter: equal(Expr::Field("id".into()), int(1)),
        guard: None,
        values: vec![("n".into(), ValueExpr::Literal(Value::Int(11)))],
        intent: Intent::One,
    };
    let mut malformed = vec![
        (nothing(read(int(1))), Error::Unsupported),
        (nothing(nothing(insert(1, int(1)))), Error::Unsupported),
        (nothing(returning(insert(1, int(1)))), Error::Unsupported),
        (returning(returning(insert(1, int(1)))), Error::Unsupported),
        (returning(nothing(update)), Error::Unsupported),
    ];
    // Invalid wrapper shape wins before walking or serializing its oversized leaf.
    // These are modest caller-owned chains, not recursion stress fixtures.
    let mut chain = insert(1, value(Value::Text("x".repeat(MAX_INPUT + 1))));
    for _ in 0..8 {
        chain = nothing(chain);
    }
    malformed.push((chain.clone(), Error::Unsupported));
    malformed.push((
        Statement::OnConflict {
            statement: Box::new(chain.clone()),
            target: vec!["id".into(); 65],
            action: ConflictAction::Nothing,
        },
        Error::Unsupported,
    ));
    malformed.push((
        Statement::OnConflict {
            statement: Box::new(chain),
            target: vec!["id".into()],
            action: ConflictAction::Update {
                guard: None,
                values: vec![("n".into(), int(0)); 65],
            },
        },
        Error::Unsupported,
    ));
    // Valid wrapper shapes still enforce those collection limits.
    malformed.push((
        Statement::OnConflict {
            statement: Box::new(insert(1, int(0))),
            target: vec!["id".into(); 65],
            action: ConflictAction::Nothing,
        },
        Error::Limit,
    ));
    malformed.push((
        conflict(
            insert(1, int(0)),
            ConflictAction::Update {
                guard: None,
                values: vec![("n".into(), int(0)); 65],
            },
        ),
        Error::Bind("assignments".into()),
    ));
    let before = digest(&admin);
    for (statement, expected) in malformed {
        with_sql_parser_forbidden(|| {
            assert_eq!(
                engine.execute(&statement, &Bindings::new()),
                Err(expected.clone())
            );
            assert_eq!(
                engine.prepare_bound(&statement, &Bindings::new()).err(),
                Some(expected.clone())
            );
            for step in [
                TransactionStep::Execute(statement.clone()),
                TransactionStep::Let {
                    name: "invalid".into(),
                    statement: statement.clone(),
                },
            ] {
                let transaction = Transaction {
                    read_only: false,
                    steps: vec![TransactionStep::Execute(insert(50, int(50))), step],
                    emit: None,
                };
                assert_eq!(
                    engine.execute_transaction(&transaction, &Bindings::new()),
                    Err(expected.clone())
                );
                assert_eq!(
                    engine
                        .prepare_transaction_bound(&transaction, &Bindings::new())
                        .unwrap_err(),
                    expected
                );
            }
        });
        assert_eq!(digest(&admin), before);
    }
    for (source, expected) in [
        (
            "insert many into items(id,n) values(1,1/0) on conflict(id) do update when 1/0=0 set n=1/0 at most 0 return 1/0 as out",
            Error::TooManyMatches,
        ),
        (
            "insert many into items(id,n) values(1,1),(2,2) on conflict(id) do update when existing.id=1 set n=1/0 at most 2",
            Error::ConditionFailed,
        ),
        (
            "insert many into items(id,n) values(1,1),(2,2) on conflict(id) do update when null set n=incoming.n at most 2",
            Error::ConditionFailed,
        ),
        (
            "insert many into items(id) values(1),(1) on conflict(id) do nothing at most 2",
            Error::DuplicateTarget,
        ),
        (
            "insert into items(id,n) values(1,1/0) on conflict(id) do nothing",
            Error::DivisionByZero,
        ),
        (
            "insert many into items(id,n) values(1,11),(3,30) on conflict(id) do update set n=incoming.n at most 2 return 1/(id-3) as out",
            Error::DivisionByZero,
        ),
    ] {
        runtime_failure(&admin, &engine, source, expected);
    }
    let out = with_sql_parser_forbidden(|| {
        engine.execute_text(
            "insert into items(id,n) values(1,99) on conflict(id) do nothing return 1/0 as out",
            &Bindings::new(),
        )
    })
    .unwrap();
    assert!(out.rows.is_empty());
    assert_eq!(out.columns, ["out"]);
    assert_eq!(
        out.conflict_results.unwrap()[0].status,
        ConflictStatus::Skipped
    );
    let out=with_sql_parser_forbidden(||engine.execute_text("insert into items(id,n) values(3,30) on conflict(id) do update when 1/0=0 set n=1/0 return *",&Bindings::new())).unwrap();
    assert_eq!(
        out.rows[0].values,
        [Value::Int(3), Value::Int(30), Value::Null]
    );
    let before = digest(&admin);
    for source in [
        "insert into items(id,n) values(1,1) on conflict(id) do update set n=n+1",
        "insert into items(id,n) values(1,1) on conflict(id) do update when id=1 set n=incoming.n",
        "insert into items(id,n) values(1,1) on conflict(id) do update set n=incoming.missing",
        "insert into items(id,n) values(1,1) on conflict(id) do update set id=incoming.id",
        "insert into items(id,n) values(1,1) on conflict(n) do nothing",
        "insert into items(id,n) values(1,1) on conflict(id,id) do nothing",
        "insert into items(id,n) values(1,1) on conflict(id) do update when false set n=$missing",
        "insert into items(id,n) values(1,existing.n) on conflict(id) do nothing",
        "insert into items(id) values(1) on conflict(id) do nothing return incoming.id as out",
        "items select existing.n as out",
        "insert items set id=1 on conflict(id) do nothing",
        "insert into items(id) values(1) on conflict do nothing",
        "insert into items(id) values(1) on conflict(id) do nothing on conflict(id) do nothing",
        "insert many into items(id) values(1) at most 1 on conflict(id) do nothing",
        "insert into items(id) values(1) on conflict(id) do nothing at most 1",
        "transaction read only { insert into items(id) values(1) on conflict(id) do nothing }",
        "transaction { require existing.n=1 }",
        "transaction { emit case when true then 1 else incoming.n end }",
    ] {
        with_sql_parser_forbidden(|| match parse_input_with_version(source, V10) {
            Ok(ParsedInput::Statement(tree)) => {
                assert!(
                    engine.prepare_bound(&tree, &Bindings::new()).is_err(),
                    "{source}"
                );
                assert!(
                    engine.execute_text(source, &Bindings::new()).is_err(),
                    "{source}"
                );
            }
            Ok(ParsedInput::Transaction(tree)) => {
                assert!(
                    engine
                        .prepare_transaction_bound(&tree, &Bindings::new())
                        .is_err(),
                    "{source}"
                );
                assert!(
                    engine
                        .execute_transaction_text(source, &Bindings::new())
                        .is_err(),
                    "{source}"
                );
            }
            Ok(ParsedInput::Catalog(_)) => panic!("unexpected catalog input: {source}"),
            Ok(ParsedInput::Once(_) | ParsedInput::Outcome(_)) => {
                panic!("version 0.10 admitted a retry envelope: {source}")
            }
            Err(_) => {}
        });
    }
    assert_eq!(digest(&admin), before);
    let (_, admin, engine) =
        database("CREATE TABLE items(tenant TEXT,id BYTES,n BIGINT,PRIMARY KEY(tenant,id))");
    let source = "insert into items(tenant,id,n) values($tenant,$id,7) on conflict(id,tenant) do nothing return *";
    let bindings = Bindings::from([
        ("tenant".into(), Value::Text("t".into())),
        ("id".into(), Value::Bytes(vec![0, 1, 0])),
    ]);
    let bound = engine.prepare_bound_text(source, &bindings).unwrap();
    assert_eq!(engine.execute_bound(&bound).unwrap().changed, 1);
    assert_eq!(engine.execute_bound(&bound).unwrap().affected, 0);
    assert_eq!(rows(&admin).len(), 1);
    let (_, _, engine) = database(
        "CREATE TABLE items(id BIGINT PRIMARY KEY,existing BIGINT,incoming BIGINT,case BIGINT)",
    );
    with_sql_parser_forbidden(|| {
        engine
            .execute_text(
                "insert into items(id,existing,incoming,`case`) values(1,2,3,4)",
                &Bindings::new(),
            )
            .unwrap();
        let source = "insert into items(id,`case`) values(1,7) on conflict(id) do update set `case`=coalesce(incoming.`case`,existing.`case`) return existing,incoming,`case`";
        assert_eq!(
            engine.execute_text(source, &Bindings::new()).unwrap().rows[0].values,
            [Value::Int(2), Value::Int(3), Value::Int(7)]
        );
    });
    // Raw empty MANY remains an accepted typed-only intent with explicit empty metadata.
    let (_, _, engine) = fixture();
    let empty = returning(nothing(Statement::InsertMany {
        table: "items".into(),
        columns: vec!["n".into()],
        rows: vec![],
        cap: ValueExpr::Literal(Value::Int(0)),
    }));
    let out = engine.execute(&empty, &Bindings::new()).unwrap();
    assert_eq!(out.conflict_results, Some(vec![]));
    assert!(out.rows.is_empty());
    assert_eq!(out.columns, ["id", "n", "txt"]);
}

#[test]
fn upsert_cli_and_selected_manifest_match_version_ten() {
    // The inherited manifest's duplicate-input example supplies state.
    let (database, admin, engine) =
        database("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,state TEXT)");
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES(1,10,'ready')", &[], None)
        .unwrap();
    let manifest = capability_manifest_for(V10);
    let metadata = serde_json::to_value(&manifest).unwrap();
    assert_eq!(metadata["language_version"], "0.10");
    assert_eq!(metadata["limits"]["conflict_target_columns"], 64);
    assert_eq!(metadata["limits"]["conflict_input_outcomes"], 1024);
    for version in EARLIER {
        let value = serde_json::to_value(capability_manifest_for(version)).unwrap();
        assert!(value["limits"].get("conflict_target_columns").is_none());
        assert!(value["limits"].get("conflict_input_outcomes").is_none());
    }
    let before = digest(&admin);
    let mut count = 0;
    let mut conflicts = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            count += 1;
            if source.contains("on conflict") {
                conflicts += 1;
            }
            with_sql_parser_forbidden(|| match parse_input_with_version(source, V10) {
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
                Ok(ParsedInput::Once(_) | ParsedInput::Outcome(_)) => {
                    panic!("version 0.10 admitted a retry envelope: {source}")
                }
                Err(_) => {}
            });
        }
    }
    assert!(count > 0 && conflicts > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let db = database.to_str().unwrap();
    let out:Outcome=serde_json::from_value(json(&invoke(&["run","--language-version","0.10",db,"insert many into items(id,n) values(2,20),(1,99) on conflict(id) do nothing at most 2 return id,n"]))).unwrap();
    assert_eq!((out.matched, out.affected, out.changed), (2, 1, 1));
    assert_eq!(out.rows[0].values, [Value::Int(2), Value::Int(20)]);
    assert_eq!(
        out.conflict_results.as_ref().unwrap()[0].returned_row,
        Some(0)
    );
    assert_eq!(
        out.conflict_results.as_ref().unwrap()[1].status,
        ConflictStatus::Skipped
    );
    let out:TransactionOutcome=serde_json::from_value(json(&invoke(&["run","--language-version","0.10",db,"transaction { let saved=(insert into items(id,n) values(1,99) on conflict(id) do nothing return id); emit @saved.status,@saved.changed }"]))).unwrap();
    assert_eq!(
        out.emitted,
        [
            EmittedValue::Scalar(Value::Text("skipped".into())),
            EmittedValue::Scalar(Value::Int(0))
        ]
    );
    let admin = Admin::open(&database, Limits::default()).unwrap();
    let before = digest(&admin);
    drop(admin);
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.10",
            db,
            "insert many into items(id,n) values(1,11),(3,30) on conflict(id) do update set n=incoming.n at most 2 return 1/(id-3) as out"
        ])),
        Error::DivisionByZero
    );
    let mut count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::Cli)
    {
        for source in &capability.rejection_examples {
            count += 1;
            error(&invoke(&["run", "--language-version", "0.10", db, source]));
        }
    }
    assert!(count > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
    drop(admin);
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.10"]))["name"],
        "embedded-trusted-native-0.10"
    );
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.10"
        ])),
        metadata
    );
    let absent = path();
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

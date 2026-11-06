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

const V5: LanguageVersion = LanguageVersion::Embedded05;
const EARLIER: [LanguageVersion; 4] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
];

fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-transaction-native-{}-{}-{}.db",
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
    for source in [
        "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,narrow SMALLINT,state TEXT NOT NULL)",
        "CREATE TABLE outbox (id BIGINT PRIMARY KEY,x BIGINT)",
        "INSERT INTO items VALUES(1,10,NULL,'ready')",
        "INSERT INTO items VALUES(2,20,32767,'ready')",
    ] {
        admin.sql_session().execute(source, &[], None).unwrap();
    }
    (path, admin.clone(), Engine::with_version(admin, V5))
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
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn add(a: Expr, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), Arithmetic::Add, Box::new(b))
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn local(name: &str, column: &str) -> Expr {
    Expr::Local(LocalRef::Field {
        local: name.into(),
        field: column.into(),
    })
}
fn scalar(value: Value) -> EmitExpr {
    EmitExpr::Scalar(Expr::Value(ValueExpr::Literal(value)))
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn update(rhs: Expr) -> Statement {
    Statement::UpdateExpressions {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        values: vec![("x".into(), rhs)],
        intent: Intent::One,
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
    let value: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    serde_json::from_value(value["error"].clone()).unwrap()
}

#[test]
fn transaction_version_gates_and_contextual_keywords_preserve_legacy() {
    let (database, admin, engine) = fixture();
    let source = "transaction read only { let page=(items limit 1 select id); emit @page.count }";
    let tree = parse_transaction_with_version(source, V5).unwrap();
    assert_eq!(
        parse_input_with_version(source, V5).unwrap(),
        ParsedInput::Transaction(tree.clone())
    );
    let decoded: Transaction =
        serde_json::from_value(serde_json::to_value(&tree).unwrap()).unwrap();
    assert_eq!(tree, decoded);
    let bound = engine
        .prepare_transaction_bound(&tree, &Bindings::new())
        .unwrap();
    assert_eq!(bound.language_version(), "0.5");
    assert_eq!(bound.profile(), "embedded-trusted-native-0.5");
    let before = digest(&admin);
    for version in EARLIER {
        let old = Engine::with_version(admin.clone(), version);
        assert_eq!(
            parse_transaction_with_version(source, version),
            Err(Error::Unsupported)
        );
        assert!(parse_input_with_version(source, version).is_err());
        assert_eq!(
            old.execute_transaction_text(source, &Bindings::new()),
            Err(Error::Unsupported)
        );
        assert_eq!(
            old.prepare_transaction_bound_text(source, &Bindings::new())
                .unwrap_err(),
            Error::Unsupported
        );
        assert_eq!(
            old.execute_transaction(&decoded, &Bindings::new()),
            Err(Error::Unsupported)
        );
        assert_eq!(
            old.prepare_transaction_bound(&decoded, &Bindings::new())
                .unwrap_err(),
            Error::Unsupported
        );
        assert_eq!(
            old.execute_transaction_bound(&bound),
            Err(Error::Unsupported)
        );
        for source in [
            "insert into items(id,x,state) values(3,1+2,\"ready\")",
            "update many items where true when true set x=1 at most 2",
            "delete many items where true when true at most 2",
        ] {
            let statement = parse_with_version(source, V5).unwrap();
            assert_eq!(
                old.execute(&statement, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                old.prepare_bound(&statement, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
            assert!(old.execute_text(source, &Bindings::new()).is_err());
            assert!(old.prepare_bound_text(source, &Bindings::new()).is_err());
        }
    }
    assert!(
        Engine::new(admin.clone())
            .execute_transaction(&tree, &Bindings::new())
            .is_err()
    );
    assert_eq!(digest(&admin), before);
    for name in ["transaction", "let", "require", "emit"] {
        admin
            .sql_session()
            .execute(
                &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY,{name} BIGINT)"),
                &[],
                None,
            )
            .unwrap();
        admin
            .sql_session()
            .execute(&format!("INSERT INTO {name} VALUES(1,7)"), &[], None)
            .unwrap();
        let text = format!("{name} where id=1 select {name}");
        for version in EARLIER.into_iter().chain([V5]) {
            assert_eq!(
                parse_with_version(&text, version).unwrap(),
                parse(&text).unwrap()
            );
            assert!(matches!(
                parse_input_with_version(&text, version).unwrap(),
                ParsedInput::Statement(_)
            ));
            assert_eq!(
                Engine::with_version(admin.clone(), version)
                    .execute_text(&text, &Bindings::new())
                    .unwrap()
                    .rows[0]
                    .values,
                [Value::Int(7)]
            );
        }
        let block =
            format!("transaction read only {{ let saved=({text}); {text}; emit @saved.rows }}");
        assert_eq!(
            engine
                .execute_transaction_text(&block, &Bindings::new())
                .unwrap()
                .summaries
                .len(),
            2
        );
    }
    let old_bound = Engine::with_version(admin.clone(), LanguageVersion::Embedded04)
        .prepare_bound_text("items where id=1", &Bindings::new())
        .unwrap();
    assert_eq!(engine.execute_bound(&old_bound), Err(Error::Unsupported));
    let plain = "insert items set id=3,x=-9223372036854775808,state=\"ready\"";
    assert_eq!(
        parse_with_version(plain, V5).unwrap(),
        parse(plain).unwrap()
    );
    assert_eq!(V5.as_str(), "0.5");
    assert_eq!(serde_json::to_value(V5).unwrap(), "0.5");
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    assert_eq!(profile_for(V5).name, "embedded-trusted-native-0.5");
    let before_cli = digest(&admin);
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
    }
    error(&invoke(&["run", database.to_str().unwrap(), source]));
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before_cli);
}

fn example() -> (&'static str, Transaction) {
    let source = "transaction { \
        let saved=(update items where id=1 set x=x+$delta return x as value,narrow); \
        update items where id=1 set x=x+1; \
        let page=(items where id=1 select id,x); \
        insert into outbox(id,x) values($id,@saved.row.value); \
        require @saved.changed=1; \
        emit @saved.row,@page.rows,@saved.status,@saved.changed,@page.count,@saved.row.narrow,@saved.row.narrow+1,null }";
    let tree = Transaction {
        read_only: false,
        steps: vec![
            TransactionStep::Let {
                name: "saved".into(),
                statement: Statement::Returning {
                    statement: Box::new(update(add(field("x"), parameter("delta")))),
                    projection: vec![
                        projection(field("x"), Some("value")),
                        projection(field("narrow"), None),
                    ],
                },
            },
            TransactionStep::Execute(update(add(field("x"), int(1)))),
            TransactionStep::Let {
                name: "page".into(),
                statement: Statement::Read {
                    table: "items".into(),
                    filter: Some(equal(field("id"), int(1))),
                    order: vec![],
                    limit: None,
                    projection: vec!["id".into(), "x".into()],
                },
            },
            TransactionStep::Execute(Statement::InsertExpressions {
                table: "outbox".into(),
                values: vec![
                    ("id".into(), parameter("id")),
                    ("x".into(), local("saved", "value")),
                ],
            }),
            TransactionStep::Require(equal(
                Expr::Local(LocalRef::Changed {
                    local: "saved".into(),
                }),
                int(1),
            )),
        ],
        emit: Some(vec![
            EmitExpr::Row("saved".into()),
            EmitExpr::Rows("page".into()),
            EmitExpr::Scalar(Expr::Local(LocalRef::Status {
                local: "saved".into(),
            })),
            EmitExpr::Scalar(Expr::Local(LocalRef::Changed {
                local: "saved".into(),
            })),
            EmitExpr::Scalar(Expr::Local(LocalRef::Count {
                local: "page".into(),
            })),
            EmitExpr::Scalar(local("saved", "narrow")),
            EmitExpr::Scalar(add(local("saved", "narrow"), int(1))),
            scalar(Value::Null),
        ]),
    };
    (source, tree)
}

#[test]
fn transaction_text_tree_and_bound_execution_share_atomic_results() {
    let (source, tree) = example();
    assert_eq!(parse_transaction_with_version(source, V5).unwrap(), tree);
    let bindings = Bindings::from([
        ("id".into(), Value::Int(77)),
        ("delta".into(), Value::Int(2)),
    ]);
    let mut baseline = None;
    for mode in 0..4 {
        let (_, admin, engine) = fixture();
        let result = with_sql_parser_forbidden(|| match mode {
            0 => engine.execute_transaction_text(source, &bindings),
            1 => engine.execute_transaction(&tree, &bindings),
            _ => {
                let bound = if mode == 2 {
                    engine.prepare_transaction_bound_text(source, &bindings)
                } else {
                    engine.prepare_transaction_bound(&tree, &bindings)
                }
                .unwrap();
                assert_eq!(bound.description().tables.len(), 2);
                assert_eq!(
                    bound.description().emitted[5],
                    EmitDescription::Scalar {
                        data_type: Some(DataType::SmallInt),
                        nullable: true
                    }
                );
                assert_eq!(
                    bound.description().emitted[6],
                    EmitDescription::Scalar {
                        data_type: Some(DataType::BigInt),
                        nullable: true
                    }
                );
                assert_eq!(
                    bound.description().emitted[7],
                    EmitDescription::Scalar {
                        data_type: None,
                        nullable: true
                    }
                );
                engine.execute_transaction_bound(&bound)
            }
        })
        .unwrap();
        assert_eq!(
            result
                .summaries
                .iter()
                .map(|s| s.item_index)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(
            result.emitted[2],
            EmittedValue::Scalar(Value::Text("success".into()))
        );
        assert_eq!(result.emitted[3], EmittedValue::Scalar(Value::Int(1)));
        assert_eq!(result.emitted[4], EmittedValue::Scalar(Value::Int(1)));
        assert_eq!(
            &result.emitted[5..],
            [
                EmittedValue::Scalar(Value::Null),
                EmittedValue::Scalar(Value::Null),
                EmittedValue::Scalar(Value::Null)
            ]
        );
        let EmittedValue::Row { row: saved, .. } = &result.emitted[0] else {
            panic!("singular RETURN")
        };
        let EmittedValue::Rows {
            rows: page,
            complete,
            ..
        } = &result.emitted[1]
        else {
            panic!("read page")
        };
        assert_eq!(saved.values, [Value::Int(12), Value::Null]);
        assert_eq!(page[0].values, [Value::Int(1), Value::Int(13)]);
        assert!(*complete);
        assert!(saved.version < page[0].version);
        assert_eq!(rows(&admin)[0].values[1], Value::Int(13));
        assert_eq!(
            admin
                .sql_session()
                .execute("SELECT * FROM outbox LIMIT 1024", &[], None)
                .unwrap()
                .rows[0]
                .values,
            [Value::Int(77), Value::Int(12)]
        );
        if let Some(baseline) = &baseline {
            assert_eq!(&result, baseline);
        } else {
            baseline = Some(result);
        }
    }
    let (_, admin, engine) = fixture();
    let mut mutable = bindings;
    let before = digest(&admin);
    let bound =
        with_sql_parser_forbidden(|| engine.prepare_transaction_bound_text(source, &mutable))
            .unwrap();
    assert_eq!(digest(&admin), before);
    mutable.insert("delta".into(), Value::Int(99));
    mutable.insert("id".into(), Value::Int(88));
    admin
        .sql_session()
        .execute("UPDATE items SET x=40 WHERE id=1", &[], None)
        .unwrap();
    let result = with_sql_parser_forbidden(|| engine.execute_transaction_bound(&bound)).unwrap();
    let EmittedValue::Row { row, .. } = &result.emitted[0] else {
        panic!("row")
    };
    assert_eq!(row.values, [Value::Int(42), Value::Null]);
    assert_eq!(rows(&admin)[0].values[1], Value::Int(43));
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM outbox LIMIT 1024", &[], None)
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(77), Value::Int(42)]
    );

    let source = "transaction read only { let first=(items order by id limit 1 select id); let none=(items limit 0); emit @first.count,@first.rows,@none.count,@none.rows }";
    let before = digest(&admin);
    let result =
        with_sql_parser_forbidden(|| engine.execute_transaction_text(source, &Bindings::new()))
            .unwrap();
    assert_eq!(result.emitted[0], EmittedValue::Scalar(Value::Int(1)));
    assert_eq!(result.emitted[2], EmittedValue::Scalar(Value::Int(0)));
    assert!(
        matches!(&result.emitted[1], EmittedValue::Rows { rows, complete: false, .. } if rows.len()==1)
    );
    assert!(
        matches!(&result.emitted[3], EmittedValue::Rows { rows, complete: false, .. } if rows.is_empty())
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn transaction_local_shapes_and_upfront_binding_reject_invalid_blocks() {
    let (_, admin, engine) = fixture();
    let before = digest(&admin);
    for earlier in ["insert items set id=1,x=5,state=\"ready\"", "require 1/0=0"] {
        let text = format!("transaction {{ {earlier}; items select absent }}");
        let tree = parse_transaction_with_version(&text, V5).unwrap();
        with_sql_parser_forbidden(|| {
            assert_eq!(
                engine
                    .prepare_transaction_bound_text(&text, &Bindings::new())
                    .unwrap_err(),
                Error::Bind("column".into())
            );
            assert_eq!(
                engine.execute_transaction_text(&text, &Bindings::new()),
                Err(Error::Bind("column".into()))
            );
            assert_eq!(
                engine.execute_transaction(&tree, &Bindings::new()),
                Err(Error::Bind("column".into()))
            );
        });
    }
    for text in [
        "transaction { let a=(items); let a=(items) }",
        "transaction { let a=(items where x=@a.count) }",
        "transaction { require @later.count=1; let later=(items) }",
        "transaction { let a=(items); emit @a.row }",
        "transaction { let a=(items); emit @a.status }",
        "transaction { let a=(items); emit @a.changed }",
        "transaction { let a=(update items where id=1 set x=11); emit @a.row }",
        "transaction { let a=(update items where id=1 set x=11 return x as value); emit @a.row.x }",
        "transaction { let a=(update items where id=1 set x=11 return x); emit @a.rows }",
        "transaction { let a=(update items where id=1 set x=11 return x); emit @a.count }",
        "transaction { let a=(update many items where true set x=11 at most 2 return x); emit @a.row }",
        "transaction { require x=1 }",
        "transaction { emit x }",
        "transaction { insert into outbox(id,x) values(3,x) }",
        "transaction { let a=(items); items limit @a.count }",
        "transaction read only { require 1/0=0; update items where id=1 set x=11 }",
        "transaction { claim items where x=10 order by id set x=11 }",
        "transaction { claim items where x=10 order by id set x=11 return x }",
        "transaction { transaction { require true } }",
        "transaction once { require true }",
        "transaction { create table x(id bigint primary key) }",
        "transaction { emit 1; require true }",
        "transaction { emit 1; emit 2 }",
        "transaction {}",
        "transaction { emit }",
    ] {
        with_sql_parser_forbidden(|| {
            assert!(
                engine
                    .prepare_transaction_bound_text(text, &Bindings::new())
                    .is_err(),
                "{text}"
            );
            assert!(
                engine
                    .execute_transaction_text(text, &Bindings::new())
                    .is_err(),
                "{text}"
            );
            if let Ok(tree) = parse_transaction_with_version(text, V5) {
                assert!(
                    engine
                        .prepare_transaction_bound(&tree, &Bindings::new())
                        .is_err(),
                    "{text}"
                );
                assert!(
                    engine.execute_transaction(&tree, &Bindings::new()).is_err(),
                    "{text}"
                );
            }
        });
        assert_eq!(digest(&admin), before, "{text}");
    }
    for (tail, expected) in [
        ("require false", Error::RequirementFailed),
        ("require null", Error::RequirementFailed),
        ("emit 1/0", Error::DivisionByZero),
        ("emit 9223372036854775807+1", Error::Overflow),
    ] {
        let text = format!(
            "transaction {{ insert into outbox(id,x) values(3,99); update items where id=1 set x=99; {tail} }}"
        );
        let tree = parse_transaction_with_version(&text, V5).unwrap();
        with_sql_parser_forbidden(|| {
            let bound = engine
                .prepare_transaction_bound_text(&text, &Bindings::new())
                .unwrap();
            assert_eq!(
                engine.execute_transaction_bound(&bound),
                Err(expected.clone())
            );
            assert_eq!(
                engine.execute_transaction(&tree, &Bindings::new()),
                Err(expected.clone())
            );
            assert_eq!(
                engine.execute_transaction_text(&text, &Bindings::new()),
                Err(expected)
            );
        });
        assert_eq!(digest(&admin), before);
    }
    assert!(
        admin
            .sql_session()
            .execute("SELECT * FROM outbox LIMIT 1024", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    for text in [
        "items where x=@saved.row.x",
        "insert items set id=3,x=1+2,state=\"ready\"",
        "insert into outbox(id,x) values(3,1),(4,2)",
    ] {
        assert!(
            engine.execute_text(text, &Bindings::new()).is_err(),
            "{text}"
        );
    }
}

#[test]
fn transaction_native_limits_share_budget_across_steps_and_emits() {
    let (_, admin, engine) = fixture();
    let before = digest(&admin);
    let exact = format!(
        "transaction read only {{ {} }}",
        "require true;".repeat(MAX_TRANSACTION_ITEMS)
    );
    assert!(
        engine
            .execute_transaction_text(&exact, &Bindings::new())
            .unwrap()
            .summaries
            .is_empty()
    );
    let over = format!(
        "transaction read only {{ {} emit true }}",
        "require true;".repeat(MAX_TRANSACTION_ITEMS)
    );
    assert_eq!(parse_transaction_with_version(&over, V5), Err(Error::Limit));
    let tree = Transaction {
        read_only: true,
        steps: vec![],
        emit: Some(vec![scalar(Value::Int(1)); MAX_EMIT_VALUES]),
    };
    assert_eq!(
        engine
            .execute_transaction(&tree, &Bindings::new())
            .unwrap()
            .emitted
            .len(),
        MAX_EMIT_VALUES
    );
    let mut over = tree.clone();
    over.emit.as_mut().unwrap().push(scalar(Value::Int(1)));
    assert_eq!(
        engine
            .prepare_transaction_bound(&over, &Bindings::new())
            .unwrap_err(),
        Error::Limit
    );
    assert_eq!(
        engine.execute_transaction(&over, &Bindings::new()),
        Err(Error::Limit)
    );
    let mut deep = int(1);
    for _ in 0..MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    let exact_depth = Transaction {
        read_only: true,
        steps: vec![],
        emit: Some(vec![EmitExpr::Scalar(deep.clone())]),
    };
    let bound = engine
        .prepare_transaction_bound(&exact_depth, &Bindings::new())
        .unwrap();
    let expected = [EmittedValue::Scalar(Value::Int(1))];
    assert_eq!(
        engine.execute_transaction_bound(&bound).unwrap().emitted,
        expected
    );
    assert_eq!(
        engine
            .execute_transaction(&exact_depth, &Bindings::new())
            .unwrap()
            .emitted,
        expected
    );
    deep = Expr::Negate(Box::new(deep));
    for tree in [
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![EmitExpr::Scalar(deep)]),
        },
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![scalar(Value::Text("x".repeat(MAX_INPUT)))]),
        },
    ] {
        assert_eq!(
            engine
                .prepare_transaction_bound(&tree, &Bindings::new())
                .unwrap_err(),
            Error::Limit
        );
        assert_eq!(
            engine.execute_transaction(&tree, &Bindings::new()),
            Err(Error::Limit)
        );
    }
    let text = "transaction read only { require true }";
    for bindings in [
        (0..=MAX_BINDINGS)
            .map(|i| (format!("unused{i}"), Value::Null))
            .collect(),
        Bindings::from([(
            "unused".into(),
            Value::Text("x".repeat(MAX_BINDING_BYTES + 1)),
        )]),
    ] {
        assert_eq!(
            engine
                .prepare_transaction_bound_text(text, &bindings)
                .unwrap_err(),
            Error::Limit
        );
        assert_eq!(
            engine.execute_transaction_text(text, &bindings),
            Err(Error::Limit)
        );
    }
    assert_eq!(digest(&admin), before);
    // Parameters are charged at every bound occurrence, across all steps.
    let bindings = Bindings::from([(
        "p".into(),
        Value::Text("x".repeat(MAX_BOUND_VALUE_BYTES / 4)),
    )]);
    let exact = "transaction read only { let a=(items select $p as p); let b=(items select $p as p); let c=(items select $p as p); let d=(items select $p as p) }";
    assert!(
        engine
            .prepare_transaction_bound_text(exact, &bindings)
            .is_ok()
    );
    assert!(engine.execute_transaction_text(exact, &bindings).is_ok());
    let over = exact.replace(" }", "; let e=(items select $p as p) }");
    assert_eq!(
        engine
            .prepare_transaction_bound_text(&over, &bindings)
            .unwrap_err(),
        Error::Limit
    );
    assert_eq!(
        engine.execute_transaction_text(&over, &bindings),
        Err(Error::Limit)
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn transaction_values_and_guarded_bulk_use_shared_atomic_rules() {
    let (_, admin, engine) = fixture();
    for (source, expected) in [
        (
            "update many items where true when false set x=x+1 at most 1",
            Error::TooManyMatches,
        ),
        (
            "update many items where true when false set x=x+1 at most 2",
            Error::ConditionFailed,
        ),
        (
            "delete many items where true when null at most 2",
            Error::ConditionFailed,
        ),
        (
            "update many items where true when 1/(2-id)=1 set x=x+1 at most 2",
            Error::DivisionByZero,
        ),
    ] {
        let before = digest(&admin);
        with_sql_parser_forbidden(|| {
            let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
            assert_eq!(engine.execute_bound(&bound), Err(expected.clone()));
            assert_eq!(engine.execute_text(source, &Bindings::new()), Err(expected));
        });
        assert_eq!(digest(&admin), before);
    }
    let source = "insert into outbox(id,x) values($id,2*3+1) return *";
    let bindings = Bindings::from([("id".into(), Value::Int(77))]);
    let inserted = with_sql_parser_forbidden(|| engine.execute_text(source, &bindings)).unwrap();
    assert_eq!(inserted.rows[0].values, [Value::Int(77), Value::Int(7)]);
    let source = "transaction { update many items where true when state=\"ready\" set x=x+1 at most 2; update many items where true when x>10 set x=x+1 at most 2; let done=(items order by id select x); emit @done.rows }";
    let outcome =
        with_sql_parser_forbidden(|| engine.execute_transaction_text(source, &Bindings::new()))
            .unwrap();
    assert_eq!(
        outcome
            .summaries
            .iter()
            .map(|s| s.changed)
            .collect::<Vec<_>>(),
        [2, 2, 0]
    );
    let EmittedValue::Rows { rows, .. } = &outcome.emitted[0] else {
        panic!("rows")
    };
    assert_eq!(
        rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        [vec![Value::Int(12)], vec![Value::Int(22)]]
    );
    let removed = with_sql_parser_forbidden(|| {
        engine.execute_text(
            "delete many items where true when x>10 at most 2 return id",
            &Bindings::new(),
        )
    })
    .unwrap();
    assert_eq!(removed.changed, 2);
    assert_eq!(removed.rows.len(), 2);
}

#[test]
fn transaction_cli_and_selected_manifest_match_version_five() {
    let (database, admin, engine) = fixture();
    let manifest = capability_manifest_for(V5);
    let metadata = serde_json::to_value(&manifest).unwrap();
    assert_eq!(metadata["language_version"], "0.5");
    for (name, expected) in [
        ("transaction_items", MAX_TRANSACTION_ITEMS),
        ("transaction_locals", MAX_TRANSACTION_LOCALS),
        ("emit_values", MAX_EMIT_VALUES),
        ("retained_local_bytes", MAX_RETAINED_LOCAL_BYTES),
        ("emit_bytes", MAX_EMIT_BYTES),
    ] {
        assert_eq!(metadata["limits"][name], expected);
        for version in EARLIER {
            assert!(
                serde_json::to_value(capability_manifest_for(version)).unwrap()["limits"]
                    .get(name)
                    .is_none()
            );
        }
    }
    let before = digest(&admin);
    let mut transaction_examples = 0;
    let mut rejected = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            rejected += 1;
            with_sql_parser_forbidden(|| match parse_input_with_version(source, V5) {
                Ok(ParsedInput::Statement(tree)) => {
                    assert!(
                        engine.execute_text(source, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine.prepare_bound_text(source, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                    assert!(
                        engine.execute(&tree, &Bindings::new()).is_err(),
                        "{}: {source}",
                        capability.feature
                    );
                }
                Ok(ParsedInput::Transaction(tree)) => {
                    transaction_examples += 1;
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
                    panic!("profile 0.5 accepted catalog syntax: {source}");
                }
                Ok(ParsedInput::Once(_) | ParsedInput::Outcome(_) | ParsedInput::Inspection(_)) => {
                    panic!("earlier profile admitted a retry envelope: {source}")
                }
                Err(_) => {}
            });
        }
    }
    assert!(rejected > 0 && transaction_examples > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let db = database.to_str().unwrap();
    let result: TransactionOutcome = serde_json::from_value(json(&invoke(&[
        "run", "--language-version", "0.5", db,
        "transaction { let saved=(update items where id=1 set x=x+$delta return id,x); require @saved.changed=1; emit @saved.row,@saved.status }",
        r#"{"delta":{"Int":2}}"#,
    ]))).unwrap();
    assert_eq!(result.summaries.len(), 1);
    assert!(
        matches!(&result.emitted[0], EmittedValue::Row { row, .. } if row.values==[Value::Int(1),Value::Int(12)])
    );
    assert_eq!(
        result.emitted[1],
        EmittedValue::Scalar(Value::Text("success".into()))
    );
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.5",
            db,
            "transaction { update items where id=1 set x=99; require null; emit 123 }"
        ])),
        Error::RequirementFailed
    );
    let read: TransactionOutcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.5",
        db,
        "transaction read only { let a=(items where id=1 select x); emit @a.rows }",
    ])))
    .unwrap();
    assert!(
        matches!(&read.emitted[0], EmittedValue::Rows { rows, complete:true, .. } if rows[0].values==[Value::Int(12)])
    );
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.5"]))["name"],
        "embedded-trusted-native-0.5"
    );
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.5"
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
            error(&invoke(&["run", "--language-version", "0.5", db, source]));
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
            "transaction { require true }"
        ])),
        Error::Unsupported
    );
    assert!(!absent.exists());
}

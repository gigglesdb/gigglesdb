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
const V9: LanguageVersion = LanguageVersion::Embedded09;
const EARLIER: [LanguageVersion; 8] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
    LanguageVersion::Embedded06,
    LanguageVersion::Embedded07,
    LanguageVersion::Embedded08,
];
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-case-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn database() -> (PathBuf, Admin, Engine) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    (path, admin.clone(), Engine::with_version(admin, V9))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let (path, admin, engine) = database();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,state TEXT NOT NULL,case BOOLEAN,guard BOOLEAN,then BIGINT,else BIGINT,end BIGINT)",&[],None).unwrap();
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,-5,'ready',true,true,11,12,13)",
            &[],
            None,
        )
        .unwrap();
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(2,NULL,'ready',false,true,21,22,23)",
            &[],
            None,
        )
        .unwrap();
    (path, admin, engine)
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
fn yes() -> Expr {
    value(Value::Bool(true))
}
fn no() -> Expr {
    value(Value::Bool(false))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn abs(e: Expr) -> Expr {
    Expr::Call {
        function: ScalarFunction::Abs,
        arguments: vec![e],
    }
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn case(operand: Option<Expr>, arms: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Expr {
    Expr::Case {
        operand: operand.map(Box::new),
        arms,
        else_expr: else_expr.map(Box::new),
    }
}
fn searched(arms: Vec<(Expr, Expr)>, otherwise: Option<Expr>) -> Expr {
    case(None, arms, otherwise)
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn read(expression: Expr) -> Statement {
    Statement::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![projection(expression, Some("out"))],
    }
}
fn update(expression: Expr) -> Statement {
    Statement::UpdateExpressions {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        values: vec![("n".into(), expression)],
        intent: Intent::One,
    }
}
fn returning(statement: Statement, expression: Expr) -> Statement {
    Statement::Returning {
        statement: Box::new(statement),
        projection: vec![projection(expression, Some("out"))],
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
fn case_version_gates_cover_typed_roots_and_keyword_compatibility() {
    let (database, admin, engine) = fixture();
    let expression = searched(vec![(yes(), int(1))], Some(int(2)));
    let boolean = searched(vec![(yes(), yes())], Some(no()));
    let statements = vec![
        Statement::Read {
            table: "items".into(),
            filter: Some(boolean.clone()),
            order: vec![],
            limit: None,
            projection: vec![],
        },
        read(expression.clone()),
        update(expression.clone()),
        Statement::InsertExpressions {
            table: "items".into(),
            values: vec![
                ("id".into(), int(3)),
                ("n".into(), expression.clone()),
                ("state".into(), value(Value::Text("ready".into()))),
            ],
        },
        Statement::InsertMany {
            table: "items".into(),
            columns: vec!["id".into(), "n".into(), "state".into()],
            rows: vec![vec![
                int(3),
                expression.clone(),
                value(Value::Text("ready".into())),
            ]],
            cap: ValueExpr::Literal(Value::Int(1)),
        },
        returning(update(int(1)), expression.clone()),
        read(abs(expression.clone())),
    ];
    let blocks = vec![
        Transaction {
            read_only: true,
            steps: vec![TransactionStep::Require(boolean.clone())],
            emit: None,
        },
        Transaction {
            read_only: true,
            steps: vec![TransactionStep::Require(Expr::And(
                Box::new(yes()),
                Box::new(boolean),
            ))],
            emit: None,
        },
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![EmitExpr::Scalar(expression.clone())]),
        },
        Transaction {
            read_only: true,
            steps: vec![],
            emit: Some(vec![EmitExpr::Scalar(abs(expression))]),
        },
    ];
    let source = "items select case when true then 1 else 2 end as out";
    assert_eq!(parse_with_version(source, V9).unwrap(), statements[1]);
    let prepared = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
    assert_eq!(prepared.language_version(), "0.9");
    let block = engine
        .prepare_transaction_bound(&blocks[0], &Bindings::new())
        .unwrap();
    let before = digest(&admin);
    for version in EARLIER {
        let older = Engine::with_version(admin.clone(), version);
        for statement in &statements {
            let tree: Statement =
                serde_json::from_value(serde_json::to_value(statement).unwrap()).unwrap();
            assert_eq!(
                older.execute(&tree, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                older.prepare_bound(&tree, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
        }
        for transaction in &blocks {
            let tree: Transaction =
                serde_json::from_value(serde_json::to_value(transaction).unwrap()).unwrap();
            assert_eq!(
                older.execute_transaction(&tree, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                older
                    .prepare_transaction_bound(&tree, &Bindings::new())
                    .err(),
                Some(Error::Unsupported)
            );
        }
        assert!(parse_with_version(source, version).is_err());
        assert!(parse_with_version("`items`", version).is_err());
        assert_eq!(older.execute_bound(&prepared), Err(Error::Unsupported));
        assert_eq!(
            older.execute_transaction_bound(&block),
            Err(Error::Unsupported)
        );
    }
    assert_eq!(digest(&admin), before);
    let old = Engine::with_version(admin.clone(), LanguageVersion::Embedded08);
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
    let legacy = "update items where case when guard set n=1";
    let migration = "update items where `case` when guard set n=1";
    for version in EARLIER {
        assert_eq!(
            parse_with_version(legacy, version).unwrap(),
            parse(legacy).unwrap()
        );
    }
    assert!(parse_with_version(legacy, V9).is_err());
    assert_eq!(
        parse_with_version(migration, V9).unwrap(),
        parse(legacy).unwrap()
    );
    for name in ["case", "then", "else", "end"] {
        let bare = format!("items where id=1 select {name}");
        let quoted = format!("items where id=1 select `{name}`");
        for version in EARLIER {
            assert_eq!(
                parse_with_version(&bare, version).unwrap(),
                parse(&bare).unwrap()
            );
        }
        assert!(parse_with_version(&bare, V9).is_err());
        assert_eq!(
            parse_with_version(&quoted, V9).unwrap(),
            parse(&bare).unwrap()
        );
    }
    let simple = "items select case `case` when true then 1 else 0 end as out";
    let expected = read(case(
        Some(field("case")),
        vec![(yes(), int(1))],
        Some(int(0)),
    ));
    assert_eq!(parse_with_version(simple, V9).unwrap(), expected);
    assert_eq!(
        engine
            .execute_text(simple, &Bindings::new())
            .unwrap()
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        [vec![Value::Int(1)], vec![Value::Int(0)]]
    );
    assert_eq!(serde_json::to_value(V9).unwrap(), "0.9");
    assert_eq!(profile_for(V9).name, "embedded-trusted-native-0.9");
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    let before = digest(&admin);
    drop(engine);
    drop(old);
    drop(admin);
    for version in EARLIER {
        for text in [
            source,
            "`items` where id=1",
            "transaction { update items where id=1 set n=99; emit case when true then 1 end }",
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
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

#[test]
fn case_text_tree_and_bound_execution_match_without_sql() {
    let source = "update many items where true set n=case when n is null then $fallback else abs(n) end at most 2 return id,n,case n when 5 then \"five\" when 7 then \"seven\" else \"other\" end as label";
    let tree = Statement::Returning {
        statement: Box::new(Statement::UpdateExpressions {
            table: "items".into(),
            filter: yes(),
            guard: None,
            values: vec![(
                "n".into(),
                searched(
                    vec![(
                        Expr::IsNull(Box::new(field("n")), false),
                        parameter("fallback"),
                    )],
                    Some(abs(field("n"))),
                ),
            )],
            intent: Intent::Many {
                cap: ValueExpr::Literal(Value::Int(2)),
            },
        }),
        projection: vec![
            projection(field("id"), None),
            projection(field("n"), None),
            projection(
                case(
                    Some(field("n")),
                    vec![
                        (int(5), value(Value::Text("five".into()))),
                        (int(7), value(Value::Text("seven".into()))),
                    ],
                    Some(value(Value::Text("other".into()))),
                ),
                Some("label"),
            ),
        ],
    };
    assert_eq!(parse_with_version(source, V9).unwrap(), tree);
    let mut baseline = None;
    for mode in 0..4 {
        let (_, admin, engine) = fixture();
        let mut bindings = Bindings::from([("fallback".into(), Value::Int(7))]);
        let outcome = with_sql_parser_forbidden(|| match mode {
            0 => engine.execute_text(source, &bindings),
            1 => engine.execute(&tree, &bindings),
            _ => {
                let bound = if mode == 2 {
                    engine.prepare_bound_text(source, &bindings)
                } else {
                    engine.prepare_bound(&tree, &bindings)
                }
                .unwrap();
                assert_eq!(bound.description().columns[2].id, 0);
                assert_eq!(bound.description().columns[2].data_type, DataType::Text);
                assert!(!bound.description().columns[2].not_null);
                bindings.insert("fallback".into(), Value::Int(99));
                engine.execute_bound(&bound)
            }
        })
        .unwrap();
        assert_eq!(
            outcome.rows[0].values,
            [Value::Int(1), Value::Int(5), Value::Text("five".into())]
        );
        assert_eq!(
            outcome.rows[1].values,
            [Value::Int(2), Value::Int(7), Value::Text("seven".into())]
        );
        assert_eq!(outcome.changed, 2);
        for (returned, stored) in outcome.rows.iter().zip(rows(&admin)) {
            assert_eq!(returned.version, stored.version);
        }
        if let Some(baseline) = &baseline {
            assert_eq!(&outcome, baseline);
        } else {
            baseline = Some(outcome);
        }
    }
    let (_, admin, engine) = fixture();
    let bound = engine
        .prepare_bound_text(
            source,
            &Bindings::from([("fallback".into(), Value::Int(7))]),
        )
        .unwrap();
    admin
        .sql_session()
        .execute("UPDATE items SET n=-10 WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [Value::Int(1), Value::Int(10), Value::Text("other".into())]
    );
    // END closes the CASE primary; surrounding operators retain their usual precedence.
    for (expression, expected_tree, expected_value) in [
        (
            "case when true then 1 else 0 end + 2 * 3 = 7",
            equal(
                Expr::Arithmetic(
                    Box::new(searched(vec![(yes(), int(1))], Some(int(0)))),
                    Arithmetic::Add,
                    Box::new(Expr::Arithmetic(
                        Box::new(int(2)),
                        Arithmetic::Multiply,
                        Box::new(int(3)),
                    )),
                ),
                int(7),
            ),
            Value::Bool(true),
        ),
        (
            "case when true then 2 else 0 end = 2",
            equal(searched(vec![(yes(), int(2))], Some(int(0))), int(2)),
            Value::Bool(true),
        ),
        (
            "case case when true then 1 else 2 end when 1 then 7 else 0 end",
            case(
                Some(searched(vec![(yes(), int(1))], Some(int(2)))),
                vec![(int(1), int(7))],
                Some(int(0)),
            ),
            Value::Int(7),
        ),
        (
            "case when true then case when false then 3 else 4 end else 5 end",
            searched(
                vec![(yes(), searched(vec![(no(), int(3))], Some(int(4))))],
                Some(int(5)),
            ),
            Value::Int(4),
        ),
        (
            "case when case when true then 1 else 0 end = 1 then 7 else 8 end",
            searched(
                vec![(
                    equal(searched(vec![(yes(), int(1))], Some(int(0))), int(1)),
                    int(7),
                )],
                Some(int(8)),
            ),
            Value::Int(7),
        ),
    ] {
        let source = format!("items select {expression} as out");
        assert_eq!(
            parse_with_version(&source, V9).unwrap(),
            read(expected_tree)
        );
        with_sql_parser_forbidden(|| {
            let bound = engine
                .prepare_bound_text(&source, &Bindings::new())
                .unwrap();
            let direct = engine.execute_text(&source, &Bindings::new()).unwrap();
            assert_eq!(engine.execute_bound(&bound).unwrap(), direct);
            assert_eq!(direct.rows[0].values, [expected_value]);
        });
    }
    for (expression, expected) in [
        (
            "case when true then 7 when 1/0=0 then 9 else abs(-9223372036854775808) end",
            Value::Int(7),
        ),
        (
            "case null when null then 1 when 0 then 2 else 3 end",
            Value::Int(3),
        ),
        ("case when false then 1 end", Value::Null),
        ("abs(case when true then -7 else 1/0 end)", Value::Int(7)),
        (
            "case (case when true then 1 else 2 end) when 1 then 7 else 0 end",
            Value::Int(7),
        ),
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
    for (expression, expected) in [
        (
            "case null when 1/0 then 1 else 2 end",
            Error::DivisionByZero,
        ),
        (
            "case when false and 1/0=0 then 1 else 2 end",
            Error::DivisionByZero,
        ),
        (
            "case when true then 1/0 when true then 7 end",
            Error::DivisionByZero,
        ),
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
        "case when true then 1 else \"wrong\" end",
        "case null when 1 then 1 when \"x\" then 2 end",
        "case when 1 then 2 end",
        "case when false then null end",
    ] {
        let source = format!("items where false select {expression} as out");
        assert_eq!(
            engine.prepare_bound_text(&source, &Bindings::new()).err(),
            Some(Error::Type)
        );
        assert_eq!(
            engine.execute_text(&source, &Bindings::new()),
            Err(Error::Type)
        );
    }
    let source = "transaction read only { emit case when false then null end,case when false then abs(null) else length(null) end }";
    let bound = engine
        .prepare_transaction_bound_text(source, &Bindings::new())
        .unwrap();
    assert_eq!(
        bound.description().emitted,
        [
            EmitDescription::Scalar {
                data_type: None,
                nullable: true
            },
            EmitDescription::Scalar {
                data_type: Some(DataType::BigInt),
                nullable: true
            }
        ]
    );
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_transaction_bound(&bound))
            .unwrap()
            .emitted,
        [
            EmittedValue::Scalar(Value::Null),
            EmittedValue::Scalar(Value::Null)
        ]
    );
    let exact = format!(
        "items select case {} end as out",
        vec!["when false then $p"; MAX_CASE_ARMS].join(" ")
    );
    let bindings = Bindings::from([("p".into(), Value::Text("x".repeat(1015)))]);
    let bound = engine.prepare_bound_text(&exact, &bindings).unwrap();
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [Value::Null]
    );
    assert_eq!(
        engine
            .prepare_bound_text(
                &exact,
                &Bindings::from([("p".into(), Value::Text("x".repeat(1016)))])
            )
            .err(),
        Some(Error::Limit)
    );
    let explicit = exact.replace(" end as out", " else null end as out");
    assert_eq!(
        engine.prepare_bound_text(&explicit, &bindings).err(),
        Some(Error::Limit)
    );
    let too_many = format!(
        "items select case {} end as out",
        vec!["when true then 1"; MAX_CASE_ARMS + 1].join(" ")
    );
    assert_eq!(parse_with_version(&too_many, V9), Err(Error::Limit));
    assert_eq!(
        engine
            .prepare_bound(&read(searched(vec![], None)), &Bindings::new())
            .err(),
        Some(Error::Type)
    );
    let mut deep = int(1);
    for _ in 0..MAX_DEPTH {
        deep = searched(vec![(yes(), deep)], None);
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
            .prepare_bound(&read(searched(vec![(yes(), deep)], None)), &Bindings::new())
            .err(),
        Some(Error::Limit)
    );
    for limit in [0, 1] {
        let source =
            format!("items limit {limit} select case when id=1 then 7 else 1/0 end as out");
        let before = digest(&admin);
        assert_eq!(
            engine.execute_text(&source, &Bindings::new()),
            Err(Error::DivisionByZero)
        );
        assert_eq!(digest(&admin), before);
    }
    let before = digest(&admin);
    assert_eq!(engine.execute_text("update many items where true set n=1 at most 2 return case when id=1 then 7 else 1/0 end as out",&Bindings::new()),Err(Error::DivisionByZero));
    assert_eq!(digest(&admin), before);
    for source in [
        "items select case end as x",
        "items select case when true 1 end as x",
        "items select case when true then 1 else 2 else 3 end as x",
        "items select case when true then 1 as x",
        "items select case when true then 1,when false then 2 end as x",
        "items limit case when true then 1 end",
        "delete many items where true at most case when true then 1 end",
        "insert items set id=3,n=case when true then 1 end,state=\"ready\"",
    ] {
        assert!(
            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
    }
}

#[test]
fn case_quoted_names_are_canonical_and_share_sql_catalog() {
    let (_, admin, engine) = database();
    let source = "create table `select` (`where` int64,`case` int32,`null` nullable text,`abs` int64,primary key (`where`))";
    let tree = CatalogStatement::CreateTable {
        table: "select".into(),
        columns: vec![
            ColumnSpec {
                name: "where".into(),
                data_type: CatalogType::Int64,
                nullable: false,
            },
            ColumnSpec {
                name: "case".into(),
                data_type: CatalogType::Int32,
                nullable: false,
            },
            ColumnSpec {
                name: "null".into(),
                data_type: CatalogType::Text,
                nullable: true,
            },
            ColumnSpec {
                name: "abs".into(),
                data_type: CatalogType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["where".into()],
    };
    assert_eq!(parse_catalog_with_version(source, V9).unwrap(), tree);
    let created = with_sql_parser_forbidden(|| engine.execute_catalog(&tree)).unwrap();
    assert_eq!(created.table.name, "select");
    assert_eq!(created.table.primary_key, [0]);
    let longest = "a".repeat(63);
    let longest_column = "b".repeat(63);
    let long = format!(
        "create table `{longest}` (`{longest_column}` int64,primary key (`{longest_column}`))"
    );
    assert_eq!(
        engine.execute_catalog_text(&long).unwrap().table.columns[0].name,
        longest_column
    );
    for source in [
        "create table `deliveries`(id int64,primary key(id))",
        "create table bad(id int64,`pg_x` text,primary key(id))",
    ] {
        let before = digest(&admin);
        assert_eq!(
            engine.execute_catalog_text(source),
            Err(Error::Sql { sqlstate: "0A000".into() })
        );
        assert_eq!(digest(&admin), before);
    }
    let source =
        "insert into `select` (`where`,`case`,`null`,`abs`) values (1,2,\"kept\",-7) return *";
    let inserted =
        with_sql_parser_forbidden(|| engine.execute_text(source, &Bindings::new())).unwrap();
    let sql = admin
        .sql_session()
        .execute("SELECT * FROM select LIMIT 1024", &[], None)
        .unwrap();
    assert_eq!(sql.rows, inserted.rows);
    assert_eq!(sql.columns, ["where", "case", "null", "abs"]);
    let qualified = admin
        .sql_session()
        .execute(
            "SELECT q.where,q.case,q.null,q.abs FROM select q WHERE q.where=1",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        qualified.rows[0].values,
        [
            Value::Int(1),
            Value::Int(2),
            Value::Text("kept".into()),
            Value::Int(-7)
        ]
    );
    let source = "`select` where `where`=1 select `null` as `then`,abs(`abs`) as `end`";
    let outcome =
        with_sql_parser_forbidden(|| engine.execute_text(source, &Bindings::new())).unwrap();
    assert_eq!(outcome.columns, ["then", "end"]);
    assert_eq!(
        outcome.rows[0].values,
        [Value::Text("kept".into()), Value::Int(7)]
    );
    assert_eq!(
        parse_with_version("`select` select `null`", V9).unwrap(),
        Statement::Read {
            table: "select".into(),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec!["null".into()]
        }
    );
    assert!(
        engine
            .prepare_bound_text("`select` select `abs`(-1) as value", &Bindings::new())
            .is_err()
    );
    assert_eq!(
        engine
            .prepare_bound_text("`select` select null as value", &Bindings::new())
            .err(),
        Some(Error::Type)
    );
    let before = digest(&admin);
    for source in [
        "`A`", "`é`", "`a.b`", "`_a`", "`a b`", "``", "`a``b`", "`a-b`",
    ] {
        assert_eq!(
            parse_with_version(source, V9),
            Err(Error::Unsupported),
            "{source}"
        );
        assert!(engine.execute_text(source, &Bindings::new()).is_err());
    }
    assert_eq!(
        parse_with_version(&format!("`{}`", "x".repeat(64)), V9),
        Err(Error::Limit)
    );
    assert!(matches!(
        parse_with_version("`unfinished", V9),
        Err(Error::Syntax { .. })
    ));
    assert_eq!(digest(&admin), before);
    let source = "transaction { let case=(update `select` where `where`=1 set `abs`=`abs` return `abs` as `then`); require @case.changed=0; emit @case.row.`then`,$end }";
    let bindings = Bindings::from([("end".into(), Value::Int(9))]);
    let parsed = parse_transaction_with_version(source, V9).unwrap();
    assert!(matches!(&parsed.steps[0],TransactionStep::Let{name,..} if name=="case"));
    let outcome =
        with_sql_parser_forbidden(|| engine.execute_transaction(&parsed, &bindings)).unwrap();
    assert_eq!(
        outcome.emitted,
        [
            EmittedValue::Scalar(Value::Int(-7)),
            EmittedValue::Scalar(Value::Int(9))
        ]
    );
    for source in [
        "transaction { let `case`=(`select`); emit 1 }",
        "transaction { let case=(`select`); emit @`case`.count }",
        "transaction { let case=(update `select` where `where`=1 set `abs`=1 return `abs` as `then`); emit @case.row.then }",
    ] {
        let before = digest(&admin);
        assert!(
            engine
                .prepare_transaction_bound_text(source, &Bindings::new())
                .is_err()
        );
        assert!(
            engine
                .execute_transaction_text(source, &Bindings::new())
                .is_err()
        );
        assert_eq!(digest(&admin), before);
    }
    let decoded: Statement = serde_json::from_value(
        serde_json::to_value(parse_with_version("`select` select `null`", V9).unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        engine.execute(&decoded, &Bindings::new()).unwrap().rows[0].values,
        [Value::Text("kept".into())]
    );
}

#[test]
fn case_cli_and_selected_manifest_match_version_nine() {
    let (database, admin, engine) = fixture();
    let manifest = capability_manifest_for(V9);
    let metadata = serde_json::to_value(&manifest).unwrap();
    assert_eq!(metadata["language_version"], "0.9");
    assert_eq!(metadata["limits"]["case_arms"], MAX_CASE_ARMS);
    assert_eq!(metadata["limits"]["quoted_identifier_bytes"], 63);
    for version in EARLIER {
        let v = serde_json::to_value(capability_manifest_for(version)).unwrap();
        assert!(v["limits"].get("case_arms").is_none());
        assert!(v["limits"].get("quoted_identifier_bytes").is_none());
    }
    let before = digest(&admin);
    let mut count = 0;
    let mut case_examples = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            count += 1;
            if source.contains("case ") {
                case_examples += 1;
            }
            with_sql_parser_forbidden(|| match parse_input_with_version(source, V9) {
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
    assert!(count > 0 && case_examples > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let db = database.to_str().unwrap();
    let outcome: Outcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.9",
        db,
        "items select case `case` when true then 1 else 0 end as result,`then`",
    ])))
    .unwrap();
    assert_eq!(outcome.rows[0].values, [Value::Int(1), Value::Int(11)]);
    assert_eq!(outcome.rows[1].values, [Value::Int(0), Value::Int(21)]);
    let outcome:TransactionOutcome=serde_json::from_value(json(&invoke(&["run","--language-version","0.9",db,"transaction read only { require case when true then true else false end; emit case $end when 1 then 7 else 0 end }",r#"{"end":{"Int":1}}"#]))).unwrap();
    assert_eq!(outcome.emitted, [EmittedValue::Scalar(Value::Int(7))]);
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.9",
            db,
            "update many items where true set n=1 at most 2 return case when id=1 then 7 else 1/0 end as out"
        ])),
        Error::DivisionByZero
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
            error(&invoke(&["run", "--language-version", "0.9", db, source]));
        }
    }
    assert!(count > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
    drop(admin);
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.9"]))["name"],
        "embedded-trusted-native-0.9"
    );
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.9"
        ])),
        metadata
    );
    let empty = path();
    json(&invoke(&[
        "init",
        "--engine",
        "legacy",
        empty.to_str().unwrap(),
    ]));
    let created: CatalogOutcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.9",
        empty.to_str().unwrap(),
        "create table `case` (`then` int64,primary key(`then`))",
    ])))
    .unwrap();
    assert_eq!(created.table.name, "case");
    assert_eq!(created.table.columns[0].name, "then");
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

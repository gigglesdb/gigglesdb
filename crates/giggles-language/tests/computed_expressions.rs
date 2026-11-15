use giggles_core::{
    Admin, Limits,
    relational::{DataType, with_sql_parser_forbidden},
};
use giggles_language::*;
use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

const V3: LanguageVersion = LanguageVersion::Embedded03;
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-computed-native-{}-{}-{}.db",
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
    (path, admin.clone(), Engine::with_version(admin, V3))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let (path, admin, engine) = database(
        "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,y BIGINT,narrow SMALLINT,src TEXT,dest TEXT,state TEXT NOT NULL)",
    );
    let mut sql = admin.sql_session();
    sql.execute(
        "INSERT INTO items VALUES(1,5,7,32767,'alpha','old','ready')",
        &[],
        None,
    )
    .unwrap();
    sql.execute(
        "INSERT INTO items VALUES(2,-3,2,-32768,'beta','old','ready')",
        &[],
        None,
    )
    .unwrap();
    (path, admin, engine)
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn int(n: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(n)))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
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
fn read(projection: Vec<Projection>) -> Statement {
    Statement::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection,
    }
}
fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(args)
        .output()
        .unwrap()
}
fn output_json(out: &Output) -> serde_json::Value {
    assert!(out.status.success(), "{out:?}");
    assert!(out.stderr.is_empty());
    serde_json::from_slice(&out.stdout).unwrap()
}
fn output_error(out: &Output) -> Error {
    assert!(!out.status.success(), "{out:?}");
    assert!(out.stdout.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    serde_json::from_value(value["error"].clone()).unwrap()
}

#[test]
fn computed_version_gates_preserve_legacy_statement_shapes() {
    let (_, admin, v3) = fixture();
    let before = rows(&admin);
    for text in [
        "items",
        "items select id,x",
        "update items where id=1 set x=2,y=$y",
        "update items where id=1 set x=-9223372036854775808",
    ] {
        assert_eq!(
            parse_with_version(text, V3).unwrap(),
            parse_with_version(text, LanguageVersion::Embedded02).unwrap(),
            "{text}"
        );
    }
    let source = "update items where id=1 set x=y,y=x+1";
    let tree = Statement::UpdateExpressions {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        intent: Intent::One,
        values: vec![
            ("x".into(), field("y")),
            ("y".into(), binary(field("x"), Arithmetic::Add, int(1))),
        ],
    };
    assert_eq!(parse_with_version(source, V3).unwrap(), tree);
    let read_source = "items select x as old_x,y+1 as next_y";
    let read_tree = read(vec![
        projection(field("x"), Some("old_x")),
        projection(binary(field("y"), Arithmetic::Add, int(1)), Some("next_y")),
    ]);
    assert_eq!(parse_with_version(read_source, V3).unwrap(), read_tree);
    assert!(matches!(
        parse_with_version("items select x as x", V3).unwrap(),
        Statement::ReadExpressions { .. }
    ));
    assert!(matches!(
        parse_with_version("update items where id=1 set x=y", V3).unwrap(),
        Statement::UpdateExpressions { .. }
    ));
    for version in [LanguageVersion::Embedded01, LanguageVersion::Embedded02] {
        let old = Engine::with_version(admin.clone(), version);
        for (source, tree) in [(source, &tree), (read_source, &read_tree)] {
            assert!(parse_with_version(source, version).is_err());
            assert!(old.execute_text(source, &Bindings::new()).is_err());
            assert!(old.prepare_bound_text(source, &Bindings::new()).is_err());
            let decoded: Statement =
                serde_json::from_value(serde_json::to_value(tree).unwrap()).unwrap();
            assert_eq!(
                old.execute(&decoded, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                old.prepare_bound(&decoded, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
            let bound = v3.prepare_bound(tree, &Bindings::new()).unwrap();
            assert_eq!(old.execute_bound(&bound), Err(Error::Unsupported));
        }
        // Even a new variant containing only a plain field is version-gated.
        assert_eq!(
            old.execute(&read(vec![projection(field("x"), None)]), &Bindings::new()),
            Err(Error::Unsupported)
        );
        let bound = old
            .prepare_bound_text("items where id=1", &Bindings::new())
            .unwrap();
        assert_eq!(v3.execute_bound(&bound), Err(Error::Unsupported));
    }
    assert!(parse("items select x+1 as next").is_err());
    assert!(parse_with_version("items where x+1=6 select x", LanguageVersion::Embedded02).is_ok());
    assert_eq!(rows(&admin), before);
    assert_eq!(V3.as_str(), "0.3");
    assert_eq!(profile_for(V3).name, "embedded-trusted-native-0.3");
    assert_eq!(serde_json::to_value(V3).unwrap(), "0.3");
    assert_eq!(
        serde_json::from_str::<LanguageVersion>("\"0.3\"").unwrap(),
        V3
    );
    let manifest = capability_manifest_for(V3);
    assert_eq!(manifest.language_version, "0.3");
    assert!(
        manifest
            .capabilities
            .iter()
            .all(|c| c.language_version == "0.3")
    );
    let metadata = serde_json::to_value(manifest).unwrap();
    assert_eq!(metadata["limits"]["computed_mutation_bytes"], 1048576);
    assert_eq!(metadata["limits"]["computed_projection_bytes"], 1048576);
    for version in [LanguageVersion::Embedded01, LanguageVersion::Embedded02] {
        let metadata = serde_json::to_value(capability_manifest_for(version)).unwrap();
        assert!(metadata["limits"].get("computed_mutation_bytes").is_none());
        assert!(
            metadata["limits"]
                .get("computed_projection_bytes")
                .is_none()
        );
    }
}

#[test]
fn computed_native_text_tree_and_bound_requests_match_without_sql() {
    for source in [
        "update many items where true set x=y,y=x+1,dest=src at most 2",
        "update items where id=1 when x+1=6 set x=x*2",
        "claim items where x>0 order by id set x=x-10",
        "items where x+1>0 select id,x*2 as doubled,src as label",
    ] {
        let mut outcomes = Vec::new();
        for mode in 0..3 {
            let (_, admin, engine) = fixture();
            let tree = parse_with_version(source, V3).unwrap();
            let outcome = with_sql_parser_forbidden(|| match mode {
                0 => engine.execute_text(source, &Bindings::new()),
                1 => engine.execute(&tree, &Bindings::new()),
                _ => {
                    let bound = engine.prepare_bound(&tree, &Bindings::new()).unwrap();
                    assert_eq!(bound.language_version(), "0.3");
                    engine.execute_bound(&bound)
                }
            })
            .unwrap();
            if source.starts_with("claim") {
                assert_eq!(outcome.rows[0], rows(&admin)[0]);
            }
            outcomes.push((outcome, rows(&admin)));
        }
        assert_eq!(outcomes[0], outcomes[1], "{source}");
        assert_eq!(outcomes[0], outcomes[2], "{source}");
    }
    let (_, admin, engine) = fixture();
    let before = rows(&admin);
    let catalog = admin.catalog().unwrap();
    for (source, error) in [
        (
            "update many items where true set x=10/(id-2) at most 2",
            Error::DivisionByZero,
        ),
        ("update items where true set x=1/0", Error::AmbiguousMatch),
        (
            "update items where id=1 when false set x=1/0",
            Error::ConditionFailed,
        ),
        ("update items where id=1 set narrow=narrow+1", Error::Overflow),
        (
            "claim items where 10/x>0 order by id set x=x-x",
            Error::DivisionByZero,
        ),
        (
            "claim items where x>0 order by id set x=x",
            Error::ClaimNotConsumed,
        ),
    ] {
        with_sql_parser_forbidden(|| {
            let tree = parse_with_version(source, V3).unwrap();
            let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
            assert_eq!(
                engine.execute_text(source, &Bindings::new()),
                Err(error.clone()),
                "{source}"
            );
            assert_eq!(
                engine.execute(&tree, &Bindings::new()),
                Err(error.clone()),
                "{source}"
            );
            assert_eq!(engine.execute_bound(&bound), Err(error), "{source}");
        });
        assert_eq!(rows(&admin), before);
        assert_eq!(admin.catalog().unwrap(), catalog);
    }
    let mut bindings = Bindings::from([("delta".into(), Value::Int(2))]);
    let bound = engine
        .prepare_bound_text("update items where id=1 set x=x+$delta", &bindings)
        .unwrap();
    bindings.insert("delta".into(), Value::Int(99));
    admin
        .sql_session()
        .execute("UPDATE items SET x=20 WHERE id=1", &[], None)
        .unwrap();
    with_sql_parser_forbidden(|| engine.execute_bound(&bound)).unwrap();
    assert_eq!(rows(&admin)[0].values[1], Value::Int(22));
}

#[test]
fn computed_projection_metadata_and_hidden_page_errors_match() {
    let (_, admin, engine) = fixture();
    let fields = admin.catalog().unwrap().1[0].columns.clone();
    let source = "items order by id select narrow as small,x+1 as next,x<y as less,src as label,\"text\" as constant,dest is null as missing,null+1 as nullable_int";
    let bound =
        with_sql_parser_forbidden(|| engine.prepare_bound_text(source, &Bindings::new())).unwrap();
    let columns = &bound.description().columns;
    let mut source_field = fields[3].clone();
    source_field.name = "small".into();
    assert_eq!(columns[0], source_field);
    let mut source_field = fields[4].clone();
    source_field.name = "label".into();
    assert_eq!(columns[3], source_field);
    for (i, kind) in [
        (1, DataType::BigInt),
        (2, DataType::Boolean),
        (4, DataType::Text),
        (5, DataType::Boolean),
        (6, DataType::BigInt),
    ] {
        assert_eq!(columns[i].id, 0);
        assert_eq!(columns[i].data_type, kind);
    }
    assert!(!columns[1].not_null);
    assert!(!columns[2].not_null);
    assert!(columns[5].not_null);
    assert!(!columns[6].not_null);
    let result = with_sql_parser_forbidden(|| engine.execute_bound(&bound)).unwrap();
    assert_eq!(
        result.rows[0].values,
        [
            Value::Int(32767),
            Value::Int(6),
            Value::Bool(true),
            Value::Text("alpha".into()),
            Value::Text("text".into()),
            Value::Bool(false),
            Value::Null
        ]
    );
    assert_eq!(result.rows[0].version, rows(&admin)[0].version);
    for source in [
        "items select x+1",
        "items select (x*2)",
        "items order by x+1 select x",
        "items limit 1+1 select x",
    ] {
        assert!(parse_with_version(source, V3).is_err(), "{source}");
    }
    for source in [
        "items select x as same,y as same",
        "items select x as y,y",
        "items where total=6 select x+1 as total",
        "items order by total select x+1 as total",
    ] {
        assert!(
            engine.execute_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
        assert!(
            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
    }
    assert_eq!(
        engine
            .prepare_bound_text("items select null as untyped", &Bindings::new())
            .err(),
        Some(Error::Type)
    );
    let result = engine
        .execute_text("items order by y select -x as y", &Bindings::new())
        .unwrap();
    assert_eq!(
        result
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(3)], vec![Value::Int(-5)]]
    );
    let before = rows(&admin);
    for source in [
        "items limit 0 select 1/(id-2) as quotient",
        "items limit 1 select 1/(id-2) as quotient",
    ] {
        with_sql_parser_forbidden(|| {
            let tree = parse_with_version(source, V3).unwrap();
            let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
            assert_eq!(
                engine.execute_text(source, &Bindings::new()),
                Err(Error::DivisionByZero)
            );
            assert_eq!(
                engine.execute(&tree, &Bindings::new()),
                Err(Error::DivisionByZero)
            );
            assert_eq!(engine.execute_bound(&bound), Err(Error::DivisionByZero));
        });
        assert_eq!(rows(&admin), before);
    }
    assert_eq!(
        engine
            .execute_text(
                "items where id=1 limit 0 select 1/(id-2) as quotient",
                &Bindings::new()
            )
            .unwrap()
            .matched,
        1
    );
    admin
        .sql_session()
        .execute(
            "CREATE TABLE empty_items (id BIGINT PRIMARY KEY,note TEXT)",
            &[],
            None,
        )
        .unwrap();
    for source in [
        "empty_items select note+1 as bad",
        "empty_items select absent as bad",
        "empty_items select null as bad",
    ] {
        assert!(
            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
        assert!(
            engine.execute_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
    }
    let empty = engine
        .prepare_bound_text("empty_items select 1/0 as runtime", &Bindings::new())
        .unwrap();
    assert!(engine.execute_bound(&empty).unwrap().rows.is_empty());
}

#[test]
fn computed_native_expression_admission_and_expansion_are_bounded() {
    let (_, admin, engine) = fixture();
    let before = rows(&admin);
    let mut deep = int(1);
    for _ in 0..=MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    let tree = Statement::UpdateExpressions {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        intent: Intent::One,
        values: vec![("x".into(), deep.clone())],
    };
    for tree in [tree, read(vec![projection(deep, Some("deep"))])] {
        assert_eq!(
            engine.prepare_bound(&tree, &Bindings::new()).err(),
            Some(Error::Limit)
        );
        assert_eq!(engine.execute(&tree, &Bindings::new()), Err(Error::Limit));
    }
    let bindings = Bindings::from([(
        "p".into(),
        Value::Text("x".repeat(MAX_BINDING_BYTES / 2 + 1)),
    )]);
    for source in [
        "items select $p as a,$p as b",
        "update items where id=1 set src=$p,dest=$p,x=x",
    ] {
        let tree = parse_with_version(source, V3).unwrap();
        assert_eq!(
            engine.prepare_bound_text(source, &bindings).err(),
            Some(Error::Limit)
        );
        assert_eq!(engine.execute(&tree, &bindings), Err(Error::Limit));
    }
    assert_eq!(rows(&admin), before);
    for extra in [0, 1] {
        let (_, admin, engine) =
            database("CREATE TABLE items (id BIGINT PRIMARY KEY,a BYTEA,b BYTEA)");
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,$1,$2)",
                &[
                    Value::Bytes(vec![1; 16384]),
                    Value::Bytes(vec![2; 16384 + extra]),
                ],
                None,
            )
            .unwrap();
        let columns = (0..64)
            .map(|i| format!("{} as c{i}", if i == 63 { "b" } else { "a" }))
            .collect::<Vec<_>>()
            .join(",");
        for limit in [0, 1] {
            let source = format!("items limit {limit} select {columns}");
            with_sql_parser_forbidden(|| {
                let tree = parse_with_version(&source, V3).unwrap();
                let bound = engine
                    .prepare_bound_text(&source, &Bindings::new())
                    .unwrap();
                if extra == 0 {
                    let expected = engine.execute_text(&source, &Bindings::new()).unwrap();
                    assert_eq!(expected.matched, 1);
                    assert_eq!(expected.rows.len(), limit as usize);
                    assert_eq!(engine.execute(&tree, &Bindings::new()).unwrap(), expected);
                    assert_eq!(engine.execute_bound(&bound).unwrap(), expected);
                } else {
                    assert_eq!(
                        engine.execute_text(&source, &Bindings::new()),
                        Err(Error::Limit)
                    );
                    assert_eq!(engine.execute(&tree, &Bindings::new()), Err(Error::Limit));
                    assert_eq!(engine.execute_bound(&bound), Err(Error::Limit));
                }
            });
        }
    }
    let (_, admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,src TEXT,copy TEXT,marker TEXT)");
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,$1,NULL,'mm')",
            &[Value::Text("x".repeat(32763))],
            None,
        )
        .unwrap();
    let before = rows(&admin);
    let source = "update items where id=1 set copy=src";
    let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
    assert_eq!(engine.execute_bound(&bound), Err(Error::Limit));
    assert_eq!(
        engine.execute_text(source, &Bindings::new()),
        Err(Error::Limit)
    );
    assert_eq!(rows(&admin), before);
}

fn initialize_cli(database: &Path) {
    output_json(&invoke(&[
        "init",
        "--engine",
        "legacy",
        database.to_str().unwrap(),
        "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,y BIGINT,n INTEGER,state TEXT NOT NULL)",
    ]));
    output_json(&invoke(&[
        "run",
        database.to_str().unwrap(),
        "insert items set id=1,x=5,y=7,n=5,state=\"ready\"",
    ]));
}

#[test]
fn computed_cli_selects_version_three_explicitly() {
    let database = path();
    initialize_cli(&database);
    let p = database.to_str().unwrap();
    for source in [
        "update items where id=1 set x=x+1",
        "items select x+1 as next",
    ] {
        output_error(&invoke(&["run", p, source]));
        for version in ["0.1", "0.2"] {
            output_error(&invoke(&["run", "--language-version", version, p, source]));
        }
    }
    assert_eq!(
        output_json(&invoke(&[
            "run",
            "--language-version",
            "0.3",
            p,
            "update items where id=1 set x=y,y=x+$delta",
            r#"{"delta":{"Int":1}}"#
        ]))["changed"],
        1
    );
    let output = output_json(&invoke(&[
        "run",
        "--language-version",
        "0.3",
        p,
        "items select x as old_y,y as old_x_plus_one,x+y as total",
    ]));
    assert_eq!(
        output["columns"],
        serde_json::json!(["old_y", "old_x_plus_one", "total"])
    );
    assert_eq!(
        output["rows"][0]["values"],
        serde_json::json!([{"Int":7},{"Int":6},{"Int":13}])
    );
    assert_eq!(
        output_json(&invoke(&["run", p, "items select x,y"]))["rows"][0]["values"],
        serde_json::json!([{"Int":7},{"Int":6}])
    );
    assert_eq!(
        output_error(&invoke(&[
            "run",
            "--language-version",
            "0.3",
            p,
            "items limit 0 select 1/0 as bad"
        ])),
        Error::DivisionByZero
    );
    assert_eq!(
        output_json(&invoke(&["capabilities", "--language-version", "0.3"]))["name"],
        "embedded-trusted-native-0.3"
    );
    let manifest = output_json(&invoke(&[
        "capability-manifest",
        "--language-version",
        "0.3",
    ]));
    assert_eq!(manifest["language_version"], "0.3");
    assert_eq!(manifest["limits"]["computed_mutation_bytes"], 1048576);
    assert_eq!(manifest["limits"]["computed_projection_bytes"], 1048576);
    let absent = database.with_extension("absent");
    assert!(!absent.exists());
    assert_eq!(
        output_error(&invoke(&[
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

#[test]
fn computed_selected_manifest_rejections_use_selected_version() {
    let database = path();
    initialize_cli(&database);
    let manifest = capability_manifest_for(V3);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    let before = rows(&admin);
    let engine = Engine::with_version(admin.clone(), V3);
    let mut count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            count += 1;
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
        }
    }
    assert!(count > 0);
    assert_eq!(rows(&admin), before);
    drop(engine);
    drop(admin);
    let mut count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::Cli)
    {
        for source in &capability.rejection_examples {
            count += 1;
            output_error(&invoke(&[
                "run",
                "--language-version",
                "0.3",
                database.to_str().unwrap(),
                source,
            ]));
        }
    }
    assert!(count > 0);
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(rows(&admin), before);
}

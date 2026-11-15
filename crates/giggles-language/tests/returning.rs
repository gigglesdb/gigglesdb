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

const V4: LanguageVersion = LanguageVersion::Embedded04;
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-returning-native-{}-{}-{}.db",
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
    (path, admin.clone(), Engine::with_version(admin, V4))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let (path, admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,state TEXT NOT NULL)");
    for source in [
        "INSERT INTO items VALUES(1,10,'ready')",
        "INSERT INTO items VALUES(2,20,'ready')",
    ] {
        admin.sql_session().execute(source, &[], None).unwrap();
    }
    (path, admin, engine)
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn int(n: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(n)))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
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
    let value: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    serde_json::from_value(value["error"].clone()).unwrap()
}

#[test]
fn returning_version_gates_and_contextual_keyword_preserve_legacy() {
    let (database, admin, engine) = fixture();
    let before = rows(&admin);
    let base = "update items where id=1 set x=-9223372036854775808";
    let source = format!("{base} return *");
    let tree = parse_with_version(&source, V4).unwrap();
    assert_eq!(
        tree,
        returning(
            parse_with_version(base, LanguageVersion::Embedded03).unwrap(),
            vec![]
        )
    );
    let plain = "update items where id=1 set x=$x";
    assert_eq!(
        parse_with_version(plain, V4).unwrap(),
        parse_with_version(plain, LanguageVersion::Embedded03).unwrap()
    );
    for version in [
        LanguageVersion::Embedded01,
        LanguageVersion::Embedded02,
        LanguageVersion::Embedded03,
    ] {
        let older = Engine::with_version(admin.clone(), version);
        assert!(parse_with_version(&source, version).is_err());
        assert!(older.execute_text(&source, &Bindings::new()).is_err());
        assert!(older.prepare_bound_text(&source, &Bindings::new()).is_err());
        let decoded: Statement =
            serde_json::from_value(serde_json::to_value(&tree).unwrap()).unwrap();
        assert_eq!(
            older.execute(&decoded, &Bindings::new()),
            Err(Error::Unsupported)
        );
        assert_eq!(
            older.prepare_bound(&decoded, &Bindings::new()).err(),
            Some(Error::Unsupported)
        );
        let bound = engine.prepare_bound(&tree, &Bindings::new()).unwrap();
        assert_eq!(bound.language_version(), "0.4");
        assert_eq!(older.execute_bound(&bound), Err(Error::Unsupported));
        let old_bound = older
            .prepare_bound_text("items where id=1", &Bindings::new())
            .unwrap();
        assert_eq!(engine.execute_bound(&old_bound), Err(Error::Unsupported));
    }
    assert!(parse(&source).is_err());
    assert_eq!(rows(&admin), before);
    for source in [
        "update items where id=1 set x=1 return",
        "update items where id=1 set x=1 return *,id",
        "update items where id=1 set x=1 return id,*",
        "update items where id=1 set x=1 return id return x",
        "update items where id=1 set x=1 returning id",
        "items return id",
    ] {
        assert!(
            engine.prepare_bound_text(source, &Bindings::new()).is_err(),
            "{source}"
        );
    }
    admin
        .sql_session()
        .execute(
            "CREATE TABLE return (id BIGINT PRIMARY KEY,return BIGINT)",
            &[],
            None,
        )
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO return VALUES(1,1)", &[], None)
        .unwrap();
    for version in [
        LanguageVersion::Embedded01,
        LanguageVersion::Embedded02,
        LanguageVersion::Embedded03,
        V4,
    ] {
        let selected = Engine::with_version(admin.clone(), version);
        let text = "return where id=1 select return";
        assert_eq!(
            parse_with_version(text, version).unwrap(),
            parse(text).unwrap()
        );
        assert_eq!(
            selected.execute_text(text, &Bindings::new()).unwrap().rows[0].values,
            [Value::Int(1)]
        );
        assert_eq!(
            selected
                .execute_text("update return where id=1 set return=1", &Bindings::new())
                .unwrap()
                .changed,
            0
        );
    }
    let named = engine
        .execute_text(
            "update return where id=1 set return=return+1 return return as return",
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(named.columns, ["return"]);
    assert_eq!(named.rows[0].values, [Value::Int(2)]);
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    assert_eq!(profile_for(V4).name, "embedded-trusted-native-0.4");
    assert_eq!(V4.as_str(), "0.4");
    assert_eq!(serde_json::to_value(V4).unwrap(), "0.4");
    assert_eq!(
        serde_json::from_str::<LanguageVersion>("\"0.4\"").unwrap(),
        V4
    );
    let manifest = serde_json::to_value(capability_manifest_for(V4)).unwrap();
    assert_eq!(manifest["language_version"], "0.4");
    assert_eq!(manifest["limits"]["return_result_bytes"], 1048576);
    for version in [
        LanguageVersion::Embedded01,
        LanguageVersion::Embedded02,
        LanguageVersion::Embedded03,
    ] {
        assert!(
            serde_json::to_value(capability_manifest_for(version)).unwrap()["limits"]
                .get("return_result_bytes")
                .is_none()
        );
    }
    drop(engine);
    drop(admin);
    let source = "update items where id=1 set x=1 return id";
    error(&invoke(&["run", database.to_str().unwrap(), source]));
    for version in ["0.1", "0.2", "0.3"] {
        error(&invoke(&[
            "run",
            "--language-version",
            version,
            database.to_str().unwrap(),
            source,
        ]));
    }
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(rows(&admin), before);
}

#[test]
fn returning_text_tree_and_prepared_paths_share_images_and_versions() {
    for source in [
        "insert items set id=3,x=30,state=\"ready\" return *",
        "update items where id=1 set x=12 return *",
        "update items where id=1 set x=10 return *",
        "update many items where true set x=x+1 at most 2 return id,x+1 as next",
        "claim items where state=\"ready\" order by id set state=\"claimed\" return id,state",
        "delete items where id=2 return *",
        "delete many items where true at most 2 return *",
    ] {
        let mut outcomes = Vec::new();
        for mode in 0..3 {
            let (_, admin, engine) = fixture();
            let before = rows(&admin);
            let tree = parse_with_version(source, V4).unwrap();
            assert!(matches!(tree, Statement::Returning { .. }));
            let result = with_sql_parser_forbidden(|| match mode {
                0 => engine.execute_text(source, &Bindings::new()),
                1 => engine.execute(&tree, &Bindings::new()),
                _ => {
                    let bound = engine.prepare_bound(&tree, &Bindings::new()).unwrap();
                    assert_eq!(bound.profile(), "embedded-trusted-native-0.4");
                    engine.execute_bound(&bound)
                }
            })
            .unwrap();
            assert!(result.complete);
            assert_eq!(result.rows.len(), result.affected);
            let persisted = rows(&admin);
            let image = if source.starts_with("delete") {
                &before
            } else {
                &persisted
            };
            for row in &result.rows {
                let actual = image
                    .iter()
                    .find(|image| image.values[0] == row.values[0])
                    .unwrap();
                assert_eq!(row.version, actual.version);
                if result.columns == ["id", "next"] {
                    let Value::Int(n) = actual.values[1] else {
                        panic!("integer source")
                    };
                    assert_eq!(row.values, [actual.values[0].clone(), Value::Int(n + 1)]);
                } else if result.columns == ["id", "state"] {
                    assert_eq!(
                        row.values,
                        [actual.values[0].clone(), actual.values[2].clone()]
                    );
                } else {
                    assert_eq!(row, actual);
                }
            }
            if source == "update items where id=1 set x=10 return *" {
                assert_eq!(result.changed, 0);
                assert_eq!(result.rows[0], before[0]);
            }
            outcomes.push((result, persisted));
        }
        assert_eq!(outcomes[0], outcomes[1], "{source}");
        assert_eq!(outcomes[0], outcomes[2], "{source}");
    }
    let (_, admin, engine) = fixture();
    let fields = admin.catalog().unwrap().1[0].columns.clone();
    let mut bindings = Bindings::from([("tag".into(), Value::Text("frozen".into()))]);
    let source = "update items where id=1 set x=x+1 return x as current,x+1 as next,$tag as tag";
    let bound = with_sql_parser_forbidden(|| engine.prepare_bound_text(source, &bindings)).unwrap();
    let columns = &bound.description().columns;
    let mut expected = fields[1].clone();
    expected.name = "current".into();
    assert_eq!(columns[0], expected);
    assert_eq!(
        (columns[1].id, &columns[1].data_type),
        (0, &DataType::BigInt)
    );
    assert_eq!((columns[2].id, &columns[2].data_type), (0, &DataType::Text));
    bindings.insert("tag".into(), Value::Text("new".into()));
    admin
        .sql_session()
        .execute("UPDATE items SET x=40 WHERE id=1", &[], None)
        .unwrap();
    let result = with_sql_parser_forbidden(|| engine.execute_bound(&bound)).unwrap();
    assert_eq!(
        result.rows[0].values,
        [Value::Int(41), Value::Int(42), Value::Text("frozen".into())]
    );
    assert_eq!(result.rows[0].version, rows(&admin)[0].version);
}

#[test]
fn returning_output_errors_and_limits_abort_writes() {
    let (_, admin, engine) = fixture();
    let before = rows(&admin);
    let catalog = admin.catalog().unwrap();
    for (source, expected) in [
        (
            "insert items set id=3,x=30,state=\"ready\" return 1/0 as bad",
            Error::DivisionByZero,
        ),
        (
            "update many items where true set x=x+1 at most 2 return 1/(id-2) as bad",
            Error::DivisionByZero,
        ),
        (
            "delete many items where true at most 2 return 1/(id-2) as bad",
            Error::DivisionByZero,
        ),
        (
            "update items where true set x=1 return 1/0 as bad",
            Error::AmbiguousMatch,
        ),
        (
            "update items where id=1 when false set x=1 return 1/0 as bad",
            Error::ConditionFailed,
        ),
        (
            "update many items where true set x=1 at most 1 return 1/0 as bad",
            Error::TooManyMatches,
        ),
    ] {
        with_sql_parser_forbidden(|| {
            let tree = parse_with_version(source, V4).unwrap();
            let bound = engine.prepare_bound_text(source, &Bindings::new()).unwrap();
            assert_eq!(
                engine.execute_text(source, &Bindings::new()),
                Err(expected.clone()),
                "{source}"
            );
            assert_eq!(
                engine.execute(&tree, &Bindings::new()),
                Err(expected.clone()),
                "{source}"
            );
            assert_eq!(engine.execute_bound(&bound), Err(expected), "{source}");
        });
        assert_eq!(rows(&admin), before);
        assert_eq!(admin.catalog().unwrap(), catalog);
    }
    for source in [
        "delete many items where id=99 at most 0 return 1/0 as bad",
        "claim items where id=99 order by id set state=\"claimed\" return 1/0 as bad",
    ] {
        let result = engine.execute_text(source, &Bindings::new()).unwrap();
        assert_eq!(result.columns, ["bad"]);
        assert!(result.rows.is_empty());
        assert_eq!(result.affected, 0);
        assert!(result.complete);
    }
    for extra in [0, 1] {
        let (_, admin, engine) =
            database("CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,a BYTEA,b BYTEA)");
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,0,$1,$2)",
                &[
                    Value::Bytes(vec![1; 16384]),
                    Value::Bytes(vec![2; 16384 + extra]),
                ],
                None,
            )
            .unwrap();
        let before = rows(&admin);
        let columns = (0..64)
            .map(|i| format!("{} as c{i}", if i == 63 { "b" } else { "a" }))
            .collect::<Vec<_>>()
            .join(",");
        let source = format!("update items where id=1 set n=1 return {columns}");
        with_sql_parser_forbidden(|| {
            let tree = parse_with_version(&source, V4).unwrap();
            let bound = engine
                .prepare_bound_text(&source, &Bindings::new())
                .unwrap();
            if extra == 0 {
                let result = engine.execute_bound(&bound).unwrap();
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0].values.len(), 64);
                assert_eq!(result.changed, 1);
            } else {
                assert_eq!(
                    engine.execute_text(&source, &Bindings::new()),
                    Err(Error::Limit)
                );
                assert_eq!(engine.execute(&tree, &Bindings::new()), Err(Error::Limit));
                assert_eq!(engine.execute_bound(&bound), Err(Error::Limit));
            }
        });
        if extra == 1 {
            assert_eq!(rows(&admin), before);
        } else {
            assert_eq!(rows(&admin)[0].values[1], Value::Int(1));
        }
    }
}

#[test]
fn returning_native_wrapper_admission_and_alias_binding_are_bounded() {
    let (_, admin, engine) = fixture();
    let before = rows(&admin);
    let write = parse_with_version("update items where id=1 set x=1", V4).unwrap();
    let read = parse_with_version("items", V4).unwrap();
    let mut nested = write.clone();
    for _ in 0..MAX_DEPTH + 4 {
        nested = returning(nested, vec![]);
    }
    for invalid in [returning(read, vec![]), nested] {
        assert_eq!(
            engine.prepare_bound(&invalid, &Bindings::new()).err(),
            Some(Error::Unsupported)
        );
        assert_eq!(
            engine.execute(&invalid, &Bindings::new()),
            Err(Error::Unsupported)
        );
    }
    let mut deep = int(1);
    for _ in 0..=MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    for invalid in [
        returning(write.clone(), vec![projection(deep, Some("deep"))]),
        returning(
            write.clone(),
            vec![projection(
                Expr::Value(ValueExpr::Literal(Value::Text("x".repeat(MAX_INPUT)))),
                Some("huge"),
            )],
        ),
        returning(
            write,
            (0..65)
                .map(|i| Projection {
                    expression: field("x"),
                    alias: Some(format!("c{i}")),
                })
                .collect(),
        ),
    ] {
        assert_eq!(
            engine.prepare_bound(&invalid, &Bindings::new()).err(),
            Some(Error::Limit)
        );
        assert_eq!(
            engine.execute(&invalid, &Bindings::new()),
            Err(Error::Limit)
        );
    }
    for source in [
        "delete many items where id=99 at most 0 return null as untyped",
        "update items where id=1 set x=1 return absent",
        "update items where id=1 set x=1 return id as same,x as same",
        "update items where id=1 set x=1 return 1+1",
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
    assert_eq!(rows(&admin), before);
    let bindings = Bindings::from([("p".into(), Value::Text("x".repeat(32763)))]);
    let exact = "update items where id=1 set state=$p return $p as state,\"z\" as tag";
    let over = "update items where id=1 set state=$p return $p as state,\"zz\" as tag";
    assert_eq!(32763 * 2 + 9 + 1, MAX_BOUND_VALUE_BYTES);
    with_sql_parser_forbidden(|| {
        let bound = engine.prepare_bound_text(exact, &bindings).unwrap();
        assert_eq!(engine.execute_bound(&bound).unwrap().changed, 1);
    });
    let before = rows(&admin);
    assert_eq!(
        engine.prepare_bound_text(over, &bindings).err(),
        Some(Error::Limit)
    );
    assert_eq!(engine.execute_text(over, &bindings), Err(Error::Limit));
    assert_eq!(rows(&admin), before);
}

fn initialize_cli(database: &Path) {
    json(&invoke(&[
        "init",
        "--engine",
        "legacy",
        database.to_str().unwrap(),
        "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,state TEXT NOT NULL)",
    ]));
    json(&invoke(&[
        "run",
        database.to_str().unwrap(),
        "insert items set id=1,x=10,state=\"ready\"",
    ]));
}

#[test]
fn returning_cli_uses_explicit_version_four() {
    let database = path();
    initialize_cli(&database);
    let path = database.to_str().unwrap();
    let changed = json(&invoke(&[
        "run",
        "--language-version",
        "0.4",
        path,
        "update items where id=1 set x=x+$delta return id,x",
        r#"{"delta":{"Int":1}}"#,
    ]));
    assert_eq!(changed["columns"], serde_json::json!(["id", "x"]));
    assert_eq!(
        changed["rows"][0]["values"],
        serde_json::json!([{"Int":1},{"Int":11}])
    );
    assert_eq!(changed["changed"], 1);
    let noop = json(&invoke(&[
        "run",
        "--language-version",
        "0.4",
        path,
        "update items where id=1 set x=11 return id,x",
    ]));
    assert_eq!(noop["changed"], 0);
    assert_eq!(noop["rows"], changed["rows"]);
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "0.4",
            path,
            "delete items where id=1 return 1/0 as bad"
        ])),
        Error::DivisionByZero
    );
    let deleted = json(&invoke(&[
        "run",
        "--language-version",
        "0.4",
        path,
        "delete items where id=1 return id,x",
    ]));
    assert_eq!(deleted["rows"], changed["rows"]);
    assert_eq!(json(&invoke(&["run", path, "items"]))["matched"], 0);
    assert_eq!(
        json(&invoke(&["capabilities", "--language-version", "0.4"]))["name"],
        "embedded-trusted-native-0.4"
    );
    let manifest = json(&invoke(&[
        "capability-manifest",
        "--language-version",
        "0.4",
    ]));
    assert_eq!(manifest["limits"]["return_result_bytes"], 1048576);
    assert_eq!(manifest["language_version"], "0.4");
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

#[test]
fn returning_selected_manifest_rejections_use_selected_version() {
    let database = path();
    initialize_cli(&database);
    let manifest = capability_manifest_for(V4);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    let before = rows(&admin);
    let engine = Engine::with_version(admin.clone(), V4);
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
            error(&invoke(&[
                "run",
                "--language-version",
                "0.4",
                database.to_str().unwrap(),
                source,
            ]));
        }
    }
    assert!(count > 0);
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(rows(&admin), before);
}

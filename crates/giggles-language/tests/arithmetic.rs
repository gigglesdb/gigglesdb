use giggles_core::{Admin, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

const V2: LanguageVersion = LanguageVersion::Embedded02;
const V1: LanguageVersion = LanguageVersion::Embedded01;

fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-arithmetic-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let path = path();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    for source in [
        "CREATE TABLE items (id BIGINT PRIMARY KEY, n BIGINT, state TEXT NOT NULL, note TEXT, flag BOOLEAN, payload BYTEA)",
        "INSERT INTO items (id,n,state) VALUES(1,5,'ready')",
        "INSERT INTO items (id,n,state) VALUES(2,-3,'ready')",
        "INSERT INTO items (id,n,state) VALUES(3,NULL,'ready')",
    ] {
        sql.execute(source, &[], None).unwrap();
    }
    (path, admin.clone(), Engine::with_version(admin, V2))
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
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn binary(left: Expr, op: Arithmetic, right: Expr) -> Expr {
    Expr::Arithmetic(Box::new(left), op, Box::new(right))
}
fn equal(left: Expr, right: Expr) -> Expr {
    Expr::Compare(Box::new(left), Compare::Eq, Box::new(right))
}
fn read(filter: Expr) -> Statement {
    Statement::Read {
        table: "items".into(),
        filter: Some(filter),
        order: vec![],
        limit: None,
        projection: vec![],
    }
}
fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(args)
        .output()
        .unwrap()
}
fn output_json(output: &Output) -> serde_json::Value {
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}
fn output_error(output: &Output) -> Error {
    assert!(!output.status.success(), "{output:?}");
    assert!(output.stdout.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    serde_json::from_value(value["error"].clone()).unwrap()
}

#[test]
fn arithmetic_version_gates_cover_text_trees_and_bound_handles() {
    let (_, admin, v2) = fixture();
    let legacy = Engine::new(admin.clone());
    let explicit_v1 = Engine::with_version(admin.clone(), V1);
    let text = "items where n + 1 = 6";
    assert!(parse(text).is_err());
    assert!(parse_with_version(text, V1).is_err());
    let tree = parse_with_version(text, V2).unwrap();
    assert_eq!(
        tree,
        read(equal(binary(field("n"), Arithmetic::Add, int(1)), int(6)))
    );
    let roundtrip: Statement =
        serde_json::from_value(serde_json::to_value(&tree).unwrap()).unwrap();
    let before = rows(&admin);
    with_sql_parser_forbidden(|| {
        for engine in [&legacy, &explicit_v1] {
            assert!(engine.execute_text(text, &Bindings::new()).is_err());
            assert!(engine.prepare_bound_text(text, &Bindings::new()).is_err());
            assert_eq!(
                engine.execute(&roundtrip, &Bindings::new()),
                Err(Error::Unsupported)
            );
            assert_eq!(
                engine.prepare_bound(&roundtrip, &Bindings::new()).err(),
                Some(Error::Unsupported)
            );
        }
        let prepared = v2.prepare_bound_text(text, &Bindings::new()).unwrap();
        assert_eq!(prepared.language_version(), "0.2");
        assert_eq!(prepared.profile(), profile_for(V2).name);
        assert_eq!(v2.execute_bound(&prepared).unwrap().matched, 1);
        assert_eq!(legacy.execute_bound(&prepared), Err(Error::Unsupported));
        let old_handle = legacy
            .prepare_bound_text("items where id=1", &Bindings::new())
            .unwrap();
        assert_eq!(old_handle.language_version(), "0.1");
        assert_eq!(v2.execute_bound(&old_handle), Err(Error::Unsupported));
        assert_eq!(explicit_v1.execute_bound(&old_handle).unwrap().matched, 1);
        // A disabled arithmetic node remains disabled inside an otherwise
        // decisive boolean branch and in both predicate positions.
        let guarded = Statement::Update {
            table: "items".into(),
            filter: equal(field("id"), int(1)),
            guard: Some(Expr::Or(
                Box::new(Expr::Value(ValueExpr::Literal(Value::Bool(true)))),
                Box::new(equal(
                    binary(int(1), Arithmetic::Add, parameter("missing")),
                    int(2),
                )),
            )),
            intent: Intent::One,
            values: vec![(
                "state".into(),
                ValueExpr::Literal(Value::Text("done".into())),
            )],
        };
        assert_eq!(
            legacy.execute(&guarded, &Bindings::new()),
            Err(Error::Unsupported)
        );
        assert_eq!(
            legacy.prepare_bound(&guarded, &Bindings::new()).err(),
            Some(Error::Unsupported)
        );
    });
    assert_eq!(rows(&admin), before);
    assert_eq!(profile().name, profile_for(V1).name);
    assert_eq!(profile_for(V2).name, "embedded-trusted-native-0.2");
    assert!(profile_for(V1).unsupported.contains(&"arithmetic"));
    assert!(!profile_for(V2).unsupported.contains(&"arithmetic"));
    assert_eq!(capability_manifest().language_version, "0.1");
    assert!(capability_manifest().capabilities.iter().any(|c| {
        c.interface == CapabilityInterface::RustTree
            && c.feature == "arithmetic"
            && c.state == CapabilityState::Rejected
    }));
    let manifest = capability_manifest_for(V2);
    assert_eq!(manifest.language_version, "0.2");
    assert!(
        manifest
            .capabilities
            .iter()
            .all(|c| c.language_version == "0.2")
    );
    assert!(
        manifest
            .capabilities
            .iter()
            .any(|c| c.feature == "integer_predicate_arithmetic"
                && c.state == CapabilityState::Implemented)
    );
    assert_eq!(V1.as_str(), "0.1");
    assert_eq!(V2.as_str(), "0.2");
    assert_eq!(serde_json::to_value(V2).unwrap(), "0.2");
    assert_eq!(
        serde_json::from_str::<LanguageVersion>("\"0.1\"").unwrap(),
        V1
    );
    assert!(serde_json::from_str::<LanguageVersion>("\"2.0\"").is_err());
}

#[test]
fn arithmetic_parser_precedence_minimum_and_comment_rules() {
    let (_, _, engine) = fixture();
    let text = "items where 1 + 2 * 3 = 7 or 8 - 4 - 2 = 2 and not 5 % 2 = 0";
    let expected = read(Expr::Or(
        Box::new(equal(
            binary(
                int(1),
                Arithmetic::Add,
                binary(int(2), Arithmetic::Multiply, int(3)),
            ),
            int(7),
        )),
        Box::new(Expr::And(
            Box::new(equal(
                binary(
                    binary(int(8), Arithmetic::Subtract, int(4)),
                    Arithmetic::Subtract,
                    int(2),
                ),
                int(2),
            )),
            Box::new(Expr::Not(Box::new(equal(
                binary(int(5), Arithmetic::Remainder, int(2)),
                int(0),
            )))),
        )),
    ));
    assert_eq!(parse_with_version(text, V2).unwrap(), expected);
    for predicate in [
        "1+2*3=7",
        "(1+2)*3=9",
        "8-4-2=2",
        "24/3/2=4",
        "24/(3*2)=4",
        "-7/3=-2",
        "7/-3=-2",
        "-7%3=-1",
        "7%-3=1",
        "-7%-3=-1",
        "-(2+3)*2=-10",
        "not 1+1=3 and 3*3=9",
        "1 - -2=3",
        "1-- this remains a comment, not two minus operators\n=1",
        "-9223372036854775808=-9223372036854775808",
        "-9223372036854775808 % -1=0",
        "null+1 is null",
        "-null is null",
    ] {
        let text = format!("items where {predicate}");
        assert_eq!(
            engine
                .execute_text(&text, &Bindings::new())
                .unwrap()
                .matched,
            3,
            "{text}"
        );
    }
    for text in [
        "items where 9223372036854775808=0",
        "items where -9223372036854775809=0",
        "items where +1=1",
        "items where 1.5+2=3",
        "items where 2**3=8",
        "items select n+1",
        "items order by n+1",
        "items limit 1+1",
        "insert items set id=9,n=1+2,state=\"ready\"",
        "update items where id=1 set n=n+1",
        "update many items where true set n=1 at most 1+1",
    ] {
        assert!(parse_with_version(text, V2).is_err(), "{text}");
    }
    for (predicate, expected) in [
        ("-(-9223372036854775808)=0", Error::Overflow),
        ("-9223372036854775808 / -1=0", Error::Overflow),
        ("9223372036854775807+1=0", Error::Overflow),
        ("1/0=0", Error::DivisionByZero),
        ("1%0=0", Error::DivisionByZero),
    ] {
        assert_eq!(
            engine.execute_text(&format!("items where {predicate}"), &Bindings::new()),
            Err(expected)
        );
    }
}

#[test]
fn arithmetic_text_tree_and_prepared_execution_match_without_sql() {
    let (_, _, engine) = fixture();
    let text = "items where n * $scale + 1 = $want";
    let tree = read(equal(
        binary(
            binary(field("n"), Arithmetic::Multiply, parameter("scale")),
            Arithmetic::Add,
            int(1),
        ),
        parameter("want"),
    ));
    assert_eq!(parse_with_version(text, V2).unwrap(), tree);
    let mut bindings = Bindings::from([
        ("scale".into(), Value::Int(2)),
        ("want".into(), Value::Int(11)),
    ]);
    with_sql_parser_forbidden(|| {
        let expected = engine.execute_text(text, &bindings).unwrap();
        assert_eq!(expected.matched, 1);
        assert_eq!(expected.rows[0].values[0], Value::Int(1));
        assert_eq!(engine.execute(&tree, &bindings).unwrap(), expected);
        let parsed = engine.prepare_bound_text(text, &bindings).unwrap();
        let typed = engine.prepare_bound(&tree, &bindings).unwrap();
        assert_eq!(engine.execute_bound(&parsed).unwrap(), expected);
        assert_eq!(engine.execute_bound(&typed).unwrap(), expected);
        bindings.insert("want".into(), Value::Int(999));
        assert_eq!(engine.execute_bound(&typed).unwrap(), expected);
        assert_eq!(engine.execute_text(text, &bindings).unwrap().matched, 0);
    });
}

#[test]
fn arithmetic_native_mutations_and_runtime_errors_have_prepared_parity() {
    for source in [
        "update items where id+1=2 when n*2=10 set state=\"done\"",
        "delete items where id*2=4 when n-1=-4",
        "claim items where n+1>0 order by id set n=-2",
    ] {
        let mut outputs = Vec::new();
        for mode in 0..3 {
            let (_, admin, engine) = fixture();
            let tree = parse_with_version(source, V2).unwrap();
            let result = with_sql_parser_forbidden(|| match mode {
                0 => engine.execute_text(source, &Bindings::new()),
                1 => engine.execute(&tree, &Bindings::new()),
                _ => {
                    let bound = engine.prepare_bound(&tree, &Bindings::new()).unwrap();
                    engine.execute_bound(&bound)
                }
            })
            .unwrap();
            assert_eq!((result.matched, result.affected, result.changed), (1, 1, 1));
            if source.starts_with("claim") {
                assert_eq!(result.rows[0], rows(&admin)[0]);
            }
            outputs.push((result, rows(&admin)));
        }
        assert_eq!(outputs[0], outputs[1], "{source}");
        assert_eq!(outputs[0], outputs[2], "{source}");
    }
    let (_, admin, engine) = fixture();
    let before = rows(&admin);
    let catalog = admin.catalog().unwrap();
    for (source, expected) in [
        ("items where false and 1/0=1 limit 0", Error::DivisionByZero),
        (
            "items where true or 9223372036854775807+1=0",
            Error::Overflow,
        ),
        ("items where null+(1/0) is null", Error::DivisionByZero),
        ("items where 1/(id-2)>0 limit 1", Error::DivisionByZero),
        (
            "update items where true when 1/0=1 set state=\"done\"",
            Error::AmbiguousMatch,
        ),
        (
            "update many items where 1/(id-2)>0 set state=\"done\" at most 3",
            Error::DivisionByZero,
        ),
        (
            "claim items where 10/n>0 order by id set n=0",
            Error::DivisionByZero,
        ),
        (
            "claim items where n+1>0 order by id set n=5",
            Error::ClaimNotConsumed,
        ),
    ] {
        with_sql_parser_forbidden(|| {
            let tree = parse_with_version(source, V2).unwrap();
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
        assert_eq!(rows(&admin), before, "{source}");
        assert_eq!(admin.catalog().unwrap(), catalog, "{source}");
    }
}

#[test]
fn arithmetic_nodes_preserve_limits_and_binding_admission() {
    let (_, admin, engine) = fixture();
    let before = rows(&admin);
    let mut deep = int(1);
    for _ in 0..=MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    let huge = Expr::Value(ValueExpr::Literal(Value::Text("x".repeat(MAX_INPUT))));
    for tree in [
        read(equal(deep, int(1))),
        read(equal(binary(huge, Arithmetic::Add, int(1)), int(2))),
    ] {
        assert_eq!(engine.execute(&tree, &Bindings::new()), Err(Error::Limit));
        assert_eq!(
            engine.prepare_bound(&tree, &Bindings::new()).err(),
            Some(Error::Limit)
        );
    }
    let unary_text = format!("items where {}1=1", "- ".repeat(MAX_DEPTH + 2));
    assert_eq!(
        parse_with_version(&unary_text, V2).unwrap_err(),
        Error::Limit
    );
    let oversized = format!("items where \"{}\"+1=1", "x".repeat(MAX_INPUT));
    assert_eq!(
        parse_with_version(&oversized, V2).unwrap_err(),
        Error::Limit
    );
    for (source, bytes) in [
        ("items where $p+1=2", MAX_BINDING_BYTES + 1),
        ("items where $p+$p=2", MAX_BINDING_BYTES / 2 + 1),
    ] {
        let bindings = Bindings::from([("p".into(), Value::Text("x".repeat(bytes)))]);
        let tree = parse_with_version(source, V2).unwrap();
        assert_eq!(engine.execute_text(source, &bindings), Err(Error::Limit));
        assert_eq!(engine.execute(&tree, &bindings), Err(Error::Limit));
        assert_eq!(
            engine.prepare_bound_text(source, &bindings).err(),
            Some(Error::Limit)
        );
        assert_eq!(
            engine.prepare_bound(&tree, &bindings).err(),
            Some(Error::Limit)
        );
    }
    admin
        .sql_session()
        .execute(
            "CREATE TABLE empty_items (id BIGINT PRIMARY KEY, note TEXT)",
            &[],
            None,
        )
        .unwrap();
    for source in [
        "empty_items where note+1=2",
        "empty_items where false and \"x\"*2=0",
        "empty_items where null+true is null",
        "empty_items where -false=0",
    ] {
        assert_eq!(
            engine.execute_text(source, &Bindings::new()),
            Err(Error::Type),
            "{source}"
        );
        assert_eq!(
            engine.prepare_bound_text(source, &Bindings::new()).err(),
            Some(Error::Type),
            "{source}"
        );
    }
    // There is no constant folding that turns a rowless operation into a runtime
    // arithmetic error; binding still checks every expression's scalar type.
    let empty = engine
        .prepare_bound_text("empty_items where 1/0=0", &Bindings::new())
        .unwrap();
    assert_eq!(engine.execute_bound(&empty).unwrap().matched, 0);
    assert_eq!(rows(&admin), before);
}

fn initialize_cli(path: &Path) {
    output_json(&invoke(&[
        "init",
        "--engine",
        "legacy",
        path.to_str().unwrap(),
        "CREATE TABLE items (id BIGINT PRIMARY KEY, n BIGINT, state TEXT NOT NULL)",
    ]));
    output_json(&invoke(&[
        "run",
        path.to_str().unwrap(),
        "insert items set id=1,n=5,state=\"ready\"",
    ]));
}

#[test]
fn arithmetic_cli_version_selection_is_explicit_and_reported() {
    // Incomplete option forms must not fall through to a legacy database path
    // named "--language-version", even if the selected version is invalid.
    for args in [
        vec!["run", "--language-version", "9.9"],
        vec!["run", "--language-version", "9.9", "{}"],
    ] {
        assert_eq!(output_error(&invoke(&args)), Error::Unsupported);
    }
    for args in [
        vec!["run", "--language-version"],
        vec!["run", "--language-version", "0.2"],
        vec!["run", "--language-version", "0.2", "{}"],
    ] {
        assert!(matches!(output_error(&invoke(&args)), Error::Bind(_)));
    }
    let path = path();
    initialize_cli(&path);
    let database = path.to_str().unwrap();
    let source = "items where n+$delta=6";
    let bindings = r#"{"delta":{"Int":1}}"#;
    assert!(matches!(
        output_error(&invoke(&["run", database, source, bindings])),
        Error::Syntax { .. }
    ));
    assert!(matches!(
        output_error(&invoke(&[
            "run",
            "--language-version",
            "0.1",
            database,
            source,
            bindings
        ])),
        Error::Syntax { .. }
    ));
    let result = output_json(&invoke(&[
        "run",
        "--language-version",
        "0.2",
        database,
        source,
        bindings,
    ]));
    assert_eq!(result["matched"], 1);
    assert_eq!(result["rows"][0]["values"][0]["Int"], 1);
    for (command, key) in [("capabilities", "name"), ("capability-manifest", "profile")] {
        assert_eq!(
            output_json(&invoke(&[command]))[key],
            "embedded-trusted-native-0.1"
        );
        assert_eq!(
            output_json(&invoke(&[command, "--language-version", "0.1"]))[key],
            "embedded-trusted-native-0.1"
        );
        assert_eq!(
            output_json(&invoke(&[command, "--language-version", "0.2"]))[key],
            "embedded-trusted-native-0.2"
        );
        assert_eq!(
            output_error(&invoke(&[command, "--language-version", "9.9"])),
            Error::Unsupported
        );
    }
    let absent = path.with_extension("does-not-exist");
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
    assert_eq!(
        output_error(&invoke(&[
            "run",
            "--language-version",
            "0.2",
            database,
            "items where 1/0=0"
        ])),
        Error::DivisionByZero
    );
    assert_eq!(
        output_json(&invoke(&["run", database, "items"]))["rows"][0]["values"][1]["Int"],
        5
    );
}

#[test]
fn arithmetic_selected_manifest_rejections_use_selected_version() {
    let database = path();
    initialize_cli(&database);
    let manifest = capability_manifest_for(V2);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    let before = rows(&admin);
    let engine = Engine::with_version(admin.clone(), V2);
    let mut text_count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::RustText)
    {
        for source in &capability.rejection_examples {
            text_count += 1;
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
    assert!(text_count > 0);
    assert_eq!(rows(&admin), before);
    drop(engine);
    drop(admin);
    let mut cli_count = 0;
    for capability in manifest
        .capabilities
        .iter()
        .filter(|c| c.interface == CapabilityInterface::Cli)
    {
        for source in &capability.rejection_examples {
            cli_count += 1;
            output_error(&invoke(&[
                "run",
                "--language-version",
                "0.2",
                database.to_str().unwrap(),
                source,
            ]));
        }
    }
    assert!(cli_count > 0);
    let admin = Admin::open(database, Limits::default()).unwrap();
    assert_eq!(rows(&admin), before);
}

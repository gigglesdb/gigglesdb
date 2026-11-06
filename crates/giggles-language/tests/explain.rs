use giggles_core::{Admin, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
const V: LanguageVersion = LanguageVersion::Embedded013;
const OLD: [LanguageVersion; 12] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
    LanguageVersion::Embedded06,
    LanguageVersion::Embedded07,
    LanguageVersion::Embedded08,
    LanguageVersion::Embedded09,
    LanguageVersion::Embedded010,
    LanguageVersion::Embedded011,
    LanguageVersion::Embedded012,
];
fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-explain-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn fixture() -> (Admin, Engine) {
    let admin = Admin::create(path(), Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "create table items(id bigint primary key, label text)",
            &[],
            None,
        )
        .unwrap();
    (admin.clone(), Engine::with_version(admin, V))
}
fn bindings() -> Bindings {
    [("id".into(), Value::Int(1)), ("page".into(), Value::Int(7))].into()
}
const READ: &str = "explain items where id=$id order by id desc limit $page select id";
const BAD: &[&str] = &[
    "explain",
    "explain update items where id=1 set label=\"x\"",
    "explain delete items where id=1",
    "explain insert items set id=1",
    "explain transaction { items; }",
    "explain items once \"key\" for 1h",
    "explain analyze items",
    "explain analyze",
    "explain show tables",
    "explain describe table items",
    "explain explain items",
    "explain explain",
    "explain items select id+1 as next",
    "explain items; items",
    "transaction { explain items; }",
];
#[test]
fn explain_text_tree_bindings_and_fresh_catalog() {
    let (admin, engine) = fixture();
    let tree = parse_explain_with_version(READ, V).unwrap();
    assert_eq!(
        parse_input_with_version(READ, V).unwrap(),
        ParsedInput::Explain(tree.clone())
    );
    let before = admin.export(path()).unwrap();
    let outcome = with_sql_parser_forbidden(|| {
        let outcome = engine.explain_text(READ, &bindings()).unwrap();
        assert_eq!(outcome, engine.explain(&tree, &bindings()).unwrap());
        assert!(engine.execute_text(READ, &bindings()).is_err());
        assert!(engine.prepare_bound_text(READ, &bindings()).is_err());
        // Binding does not evaluate arithmetic or logical business rows.
        assert!(
            engine
                .explain_text("explain items where 1/0=1", &Bindings::new())
                .is_ok()
        );
        outcome
    });
    assert_eq!(before, admin.export(path()).unwrap());
    assert_eq!(outcome.table_name, "items");
    assert_eq!(outcome.limits.output_rows, 7);
    assert_eq!(outcome.limits.input_rows, 1024);
    assert_eq!(
        outcome.projection_column_ids,
        vec![outcome.order[0].column_id]
    );
    assert!(outcome.order[0].descending && outcome.has_filter);
    assert_eq!(outcome.estimated_rows, None);
    assert_eq!(outcome.estimated_cost, None);
    let json = serde_json::to_value(&outcome).unwrap();
    assert_eq!(json["database_domain"], outcome.database_domain.to_string());
    for text in ["01", "+1", "-1", "340282366920938463463374607431768211456"] {
        let mut invalid = json.clone();
        invalid["database_domain"] = text.into();
        assert!(serde_json::from_value::<ReadExplainOutcome>(invalid).is_err());
    }
    admin
        .sql_session()
        .execute("create table other(id bigint primary key)", &[], None)
        .unwrap();
    let fresh = engine.explain(&tree, &bindings()).unwrap();
    assert_eq!(outcome.database_domain, fresh.database_domain);
    assert!(fresh.catalog_epoch > outcome.catalog_epoch);
    assert_eq!(fresh.table_id, outcome.table_id);
    assert_eq!(fresh.projection_column_ids, outcome.projection_column_ids);
    let other = engine
        .explain_text("explain other select id", &Bindings::new())
        .unwrap();
    let (_, tables) = admin.catalog().unwrap();
    let catalog = tables.iter().find(|t| t.name == "other").unwrap();
    assert_eq!(other.table_id, catalog.id);
    assert_eq!(other.projection_column_ids, vec![catalog.columns[0].id]);
    assert_ne!(other.table_id, outcome.table_id);
}
#[test]
fn explain_rejections_have_no_effects() {
    let (admin, engine) = fixture();
    let before = admin.export(path()).unwrap();
    with_sql_parser_forbidden(|| {
        for source in BAD {
            assert!(
                engine.explain_text(source, &Bindings::new()).is_err(),
                "{source}"
            );
        }
        for source in ["items select id+1 as next", "insert items set id=1"] {
            let tree = ExplainStatement {
                read: parse_with_version(source, V).unwrap(),
            };
            assert_eq!(
                engine.explain(&tree, &Bindings::new()),
                Err(Error::Unsupported)
            );
        }
        for values in [
            Bindings::new(),
            [
                ("id".into(), Value::Text("x".into())),
                ("page".into(), Value::Int(1)),
            ]
            .into(),
            [
                ("id".into(), Value::Int(1)),
                ("page".into(), Value::Int(1025)),
            ]
            .into(),
            [
                ("id".into(), Value::Int(1)),
                ("page".into(), Value::Int(1)),
                ("extra".into(), Value::Int(1)),
            ]
            .into(),
        ] {
            assert!(engine.explain_text(READ, &values).is_err());
        }
        assert!(
            engine
                .explain_text("explain absent", &Bindings::new())
                .is_err()
        );
        assert!(
            engine
                .explain_text("explain items select absent", &Bindings::new())
                .is_err()
        );
        assert!(
            engine
                .explain_text(
                    &format!("explain {}", "x".repeat(MAX_INPUT)),
                    &Bindings::new()
                )
                .is_err()
        );
    });
    assert_eq!(before, admin.export(path()).unwrap());
}
#[test]
fn explain_preserves_all_older_versions() {
    let (admin, _) = fixture();
    for version in OLD {
        assert_eq!(
            parse_explain_with_version(READ, version),
            Err(Error::Unsupported)
        );
        let engine = Engine::with_version(admin.clone(), version);
        let tree = parse_explain_with_version("explain items", V).unwrap();
        assert_eq!(
            engine.explain(&tree, &Bindings::new()),
            Err(Error::Unsupported)
        );
        for name in ["explain", "analyze"] {
            assert!(
                matches!(parse_input_with_version(name,version).unwrap(),ParsedInput::Statement(Statement::Read{table,..}) if table==name)
            );
            assert!(parse_with_version(&format!("items select {name}"), version).is_ok());
        }
        assert!(
            !capability_manifest_for(version)
                .capabilities
                .iter()
                .any(|c| c.feature == "bounded_simple_read_static_explain")
        );
    }
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    assert_eq!(V.as_str(), "0.13");
}
fn cli(db: &std::path::Path, text: &str, values: Option<&str>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_giggles-language"));
    cmd.args([
        "run",
        "--language-version",
        "0.13",
        db.to_str().unwrap(),
        text,
    ]);
    if let Some(values) = values {
        cmd.arg(values);
    }
    cmd.output().unwrap()
}
#[test]
fn explain_cli_preflight_and_actual_bindings() {
    let absent = path();
    for source in BAD {
        let out = cli(&absent, source, None);
        assert!(!out.status.success(), "{source}");
        assert!(out.stdout.is_empty());
        let value: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
        let error: Error = serde_json::from_value(value["error"].clone()).unwrap();
        assert!(
            !matches!(error, Error::Core(_)),
            "opened database for {source}: {error:?}"
        );
        assert!(!absent.exists());
    }
    let db = path();
    let admin = Admin::create(&db, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "create table items(id bigint primary key, label text)",
            &[],
            None,
        )
        .unwrap();
    drop(admin);
    let values = serde_json::to_string(&bindings()).unwrap();
    let out = cli(&db, READ, Some(&values));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["limits"]["output_rows"], 7);
    assert!(value["database_domain"].is_string());
    assert!(!cli(&db, READ, None).status.success());
    assert!(!cli(&db, READ, Some("{} extra")).status.success());
    for version in OLD {
        let out = Command::new(env!("CARGO_BIN_EXE_giggles-language"))
            .args([
                "run",
                "--language-version",
                version.as_str(),
                absent.to_str().unwrap(),
                "explain items",
            ])
            .output()
            .unwrap();
        assert!(!out.status.success());
    }
}
#[test]
fn explain_capabilities_are_granular() {
    let manifest = capability_manifest_for(V);
    assert_eq!(manifest.language_version, "0.13");
    for interface in [
        CapabilityInterface::RustText,
        CapabilityInterface::RustTree,
        CapabilityInterface::Cli,
    ] {
        let feature = |name| {
            manifest
                .capabilities
                .iter()
                .find(|c| c.interface == interface && c.feature == name)
                .unwrap()
        };
        assert_eq!(
            feature("bounded_simple_read_static_explain").state,
            CapabilityState::Implemented
        );
        for name in [
            "full_reference_explain",
            "local_core_bundle",
            "index_creation",
        ] {
            assert_eq!(feature(name).state, CapabilityState::Unverified);
        }
    }
}

#[test]
fn explain_selected_manifest_rejections_use_selected_version() {
    let database = path();
    let admin = Admin::create(&database, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,state TEXT)",
            &[],
            None,
        )
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES(1,10,'ready')", &[], None)
        .unwrap();
    let engine = Engine::with_version(admin.clone(), V);
    let scope = giggles_language::once::OnceScope {
        database_domain: admin.snapshot().unwrap().domain,
        tenant: 42,
        namespace: "inspection-tests".into(),
    };
    let no_clock =
        || -> std::result::Result<giggles_core::operations::once::OnceTime, giggles_core::operations::once::OnceClockUnavailable> {
            panic!("rejected capability reached publication clock")
        };
    let before = admin.export(path()).unwrap();
    let manifest = capability_manifest_for(V);
    let mut checked = 0;
    for capability in manifest.capabilities.iter().filter(|entry| {
        entry.interface == CapabilityInterface::RustText && entry.state == CapabilityState::Rejected
    }) {
        for source in &capability.rejection_examples {
            checked += 1;
            let rejected =
                with_sql_parser_forbidden(|| match parse_input_with_version(source, V) {
                    Ok(ParsedInput::Statement(statement)) => {
                        engine.execute(&statement, &Bindings::new()).is_err()
                    }
                    Ok(ParsedInput::Transaction(transaction)) => engine
                        .execute_transaction(&transaction, &Bindings::new())
                        .is_err(),
                    Ok(ParsedInput::Catalog(catalog)) => engine.execute_catalog(&catalog).is_err(),
                    Ok(ParsedInput::Once(envelope)) => engine
                        .execute_once(&envelope, &Bindings::new(), &scope, &no_clock)
                        .is_err(),
                    Ok(ParsedInput::Outcome(query)) => {
                        engine.outcome(&query, &Bindings::new(), &scope).is_err()
                    }
                    Ok(ParsedInput::Inspection(inspection)) => engine.inspect(&inspection).is_err(),
                    Ok(ParsedInput::Explain(statement)) => {
                        engine.explain(&statement, &Bindings::new()).is_err()
                    }
                    Err(_) => true,
                });
            assert!(rejected, "{}: {source}", capability.feature);
        }
    }
    assert!(checked > 0);
    assert_eq!(admin.export(path()).unwrap(), before);
    drop(engine);
    drop(admin);
    let mut checked = 0;
    for capability in manifest.capabilities.iter().filter(|entry| {
        entry.interface == CapabilityInterface::Cli && entry.state == CapabilityState::Rejected
    }) {
        for source in &capability.rejection_examples {
            checked += 1;
            assert!(
                !cli(&database, source, None).status.success(),
                "{}: {source}",
                capability.feature
            );
        }
    }
    assert!(checked > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(admin.export(path()).unwrap(), before);
}

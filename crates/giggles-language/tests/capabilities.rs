use giggles_core::{Admin, Limits};
use giggles_language::*;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "giggles-capabilities-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    Admin::create(&path, Limits::default())
        .unwrap()
        .sql_session()
        .execute(
            "create table items (id bigint primary key, state text not null, payload bytea, flag boolean, n smallint)",
            &[],
            None,
        )
        .unwrap();
    path
}

fn execute(
    interface: CapabilityInterface,
    path: &Path,
    text: &str,
    bindings: &Bindings,
) -> Result<Outcome> {
    match interface {
        CapabilityInterface::RustText | CapabilityInterface::RustTree => {
            let engine = Engine::new(Admin::open(path, Limits::default()).unwrap());
            match interface {
                CapabilityInterface::RustText => engine.execute_text(text, bindings),
                CapabilityInterface::RustTree => engine.execute(&parse(text)?, bindings),
                CapabilityInterface::Cli => unreachable!(),
            }
        }
        CapabilityInterface::Cli => {
            let output = Command::new(env!("CARGO_BIN_EXE_giggles-language"))
                .arg("run")
                .arg(path)
                .arg(text)
                .arg(serde_json::to_string(bindings).unwrap())
                .output()
                .unwrap();
            if output.status.success() {
                assert!(output.stderr.is_empty(), "{output:?}");
                Ok(serde_json::from_slice(&output.stdout).unwrap())
            } else {
                assert!(output.stdout.is_empty(), "{output:?}");
                let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
                Err(serde_json::from_value(error["error"].clone()).unwrap())
            }
        }
    }
}

#[test]
fn advertised_operations_execute_through_each_interface() {
    let manifest = capability_manifest();
    for interface in [
        CapabilityInterface::RustText,
        CapabilityInterface::RustTree,
        CapabilityInterface::Cli,
    ] {
        let path = fixture();
        let run = |text: &str| execute(interface, &path, text, &Bindings::new());
        let claims = [
            "paged_reads",
            "typed_bindings",
            "literal_insert",
            "guarded_singular_writes",
            "atomic_bulk_caps",
            "consuming_claim",
            "native_no_op_counts",
        ];
        for feature in claims {
            assert!(
                manifest.capabilities.iter().any(|entry| {
                    entry.interface == interface
                        && entry.feature == feature
                        && entry.state == CapabilityState::Implemented
                }),
                "missing {interface:?} {feature}"
            );
        }
        let bindings = Bindings::from([
            ("id".into(), Value::Int(1)),
            ("state".into(), Value::Text("ready".into())),
            ("payload".into(), Value::Bytes(vec![0, 255])),
            ("flag".into(), Value::Bool(true)),
            ("n".into(), Value::Int(32767)),
        ]);
        assert_eq!(
            execute(
                interface,
                &path,
                "insert items set id=$id,state=$state,payload=$payload,flag=$flag,n=$n",
                &bindings
            )
            .unwrap()
            .changed,
            1
        );
        assert_eq!(
            run("insert items set id=2,state=\"ready\"")
                .unwrap()
                .changed,
            1
        );
        let bytes = Bindings::from([("payload".into(), Value::Bytes(vec![0, 255]))]);
        assert_eq!(
            execute(interface, &path, "items where payload=$payload", &bytes)
                .unwrap()
                .matched,
            1
        );
        assert_eq!(run("items where payload is null").unwrap().matched, 1);
        let page = run("items order by id desc limit 1 select id").unwrap();
        assert_eq!(page.rows[0].values, [Value::Int(2)]);
        assert_eq!(page.matched, 2);
        assert!(!page.complete);

        let before = run("items").unwrap();
        assert_eq!(
            run("update items where true when false set state=\"done\""),
            Err(Error::AmbiguousMatch)
        );
        assert_eq!(
            run("update many items where true set state=\"done\" at most 1"),
            Err(Error::TooManyMatches)
        );
        assert_eq!(run("items").unwrap(), before);
        let no_op = run("update items where id=1 set state=\"ready\"").unwrap();
        assert_eq!((no_op.matched, no_op.affected, no_op.changed), (1, 1, 0));
        assert_eq!(run("items").unwrap(), before);
        let claim =
            run("claim items where state=\"ready\" order by id set state=\"taken\"").unwrap();
        assert_eq!(claim.rows[0].values[0], Value::Int(1));
        assert_eq!(claim.rows[0].values[1], Value::Text("taken".into()));
        assert_ne!(claim.rows[0].version, before.rows[0].version);
        assert_eq!(
            run("update items where id=2 when state=\"ready\" set state=\"done\"")
                .unwrap()
                .changed,
            1
        );
        let deleted = run("delete many items where true at most 2").unwrap();
        assert_eq!(
            (deleted.matched, deleted.affected, deleted.changed),
            (2, 2, 2)
        );
        assert!(run("items").unwrap().rows.is_empty());
    }
}

#[test]
fn rejected_capabilities_fail_before_effects() {
    let manifest = capability_manifest();
    for interface in [CapabilityInterface::RustText, CapabilityInterface::Cli] {
        let path = fixture();
        let run = |text: &str| execute(interface, &path, text, &Bindings::new());
        run("insert items set id=1,state=\"ready\"").unwrap();
        let before = run("items").unwrap();
        for entry in manifest.capabilities.iter().filter(|entry| {
            entry.interface == interface && entry.state == CapabilityState::Rejected
        }) {
            assert!(
                !entry.rejection_examples.is_empty(),
                "{} has no fixture",
                entry.feature
            );
            for example in &entry.rejection_examples {
                assert!(
                    run(example).is_err(),
                    "accepted {}: {example}",
                    entry.feature
                );
                assert_eq!(
                    run("items").unwrap(),
                    before,
                    "effects from {}",
                    entry.feature
                );
            }
        }
    }
}

#[test]
fn unsupported_tree_combination_has_zero_effects() {
    let path = fixture();
    let engine = Engine::new(Admin::open(path, Limits::default()).unwrap());
    engine
        .execute_text("insert items set id=1,state=\"ready\"", &Bindings::new())
        .unwrap();
    let before = engine.execute_text("items", &Bindings::new()).unwrap();
    let filter = Expr::Value(ValueExpr::Literal(Value::Bool(true)));
    let unsupported = Statement::Update {
        table: "items".into(),
        filter: filter.clone(),
        guard: Some(filter.clone()),
        values: vec![(
            "state".into(),
            ValueExpr::Literal(Value::Text("done".into())),
        )],
        intent: Intent::Many {
            cap: ValueExpr::Literal(Value::Int(1)),
        },
    };
    assert_eq!(
        engine.execute(&unsupported, &Bindings::new()),
        Err(Error::Unsupported)
    );
    let unsupported = Statement::Delete {
        table: "items".into(),
        filter: filter.clone(),
        guard: Some(filter),
        cap: Some(ValueExpr::Literal(Value::Int(1))),
    };
    assert_eq!(
        engine.execute(&unsupported, &Bindings::new()),
        Err(Error::Unsupported)
    );
    assert_eq!(
        engine.execute_text("items", &Bindings::new()).unwrap(),
        before
    );
}

#[test]
fn advertised_limits_are_enforced() {
    let manifest = capability_manifest();
    let limits = manifest.limits;
    assert_eq!(parse(&" ".repeat(limits.text_bytes + 1)), Err(Error::Limit));
    assert_eq!(
        parse(&"x ".repeat(limits.text_tokens + 1)),
        Err(Error::Limit)
    );
    assert_eq!(
        parse(&format!(
            "items where {}true",
            "not ".repeat(limits.expression_depth + 1)
        )),
        Err(Error::Limit)
    );
    let path = fixture();
    let admin = Admin::open(path, Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    assert_eq!(
        engine.execute_text(
            &format!("items limit {}", limits.result_page_rows + 1),
            &Bindings::new()
        ),
        Err(Error::Type)
    );
    let tree = Statement::Insert {
        table: "items".into(),
        values: vec![(
            "state".into(),
            ValueExpr::Literal(Value::Text("x".repeat(limits.serialized_tree_bytes))),
        )],
    };
    assert_eq!(engine.execute(&tree, &Bindings::new()), Err(Error::Limit));

    let mut sessions = Vec::new();
    for _ in 0..limits.concurrent_explicit_sessions {
        let mut session = admin.sql_session();
        session.execute("begin", &[], None).unwrap();
        sessions.push(session);
    }
    assert!(
        matches!(engine.execute_text("items", &Bindings::new()), Err(Error::Core(message)) if message == "Busy")
    );
    drop(sessions);
    assert!(
        engine
            .execute_text("items", &Bindings::new())
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn capability_evidence_points_to_executable_repository_tests() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut keys = BTreeSet::new();
    for version in [
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
    ] {
        let manifest = capability_manifest_for(version);
        for entry in &manifest.capabilities {
            assert!(
                keys.insert((
                    entry.language_version,
                    entry.profile,
                    entry.interface,
                    entry.deployment,
                    entry.feature
                )),
                "duplicate capability key"
            );
            assert_eq!(entry.language_version, manifest.language_version);
            assert_eq!(entry.profile, profile_for(version).name);
            if entry.state == CapabilityState::Unverified {
                assert!(entry.evidence.is_empty());
            } else {
                assert!(
                    !entry.evidence.is_empty(),
                    "{} has no evidence",
                    entry.feature
                );
            }
            for evidence in &entry.evidence {
                assert!(!Path::new(evidence.path).is_absolute());
                assert!(!evidence.path.contains(".."));
                let source = std::fs::read_to_string(root.join(evidence.path)).unwrap();
                let declaration = format!("fn {}(", evidence.test);
                let offset = source
                    .find(&declaration)
                    .unwrap_or_else(|| panic!("missing {} in {}", evidence.test, evidence.path));
                let previous = &source[..offset];
                assert!(
                    previous.trim_end().ends_with("#[test]"),
                    "{} is not a test",
                    evidence.test
                );
            }
        }
        let references: BTreeSet<_> = manifest
            .references
            .iter()
            .map(|reference| reference.supporting_doc_id)
            .collect();
        assert_eq!(references.len(), 6);
        for reference in &manifest.references {
            assert!(reference.revision > 0);
            assert!(
                reference
                    .url
                    .ends_with(&format!("?revision={}", reference.revision))
            );
        }
    }
}

#[test]
fn manifest_reference_pins_match_executable_corpus() {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/reference_examples.json")).unwrap();
    let corpus_pins: BTreeSet<_> = corpus["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|doc| {
            (
                doc["id"].as_str().unwrap().to_owned(),
                doc["revision"].as_u64().unwrap(),
            )
        })
        .collect();
    let manifest_pins: BTreeSet<_> = capability_manifest()
        .references
        .into_iter()
        .map(|reference| {
            (
                reference.supporting_doc_id.to_owned(),
                u64::from(reference.revision),
            )
        })
        .collect();
    assert_eq!(manifest_pins, corpus_pins);
}

#[test]
fn cli_manifest_is_separate_from_legacy_capabilities() {
    let invoke = |command| {
        let output = Command::new(env!("CARGO_BIN_EXE_giggles-language"))
            .arg(command)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    let legacy = invoke("capabilities");
    assert_eq!(legacy, serde_json::to_value(profile()).unwrap());
    assert!(legacy.get("manifest_version").is_none());
    let manifest = invoke("capability-manifest");
    assert_eq!(manifest["manifest_version"], 1);
    assert_eq!(
        manifest,
        serde_json::to_value(capability_manifest()).unwrap()
    );
    assert!(
        manifest["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["state"] == "unverified")
    );
}

#[test]
fn qualified_rejection_examples_fail() {
    use giggles_core::relational::with_sql_parser_forbidden;

    let path = fixture();
    let admin = Admin::open(&path, Limits::default()).unwrap();
    for sql in [
        "create schema app",
        "create table app.items(id bigint primary key)",
        "insert into app.items values(1)",
    ] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    let snapshot = || {
        let mut session = admin.sql_session();
        let m = session.catalog_metadata().unwrap();
        let catalog = format!("{:?}", (&m.tables, &m.indexes, &m.schemas,
            &m.presentation, &m.types, &m.typmods, m.generation));
        let rows = ["items", "app.items"].map(|table| {
            session.execute(&format!("select * from {table} order by id"), &[], None)
                .unwrap().rows.into_iter().map(|row| row.values)
                .collect::<Vec<_>>()
        });
        (catalog, rows)
    };
    let before = snapshot();
    for (version, offset) in [
        (LanguageVersion::Embedded018, 18),
        (LanguageVersion::Embedded019, 26),
        (LanguageVersion::Embedded020, 26),
    ] {
        let engine = Engine::with_version(admin.clone(), version);
        let entries = capability_manifest_for(version).capabilities.into_iter()
            .filter(|entry| entry.feature == "general_qualified_names")
            .collect::<Vec<_>>();
        assert!(!entries.is_empty());
        for entry in entries {
            assert_eq!(entry.state, CapabilityState::Rejected);
            assert!(!entry.rejection_examples.is_empty());
            for text in entry.rejection_examples {
                let expected = Error::Syntax { offset };
                assert_eq!(with_sql_parser_forbidden(|| parse_input_with_version(text, version))
                    .unwrap_err(), expected, "{version:?}: {text}");
                assert_eq!(with_sql_parser_forbidden(|| engine.execute_text(text, &Bindings::new())),
                    Err(expected), "{version:?}: {text}");
                assert_eq!(snapshot(), before, "effects from {version:?}: {text}");
            }
        }
    }
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

use giggles_core::{
    Admin, Limits, operations::once as core_once, relational::with_sql_parser_forbidden,
};
use giggles_language::{
    Bindings, CapabilityInterface, CapabilityState, Engine, Error, LanguageVersion, OnceScope,
    Outcome, ParsedInput, capability_manifest_for, parse_input_with_version, profile_for,
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

const VERSION: LanguageVersion = LanguageVersion::Embedded011;
const EARLIER: [LanguageVersion; 10] = [
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
];

fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-once-capabilities-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn invoke(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(arguments)
        .output()
        .unwrap()
}
fn json(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn error(output: &Output) -> Error {
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "failed command disclosed success output"
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    serde_json::from_value(value["error"].clone()).unwrap()
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(path()).unwrap()
}

#[test]
fn once_cli_refuses_retry_context_before_open() {
    let sources = [
        "insert items set id=3,state=\"ready\" once \"request-3\" for 1h",
        "insert many into items(id) values(3),(4) at most 2 once \"batch\" for duration(\"1h\") return id",
        "transaction once \"block\" for 1h { insert items set id=3; }",
        "outcome \"request-3\"",
        "outcome $key",
    ];
    for source in sources {
        assert!(matches!(
            parse_input_with_version(source, VERSION).unwrap(),
            ParsedInput::Once(_) | ParsedInput::Outcome(_)
        ));
        let absent = path();
        assert_eq!(
            error(&invoke(&[
                "run",
                "--language-version",
                "0.11",
                absent.to_str().unwrap(),
                source,
            ])),
            Error::Unsupported,
            "{source}"
        );
        assert!(!absent.exists());
        for version in EARLIER {
            let failure = error(&invoke(&[
                "run",
                "--language-version",
                version.as_str(),
                absent.to_str().unwrap(),
                source,
            ]));
            assert!(
                !matches!(failure, Error::Core(_)),
                "opened path for {version:?}: {source}"
            );
        }
        assert!(!absent.exists());
    }

    // Holding Admin keeps the database locked. Unsupported must win over opening
    // that lock, and refusal cannot change either data or durable metadata.
    let database = path();
    let admin = Admin::create(&database, Limits::default()).unwrap();
    let before = digest(&admin);
    for source in sources {
        assert_eq!(
            error(&invoke(&[
                "run",
                "--language-version",
                "0.11",
                database.to_str().unwrap(),
                source,
            ])),
            Error::Unsupported,
            "{source}"
        );
    }
    assert_eq!(digest(&admin), before);
}

#[test]
fn once_cli_version_selection_keeps_ordinary_commands_and_defaults() {
    let database = path();
    let db = database.to_str().unwrap();
    json(&invoke(&["init", db]));
    let catalog = json(&invoke(&[
        "run",
        "--language-version",
        "0.11",
        db,
        "create table items(id int64,n int64,state nullable text,primary key(id))",
    ]));
    assert_eq!(catalog["table"]["name"], "items");
    let out: Outcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.11",
        db,
        "insert into items(id,n) values(1,case when true then abs(-7) else 0 end) return id,n",
    ])))
    .unwrap();
    assert_eq!(out.changed, 1);
    assert_eq!(
        out.rows[0].values,
        [
            giggles_language::Value::Int(1),
            giggles_language::Value::Int(7)
        ]
    );
    let out: Outcome = serde_json::from_value(json(&invoke(&[
        "run",
        "--language-version",
        "0.11",
        db,
        "insert into items(id,n) values(1,99) on conflict(id) do nothing return id",
    ])))
    .unwrap();
    assert_eq!((out.affected, out.changed), (0, 0));
    let transaction = json(&invoke(&[
        "run",
        "--language-version",
        "0.11",
        db,
        "transaction read only { let selected=(items); emit @selected.count; }",
    ]));
    assert_eq!(transaction["emitted"][0]["Scalar"]["Int"], 1);
    let ordinary: Outcome = serde_json::from_value(json(&invoke(&["run", db, "items"]))).unwrap();
    assert_eq!(ordinary.rows.len(), 1);

    let default = json(&invoke(&["capabilities"]));
    assert_eq!(default["name"], "embedded-trusted-native-0.1");
    assert_eq!(default["automatic_retries"], false);
    assert!(
        default["unsupported"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("once"))
    );
    let selected = json(&invoke(&["capabilities", "--language-version", "0.11"]));
    assert_eq!(selected["name"], "embedded-trusted-native-0.11");
    assert_eq!(selected["automatic_retries"], false);
    assert_eq!(selected["trusted_admin"], true);
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.11"
        ])),
        serde_json::to_value(capability_manifest_for(VERSION)).unwrap()
    );
    let absent = path();
    for arguments in [
        vec![
            "run",
            "--language-version",
            "9.9",
            absent.to_str().unwrap(),
            "items",
        ],
        vec!["run", "--language-version", "9.9"],
    ] {
        assert_eq!(error(&invoke(&arguments)), Error::Unsupported);
    }
    assert!(!absent.exists());
}

#[test]
fn once_manifest_distinguishes_interfaces_and_bounded_contract() {
    let manifest = capability_manifest_for(VERSION);
    let limits = &manifest.limits;
    assert_eq!(limits.once_key_bytes, Some(core_once::MAX_ONCE_KEY_BYTES));
    assert_eq!(
        limits.once_namespace_bytes,
        Some(core_once::MAX_ONCE_NAMESPACE_BYTES)
    );
    assert_eq!(
        limits.once_canonical_intent_bytes,
        Some(core_once::MAX_ONCE_INTENT_BYTES)
    );
    assert_eq!(
        limits.once_encoded_result_bytes,
        Some(core_once::MAX_ONCE_RESULT_BYTES)
    );
    assert_eq!(limits.once_retained_keys, Some(core_once::MAX_ONCE_RECORDS));
    assert_eq!(
        limits.once_ledger_bytes,
        Some(core_once::MAX_ONCE_LEDGER_BYTES)
    );
    assert_eq!(limits.once_min_retention_seconds, Some(1));
    assert_eq!(
        limits.once_max_retention_seconds,
        Some(core_once::MAX_ONCE_RETENTION_SECONDS)
    );
    assert_eq!(
        limits.once_max_clock_lease_seconds,
        Some(core_once::MAX_ONCE_CLOCK_LEASE_SECONDS)
    );
    for version in EARLIER {
        let previous = serde_json::to_value(capability_manifest_for(version)).unwrap();
        assert!(
            previous["limits"]
                .as_object()
                .unwrap()
                .keys()
                .all(|key| !key.starts_with("once_"))
        );
    }
    for feature in ["once", "receipts_and_outcome_lookup"] {
        for interface in [
            CapabilityInterface::RustText,
            CapabilityInterface::RustTree,
            CapabilityInterface::Cli,
        ] {
            let entry = manifest
                .capabilities
                .iter()
                .find(|entry| entry.feature == feature && entry.interface == interface)
                .unwrap();
            assert_eq!(
                entry.state,
                if interface == CapabilityInterface::Cli {
                    CapabilityState::Rejected
                } else {
                    CapabilityState::Implemented
                }
            );
            if interface == CapabilityInterface::Cli {
                assert!(!entry.rejection_examples.is_empty());
            }
        }
    }

    // Also verify new evidence names and the complete selected inventory, without
    // changing the frozen 0.1 corpus or older profile tests.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut keys = BTreeSet::new();
    for entry in &manifest.capabilities {
        assert!(
            keys.insert((entry.interface, entry.feature)),
            "duplicate capability key"
        );
        assert_eq!(entry.language_version, "0.11");
        assert_eq!(entry.profile, profile_for(VERSION).name);
        if entry.state == CapabilityState::Unverified {
            assert!(entry.evidence.is_empty());
        } else {
            assert!(!entry.evidence.is_empty());
        }
        for evidence in &entry.evidence {
            assert!(!Path::new(evidence.path).is_absolute());
            assert!(!evidence.path.contains(".."));
            let source = std::fs::read_to_string(root.join(evidence.path)).unwrap();
            let declaration = format!("fn {}(", evidence.test);
            let index = source
                .find(&declaration)
                .unwrap_or_else(|| panic!("missing {} in {}", evidence.test, evidence.path));
            assert!(source[..index].trim_end().ends_with("#[test]"));
        }
    }
}

#[test]
fn once_selected_manifest_rejections_use_selected_version() {
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
    let engine = Engine::with_version(admin.clone(), VERSION);
    let scope = OnceScope {
        database_domain: admin.snapshot().unwrap().domain,
        tenant: 42,
        namespace: "capability-tests".into(),
    };
    let no_clock =
        || -> std::result::Result<core_once::OnceTime, core_once::OnceClockUnavailable> {
            panic!("rejected capability reached publication clock")
        };
    let before = digest(&admin);
    let manifest = capability_manifest_for(VERSION);
    let mut checked = 0;
    for capability in manifest.capabilities.iter().filter(|entry| {
        entry.interface == CapabilityInterface::RustText && entry.state == CapabilityState::Rejected
    }) {
        for source in &capability.rejection_examples {
            checked += 1;
            let rejected =
                with_sql_parser_forbidden(|| match parse_input_with_version(source, VERSION) {
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
                    Ok(ParsedInput::Inspection(_)) => {
                        panic!("inspection accepted by profile 0.11: {source}")
                    }
                    Err(_) => true,
                });
            assert!(rejected, "{}: {source}", capability.feature);
        }
    }
    assert!(checked > 0);
    assert_eq!(digest(&admin), before);
    drop(engine);
    drop(admin);
    let mut checked = 0;
    for capability in manifest.capabilities.iter().filter(|entry| {
        entry.interface == CapabilityInterface::Cli && entry.state == CapabilityState::Rejected
    }) {
        for source in &capability.rejection_examples {
            checked += 1;
            error(&invoke(&[
                "run",
                "--language-version",
                "0.11",
                database.to_str().unwrap(),
                source,
            ]));
        }
    }
    assert!(checked > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

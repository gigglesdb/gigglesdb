use giggles_core::{
    Admin, Limits,
    operations::{MAX_INSPECTION_METADATA_BYTES, once as core_once},
    relational::{DataType, with_sql_parser_forbidden},
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

const VERSION: LanguageVersion = LanguageVersion::Embedded012;
const EARLIER: [LanguageVersion; 11] = [
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
];

fn path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "giggles-native-inspection-capabilities-{}-{}-{}.db",
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
fn run(db: &Path, source: &str) -> Output {
    invoke(&[
        "run",
        "--language-version",
        "0.12",
        db.to_str().unwrap(),
        source,
    ])
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(path()).unwrap()
}

#[test]
fn inspection_cli_local_discovery_precedes_database_open() {
    let manifest = serde_json::to_value(capability_manifest_for(VERSION)).unwrap();
    for (source, variant, expected) in [
        ("show capabilities", "Capabilities", manifest.clone()),
        ("show limits", "Limits", manifest["limits"].clone()),
    ] {
        let absent = path();
        let result = json(&run(&absent, source));
        assert_eq!(result.as_object().unwrap().len(), 1);
        assert_eq!(result[variant], expected);
        assert!(!absent.exists());

        let locked = path();
        let admin = Admin::create(&locked, Limits::default()).unwrap();
        let before = digest(&admin);
        assert_eq!(json(&run(&locked, source)), result);
        assert_eq!(digest(&admin), before);
    }

    let absent = path();
    let locked = path();
    let admin = Admin::create(&locked, Limits::default()).unwrap();
    let before = digest(&admin);
    for source in [
        "show capabilities",
        "show limits",
        "show tables",
        "describe table items",
    ] {
        for database in [&absent, &locked] {
            assert_eq!(
                error(&invoke(&[
                    "run",
                    "--language-version",
                    "0.12",
                    database.to_str().unwrap(),
                    source,
                    "{\"unused\":{\"Int\":1}}",
                ])),
                Error::Bind("inspection bindings".into()),
                "{source}"
            );
        }
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
    assert_eq!(digest(&admin), before);
    // Catalog commands really require a database, unlike local discovery.
    assert!(matches!(
        error(&run(&absent, "show tables")),
        Error::Core(_)
    ));
    assert!(matches!(
        error(&run(&absent, "describe table items")),
        Error::Core(_)
    ));
    assert!(!absent.exists());
}

#[test]
fn inspection_cli_catalog_and_version_selection_are_explicit() {
    let database = path();
    let admin = Admin::create(&database, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    for statement in [
        "CREATE TABLE zeta(id BIGINT PRIMARY KEY)",
        "CREATE TABLE items(tenant BIGINT,id SMALLINT,n INTEGER,state TEXT,PRIMARY KEY(tenant,id))",
        "CREATE TABLE describe(id BIGINT PRIMARY KEY)",
        "INSERT INTO describe VALUES(1)",
        "CREATE TABLE show(id BIGINT PRIMARY KEY)",
        "INSERT INTO show VALUES(1)",
    ] {
        sql.execute(statement, &[], None).unwrap();
    }
    drop(sql);
    let (epoch, tables) = admin.catalog().unwrap();
    let domain = admin.snapshot().unwrap().domain;
    let expected = tables.iter().find(|table| table.name == "items").unwrap();
    assert_eq!(expected.columns[1].data_type, DataType::SmallInt);
    let expected = serde_json::to_value(expected).unwrap();
    let expected_summaries: Vec<_> = tables
        .iter()
        .map(|table| {
            serde_json::json!({
                "id": table.id,
                "name": table.name,
                "column_count": table.columns.len(),
                "primary_key_columns": table.primary_key.len(),
            })
        })
        .collect();
    let before = digest(&admin);
    drop(admin);
    let listed = json(&run(&database, "show tables"));
    let described = json(&run(&database, "describe table items"));
    for output in [&listed, &described] {
        assert_eq!(output["Catalog"]["database_domain"], domain.to_string());
        assert_eq!(output["Catalog"]["catalog_epoch"], epoch);
        assert_eq!(output["Catalog"]["complete"], true);
        assert!(output["Catalog"]["contract_version"].as_u64().unwrap() > 0);
    }
    assert_eq!(
        listed["Catalog"]["result"]["Tables"],
        serde_json::json!(expected_summaries)
    );
    assert_eq!(described["Catalog"]["result"]["Table"], expected);
    assert_eq!(json(&run(&database, "describe table `items`")), described);
    assert_eq!(
        error(&run(&database, "describe table missing")),
        Error::Bind("table".into())
    );
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
    drop(admin);

    // Ordinary contextual names execute in every profile, including the default.
    let ordinary: Outcome =
        serde_json::from_value(json(&run(&database, "describe where id=1"))).unwrap();
    assert_eq!(ordinary.rows.len(), 1);
    for version in EARLIER.into_iter().chain([VERSION]) {
        let selected: Outcome = serde_json::from_value(json(&invoke(&[
            "run",
            "--language-version",
            version.as_str(),
            database.to_str().unwrap(),
            "describe where id=1",
        ])))
        .unwrap();
        assert_eq!(selected.rows, ordinary.rows);
    }
    let default: Outcome = serde_json::from_value(json(&invoke(&[
        "run",
        database.to_str().unwrap(),
        "describe where id=1",
    ])))
    .unwrap();
    assert_eq!(default.rows, ordinary.rows);
    let selected_show: Outcome =
        serde_json::from_value(json(&run(&database, "show where id=1"))).unwrap();
    assert_eq!(selected_show.rows.len(), 1);
    assert_eq!(selected_show.rows[0].values, ordinary.rows[0].values);
    error(&invoke(&[
        "run",
        database.to_str().unwrap(),
        "show where id=1",
    ]));
    let out: Outcome = serde_json::from_value(json(&run(
        &database,
        "insert into zeta(id) values(abs(-1)) on conflict(id) do nothing return id",
    )))
    .unwrap();
    assert_eq!(out.changed, 1);
    let read = json(&run(
        &database,
        "transaction read only { let selected=(zeta); emit @selected.count; }",
    ));
    assert_eq!(read["emitted"][0]["Scalar"]["Int"], 1);

    let absent = path();
    for source in [
        "insert zeta set id=2 once \"key\" for 1h",
        "transaction once \"key\" for 1h { insert zeta set id=2; }",
        "outcome \"key\"",
    ] {
        assert_eq!(error(&run(&absent, source)), Error::Unsupported);
    }
    assert_eq!(
        error(&invoke(&[
            "run",
            "--language-version",
            "9.9",
            absent.to_str().unwrap(),
            "show capabilities"
        ])),
        Error::Unsupported
    );
    assert_eq!(
        error(&invoke(&["run", "--language-version", "9.9"])),
        Error::Unsupported
    );
    assert!(!absent.exists());
    assert_eq!(
        json(&invoke(&["capabilities"]))["name"],
        "embedded-trusted-native-0.1"
    );
    let selected = json(&invoke(&["capabilities", "--language-version", "0.12"]));
    assert_eq!(selected["name"], "embedded-trusted-native-0.12");
    assert_eq!(selected["trusted_admin"], true);
    assert_eq!(selected["automatic_retries"], false);
}

#[test]
fn inspection_manifest_reports_only_finite_trusted_metadata() {
    let manifest = capability_manifest_for(VERSION);
    assert_eq!(
        manifest.limits.inspection_metadata_bytes,
        Some(MAX_INSPECTION_METADATA_BYTES)
    );
    assert_eq!(MAX_INSPECTION_METADATA_BYTES, 65536);
    for version in EARLIER {
        let previous = serde_json::to_value(capability_manifest_for(version)).unwrap();
        assert!(
            previous["limits"]
                .get("inspection_metadata_bytes")
                .is_none()
        );
    }
    for feature in ["local_discovery", "catalog_inspection"] {
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
            assert_eq!(entry.state, CapabilityState::Implemented);
        }
    }
    for feature in ["once", "receipts_and_outcome_lookup"] {
        let cli = manifest
            .capabilities
            .iter()
            .find(|entry| entry.feature == feature && entry.interface == CapabilityInterface::Cli)
            .unwrap();
        assert_eq!(cli.state, CapabilityState::Rejected);
    }
    for entry in &manifest.capabilities {
        if [
            "task_authorization",
            "schema_jobs",
            "inspection_scope",
            "inspection_names",
        ]
        .contains(&entry.feature)
        {
            assert_ne!(entry.state, CapabilityState::Implemented);
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut keys = BTreeSet::new();
    for entry in &manifest.capabilities {
        assert!(
            keys.insert((entry.interface, entry.feature)),
            "duplicate capability key"
        );
        assert_eq!(entry.language_version, "0.12");
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
    assert_eq!(
        json(&invoke(&[
            "capability-manifest",
            "--language-version",
            "0.12"
        ])),
        serde_json::to_value(manifest).unwrap()
    );
}

#[test]
fn inspection_selected_manifest_rejections_use_selected_version() {
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
        namespace: "inspection-tests".into(),
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
                    Ok(ParsedInput::Inspection(inspection)) => engine.inspect(&inspection).is_err(),
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
            error(&run(&database, source));
        }
    }
    assert!(checked > 0);
    let admin = Admin::open(&database, Limits::default()).unwrap();
    assert_eq!(digest(&admin), before);
}

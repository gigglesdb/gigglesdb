use giggles_core::{
    Admin, Limits,
    operations::{InspectionResult, TableSummary},
    relational::{DataType, with_sql_parser_forbidden},
};
use giggles_language::{
    once::{OnceClockUnavailable, OnceStatus, OnceTime},
    *,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const VERSION: LanguageVersion = LanguageVersion::Embedded012;
const OLDER: [LanguageVersion; 11] = [
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
        "giggles-native-inspection-{}-{}-{}",
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
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE zeta(tenant INTEGER,id BIGINT,narrow SMALLINT,flag BOOLEAN,txt TEXT,raw BYTES,PRIMARY KEY(tenant,id))",
        &[],
        None,
    )
    .unwrap();
    sql.execute("CREATE TABLE alpha(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    sql.execute("CREATE TABLE describe(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    sql.execute("INSERT INTO describe VALUES(1)", &[], None)
        .unwrap();
    (admin.clone(), Engine::with_version(admin, VERSION))
}
fn json(result: InspectionOutcome) -> serde_json::Value {
    serde_json::to_value(result).unwrap()
}
fn catalog(result: InspectionOutcome) -> giggles_core::operations::InspectionOutcome {
    let InspectionOutcome::Catalog(result) = result else {
        panic!("catalog metadata expected")
    };
    result
}

#[test]
fn inspection_version_gates_and_contextual_names_preserve_legacy() {
    let (admin, engine) = fixture();
    let commands = [
        ("show capabilities", InspectionStatement::ShowCapabilities),
        ("show limits", InspectionStatement::ShowLimits),
        ("show tables", InspectionStatement::ShowTables),
        (
            "describe table zeta",
            InspectionStatement::DescribeTable {
                table: "zeta".into(),
            },
        ),
    ];
    assert_eq!(profile().name, "embedded-trusted-native-0.1");
    for (text, tree) in &commands {
        assert_eq!(parse_inspection_with_version(text, VERSION).unwrap(), *tree);
        assert_eq!(
            parse_input_with_version(text, VERSION).unwrap(),
            ParsedInput::Inspection(tree.clone())
        );
        assert!(parse(text).is_err());
        assert!(parse_with_version(text, VERSION).is_err());
        assert!(engine.execute_text(text, &Bindings::new()).is_err());
        assert!(engine.prepare_bound_text(text, &Bindings::new()).is_err());
        for version in OLDER {
            assert_eq!(
                parse_inspection_with_version(text, version),
                Err(Error::Unsupported)
            );
            assert!(parse_input_with_version(text, version).is_err());
            assert!(matches!(
                Engine::with_version(admin.clone(), version).inspect(tree),
                Err(Error::Unsupported)
            ));
        }
    }
    for version in OLDER.into_iter().chain([VERSION]) {
        for name in ["show", "describe", "tables", "capabilities", "limits"] {
            // Bare SHOW stays reserved; contextual SHOW also needs a profile
            // that permits canonical reserved names as semantic identifiers.
            if name == "show" {
                assert!(parse_input_with_version(name, version).is_err());
            } else {
                assert!(matches!(
                    parse_input_with_version(name, version).unwrap(),
                    ParsedInput::Statement(Statement::Read { table, .. }) if table == name
                ));
            }
            for source in [
                format!("{name} where id=1"),
                format!("{name} order by id"),
                format!("{name} limit 1"),
                format!("{name} select id"),
            ] {
                if name == "show"
                    && !matches!(
                        version,
                        LanguageVersion::Embedded09
                            | LanguageVersion::Embedded010
                            | LanguageVersion::Embedded011
                            | LanguageVersion::Embedded012
                    )
                {
                    assert!(
                        parse_input_with_version(&source, version).is_err(),
                        "{source}"
                    );
                    continue;
                }
                assert!(matches!(
                    parse_input_with_version(&source, version).unwrap(),
                    ParsedInput::Statement(Statement::Read { table, .. }) if table == name
                ));
            }
        }
        let parsed = parse_input_with_version("describe where id=1", version).unwrap();
        assert!(
            matches!(parsed, ParsedInput::Statement(Statement::Read { table, .. }) if table == "describe")
        );
        with_sql_parser_forbidden(|| {
            assert_eq!(
                Engine::with_version(admin.clone(), version)
                    .execute_text("describe where id=1", &Bindings::new())
                    .unwrap()
                    .rows[0]
                    .values,
                [Value::Int(1)]
            );
        });
    }
    assert_eq!(
        parse_inspection_with_version("-- comment\nSHOW TABLES; -- end", VERSION).unwrap(),
        InspectionStatement::ShowTables
    );
    assert!(matches!(
        parse_input_with_version("`describe` where id=1", VERSION).unwrap(),
        ParsedInput::Statement(_)
    ));
}

#[test]
fn inspection_domain_serialization_preserves_core_u128_encoding() {
    let core = giggles_core::operations::InspectionOutcome {
        contract_version: giggles_core::operations::ENGINE_CONTRACT_VERSION,
        database_domain: u128::MAX,
        catalog_epoch: 1,
        complete: true,
        result: InspectionResult::Tables(vec![]),
    };
    let expected = serde_json::to_value(&core).unwrap();
    let wrapped = json(InspectionOutcome::Catalog(core.clone()));
    assert_eq!(wrapped["Catalog"], expected);
    assert_eq!(wrapped["Catalog"]["database_domain"], u128::MAX.to_string());
    assert_eq!(
        serde_json::from_value::<giggles_core::operations::InspectionOutcome>(
            wrapped["Catalog"].clone()
        )
        .unwrap(),
        core
    );
}

#[test]
fn inspection_catalog_text_and_tree_match_without_sql_or_effects() {
    let (admin, engine) = fixture();
    let before = admin.export(path()).unwrap();
    let (epoch, tables) = admin.catalog().unwrap();
    with_sql_parser_forbidden(|| {
        for (source, tree) in [
            ("show capabilities", InspectionStatement::ShowCapabilities),
            ("show limits", InspectionStatement::ShowLimits),
        ] {
            assert_eq!(
                json(engine.inspect_text(source).unwrap()),
                json(engine.inspect(&tree).unwrap())
            );
        }
        let InspectionOutcome::Capabilities(manifest) =
            engine.inspect_text("show capabilities").unwrap()
        else {
            panic!("capabilities expected")
        };
        assert_eq!(manifest.language_version, "0.12");
        assert_eq!(manifest.profile, "embedded-trusted-native-0.12");
        assert_eq!(
            serde_json::to_value(manifest).unwrap(),
            serde_json::to_value(capability_manifest_for(VERSION)).unwrap()
        );
        let InspectionOutcome::Limits(limits) = engine.inspect_text("show limits").unwrap() else {
            panic!("limits expected")
        };
        assert_eq!(limits.native_table_rows, MAX_ROWS);
        assert_eq!(
            limits.inspection_metadata_bytes,
            Some(MAX_INSPECTION_METADATA_BYTES)
        );

        let listed = catalog(engine.inspect_text("show tables").unwrap());
        assert!(listed.complete);
        assert_eq!(listed.catalog_epoch, epoch);
        assert_eq!(listed.database_domain, admin.snapshot().unwrap().domain);
        assert_eq!(
            listed.contract_version,
            giggles_core::operations::ENGINE_CONTRACT_VERSION
        );
        let mut expected: Vec<_> = tables
            .iter()
            .map(|table| TableSummary {
                id: table.id,
                name: table.name.clone(),
                column_count: table.columns.len(),
                primary_key_columns: table.primary_key.len(),
            })
            .collect();
        expected.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(listed.result, InspectionResult::Tables(expected));
        assert_eq!(
            json(InspectionOutcome::Catalog(listed)),
            json(engine.inspect(&InspectionStatement::ShowTables).unwrap())
        );
        for table in &tables {
            let request = InspectionStatement::DescribeTable {
                table: table.name.clone(),
            };
            let described = catalog(engine.inspect(&request).unwrap());
            assert_eq!(described.result, InspectionResult::Table(table.clone()));
            assert_eq!(described.catalog_epoch, epoch);
            assert!(described.complete);
            assert_eq!(
                json(InspectionOutcome::Catalog(described)),
                json(
                    engine
                        .inspect_text(&format!("describe table {}", table.name))
                        .unwrap()
                )
            );
        }
        let zeta = tables.iter().find(|table| table.name == "zeta").unwrap();
        assert_eq!(
            zeta.columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect::<Vec<_>>(),
            [
                DataType::Integer,
                DataType::BigInt,
                DataType::SmallInt,
                DataType::Boolean,
                DataType::Text,
                DataType::Bytes,
            ]
        );
        assert_eq!(zeta.primary_key, [0, 1]);
        assert!(zeta.columns[0].not_null && zeta.columns[1].not_null);
        assert!(!zeta.columns[2].not_null);
    });
    assert_eq!(admin.export(path()).unwrap(), before);

    // Another session's staged catalog is not an ambient inspection snapshot.
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    sql.execute("CREATE TABLE pending(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert!(
        matches!(with_sql_parser_forbidden(|| engine.inspect_text("describe table pending")), Err(Error::Bind(message)) if message == "table")
    );
    assert_eq!(
        catalog(engine.inspect_text("show tables").unwrap()).catalog_epoch,
        epoch
    );
    sql.execute("COMMIT", &[], None).unwrap();
    let committed = admin.export(path()).unwrap();
    let pending = with_sql_parser_forbidden(|| {
        catalog(engine.inspect_text("describe table pending").unwrap())
    });
    assert!(pending.catalog_epoch > epoch);
    assert!(matches!(pending.result, InspectionResult::Table(table) if table.name == "pending"));
    assert_eq!(admin.export(path()).unwrap(), committed);
}

#[test]
fn inspection_rejections_and_identifier_bounds_are_explicit() {
    let (admin, engine) = fixture();
    let before = admin.export(path()).unwrap();
    with_sql_parser_forbidden(|| {
        for source in [
            "show",
            "show users",
            "show tables limit 1",
            "show limits $n",
            "show capabilities return id",
            "describe table",
            "describe table $name",
            "describe table public.zeta",
            "describe table zeta extra",
            "show tables; show tables",
            "show tables;;",
            "describe table zeta once \"key\" for 1s",
            "show tables once \"key\" for 1s",
            "transaction { show tables }",
            "transaction { describe table zeta }",
            "transaction { let x=(show limits); emit 1 }",
            "transaction once \"key\" for 1s { describe table zeta }",
        ] {
            assert!(engine.inspect_text(source).is_err(), "{source}");
            assert!(
                parse_input_with_version(source, VERSION).is_err(),
                "{source}"
            );
            assert!(
                parse_once_with_version(source, VERSION).is_err(),
                "{source}"
            );
        }
        for name in ["", "Upper", "a.b", "_field", "naïve", "a-b"] {
            assert!(
                matches!(engine.inspect(&InspectionStatement::DescribeTable { table: name.into() }), Err(Error::Bind(message)) if message == "identifier")
            );
        }
        for length in [64, MAX_INPUT + 1] {
            assert!(matches!(
                engine.inspect(&InspectionStatement::DescribeTable {
                    table: "x".repeat(length)
                }),
                Err(Error::Limit)
            ));
        }
        assert!(
            matches!(engine.inspect_text("describe table missing"), Err(Error::Bind(message)) if message == "table")
        );
        assert!(matches!(
            engine.inspect_text(&" ".repeat(MAX_INPUT + 1)),
            Err(Error::Limit)
        ));
        assert!(matches!(
            engine.inspect_text(&format!("describe table `{}`", "x".repeat(64))),
            Err(Error::Limit)
        ));
        assert!(matches!(
            engine.inspect_text("describe table `Upper`"),
            Err(Error::Unsupported)
        ));
    });
    assert_eq!(admin.export(path()).unwrap(), before);
    let name = "n".repeat(63);
    admin
        .sql_session()
        .execute(
            &format!("CREATE TABLE {name}(id BIGINT PRIMARY KEY)"),
            &[],
            None,
        )
        .unwrap();
    with_sql_parser_forbidden(|| {
        let raw = engine
            .inspect(&InspectionStatement::DescribeTable {
                table: name.clone(),
            })
            .unwrap();
        assert_eq!(
            json(raw),
            json(
                engine
                    .inspect_text(&format!("describe table `{name}`"))
                    .unwrap()
            )
        );
    });
}

fn clock() -> std::result::Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: 1000,
        upper_unix_seconds: 1001,
        valid_until: Instant::now() + Duration::from_secs(10),
    })
}

#[test]
fn inspection_version_twelve_preserves_once_identity_and_envelope_fences() {
    let (admin, engine) = fixture();
    let older = Engine::with_version(admin.clone(), LanguageVersion::Embedded011);
    let scope = OnceScope {
        database_domain: admin.snapshot().unwrap().domain,
        tenant: 42,
        namespace: "inspection.tests".into(),
    };
    let source = "insert into alpha(id) values(11) once \"kept\" for 1m return id";
    let previous = parse_once_with_version(source, LanguageVersion::Embedded011).unwrap();
    let selected = parse_once_with_version(source, VERSION).unwrap();
    assert_eq!(selected.language_version, VERSION);
    let no_clock = || -> std::result::Result<OnceTime, OnceClockUnavailable> {
        panic!("cross-profile refusal or retained replay must not certify time")
    };
    with_sql_parser_forbidden(|| {
        assert!(matches!(
            engine.execute_once(&previous, &Bindings::new(), &scope, &no_clock),
            Err(OnceError::Frontend(Error::Unsupported))
        ));
        assert!(matches!(
            older.execute_once(&selected, &Bindings::new(), &scope, &no_clock),
            Err(OnceError::Frontend(Error::Unsupported))
        ));
        let first = engine
            .execute_once(&selected, &Bindings::new(), &scope, &clock)
            .unwrap();
        assert!(!first.replayed);
        let replay = engine
            .execute_once_text(source, &Bindings::new(), &scope, &no_clock)
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(first.accepted_fingerprint, replay.accepted_fingerprint);
        assert_eq!(first.description, replay.description);
        assert_eq!(first.outcome, replay.outcome);
        let query = parse_outcome_with_version("outcome \"kept\"", VERSION).unwrap();
        assert!(matches!(
            engine.outcome(&query, &Bindings::new(), &scope).unwrap(),
            OnceStatus::Retained(_)
        ));
        assert!(matches!(
            older.outcome(&query, &Bindings::new(), &scope),
            Err(OnceError::Frontend(Error::Unsupported))
        ));
        let previous_query =
            parse_outcome_with_version("outcome \"kept\"", LanguageVersion::Embedded011).unwrap();
        assert!(matches!(
            engine.outcome(&previous_query, &Bindings::new(), &scope),
            Err(OnceError::Frontend(Error::Unsupported))
        ));
        let block = "transaction once \"block\" for 1m { let made=(insert many into alpha(id) values(12),(13) on conflict(id) do nothing at most 2 return id); require @made.changed=2; emit coalesce(null,case when true then abs(-12) else 0 end) }";
        let result = engine
            .execute_once_text(block, &Bindings::new(), &scope, &clock)
            .unwrap();
        assert!(result.outcome.is_ok());
    });
}

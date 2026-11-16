use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use skedaddle::{CapabilityReport, Criticality, DefinitionError, ItemStatus, MigrationDefinition};
use skedaddle::{definition::HookPhase, hooks::OperatorHook, security::TlsPolicy};

fn readme_examples() -> Vec<String> {
    let readme =
        fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md")).unwrap();
    let mut examples = Vec::new();
    let mut current = None;
    for line in readme.lines() {
        if let Some(example) = current.as_mut() {
            if line.trim() == "```" {
                examples.push(current.take().unwrap());
            } else {
                let example: &mut String = example;
                example.push_str(line);
                example.push('\n');
            }
        } else if line.trim() == "```json" {
            current = Some(String::new());
        }
    }
    assert!(current.is_none(), "unterminated definition fence");
    assert!(!examples.is_empty(), "README must contain a definition");
    examples
}

fn example_value() -> Value {
    serde_json::from_str(&readme_examples()[0]).unwrap()
}

/// Preserve every fixture for coordinator review. Never use a cleanup-on-drop
/// temp directory or the user's default temporary directory.
fn fixture(input: impl AsRef<[u8]>) -> PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = ROOT.get_or_init(|| {
        let base = std::env::var_os("SKEDADDLE_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("J:/gigglesdb-work"));
        assert!(
            base.starts_with("J:/gigglesdb-work"),
            "fixtures must stay under J:/gigglesdb-work"
        );
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = base.join(format!("skedaddle-tests-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    });
    let path = root.join(format!(
        "definition-{}.json",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, input).unwrap();
    path
}

fn parse_value(value: &Value) -> Result<MigrationDefinition, DefinitionError> {
    MigrationDefinition::from_path(fixture(serde_json::to_string_pretty(value).unwrap()))
}

fn valid_definition() -> MigrationDefinition {
    parse_value(&example_value()).unwrap()
}

fn spanner_connection(dialect: &str) -> Value {
    json!({
        "engine": "spanner",
        "spanner": {
            "project": "unresolved-project", "instance": "instance", "database": "database",
            "dialect": dialect, "change_stream": "changes",
            "credentials": "file:/unread/spanner-credentials", "emulator": false
        }
    })
}

fn gigglesdb_connection(deployment: &str) -> Value {
    let mut value = json!({
        "engine": "gigglesdb",
        "version": "PostgreSQL 18.0 (GigglesDB 0.1.0)",
        "gigglesdb": {"deployment": deployment}
    });
    if deployment == "embedded" {
        value["gigglesdb"]["database_path"] = json!("J:/gigglesdb-work/declared-db");
    } else {
        value["connection_ref"] = json!("env:UNRESOLVED_GIGGLESDB_CONNECTION");
        value["tls_ca_ref"] = json!("env:UNRESOLVED_GIGGLESDB_CA");
    }
    value
}

fn assert_spanner_blockers(report: &CapabilityReport, side: &str) {
    assert!(!report.blocking().is_empty());
    for capability in [
        "connector_cdc_ordering",
        "snapshot_consistency",
        "schema_type_mapping",
        "fence_cutover_reverse",
    ] {
        let id = format!("{side}.spanner.{capability}");
        let entry = report.items.iter().find(|item| item.id == id).unwrap();
        assert_eq!(entry.status, ItemStatus::Unverified);
        assert_eq!(entry.criticality, Criticality::Critical);
        assert!(entry.remediation.contains("not yet built or qualified"));
        assert!(report.blocking().contains(&entry));
    }
    assert!(report.items.iter().all(|item| {
        item.id != format!("{side}.version")
            && item.id != format!("{side}.tls_trust")
            && item.id != "postgresql.preflight"
            && !(item.id.starts_with("selected_objects[")
                && item.remediation.contains("PostgreSQL connector"))
    }));
    for entry in report
        .items
        .iter()
        .filter(|item| item.id.starts_with(&format!("{side}.")))
    {
        assert!(entry.object.is_none());
        assert_eq!(entry.status, ItemStatus::Unverified);
        let text = serde_json::to_string(entry).unwrap();
        assert!(!text.contains("PostgreSQL"));
        assert!(!text.contains("PEM"));
    }
}

#[test]
fn spanner_capabilities_block_both_dialects_on_either_side_without_pg_claims() {
    for dialect in ["google-sql", "postgresql"] {
        for (side, pg_side) in [("source", "destination"), ("destination", "source")] {
            let mut value = example_value();
            value[side] = spanner_connection(dialect);
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            assert_spanner_blockers(&report, side);
            assert_eq!(report.blocking().len(), 4);
            assert!(
                report
                    .items
                    .iter()
                    .all(|item| item.status != ItemStatus::Unsupported)
            );
            assert!(
                report
                    .items
                    .iter()
                    .any(|item| item.id == format!("{pg_side}.tls_trust"))
            );

            // The opposite PostgreSQL endpoint is classified independently.
            value[pg_side]["version"] = json!("15.8");
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            assert_spanner_blockers(&report, side);
            assert!(report.blocking().iter().any(|item| {
                item.id == format!("{pg_side}.version") && item.status == ItemStatus::Unsupported
            }));
        }
    }
}

#[test]
fn spanner_emulator_adds_a_critical_unverified_item_without_promoting_results() {
    for dialect in ["google-sql", "postgresql"] {
        for side in ["source", "destination"] {
            let mut value = example_value();
            value[side] = spanner_connection(dialect);
            let baseline =
                CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            value[side]["spanner"]["emulator"] = json!(true);
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            assert_spanner_blockers(&report, side);
            assert_eq!(report.blocking().len(), 5);
            assert_eq!(report.items.len(), baseline.items.len() + 1);
            assert!(
                baseline
                    .items
                    .iter()
                    .all(|item| report.items.contains(item))
            );
            let entry = report
                .items
                .iter()
                .find(|item| item.id == format!("{side}.spanner.emulator"))
                .unwrap();
            assert_eq!(entry.status, ItemStatus::Unverified);
            assert_eq!(entry.criticality, Criticality::Critical);
            assert!(
                entry
                    .remediation
                    .contains("emulator results stay unverified")
            );
            assert!(
                report
                    .items
                    .iter()
                    .all(|item| item.status != ItemStatus::Unsupported)
            );
        }
    }
}

#[test]
fn spanner_and_gigglesdb_are_independently_unverified_and_unsupported() {
    for dialect in ["google-sql", "postgresql"] {
        for (side, unsupported_side) in [("source", "destination"), ("destination", "source")] {
            let mut value = example_value();
            value[side] = spanner_connection(dialect);
            value[unsupported_side] = gigglesdb_connection("server");
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            assert_spanner_blockers(&report, side);
            assert_eq!(report.blocking().len(), 7);
            let unsupported: Vec<_> = report
                .items
                .iter()
                .filter(|item| item.status == ItemStatus::Unsupported)
                .collect();
            assert_eq!(
                unsupported.len(),
                if unsupported_side == "source" { 2 } else { 1 }
            );
            assert!(unsupported.iter().all(|item| {
                item.id
                    .starts_with(&format!("{unsupported_side}.gigglesdb."))
                    && item.criticality == Criticality::Critical
            }));
            assert!(
                report
                    .items
                    .iter()
                    .all(|item| item.id != "connector.unsupported")
            );
        }
    }
}

#[test]
fn both_spanner_sides_have_their_own_critical_unverified_blockers() {
    let mut value = example_value();
    value["source"] = spanner_connection("google-sql");
    value["destination"] = spanner_connection("postgresql");
    let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
    for side in ["source", "destination"] {
        assert_spanner_blockers(&report, side);
    }
    assert_eq!(report.blocking().len(), 8);
    assert!(
        report
            .items
            .iter()
            .all(|item| item.status != ItemStatus::Unsupported)
    );
}

#[test]
fn spanner_dialects_validate_without_pg_connection_version_or_pem_trust() {
    use skedaddle::definition::{DEFINITION_VERSION, Engine, SpannerDialect};
    assert_eq!(DEFINITION_VERSION, 1);
    for (dialect, expected) in [
        ("google-sql", SpannerDialect::GoogleSql),
        ("postgresql", SpannerDialect::Postgresql),
    ] {
        for side in ["source", "destination"] {
            let mut value = example_value();
            value[side] = spanner_connection(dialect);
            let definition = parse_value(&value).unwrap();
            let connection = if side == "source" {
                &definition.source
            } else {
                &definition.destination
            };
            assert_eq!(connection.engine, Engine::Spanner);
            assert_eq!(connection.engine.to_string(), "spanner");
            assert!(connection.connection_ref.is_none());
            assert!(connection.version.is_empty());
            assert!(connection.tls_ca_ref.is_none());
            assert_eq!(connection.tls, TlsPolicy::Required);
            assert_eq!(connection.spanner.as_ref().unwrap().dialect, expected);
            let serialized = serde_json::to_value(&definition).unwrap();
            assert!(serialized[side].get("connection_ref").is_none());
            assert_eq!(serialized[side]["spanner"]["dialect"], json!(dialect));
            assert_eq!(parse_value(&serialized).unwrap(), definition);
            value[side]["spanner"]["emulator"] = json!(true);
            value[side]["spanner"]["credentials"] = json!("env:UNRESOLVED_SPANNER_CREDENTIALS");
            value[side]["connection_ref"] = json!("env:OPTIONAL_SPANNER_CONNECTION");
            assert!(parse_value(&value).is_ok());
        }
    }
}

#[test]
fn spanner_blocks_must_match_the_engine() {
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side]["engine"] = json!("spanner");
        let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
            panic!("Spanner must require an endpoint block");
        };
        assert!(
            errors
                .iter()
                .any(|error| error.path == format!("{side}.spanner"))
        );
        value[side]["spanner"] = spanner_connection("google-sql")["spanner"].clone();
        for engine in [
            "postgresql",
            "mysql",
            "mariadb",
            "sqlite",
            "sql-server",
            "oracle",
            "mongodb",
            "gigglesdb",
        ] {
            value[side]["engine"] = json!(engine);
            let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
                panic!("{engine} must forbid a Spanner endpoint block");
            };
            assert!(
                errors
                    .iter()
                    .any(|error| error.path == format!("{side}.spanner"))
            );
        }
    }
}

#[test]
fn spanner_identifiers_are_nonempty_trimmed_and_control_free() {
    for side in ["source", "destination"] {
        for field in ["project", "instance", "database", "change_stream"] {
            for invalid in ["", " ", " padded", "padded ", "a\nb", "a\0b", "a\u{7f}b"] {
                let mut value = example_value();
                value[side] = spanner_connection("google-sql");
                value[side]["spanner"][field] = json!(invalid);
                let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
                    panic!("accepted invalid {side} {field}: {invalid:?}");
                };
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].path, format!("{side}.spanner.{field}"));
            }
        }
    }
}

#[test]
fn spanner_structure_dialect_and_credentials_are_strict() {
    let endpoint = spanner_connection("google-sql")["spanner"].clone();
    for (field, invalid) in [
        ("typo", json!(true)),
        ("dialect", json!("GoogleSQL")),
        ("dialect", json!("google_sql")),
        ("dialect", json!("POSTGRESQL")),
        ("credentials", json!("private-inline-credential")),
        (
            "credentials",
            json!({"private_key": "private-inline-credential"}),
        ),
    ] {
        let mut value = example_value();
        value["source"] = spanner_connection("google-sql");
        value["source"]["spanner"][field] = invalid;
        let Err(DefinitionError::Json(error)) = parse_value(&value) else {
            panic!("Spanner {field} must reject invalid structure");
        };
        assert!(!error.to_string().contains("private-inline-credential"));
    }
    for field in endpoint.as_object().unwrap().keys() {
        let mut value = example_value();
        value["source"] = spanner_connection("google-sql");
        value["source"]["spanner"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(matches!(parse_value(&value), Err(DefinitionError::Json(_))));
    }
}

#[test]
fn non_spanner_endpoints_require_engine_specific_shapes_and_versions() {
    for engine in [
        "postgresql",
        "mariadb",
        "sqlite",
        "sql-server",
        "oracle",
        "mongodb",
        "gigglesdb",
    ] {
        for side in ["source", "destination"] {
            for field in ["connection_ref", "version"] {
                let mut value = example_value();
                value[side]["engine"] = json!(engine);
                if engine == "gigglesdb" {
                    value[side] = gigglesdb_connection("server");
                }
                value[side].as_object_mut().unwrap().remove(field);
                let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
                    panic!("{engine} must require {field}");
                };
                assert!(
                    errors
                        .iter()
                        .any(|error| error.path == format!("{side}.{field}"))
                );
            }
        }
    }
    for side in ["source", "destination"] {
        for pg_reference in [false, true] {
            let mut value = example_value();
            value[side]["engine"] = json!("mysql");
            value[side]["version"] = json!("8.4");
            if !pg_reference {
                value[side]
                    .as_object_mut()
                    .unwrap()
                    .remove("connection_ref");
            }
            let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
                panic!("MySQL must require its typed endpoint block");
            };
            assert!(errors.iter().any(|error| {
                error.path == format!("{side}.mysql")
                    && error.message == "MySQL engine requires a mysql endpoint block"
            }));
            if pg_reference {
                assert!(errors.iter().any(|error| {
                    error.path == side
                        && error.message
                            == "MySQL requires only its mysql endpoint block, without connection_ref or spanner"
                }));
            }
        }
        let mut value = example_value();
        value[side] = mysql_connection();
        assert!(parse_value(&value).is_ok());
        value[side].as_object_mut().unwrap().remove("version");
        let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
            panic!("MySQL must require its version family");
        };
        assert!(
            errors
                .iter()
                .any(|error| error.path == format!("{side}.version"))
        );
    }
}

#[test]
fn gigglesdb_is_declared_and_preflight_remains_unsupported() {
    let mut value = example_value();
    value["source"] = gigglesdb_connection("server");
    let definition = parse_value(&value).unwrap();
    assert_eq!(
        definition.source.engine,
        skedaddle::definition::Engine::Gigglesdb
    );
    assert_eq!(definition.source.engine.to_string(), "gigglesdb");
    assert_eq!(
        serde_json::to_value(&definition).unwrap()["source"]["engine"],
        json!("gigglesdb")
    );
    let report = CapabilityReport::from_definition(&definition).unwrap();
    assert!(
        report
            .blocking()
            .iter()
            .any(|item| item.id == "source.gigglesdb.source_capture"
                && item.status == ItemStatus::Unsupported)
    );
}

#[test]
fn gigglesdb_server_and_embedded_round_trip_on_either_side() {
    use skedaddle::definition::GigglesdbDeployment;
    for (deployment, expected) in [
        ("server", GigglesdbDeployment::Server),
        ("embedded", GigglesdbDeployment::Embedded),
    ] {
        for side in ["source", "destination"] {
            let mut value = example_value();
            value[side] = gigglesdb_connection(deployment);
            let definition = parse_value(&value).unwrap();
            let connection = if side == "source" {
                &definition.source
            } else {
                &definition.destination
            };
            assert_eq!(connection.gigglesdb.as_ref().unwrap().deployment, expected);
            assert_eq!(connection.version, "PostgreSQL 18.0 (GigglesDB 0.1.0)");
            let bytes = serde_json::to_string(&definition).unwrap();
            assert_eq!(MigrationDefinition::from_json(&bytes).unwrap(), definition);
            let serialized = serde_json::to_value(connection).unwrap();
            assert_eq!(serialized["gigglesdb"]["deployment"], deployment);
            assert_eq!(
                serialized["gigglesdb"].get("database_path").is_some(),
                deployment == "embedded"
            );
        }
    }
}

fn assert_validation_at(value: &Value, path: &str) {
    let Err(DefinitionError::Validation(errors)) = parse_value(value) else {
        panic!("expected validation error at {path}");
    };
    assert!(errors.iter().any(|error| error.path == path), "{errors:?}");
}

#[test]
fn gigglesdb_blocks_must_match_the_engine_on_either_side() {
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side] = gigglesdb_connection("server");
        value[side].as_object_mut().unwrap().remove("gigglesdb");
        assert_validation_at(&value, &format!("{side}.gigglesdb"));
        for engine in [
            "postgresql",
            "mysql",
            "mariadb",
            "sqlite",
            "sql-server",
            "oracle",
            "mongodb",
            "spanner",
        ] {
            value[side] = if engine == "spanner" {
                spanner_connection("google-sql")
            } else {
                example_value()[side].clone()
            };
            value[side]["engine"] = json!(engine);
            value[side]["gigglesdb"] = json!({"deployment": "server"});
            assert_validation_at(&value, &format!("{side}.gigglesdb"));
        }
    }
}

#[test]
fn gigglesdb_server_requires_pgwire_and_forbids_database_path() {
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side] = gigglesdb_connection("server");
        value[side]
            .as_object_mut()
            .unwrap()
            .remove("connection_ref");
        assert_validation_at(&value, &format!("{side}.connection_ref"));
        value[side] = gigglesdb_connection("server");
        value[side]["gigglesdb"]["database_path"] = json!("/absolute/db");
        assert_validation_at(&value, &format!("{side}.gigglesdb.database_path"));
        value[side] = gigglesdb_connection("server");
        value[side].as_object_mut().unwrap().remove("tls_ca_ref");
        assert_validation_at(&value, &format!("{side}.tls_ca_ref"));
        value[side]["tls"] = json!("disable-for-local-test");
        assert!(parse_value(&value).is_ok());
        value[side]["tls_ca_ref"] = json!("env:UNRESOLVED_CA");
        assert_validation_at(&value, &format!("{side}.tls_ca_ref"));
    }
}

#[test]
fn gigglesdb_embedded_requires_a_clean_absolute_database_path() {
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side] = gigglesdb_connection("embedded");
        value[side]["gigglesdb"]
            .as_object_mut()
            .unwrap()
            .remove("database_path");
        assert_validation_at(&value, &format!("{side}.gigglesdb.database_path"));
        for invalid in [
            "",
            " ",
            "relative/db",
            "C:relative",
            "\\rooted",
            "\\\\",
            "\\\\server",
            "\\\\server\\",
            " /db",
            "/db ",
            "/a\nb",
            "/a\0b",
            "/a\u{7f}b",
        ] {
            value[side]["gigglesdb"]["database_path"] = json!(invalid);
            assert_validation_at(&value, &format!("{side}.gigglesdb.database_path"));
        }
        for valid in [
            "/var/lib/gigglesdb",
            "J:/gigglesdb-work/db",
            "J:\\gigglesdb-work\\db",
            "\\\\server\\share\\db",
        ] {
            value[side]["gigglesdb"]["database_path"] = json!(valid);
            assert!(parse_value(&value).is_ok(), "rejected {valid:?}");
        }
    }
}

#[test]
fn gigglesdb_embedded_rejects_pgwire_and_tls_configuration() {
    for side in ["source", "destination"] {
        for (field, invalid) in [
            ("connection_ref", json!("env:UNRESOLVED_CONNECTION")),
            ("tls", json!("disable-for-local-test")),
            ("tls_ca_ref", json!("env:UNRESOLVED_CA")),
        ] {
            let mut value = example_value();
            value[side] = gigglesdb_connection("embedded");
            value[side][field] = invalid;
            assert_validation_at(&value, &format!("{side}.{field}"));
        }
        let mut value = example_value();
        value[side] = gigglesdb_connection("embedded");
        value[side]["tls"] = json!("require");
        assert!(parse_value(&value).is_ok());
    }
}

#[test]
fn gigglesdb_distributed_parses_but_fails_with_the_scope_decision() {
    use skedaddle::definition::GigglesdbDeployment;
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side] = gigglesdb_connection("distributed");
        let definition: MigrationDefinition = serde_json::from_value(value.clone()).unwrap();
        let connection = if side == "source" {
            &definition.source
        } else {
            &definition.destination
        };
        assert_eq!(
            connection.gigglesdb.as_ref().unwrap().deployment,
            GigglesdbDeployment::Distributed
        );
        let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
            panic!("distributed deployment accepted");
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].path, format!("{side}.gigglesdb.deployment"));
        assert_eq!(
            errors[0].message,
            "distributed GigglesDB deployments are out of scope (GD-0065 Q3)"
        );
        assert!(CapabilityReport::from_definition(&definition).is_err());
    }
}

#[test]
fn gigglesdb_version_is_required_for_both_deployments_and_sides() {
    for deployment in ["server", "embedded"] {
        for side in ["source", "destination"] {
            for version in [None, Some(""), Some(" \t\n")] {
                let mut value = example_value();
                value[side] = gigglesdb_connection(deployment);
                if let Some(version) = version {
                    value[side]["version"] = json!(version);
                } else {
                    value[side].as_object_mut().unwrap().remove("version");
                }
                assert_validation_at(&value, &format!("{side}.version"));
            }
        }
    }
}

#[test]
fn gigglesdb_endpoint_structure_is_strict() {
    for endpoint in [
        json!({}),
        json!({"deployment": "Server"}),
        json!({"deployment": "unknown"}),
        json!({"deployment": "server", "typo": true}),
        json!({"deployment": "embedded", "database_path": 42}),
    ] {
        let mut value = example_value();
        value["destination"] = gigglesdb_connection("server");
        value["destination"]["gigglesdb"] = endpoint;
        assert!(matches!(parse_value(&value), Err(DefinitionError::Json(_))));
    }
}

#[test]
fn gigglesdb_capability_gates_are_separate_critical_and_never_supported() {
    for side in ["source", "destination"] {
        for deployment in ["server", "embedded"] {
            let mut value = example_value();
            value[side] = gigglesdb_connection(deployment);
            // No affirmative exclusion declarations in this capability-only fixture.
            value["excluded_objects"] = json!([]);
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            let mut expected = vec![
                (
                    format!("{side}.gigglesdb.fenced_reversible"),
                    ItemStatus::Unsupported,
                    "needs source capture (GD-0077 stage 2) and the fence for the declared deployment",
                ),
                (
                    format!("{side}.gigglesdb.version_pin"),
                    ItemStatus::Unverified,
                    "version() reports only the crate version; no build or capability identity yet (GD-0077 question)",
                ),
                (
                    "rollback.gigglesdb".to_owned(),
                    ItemStatus::Unsupported,
                    "forward-only until GD-0077 capture ships; rollback requires the GigglesDB source connector",
                ),
            ];
            expected.push(if side == "destination" {
                (format!("{side}.gigglesdb.destination_apply"), ItemStatus::Unverified, "connector not yet built or qualified; conformance tests against a real giggles-server pending (GD-0065 N3)")
            } else {
                (format!("{side}.gigglesdb.source_capture"), ItemStatus::Unsupported, "GD-0077 capture stage not built")
            });
            if deployment == "embedded" {
                expected.push((
                    format!("{side}.gigglesdb.embedded_fence"),
                    ItemStatus::Unsupported,
                    "GD-0077 E4 pending DD-0068 pre-release",
                ));
            }
            assert_eq!(report.blocking().len(), expected.len());
            for (id, status, remediation) in &expected {
                let item = report.items.iter().find(|item| &item.id == id).unwrap();
                assert_eq!(&item.status, status);
                assert_eq!(&item.remediation, remediation);
                assert_eq!(item.criticality, Criticality::Critical);
                assert!(!item.sampled);
                assert!(item.object.is_none());
                assert!(report.blocking().contains(&item));
            }
            let pg_side = if side == "source" {
                "destination"
            } else {
                "source"
            };
            let mut expected_ids: Vec<_> = expected.iter().map(|(id, _, _)| id.clone()).collect();
            expected_ids.push(format!("{pg_side}.tls_trust"));
            if deployment == "server" {
                expected_ids.push(format!("{side}.tls_trust"));
            }
            expected_ids.sort();
            let mut actual_ids: Vec<_> = report.items.iter().map(|item| item.id.clone()).collect();
            actual_ids.sort();
            assert_eq!(actual_ids, expected_ids);
            assert!(report.items.iter().all(|item| !matches!(
                item.status,
                ItemStatus::Supported | ItemStatus::SupportedWithTransformation
            )));
            value[pg_side]["version"] = json!("15.8");
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            assert!(
                report
                    .blocking()
                    .iter()
                    .any(|item| item.id == format!("{pg_side}.version")
                        && item.status == ItemStatus::Unsupported)
            );
        }
    }
}

#[test]
fn spanner_connection_serialization_preserves_existing_bytes() {
    for dialect in ["google-sql", "postgresql"] {
        let mut value = example_value();
        value["source"] = spanner_connection(dialect);
        let definition = parse_value(&value).unwrap();
        let old_json = format!(
            "{{\"engine\":\"spanner\",\"tls\":\"require\",\"version\":\"\",\"spanner\":{{\"project\":\"unresolved-project\",\"instance\":\"instance\",\"database\":\"database\",\"dialect\":\"{dialect}\",\"change_stream\":\"changes\",\"credentials\":\"file:/unread/spanner-credentials\",\"emulator\":false}}}}"
        );
        assert_eq!(serde_json::to_string(&definition.source).unwrap(), old_json);
    }
}

#[test]
fn pg_connection_serialization_preserves_existing_bytes_and_digest() {
    use sha2::{Digest, Sha256};
    let definition = valid_definition();
    for connection in [&definition.source, &definition.destination] {
        let old_json = format!(
            "{{\"connection_ref\":{},\"engine\":{},\"tls\":{},\"tls_ca_ref\":{},\"version\":{}}}",
            serde_json::to_string(connection.connection_ref.as_ref().unwrap()).unwrap(),
            serde_json::to_string(&connection.engine).unwrap(),
            serde_json::to_string(&connection.tls).unwrap(),
            serde_json::to_string(connection.tls_ca_ref.as_ref().unwrap()).unwrap(),
            serde_json::to_string(&connection.version).unwrap(),
        );
        assert_eq!(serde_json::to_string(connection).unwrap(), old_json);
    }
    let mut legacy = example_value();
    for side in ["source", "destination"] {
        legacy[side]["tls"] = json!("require");
    }
    // The existing hook alias serializes to its canonical phase name.
    legacy["hooks"][0]["phase"] = json!("pre-fence");
    assert_eq!(serde_json::to_value(&definition).unwrap(), legacy);
    assert_eq!(
        definition.canonical_digest().unwrap(),
        format!("{:x}", Sha256::digest(serde_json::to_vec(&legacy).unwrap()))
    );
}

fn s3_backend() -> Value {
    json!({"type": "s3-compatible", "options": {
        "endpoint": "https://s3.example.test", "bucket": "migration-buffer",
        "prefix": "", "region": "us-east-1", "credentials_ref": "env:S3_CREDENTIALS"
    }})
}

fn gcs_backend() -> Value {
    json!({"type": "gcs", "options": {
        "bucket": "migration-buffer", "prefix": "", "credentials_ref": "file:/run/secrets/gcs"
    }})
}

#[test]
fn reserved_schema_is_rejected_only_for_selected_objects() {
    let mut value = example_value();
    value["selected_objects"][0]["schema"] = json!("_skedaddle");
    let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
        panic!("reserved schema needs typed definition validation error");
    };
    assert!(
        errors
            .iter()
            .any(|error| error.path == "selected_objects[0].schema"
                && error.message.contains("reserved"))
    );
    for schema in ["public", "_Skedaddle", "_skedaddle_extra"] {
        value["selected_objects"][0]["schema"] = json!(schema);
        assert!(parse_value(&value).is_ok());
    }
    value["excluded_objects"] = json!([{
        "object": {"schema":"_skedaddle", "name":"apply_progress", "kind":"table"},
        "reason":"tool metadata"
    }]);
    assert!(parse_value(&value).is_ok());
}

#[test]
fn connections_default_to_required_tls_and_reject_plaintext() {
    let definition = valid_definition();
    for connection in [&definition.source, &definition.destination] {
        assert_eq!(connection.tls, TlsPolicy::Required);
        assert!(connection.tls.check(true).unwrap().is_empty());
        assert_eq!(
            connection.tls.check(false),
            Err(skedaddle::security::SecurityError::PlaintextRefused)
        );
    }
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side]["tls"] = json!("require");
        assert_eq!(parse_value(&value).unwrap(), definition);
        for invalid in [json!("disable"), json!(null), json!(false)] {
            value[side]["tls"] = invalid;
            assert!(matches!(parse_value(&value), Err(DefinitionError::Json(_))));
        }
    }
}

#[test]
fn local_test_tls_policy_produces_security_warning() {
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side]["tls"] = json!("disable-for-local-test");
        value[side].as_object_mut().unwrap().remove("tls_ca_ref");
        let definition = parse_value(&value).unwrap();
        let connection = if side == "source" {
            &definition.source
        } else {
            &definition.destination
        };
        assert_eq!(connection.tls, TlsPolicy::DisableForLocalTest);
        for encrypted in [false, true] {
            let warnings = connection.tls.check(encrypted).unwrap();
            assert_eq!(warnings.len(), 1);
            assert_eq!(warnings[0].code, "plaintext_local_test_only");
            assert!(warnings[0].message.contains("TLS DISABLED FOR LOCAL TEST"));
        }
        assert_eq!(
            serde_json::to_value(connection).unwrap()["tls"],
            "disable-for-local-test"
        );
    }
}

#[test]
fn hook_timeout_must_be_present_and_positive() {
    let mut value = example_value();
    value["hooks"][0]["timeout_seconds"] = json!(0);
    let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
        panic!("zero timeout must produce a semantic validation error");
    };
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].path, "hooks[0].timeout_seconds");
    assert_eq!(errors[0].message, "must be greater than zero");
    for seconds in [1, u64::MAX] {
        value["hooks"][0]["timeout_seconds"] = json!(seconds);
        let definition = parse_value(&value).unwrap();
        assert_eq!(
            OperatorHook::try_from(&definition.hooks[0])
                .unwrap()
                .timeout,
            Duration::from_secs(seconds)
        );
    }
    let mut hook = valid_definition().hooks.remove(0);
    hook.timeout_seconds = 0;
    assert_eq!(
        OperatorHook::try_from(&hook).unwrap_err().path,
        "timeout_seconds"
    );
    value["hooks"][0]
        .as_object_mut()
        .unwrap()
        .remove("timeout_seconds");
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("missing field `timeout_seconds`")
    );
}

#[test]
fn hook_phases_and_legacy_aliases_convert_to_operator_hooks() {
    use skedaddle::hooks::HookPhase as OperatorPhase;

    for (input, canonical, phase, operator_phase) in [
        (
            "pre-fence",
            "pre-fence",
            HookPhase::PreFence,
            OperatorPhase::PreFence,
        ),
        (
            "post-activate",
            "post-activate",
            HookPhase::PostActivate,
            OperatorPhase::PostActivate,
        ),
        (
            "pre-rollback",
            "pre-rollback",
            HookPhase::PreRollback,
            OperatorPhase::PreRollback,
        ),
        (
            "post-rollback",
            "post-rollback",
            HookPhase::PostRollback,
            OperatorPhase::PostRollback,
        ),
        (
            "before-fence",
            "pre-fence",
            HookPhase::PreFence,
            OperatorPhase::PreFence,
        ),
        (
            "after-cutover",
            "post-activate",
            HookPhase::PostActivate,
            OperatorPhase::PostActivate,
        ),
    ] {
        let mut value = example_value();
        value["hooks"][0]["phase"] = json!(input);
        let definition = parse_value(&value).unwrap();
        let hook = &definition.hooks[0];
        assert_eq!(hook.phase, phase);
        assert_eq!(serde_json::to_value(hook).unwrap()["phase"], canonical);
        let operator = OperatorHook::try_from(hook).unwrap();
        assert_eq!(operator.phase, operator_phase);
        assert_eq!(operator.name, hook.name);
        assert_eq!(operator.command, hook.command);
        assert_eq!(operator.secret_env, hook.secret_env);
        assert_eq!(operator.timeout, Duration::from_secs(hook.timeout_seconds));
        value["hooks"][0]["phase"] = json!(canonical);
        assert_eq!(
            definition.canonical_digest().unwrap(),
            parse_value(&value).unwrap().canonical_digest().unwrap()
        );
    }
}

#[test]
fn ambiguous_on_rollback_phase_has_actionable_error() {
    let mut value = example_value();
    value["hooks"][0]["phase"] = json!("on-rollback");
    let error = parse_value(&value).unwrap_err();
    assert!(matches!(error, DefinitionError::Json(_)));
    assert!(
        error
            .to_string()
            .contains("on-rollback is ambiguous; choose pre-rollback or post-rollback")
    );
}

#[test]
fn before_snapshot_still_parses_but_has_no_operator_phase() {
    let mut value = example_value();
    value["hooks"][0]["phase"] = json!("before-snapshot");
    let definition = parse_value(&value).unwrap();
    assert_eq!(definition.hooks[0].phase, HookPhase::BeforeSnapshot);
    let error = OperatorHook::try_from(&definition.hooks[0]).unwrap_err();
    assert_eq!(error.path, "phase");
    assert!(error.message.contains("before-snapshot"));
}

#[test]
fn definition_with_utf8_bom_parses() {
    let input = readme_examples()[0].clone();
    let with_bom = format!("\u{feff}{input}");
    let plain = MigrationDefinition::from_path(fixture(&input)).unwrap();
    let bom = MigrationDefinition::from_path(fixture(&with_bom)).unwrap();
    assert_eq!(plain, bom);
    assert_eq!(
        plain.canonical_digest().unwrap(),
        bom.canonical_digest().unwrap()
    );
    assert!(MigrationDefinition::from_json(&with_bom).is_err());
}

#[test]
fn definition_with_double_bom_rejected() {
    let input = readme_examples()[0].clone();
    for malformed in [
        format!("\u{feff}\u{feff}{input}"),
        format!(" \u{feff}{input}"),
        format!("{input}\u{feff}"),
        input.replacen('{', "{\u{feff}", 1),
    ] {
        assert!(matches!(
            MigrationDefinition::from_path(fixture(&malformed)),
            Err(DefinitionError::Json(_))
        ));
    }
}

#[test]
fn definition_utf16_rejected_with_clear_error() {
    let input = readme_examples()[0].clone();
    for little_endian in [true, false] {
        let bytes: Vec<_> = std::iter::once(0xfeff)
            .chain(input.encode_utf16())
            .flat_map(|unit| {
                if little_endian {
                    unit.to_le_bytes()
                } else {
                    unit.to_be_bytes()
                }
            })
            .collect();
        let error = MigrationDefinition::from_path(fixture(bytes)).unwrap_err();
        assert!(matches!(error, DefinitionError::Io(_)), "{error}");
        assert!(
            error.to_string().contains("definition must be UTF-8"),
            "{error}"
        );
    }
}

#[test]
fn rejects_unknown_fields() {
    for pointer in [
        "",
        "/source",
        "/destination",
        "/selected_objects/0",
        "/excluded_objects/0",
        "/excluded_objects/0/object",
        "/connector_versions",
        "/buffer",
        "/buffer/backend",
        "/buffer/backend/options",
        "/budgets",
        "/readiness_policy",
        "/buffer_retention",
        "/fence_plan",
        "/hooks/0",
        "/rollback_contract",
    ] {
        let mut value = example_value();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("typo".to_owned(), json!(true));
        let error = parse_value(&value).unwrap_err().to_string();
        // Adjacent-tag enums diagnose unexpected keys as invalid tag keys;
        // their backend option structs use the usual unknown-field error.
        assert!(
            error.contains("unknown field")
                || (pointer == "/buffer/backend"
                    && error.contains("expected \"type\" or \"options\"")),
            "{pointer}: {error}"
        );
    }
    for backend in [s3_backend(), gcs_backend()] {
        let mut value = example_value();
        value["buffer"]["backend"] = backend;
        value["buffer"]["backend"]["options"]["typo"] = json!(true);
        assert!(
            parse_value(&value)
                .unwrap_err()
                .to_string()
                .contains("unknown field")
        );
    }
}

#[test]
fn rejects_inline_secret_values() {
    for pointer in [
        "/source/connection_ref",
        "/destination/connection_ref",
        "/source/tls_ca_ref",
        "/destination/tls_ca_ref",
        "/hooks/0/secret_env/ADMIN_TOKEN",
    ] {
        let mut value = example_value();
        *value.pointer_mut(pointer).unwrap() =
            json!("postgresql://user:very-private-value@localhost/db");
        let error = parse_value(&value).unwrap_err().to_string();
        assert!(
            error.contains("inline secret values are forbidden"),
            "{error}"
        );
        assert!(
            !error.contains("very-private-value"),
            "errors must not echo credentials"
        );
    }
    for backend in [s3_backend(), gcs_backend()] {
        let mut value = example_value();
        value["buffer"]["backend"] = backend;
        value["buffer"]["backend"]["options"]["credentials_ref"] = json!("very-private-value");
        assert!(
            parse_value(&value)
                .unwrap_err()
                .to_string()
                .contains("inline secret values are forbidden")
        );
    }
}

#[test]
fn rejects_empty_or_relative_file_secrets_and_invalid_env_names() {
    for reference in [
        "file:",
        "file: ",
        "file:relative/path",
        "file:C:relative",
        "env:",
        "env:9BAD",
        "env:HAS SPACE",
        "env:BAD\n",
        "file:/bad\npath",
    ] {
        let mut value = example_value();
        value["source"]["connection_ref"] = json!(reference);
        assert!(parse_value(&value).is_err(), "accepted {reference:?}");
    }
    for reference in [
        "env:_VALID_42",
        "file:/absolute/secret",
        "file:C:/secrets/source",
        "file:C:\\secrets\\source",
        "file:\\\\server\\secrets\\source",
    ] {
        let mut value = example_value();
        value["source"]["connection_ref"] = json!(reference);
        assert!(parse_value(&value).is_ok(), "rejected {reference:?}");
    }
}

fn reverse_keys(value: &Value) -> String {
    match value {
        Value::Object(map) => format!(
            "{{{}}}",
            map.iter()
                .rev()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        reverse_keys(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(reverse_keys)
                .collect::<Vec<_>>()
                .join(",")
        ),
        scalar => serde_json::to_string(scalar).unwrap(),
    }
}

#[test]
fn digest_stable_under_key_order() {
    let value = example_value();
    let original = serde_json::to_string_pretty(&value).unwrap();
    let reordered = reverse_keys(&value);
    assert_ne!(original, reordered);
    let first = MigrationDefinition::from_path(fixture(&original)).unwrap();
    let second = MigrationDefinition::from_path(fixture(&reordered)).unwrap();
    let digest = first.canonical_digest().unwrap();
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    assert_eq!(digest, second.canonical_digest().unwrap());
    let escaped = original.replace("accounts", "acc\\u006funts");
    assert_eq!(
        digest,
        MigrationDefinition::from_path(fixture(&escaped))
            .unwrap()
            .canonical_digest()
            .unwrap()
    );
    let mut changed = first;
    changed.mapping_version.push_str("-changed");
    assert_ne!(digest, changed.canonical_digest().unwrap());
}

#[test]
fn exclusions_require_reason_and_appear_in_report() {
    let mut value = example_value();
    value["excluded_objects"][0]
        .as_object_mut()
        .unwrap()
        .remove("reason");
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("missing field `reason`")
    );
    for reason in ["", " \t\n"] {
        value["excluded_objects"][0]["reason"] = json!(reason);
        assert!(
            parse_value(&value)
                .unwrap_err()
                .to_string()
                .contains("excluded_objects[0].reason")
        );
    }
    let definition = valid_definition();
    let report = CapabilityReport::from_definition(&definition).unwrap();
    let exclusion = report
        .items
        .iter()
        .find(|item| item.id == "excluded_objects[0]")
        .unwrap();
    assert_eq!(
        exclusion.object.as_ref(),
        Some(&definition.excluded_objects[0].object)
    );
    assert!(exclusion.evidence.iter().any(|e| {
        e.description
            .contains(&definition.excluded_objects[0].reason)
    }));
    assert_eq!(exclusion.criticality, Criticality::Advisory);
    assert!(!report.blocking().iter().any(|item| item.id == exclusion.id));
}

#[test]
fn unsupported_engine_blocks_preflight() {
    for engine in ["mariadb", "sqlite", "sql-server", "oracle", "mongodb"] {
        for side in ["source", "destination", "both"] {
            let mut value = example_value();
            if side != "destination" {
                value["source"]["engine"] = json!(engine);
            }
            if side != "source" {
                value["destination"]["engine"] = json!(engine);
            }
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            assert_eq!(report.blocking().len(), 1, "{side} {engine}");
            assert_eq!(report.blocking()[0].status, ItemStatus::Unsupported);
            assert_eq!(
                report
                    .items
                    .iter()
                    .filter(|i| i.criticality == Criticality::Critical)
                    .count(),
                1
            );
            assert_eq!(
                report.items.len(),
                4,
                "one blocker, two advisory trust summaries and one advisory exclusion"
            );
        }
    }
    for version in ["8.0", "8.4"] {
        for side in ["source", "destination", "both"] {
            let mut value = example_value();
            for endpoint in ["source", "destination"] {
                if side == endpoint || side == "both" {
                    value[endpoint] = mysql_connection();
                    value[endpoint]["version"] = json!(version);
                }
            }
            let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
            let blocking = report.blocking();
            if side != "destination" {
                let mysql_items: Vec<_> = report
                    .items
                    .iter()
                    .filter(|item| item.id.starts_with("source.mysql."))
                    .collect();
                assert_eq!(
                    mysql_items
                        .iter()
                        .map(|item| item.id.as_str())
                        .collect::<Vec<_>>(),
                    ["source.mysql.preflight", "source.mysql.mapping"]
                );
                for item in mysql_items {
                    assert_eq!(item.status, ItemStatus::Unverified);
                    assert_eq!(item.criticality, Criticality::Critical);
                    assert!(blocking.contains(&item));
                }
            }
            if side != "source" {
                assert!(blocking.iter().any(|item| {
                    item.id == "connector.unsupported"
                        && item.status == ItemStatus::Unsupported
                        && item.criticality == Criticality::Critical
                }));
            } else {
                assert!(
                    blocking
                        .iter()
                        .all(|item| item.status == ItemStatus::Unverified)
                );
                assert_eq!(blocking.len(), 2);
            }
        }
    }
}

#[test]
fn sampled_items_are_labelled() {
    let mut report = CapabilityReport::from_definition(&valid_definition()).unwrap();
    assert!(report.items.iter().all(|item| !item.sampled));
    report.items[0].sampled = true;
    let encoded = serde_json::to_value(&report).unwrap();
    assert_eq!(encoded["items"][0]["sampled"], true);
    assert_eq!(encoded["items"][1]["sampled"], false);
    let decoded: CapabilityReport = serde_json::from_value(encoded).unwrap();
    assert!(decoded.items[0].sampled);
    assert_eq!(report.blocking(), decoded.blocking());
    let mut missing_flag = serde_json::to_value(&report).unwrap();
    missing_flag["items"][0]
        .as_object_mut()
        .unwrap()
        .remove("sampled");
    assert!(serde_json::from_value::<CapabilityReport>(missing_flag).is_err());
}

#[test]
fn definition_examples_in_readme_parse() {
    for example in readme_examples() {
        let definition = MigrationDefinition::from_path(fixture(&example)).unwrap();
        assert!(definition.validate().is_empty());
        assert!(CapabilityReport::from_definition(&definition).is_ok());
    }
}

#[test]
fn rejects_unknown_definition_versions() {
    for version in [0, 2, u32::MAX] {
        let mut value = example_value();
        value["definition_version"] = json!(version);
        assert!(
            parse_value(&value)
                .unwrap_err()
                .to_string()
                .contains("only version 1 is understood")
        );
    }
}

#[test]
fn validation_collects_all_semantic_errors() {
    let mut definition = valid_definition();
    definition.definition_version = 99;
    definition.mapping_version.clear();
    definition.excluded_objects[0].reason.clear();
    definition.budgets.max_buffer_bytes = 0;
    definition.readiness_policy.max_lag_seconds = 999;
    definition.buffer_retention.max_seconds = 0;
    definition.fence_plan.application_roles.clear();
    definition.rollback_contract.procedure.clear();
    let errors = definition.validate();
    assert_eq!(errors.len(), 8, "{errors:?}");
    let paths: Vec<_> = errors.iter().map(|error| error.path.as_str()).collect();
    for expected in [
        "definition_version",
        "mapping_version",
        "excluded_objects[0].reason",
        "budgets.max_buffer_bytes",
        "readiness_policy.max_lag_seconds",
        "buffer_retention.max_seconds",
        "fence_plan.application_roles",
        "rollback_contract.procedure",
    ] {
        assert!(paths.contains(&expected), "missing {expected}");
    }
    assert!(matches!(
        CapabilityReport::from_definition(&definition),
        Err(DefinitionError::Validation(_))
    ));
}

#[test]
fn blocking_covers_every_status_and_criticality() {
    let mut report = CapabilityReport::from_definition(&valid_definition()).unwrap();
    let template = report.items[0].clone();
    report.items.clear();
    for criticality in [Criticality::Critical, Criticality::Advisory] {
        for status in [
            ItemStatus::Supported,
            ItemStatus::SupportedWithTransformation,
            ItemStatus::RequiresExternalChange,
            ItemStatus::Unsupported,
            ItemStatus::Unverified,
        ] {
            let mut item = template.clone();
            item.id = format!("{criticality:?}/{status:?}");
            item.status = status;
            item.criticality = criticality;
            report.items.push(item);
        }
    }
    assert_eq!(
        report
            .blocking()
            .iter()
            .map(|item| item.status)
            .collect::<Vec<_>>(),
        vec![
            ItemStatus::RequiresExternalChange,
            ItemStatus::Unsupported,
            ItemStatus::Unverified
        ]
    );
}

#[test]
fn buffers_accept_all_backends_and_only_create_only_manifests() {
    for backend in [
        example_value()["buffer"]["backend"].clone(),
        s3_backend(),
        gcs_backend(),
    ] {
        let mut value = example_value();
        value["buffer"]["backend"] = backend;
        assert!(parse_value(&value).is_ok());
        value["buffer"]["manifest_put"] = json!("overwrite");
        assert!(parse_value(&value).is_err());
    }
}

#[test]
fn rejects_s3_endpoint_credentials() {
    for endpoint in [
        "https://user:secret@example.test",
        "https://example.test?token=secret",
        "https://example.test#secret",
        "ftp://example.test",
        "https://",
        "https://example.test\\@other.test",
    ] {
        let mut value = example_value();
        value["buffer"]["backend"] = s3_backend();
        value["buffer"]["backend"]["options"]["endpoint"] = json!(endpoint);
        let error = parse_value(&value).unwrap_err().to_string();
        assert!(error.contains("without inline credentials"), "{error}");
        assert!(!error.contains("token=secret"));
    }
}

#[test]
fn rejects_duplicate_or_conflicting_objects() {
    let mut value = example_value();
    let object = value["selected_objects"][0].clone();
    value["selected_objects"]
        .as_array_mut()
        .unwrap()
        .push(object.clone());
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("duplicate selected object")
    );
    value["selected_objects"].as_array_mut().unwrap().pop();
    value["excluded_objects"][0]["object"] = object;
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("both selected and excluded")
    );
}

#[test]
fn postgres_versions_are_declared_but_never_live_verified() {
    for version in ["16", "17.2", "18.0"] {
        let mut value = example_value();
        value["source"]["version"] = json!(version);
        let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
        assert!(
            report
                .blocking()
                .iter()
                .all(|item| item.status == ItemStatus::Unverified)
        );
    }
    let mut value = example_value();
    value["source"]["version"] = json!("15.8");
    let report = CapabilityReport::from_definition(&parse_value(&value).unwrap()).unwrap();
    assert!(
        report
            .blocking()
            .iter()
            .any(|item| item.id == "source.version" && item.status == ItemStatus::Unsupported)
    );
    for version in ["", "banana", "17.", "17beta1", "-17", "17..2"] {
        value["source"]["version"] = json!(version);
        assert!(parse_value(&value).is_err());
    }
}

#[test]
fn cli_emits_json_for_blockers_and_rejects_invalid_input() {
    let definition = MigrationDefinition::from_path(fixture(&readme_examples()[0])).unwrap();
    let path = fixture(&readme_examples()[0]);
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("check")
        .arg(path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["schema_version"], 1);
    assert_eq!(envelope["outcome"], "blocked");
    assert_eq!(envelope["exit_code"], 2);
    let report: CapabilityReport = serde_json::from_value(envelope["data"].clone()).unwrap();
    assert_eq!(
        report.definition_digest,
        definition.canonical_digest().unwrap()
    );
    assert!(!report.blocking().is_empty());
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("check")
        .arg(fixture("{\"typo\":true}"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(output.stderr.is_empty());
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["schema_version"], 1);
    assert_eq!(envelope["outcome"], "invalid-input");
    assert_eq!(envelope["exit_code"], 3);
    assert!(
        envelope["problem"]["what"]
            .as_str()
            .unwrap()
            .contains("unknown field")
    );
}

#[test]
fn denies_unknown_fields_in_capability_structures() {
    let report = CapabilityReport::from_definition(&valid_definition()).unwrap();
    for pointer in ["", "/items/0", "/items/0/evidence/0"] {
        let mut value = serde_json::to_value(&report).unwrap();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("typo".to_owned(), json!(true));
        assert!(serde_json::from_value::<CapabilityReport>(value).is_err());
    }
}

#[test]
fn tls_ca_reference_is_required_and_contradictory_plaintext_is_rejected() {
    for side in ["source", "destination"] {
        let mut value = example_value();
        value[side].as_object_mut().unwrap().remove("tls_ca_ref");
        for policy in [None, Some("require")] {
            if let Some(policy) = policy {
                value[side]["tls"] = json!(policy);
            }
            let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
                panic!("required TLS without explicit CA trust must be rejected");
            };
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].path, format!("{side}.tls_ca_ref"));
            assert!(errors[0].message.contains("set tls_ca_ref"));
            assert!(errors[0].message.contains("PEM bundle path"));
        }
        value[side]["tls_ca_ref"] = json!("env:UNRESOLVED_CA_PATH");
        value[side]["tls"] = json!("disable-for-local-test");
        let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
            panic!("plaintext with CA trust must be rejected");
        };
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].path, format!("{side}.tls_ca_ref"));
        assert!(errors[0].message.contains("remove tls_ca_ref"));
    }
}

#[test]
fn explicit_ca_references_round_trip_without_resolution_and_reports_hide_identifiers() {
    let mut value = example_value();
    value["source"]["tls_ca_ref"] = json!("env:PRIVATE_UNRESOLVED_CA_PATH");
    value["destination"]["tls_ca_ref"] = json!("file:/private/unread-ca-path-reference");
    let definition = parse_value(&value).unwrap();
    let serialized = serde_json::to_string(&definition).unwrap();
    assert_eq!(
        MigrationDefinition::from_json(&serialized).unwrap(),
        definition
    );
    let report = CapabilityReport::from_definition(&definition).unwrap();
    let output = serde_json::to_string(&report).unwrap();
    assert!(!output.contains("PRIVATE_UNRESOLVED_CA_PATH"));
    assert!(!output.contains("/private/unread-ca-path-reference"));
    for (side, kind) in [("source", "env"), ("destination", "file")] {
        let item = report
            .items
            .iter()
            .find(|item| item.id == format!("{side}.tls_trust"))
            .unwrap();
        assert_eq!(item.status, ItemStatus::Unverified);
        assert_eq!(item.criticality, Criticality::Advisory);
        assert!(item.evidence[0].description.contains("CA configured: true"));
        assert!(
            item.evidence[0]
                .description
                .contains(&format!("reference kind: {kind}"))
        );
    }
    let mut changed = definition.clone();
    changed.source.tls_ca_ref =
        Some(skedaddle::definition::SecretRef::parse("env:OTHER_CA").unwrap());
    assert_ne!(
        definition.canonical_digest().unwrap(),
        changed.canonical_digest().unwrap()
    );
}

#[test]
fn column_exclusions_default_empty_and_preserve_existing_digest() {
    use sha2::{Digest, Sha256};
    let baseline = valid_definition();
    let serialized = serde_json::to_value(&baseline).unwrap();
    assert!(
        serialized["selected_objects"][0]
            .get("exclude_columns")
            .is_none()
    );
    assert!(
        serialized["excluded_objects"][0]["object"]
            .get("exclude_columns")
            .is_none()
    );
    // serde_json's default map order is the canonical lexicographic key order.
    let old_digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&serialized).unwrap())
    );
    assert_eq!(baseline.canonical_digest().unwrap(), old_digest);
    let mut explicit_empty = example_value();
    explicit_empty["selected_objects"][0]["exclude_columns"] = json!([]);
    explicit_empty["excluded_objects"][0]["object"]["exclude_columns"] = json!([]);
    assert_eq!(parse_value(&explicit_empty).unwrap(), baseline);
    assert_eq!(
        parse_value(&explicit_empty)
            .unwrap()
            .canonical_digest()
            .unwrap(),
        old_digest
    );
    explicit_empty["selected_objects"][0]["exclude_columns"] = json!(["private_payload"]);
    let excluded = parse_value(&explicit_empty).unwrap();
    assert_ne!(excluded.canonical_digest().unwrap(), old_digest);
    assert_eq!(
        serde_json::to_value(excluded).unwrap()["selected_objects"][0]["exclude_columns"],
        json!(["private_payload"])
    );
}

#[test]
fn column_exclusions_reject_empty_duplicate_and_non_table_names() {
    for (names, expected) in [
        (json!([""]), "must not be empty"),
        (json!(["  "]), "must not be empty"),
        (json!(["secret", "secret"]), "duplicate excluded column"),
    ] {
        let mut value = example_value();
        value["selected_objects"][0]["exclude_columns"] = names;
        let Err(DefinitionError::Validation(errors)) = parse_value(&value) else {
            panic!("invalid exclusions accepted");
        };
        assert!(
            errors.iter().any(|error| error
                .path
                .starts_with("selected_objects[0].exclude_columns")
                && error.message.contains(expected)),
            "{errors:?}"
        );
    }
    for kind in [
        "sequence",
        "view",
        "materialized-view",
        "function",
        "type",
        "extension",
    ] {
        let mut value = example_value();
        value["selected_objects"][0]["kind"] = json!(kind);
        value["selected_objects"][0]["exclude_columns"] = json!(["private_payload"]);
        assert!(
            parse_value(&value)
                .unwrap_err()
                .to_string()
                .contains("column exclusions are allowed only for tables")
        );
    }
}

#[test]
fn column_exclusions_do_not_change_object_identity_validation() {
    let mut value = example_value();
    let mut same = value["selected_objects"][0].clone();
    same["exclude_columns"] = json!(["private_payload"]);
    value["selected_objects"]
        .as_array_mut()
        .unwrap()
        .push(same.clone());
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("duplicate selected object")
    );
    value["selected_objects"].as_array_mut().unwrap().pop();
    value["excluded_objects"][0]["object"] = same;
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("both selected and excluded")
    );
    let mut value = example_value();
    let mut duplicate = value["excluded_objects"][0].clone();
    duplicate["object"]["exclude_columns"] = json!(["private_payload"]);
    value["excluded_objects"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert!(
        parse_value(&value)
            .unwrap_err()
            .to_string()
            .contains("duplicate excluded object")
    );
}

#[test]
fn capability_report_lists_column_exclusion_names_per_table() {
    let mut value = example_value();
    value["selected_objects"][0]["exclude_columns"] = json!(["private_payload", "internal_note"]);
    let definition = parse_value(&value).unwrap();
    let report = CapabilityReport::from_definition(&definition).unwrap();
    let entry = report
        .items
        .iter()
        .find(|item| item.id == "selected_objects[0].exclude_columns")
        .unwrap();
    assert_eq!(entry.object.as_ref(), Some(&definition.selected_objects[0]));
    assert_eq!(entry.status, ItemStatus::Supported);
    assert_eq!(entry.criticality, Criticality::Advisory);
    assert!(entry.evidence[0].description.contains("private_payload"));
    assert!(entry.evidence[0].description.contains("internal_note"));
    assert!(entry.evidence[0].description.contains("No column values"));
}

#[test]
fn destination_acknowledgments_are_optional_strict_and_digest_compatible() {
    let mut value = example_value();
    let old = parse_value(&value).unwrap();
    assert!(
        old.fence_plan
            .acknowledged_destination_residual_risks
            .is_empty()
    );
    // README fixture predates the field. Its canonical JSON must remain exact.
    let mut legacy = value.clone();
    legacy["source"]["tls"] = json!("require");
    legacy["destination"]["tls"] = json!("require");
    legacy["hooks"][0]["phase"] = json!("pre-fence");
    assert_eq!(serde_json::to_value(&old).unwrap(), legacy);
    let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
    use sha2::{Digest, Sha256};
    let legacy_digest = format!("{:x}", Sha256::digest(&legacy_bytes));
    assert_eq!(
        legacy_digest,
        "7367b3e1e9642c44fbdaa0a4932f2e68d501646cb3bd520171864bedf19e425a"
    );
    assert_eq!(old.canonical_digest().unwrap(), legacy_digest);
    value["fence_plan"]["acknowledged_destination_residual_risks"] = json!([]);
    assert_eq!(
        parse_value(&value).unwrap().canonical_digest().unwrap(),
        legacy_digest
    );
    value["fence_plan"]["acknowledged_destination_residual_risks"] =
        json!(["fence-v1:table-owner:64657374:732e74"]);
    let new = parse_value(&value).unwrap();
    assert_eq!(
        new.fence_plan.acknowledged_destination_residual_risks.len(),
        1
    );
    assert_ne!(new.canonical_digest().unwrap(), legacy_digest);
    value["fence_plan"]["acknowledged_destination_residual_risks"] = json!([""]);
    assert!(parse_value(&value).is_err());
    value["fence_plan"]["acknowledged_destination_residual_risks"] = json!(null);
    assert!(parse_value(&value).is_err());
    value["fence_plan"]["acknowledged_destination_residual_risks"] = json!([]);
    value["fence_plan"]["unknown_destination_field"] = json!([]);
    assert!(matches!(parse_value(&value), Err(DefinitionError::Json(_))));
}

#[tokio::test]
async fn live_spanner_seam_without_connections_preserves_offline_report() {
    for dialect in ["google-sql", "postgresql"] {
        for side in ["source", "destination"] {
            let mut value = example_value();
            value[side] = spanner_connection(dialect);
            let definition = parse_value(&value).unwrap();
            let offline = CapabilityReport::from_definition(&definition).unwrap();
            let live = CapabilityReport::inspect_spanner(
                &definition,
                None,
                &skedaddle::spanner::preflight::Options::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(offline, live);
        }
    }
}

fn mysql_connection() -> Value {
    json!({"engine":"mysql", "version":"8.4", "tls_ca_ref":"env:MYSQL_CA",
        "mysql":{"host":"localhost", "port":3306, "user":"env:MYSQL_USER",
        "password":"env:MYSQL_PASSWORD", "gtid_mode":"ON"}})
}

#[test]
fn mysql_endpoint_validates_strict_shape_and_policy() {
    let mut value = example_value();
    value["source"] = mysql_connection();
    let definition = parse_value(&value).unwrap();
    assert_eq!(definition.source.tls, TlsPolicy::Required);
    for (field, invalid) in [
        ("port", json!(0)),
        ("port", json!(65536)),
        ("host", json!(" padded")),
        ("user", json!("inline-secret")),
        ("password", json!("inline-secret")),
        ("gtid_mode", json!("ON_PERMISSIVE")),
        ("unknown", json!(true)),
    ] {
        let mut invalid_value = value.clone();
        invalid_value["source"]["mysql"][field] = invalid;
        let error = parse_value(&invalid_value).unwrap_err().to_string();
        assert!(!error.contains("inline-secret"));
    }
    for version in ["8", "8.0.40", "8.4.7", "9.0", "MariaDB"] {
        value["source"]["version"] = json!(version);
        assert!(parse_value(&value).is_err());
    }
    value["source"]["version"] = json!("8.0");
    value["source"]
        .as_object_mut()
        .unwrap()
        .remove("tls_ca_ref");
    assert!(parse_value(&value).is_err());
    value["source"]["tls"] = json!("disable-for-local-test");
    assert!(parse_value(&value).is_ok());
    value["source"]["engine"] = json!("postgresql");
    assert!(parse_value(&value).is_err());
    let mut changed = definition.clone();
    changed.source.mysql.as_mut().unwrap().gtid_mode =
        Some(skedaddle::definition::MysqlGtidMode::Off);
    assert_ne!(
        definition.canonical_digest().unwrap(),
        changed.canonical_digest().unwrap()
    );
}

#[test]
fn mysql_offline_capability_blocks_and_endpoint_debug_is_redacted() {
    let mut value = example_value();
    value["source"] = mysql_connection();
    let definition = parse_value(&value).unwrap();
    let debug = format!("{:?}", definition.source);
    assert!(!debug.contains("MYSQL_USER"));
    assert!(!debug.contains("MYSQL_PASSWORD"));
    assert!(debug.contains("[REDACTED]"));
    let report = CapabilityReport::from_definition(&definition).unwrap();
    let items: Vec<_> = report
        .items
        .iter()
        .filter(|i| i.id.starts_with("source.mysql."))
        .collect();
    assert_eq!(items.len(), 2);
    assert!(
        items
            .iter()
            .all(|i| i.status == ItemStatus::Unverified && i.criticality == Criticality::Critical)
    );
    assert_eq!(report.blocking().len(), 2);
    value["destination"] = mysql_connection();
    assert!(
        CapabilityReport::from_definition(&parse_value(&value).unwrap())
            .unwrap()
            .blocking()
            .iter()
            .any(|i| i.status == ItemStatus::Unsupported)
    );
}

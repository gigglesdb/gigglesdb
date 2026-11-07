use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use skedaddle::{CapabilityReport, Criticality, DefinitionError, ItemStatus, MigrationDefinition};

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
    for engine in [
        "mysql",
        "mariadb",
        "sqlite",
        "sql-server",
        "oracle",
        "mongodb",
    ] {
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
                2,
                "one blocker and one advisory exclusion"
            );
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
    let path = fixture(&readme_examples()[0]);
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("check")
        .arg(path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let report: CapabilityReport = serde_json::from_slice(&output.stdout).unwrap();
    assert!(!report.blocking().is_empty());
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("check")
        .arg(fixture("{\"typo\":true}"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
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

//! Read-only, fail-closed destination checks against the migration principal.
//!
//! The shared report has no `Verified` status. `SupportedWithTransformation` is
//! its closest affirmative status and here means this individual live check was
//! verified, not that the destination or migration as a whole is supported.

use std::collections::BTreeSet;

use tokio_postgres::{Client, SimpleQueryMessage};

use crate::{
    capability::{CapabilityItem, Criticality, Evidence, ItemStatus},
    definition::{Connection, Engine, GigglesdbDeployment, ObjectKind, ObjectSelector},
    pg::tls,
};

pub const REQUIRED_DESTINATION_CAPABILITIES: &[&str] = &[
    "add_column_if_not_exists",
    "role_session_termination",
    "server_encoding",
    "union_common_type",
];

/// `config` contains the caller's resolved connection secret and CA path, just
/// as for PostgreSQL. No secret resolution or ambient DSN lookup occurs here.
/// Connection/TLS failures are returned; inspection failures become blockers.
pub async fn check(
    definition_side: &Connection,
    selected: &[ObjectSelector],
    config: &tls::Config,
) -> Result<Vec<CapabilityItem>, tls::Error> {
    check_with_required_capabilities_for_test(
        definition_side,
        selected,
        config,
        REQUIRED_DESTINATION_CAPABILITIES,
    )
    .await
}

/// Live qualification seam for integration tests to require an absent token.
/// Production callers must use `check`, whose requirement set is fixed above.
#[doc(hidden)]
pub async fn check_with_required_capabilities_for_test(
    definition_side: &Connection,
    selected: &[ObjectSelector],
    config: &tls::Config,
    required: &[&str],
) -> Result<Vec<CapabilityItem>, tls::Error> {
    assert_eq!(definition_side.engine, Engine::Gigglesdb);
    let deployment = definition_side
        .gigglesdb
        .as_ref()
        .expect("definition validation requires a GigglesDB endpoint")
        .deployment;
    match deployment {
        GigglesdbDeployment::Embedded => {
            return Ok(vec![item(
                "deployment",
                false,
                "definition.destination.gigglesdb.deployment",
                "embedded preflight lands with GD-0065 N6".into(),
            )]);
        }
        GigglesdbDeployment::Distributed => {
            unreachable!("definition validation rejects distributed GigglesDB")
        }
        GigglesdbDeployment::Server => {}
    }
    let (client, driver, _) = tls::connect(config, &definition_side.tls).await?;
    // Aborting on drop also covers cancellation of the preflight future.
    let _driver = DriverGuard(tokio::spawn(driver));
    let version = match scalar(&client, "SELECT version()").await {
        Ok(actual) => version_item(&definition_side.version, &actual),
        Err(error) => failed_query("version_pin", "SELECT version()", &error),
    };
    let capabilities = match scalar(&client, "SHOW giggles_capabilities").await {
        Ok(actual) => capabilities_item(&actual, required),
        Err(error) => item(
            "capabilities",
            false,
            "SHOW giggles_capabilities",
            if error == "SQLSTATE 42704" {
                "engine predates GD-0077 E2d (SQLSTATE 42704)".into()
            } else {
                format!("Capability inspection failed: {error}")
            },
        ),
    };
    let encoding = match scalar(&client, "SHOW server_encoding").await {
        Ok(actual) => item(
            "encoding",
            actual == "UTF8",
            "SHOW server_encoding",
            format!("Required UTF8; observed {actual:?}"),
        ),
        Err(error) => failed_query("encoding", "SHOW server_encoding", &error),
    };
    let scope = column_scope(&client, selected).await;
    Ok(vec![
        version,
        capabilities,
        encoding,
        item(
            "deployment",
            true,
            "definition.destination.gigglesdb.deployment; TLS-policy-aware connection",
            "Server deployment; connected to the configured endpoint.".into(),
        ),
        scope,
    ])
}

struct DriverGuard(tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>);

impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn item(id: &str, verified: bool, source: &str, description: String) -> CapabilityItem {
    CapabilityItem {
        id: format!("destination.gigglesdb.{id}"),
        object: None,
        status: if verified {
            ItemStatus::SupportedWithTransformation
        } else {
            ItemStatus::RequiresExternalChange
        },
        sampled: false,
        criticality: Criticality::Critical,
        remediation: format!(
            "Resolve the reported GigglesDB {id} requirements and rerun live destination preflight."
        ),
        evidence: vec![Evidence {
            source: source.into(),
            description,
        }],
    }
}

fn failed_query(id: &str, sql: &str, error: &str) -> CapabilityItem {
    item(id, false, sql, format!("Inspection failed: {error}"))
}

fn qualified_build(version: &str) -> Option<&str> {
    let (postgres, engine) = version
        .strip_prefix("PostgreSQL ")?
        .split_once(" (GigglesDB ")?;
    if postgres.is_empty() || !postgres.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return None;
    }
    let (release, build) = engine.strip_suffix(')')?.split_once('+')?;
    let components: Vec<_> = release.split('.').collect();
    if components.len() != 3
        || components
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    (build == "unknown" || (build.len() == 12 && build.bytes().all(|b| b.is_ascii_hexdigit())))
        .then_some(build)
}

fn version_item(expected: &str, actual: &str) -> CapabilityItem {
    let build = qualified_build(actual);
    let reason = match build {
        Some("unknown") => "; unidentified build",
        None => "; invalid qualified GigglesDB version string",
        _ if actual != expected => "; version pin mismatch",
        _ => "",
    };
    item(
        "version_pin",
        actual == expected && matches!(build, Some(build) if build != "unknown"),
        "SELECT version(); definition.destination.version",
        format!("Expected {expected:?}; observed {actual:?}{reason}"),
    )
}

fn parse_capabilities(value: &str) -> Option<BTreeSet<&str>> {
    if value.is_empty() {
        return Some(BTreeSet::new());
    }
    value
        .split(',')
        .map(str::trim)
        .map(|token| {
            (!token.is_empty()
                && token
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'))
            .then_some(token)
        })
        .collect()
}

fn capabilities_item(actual: &str, required: &[&str]) -> CapabilityItem {
    let Some(tokens) = parse_capabilities(actual) else {
        return item(
            "capabilities",
            false,
            "SHOW giggles_capabilities",
            format!("Malformed capability token list: {actual:?}"),
        );
    };
    let missing: Vec<_> = required
        .iter()
        .filter(|token| !tokens.contains(**token))
        .copied()
        .collect();
    item(
        "capabilities",
        missing.is_empty(),
        "SHOW giggles_capabilities",
        format!("Observed {actual:?}; missing required tokens: {missing:?}"),
    )
}

// Simple Query uses text results, including SHOW, without assuming server OIDs.
async fn rows(client: &Client, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let messages = client.simple_query(sql).await.map_err(|error| {
        error
            .code()
            .map(|code| format!("SQLSTATE {}", code.code()))
            .unwrap_or_else(|| "connection/protocol error (details redacted)".into())
    })?;
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| {
                        row.get(index)
                            .map(str::to_owned)
                            .ok_or_else(|| "NULL inspection result".into())
                    })
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn scalar(client: &Client, sql: &str) -> Result<String, String> {
    let mut values = rows(client, sql).await?;
    if values.len() != 1 || values[0].len() != 1 {
        return Err("expected exactly one inspection value".into());
    }
    Ok(values.remove(0).remove(0))
}

fn literal(value: &str) -> String {
    // E strings escape backslashes independently of standard_conforming_strings.
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn qualified(table: &ObjectSelector) -> String {
    format!(
        "\"{}\".\"{}\"",
        table.schema.replace('"', "\"\""),
        table.name.replace('"', "\"\"")
    )
}

async fn privilege(client: &Client, sql: &str) -> Result<bool, String> {
    match scalar(client, sql).await?.as_str() {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err("invalid privilege inspection result".into()),
    }
}

async fn column_scope(client: &Client, selected: &[ObjectSelector]) -> CapabilityItem {
    let mut evidence = Vec::new();
    let mut verified = true;
    for table in selected
        .iter()
        .filter(|table| table.kind == ObjectKind::Table)
    {
        let name = qualified(table);
        // Inventory the catalog independently of SELECT access, so unreadable
        // columns remain in scope unless explicitly excluded.
        let sql = format!(
            "SELECT a.attname FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = {} AND c.relname = {} \
             AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            literal(&table.schema),
            literal(&table.name),
        );
        let columns = match rows(client, &sql).await {
            Ok(columns) if !columns.is_empty() && columns.iter().all(|row| row.len() == 1) => {
                columns
                    .into_iter()
                    .map(|mut row| row.remove(0))
                    .filter(|column| !table.exclude_columns.contains(column))
                    .collect::<Vec<_>>()
            }
            result => {
                verified = false;
                evidence.push(Evidence {
                    source: sql,
                    description: format!(
                        "{name}: column scope cannot be verified; complete column inventory unavailable ({result:?})"
                    ),
                });
                continue;
            }
        };
        let result = inspect_privileges(client, table, &columns).await;
        let (passed, description) = match result {
            Ok(problems) => (
                problems.is_empty(),
                format!(
                    "{name}: inspected selected columns {columns:?}; {}",
                    problems.join("; ")
                ),
            ),
            Err(error) => (
                false,
                format!("{name}: column scope cannot be verified; {error}"),
            ),
        };
        verified &= passed;
        evidence.push(Evidence {
            source: format!("{sql}; has_column_privilege/has_table_privilege probes"),
            description,
        });
    }
    let mut result = item(
        "column_scope",
        verified,
        "selected table inventory",
        "No selected tables require column privilege inspection.".into(),
    );
    if !evidence.is_empty() {
        result.evidence = evidence;
    }
    result
}

async fn inspect_privileges(
    client: &Client,
    table: &ObjectSelector,
    columns: &[String],
) -> Result<Vec<String>, String> {
    let name = qualified(table);
    let mut problems = Vec::new();
    // GigglesDB has table ACLs but no column-level GRANT. Use the engine
    // helpers for effective privileges, and fail closed on any inspection error.
    for operation in ["INSERT", "UPDATE", "DELETE"] {
        let sql = format!(
            "SELECT has_table_privilege(current_user, {}, '{operation}')",
            literal(&name),
        );
        match privilege(client, &sql).await {
            Ok(true) => {}
            Ok(false) => problems.push(format!("missing {operation} on table {name}")),
            Err(error) => return Err(format!("has_table_privilege: {error}")),
        }
    }
    for column in columns {
        let sql = format!(
            "SELECT has_column_privilege(current_user, {}, {}, 'SELECT')",
            literal(&name),
            literal(column),
        );
        match privilege(client, &sql).await {
            Ok(true) => {}
            Ok(false) => problems.push(format!("unreadable column {name}.{column}")),
            Err(error) => return Err(format!("has_column_privilege: {error}")),
        }
    }
    Ok(problems)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION: &str = "PostgreSQL 18.0 (GigglesDB 0.1.0+a6719f650b7d)";

    #[test]
    fn qualified_version_requires_engine_release_and_build() {
        assert_eq!(qualified_build(VERSION), Some("a6719f650b7d"));
        for invalid in [
            "PostgreSQL 18.0",
            "GigglesDB 0.1.0+a6719f650b7d",
            "PostgreSQL 18.0 (GigglesDB 0.1+a6719f650b7d)",
            "PostgreSQL 18.0 (GigglesDB 0.1.0+abc)",
            "PostgreSQL 18.0 (GigglesDB 0.1.0+zzzzzzzzzzzz)",
        ] {
            assert_eq!(qualified_build(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn exact_version_and_unidentified_build() {
        assert_eq!(
            version_item(VERSION, VERSION).status,
            ItemStatus::SupportedWithTransformation
        );
        let mismatch = version_item("wrong version", VERSION);
        assert_eq!(mismatch.status, ItemStatus::RequiresExternalChange);
        assert!(mismatch.evidence[0].description.contains(VERSION));
        assert!(mismatch.evidence[0].description.contains("wrong version"));
        let unknown = "PostgreSQL 18.0 (GigglesDB 0.1.0+unknown)";
        let blocked = version_item(unknown, unknown);
        assert_eq!(blocked.status, ItemStatus::RequiresExternalChange);
        assert!(
            blocked.evidence[0]
                .description
                .contains("unidentified build")
        );
    }

    #[test]
    fn capability_tokens_are_trimmed_deduplicated_and_order_independent() {
        assert_eq!(
            parse_capabilities(" b,a, a "),
            Some(BTreeSet::from(["a", "b"]))
        );
        assert_eq!(parse_capabilities(""), Some(BTreeSet::new()));
        for invalid in ["a,,b", "a,", "a,b c", "A"] {
            assert!(parse_capabilities(invalid).is_none());
        }
        let actual = REQUIRED_DESTINATION_CAPABILITIES.join(",");
        assert_eq!(
            capabilities_item(&actual, REQUIRED_DESTINATION_CAPABILITIES).status,
            ItemStatus::SupportedWithTransformation
        );
        let blocked = capabilities_item(&actual, &["made_up_token"]);
        assert_eq!(blocked.status, ItemStatus::RequiresExternalChange);
        assert!(blocked.evidence[0].description.contains("made_up_token"));
    }
}

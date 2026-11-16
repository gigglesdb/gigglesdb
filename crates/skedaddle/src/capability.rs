//! Offline declarations and explicit live Spanner inspection. An accepted
//! definition or emulator result is not proof of production readiness.

use serde::{Deserialize, Serialize};

use crate::definition::{
    DefinitionError, Engine, MigrationDefinition, ObjectSelector, postgres_major,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemStatus {
    Supported,
    SupportedWithTransformation,
    RequiresExternalChange,
    Unsupported,
    Unverified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Criticality {
    Critical,
    Advisory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub source: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityItem {
    pub id: String,
    pub object: Option<ObjectSelector>,
    pub status: ItemStatus,
    /// True means evidence came from a sample rather than complete inspection.
    pub sampled: bool,
    pub criticality: Criticality,
    pub remediation: String,
    pub evidence: Vec<Evidence>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReport {
    pub definition_digest: String,
    pub items: Vec<CapabilityItem>,
}

impl CapabilityReport {
    /// Explicit live inspection seam. Missing connections preserve the offline
    /// declarations; inspection errors fail closed and never qualify production.
    /// Caller binds the destination DatabaseClient to the declared database (the
    /// official client exposes no database-name accessor).
    pub async fn inspect_spanner(
        definition: &MigrationDefinition,
        source: Option<&crate::spanner::client::Client>,
        source_options: &crate::spanner::preflight::Options,
        destination: Option<(
            &crate::pg::preflight::Snapshot,
            &crate::spanner_dest::apply::Destination,
            &crate::spanner_dest::preflight::Options,
        )>,
    ) -> Result<Self, crate::spanner::client::Error> {
        let mut report = Self::from_definition(definition)?;
        let replace = |items: &mut Vec<CapabilityItem>, side: &str| {
            items.retain(|i| {
                ![
                    "connector_cdc_ordering",
                    "snapshot_consistency",
                    "schema_type_mapping",
                ]
                .iter()
                .any(|c| i.id == format!("{side}.spanner.{c}"))
            });
        };
        if let Some(client) = source {
            let declared = definition
                .source
                .spanner
                .as_ref()
                .ok_or("source is not Spanner")?;
            if definition.source.engine != Engine::Spanner
                || client.snapshot_connection().1 != declared
            {
                return Err("live source endpoint differs from definition".into());
            }
            let source_report = crate::spanner::preflight::inspect(
                client,
                &definition.selected_objects,
                source_options,
            )
            .await?;
            replace(&mut report.items, "source");
            report.items.extend(source_report.capability_items());
        }
        if let Some((snapshot, connection, options)) = destination {
            let declared = definition
                .destination
                .spanner
                .as_ref()
                .ok_or("destination is not Spanner")?;
            if definition.destination.engine != Engine::Spanner
                || connection.dialect != declared.dialect
            {
                return Err("live destination endpoint differs from definition".into());
            }
            let destination_report = crate::spanner_dest::preflight::inspect(
                snapshot,
                &definition.selected_objects,
                connection,
                options,
            )
            .await?;
            replace(&mut report.items, "destination");
            report.items.extend(destination_report.capability_items());
        }
        Ok(report)
    }
    /// All critical items except the two affirmative statuses block preflight,
    /// regardless of whether evidence was sampled.
    pub fn blocking(&self) -> Vec<&CapabilityItem> {
        self.items
            .iter()
            .filter(|item| {
                item.criticality == Criticality::Critical
                    && !matches!(
                        item.status,
                        ItemStatus::Supported | ItemStatus::SupportedWithTransformation
                    )
            })
            .collect()
    }

    pub fn from_definition(definition: &MigrationDefinition) -> Result<Self, DefinitionError> {
        definition.ensure_valid()?;
        let mut items = Vec::new();
        if let Err(crate::coordinator::Error::BudgetExceeded(blocker)) =
            crate::coordinator::budget::preflight(definition)
        {
            items.push(item(
                &blocker.id(),
                ItemStatus::RequiresExternalChange,
                Criticality::Critical,
                &blocker.remediation,
                format!(
                    "Estimated normalized definition: {} bytes; local budget: {} bytes.",
                    blocker.used, blocker.limit
                ),
            ));
        }
        let unsupported: Vec<_> = [
            ("source", &definition.source),
            ("destination", &definition.destination),
        ]
        .into_iter()
        .filter(|(side, connection)| {
            !matches!(connection.engine, Engine::Postgresql | Engine::Spanner)
                && !(*side == "source" && connection.engine == Engine::Mysql)
        })
        .map(|(side, connection)| format!("{side}: {}", connection.engine))
        .collect();

        if !unsupported.is_empty() {
            // One aggregate critical item even when both endpoints have no
            // connector. Do not create misleading per-object checks after this.
            items.push(item(
                "connector.unsupported",
                ItemStatus::Unsupported,
                Criticality::Critical,
                "Use PostgreSQL on both endpoints for M1, or implement and qualify the required connector.",
                format!("M1 declares only a PostgreSQL connector; no connector for {}.", unsupported.join(", ")),
            ));
        }
        for (side, connection) in [
            ("source", &definition.source),
            ("destination", &definition.destination),
        ] {
            match connection.engine {
                Engine::Postgresql
                    if !matches!(postgres_major(&connection.version), Some(16..=18)) =>
                {
                    items.push(item(
                        &format!("{side}.version"),
                        ItemStatus::Unsupported,
                        Criticality::Critical,
                        "Use a PostgreSQL 16, 17, or 18 server for the M1 qualification matrix.",
                        format!(
                            "Declared {side} PostgreSQL version: {}.",
                            connection.version
                        ),
                    ));
                }
                Engine::Mysql if side == "source" => {
                    for capability in ["preflight", "mapping"] {
                        items.push(item(
                            &format!("source.mysql.{capability}"), ItemStatus::Unverified,
                            Criticality::Critical,
                            "Run live MySQL preflight with explicit copy/catch-up estimates and selected-column mapping inspection.",
                            "No live MySQL evidence has been collected; declarations do not qualify source capabilities.".into(),
                        ));
                    }
                }
                Engine::Spanner => {
                    for (capability, remediation) in [
                        (
                            "connector_cdc_ordering",
                            "The Spanner connector and CDC ordering are not yet built or qualified; implement and qualify connector-defined ordering before migration.",
                        ),
                        (
                            "snapshot_consistency",
                            "Spanner snapshot consistency is not yet built or qualified; implement and qualify consistent snapshot capture before migration.",
                        ),
                        (
                            "schema_type_mapping",
                            "Spanner schema/type mapping is not yet built or qualified; implement and qualify schema and type translation before migration.",
                        ),
                        (
                            "fence_cutover_reverse",
                            "Spanner fence/cutover/reverse qualification is not yet built or qualified; implement and qualify these protocols before migration.",
                        ),
                    ] {
                        items.push(item(
                            &format!("{side}.spanner.{capability}"),
                            ItemStatus::Unverified,
                            Criticality::Critical,
                            remediation,
                            format!("Declared {side} Spanner endpoint; no connector qualification evidence has been collected."),
                        ));
                    }
                    if connection
                        .spanner
                        .as_ref()
                        .is_some_and(|endpoint| endpoint.emulator)
                    {
                        items.push(item(
                            &format!("{side}.spanner.emulator"),
                            ItemStatus::Unverified,
                            Criticality::Critical,
                            "Spanner production qualification is not yet built or qualified; emulator results stay unverified and cannot promote a capability status.",
                            format!("Declared {side} Spanner emulator; emulator results do not qualify production behavior."),
                        ));
                    }
                }
                _ => {}
            }
        }
        // These legacy claims apply only to a PostgreSQL-to-PostgreSQL migration.
        if definition.source.engine == Engine::Postgresql
            && definition.destination.engine == Engine::Postgresql
        {
            items.push(item(
                "postgresql.preflight",
                ItemStatus::Unverified,
                Criticality::Critical,
                "Run live connector qualification, catalog inspection, pgoutput/replication checks, buffer create-only checks, fence and rollback verification when implemented.",
                "P1 performs offline definition checks only; no database, buffer, hook, or secret has been accessed.".to_owned(),
            ));
            for (index, object) in definition.selected_objects.iter().enumerate() {
                let mut entry = item(
                    &format!("selected_objects[{index}]"),
                    ItemStatus::Unverified,
                    Criticality::Critical,
                    "Inspect the object's catalog, data types, dependencies, and replication identity with the PostgreSQL connector.",
                    "Object is selected by the definition; no catalog or data has been inspected."
                        .to_owned(),
                );
                entry.object = Some(object.clone());
                items.push(entry);
            }
        }

        for (index, object) in definition.selected_objects.iter().enumerate() {
            if !object.exclude_columns.is_empty() {
                let mut entry = item(
                    &format!("selected_objects[{index}].exclude_columns"),
                    ItemStatus::Supported,
                    Criticality::Advisory,
                    "Review excluded column names; live preparation must retain every operational and replica-identity key.",
                    format!(
                        "Explicitly excluded columns: {}. No column values are inspected or reported.",
                        serde_json::to_string(&object.exclude_columns)
                            .expect("column names serialize")
                    ),
                );
                entry.object = Some(object.clone());
                items.push(entry);
            }
        }

        for (side, connection) in [
            ("source", &definition.source),
            ("destination", &definition.destination),
        ] {
            if connection.engine == Engine::Spanner {
                continue;
            }
            let trust = tls_trust(connection);
            items.push(item(
                &format!("{side}.tls_trust"),
                ItemStatus::Unverified,
                Criticality::Advisory,
                "Resolve the explicit CA reference to a PEM bundle path and qualify certificate and hostname verification before migration.",
                format!(
                    "CA configured: {}; reference kind: {}. Trust material has not been resolved or verified.",
                    trust.ca_configured,
                    trust.reference_kind.unwrap_or("none")
                ),
            ));
        }

        for (index, excluded) in definition.excluded_objects.iter().enumerate() {
            let mut entry = item(
                &format!("excluded_objects[{index}]"),
                ItemStatus::Supported,
                Criticality::Advisory,
                "Keep the exclusion reason in migration review; excluded objects are not migrated.",
                format!("Explicit exclusion reason: {}", excluded.reason),
            );
            entry.object = Some(excluded.object.clone());
            items.push(entry);
        }

        Ok(Self {
            definition_digest: definition
                .canonical_digest()
                .map_err(DefinitionError::Json)?,
            items,
        })
    }
}

fn item(
    id: &str,
    status: ItemStatus,
    criticality: Criticality,
    remediation: &str,
    description: String,
) -> CapabilityItem {
    CapabilityItem {
        id: id.to_owned(),
        object: None,
        status,
        sampled: false,
        criticality,
        remediation: remediation.to_owned(),
        evidence: vec![Evidence {
            source: "migration-definition".to_owned(),
            description,
        }],
    }
}

/// Safe offline trust summary: neither the identifier nor its resolved value
/// crosses the reporting boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TlsTrust {
    pub ca_configured: bool,
    pub reference_kind: Option<&'static str>,
}

pub fn tls_trust(connection: &crate::definition::Connection) -> TlsTrust {
    TlsTrust {
        ca_configured: connection.tls_ca_ref.is_some(),
        reference_kind: connection.tls_ca_ref.as_ref().map(|reference| {
            if reference.as_str().starts_with("env:") {
                "env"
            } else {
                "file"
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mysql_offline_items_block_without_resolving_secrets() {
        let readme = include_str!("../README.md");
        let json = readme
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        let mut value: serde_json::Value = serde_json::from_str(json).unwrap();
        value["source"] = serde_json::json!({"engine":"mysql","version":"8.0","tls_ca_ref":"env:UNRESOLVED_CA", "mysql":{"host":"localhost","port":3306,"user":"env:UNRESOLVED_USER","password":"env:UNRESOLVED_PASSWORD"}});
        let definition =
            MigrationDefinition::from_json(&serde_json::to_string(&value).unwrap()).unwrap();
        let report = CapabilityReport::from_definition(&definition).unwrap();
        assert_eq!(report.blocking().len(), 2);
        assert!(
            report
                .blocking()
                .iter()
                .all(|i| i.status == ItemStatus::Unverified)
        );
    }
}

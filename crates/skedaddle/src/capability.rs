//! Offline capability declarations. An accepted definition is not proof of
//! database readiness; P1 cannot produce live inspection evidence.

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
        .filter(|(_, connection)| connection.engine != Engine::Postgresql)
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
        } else {
            for (side, connection) in [
                ("source", &definition.source),
                ("destination", &definition.destination),
            ] {
                if !matches!(postgres_major(&connection.version), Some(16..=18)) {
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
            }
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

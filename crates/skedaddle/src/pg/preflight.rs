//! Read-only PostgreSQL 16–18 source preflight (plan §3.7).
//!
//! Collection and classification are deliberately separate. Catalog inspection
//! cannot prove disk free space, trigger side effects, or future readiness; these
//! remain explicit blockers until supplied with operator evidence. A source
//! report alone never establishes §3.12 cutover readiness.

mod queries;

use std::collections::BTreeMap;

use serde_json::Value;
use tokio_postgres::{Client, IsolationLevel, Transaction};

use crate::{
    capability::{CapabilityItem, CapabilityReport, Criticality, Evidence, ItemStatus},
    definition::ObjectSelector,
};

/// Mapping seam: pass the P4 mapping's domain decision, including typmod,
/// domains, arrays, enums and extension ownership. No mapping internals assumed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnType {
    pub schema: String,
    pub name: String,
    pub oid: u32,
    pub typmod: i32,
    pub kind: String,
    pub base_oid: u32,
    pub element_oid: u32,
    pub formatted: String,
}

#[derive(Clone, Debug)]
pub struct DiskHeadroom {
    /// Measured on the source WAL filesystem, not the coordinator's disk.
    pub free_bytes: u64,
    pub required_bytes: u64,
    pub evidence: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerEffects {
    /// SQL effects are captured on other explicitly selected tables.
    Replicated,
    ExternalSideEffects,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub required_slots: u64,
    pub required_wal_senders: u64,
    pub minimum_wal_keep_bytes: u64,
    pub long_transaction_seconds: u64,
    pub wal_disk: Option<DiskHeadroom>,
    /// Keys are `schema.table.trigger`, as reported by the trigger query.
    pub trigger_effects: BTreeMap<String, TriggerEffects>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            required_slots: 1,
            required_wal_senders: 1,
            minimum_wal_keep_bytes: 1024 * 1024 * 1024,
            long_transaction_seconds: 300,
            wal_disk: None,
            trigger_effects: BTreeMap::new(),
        }
    }
}

/// Exact executed SQL and complete JSON result rows, or a redacted SQLSTATE.
/// Query failures block instead of disappearing from the report.
#[derive(Clone, Debug)]
pub struct Observation {
    pub check: Check,
    pub object: Option<ObjectSelector>,
    pub sql: String,
    pub rows: Vec<Value>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub observations: Vec<Observation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Check {
    Version,
    WalLevel,
    SlotHeadroom,
    WalSenderHeadroom,
    WalRetention,
    WalDisk,
    ReplicationPrivilege,
    DatabaseCreate,
    EventTriggerPrivilege,
    OriginGrants,
    Table,
    Key,
    ReplicaIdentity,
    NullableKeyDuplicates,
    MutableKeys,
    ColumnTypes,
    Sequences,
    GeneratedColumns,
    IdentityColumns,
    Triggers,
    Views,
    Routines,
    Partitions,
    Grants,
    Extensions,
    LargeObjects,
    PreparedTransactions,
    LongTransactions,
    RestartLocks,
    SelectedObject,
}

impl Check {
    pub fn id(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::WalLevel => "wal_level",
            Self::SlotHeadroom => "slot_headroom",
            Self::WalSenderHeadroom => "wal_sender_headroom",
            Self::WalRetention => "wal_retention",
            Self::WalDisk => "wal_disk",
            Self::ReplicationPrivilege => "replication_privilege",
            Self::DatabaseCreate => "database_create",
            Self::EventTriggerPrivilege => "event_trigger_privilege",
            Self::OriginGrants => "origin_grants",
            Self::Table => "table",
            Self::Key => "key",
            Self::ReplicaIdentity => "replica_identity",
            Self::NullableKeyDuplicates => "nullable_key_duplicates",
            Self::MutableKeys => "mutable_keys",
            Self::ColumnTypes => "column_types",
            Self::Sequences => "sequences",
            Self::GeneratedColumns => "generated_columns",
            Self::IdentityColumns => "identity_columns",
            Self::Triggers => "triggers",
            Self::Views => "views",
            Self::Routines => "routines",
            Self::Partitions => "partitions",
            Self::Grants => "grants",
            Self::Extensions => "extensions",
            Self::LargeObjects => "large_objects",
            Self::PreparedTransactions => "prepared_transactions",
            Self::LongTransactions => "long_transactions",
            Self::RestartLocks => "restart_locks",
            Self::SelectedObject => "selected_object",
        }
    }

    fn remediation(self) -> &'static str {
        match self {
            Self::Version => "Use PostgreSQL 16, 17 or 18; qualify newer majors before migration.",
            Self::WalLevel => {
                "Use the writable primary; set wal_level=logical and restart PostgreSQL before starting capture."
            }
            Self::SlotHeadroom => {
                "Reserve a replication slot for this migration; raise max_replication_slots and restart if necessary. Never drop another owner's slot."
            }
            Self::WalSenderHeadroom => {
                "Reserve a WAL sender for capture; raise max_wal_senders and restart if necessary."
            }
            Self::WalRetention => {
                "Size max_slot_wal_keep_size for the outage/rollback budget and monitor retained WAL; -1 is unlimited and requires disk monitoring."
            }
            Self::WalDisk => {
                "Measure free bytes on the source pg_wal filesystem and provide dated evidence plus the required retention budget; PostgreSQL has no built-in free-space query."
            }
            Self::ReplicationPrivilege => {
                "Use a LOGIN role with REPLICATION (or a dedicated privileged installer); REPLICATION is not inherited through role membership."
            }
            Self::DatabaseCreate => "GRANT CREATE ON DATABASE to the publication installer.",
            Self::EventTriggerPrivilege => {
                "On PostgreSQL 16/17/18 a superuser must install the DDL event trigger; otherwise explicitly select and qualify the forbid-DDL/drift-blocking fallback. Preflight installs nothing."
            }
            Self::OriginGrants => {
                "GRANT EXECUTE on each reported pg_catalog.pg_replication_origin_* function to the reverse apply role and recheck that role on the reverse endpoint."
            }
            Self::Table => {
                "Select an existing permanent ordinary/partitioned table; remove RLS restrictions for the capture role or explicitly exclude the table."
            }
            Self::Key => {
                "Add a primary key or a valid immediate non-null unique index (no predicate or expressions), or explicitly exclude this table (D16). REPLICA IDENTITY FULL alone does not supply a chunk key."
            }
            Self::ReplicaIdentity => {
                "Set REPLICA IDENTITY USING INDEX to a suitable non-null unique index, use the primary-key DEFAULT identity, or use FULL with a valid separate chunk key; NOTHING cannot capture updates/deletes."
            }
            Self::NullableKeyDuplicates => {
                "Resolve duplicate nullable unique-key groups, then choose a non-null unique key or primary key; nullable uniqueness alone never satisfies D16."
            }
            Self::MutableKeys => {
                "Treat chunk keys as mutable: capture old/new identities, remove both keys from watermark chunks, and verify key-changing UPDATEs before readiness. A catalog cannot prove application immutability."
            }
            Self::ColumnTypes => {
                "Supply the qualified P4 mapping-domain predicate; transform unsupported columns explicitly or exclude their table. Include domains, arrays, typmods and extension types in qualification."
            }
            Self::Sequences => {
                "GRANT SELECT on each source sequence to the capture role. Inventory sequence ownership and dependencies; preserve settings and synchronize values at the final boundary before destination writes. Source inspection does not prove sequence sync readiness."
            }
            Self::GeneratedColumns => {
                "Preserve generation expressions and recompute on PG16/17. PG18 can publish stored generated columns explicitly; virtual columns must be recomputed. Qualify destination expressions before readiness."
            }
            Self::IdentityColumns => {
                "Preserve ALWAYS/BY DEFAULT identity semantics, override system values during copy where required, and synchronize the owned sequence before activation."
            }
            Self::Triggers => {
                "Classify every user trigger by qualified name: replicated SQL effects must target selected tables and not fire twice on apply; external side effects need an explicit disable/replay plan. Review ALWAYS/REPLICA triggers separately."
            }
            Self::Views => {
                "Recreate views/materialized views in dependency order; refresh materialized views explicitly and qualify destination dependencies or exclude them."
            }
            Self::Routines => {
                "Review routine languages, SECURITY DEFINER, dependencies and external effects; recreate and qualify routines and ACLs on the destination or explicitly exclude them."
            }
            Self::Partitions => {
                "Choose root publication (publish_via_partition_root=true) or an explicit leaf set; include every required descendant, preserve bounds and verify destination routing. Do not publish overlapping roots/leaves accidentally."
            }
            Self::Grants => {
                "Grant schema USAGE, complete table SELECT and publication ownership to the installer/capture role; handle RLS explicitly. Preserve source ACLs/owners/default grants and review broad authority before cutover."
            }
            Self::Extensions => {
                "Install compatible destination extension versions separately and qualify extension-owned objects/types; explicitly exclude extensions outside the migration contract."
            }
            Self::LargeObjects => {
                "Large objects are unsupported: migrate referenced objects to bytea/external storage or explicitly exclude referencing tables. Review OID/domain/array and application references; absence of a catalog dependency is not proof of no references."
            }
            Self::PreparedTransactions => {
                "Resolve prepared transactions with their owning application before creating the logical slot; preflight never commits or rolls them back."
            }
            Self::LongTransactions => {
                "Drain long-running transactions before slot creation; grant pg_read_all_stats for complete visibility, then recheck immediately before capture."
            }
            Self::RestartLocks => {
                "Disclose restart-required settings before the run. Publication/table changes acquire locks, replica-identity changes can require ACCESS EXCLUSIVE, and slot creation waits for old transactions. Schedule installation with lock_timeout and a maintenance window; no locks/configuration are changed here."
            }
            Self::SelectedObject => {
                "Review the explicitly selected object's catalog and dependencies; non-table objects require an explicit schema migration plan before assessment can pass."
            }
        }
    }
}

/// Opens a consistent, read-only catalog snapshot; performs no installs.
pub async fn collect(
    client: &mut Client,
    objects: &[ObjectSelector],
) -> Result<Snapshot, tokio_postgres::Error> {
    let transaction = client
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let snapshot = collect_in(&transaction, objects).await?;
    transaction.rollback().await?;
    Ok(snapshot)
}

/// Inspect within a caller-owned transaction, useful for rollback-only fixtures.
/// Every generated data query is SELECT-only. The caller owns transaction mode.
pub async fn collect_in(
    transaction: &Transaction<'_>,
    objects: &[ObjectSelector],
) -> Result<Snapshot, tokio_postgres::Error> {
    queries::collect(transaction, objects).await
}

pub async fn assess<F: Fn(&ColumnType) -> bool>(
    client: &mut Client,
    definition_digest: &str,
    objects: &[ObjectSelector],
    options: &Options,
    type_supported: F,
) -> Result<CapabilityReport, tokio_postgres::Error> {
    let snapshot = collect(client, objects).await?;
    Ok(classify(
        definition_digest,
        &snapshot,
        options,
        type_supported,
    ))
}

/// Pure classification of complete result rows. Missing/ill-typed evidence
/// fails closed. Mapping decisions are supplied by the caller, never guessed.
pub fn classify<F: Fn(&ColumnType) -> bool>(
    definition_digest: &str,
    snapshot: &Snapshot,
    options: &Options,
    type_supported: F,
) -> CapabilityReport {
    let major = snapshot
        .observations
        .iter()
        .find(|o| o.check == Check::Version)
        .and_then(|o| o.rows.first())
        .and_then(|r| r["version_num"].as_u64())
        .unwrap_or(0)
        / 10000;
    let items = snapshot
        .observations
        .iter()
        .map(|observation| {
            let status = if observation.error.is_some() {
                ItemStatus::Unverified
            } else {
                classify_rows(observation, major, options, &type_supported)
            };
            let criticality = match observation.check {
                Check::MutableKeys | Check::RestartLocks => Criticality::Advisory,
                _ => Criticality::Critical,
            };
            let mut evidence = vec![Evidence {
                source: observation.sql.clone(),
                description: match &observation.error {
                    Some(error) => format!("Query failed: {error}; no successful result."),
                    None => {
                        serde_json::to_string(&observation.rows).expect("JSON values serialize")
                    }
                },
            }];
            if observation.check == Check::WalDisk
                && let Some(disk) = &options.wal_disk
            {
                evidence.push(Evidence {
                    source: "operator:source-WAL-filesystem".into(),
                    description: format!(
                        "free_bytes={}, required_bytes={}; {}",
                        disk.free_bytes, disk.required_bytes, disk.evidence
                    ),
                });
            }
            if observation.check == Check::Triggers {
                evidence.push(Evidence {
                    source: "operator:trigger-effects".into(),
                    description: format!("{:?}", options.trigger_effects),
                });
            }
            CapabilityItem {
                id: format!("pg.{}", observation.check.id()),
                object: observation.object.clone(),
                status,
                sampled: false,
                criticality,
                remediation: observation.check.remediation().into(),
                evidence,
            }
        })
        .collect();
    CapabilityReport {
        definition_digest: definition_digest.into(),
        items,
    }
}

fn verdict(ok: bool) -> ItemStatus {
    if ok {
        ItemStatus::Supported
    } else {
        ItemStatus::RequiresExternalChange
    }
}

fn valid_key(row: &Value) -> bool {
    ["unique", "valid", "ready", "immediate", "not_null"]
        .iter()
        .all(|key| row[*key].as_bool() == Some(true))
        && row["partial"].as_bool() == Some(false)
        && row["expression"].as_bool() == Some(false)
}

fn classify_rows<F: Fn(&ColumnType) -> bool>(
    observation: &Observation,
    major: u64,
    options: &Options,
    mapping: &F,
) -> ItemStatus {
    use ItemStatus::*;
    let rows = &observation.rows;
    let first = rows.first();
    let all =
        |field: &str| !rows.is_empty() && rows.iter().all(|r| r[field].as_bool() == Some(true));
    let empty_or_review = || {
        if rows.is_empty() {
            Supported
        } else {
            Unverified
        }
    };
    match observation.check {
        Check::Version => {
            if (16..=18).contains(&major) {
                Supported
            } else {
                Unsupported
            }
        }
        Check::WalLevel => {
            verdict(first.is_some_and(|r| r["wal_level"] == "logical" && r["in_recovery"] == false))
        }
        Check::SlotHeadroom | Check::WalSenderHeadroom => {
            let needed = if observation.check == Check::SlotHeadroom {
                options.required_slots
            } else {
                options.required_wal_senders
            };
            verdict(
                needed > 0
                    && first
                        .and_then(|r| r["maximum"].as_u64().zip(r["used"].as_u64()))
                        .is_some_and(|(maximum, used)| maximum.saturating_sub(used) >= needed),
            )
        }
        Check::WalRetention => match first.and_then(|r| r["bytes"].as_i64()) {
            Some(-1) => SupportedWithTransformation,
            Some(bytes) => verdict(bytes >= 0 && bytes as u64 >= options.minimum_wal_keep_bytes),
            None => Unverified,
        },
        Check::WalDisk => match &options.wal_disk {
            Some(disk) if !disk.evidence.trim().is_empty() && disk.required_bytes > 0 => {
                verdict(disk.free_bytes >= disk.required_bytes.max(options.minimum_wal_keep_bytes))
            }
            _ => Unverified,
        },
        Check::ReplicationPrivilege => verdict(
            all("login")
                && rows
                    .iter()
                    .all(|r| r["replication"] == true || r["superuser"] == true),
        ),
        Check::DatabaseCreate => verdict(all("create")),
        Check::EventTriggerPrivilege => verdict((16..=18).contains(&major) && all("superuser")),
        Check::OriginGrants => verdict(rows.len() >= 11 && all("execute")),
        Check::Table => {
            if rows.is_empty() {
                Unsupported
            } else {
                verdict(rows.iter().all(|r| {
                    (r["kind"] == "r" || r["kind"] == "p")
                        && r["persistence"] == "p"
                        && r["rls_active"] == false
                }))
            }
        }
        Check::Key => {
            if rows.iter().any(valid_key) {
                Supported
            } else {
                Unsupported
            }
        }
        Check::ReplicaIdentity => verdict(rows.iter().any(|r| {
            r["replica_identity"] == "f"
                || (valid_key(r)
                    && ((r["replica_identity"] == "d" && r["primary"] == true)
                        || (r["replica_identity"] == "i" && r["identity_index"] == true)))
        })),
        Check::NullableKeyDuplicates => verdict(
            rows.iter()
                .all(|r| r["duplicate_groups"].as_u64() == Some(0)),
        ),
        Check::MutableKeys => {
            if rows.iter().any(valid_key) {
                SupportedWithTransformation
            } else {
                Unverified
            }
        }
        Check::ColumnTypes => {
            if rows.is_empty() {
                Unverified
            } else if rows
                .iter()
                .all(|r| column_type(r).is_some_and(|column| mapping(&column)))
            {
                Supported
            } else {
                Unsupported
            }
        }
        Check::Sequences if !rows.is_empty() && !all("select") => RequiresExternalChange,
        Check::Sequences | Check::GeneratedColumns | Check::IdentityColumns => {
            if rows.is_empty() {
                Supported
            } else {
                SupportedWithTransformation
            }
        }
        Check::Triggers => {
            if rows.iter().any(|r| {
                r["enabled"] != "D"
                    && r["internal"] != true
                    && matches!(
                        options
                            .trigger_effects
                            .get(r["qualified_name"].as_str().unwrap_or("")),
                        Some(TriggerEffects::ExternalSideEffects)
                    )
            }) {
                RequiresExternalChange
            } else if rows.iter().any(|r| {
                r["enabled"] != "D"
                    && r["internal"] != true
                    && (r["enabled"] != "O"
                        || !matches!(
                            options
                                .trigger_effects
                                .get(r["qualified_name"].as_str().unwrap_or("")),
                            Some(TriggerEffects::Replicated)
                        ))
            }) {
                Unverified
            } else if rows.is_empty() {
                Supported
            } else {
                SupportedWithTransformation
            }
        }
        Check::Extensions if !rows.is_empty() && rows.iter().all(|r| r["name"] == "plpgsql") => {
            SupportedWithTransformation
        }
        Check::Views
        | Check::Routines
        | Check::Partitions
        | Check::Extensions
        | Check::SelectedObject => empty_or_review(),
        Check::Grants => verdict(
            all("select")
                && all("usage")
                && all("owner")
                && rows.iter().all(|r| r["rls_active"] == false),
        ),
        // PG catalogs cannot distinguish a plain oid from an application LO
        // reference. Conservatively block whenever large objects are present.
        Check::LargeObjects => match first.and_then(|r| r["count"].as_u64()) {
            Some(0) => Supported,
            Some(_) => Unsupported,
            None => Unverified,
        },
        Check::PreparedTransactions => verdict(rows.is_empty()),
        Check::LongTransactions => {
            if !all("visible") {
                Unverified
            } else {
                verdict(rows.iter().all(|r| {
                    r["oldest_seconds"]
                        .as_f64()
                        .is_some_and(|age| age < options.long_transaction_seconds as f64)
                }))
            }
        }
        Check::RestartLocks => {
            if rows.iter().any(|r| r["pending_restart"] == true) {
                RequiresExternalChange
            } else {
                SupportedWithTransformation
            }
        }
    }
}

fn column_type(row: &Value) -> Option<ColumnType> {
    Some(ColumnType {
        schema: row["schema"].as_str()?.into(),
        name: row["name"].as_str()?.into(),
        oid: row["oid"].as_u64()?.try_into().ok()?,
        typmod: row["typmod"].as_i64()?.try_into().ok()?,
        kind: row["kind"].as_str()?.into(),
        base_oid: row["base_oid"].as_u64()?.try_into().ok()?,
        element_oid: row["element_oid"].as_u64()?.try_into().ok()?,
        formatted: row["formatted"].as_str()?.into(),
    })
}

#[cfg(test)]
mod tests;

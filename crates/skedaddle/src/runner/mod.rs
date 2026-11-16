//! Qualified PostgreSQL runner preparation and joined foreground operation.

mod cutover;
mod gc;
pub use gc::request_gc;
mod activation_baseline;
mod destination_readiness;
mod fence_readiness;
mod finalize;
mod operational;
mod post_activation;
pub(crate) use finalize::allowed as finalize_allowed;
mod reverse_workers;
mod rollback;
pub use post_activation::{request_finalize, request_finalize_objects};
pub use reverse_workers::status as reverse_status;
pub use rollback::{request_rollback, status as rollback_status};
mod readiness;
mod reverse_capabilities;
mod selection;
mod verification;
pub use crate::coordinator::{SourceBinding, SourceBindingError, SourceConnector};
pub use cutover::{
    CutoverError, CutoverErrorKind, CutoverOutcome, CutoverReason, StepTiming, request_cutover,
};
pub use operational::{RunError, open_journal, run};
pub use verification::{VerifyResponse, request_verification, verification_status};

use std::{path::PathBuf, time::Duration};

use serde::Serialize;
use tokio_postgres::Client;

use crate::{
    MigrationDefinition,
    coordinator::chunk::ChunkPlan,
    definition::{Connection, Engine, ObjectKind},
    pg::{catalog, preflight, source, tls},
    security::{self, Redactor},
};

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailureKind {
    InvalidInput,
    Infrastructure,
    Blocked,
    NotWired,
}

#[derive(Debug, Serialize)]
pub struct Failure {
    pub kind: FailureKind,
    pub what: String,
    pub next_step: String,
    pub table: Option<String>,
    pub column: Option<String>,
}

fn failure(kind: FailureKind, what: &str, next_step: &str) -> Failure {
    Failure {
        kind,
        what: what.into(),
        next_step: next_step.into(),
        table: None,
        column: None,
    }
}

/// Counts describe live preparation only, never copy or readiness evidence.
#[derive(Debug, Serialize)]
pub struct Qualification {
    pub qualified_tables: usize,
    pub source_observations: usize,
    pub destination_observations: usize,
    pub utf8_readbacks_verified: bool,
    pub external_effects_performed: bool,
}

struct Session {
    client: Client,
    driver: Driver,
}
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn config(connection: &Connection, redactor: &mut Redactor) -> Result<tls::Config, Failure> {
    if connection.engine != Engine::Postgresql {
        return Err(failure(
            FailureKind::Blocked,
            "Runner preparation requires PostgreSQL endpoints",
            "Select qualified PostgreSQL source and destination connectors.",
        ));
    }
    let reference = connection.connection_ref.as_ref().ok_or_else(|| {
        failure(
            FailureKind::InvalidInput,
            "PostgreSQL connection reference missing",
            "Set connection_ref to an environment variable or UTF-8 secret file reference and retry.",
        )
    })?;
    let secret = security::resolve(reference).map_err(|_| {
        failure(
            FailureKind::Infrastructure,
            "Connection secret unavailable",
            "Restore the referenced environment variable or UTF-8 secret file and retry.",
        )
    })?;
    redactor.register(&secret);
    let mut postgres: tokio_postgres::Config = secret.expose_secret().parse().map_err(|_| {
        failure(
            FailureKind::InvalidInput,
            "Connection secret is not a PostgreSQL configuration",
            "Correct the referenced connection secret and retry.",
        )
    })?;
    postgres.connect_timeout(Duration::from_secs(3));
    let ca = match &connection.tls_ca_ref {
        Some(reference) => {
            let secret = security::resolve(reference).map_err(|_| {
                failure(
                    FailureKind::Infrastructure,
                    "TLS CA reference unavailable",
                    "Restore the referenced UTF-8 CA path secret and retry.",
                )
            })?;
            redactor.register(&secret);
            PathBuf::from(secret.expose_secret())
        }
        None => PathBuf::new(),
    };
    Ok(tls::Config::new(postgres, ca))
}

async fn session(config: &tls::Config, connection: &Connection) -> Result<Session, Failure> {
    let (client, driver, _) = tokio::time::timeout(
        Duration::from_secs(5),
        Box::pin(tls::connect(config, &connection.tls)),
    )
    .await
    .map_err(|_| infrastructure())?
    .map_err(|error| match error {
        tls::Error::Trust => failure(
            FailureKind::InvalidInput,
            "TLS CA bundle could not be loaded",
            "Restore a readable valid PEM CA bundle at the explicitly referenced path and retry.",
        ),
        _ => infrastructure(),
    })?;
    Ok(Session {
        client,
        driver: Driver(tokio::spawn(async move {
            // Driver errors may contain endpoint details. The query boundary
            // reports a fixed diagnostic instead of printing driver errors.
            let _ = driver.await;
        })),
    })
}

fn infrastructure() -> Failure {
    failure(
        FailureKind::Infrastructure,
        "PostgreSQL preparation connection or query failed",
        "Check endpoint reachability, TLS hostname/trust, credentials and catalog permissions; retry preparation.",
    )
}

async fn settings(client: &Client) -> Result<(), Failure> {
    client
        .batch_execute(source::REQUIRED_SETTINGS_SQL)
        .await
        .map_err(|_| infrastructure())?;
    for (name, expected) in [
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("extra_float_digits", "3"),
        ("standard_conforming_strings", "on"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
    ] {
        let actual: String = client
            .query_one(&format!("SHOW {name}"), &[])
            .await
            .map_err(|_| infrastructure())?
            .get(0);
        if actual != expected {
            return Err(failure(
                FailureKind::Blocked,
                "PostgreSQL mapping session settings or UTF8 encoding readback failed",
                "Use UTF8 databases at both endpoints and permit the required mapping session settings.",
            ));
        }
    }
    Ok(())
}

/// Resolve secrets, connect both endpoints, then qualify K1 adapters with live
/// catalog observations. No slot, publication, DDL, buffer or journal is written.
pub async fn qualify(definition: &MigrationDefinition) -> Result<Qualification, Failure> {
    Ok(Box::pin(prepare(definition)).await?.qualification)
}

struct Prepared {
    qualification: Qualification,
    source_config: tls::Config,
    destination_config: tls::Config,
    source: Session,
    destination: Session,
    tables: Vec<catalog::TableCatalog>,
}

async fn prepare(definition: &MigrationDefinition) -> Result<Prepared, Failure> {
    let mut redactor = Redactor::default();
    let source_config = config(&definition.source, &mut redactor)?;
    let destination_config = config(&definition.destination, &mut redactor)?;
    let operation = async {
        let mut source = Box::pin(session(&source_config, &definition.source)).await?;
        let mut destination =
            Box::pin(session(&destination_config, &definition.destination)).await?;
        settings(&source.client).await?;
        settings(&destination.client).await?;
        let catalog_objects = selection::catalog_objects(&definition.selected_objects);
        let source_snapshot = Box::pin(preflight::collect(&mut source.client, &catalog_objects))
            .await
            .map_err(|_| infrastructure())?;
        let destination_snapshot = Box::pin(preflight::collect(
            &mut destination.client,
            &catalog_objects,
        ))
        .await
        .map_err(|_| infrastructure())?;
        // Optional early review when the operator supplied destination IDs.
        // Empty/legacy definitions remain valid; rollback always checks again.
        if !definition
            .fence_plan
            .acknowledged_destination_residual_risks
            .is_empty()
        {
            let scope = definition
                .selected_objects
                .iter()
                .map(|o| format!("{}.{}", cutover::ident(&o.schema), cutover::ident(&o.name)))
                .collect::<Vec<_>>();
            let risks = crate::pg::fence::inventory::collect_destination(
                &destination.client,
                &scope,
                &definition.fence_plan.application_roles,
            )
            .await
            .map_err(|_| infrastructure())?;
            crate::pg::fence::inventory::destination_acknowledgments(
                &risks, &definition.fence_plan.acknowledged_destination_residual_risks,
            ).map_err(|error| failure(FailureKind::Blocked, &redactor.redact(&error),
                "Refresh the destination inventory and review exact destination IDs before rollback."))?;
        }
        let mut tables = Vec::new();
        for (object, catalog_object) in definition.selected_objects.iter().zip(&catalog_objects) {
            if object.kind != ObjectKind::Table {
                return Err(failure(
                    FailureKind::Blocked,
                    "Selected non-table object requires an operational schema adapter",
                    "Qualify schema migration for selected non-table objects before starting.",
                ));
            }
            let columns = source_snapshot
                .observations
                .iter()
                .find(|o| {
                    o.check == preflight::Check::ColumnTypes
                        && o.object.as_ref() == Some(catalog_object)
                })
                .map(|o| {
                    o.rows
                        .iter()
                        .filter_map(|row| row["column"].as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let selection = selection::from_observed(object, columns, &redactor)?;
            let table_name = format!("{}.{}", object.schema, object.name);
            let blocker = |error: catalog::Blocker| Failure {
                kind: FailureKind::Blocked,
                what: redactor.redact(&format!("{table_name}: {error}")),
                next_step: error.next_step.into(),
                table: Some(redactor.redact(&table_name)),
                column: Some(redactor.redact(&error.column)),
            };
            let qualified = catalog::build(
                &source_snapshot,
                &destination_snapshot,
                &[selection],
                0,
                true,
            )
            .map_err(blocker)?;
            for table in qualified {
                // This temporary unbounded plan qualifies the key's typed order
                // support only. It is never saved or treated as a copy plan or
                // source-lineage observation.
                let plan = ChunkPlan::new(table.key.clone(), 1, vec![]).map_err(|_| {
                    failure(
                        FailureKind::Blocked,
                        "Qualified table cannot form a chunk plan",
                        "Recollect the selected table key and qualify chunk planning.",
                    )
                })?;
                table
                    .range_classifier(&plan, "preparation-only-unsaved")
                    .map_err(blocker)?;
                tables.push(table);
            }
        }
        let qualification = Qualification {
            qualified_tables: tables.len(),
            source_observations: source_snapshot.observations.len(),
            destination_observations: destination_snapshot.observations.len(),
            utf8_readbacks_verified: true,
            external_effects_performed: false,
        };
        Ok(Prepared {
            qualification,
            source_config,
            destination_config,
            source,
            destination,
            tables,
        })
    };
    tokio::time::timeout(Duration::from_secs(30), operation)
        .await
        .map_err(|_| infrastructure())?
}

/// Live source preflight only; this does not install or start a migration.
/// Caller supplies real retention estimates and an explicit decoder bound.
pub async fn assess_mysql(
    definition: &MigrationDefinition,
    options: crate::mysql::preflight::Options,
) -> Result<crate::CapabilityReport, Failure> {
    assess_mysql_endpoints(definition, options, true).await
}
/// Inspect only the source when destination configuration is unavailable.
/// Destination evidence remains an explicit critical blocker.
pub async fn assess_mysql_source(
    definition: &MigrationDefinition,
    options: crate::mysql::preflight::Options,
) -> Result<crate::CapabilityReport, Failure> {
    assess_mysql_endpoints(definition, options, false).await
}
async fn assess_mysql_endpoints(
    definition: &MigrationDefinition,
    mut options: crate::mysql::preflight::Options,
    inspect_destination: bool,
) -> Result<crate::CapabilityReport, Failure> {
    use crate::{
        capability::{CapabilityItem, Criticality, Evidence, ItemStatus},
        definition::MysqlGtidMode,
        mysql::{client::MysqlConfig, preflight as mysql_preflight, schema::load_descriptors},
    };
    let mut report = crate::CapabilityReport::from_definition(definition).map_err(|_| {
        failure(
            FailureKind::InvalidInput,
            "Invalid MySQL migration definition",
            "Correct the definition and retry check.",
        )
    })?;
    if definition.source.engine != Engine::Mysql
        || definition.destination.engine != Engine::Postgresql
    {
        return Err(failure(
            FailureKind::Blocked,
            "Live MySQL preflight requires a MySQL source and PostgreSQL destination",
            "Select the qualified connector pair.",
        ));
    }
    if options.estimated_copy_seconds == 0
        || options.planned_catchup_seconds == 0
        || options
            .estimated_copy_seconds
            .checked_add(options.planned_catchup_seconds)
            .is_none()
        || options.max_decompressed_bytes == 0
    {
        return Err(failure(
            FailureKind::InvalidInput,
            "Explicit nonzero copy/catch-up estimates and decoder bound required",
            "Supply measured copy and catch-up planning estimates and a bounded decoder size.",
        ));
    }
    // Scope always comes from the authenticated definition, never caller options.
    options.selected_tables = definition
        .selected_objects
        .iter()
        .filter(|o| o.kind == ObjectKind::Table)
        .cloned()
        .collect();
    let mut redactor = Redactor::default();
    let endpoint = definition.source.mysql.as_ref().ok_or_else(|| {
        failure(
            FailureKind::InvalidInput,
            "MySQL endpoint missing",
            "Set the mysql endpoint block.",
        )
    })?;
    let mut resolve = |reference: &crate::definition::SecretRef| -> Result<String, Failure> {
        let secret = security::resolve(reference).map_err(|_| {
            failure(
                FailureKind::Infrastructure,
                "MySQL endpoint secret unavailable",
                "Restore the referenced UTF-8 secret and retry.",
            )
        })?;
        redactor.register(&secret);
        Ok(secret.expose_secret().to_owned())
    };
    let user = resolve(&endpoint.user)?;
    let password = resolve(&endpoint.password)?;
    let ca = match &definition.source.tls_ca_ref {
        Some(reference) => PathBuf::from(resolve(reference)?),
        None => {
            return Err(failure(
                FailureKind::Blocked,
                "MySQL live connector requires TLS CA trust",
                "Use TLS require with an explicit CA path reference; plaintext transport is not qualified.",
            ));
        }
    };
    let mysql_config = MysqlConfig {
        host: endpoint.host.clone(),
        port: endpoint.port,
        ca,
        user,
        password,
        profile: String::new(),
        gtid: String::new(),
        container_id: String::new(),
        max_decompressed_bytes: options.max_decompressed_bytes,
    };
    let make_item =
        |id: &str, object, status, description: String, remediation: &str| CapabilityItem {
            id: id.into(),
            object,
            status,
            sampled: false,
            criticality: Criticality::Critical,
            remediation: remediation.into(),
            evidence: vec![Evidence {
                source: "live-preflight".into(),
                description,
            }],
        };
    let operation = async {
        let mut client = mysql_config.connect().await.map_err(|_| failure(FailureKind::Infrastructure, "MySQL preflight connection failed (details redacted)", "Check endpoint reachability, TLS trust/hostname, credentials and supported server version."))?;
        let snapshot = mysql_preflight::collect(&mut client, &options).await;
        report.items.retain(|i| {
            !matches!(
                i.id.as_str(),
                "source.mysql.preflight" | "source.mysql.mapping"
            )
        });
        report.items.extend(
            mysql_preflight::classify(&snapshot, &options)
                .into_iter()
                .map(|mut i| {
                    i.id = format!("source.{}", i.id);
                    i
                }),
        );
        let actual_family = match client.version {
            crate::mysql::client::MysqlVersion::V80 => "8.0",
            crate::mysql::client::MysqlVersion::V84 => "8.4",
        };
        report.items.push(make_item(
            "source.mysql.declared_version",
            None,
            if actual_family == definition.source.version {
                ItemStatus::Supported
            } else {
                ItemStatus::RequiresExternalChange
            },
            format!("Observed version family: {actual_family}"),
            "Review the observed server and correct the declared version family.",
        ));
        if let Some(declared) = endpoint.gtid_mode {
            let observations: Vec<_> = snapshot
                .observations
                .iter()
                .filter(|o| o.check == mysql_preflight::MysqlCheck::GtidMode)
                .collect();
            let observed = match observations.as_slice() {
                [o] if o.error.is_none() && o.rows.len() == 1 => o.rows[0]["value"].as_str(),
                _ => None,
            };
            let expected = match declared {
                MysqlGtidMode::On => "ON",
                MysqlGtidMode::Off => "OFF",
            };
            let status = match observed {
                Some("ON" | "OFF") if observed == Some(expected) => ItemStatus::Supported,
                Some("ON" | "OFF") => ItemStatus::RequiresExternalChange,
                _ => ItemStatus::Unverified,
            };
            report.items.push(make_item("source.mysql.declared_gtid_mode", None, status,
                format!("Declared mode: {expected}; observed mode: {}", observed.unwrap_or("unverified")),
                "Review the observed GTID mode; correct the declaration or finish the documented server transition, then recollect."));
        }
        for object in &definition.selected_objects {
            if object.kind != ObjectKind::Table {
                report.items.push(make_item(
                    "source.mysql.object_kind",
                    Some(object.clone()),
                    ItemStatus::Unsupported,
                    "Selected non-table object has no MySQL mapping adapter.".into(),
                    "Select tables or qualify an adapter for this object kind.",
                ));
                continue;
            }
            let descriptors = load_descriptors(
                &mut client.conn,
                &[(object.schema.clone(), object.name.clone())],
            )
            .await;
            match descriptors {
                Ok(descriptors) if descriptors.len() == 1 => {
                    let descriptor = &descriptors[0];
                    if object.exclude_columns.iter().any(|name| descriptor.primary_key.contains(name) || !descriptor.columns.iter().any(|c| &c.name == name)) {
                        report.items.push(make_item("source.mysql.projection", Some(object.clone()), ItemStatus::Unsupported,
                            "Excluded column is missing or is an operational primary key.".into(), "Retain every primary-key column and correct excluded column names."));
                    }
                    for column in descriptor.columns.iter().filter(|c| !object.exclude_columns.contains(&c.name)) {
                        if let Err(error) = crate::mapping::mysql::plan_column(column) {
                            report.items.push(make_item(&format!("source.mysql.mapping.{}", column.name), Some(object.clone()), ItemStatus::Unsupported,
                                format!("Column {}: {error}", column.name), "Convert the column to a qualified MySQL/PG mapping or explicitly exclude this non-key column; preserve its data separately."));
                        }
                    }
                }
                _ => report.items.push(make_item("source.mysql.mapping", Some(object.clone()), ItemStatus::Unverified,
                    "Selected-table descriptor collection failed (details redacted).".into(), "Restore catalog visibility and an existing table with a primary key, then recollect every column.")),
            }
        }
        let _ = client.conn.disconnect().await;
        // Receiver checks deliberately exclude source WAL, replication, slot and
        // replica-identity requirements. Mapped destination DDL is a later item.
        if inspect_destination {
            let destination = config(&definition.destination, &mut redactor)?;
            let mut destination = Box::pin(session(&destination, &definition.destination)).await?;
            settings(&destination.client).await?;
            let mut snapshot = Box::pin(preflight::collect(&mut destination.client, &[]))
                .await
                .map_err(|_| infrastructure())?;
            snapshot.observations.retain(|o| {
                matches!(
                    o.check,
                    preflight::Check::Version
                        | preflight::Check::DatabaseCreate
                        | preflight::Check::OriginGrants
                )
            });
            let destination_report = preflight::classify(
                &report.definition_digest,
                &snapshot,
                &preflight::Options::default(),
                |_| false,
            );
            report
                .items
                .extend(destination_report.items.into_iter().map(|mut i| {
                    i.id = format!("destination.{}", i.id);
                    i
                }));
        } else {
            report.items.push(make_item(
                "destination.pg.preflight",
                None,
                ItemStatus::Unverified,
                "Source-only inspection; PostgreSQL destination was not connected.".into(),
                "Supply destination secrets and run combined live preflight.",
            ));
        }
        report.items.push(make_item(
            "destination.pg.mapped_schema",
            None,
            ItemStatus::Unverified,
            "Mapped destination schema and apply compatibility have not been qualified.".into(),
            "Qualify mapped destination DDL and apply adapters before starting the migration.",
        ));
        Ok(report)
    };
    let mut report: crate::CapabilityReport =
        tokio::time::timeout(Duration::from_secs(30), operation)
            .await
            .map_err(|_| {
                failure(
                    FailureKind::Infrastructure,
                    "Live MySQL preflight timed out",
                    "Restore endpoint access and retry preflight.",
                )
            })??;
    // Server evidence (including grants) may echo resolved credential values.
    for item in &mut report.items {
        item.id = redactor.redact(&item.id);
        item.remediation = redactor.redact(&item.remediation);
        if let Some(object) = &mut item.object {
            object.schema = redactor.redact(&object.schema);
            object.name = redactor.redact(&object.name);
            for name in &mut object.exclude_columns {
                *name = redactor.redact(name);
            }
        }
        for evidence in &mut item.evidence {
            evidence.source = redactor.redact(&evidence.source);
            evidence.description = redactor.redact(&evidence.description);
        }
    }
    Ok(report)
}

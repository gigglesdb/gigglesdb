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
pub use rollback::request_rollback;
mod readiness;
mod reverse_capabilities;
mod selection;
mod verification;
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
    let secret = security::resolve(&connection.connection_ref).map_err(|_| {
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

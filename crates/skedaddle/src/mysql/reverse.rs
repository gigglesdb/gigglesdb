//! PG-to-MySQL reverse data path, without CLI activation or access restoration.
//! The caller holds migration authority and both application fences throughout
//! preparation. Every callback must freshly observe that authority and durably
//! journal the supplied intent before returning. No callback may release a fence.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use super::{
    apply::{self, MysqlApplier},
    client::MysqlConfig,
    dest_schema,
    reverse_resolver::Resolver,
    rollback_sink::MysqlRollbackSink,
    schema::{self, TableDescriptor},
};
use crate::{
    buffer::writer::BufferWriter,
    event::{PositionOrder, SourceIdentity},
    mapping::mysql::{MysqlPgColumn, plan_column},
    pg::{ddl, fence, reverse as pg, source},
    rollback::apply as rollback,
};

type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    Pg(pg::Error),
    Apply(apply::ApplyError),
    Mysql(super::client::MysqlError),
    Catalog(dest_schema::Error),
    Schema(dest_schema::Blocker),
    Capture(source::Error),
    Invalid(&'static str),
    Timeout,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MySQL reverse preparation: {self:?}")
    }
}
impl std::error::Error for Error {}

/// Saved forward descriptors and M7 plans. The initial profile uses matching
/// PG/MySQL schema, table and column names; renaming is not inferred here.
#[derive(Clone, Debug)]
pub struct Table {
    pub descriptor: TableDescriptor,
    pub plans: Vec<MysqlPgColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Readiness {
    format: u32,
    capture: pg::CapturePreparation,
    #[serde(with = "SourceWire")]
    source: SourceIdentity,
    schema_digest: [u8; 32],
    mysql_server_uuid: String,
}
impl Readiness {
    pub fn slot(&self) -> &pg::SlotEvidence {
        self.capture.slot()
    }
    pub fn source(&self) -> &SourceIdentity {
        &self.source
    }
    /// This is combined readiness, not pg::Readiness. Durable storage under the
    /// caller's authority is still required before application access changes.
    pub fn check_activation(&self, config: &Config, activation_lsn: u64) -> Result<()> {
        self.capture
            .check_activation(&config.pg, activation_lsn)
            .map_err(Error::Pg)?;
        if self.format != 1
            || self.schema_digest != config.schema.digest()
            || self.source.engine != "postgres"
            || self.source.stream != config.pg.slot()
            || self.source.system_id.is_empty()
            || self.source.lineage.is_empty()
            || self.source.db_name.is_empty()
            || self.mysql_server_uuid.is_empty()
        {
            return Err(Error::Invalid("combined readiness identity differs"));
        }
        Ok(())
    }
}

// Local wire adapter leaves the engine event format and PG readiness unchanged.
#[derive(Serialize, Deserialize)]
#[serde(remote = "SourceIdentity", deny_unknown_fields)]
struct SourceWire {
    engine: String,
    system_id: String,
    lineage: String,
    db_name: String,
    stream: String,
}

/// Connector-specific intents deliberately never publish PG Intent::Ready.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Intent {
    Capture(pg::Endpoint, pg::Intent),
    QualifyPostgres,
    InstallMysql,
    QualifyMysql,
    Ready(Readiness),
}

pub struct Config {
    pg: pg::Config,
    tables: Vec<Table>,
    schema: dest_schema::MappedSchema,
}
impl Config {
    pub fn new(installation: &str, server_version_num: u32, tables: Vec<Table>) -> Result<Self> {
        let descriptors: Vec<_> = tables.iter().map(|t| t.descriptor.clone()).collect();
        // build also rejects duplicate tables/columns, missing/nullable/generated
        // keys, unsupported mappings and unqualified generated expressions.
        let schema = dest_schema::build(&descriptors).map_err(Error::Schema)?;
        for table in &tables {
            qualify_plans(table)?;
        }
        let pg = pg::Config::new(
            installation,
            server_version_num,
            descriptors
                .into_iter()
                .map(|t| (t.schema, t.table))
                .collect(),
        )
        .map_err(Error::Pg)?;
        Ok(Self { pg, tables, schema })
    }
    /// The rollback loop reads this retained namespace. Writers must use the
    /// same prefix, migration and stream, with this PG order domain.
    pub fn writer_config(&self, epoch: u64) -> crate::buffer::writer::WriterConfig {
        crate::buffer::writer::WriterConfig {
            prefix: "reverse-runner-v1".into(),
            migration_id: self
                .pg
                .stream()
                .strip_suffix("_rev")
                .expect("PG reverse stream suffix")
                .into(),
            stream: self.pg.stream(),
            epoch,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 64 * 1024 * 1024,
        }
    }
    pub fn order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
        Arc::new(Order(self.pg.stream()))
    }
    pub fn postgres(&self) -> &pg::Config {
        &self.pg
    }
    fn plans(&self) -> BTreeMap<String, Vec<MysqlPgColumn>> {
        self.tables
            .iter()
            .map(|t| (alias(&t.descriptor), t.plans.clone()))
            .collect()
    }
    fn descriptors(&self) -> BTreeMap<String, TableDescriptor> {
        self.tables
            .iter()
            .map(|t| (alias(&t.descriptor), t.descriptor.clone()))
            .collect()
    }

    /// Bound destination transport is available to every callback so the caller
    /// can re-observe ownership/fencing, including after each async MySQL phase.
    /// `catalog` must be the same PG endpoint: its identity is checked below.
    /// Run on a multithread Tokio runtime: synchronous PG fence transports and
    /// journal callbacks run inside block_in_place, so their existing block_on
    /// calls remain valid. Current-thread runtimes are rejected before effects.
    /// Catalog qualification uses the existing M7 verifier, including generated
    /// expressions, constraints, nullability, collations and exact type/typmods.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare<C: fence::Connection>(
        &self,
        destination: &mut C,
        catalog: &mut Client,
        mysql: &MysqlConfig,
        previous: Option<&pg::SlotEvidence>,
        timeout: Duration,
        mut before: impl FnMut(&mut C, &Intent) -> pg::Result<()>,
    ) -> Result<Prepared> {
        if timeout.is_zero() {
            return Err(Error::Invalid("setup timeout"));
        }
        blocking(|| Ok(()))?;
        tokio::time::timeout(timeout, async {
            let capture = blocking(|| self.pg.prepare_capture_bound(
                destination,
                previous,
                timeout,
                |connection, endpoint, intent| {
                    before(connection, &Intent::Capture(endpoint, intent.clone()))
                },
            ).map_err(Error::Pg))?;
            let identity = blocking(|| {
                before(destination, &Intent::QualifyPostgres).map_err(Error::Pg)?;
                destination.query(IDENTITY_SQL, timeout)
                    .map_err(|_| Error::Invalid("PG destination identity unavailable"))
            })?;
            let source = source_identity(&identity, &self.pg)?;
            let actual = catalog.query_one(IDENTITY_SQL, &[]).await
                .map_err(|_| Error::Invalid("PG catalog identity unavailable"))?;
            if identity[0] != [actual.get::<_, String>(0), actual.get(1), actual.get(2)] {
                return Err(Error::Invalid("PG catalog endpoint differs"));
            }
            let mismatches = dest_schema::verify_destination(catalog, &self.schema)
                .await.map_err(Error::Catalog)?;
            if !mismatches.is_empty() {
                return Err(Error::Catalog(dest_schema::Error::Mismatches(mismatches)));
            }
            blocking(|| before(destination, &Intent::InstallMysql).map_err(Error::Pg))?;
            apply::install_apply_tables(mysql).await.map_err(Error::Apply)?;
            blocking(|| before(destination, &Intent::QualifyMysql).map_err(Error::Pg))?;
            let mut client = mysql.connect().await.map_err(Error::Mysql)?;
            apply::require_binlogging(&mut client.conn).await.map_err(Error::Apply)?;
            let uuid: Option<String> = client.conn.query_first("SELECT @@server_uuid")
                .await.map_err(|e| Error::Mysql(e.into()))?;
            let mysql_server_uuid = uuid.filter(|s| !s.is_empty())
                .ok_or(Error::Invalid("MySQL server identity unavailable"))?;
            let names = self.tables.iter().map(|t| {
                (t.descriptor.schema.clone(), t.descriptor.table.clone())
            }).collect::<Vec<_>>();
            let actual = schema::load_descriptors(&mut client.conn, &names)
                .await.map_err(Error::Mysql)?;
            qualify_descriptors(&self.tables, &actual)?;
            for (schema, table) in &names {
                let engine: Option<String> = client.conn.exec_first(
                    "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
                    (schema, table),
                ).await.map_err(|e| Error::Mysql(e.into()))?;
                if engine.as_deref() != Some("InnoDB") {
                    return Err(Error::Invalid("MySQL reverse table must be InnoDB"));
                }
            }
            client.conn.disconnect().await.map_err(|e| Error::Mysql(e.into()))?;
            let applier = MysqlApplier::new(mysql.clone(), source.clone(), self.descriptors())
                .map_err(Error::Apply)?;
            let ready = Readiness {
                format: 1,
                capture, source, schema_digest: self.schema.digest(), mysql_server_uuid,
            };
            // A fresh observation after all MySQL awaits and a durable combined
            // journal entry are mandatory. Failure returns no activation proof.
            blocking(|| before(destination, &Intent::Ready(ready.clone())).map_err(Error::Pg))?;
            Ok(Prepared { readiness: ready, applier })
        }).await.map_err(|_| Error::Timeout)?
    }

    /// Reopen capture from the authenticated durable PG cursor. The capture
    /// keeps PG source identity and eight-byte LSN positions; no MySQL cursor
    /// translation and no second inverse conversion enter the reverse buffer.
    #[allow(clippy::too_many_arguments)]
    pub fn capture(
        &self,
        readiness: &Readiness,
        activation_lsn: u64,
        durable_lsn: u64,
        writer: BufferWriter,
        schemas: ddl::SchemaVersions,
        eligibility: pg::Eligibility,
    ) -> Result<pg::Capture<Resolver>> {
        readiness.check_activation(self, activation_lsn)?;
        if durable_lsn < readiness.slot().consistent_point {
            return Err(Error::Invalid("reverse capture cursor precedes slot"));
        }
        pg::Capture::with_mapping(
            &self.pg,
            readiness.source.clone(),
            durable_lsn,
            writer,
            Resolver::new(self.plans()).map_err(Error::Capture)?,
            schemas,
            eligibility,
            pg::ValidatedMapping::boxed,
        )
        .map_err(Error::Capture)
    }
}

/// Retains the exact PG stop-boundary API and the request-scoped rollback loop.
/// No forward Runtime checkpoint or receipt GC is performed by this path.
pub struct Prepared {
    readiness: Readiness,
    applier: MysqlApplier,
}
impl Prepared {
    pub fn readiness(&self) -> &Readiness {
        &self.readiness
    }
    pub fn sink(&mut self) -> MysqlRollbackSink<'_> {
        MysqlRollbackSink {
            applier: &mut self.applier,
        }
    }
    /// Use only after capture has stopped and its exact StopBoundary and writer
    /// checkpoint have been authenticated from the coordinator journal.
    #[allow(clippy::too_many_arguments)]
    pub async fn replay(
        &mut self,
        key: &crate::cutover::Key,
        store: Arc<dyn crate::buffer::backends::BufferStore>,
        stop: &pg::StopBoundary,
        checkpoint: &crate::buffer::writer::WriterCheckpoint,
        verification: &mut impl rollback::Verification,
        journal: &mut impl rollback::Journal,
    ) -> rollback::Result<rollback::Outcome> {
        stop.validate(checkpoint).map_err(|_| {
            rollback::Error::OperatorAction("reverse durable stop boundary differs")
        })?;
        if stop.marker.cutover != key.cutover
            || stop.marker.epoch > key.epoch
            || self.readiness.source.stream != format!("skedaddle_{}_rev", key.migration)
        {
            return Err(rollback::Error::OperatorAction(
                "reverse marker request differs",
            ));
        }
        let order: Arc<dyn PositionOrder + Send + Sync> =
            Arc::new(Order(format!("{}_rev", key.migration)));
        let replay = rollback::Replay {
            store,
            order,
            required_next_mseq: stop.next_mseq,
            tables: vec![],
        };
        rollback::execute_next_with_sink(key, &replay, &mut self.sink(), verification, journal)
            .await
    }
}

// Existing fence::Connection implementations and ownership callbacks may drive
// their own Tokio runtime synchronously. Admit that explicitly, never panic on
// nested block_on or silently run them on an async executor worker.
fn blocking<T>(operation: impl FnOnce() -> Result<T>) -> Result<T> {
    if tokio::runtime::Handle::current().runtime_flavor()
        != tokio::runtime::RuntimeFlavor::MultiThread
    {
        return Err(Error::Invalid(
            "reverse setup requires a multithread Tokio runtime",
        ));
    }
    tokio::task::block_in_place(operation)
}

/// Same strict eight-byte big-endian PG LSN order as the PG reverse runner.
struct Order(String);
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        &self.0
    }
    fn compare(
        &self,
        a: &[u8],
        b: &[u8],
    ) -> std::result::Result<std::cmp::Ordering, crate::event::EventError> {
        if a.len() != 8 || b.len() != 8 {
            return Err(crate::event::EventError::InvalidEncoding(
                "PostgreSQL LSN width",
            ));
        }
        Ok(a.cmp(b))
    }
}

const IDENTITY_SQL: &str = "SELECT system_identifier::text, (pg_control_checkpoint()).timeline_id::text, current_database() FROM pg_control_system()";
fn source_identity(rows: &fence::Rows, config: &pg::Config) -> Result<SourceIdentity> {
    if rows.len() != 1 || rows[0].len() != 3 || rows[0].iter().any(String::is_empty) {
        return Err(Error::Invalid("PG reverse source identity"));
    }
    Ok(SourceIdentity {
        engine: "postgres".into(),
        system_id: rows[0][0].clone(),
        lineage: rows[0][1].clone(),
        db_name: rows[0][2].clone(),
        stream: config.slot(),
    })
}
fn alias(d: &TableDescriptor) -> String {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    format!("{}.{}", quote(&d.schema), quote(&d.table))
}
fn qualify_descriptors(saved: &[Table], actual: &[TableDescriptor]) -> Result<()> {
    if saved.len() != actual.len() || saved.iter().zip(actual).any(|(s, a)| &s.descriptor != a) {
        return Err(Error::Invalid(
            "MySQL descriptor differs from saved M7 plan",
        ));
    }
    Ok(())
}
fn qualify_plans(table: &Table) -> Result<()> {
    if table.plans.len() != table.descriptor.columns.len() {
        return Err(Error::Invalid("saved M7 plan column count"));
    }
    for (column, saved) in table.descriptor.columns.iter().zip(&table.plans) {
        let actual = plan_column(column).map_err(|e| Error::Apply(e.into()))?;
        // Compare the complete derived plan, including private conversion args
        // and labels, rather than just its public PG type/typmod/type identity.
        if format!("{actual:?}") != format!("{saved:?}") {
            return Err(Error::Invalid("saved M7 plan differs from descriptor"));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "reverse_tests.rs"]
mod tests;

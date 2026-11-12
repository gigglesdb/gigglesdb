//! Pre-activation reverse replication. The coordinator retains the destination
//! fence until `setup` returns and durably records its readiness evidence.
//! Capture and apply use the existing engines with the endpoints swapped.

mod eligibility;
pub use eligibility::{Eligibility, InverseMapping, MappingFailure, ReverseResolver, Violation};

use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};

use super::{
    apply, ddl, fence,
    pgoutput::{CopyData, Decoder, Message},
    replication, source,
};
use crate::{
    buffer::writer::BufferWriter,
    event::{AssembledTxn, EventError, Position, PositionOrder, SourceIdentity},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Database(String),
    Journal(String),
    NotFenced,
    SlotChanged,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reverse replication: {self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Endpoint {
    Destination,
    OldSource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    Publication,
    Slot,
    SlotReady(SlotEvidence),
    DdlTrigger,
    ApplyObjects,
    Ready(Readiness),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotEvidence {
    pub slot: String,
    /// Consistent point observed while application writes were fenced. On a
    /// fenced crash retry an unrecorded slot may be adopted at its current
    /// confirmed point; activation is still forbidden until setup completes.
    pub consistent_point: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    installation: String,
    slot: SlotEvidence,
}
impl Readiness {
    pub fn slot(&self) -> &SlotEvidence {
        &self.slot
    }

    /// `activation_lsn` is a destination WAL barrier taken before releasing the
    /// fence, never a timestamp or an LSN from the old source. Fence ownership
    /// and durable storage of this proof remain coordinator responsibilities.
    pub fn check_activation(&self, config: &Config, activation_lsn: u64) -> Result<()> {
        if self.installation != config.installation
            || self.slot.slot != config.slot()
            || self.slot.consistent_point == 0
            || self.slot.consistent_point >= activation_lsn
        {
            return Err(Error::Invalid("reverse slot must precede activation"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    installation: String,
    tables: Vec<(String, String)>,
    trigger: ddl::TriggerSql,
}
impl Config {
    /// Tables are destination publication tables (schema, name), qualified by
    /// preflight. Explicit tables exclude Skedaddle's progress/receipt objects.
    pub fn new(id: &str, server_version_num: u32, tables: Vec<(String, String)>) -> Result<Self> {
        let trigger = ddl::TriggerSql::new(id, server_version_num)
            .map_err(|_| Error::Invalid("installation or PostgreSQL version"))?;
        let mut unique = std::collections::BTreeSet::new();
        if tables.is_empty()
            || tables.iter().any(|(schema, table)| {
                schema == "_skedaddle"
                    || ident(schema).is_err()
                    || ident(table).is_err()
                    || !unique.insert((schema, table))
            })
        {
            return Err(Error::Invalid("reverse publication tables"));
        }
        Ok(Self {
            installation: id.into(),
            tables,
            trigger,
        })
    }
    pub fn slot(&self) -> String {
        format!("skedaddle_{}_rev", self.installation)
    }
    pub fn publication(&self) -> String {
        format!("skedaddle_{}_rev", self.installation)
    }
    pub fn origin(&self) -> String {
        format!("skedaddle_{}_rev", self.installation)
    }
    pub fn stream(&self) -> String {
        format!("{}_rev", self.installation)
    }
    pub fn apply_context(&self, mseq: u64) -> apply::PlanContext {
        apply::PlanContext {
            stream: self.stream(),
            mseq,
            origin: self.origin(),
            synthetic_commit_time: None,
        }
    }
    /// Bind reverse planning to this configuration's installation, independently
    /// of any installation declared by captured DDL payloads.
    pub fn plan_transaction(
        &self,
        txn: &AssembledTxn,
        tables: &[apply::Table],
        mseq: u64,
        range: impl FnMut(
            &apply::Table,
            &crate::event::Row,
        ) -> std::result::Result<apply::RangeState, apply::ApplyError>,
    ) -> std::result::Result<apply::TransactionPlan, apply::ApplyError> {
        apply::plan_transaction_with_ddl(
            txn,
            tables,
            &self.apply_context(mseq),
            &self.installation,
            range,
        )
    }
    pub fn executor<C: apply::executor::Connector>(
        &self,
        connector: C,
        max_receipts: u32,
    ) -> apply::executor::Result<apply::executor::Executor<C>> {
        apply::executor::Executor::new(connector, self.stream(), self.origin(), max_receipts)
    }
    /// Connects to the destination, with source's session/lineage verification
    /// and the existing START_REPLICATION origin 'none' option.
    pub async fn reconnect(
        &self,
        destination: &replication::ConnectionConfig,
        checkpoint: &replication::Checkpoint,
    ) -> source::Result<replication::ReplicationConnection> {
        if checkpoint.slot != self.slot() {
            return Err(source::Error::RecoveryRequired("reverse slot mismatch"));
        }
        source::reconnect(destination, checkpoint, &[self.publication()]).await
    }
    pub fn start_command(&self, lsn: u64) -> replication::Result<String> {
        replication::start_command(&self.slot(), lsn, &[self.publication()])
    }
    /// Forward capture's fence detector trusts this exact reverse origin on
    /// the old source. Other origins (including lookalike suffixes) still breach.
    pub fn detect_forward_breach(
        &self,
        txn: &AssembledTxn,
        drain: &Position,
        order: &(impl PositionOrder + ?Sized),
        window: fence::FenceWindow,
    ) -> std::result::Result<fence::Breach, EventError> {
        fence::detect_breach(txn, drain, order, &self.origin(), window)
    }

    fn publication_sql(&self) -> Result<String> {
        let names = self
            .tables
            .iter()
            .map(|(s, t)| Ok(format!("{}.{}", ident(s)?, ident(t)?)))
            .collect::<Result<Vec<_>>>()?;
        let expected = names
            .iter()
            .map(|n| format!("{}::regclass::oid", literal(n)))
            .collect::<Vec<_>>()
            .join(",");
        let name = self.publication();
        let body = format!(
            "BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname='{name}') THEN
 CREATE PUBLICATION {name} FOR TABLE {};
END IF;
IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname='{name}' AND NOT puballtables
 AND pubinsert AND pubupdate AND pubdelete AND pubtruncate AND NOT pubviaroot)
 OR EXISTS (SELECT 1 FROM pg_publication_rel WHERE prpubid=(SELECT oid FROM pg_publication WHERE pubname='{name}') AND (prqual IS NOT NULL OR prattrs IS NOT NULL))
 OR EXISTS (SELECT 1 FROM pg_publication_namespace WHERE pnpubid=(SELECT oid FROM pg_publication WHERE pubname='{name}'))
 OR (SELECT array_agg(prrelid ORDER BY prrelid) FROM pg_publication_rel WHERE prpubid=(SELECT oid FROM pg_publication WHERE pubname='{name}'))
 IS DISTINCT FROM (SELECT array_agg(x ORDER BY x) FROM unnest(ARRAY[{expected}]) x)
THEN RAISE EXCEPTION 'reverse publication identity changed'; END IF;
END;",
            names.join(",")
        );
        Ok(format!("DO {};", literal(&body)))
    }

    /// Each callback must durably journal before returning. `fenced` must
    /// verify retained destination fence ownership on EVERY step; setup must
    /// not run concurrently with activation or another installer. A failed
    /// callback prevents that step. Replay the same plan after any failure.
    #[allow(clippy::too_many_arguments)]
    pub fn setup(
        &self,
        destination: &mut impl fence::Connection,
        old_source: &mut impl fence::Connection,
        previous: Option<&SlotEvidence>,
        timeout: Duration,
        mut fenced: impl FnMut() -> Result<bool>,
        mut journal: impl FnMut(Endpoint, &Intent) -> Result<()>,
    ) -> Result<Readiness> {
        self.setup_bound(
            destination,
            old_source,
            previous,
            timeout,
            |_, endpoint, intent| {
                if !fenced()? {
                    return Err(Error::NotFenced);
                }
                journal(endpoint, intent)
            },
        )
    }

    /// Coordinator entry point: the same destination transport is available for
    /// a fresh ownership-bound fence observation before every journal callback.
    pub(crate) fn setup_bound<C: fence::Connection>(
        &self,
        destination: &mut C,
        old_source: &mut impl fence::Connection,
        previous: Option<&SlotEvidence>,
        timeout: Duration,
        mut before: impl FnMut(&mut C, Endpoint, &Intent) -> Result<()>,
    ) -> Result<Readiness> {
        if timeout.is_zero() {
            return Err(Error::Invalid("setup timeout"));
        }
        before(destination, Endpoint::Destination, &Intent::Publication)?;
        query(destination, &self.publication_sql()?, timeout)?;
        before(destination, Endpoint::Destination, &Intent::Slot)?;
        let slot = self.slot();
        let rows = query(
            destination,
            &format!(
                "SELECT plugin, database, confirmed_flush_lsn::text, restart_lsn::text, wal_status, temporary::text, active::text FROM pg_replication_slots WHERE slot_name='{slot}' AND slot_type='logical' AND database=current_database()"
            ),
            timeout,
        )?;
        let point = if rows.is_empty() {
            if previous.is_some() {
                return Err(Error::SlotChanged);
            }
            let created = query(
                destination,
                &format!(
                    "SELECT lsn::text FROM pg_create_logical_replication_slot('{slot}', 'pgoutput')"
                ),
                timeout,
            )?;
            parse_cell(&created, 0, 0)?
        } else {
            if rows.len() != 1
                || rows[0].len() != 7
                || rows[0][0] != "pgoutput"
                || !matches!(rows[0][4].as_str(), "reserved" | "extended")
                || rows[0][5] != "false"
                || rows[0][6] != "false"
            {
                return Err(Error::SlotChanged);
            }
            let confirmed = parse_cell(&rows, 0, 2)?;
            let restart = parse_cell(&rows, 0, 3)?;
            match previous {
                Some(e)
                    if e.slot == slot
                        && e.consistent_point > 0
                        && restart <= e.consistent_point
                        && e.consistent_point <= confirmed =>
                {
                    e.consistent_point
                }
                Some(_) => return Err(Error::SlotChanged),
                None => confirmed,
            }
        };
        if point == 0 {
            return Err(Error::SlotChanged);
        }
        let evidence = SlotEvidence {
            slot,
            consistent_point: point,
        };
        before(
            destination,
            Endpoint::Destination,
            &Intent::SlotReady(evidence.clone()),
        )?;
        before(destination, Endpoint::Destination, &Intent::DdlTrigger)?;
        query(destination, &self.trigger.install, timeout)?;
        let verified = query(
            destination,
            &format!(
                "SELECT ({})::text",
                self.trigger
                    .verify
                    .trim_end_matches(';')
                    .trim_start_matches("SELECT ")
            ),
            timeout,
        )?;
        if verified.first().and_then(|r| r.first()).map(String::as_str) != Some("true") {
            return Err(Error::Invalid("reverse DDL trigger verification"));
        }
        before(destination, Endpoint::OldSource, &Intent::ApplyObjects)?;
        let origin = self.origin();
        let administrative_origin = ddl::administrative_origin(old_source, timeout)
            .map_err(|_| Error::Invalid("old source DDL capture installation lookup"))?;
        query(
            old_source,
            &apply_objects_sql(&origin, &administrative_origin),
            timeout,
        )?;
        let ready = Readiness {
            installation: self.installation.clone(),
            slot: evidence,
        };
        before(
            destination,
            Endpoint::Destination,
            &Intent::Ready(ready.clone()),
        )?;
        Ok(ready)
    }
}

fn apply_objects_sql(origin: &str, administrative_origin: &str) -> String {
    format!("BEGIN; {administrative_origin}SELECT pg_advisory_xact_lock(1936418148,49); {} REVOKE ALL ON _skedaddle.apply_progress, _skedaddle.apply_receipts FROM PUBLIC;
DO $reverse$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname={}) THEN PERFORM pg_replication_origin_create({}); END IF; END $reverse$; COMMIT;", apply::executor::PROGRESS_DDL, literal(origin), literal(origin))
}

fn ident(s: &str) -> Result<String> {
    if s.is_empty() || s.len() > 63 || s.contains('\0') {
        return Err(Error::Invalid("SQL identifier"));
    }
    Ok(format!("\"{}\"", s.replace('"', "\"\"")))
}
fn literal(s: &str) -> String {
    format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
fn query(c: &mut impl fence::Connection, sql: &str, timeout: Duration) -> Result<fence::Rows> {
    c.query(sql, timeout)
        .map_err(|_| Error::Database("setup operation failed".into()))
}
fn parse_cell(rows: &fence::Rows, row: usize, column: usize) -> Result<u64> {
    let s = rows
        .get(row)
        .and_then(|r| r.get(column))
        .ok_or(Error::SlotChanged)?;
    fence::parse_lsn(s).map_err(|_| Error::SlotChanged)
}

/// Adds eligibility observation to source::Capture; the wire decoder, buffer
/// durability and DDL classification remain the original implementation.
pub struct Capture<R> {
    inner: source::Capture<ReverseResolver<R>>,
    eligibility: Eligibility,
    schemas: ddl::SchemaVersions,
    installation: String,
}
impl<R: source::TypeResolver> Capture<R> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &Config,
        source: SourceIdentity,
        durable_lsn: u64,
        writer: BufferWriter,
        resolver: R,
        schemas: ddl::SchemaVersions,
        eligibility: Eligibility,
    ) -> source::Result<Self> {
        let eligibility_schemas = schemas.clone();
        let inner = source::Capture::new(
            source,
            config.stream(),
            durable_lsn,
            writer,
            ReverseResolver::new(resolver, eligibility.clone()),
            config.installation.clone(),
            schemas,
            format!("skedaddle_{}_rev_watermark", config.installation),
        )?;
        Ok(Self {
            inner,
            eligibility,
            schemas: eligibility_schemas,
            installation: config.installation.clone(),
        })
    }
    pub fn durable_lsn(&self) -> u64 {
        self.inner.durable_lsn()
    }
    pub fn writer(&self) -> &BufferWriter {
        self.inner.writer()
    }
    pub async fn step(
        &mut self,
        transport: &mut impl source::Transport,
        sink: &mut impl source::WatermarkSink,
        paused: &mut tokio::sync::watch::Receiver<bool>,
    ) -> source::Result<source::Step> {
        self.eligibility.ensure().map_err(source::Error::Mapping)?;
        let mut checked = CheckedTransport {
            inner: transport,
            eligibility: &self.eligibility,
            schemas: &mut self.schemas,
            installation: &self.installation,
        };
        let result = self.inner.step(&mut checked, sink, paused).await;
        match &result {
            Ok(source::Step::Durable {
                stop: Some(stop), ..
            }) => {
                self.eligibility
                    .end(Violation::UnsupportedDdl {
                        reason: stop.reason.clone(),
                    })
                    .map_err(source::Error::Mapping)?;
            }
            Err(source::Error::Mapping(e)) => {
                self.eligibility
                    .end(Violation::Value { reason: e.into() })
                    .map_err(source::Error::Mapping)?;
            }
            Err(source::Error::Ddl(_)) => {
                self.eligibility
                    .end(Violation::UnsupportedDdl {
                        reason: "invalid destination DDL".into(),
                    })
                    .map_err(source::Error::Mapping)?;
            }
            _ => {}
        }
        result
    }
}

/// Observe DDL at its wire position, before a later row could fail mapping.
/// This uses the existing decoder and classifier; source::Capture still owns
/// transaction assembly, publication and acknowledgment. No WAL is skipped.
struct CheckedTransport<'a, T> {
    inner: &'a mut T,
    eligibility: &'a Eligibility,
    schemas: &'a mut ddl::SchemaVersions,
    installation: &'a str,
}
impl<T: source::Transport> source::Transport for CheckedTransport<'_, T> {
    async fn next(&mut self, durable_lsn: u64) -> source::Result<Vec<u8>> {
        let payload = self.inner.next(durable_lsn).await?;
        let decoder = Decoder::default();
        if let CopyData::XLogData { data, .. } = decoder.decode_copy_data(&payload)? {
            let message = decoder.decode(data)?;
            if let Message::LogicalMessage {
                prefix: ddl::MESSAGE_PREFIX,
                ..
            } = &message
            {
                let record = ddl::decode(&message, self.installation)?;
                self.eligibility
                    .check_ddl(self.schemas, &[record])
                    .map_err(source::Error::Mapping)?;
            }
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod origin_tests {
    use super::*;

    #[test]
    fn reverse_apply_objects_tag_the_installed_identity_inside_the_transaction() {
        let setting =
            ddl::administrative_origin_sql(&vec![vec!["forward_capture".into()]]).unwrap();
        let sql = apply_objects_sql("reverse_origin", &setting);
        assert!(sql.starts_with("BEGIN; SET LOCAL skedaddle.admin_origin = 'forward_capture'; "));
        assert!(
            sql.find("SET LOCAL skedaddle.admin_origin").unwrap()
                < sql.find("CREATE SCHEMA").unwrap()
        );
        assert!(sql.contains("REVOKE ALL ON _skedaddle.apply_progress"));
        assert!(sql.ends_with("COMMIT;"));
        let absent = ddl::administrative_origin_sql(&vec![]).unwrap();
        assert!(!apply_objects_sql("reverse_origin", &absent).contains("admin_origin"));
        assert!(
            ddl::administrative_origin_sql(&vec![vec!["one".into()], vec!["two".into()]]).is_err()
        );
        assert!(ddl::administrative_origin_sql(&vec![vec!["invalid'".into()]]).is_err());
    }
}

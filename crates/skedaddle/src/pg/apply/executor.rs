//! Atomic destination apply. Use a dedicated, non-pooled connection; the origin
//! session lock excludes competing executors before any evidence is read.
//! Callers preserve the dedicated stream/origin binding under coordinator authority.
//! Transactional progress and exact receipts prove commit; native origin high-water
//! can lead them after abort and supplies only a lower-bound consistency check.
//!
//! Receipt retention follows the *durably saved*, never rewound reader checkpoint.
//! Retain all transactions in the unfinished segment; delete only mseqs below
//! checkpoint.next_mseq after its persistence succeeds. A configured receipt cap
//! fails closed instead of dropping evidence if checkpoint persistence stalls.
//! Cancellation poisons an executor/reader loop: reconstruct both from durable
//! state rather than continuing with a partly consumed record or open SQL txn.
use super::{ApplyError, CommitDecision, Progress, Statement, TransactionPlan, resolve_commit};
use crate::{
    buffer::reader::{BufferReader, ReaderCheckpoint},
    event::{AssembledTxn, TxnId},
};
use async_trait::async_trait;
use std::{fmt, future::Future};
use tokio_postgres::{Client, Row, types::ToSql};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// Includes row-count faults for the verifier; no checkpoint was saved.
    Apply(ApplyError),
    Buffer(crate::buffer::reader::Error),
    Database,
    Checkpoint,
    ReceiptLimit,
    Blocked,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never expose SQL error details, connection strings or row values.
        match self {
            Self::Apply(e) => e.fmt(f),
            Self::Buffer(e) => e.fmt(f),
            other => write!(f, "destination executor: {other:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<ApplyError> for Error {
    fn from(e: ApplyError) -> Self {
        Self::Apply(e)
    }
}
impl From<tokio_postgres::Error> for Error {
    fn from(_: tokio_postgres::Error) -> Self {
        Self::Database
    }
}

/// The coordinator must journal this intent before the installer changes SQL.
pub const INSTALL_INTENT: &str = "install-skedaddle-apply-progress-and-receipts-v2";
pub const PROGRESS_DDL: &str = "CREATE SCHEMA IF NOT EXISTS _skedaddle;
CREATE TABLE IF NOT EXISTS _skedaddle.apply_progress (
 stream text PRIMARY KEY, last_mseq numeric NOT NULL, last_txn bytea NOT NULL,
 last_position bytea NOT NULL, last_digest bytea NOT NULL);
CREATE TABLE IF NOT EXISTS _skedaddle.apply_receipts (
 stream text NOT NULL, mseq numeric NOT NULL, txn bytea NOT NULL,
 position bytea NOT NULL CHECK (octet_length(position) = 8),
 digest bytea NOT NULL CHECK (octet_length(digest) = 32),
 PRIMARY KEY (stream, mseq, position));
ALTER TABLE _skedaddle.apply_progress ADD COLUMN IF NOT EXISTS record_kind smallint,
 ADD COLUMN IF NOT EXISTS manifest_digest bytea,
 ADD COLUMN IF NOT EXISTS previous_manifest_digest bytea;
ALTER TABLE _skedaddle.apply_receipts ADD COLUMN IF NOT EXISTS record_kind smallint,
 ADD COLUMN IF NOT EXISTS manifest_digest bytea,
 ADD COLUMN IF NOT EXISTS previous_manifest_digest bytea;";

pub async fn install_progress<F, Fut>(client: &mut Client, journal_intent: F) -> Result<()>
where
    F: FnOnce(&'static str) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    journal_intent(INSTALL_INTENT).await?;
    let transaction = client.transaction().await?;
    // Serialize concurrent installers, including parallel harness fixtures.
    // IF NOT EXISTS alone does not serialize catalog creation checks.
    transaction
        .query_one(
            "SELECT pg_catalog.pg_advisory_xact_lock(1936418148, 49)",
            &[],
        )
        .await?;
    let rows = transaction
        .query(crate::pg::ddl::ADMIN_INSTALLATION_SQL, &[])
        .await?;
    let installations = rows
        .iter()
        .map(|row| vec![row.get::<_, String>(0)])
        .collect();
    let origin =
        crate::pg::ddl::administrative_origin_sql(&installations).map_err(|_| Error::Database)?;
    transaction
        .batch_execute(&format!("{origin}{PROGRESS_DDL}"))
        .await?;
    transaction.commit().await?;
    Ok(())
}

/// Production supplies its TLS connector and drives each returned Client's
/// connection future. No TLS policy or credentials are embedded in this module.
#[async_trait]
pub trait Connector: Send + Sync {
    async fn connect(&mut self) -> Result<Client>;
}

/// This seam exists to inject acknowledgment failures in local-FS ordering
/// tests. PostgreSQL effects are qualified separately using the real executor.
#[async_trait]
pub trait Destination: Send {
    async fn apply(&mut self, plan: &TransactionPlan) -> Result<CommitDecision>;
    /// Called only AFTER the checkpoint callback has durably saved this value.
    async fn retain_after_checkpoint(&mut self, checkpoint: &ReaderCheckpoint) -> Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seam {
    BeforeCommit,
    AfterCommit,
    BeforeAck,
}

/// Explicit fault-injection/observation seams; never driven by environment.
#[async_trait]
pub trait Observer: Send {
    async fn observe(&mut self, seam: Seam, client: &Client) -> Result<()>;
}
struct NoObserver;
#[async_trait]
impl Observer for NoObserver {
    async fn observe(&mut self, _: Seam, _: &Client) -> Result<()> {
        Ok(())
    }
}

pub struct Executor<C> {
    connector: C,
    client: Option<Client>,
    stream: String,
    origin: String,
    max_receipts: u32,
    blocked: bool,
}
impl<C: Connector> Executor<C> {
    pub fn new(connector: C, stream: String, origin: String, max_receipts: u32) -> Result<Self> {
        if stream.is_empty()
            || stream.len() > 512
            || stream.contains('\0')
            || origin.is_empty()
            || origin.len() > 512
            || origin.contains('\0')
            || max_receipts == 0
        {
            return Err(ApplyError::Invalid("executor stream/origin/receipt bound").into());
        }
        Ok(Self {
            connector,
            client: None,
            stream,
            origin,
            max_receipts,
            blocked: false,
        })
    }
    async fn connect(&mut self) -> Result<()> {
        // Dropping the old dedicated session releases its origin lock. If that
        // backend has not exited yet, PG rejects setup: safe failure, no replay.
        self.client.take();
        let client = self.connector.connect().await?;
        client
            .query_one(
                "SELECT pg_catalog.pg_replication_origin_session_setup($1)",
                &[&self.origin],
            )
            .await?;
        self.client = Some(client);
        Ok(())
    }
    fn client(&self) -> &Client {
        self.client.as_ref().expect("connected executor")
    }

    // Ownership stabilizes these separate reads. Flush native high-water, but
    // never infer committed effects from it or normalize a legitimate abort lead.
    async fn decision(&self, target: &Progress) -> Result<CommitDecision> {
        let latest = self.client().query_opt("SELECT last_mseq::text, last_txn, last_position, last_digest, record_kind, manifest_digest, previous_manifest_digest FROM _skedaddle.apply_progress WHERE stream = $1", &[&self.stream]).await?;
        let latest = latest
            .as_ref()
            .map(|r| decode_progress(&self.stream, r))
            .transpose()?;
        let receipt = self.client().query_opt("SELECT mseq::text, txn, position, digest, record_kind, manifest_digest, previous_manifest_digest FROM _skedaddle.apply_receipts WHERE stream = $1 AND mseq = $2::text::numeric AND position = $3", &[&self.stream, &target.mseq.to_string(), &target.lsn.to_be_bytes().as_slice()]).await?;
        let receipt = receipt
            .as_ref()
            .map(|r| decode_progress(&self.stream, r))
            .transpose()?;
        if let Some(receipt) = &receipt {
            super::verify_receipt(target, receipt)?;
        }
        let origin: Option<String> = self
            .client()
            .query_one(
                "SELECT pg_catalog.pg_replication_origin_progress($1, true)::text",
                &[&self.origin],
            )
            .await?
            .try_get(0)?;
        let origin = origin.map(|v| parse_lsn(&v)).transpose()?.unwrap_or(0);
        Ok(resolve_commit(
            target,
            latest.as_ref(),
            origin,
            receipt.as_ref(),
        )?)
    }

    async fn pending(&self, plan: &TransactionPlan) -> Result<()> {
        self.client()
            .batch_execute("SET LOCAL synchronous_commit = on")
            .await?;
        let count: i64 = self
            .client()
            .query_one(
                "SELECT count(*) FROM _skedaddle.apply_receipts WHERE stream = $1",
                &[&self.stream],
            )
            .await?
            .try_get(0)?;
        if count >= i64::from(self.max_receipts) {
            return Err(Error::ReceiptLimit);
        }
        let mut previous = None;
        for statement in &plan.statements {
            if statement.should_execute(previous)? {
                previous = Some(execute_statement(self.client(), statement).await?);
            }
        }
        let p = &plan.progress;
        let kind = p.evidence.as_ref().map(|e| e.kind as i16);
        let manifest = p.evidence.as_ref().map(|e| e.manifest_digest.to_vec());
        let previous = p
            .evidence
            .as_ref()
            .map(|e| e.previous_manifest_digest.to_vec());
        let inserted = self.client().execute("INSERT INTO _skedaddle.apply_receipts (stream, mseq, txn, position, digest, record_kind, manifest_digest, previous_manifest_digest) VALUES ($1, $2::text::numeric, $3, $4, $5, $6, $7, $8)", &[&p.stream, &p.mseq.to_string(), &p.txn.0, &p.lsn.to_be_bytes().as_slice(), &p.digest.as_slice(), &kind, &manifest, &previous]).await?;
        if inserted != 1 {
            return Err(ApplyError::UnexpectedRowCount(inserted).into());
        }
        Ok(())
    }
    async fn apply_inner(
        &mut self,
        plan: &TransactionPlan,
        observer: &mut dyn Observer,
    ) -> Result<CommitDecision> {
        if plan.progress.stream != self.stream
            || plan.progress.txn.0.len() > 512
            || plan.session_setup.parameters.len() != 1
            || plan.session_setup.parameters[0].bytes() != Some(self.origin.as_bytes())
        {
            return Err(ApplyError::Invalid("executor plan identity/receipt size").into());
        }
        if self.client.is_none() {
            self.connect().await?;
        }
        for attempt in 0..2 {
            if self.decision(&plan.progress).await? == CommitDecision::Applied {
                return Ok(CommitDecision::Applied);
            }
            self.client().batch_execute("BEGIN").await?;
            if let Err(error) = self.pending(plan).await {
                let _ = self.client().batch_execute("ROLLBACK").await;
                return Err(error);
            }
            if let Err(error) = observer.observe(Seam::BeforeCommit, self.client()).await {
                let _ = self.client().batch_execute("ROLLBACK").await;
                return Err(error);
            }
            match self.client().batch_execute("COMMIT").await {
                Ok(()) => {
                    observer.observe(Seam::AfterCommit, self.client()).await?;
                    observer.observe(Seam::BeforeAck, self.client()).await?;
                    return Ok(CommitDecision::Applied);
                }
                Err(error) if error.as_db_error().is_none() => {
                    self.connect().await?;
                    match self.decision(&plan.progress).await? {
                        CommitDecision::Applied => return Ok(CommitDecision::Applied),
                        CommitDecision::NotApplied if attempt == 0 => continue,
                        CommitDecision::NotApplied => return Err(Error::Database),
                    }
                }
                Err(_) => {
                    let _ = self.client().batch_execute("ROLLBACK").await;
                    return Err(Error::Database);
                }
            }
        }
        Err(Error::Database)
    }
    pub async fn apply_observed(
        &mut self,
        plan: &TransactionPlan,
        observer: &mut dyn Observer,
    ) -> Result<CommitDecision> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        self.blocked = true;
        let result = self.apply_inner(plan, observer).await;
        if result.is_ok() {
            self.blocked = false;
        }
        result
    }

    /// Release the dedicated origin session before dropping the client.
    /// A poisoned executor can only be closed or dropped.
    pub async fn close(mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            client.batch_execute("ROLLBACK").await?;
            client
                .query_one(
                    "SELECT pg_catalog.pg_replication_origin_session_reset()",
                    &[],
                )
                .await?;
        }
        Ok(())
    }
}
#[async_trait]
impl<C: Connector> Destination for Executor<C> {
    async fn apply(&mut self, plan: &TransactionPlan) -> Result<CommitDecision> {
        self.apply_observed(plan, &mut NoObserver).await
    }
    async fn retain_after_checkpoint(&mut self, checkpoint: &ReaderCheckpoint) -> Result<()> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        if self.client.is_none() {
            self.connect().await?;
        }
        self.client().execute("DELETE FROM _skedaddle.apply_receipts WHERE stream = $1 AND mseq < $2::text::numeric", &[&self.stream, &checkpoint.next_mseq.to_string()]).await?;
        Ok(())
    }
}

async fn execute_statement(client: &Client, statement: &Statement) -> Result<u64> {
    let parameters: Vec<&(dyn ToSql + Sync)> =
        statement.parameters.iter().map(|p| p as _).collect();
    // Explicit types are required: a parameter's text/binary format belongs to
    // its destination descriptor, not to server-side expression inference.
    let types: Vec<_> = statement
        .parameters
        .iter()
        .map(|p| p.wire_type().clone())
        .collect();
    let prepared = client.prepare_typed(&statement.sql, &types).await?;
    let rows = if prepared.columns().is_empty() {
        client.execute(&prepared, &parameters).await?
    } else {
        client.query(&prepared, &parameters).await?.len() as u64
    };
    statement.check_rows(rows)?;
    Ok(rows)
}
fn decode_progress(stream: &str, row: &Row) -> Result<Progress> {
    let sequence: String = row.try_get(0)?;
    let position: Vec<u8> = row.try_get(2)?;
    let digest: Vec<u8> = row.try_get(3)?;
    let bad = || Error::Apply(ApplyError::InconsistentProgress);
    let kind: Option<i16> = row.try_get(4)?;
    let manifest: Option<Vec<u8>> = row.try_get(5)?;
    let previous: Option<Vec<u8>> = row.try_get(6)?;
    let mut progress = Progress {
        stream: stream.into(),
        mseq: sequence.parse().map_err(|_| bad())?,
        txn: TxnId(row.try_get(1)?),
        lsn: u64::from_be_bytes(position.try_into().map_err(|_| bad())?),
        digest: digest.try_into().map_err(|_| bad())?,
        evidence: None,
    };
    progress.evidence = match (kind, manifest, previous) {
        (None, None, None) => None, // Backward-safe v1 rows, never a pair proof.
        (Some(kind), Some(manifest), Some(previous)) => Some(super::RecordEvidence::new(
            match kind {
                1 => super::RecordKind::Transaction,
                2 => super::RecordKind::ChunkRows,
                _ => return Err(bad()),
            },
            manifest.try_into().map_err(|_| bad())?,
            previous.try_into().map_err(|_| bad())?,
            &progress,
        )),
        _ => return Err(bad()),
    };
    Ok(progress)
}
fn parse_lsn(value: &str) -> Result<u64> {
    let bad = || Error::Apply(ApplyError::InconsistentProgress);
    let (hi, lo) = value.split_once('/').ok_or_else(bad)?;
    let hi = u32::from_str_radix(hi, 16).map_err(|_| bad())?;
    let lo = u32::from_str_radix(lo, 16).map_err(|_| bad())?;
    Ok((u64::from(hi) << 32) | u64::from(lo))
}

pub struct ReaderLoop {
    reader: BufferReader,
    stream: String,
    blocked: bool,
    initialized: bool,
}
impl ReaderLoop {
    pub fn new(reader: BufferReader, stream: String) -> Self {
        Self {
            reader,
            stream,
            blocked: false,
            initialized: false,
        }
    }
    /// Returns false at the idle tail. Errors/cancellation require reconstructing
    /// from the last durable checkpoint; this instance cannot consume past faults.
    pub async fn step<P, S, Fut>(
        &mut self,
        destination: &mut dyn Destination,
        plan: P,
        save_checkpoint: S,
    ) -> Result<bool>
    where
        P: FnOnce(&AssembledTxn, u64) -> super::Result<TransactionPlan>,
        S: FnOnce(ReaderCheckpoint) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if self.blocked {
            return Err(Error::Blocked);
        }
        self.blocked = true;
        if !self.initialized {
            // The constructor's checkpoint is trusted durable coordinator state.
            // Recover a crash after saving it but before receipt GC, otherwise a
            // full receipt cap could prevent the next transaction indefinitely.
            if self.reader.checkpoint().next_mseq != 0 {
                destination
                    .retain_after_checkpoint(self.reader.checkpoint())
                    .await?;
            }
            self.initialized = true;
        }
        let before = self.reader.checkpoint().clone();
        let Some(txn) = self.reader.next_txn().await.map_err(Error::Buffer)? else {
            self.blocked = false;
            return Ok(false);
        };
        let verified = self
            .reader
            .last_verified()
            .ok_or(ApplyError::Invalid("missing verified manifest"))?;
        let mseq = verified.entry().mseq;
        let mut plan = plan(&txn, mseq)?;
        if plan.progress.mseq != mseq
            || plan.progress.stream != self.stream
            || plan.progress.txn != txn.txn
            || plan.progress.digest != txn.digest
            || plan.progress.lsn != super::lsn(&txn)?
        {
            return Err(ApplyError::Invalid("reader plan identity").into());
        }
        // BufferReader exposes the authenticated envelope kind at segment ends.
        // At a cross-manifest equal-H boundary its verified ordering also proves
        // the first record is ChunkRows, even for an empty snapshot batch.
        let after = self.reader.checkpoint();
        let kind = if after.next_mseq
            == mseq
                .checked_add(1)
                .ok_or(ApplyError::InconsistentProgress)?
        {
            after.last_record.as_ref().map(|r| {
                if r.chunk_rows {
                    super::RecordKind::ChunkRows
                } else {
                    super::RecordKind::Transaction
                }
            })
        } else if before.next_mseq == mseq
            && before.last_position.as_ref() == Some(&txn.commit_position)
            && before.last_record.as_ref().is_some_and(|r| !r.chunk_rows)
        {
            Some(super::RecordKind::ChunkRows)
        } else {
            None
        };
        plan.progress.evidence = kind.map(|kind| {
            super::RecordEvidence::new(
                kind,
                verified.digest(),
                verified.entry().prev_digest,
                &plan.progress,
            )
        });
        *plan
            .statements
            .last_mut()
            .ok_or(ApplyError::Invalid("missing progress write"))? =
            super::progress_statement(&plan.progress);
        if destination.apply(&plan).await? != CommitDecision::Applied {
            return Err(ApplyError::InconsistentProgress.into());
        }
        let checkpoint = self.reader.checkpoint().clone();
        save_checkpoint(checkpoint.clone()).await?;
        destination.retain_after_checkpoint(&checkpoint).await?;
        self.blocked = false;
        Ok(true)
    }
}

#[cfg(test)]
mod tests;

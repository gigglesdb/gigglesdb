//! Atomic GigglesDB apply using transactional progress and bounded receipts.
//!
//! The coordinator must exclusively own the stream and its dedicated connection;
//! GigglesDB has no replication-origin session lock. Cancellation or errors poison
//! an executor: reconstruct it from durable state before continuing.
use crate::{
    buffer::reader::ReaderCheckpoint,
    pg::apply::{
        ApplyError, CommitDecision, OriginPolicy, Progress, RowCount, SourcePositions, TransactionPlan,
        executor::{
            Connector, Destination, Error, INSTALL_INTENT, Observer, PROGRESS_DDL, Result, Seam,
            decode_progress, execute_statement,
        },
        resolve_commit,
    },
};
use async_trait::async_trait;
use std::future::Future;
use tokio_postgres::Client;

const SESSION: &str = "SELECT pg_catalog.pg_replication_origin_session_setup($1)";
const ROLE: &str = "SET LOCAL session_replication_role = replica";
const XACT: &str = "SELECT pg_catalog.pg_replication_origin_xact_setup($1, $2)";

/// Remove only the PG planner's exact origin/role statements. The remaining SQL
/// must use a planner-generated row/progress form; new session commands require
/// an explicit compatibility decision rather than accidentally executing them.
pub fn plan_for_gigglesdb(
    mut plan: TransactionPlan,
) -> std::result::Result<TransactionPlan, ApplyError> {
    if plan.origin_policy != OriginPolicy::NativePgLsn {
        return Err(ApplyError::OriginPolicyMismatch);
    }
    if plan.session_setup.sql != SESSION
        && !(plan.session_setup.sql.is_empty() && plan.session_setup.parameters.is_empty())
    {
        return Err(ApplyError::Invalid("GigglesDB session statement"));
    }
    plan.session_setup.sql.clear();
    plan.session_setup.parameters.clear();
    plan.session_setup.row_count = RowCount::Any;
    plan.session_setup.if_previous_missing = false;
    plan.statements.retain(|s| s.sql != ROLE && s.sql != XACT);
    for statement in &plan.statements {
        if ![
            "INSERT INTO ",
            "UPDATE ",
            "DELETE FROM ",
            "SELECT 1 FROM ",
            "TRUNCATE TABLE ",
        ]
        .iter()
        .any(|prefix| statement.sql.starts_with(prefix))
        {
            return Err(ApplyError::Invalid("GigglesDB transaction statement"));
        }
    }
    Ok(plan)
}

/// Journal first, then install the shared schema atomically without PG advisory
/// locks, replication origins, or unsupported session settings.
pub async fn install_progress<F, Fut>(client: &mut Client, journal_intent: F) -> Result<()>
where
    F: FnOnce(&'static str) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    journal_intent(INSTALL_INTENT).await?;
    let transaction = client.transaction().await?;
    transaction.batch_execute(PROGRESS_DDL).await?;
    transaction.commit().await?;
    Ok(())
}

struct NoObserver;
#[async_trait]
impl Observer for NoObserver {
    async fn observe(&mut self, _: Seam, _: &Client) -> Result<()> {
        Ok(())
    }
}

pub struct GigglesdbExecutor<C> {
    connector: C,
    client: Option<Client>,
    stream: String,
    max_receipts: u32,
    blocked: bool,
    positions: SourcePositions,
}

impl<C: Connector> GigglesdbExecutor<C> {
    pub fn new(connector: C, stream: String, max_receipts: u32, positions: SourcePositions) -> Result<Self> {
        if positions.policy != OriginPolicy::NativePgLsn {
            return Err(ApplyError::OriginPolicyMismatch.into());
        }
        if stream.is_empty() || stream.len() > 512 || stream.contains('\0') || max_receipts == 0 {
            return Err(ApplyError::Invalid("executor stream/receipt bound").into());
        }
        Ok(Self {
            connector,
            client: None,
            stream,
            max_receipts,
            blocked: false,
            positions,
        })
    }

    async fn connect(&mut self) -> Result<()> {
        self.client.take();
        self.client = Some(self.connector.connect().await?);
        Ok(())
    }

    fn client(&self) -> &Client {
        self.client.as_ref().expect("connected executor")
    }

    async fn decision_connected(&self, target: &Progress) -> Result<CommitDecision> {
        let latest = self.client().query_opt("SELECT last_mseq::text, last_txn, last_position, last_digest, record_kind, manifest_digest, previous_manifest_digest FROM _skedaddle.apply_progress WHERE stream = $1", &[&self.stream]).await?;
        let latest = latest
            .as_ref()
            .map(|r| decode_progress(&self.stream, &self.positions, r))
            .transpose()?;
        let receipt = self.client().query_opt("SELECT mseq::text, txn, position, digest, record_kind, manifest_digest, previous_manifest_digest FROM _skedaddle.apply_receipts WHERE stream = $1 AND mseq = $2::text::numeric AND position = $3", &[&self.stream, &target.mseq.to_string(), &target.position.bytes()]).await?;
        let receipt = receipt
            .as_ref()
            .map(|r| decode_progress(&self.stream, &self.positions, r))
            .transpose()?;
        // No native origin exists. Derive its consistency argument from the same
        // committed row, preserving resolve_commit's identity/digest/receipt and
        // ordering checks without claiming an independent native high-water mark.
        let origin_lsn = latest.as_ref().map(|p| self.positions.native_lsn(&p.position)).transpose()?.unwrap_or(0);
        Ok(resolve_commit(
            &self.positions,
            target,
            latest.as_ref(),
            origin_lsn,
            receipt.as_ref(),
        )?)
    }

    /// Read commit evidence on a fresh executor after a crash or lost reply.
    pub async fn decision(&mut self, target: &Progress) -> Result<CommitDecision> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        if target.stream != self.stream {
            return Err(ApplyError::Invalid("executor plan identity/receipt size").into());
        }
        if self.client.is_none() {
            self.connect().await?;
        }
        self.decision_connected(target).await
    }

    async fn pending(&self, plan: &TransactionPlan) -> Result<()> {
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
        let kind = p.evidence.as_ref().map(|e| e.kind() as i16);
        let manifest = p.evidence.as_ref().map(|e| e.manifest_digest().to_vec());
        let previous = p
            .evidence
            .as_ref()
            .map(|e| e.previous_manifest_digest().to_vec());
        let inserted = self.client().execute("INSERT INTO _skedaddle.apply_receipts (stream, mseq, txn, position, digest, record_kind, manifest_digest, previous_manifest_digest) VALUES ($1, $2::text::numeric, $3, $4, $5, $6, $7, $8)", &[&p.stream, &p.mseq.to_string(), &p.txn.0, &p.position.bytes(), &p.digest.as_slice(), &kind, &manifest, &previous]).await?;
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
        if plan.progress.stream != self.stream || plan.progress.txn.0.len() > 512 {
            return Err(ApplyError::Invalid("executor plan identity/receipt size").into());
        }
        let plan = plan_for_gigglesdb(plan.clone())?;
        if self.client.is_none() {
            self.connect().await?;
        }
        for attempt in 0..2 {
            if self.decision_connected(&plan.progress).await? == CommitDecision::Applied {
                return Ok(CommitDecision::Applied);
            }
            self.client().batch_execute("BEGIN").await?;
            if let Err(error) = self.pending(&plan).await {
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
                    match self.decision_connected(&plan.progress).await? {
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

    pub async fn close(mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            client.batch_execute("ROLLBACK").await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<C: Connector> Destination for GigglesdbExecutor<C> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{AssembledTxn, Position, SourceIdentity, TxnId},
        pg::apply::{PlanContext, Statement, plan_transaction},
    };

    fn plan() -> TransactionPlan {
        let txn = AssembledTxn::new(
            SourceIdentity {
                engine: "gigglesdb".into(),
                system_id: "source".into(),
                lineage: "1".into(),
                db_name: "postgres".into(),
                stream: "source".into(),
            },
            TxnId(vec![1]),
            Position::new("pg", 1_u64.to_be_bytes()),
            Some(1_700_000_000_000_000),
            vec![],
        )
        .unwrap();
        plan_transaction(
            &txn,
            &[],
            &PlanContext {
                positions: SourcePositions::native_pg("pg"),
                stream: "forward".into(),
                mseq: 1,
                origin: "unused".into(),
                synthetic_commit_time: None,
            },
            |_, _| unreachable!(),
        )
        .unwrap()
    }

    #[test]
    fn removes_exactly_origin_and_role_statements() {
        let original = plan();
        assert_eq!(original.session_setup.sql, SESSION);
        assert_eq!(
            original
                .statements
                .iter()
                .map(|s| s.sql.as_str())
                .take(2)
                .collect::<Vec<_>>(),
            [ROLE, XACT]
        );
        let filtered = plan_for_gigglesdb(original.clone()).unwrap();
        assert!(filtered.session_setup.sql.is_empty());
        assert!(filtered.session_setup.parameters.is_empty());
        assert_eq!(filtered.statements.len(), original.statements.len() - 2);
        for (kept, expected) in filtered.statements.iter().zip(&original.statements[2..]) {
            assert_eq!(kept.sql, expected.sql);
            assert_eq!(kept.row_count, expected.row_count);
            assert_eq!(kept.if_previous_missing, expected.if_previous_missing);
            assert_eq!(
                kept.parameters
                    .iter()
                    .map(|p| (p.wire_type(), p.bytes()))
                    .collect::<Vec<_>>(),
                expected
                    .parameters
                    .iter()
                    .map(|p| (p.wire_type(), p.bytes()))
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(filtered.progress, original.progress);
        assert!(plan_for_gigglesdb(filtered).is_ok());
    }

    #[test]
    fn rejects_unknown_session_sql_and_near_matches() {
        for sql in [
            "SET LOCAL synchronous_commit = on",
            "RESET ALL",
            "SELECT pg_catalog.pg_replication_origin_session_reset()",
            "SET LOCAL session_replication_role = replica ",
            "SELECT pg_catalog.pg_replication_origin_xact_setup($1,$2)",
        ] {
            let mut p = plan();
            p.statements.push(Statement {
                sql: sql.into(),
                parameters: vec![],
                row_count: RowCount::Any,
                if_previous_missing: false,
            });
            assert!(plan_for_gigglesdb(p).is_err(), "{sql}");
        }
        let mut p = plan();
        p.session_setup.sql.push(' ');
        assert!(plan_for_gigglesdb(p).is_err());
    }
}

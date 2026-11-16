//! Request-scoped rollback adapter. Calls only the M10 transaction API: no
//! forward Runtime progress, pending receipt collection, or receipt GC. Receipts
//! remain available even after the rollback request checkpoint becomes durable.
use super::apply::{ApplyError, ApplyResult, MysqlApplier};
use crate::{
    event::AssembledTxn,
    rollback::apply::{ApplyContext, Error, Result, RollbackSink, SinkOutcome},
};
use async_trait::async_trait;

pub struct MysqlRollbackSink<'a> {
    pub applier: &'a mut MysqlApplier,
}

fn outcome(result: ApplyResult) -> SinkOutcome {
    match result {
        ApplyResult::Applied | ApplyResult::ResolvedCommitted => SinkOutcome::Committed,
        ApplyResult::AlreadyApplied => SinkOutcome::AlreadyCommitted,
    }
}

fn error(error: ApplyError) -> Error {
    match error {
        ApplyError::UnknownOutcome(_) | ApplyError::RetryExhausted => Error::Retry,
        ApplyError::Mysql(e) if e.is_transient() => Error::Retry,
        _ => Error::OperatorAction("MySQL reverse apply evidence unavailable"),
    }
}

#[async_trait]
impl RollbackSink for MysqlRollbackSink<'_> {
    async fn apply(&mut self, txn: &AssembledTxn, context: &ApplyContext) -> Result<SinkOutcome> {
        if context.chunk_rows == Some(true) {
            return Err(Error::OperatorAction("MySQL rollback snapshot unsupported"));
        }
        self.applier.apply(txn).await.map(outcome).map_err(error)
    }
    // Intentionally use the no-op checkpoint_durable default: retain receipts.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::client::MysqlError;
    #[tokio::test]
    async fn durable_checkpoint_does_not_connect_or_collect_receipts() {
        // No endpoint exists: checkpoint notification must be entirely inert.
        // Real receipt/replay behavior remains qualified by M10's live tests.
        let config = serde_json::from_value(serde_json::json!({
            "host":"127.0.0.1", "port":0, "ca":"unused",
            "user":"unused", "password":"unused", "profile":"unused",
            "gtid":"ON", "container_id":"unused", "max_decompressed_bytes":1024
        }))
        .unwrap();
        let mut applier = MysqlApplier::new(
            config,
            crate::event::SourceIdentity {
                engine: "postgres".into(),
                system_id: "1".into(),
                lineage: "1".into(),
                db_name: "db".into(),
                stream: "reverse".into(),
            },
            Default::default(),
        )
        .unwrap();
        let mut sink = MysqlRollbackSink {
            applier: &mut applier,
        };
        sink.checkpoint_durable(&Default::default()).await.unwrap();
    }
    #[test]
    fn m10_committed_outcomes_allow_checkpointing() {
        assert_eq!(outcome(ApplyResult::Applied), SinkOutcome::Committed);
        assert_eq!(
            outcome(ApplyResult::ResolvedCommitted),
            SinkOutcome::Committed
        );
        assert_eq!(
            outcome(ApplyResult::AlreadyApplied),
            SinkOutcome::AlreadyCommitted
        );
    }
    #[test]
    fn uncertain_commits_retry_and_conflicts_hold_fences() {
        for e in [
            ApplyError::UnknownOutcome(MysqlError::StreamEnded),
            ApplyError::RetryExhausted,
            ApplyError::Mysql(MysqlError::StreamEnded),
        ] {
            assert!(matches!(error(e), Error::Retry));
        }
        for e in [
            ApplyError::IdentityConflict,
            ApplyError::DigestConflict,
            ApplyError::BinlogDisabled,
            ApplyError::Blocked,
            ApplyError::Mysql(MysqlError::Invalid("configuration")),
        ] {
            assert!(matches!(error(e), Error::OperatorAction(_)));
        }
    }
}

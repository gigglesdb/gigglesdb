//! Journal-authenticated destination-applier progress; never capture progress.
use super::evidence::{MysqlAppliedCheckpoint, comparable};
use crate::{
    coordinator::{SourceConnector, journal::Journal},
    event::{Position, PositionOrder, SourceIdentity},
    mysql::MysqlOrder,
    verify::{Result, VerifyError},
};
use async_trait::async_trait;

/// Reads only the applied checkpoint published after destination commit by the
/// Opaque-v3 applier. The journal and expected identity must belong to the same
/// migration. No cached Runtime state or unflushed capture progress is accepted.
pub struct JournalMysqlCheckpoint {
    journal: Journal,
    source: SourceIdentity,
    order: MysqlOrder,
}
impl JournalMysqlCheckpoint {
    pub fn new(journal: Journal, source: SourceIdentity, order: MysqlOrder) -> Result<Self> {
        if source.engine != "mysql"
            || [
                &source.system_id,
                &source.lineage,
                &source.db_name,
                &source.stream,
            ]
            .iter()
            .any(|s| s.is_empty())
        {
            return Err(rejected());
        }
        Ok(Self {
            journal,
            source,
            order,
        })
    }
}
fn rejected() -> VerifyError {
    VerifyError::ApplierTerminal {
        cause: crate::verify::ApplierTerminalCause::CheckpointRejected,
    }
}
fn applied(
    marker: &str,
    sequence: &str,
    source: &SourceIdentity,
    order: &MysqlOrder,
) -> Result<Option<Position>> {
    let cp = crate::engine::decode_reader_checkpoint(marker).map_err(|_| rejected())?;
    if sequence != cp.next_mseq.to_string() {
        return Err(rejected());
    }
    if cp.next_mseq == 0 {
        return if cp == Default::default() {
            Ok(None)
        } else {
            Err(rejected())
        };
    }
    if cp.last_epoch == 0
        || cp.prev_digest == [0; 32]
        || cp.last_record.as_ref().is_none_or(|r| &r.source != source)
    {
        return Err(rejected());
    }
    let position = cp.last_position.ok_or_else(rejected)?;
    comparable(order, &position, &position)?;
    Ok(Some(position))
}
#[async_trait]
impl MysqlAppliedCheckpoint for JournalMysqlCheckpoint {
    async fn applied_position(&mut self) -> Result<Option<Position>> {
        // Full recovery authenticates the journal chain; malformed progress is
        // an error, not an absent checkpoint or a lower-confidence substitute.
        let state = self.journal.scrub().await.map_err(|_| rejected())?;
        let Some((marker, sequence)) = state.progress.get("applied") else {
            return Ok(None);
        };
        let binding = state.source_binding.as_ref().ok_or_else(rejected)?;
        binding.validate().map_err(|_| rejected())?;
        if binding.connector != SourceConnector::Mysql
            || binding.order_domain != self.order.domain()
            || binding.stream != self.source.stream
        {
            return Err(rejected());
        }
        applied(marker, sequence, &self.source, &self.order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::MysqlPosition;
    fn fixture() -> (SourceIdentity, MysqlOrder, String) {
        let source = SourceIdentity {
            engine: "mysql".into(),
            system_id: "cluster".into(),
            lineage: "one".into(),
            db_name: "db".into(),
            stream: "stream".into(),
        };
        let uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let order = MysqlOrder::file(uuid).unwrap();
        let position = Position::new(
            order.domain(),
            MysqlPosition::File {
                server_uuid: uuid.into(),
                file_seq: 1,
                offset: 100,
            }
            .encode()
            .unwrap(),
        );
        let cp = crate::engine::test_reader_checkpoint(source.clone(), position, 1);
        (
            source,
            order,
            crate::engine::encode_reader_checkpoint(&cp).unwrap(),
        )
    }
    #[tokio::test]
    async fn journal_absence_and_authentication_failure() {
        use std::{sync::Arc, time::Duration};
        let root = crate::test_root::scratch_root()
            .join(format!("verify-checkpoint-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let journal = Journal::new(
            Arc::new(crate::coordinator::journal::local_store(&root).unwrap()),
            "verify",
            "migration",
            Duration::from_secs(3),
        )
        .unwrap();
        let key = root.join(journal.entry_key(1).as_ref());
        let (source, order, _) = fixture();
        let mut checkpoint = JournalMysqlCheckpoint::new(journal, source, order).unwrap();
        assert_eq!(checkpoint.applied_position().await.unwrap(), None);
        std::fs::create_dir_all(key.parent().unwrap()).unwrap();
        std::fs::write(key, br#"{"version":3,"digest":"tampered"}"#).unwrap();
        assert!(matches!(
            checkpoint.applied_position().await,
            Err(VerifyError::ApplierTerminal {
                cause: crate::verify::ApplierTerminalCause::CheckpointRejected
            })
        ));
    }
    #[test]
    fn strict_applied_marker() {
        let (source, order, marker) = fixture();
        let cp = crate::engine::decode_reader_checkpoint(&marker).unwrap();
        let encode = crate::engine::encode_reader_checkpoint;
        assert_eq!(
            applied(&encode(&cp).unwrap(), "1", &source, &order).unwrap(),
            cp.last_position
        );
        assert_eq!(
            applied(&encode(&Default::default()).unwrap(), "0", &source, &order).unwrap(),
            None
        );
        assert!(applied("{}", "1", &source, &order).is_err());
        assert!(applied(&encode(&cp).unwrap(), "2", &source, &order).is_err());
        for mutate in 0..6 {
            let mut bad = cp.clone();
            match mutate {
                0 => bad.version = 2,
                1 => bad.last_epoch = 0,
                2 => bad.prev_digest = [0; 32],
                3 => bad.last_record.as_mut().unwrap().source.stream = "other".into(),
                4 => bad.last_position = Some(Position::new("pg", vec![0; 8])),
                _ => bad.last_position = Some(Position::new(order.domain(), vec![0])),
            }
            assert!(applied(&encode(&bad).unwrap(), "1", &source, &order).is_err());
        }
    }
}

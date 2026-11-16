use super::*;
use crate::{
    event::{Event, EventKind, PositionOrder, SourceIdentity, TxnId},
    mapping::Value,
    mysql::MysqlPosition,
    pg::chunk::ChunkId,
};
const UUID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
fn position(n: u64) -> Position {
    Position::new(
        MysqlOrder::file(UUID).unwrap().domain(),
        MysqlPosition::File {
            server_uuid: UUID.into(),
            file_seq: 1,
            offset: n,
        }
        .encode()
        .unwrap(),
    )
}
fn input(max_bytes: usize, max_commits: usize) -> MysqlCaptureInput {
    MysqlCaptureInput {
        source: update(1, 2).source,
        capture: CaptureEvidence::new(
            position(4),
            Arc::new(MysqlOrder::file(UUID).unwrap()),
            [("db.t".into(), vec!["id".into()])].into(),
        ),
        config: MysqlConfig {
            host: "unused".into(),
            port: 0,
            ca: "unused".into(),
            user: "unused".into(),
            password: "unused".into(),
            profile: "unused".into(),
            gtid: "OFF".into(),
            container_id: "unused".into(),
            max_decompressed_bytes: 1024,
        },
        id: "session".into(),
        attempt: 0,
        max_bytes,
        max_commits,
    }
}
fn range() -> RangePlan {
    RangePlan {
        id: ChunkId {
            table: "db.t".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo: None,
        hi: None,
    }
}
fn marker(i: &MysqlCaptureInput) -> VerifyMarker {
    i.capture
        .state
        .lock()
        .unwrap()
        .armed
        .as_ref()
        .unwrap()
        .marker
        .clone()
}
fn update(before: i64, after: i64) -> Event {
    let row = |n| {
        [(
            "id".into(),
            crate::event::OpaqueValue {
                type_id: "pg_catalog.int8".into(),
                bytes: Some(Value::Int8(n).canonical_bytes().unwrap()),
            },
        )]
        .into()
    };
    Event {
        source: SourceIdentity {
            engine: "mysql".into(),
            system_id: "cluster".into(),
            lineage: "one".into(),
            db_name: "db".into(),
            stream: "stream".into(),
        },
        txn: TxnId(vec![1]),
        ordinal_in_txn: 0,
        position: position(10),
        schema_version: 0,
        kind: EventKind::Update,
        table: Some("db.t".into()),
        key: row(before),
        before: Some(row(before)),
        after: Some(row(after)),
        unchanged_cols: Default::default(),
    }
}
#[tokio::test]
async fn marker_observation_keeps_ordered_buffer_and_all_images() {
    let mut i = input(10000, 10);
    assert_eq!(i.arm(&range()).await.unwrap(), position(4));
    let m = marker(&i);
    let mut insert = update(1, 2);
    insert.kind = EventKind::Insert;
    insert.before = None;
    let mut delete = update(2, 3);
    delete.kind = EventKind::Delete;
    delete.after = None;
    i.capture.commit(
        &position(10),
        &[insert, update(2, 3), update(3, 2), update(2, 2), delete],
        &[],
    );
    i.capture.commit(&position(20), &[], &[]);
    i.capture.commit(&position(30), &[], &[m]);
    assert_eq!(
        i.capture
            .state
            .lock()
            .unwrap()
            .armed
            .as_ref()
            .unwrap()
            .boundary,
        Some(position(30))
    );
    for n in [10, 20, 30] {
        let EvidenceEvent::Commit {
            position: p,
            changes,
        } = i.next().await.unwrap()
        else {
            panic!()
        };
        assert_eq!(p, position(n));
        if n == 10 {
            assert_eq!(changes.len(), 5);
            assert!(changes[0].before.is_none());
            assert!(changes[4].after.is_none());
            assert_eq!(changes[1].before, Some(vec![Value::Int8(2)]));
            assert_eq!(changes[1].after, Some(vec![Value::Int8(3)]));
            assert_eq!(changes[2].after, Some(vec![Value::Int8(2)]));
            assert_eq!(changes[3].before, changes[3].after);
        } else {
            assert!(changes.is_empty());
        }
    }
}
#[tokio::test]
async fn stale_attempt_and_duplicate_commit_fail_closed() {
    let mut i = input(10000, 10);
    i.arm(&range()).await.unwrap();
    let stale = marker(&i);
    i.arm(&range()).await.unwrap();
    i.capture.commit(&position(10), &[], &[stale]);
    assert!(matches!(
        i.next().await,
        Err(VerifyError::EvidenceInvalidated { .. })
    ));
    let mut i = input(10000, 10);
    i.arm(&range()).await.unwrap();
    i.capture.commit(&position(10), &[], &[]);
    i.capture.commit(&position(10), &[], &[]);
    assert!(matches!(
        i.next().await,
        Err(VerifyError::EvidenceInvalidated { .. })
    ));
}
#[tokio::test]
async fn every_coverage_loss_is_sticky_and_rearm_cannot_clear_it() {
    for reason in [
        ContinuityEvent::Gap,
        ContinuityEvent::Reconnect,
        ContinuityEvent::Purge,
        ContinuityEvent::CaptureStop,
    ] {
        let mut i = input(10000, 10);
        i.arm(&range()).await.unwrap();
        i.capture.commit(&position(10), &[], &[]);
        i.capture.invalidate(reason);
        let Err(VerifyError::EvidenceInvalidated { reason: actual }) = i.next().await else {
            panic!()
        };
        assert_eq!(
            actual,
            if matches!(reason, ContinuityEvent::Reconnect) {
                EvidenceInvalidation::Reconnected
            } else {
                EvidenceInvalidation::CaptureGap
            }
        );
        assert!(i.arm(&range()).await.is_err());
    }
}
#[tokio::test]
async fn byte_and_commit_overflow_never_drop_events_silently() {
    for (bytes, commits) in [(1, 10), (10000, 1)] {
        let mut i = input(bytes, commits);
        i.arm(&range()).await.unwrap();
        i.capture.commit(&position(10), &[], &[]);
        i.capture.commit(&position(20), &[], &[]);
        assert!(matches!(
            i.next().await,
            Err(VerifyError::EvidenceOverflow { .. })
        ));
    }
}

//! Scripted inputs exercise the pure seam, not database/capture behavior.
use super::*;
use crate::{event::PositionOrder, mapping::Value, pg::chunk::ChunkId};
use std::collections::VecDeque;

const UUID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const OTHER: &str = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee";
fn order(gtid: bool) -> MysqlOrder {
    if gtid {
        MysqlOrder::gtid("cluster").unwrap()
    } else {
        MysqlOrder::file(UUID).unwrap()
    }
}
fn position(gtid: bool, n: u64) -> Position {
    let native = if gtid {
        MysqlPosition::Gtid {
            set: format!("{UUID}:1-{n}").parse().unwrap(),
            server_uuid: UUID.into(),
            file: "binlog.000001".into(),
            offset: n + 4,
        }
    } else {
        MysqlPosition::File {
            server_uuid: UUID.into(),
            file_seq: 1,
            offset: n + 4,
        }
    };
    Position::new(order(gtid).domain(), native.encode().unwrap())
}
fn key(n: i64) -> Key {
    vec![Value::Int8(n)]
}
struct NumericOrder;
impl KeyOrder for NumericOrder {
    fn compare_keys(&self, a: &Key, b: &Key) -> Result<Ordering> {
        match (a.as_slice(), b.as_slice()) {
            ([Value::Int8(a)], [Value::Int8(b)]) => Ok(a.cmp(b)),
            _ => Err(VerifyError::Invalid("test integer key")),
        }
    }
}
fn range() -> RangePlan {
    RangePlan {
        id: ChunkId {
            table: "db.t".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo: Some(key(0)),
        hi: Some(key(100)),
    }
}
fn change(before: Option<i64>, after: Option<i64>) -> KeyChange {
    KeyChange {
        table: "db.t".into(),
        before: before.map(key),
        after: after.map(key),
    }
}
fn commit(gtid: bool, n: u64, changes: Vec<KeyChange>) -> EvidenceEvent {
    EvidenceEvent::Commit {
        position: position(gtid, n),
        changes,
    }
}
struct Input {
    start: Position,
    boundary: Position,
    events: VecDeque<EvidenceEvent>,
    armed: bool,
}
#[async_trait]
impl MysqlEvidenceInput for Input {
    async fn arm(&mut self, _: &RangePlan) -> Result<Position> {
        self.armed = true;
        Ok(self.start.clone())
    }
    async fn boundary(&mut self) -> Result<Position> {
        assert!(
            self.armed,
            "must arm before requesting the post-read marker"
        );
        Ok(self.boundary.clone())
    }
    async fn next(&mut self) -> Result<EvidenceEvent> {
        match self.events.pop_front() {
            Some(event) => Ok(event),
            None => std::future::pending().await,
        }
    }
}
fn collector(gtid: bool, events: Vec<EvidenceEvent>) -> MysqlChangeEvidence<Input, NumericOrder> {
    MysqlChangeEvidence::new(
        Input {
            start: position(gtid, 1),
            boundary: position(gtid, 5),
            events: events.into(),
            armed: false,
        },
        order(gtid),
        NumericOrder,
        Duration::from_millis(30),
        100,
    )
    .unwrap()
}
async fn collect(gtid: bool, events: Vec<EvidenceEvent>) -> Result<ChangeEvidence> {
    let mut evidence = collector(gtid, events);
    evidence.arm(&range()).await?;
    evidence.changes(&range(), position(gtid, 3)).await
}

#[tokio::test]
async fn key_change_keeps_both_identities_and_orders_deduplicates() {
    for gtid in [false, true] {
        let evidence = collect(
            gtid,
            vec![
                commit(
                    gtid,
                    2,
                    vec![change(Some(9), Some(2)), change(Some(2), Some(9))],
                ),
                commit(gtid, 5, vec![]),
            ],
        )
        .await
        .unwrap();
        assert_eq!(evidence.keys, vec![key(2), key(9)]);
        assert_eq!(evidence.through_position, position(gtid, 5));
    }
}
#[tokio::test]
async fn reverted_writes_are_retained_even_within_one_transaction() {
    for gtid in [false, true] {
        let evidence = collect(
            gtid,
            vec![
                commit(
                    gtid,
                    4,
                    vec![
                        change(Some(1), Some(7)),
                        change(Some(7), Some(1)),
                        change(Some(8), Some(8)),
                    ],
                ),
                commit(gtid, 5, vec![]),
            ],
        )
        .await
        .unwrap();
        assert_eq!(evidence.keys, vec![key(1), key(7), key(8)]);
    }
}
#[tokio::test]
async fn inserts_and_deletes_survive_absence_of_current_rows() {
    for gtid in [false, true] {
        assert_eq!(
            collect(
                gtid,
                vec![
                    commit(
                        gtid,
                        4,
                        vec![
                            change(None, Some(4)),
                            change(Some(4), None),
                            change(Some(6), None)
                        ]
                    ),
                    commit(gtid, 5, vec![])
                ]
            )
            .await
            .unwrap()
            .keys,
            vec![key(4), key(6)]
        );
    }
}
#[tokio::test]
async fn in_flight_commit_before_snapshot_sample_is_not_filtered() {
    for gtid in [false, true] {
        // Transaction began before arm; producer delivers its entire images at
        // commit 2, even though the later snapshot sample is position 3.
        assert_eq!(
            collect(
                gtid,
                vec![
                    commit(gtid, 2, vec![change(Some(3), Some(4))]),
                    commit(gtid, 5, vec![])
                ]
            )
            .await
            .unwrap()
            .keys,
            vec![key(3), key(4)]
        );
    }
}
async fn continuity(event: ContinuityEvent, reason: EvidenceInvalidation) {
    for gtid in [false, true] {
        assert!(
            matches!(collect(gtid, vec![commit(gtid, 2, vec![]), EvidenceEvent::Continuity(event), commit(gtid, 5, vec![])]).await, Err(VerifyError::EvidenceInvalidated { reason: actual }) if actual == reason)
        );
    }
}
#[tokio::test]
async fn gap_fails_closed() {
    continuity(ContinuityEvent::Gap, EvidenceInvalidation::CaptureGap).await;
}
#[tokio::test]
async fn reconnect_fails_closed() {
    continuity(
        ContinuityEvent::Reconnect,
        EvidenceInvalidation::Reconnected,
    )
    .await;
}
#[tokio::test]
async fn purge_fails_closed() {
    continuity(ContinuityEvent::Purge, EvidenceInvalidation::CaptureGap).await;
}
#[tokio::test]
async fn capture_stop_fails_closed() {
    continuity(
        ContinuityEvent::CaptureStop,
        EvidenceInvalidation::CaptureGap,
    )
    .await;
}
#[tokio::test]
async fn out_of_order_and_duplicate_commits_rejected() {
    for gtid in [false, true] {
        for n in [2, 3] {
            assert!(
                collect(
                    gtid,
                    vec![
                        commit(gtid, 3, vec![]),
                        commit(gtid, n, vec![]),
                        commit(gtid, 5, vec![])
                    ]
                )
                .await
                .is_err()
            );
        }
    }
}
#[tokio::test]
async fn missing_boundary_waits_until_timeout_and_disarms() {
    for gtid in [false, true] {
        let mut evidence = collector(gtid, vec![commit(gtid, 4, vec![])]);
        evidence.arm(&range()).await.unwrap();
        let started = Instant::now();
        assert!(matches!(
            evidence.changes(&range(), position(gtid, 3)).await,
            Err(VerifyError::EvidenceInvalidated { .. })
        ));
        assert!(started.elapsed() >= evidence.timeout);
        assert!(evidence.changes(&range(), position(gtid, 3)).await.is_err());
    }
}
#[tokio::test]
async fn jumping_over_marker_is_not_complete_evidence() {
    for gtid in [false, true] {
        assert!(collect(gtid, vec![commit(gtid, 6, vec![])]).await.is_err());
    }
}
#[tokio::test]
async fn table_and_range_filter_both_images_independently() {
    for gtid in [false, true] {
        let mut foreign = change(Some(5), Some(6));
        foreign.table = "db.other".into();
        assert_eq!(
            collect(
                gtid,
                vec![
                    commit(
                        gtid,
                        2,
                        vec![
                            foreign,
                            change(Some(-1), Some(2)),
                            change(Some(99), Some(100))
                        ]
                    ),
                    commit(gtid, 5, vec![])
                ]
            )
            .await
            .unwrap()
            .keys,
            vec![key(2), key(99)]
        );
    }
}
#[tokio::test]
async fn unarmed_wrong_range_and_overflow_fail_closed() {
    let mut evidence = collector(
        false,
        vec![commit(
            false,
            5,
            vec![change(None, Some(1)), change(None, Some(2))],
        )],
    );
    assert!(
        evidence
            .changes(&range(), position(false, 3))
            .await
            .is_err()
    );
    evidence.arm(&range()).await.unwrap();
    let mut wrong = range();
    wrong.id.ordinal = 1;
    assert!(evidence.changes(&wrong, position(false, 3)).await.is_err());
    evidence.max_keys = 1;
    evidence.arm(&range()).await.unwrap();
    assert!(matches!(
        evidence.changes(&range(), position(false, 3)).await,
        Err(VerifyError::EvidenceOverflow {
            limit: EvidenceLimit::Keys(1),
            ..
        })
    ));
}

struct Checkpoints(VecDeque<Option<Position>>);
#[async_trait]
impl MysqlAppliedCheckpoint for Checkpoints {
    async fn applied_position(&mut self) -> Result<Option<Position>> {
        Ok(self.0.pop_front().flatten())
    }
}
fn waiter(gtid: bool, positions: Vec<Option<Position>>) -> MysqlAppliedWaiter<Checkpoints> {
    MysqlAppliedWaiter::new(
        Checkpoints(positions.into()),
        order(gtid),
        Duration::from_millis(250),
        Duration::from_millis(1),
    )
    .unwrap()
}
#[tokio::test]
async fn applied_wait_succeeds_at_or_beyond_boundary_in_both_modes() {
    for gtid in [false, true] {
        for n in [5, 6] {
            assert_eq!(
                waiter(
                    gtid,
                    vec![None, Some(position(gtid, 2)), Some(position(gtid, n))]
                )
                .wait_until(position(gtid, 5))
                .await
                .unwrap(),
                position(gtid, n)
            );
        }
    }
}
#[tokio::test]
async fn applied_wait_times_out_in_both_modes() {
    for gtid in [false, true] {
        assert!(
            matches!(waiter(gtid, vec![Some(position(gtid, 4))]).wait_until(position(gtid, 5)).await, Err(VerifyError::ApplierTimeout { waited_position, .. }) if waited_position == position(gtid, 5))
        );
    }
}
#[tokio::test]
async fn applied_wait_rejects_wrong_domain_and_mode_in_both_modes() {
    for gtid in [false, true] {
        let wrong_native = if gtid {
            // A superset with an additional UUID is ordered by MysqlOrder but
            // deliberately rejected by the stronger verification domain guard.
            MysqlPosition::Gtid {
                set: format!("{UUID}:1-8,{OTHER}:1").parse().unwrap(),
                server_uuid: UUID.into(),
                file: "binlog.000001".into(),
                offset: 20,
            }
        } else {
            MysqlPosition::File {
                server_uuid: OTHER.into(),
                file_seq: 1,
                offset: 20,
            }
        };
        let wrong = Position::new(order(gtid).domain(), wrong_native.encode().unwrap());
        let mixed = Position::new(order(gtid).domain(), position(!gtid, 8).bytes().to_vec());
        let domain = Position::new("other-domain", position(gtid, 8).bytes().to_vec());
        for bad in [wrong, mixed, domain] {
            assert!(matches!(
                waiter(gtid, vec![Some(bad)])
                    .wait_until(position(gtid, 5))
                    .await,
                Err(VerifyError::Position(_))
            ));
        }
    }
}
#[tokio::test]
async fn applied_wait_rejects_malformed_positions_and_regression() {
    for gtid in [false, true] {
        let bad = Position::new(order(gtid).domain(), vec![1, 2, 3]);
        assert!(matches!(
            waiter(gtid, vec![Some(bad.clone())])
                .wait_until(position(gtid, 5))
                .await,
            Err(VerifyError::Position(_))
        ));
        assert!(waiter(gtid, vec![]).wait_until(bad).await.is_err());
        assert!(
            waiter(gtid, vec![Some(position(gtid, 3)), Some(position(gtid, 2))])
                .wait_until(position(gtid, 5))
                .await
                .is_err()
        );
    }
}

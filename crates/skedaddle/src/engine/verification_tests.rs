use super::*;
use crate::{
    event::{OpaqueValue, TxnId},
    mapping::PgType,
    pg::chunk::{Direction, KeyColumn, QualifiedName},
};

// Pure canonical-event contract fixtures. The real capture/decode/apply path
// is qualified separately by the config-gated tests/engine.rs test.
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        stream: "slot".into(),
    }
}
fn limits() -> VerificationLimits {
    VerificationLimits {
        max_keys: 8,
        max_bytes: 4096,
        timeout: Duration::from_secs(1),
    }
}
fn make_arm(limits: VerificationLimits) -> Armed {
    Armed {
        range: RangePlan {
            id: ChunkId {
                table: "public.rows".into(),
                plan: "a".repeat(64),
                ordinal: 0,
            },
            lo: None,
            hi: None,
        },
        table: Arc::new(TableKey {
            table: "public.rows".into(),
            relation: QualifiedName {
                schema: "public".into(),
                name: "rows".into(),
            },
            columns: vec![KeyColumn {
                name: "id".into(),
                pg_type: PgType::Int4,
                sql_type: QualifiedName {
                    schema: "pg_catalog".into(),
                    name: "int4".into(),
                },
                collation: None,
                direction: Direction::Asc,
            }],
        }),
        limits,
        keys: BTreeMap::new(),
        bytes: 0,
        failure: None,
        marker: verification_marker(Uuid::new_v4()),
        native: None,
        through: None,
    }
}
fn row(id: i32) -> Row {
    Row::from([(
        "id".into(),
        OpaqueValue {
            type_id: "qualified-int4".into(),
            bytes: Some(Value::Int4(id).canonical_bytes().unwrap()),
        },
    )])
}
fn event(kind: EventKind, old: i32, new: Option<i32>) -> Event {
    Event {
        source: source(),
        txn: TxnId(vec![1]),
        ordinal_in_txn: 0,
        position: Position::new("pg:1:1:slot", 10_u64.to_be_bytes()),
        schema_version: 0,
        kind,
        table: Some("public.rows".into()),
        key: row(old),
        before: Some(row(old)),
        after: new.map(row),
        unchanged_cols: Default::default(),
    }
}

#[test]
fn every_event_retains_insert_delete_both_key_identities_and_reverts() {
    let mut arm = make_arm(limits());
    for event in [
        event(EventKind::Insert, 1, Some(1)),
        event(EventKind::Update, 1, Some(2)),
        event(EventKind::Update, 2, Some(1)),
        event(EventKind::Delete, 3, None),
    ] {
        arm.observe_event(&event);
    }
    assert!(arm.failure.is_none());
    assert_eq!(arm.keys.len(), 3);
    for id in [1, 2, 3] {
        assert!(arm.keys.values().any(|key| key == &vec![Value::Int4(id)]));
    }
}

#[test]
fn key_and_encoded_byte_limits_fail_closed_with_units() {
    for (limits, expected) in [
        (
            VerificationLimits {
                max_keys: 1,
                ..limits()
            },
            EvidenceLimit::Keys(1),
        ),
        (
            VerificationLimits {
                max_bytes: 1,
                ..limits()
            },
            EvidenceLimit::EncodedBytes(1),
        ),
    ] {
        let mut arm = make_arm(limits);
        arm.observe_event(&event(EventKind::Insert, 1, Some(1)));
        arm.observe_event(&event(EventKind::Insert, 2, Some(2)));
        assert!(matches!(arm.failure, Some(ArmFailure::Overflow(limit)) if limit == expected));
        assert!(
            matches!(arm.failure.unwrap().error(&arm.range), VerifyError::EvidenceOverflow { range, limit } if range == arm.range.id && limit == expected)
        );
        assert!(arm.keys.len() <= 1);
    }
}

#[test]
fn malformed_key_and_truncate_cannot_produce_empty_success() {
    let mut missing = event(EventKind::Insert, 1, Some(1));
    missing.key.clear();
    let mut arm = make_arm(limits());
    arm.observe_event(&missing);
    assert!(matches!(
        arm.failure,
        Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap))
    ));
    let mut arm = make_arm(limits());
    arm.observe_event(&event(
        EventKind::Truncate {
            relations: vec!["public.rows".into()],
            cascade: false,
            restart_identity: false,
        },
        1,
        None,
    ));
    assert!(matches!(
        arm.failure,
        Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap))
    ));
}

#[test]
fn adapter_binding_rejects_lineage_slot_domain_and_restart() {
    let hub = Hub::default();
    hub.begin(
        source(),
        Arc::new(crate::pg::apply::NativePgOrder("pg:1:1:slot".into())),
    );
    let binding = hub.state.lock().unwrap().binding.clone().unwrap();
    for reason in [
        EvidenceInvalidation::LineageChanged,
        EvidenceInvalidation::SlotChanged,
        EvidenceInvalidation::Reconnected,
    ] {
        let mut actual = source();
        match reason {
            EvidenceInvalidation::LineageChanged => actual.lineage = "2".into(),
            EvidenceInvalidation::SlotChanged => actual.stream = "other".into(),
            _ => {}
        }
        hub.begin(
            actual,
            Arc::new(crate::pg::apply::NativePgOrder(binding.domain.clone())),
        );
        assert!(
            matches!(hub.state.lock().unwrap().check_binding(&binding), Err(VerifyError::EvidenceInvalidated { reason: r }) if r == reason)
        );
    }
    hub.begin(
        source(),
        Arc::new(crate::pg::apply::NativePgOrder("different-domain".into())),
    );
    assert!(matches!(
        hub.state.lock().unwrap().check_binding(&binding),
        Err(VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::LineageChanged
        })
    ));
}

#[test]
fn terminal_exit_invalidates_every_observer() {
    let hub = Arc::new(Hub::default());
    hub.begin(
        source(),
        Arc::new(crate::pg::apply::NativePgOrder("pg:1:1:slot".into())),
    );
    let token = Uuid::new_v4();
    hub.state
        .lock()
        .unwrap()
        .arms
        .insert(token, Box::new(make_arm(limits())));
    hub.finish(ApplierTerminalCause::CaptureStopped);
    let state = hub.state.lock().unwrap();
    assert!(!state.running);
    assert!(state.applied.is_none());
    assert_eq!(state.terminal, Some(ApplierTerminalCause::CaptureStopped));
    assert!(matches!(
        state.arms[&token].failure,
        Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap))
    ));
}

#[test]
fn cancellation_guard_removes_only_its_observer() {
    let hub = Arc::new(Hub::default());
    let token = Uuid::new_v4();
    let other = Uuid::new_v4();
    hub.state
        .lock()
        .unwrap()
        .arms
        .insert(token, Box::new(make_arm(limits())));
    hub.state
        .lock()
        .unwrap()
        .arms
        .insert(other, Box::new(make_arm(limits())));
    drop(DisarmOnExit {
        hub: hub.clone(),
        token,
    });
    let state = hub.state.lock().unwrap();
    assert!(!state.arms.contains_key(&token));
    assert!(state.arms.contains_key(&other));
}

#[test]
fn verification_state_is_heap_bounded() {
    assert_eq!(
        std::mem::size_of::<Arc<Hub>>(),
        std::mem::size_of::<usize>()
    );
    assert!(std::mem::size_of::<AppliedSourceWaiter>() <= 192);
    assert!(std::mem::size_of::<PgChangeEvidence<()>>() <= 256);
}

#[test]
fn timed_out_marker_wait_preserves_terminal_and_binding_failure_priority() {
    let hub = Hub::default();
    hub.begin(
        source(),
        Arc::new(crate::pg::apply::NativePgOrder("pg:1:1:slot".into())),
    );
    let mut state = hub.state.lock().unwrap();
    let binding = state.binding.clone().unwrap();
    let elapsed = Duration::from_millis(12);
    assert!(
        matches!(state.applier_timeout(&binding, pg_position(99), elapsed), VerifyError::ApplierTimeout { waited_position, elapsed: e } if e == elapsed && waited_position == pg_position(99))
    );
    state.invalidate(EvidenceInvalidation::LineageChanged);
    assert!(matches!(
        state.applier_timeout(&binding, pg_position(99), elapsed),
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::LineageChanged
        }
    ));
    state.terminal = Some(ApplierTerminalCause::ApplyStopped);
    assert!(matches!(
        state.applier_timeout(&binding, pg_position(99), elapsed),
        VerifyError::ApplierTerminal {
            cause: ApplierTerminalCause::ApplyStopped
        }
    ));
}

#[test]
fn timed_out_evidence_marker_preserves_gap_overflow_and_disarm() {
    let hub = Hub::default();
    hub.begin(
        source(),
        Arc::new(crate::pg::apply::NativePgOrder("pg:1:1:slot".into())),
    );
    let mut state = hub.state.lock().unwrap();
    let binding = state.binding.clone().unwrap();
    let token = Uuid::new_v4();
    let armed = make_arm(limits());
    let range = armed.range.clone();
    state.arms.insert(token, Box::new(armed));
    let elapsed = Duration::from_millis(12);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, pg_position(99), elapsed),
        VerifyError::ApplierTimeout { waited_position, .. } if waited_position == pg_position(99)
    ));
    state.terminal = Some(ApplierTerminalCause::CheckpointRejected);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, pg_position(99), elapsed),
        VerifyError::ApplierTerminal {
            cause: ApplierTerminalCause::CheckpointRejected
        }
    ));
    state.terminal = None;
    state.arms.get_mut(&token).unwrap().failure =
        Some(ArmFailure::Overflow(EvidenceLimit::Keys(8)));
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, pg_position(99), elapsed),
        VerifyError::EvidenceOverflow {
            limit: EvidenceLimit::Keys(8),
            ..
        }
    ));
    state.invalidate(EvidenceInvalidation::CaptureGap);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, pg_position(99), elapsed),
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::CaptureGap
        }
    ));
    state.invalid = None;
    state.arms.remove(&token);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, pg_position(99), elapsed),
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::CaptureGap
        }
    ));
}

fn pg_position(lsn: u64) -> Position {
    verify::sql::PgSourcePosition::new("pg:1:1:slot").position(lsn)
}

#[test]
fn applied_observations_use_native_order_and_reject_invalid_checkpoints() {
    struct DecimalOrder;
    impl crate::event::PositionOrder for DecimalOrder {
        fn domain(&self) -> &str {
            "native-decimal"
        }
        fn compare(
            &self,
            left: &[u8],
            right: &[u8],
        ) -> std::result::Result<Ordering, crate::event::EventError> {
            let parse = |bytes: &[u8]| {
                std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or(crate::event::EventError::InvalidEncoding("native decimal"))
            };
            Ok(parse(left)?.cmp(&parse(right)?))
        }
    }
    let hub = Hub::default();
    let native = |value: &str| Position::new("native-decimal", value.as_bytes());
    let apply = |position: &Position| {
        hub.applied(
            position,
            &ReaderCheckpoint {
                next_mseq: 1,
                last_position: Some(position.clone()),
                last_record: Some(crate::buffer::format::RecordBoundary {
                    source: source(),
                    chunk_rows: false,
                }),
                ..ReaderCheckpoint::default()
            },
        );
    };
    for bad in [
        native("9"),
        native("incomparable"),
        Position::new("wrong-domain", b"11"),
    ] {
        hub.begin(source(), Arc::new(DecimalOrder));
        apply(&native("9"));
        apply(&native("10"));
        {
            let state = hub.state.lock().unwrap();
            assert_eq!(state.applied, Some(native("10")));
            assert!(state.terminal.is_none());
            assert!(state.invalid.is_none());
        }
        apply(&bad);
        let state = hub.state.lock().unwrap();
        assert!(
            state.invalid.is_some()
                || state.terminal == Some(ApplierTerminalCause::CheckpointRejected)
        );
    }
}

// A connector contract fixture: no database behavior is simulated.
struct NativeOrder;
impl crate::event::PositionOrder for NativeOrder {
    fn domain(&self) -> &str {
        "native-test"
    }
    fn compare(
        &self,
        a: &[u8],
        b: &[u8],
    ) -> std::result::Result<Ordering, crate::event::EventError> {
        let parse = |v: &[u8]| {
            std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(crate::event::EventError::InvalidEncoding("native-test"))
        };
        Ok(parse(a)?.cmp(&parse(b)?))
    }
}
fn native_position(n: u64) -> Position {
    Position::new("native-test", n.to_string().as_bytes())
}
fn native_source() -> SourceIdentity {
    SourceIdentity {
        engine: "native-test".into(),
        ..source()
    }
}
struct NativeKey;
impl VerificationKey for NativeKey {
    fn table(&self) -> &str {
        "public.rows"
    }
    fn columns(&self) -> Vec<&str> {
        vec!["id"]
    }
    fn validate(&self) -> verify::Result<()> {
        Ok(())
    }
    fn validate_key(&self, key: &[Value]) -> verify::Result<()> {
        if matches!(key, [Value::Int4(_)]) {
            Ok(())
        } else {
            Err(VerifyError::Invalid("native key"))
        }
    }
}
struct NativeKeys;
impl KeyOrder for NativeKeys {
    fn compare_keys(&self, a: &Key, b: &Key) -> verify::Result<Ordering> {
        match (a.as_slice(), b.as_slice()) {
            ([Value::Int4(a)], [Value::Int4(b)]) => Ok(a.cmp(b)),
            _ => Err(VerifyError::Invalid("native order")),
        }
    }
}
fn native_batch(marker: NativeVerificationMarker) -> Captured {
    let mut events = Vec::new();
    for (i, mut e) in [
        event(EventKind::Insert, 1, Some(1)),
        event(EventKind::Update, 1, Some(2)),
        event(EventKind::Update, 2, Some(1)),
    ]
    .into_iter()
    .enumerate()
    {
        e.source = native_source();
        e.position = native_position(9);
        e.ordinal_in_txn = i as u64;
        events.push(e);
    }
    let txn = crate::event::AssembledTxn::new(
        native_source(),
        TxnId(vec![1]),
        native_position(10),
        None,
        events,
    )
    .unwrap();
    Captured {
        watermarks: vec![super::super::CapturedMarker::Verification {
            marker,
            transaction: txn.txn.clone(),
            commit: txn.commit_position.clone(),
            committed_events: txn.events.clone(),
        }],
        transaction: Some(txn),
        ..Captured::default()
    }
}
fn apply_native(hub: &Hub, position: Position) {
    hub.applied(
        &position,
        &ReaderCheckpoint {
            next_mseq: 1,
            last_position: Some(position.clone()),
            last_record: Some(crate::buffer::format::RecordBoundary {
                source: native_source(),
                chunk_rows: false,
            }),
            ..ReaderCheckpoint::default()
        },
    );
}
struct ContractEmitter {
    hub: Arc<Hub>,
    apply: bool,
}
#[async_trait]
impl VerificationEmitter for ContractEmitter {
    async fn emit(&self, marker: &NativeVerificationMarker) -> verify::Result<()> {
        self.hub.observe(&native_batch(marker.clone()));
        if self.apply {
            apply_native(&self.hub, native_position(10));
        }
        Ok(())
    }
}
fn native_provider(apply: bool) -> NativeChangeEvidence<NativeKeys> {
    let hub = Arc::new(Hub::default());
    hub.begin(native_source(), Arc::new(NativeOrder));
    let binding = hub.state.lock().unwrap().binding.clone().unwrap();
    PgChangeEvidence {
        emitter: Some(Arc::new(ContractEmitter {
            hub: hub.clone(),
            apply,
        })),
        hub,
        binding,
        admin: None,
        table: Arc::new(NativeKey),
        order: NativeKeys,
        limits: VerificationLimits {
            timeout: Duration::from_millis(30),
            ..limits()
        },
        armed: None,
    }
}
#[tokio::test]
async fn native_evidence_authenticates_and_waits_for_applied_boundary() {
    for applied in [true, false] {
        let mut provider = native_provider(applied);
        let range = make_arm(limits()).range;
        provider.arm(&range).await.unwrap();
        let result = provider.changes(&range, native_position(9)).await;
        if applied {
            let evidence = result.unwrap();
            assert_eq!(evidence.through_position, native_position(10));
            assert_eq!(
                evidence.keys,
                vec![vec![Value::Int4(1)], vec![Value::Int4(2)]]
            );
        } else {
            assert!(matches!(result, Err(VerifyError::ApplierTimeout { .. })));
        }
        assert!(provider.hub.state.lock().unwrap().arms.is_empty());
    }
}
#[tokio::test]
async fn native_closing_marker_rejects_mismatched_batch_and_version() {
    for fault in 0..7 {
        let mut provider = native_provider(false);
        let range = make_arm(limits()).range;
        provider.arm(&range).await.unwrap();
        let token = provider.armed.unwrap();
        let marker = provider.hub.state.lock().unwrap().arms[&token]
            .native
            .clone()
            .unwrap();
        let mut batch = native_batch(marker);
        let super::super::CapturedMarker::Verification {
            marker,
            commit,
            transaction,
            committed_events,
        } = &mut batch.watermarks[0]
        else {
            unreachable!()
        };
        match fault {
            0 => *commit = native_position(11),
            1 => *transaction = TxnId(vec![2]),
            2 => committed_events.clear(),
            3 => batch.transaction.as_mut().unwrap().source.stream = "other".into(),
            4 => *commit = Position::new("wrong-domain", b"10"),
            5 => {
                marker.version = 2;
                provider
                    .hub
                    .state
                    .lock()
                    .unwrap()
                    .arms
                    .get_mut(&token)
                    .unwrap()
                    .native
                    .as_mut()
                    .unwrap()
                    .version = 2;
            }
            _ => batch.transaction = None,
        }
        provider.hub.observe(&batch);
        let state = provider.hub.state.lock().unwrap();
        assert!(state.arms[&token].through.is_none());
        assert!(state.invalid.is_some() || state.arms[&token].failure.is_some());
    }
}
#[tokio::test]
async fn native_waiter_uses_numeric_order_and_fails_closed() {
    use verify::Applier;
    for fault in 0..5 {
        let provider = native_provider(false);
        let mut waiter = AppliedSourceWaiter {
            hub: provider.hub.clone(),
            binding: provider.binding.clone(),
            timeout: provider.limits.timeout,
            barrier: None,
        };
        apply_native(&provider.hub, native_position(10));
        let target = match fault {
            0 => native_position(9),
            1 => Position::new("wrong-domain", b"9"),
            2 => {
                apply_native(&provider.hub, native_position(8));
                native_position(9)
            }
            3 => native_position(11),
            _ => {
                provider.hub.finish(ApplierTerminalCause::CaptureStopped);
                native_position(9)
            }
        };
        let result = waiter.wait_until(target).await;
        match fault {
            0 => assert_eq!(result.unwrap(), native_position(10)),
            2 | 4 => assert!(matches!(result, Err(VerifyError::ApplierTerminal { .. }))),
            3 => assert!(matches!(result, Err(VerifyError::ApplierTimeout { .. }))),
            _ => assert!(result.is_err()),
        }
    }
}
#[tokio::test]
async fn native_continuity_invalidation_and_cancel_remove_trust() {
    let mut provider = native_provider(false);
    let range = make_arm(limits()).range;
    provider.arm(&range).await.unwrap();
    let token = provider.armed.unwrap();
    provider.hub.invalidate_continuity();
    provider
        .hub
        .observe(&native_batch(NativeVerificationMarker {
            version: 1,
            token,
        }));
    assert!(matches!(
        provider.changes(&range, native_position(9)).await,
        Err(VerifyError::EvidenceInvalidated { .. })
    ));
    assert!(provider.arm(&range).await.is_err());
    assert!(provider.hub.state.lock().unwrap().arms.is_empty());

    let mut provider = native_provider(false);
    provider.arm(&range).await.unwrap();
    let hub = provider.hub.clone();
    let mut pending = Box::pin(provider.changes(&range, native_position(9)));
    assert!(
        tokio::time::timeout(Duration::from_millis(2), &mut pending)
            .await
            .is_err()
    );
    drop(pending);
    assert!(hub.state.lock().unwrap().arms.is_empty());
}

#[test]
fn native_verification_marker_cannot_close_a_chunk_window() {
    use super::super::{Active, CapturedMarker};
    let table = TableKey {
        table: "public.rows".into(),
        relation: QualifiedName {
            schema: "public".into(),
            name: "rows".into(),
        },
        columns: vec![KeyColumn {
            name: "id".into(),
            pg_type: PgType::Int4,
            sql_type: QualifiedName {
                schema: "pg_catalog".into(),
                name: "int4".into(),
            },
            collation: None,
            direction: Direction::Asc,
        }],
    };
    let plan = crate::coordinator::chunk::ChunkPlan::new(table.clone(), 100, vec![]).unwrap();
    let chunk = plan.ranges[0].clone();
    let window = crate::pg::chunk::Window::new(table, chunk.clone(), vec![], 1).unwrap();
    let (complete, _) = tokio::sync::oneshot::channel();
    let (_, journaled) = tokio::sync::oneshot::channel();
    let mut active = Active {
        plan: Box::new(plan),
        chunk,
        attempt: 1,
        window: Some(window),
        backlog: Default::default(),
        bytes: 0,
        complete,
        journaled,
    };
    let captured = Captured {
        watermarks: vec![CapturedMarker::Verification {
            marker: NativeVerificationMarker {
                version: 1,
                token: Uuid::new_v4(),
            },
            commit: native_position(10),
            transaction: TxnId(vec![1]),
            committed_events: vec![],
        }],
        ..Captured::default()
    };
    assert!(active.feed(&captured, &NativeOrder).unwrap().is_none());
}

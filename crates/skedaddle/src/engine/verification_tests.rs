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
    hub.begin(source(), "pg:1:1:slot".into());
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
        hub.begin(actual, binding.domain.clone());
        assert!(
            matches!(hub.state.lock().unwrap().check_binding(&binding), Err(VerifyError::EvidenceInvalidated { reason: r }) if r == reason)
        );
    }
    hub.begin(source(), "different-domain".into());
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
    hub.begin(source(), "pg:1:1:slot".into());
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
    hub.begin(source(), "pg:1:1:slot".into());
    let mut state = hub.state.lock().unwrap();
    let binding = state.binding.clone().unwrap();
    let elapsed = Duration::from_millis(12);
    assert!(
        matches!(state.applier_timeout(&binding, 99, elapsed), VerifyError::ApplierTimeout { waited_lsn: 99, elapsed: e } if e == elapsed)
    );
    state.invalidate(EvidenceInvalidation::LineageChanged);
    assert!(matches!(
        state.applier_timeout(&binding, 99, elapsed),
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::LineageChanged
        }
    ));
    state.terminal = Some(ApplierTerminalCause::ApplyStopped);
    assert!(matches!(
        state.applier_timeout(&binding, 99, elapsed),
        VerifyError::ApplierTerminal {
            cause: ApplierTerminalCause::ApplyStopped
        }
    ));
}

#[test]
fn timed_out_evidence_marker_preserves_gap_overflow_and_disarm() {
    let hub = Hub::default();
    hub.begin(source(), "pg:1:1:slot".into());
    let mut state = hub.state.lock().unwrap();
    let binding = state.binding.clone().unwrap();
    let token = Uuid::new_v4();
    let armed = make_arm(limits());
    let range = armed.range.clone();
    state.arms.insert(token, Box::new(armed));
    let elapsed = Duration::from_millis(12);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, 99, elapsed),
        VerifyError::ApplierTimeout { waited_lsn: 99, .. }
    ));
    state.terminal = Some(ApplierTerminalCause::CheckpointRejected);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, 99, elapsed),
        VerifyError::ApplierTerminal {
            cause: ApplierTerminalCause::CheckpointRejected
        }
    ));
    state.terminal = None;
    state.arms.get_mut(&token).unwrap().failure =
        Some(ArmFailure::Overflow(EvidenceLimit::Keys(8)));
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, 99, elapsed),
        VerifyError::EvidenceOverflow {
            limit: EvidenceLimit::Keys(8),
            ..
        }
    ));
    state.invalidate(EvidenceInvalidation::CaptureGap);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, 99, elapsed),
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::CaptureGap
        }
    ));
    state.invalid = None;
    state.arms.remove(&token);
    assert!(matches!(
        state.evidence_timeout(&binding, token, &range, 99, elapsed),
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::CaptureGap
        }
    ));
}

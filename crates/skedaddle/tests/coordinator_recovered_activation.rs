//! Pure coordinator wire-contract reproducer, not a database substitute.
use skedaddle::{
    coordinator::{
        self as c, Event, Materialized, State, Transition,
        journal::Entry,
        reverse::{
            ActivationReceipt, DestinationIdentity, FenceBinding, ReverseConsistentPoint,
            SetupRecord,
        },
    },
    pg::{activate, fence, reverse as pg},
};
use std::collections::BTreeSet;
fn append(state: &mut Materialized, owner: &str, transition: Transition) {
    let entry = Entry::new(state, transition, owner).unwrap();
    state.apply(&entry).unwrap();
}
fn ready() -> c::ReadinessFacts {
    c::ReadinessFacts {
        all_chunks_done: true,
        capture_healthy: true,
        lineage_matches: true,
        apply_lag: 0,
        lag_budget: 5,
        blocked_ddl: false,
        unknown_schema_job: false,
        verified_rows: 1,
        in_scope_rows: 1,
        busy_rows_past_budget: 0,
        dead_letters: 0,
        indexes_constraints_valid: true,
        sequences_syncable: true,
        reverse_prerequisites: true,
        fence_risks_acknowledged: true,
        buffer_contract_matches: true,
    }
}
fn proof_before_activation() -> Materialized {
    let mut state = Materialized::default();
    append(
        &mut state,
        "owner-1",
        Transition::AcquireOwnership { epoch: 1 },
    );
    for to in [State::Assessed, State::Copying, State::CatchingUp] {
        append(
            &mut state,
            "owner-1",
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        );
    }
    append(
        &mut state,
        "owner-1",
        Transition::Lifecycle {
            event: Event::Readiness { facts: ready() },
        },
    );
    for to in [State::SourceFenced, State::FinalBoundaryApplied] {
        append(
            &mut state,
            "owner-1",
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        );
    }
    let table = fence::Table {
        schema: "public".into(),
        name: "items".into(),
        owner: "admin".into(),
    };
    let catalog = fence::Catalog {
        roles: vec![fence::Role {
            name: "app".into(),
            login: false,
            superuser: false,
            bypass_rls: false,
        }],
        tables: vec![table.clone()],
        ..Default::default()
    };
    let mut destination = activate::DestinationFence::new(activate::Config {
        epoch: 1,
        applications: BTreeSet::from(["app".into()]),
        tables: vec![table],
        timeout_ms: 1000,
        post_activate: None,
    })
    .unwrap();
    destination.next = activate::Step::Fenced;
    destination.saved = Some(fence::SavedState {
        catalog: catalog.clone(),
        authority: None,
    });
    destination.evidence.push(activate::FenceEvidence {
        ordinal: 0,
        observed_at: "contract-observation".into(),
        catalog,
    });
    let binding = FenceBinding {
        migration: "migration".into(),
        epoch: 1,
        owner: "owner-1".into(),
        destination: DestinationIdentity {
            system_identifier: "123".into(),
            database: "destination".into(),
        },
        fence: Box::new(destination),
    };
    let slot = pg::SlotEvidence {
        slot: "skedaddle_contract_rev".into(),
        consistent_point: 100,
    };
    // Same serialized public Ready contract used by tests/coordinator_reverse.rs.
    let readiness: pg::Readiness =
        serde_json::from_value(serde_json::json!({"installation":"contract","slot":slot})).unwrap();
    for intent in [
        pg::Intent::Publication,
        pg::Intent::SlotReady(slot.clone()),
        pg::Intent::Ready(readiness.clone()),
    ] {
        append(
            &mut state,
            "owner-1",
            Transition::ReverseSetup {
                record: Box::new(SetupRecord {
                    binding: binding.clone(),
                    endpoint: pg::Endpoint::Destination,
                    intent,
                }),
            },
        );
    }
    append(
        &mut state,
        "owner-1",
        Transition::ReverseConsistentPoint {
            proof: Box::new(ReverseConsistentPoint {
                binding,
                slot,
                readiness,
                activation_lsn: 101,
            }),
        },
    );
    state
}
fn receipt() -> ActivationReceipt {
    ActivationReceipt {
        migration: "migration".into(),
        cutover: "cutover-1".into(),
        fence_epoch: 1,
        fence_owner: "owner-1".into(),
        destination: DestinationIdentity {
            system_identifier: "123".into(),
            database: "destination".into(),
        },
        activated_at_ms: 42,
    }
}
fn terminal() -> Transition {
    Transition::Lifecycle {
        event: Event::Advance {
            to: State::DestinationActive,
        },
    }
}
fn promoted() -> Materialized {
    let mut state = proof_before_activation();
    append(
        &mut state,
        "owner-2",
        Transition::AcquireOwnership { epoch: 2 },
    );
    state
}
fn try_apply(state: &mut Materialized, owner: &str, transition: Transition) -> c::Result<()> {
    let entry = Entry::new(state, transition, owner).unwrap();
    state.apply(&entry)
}
fn mismatch(result: c::Result<()>) -> bool {
    matches!(
        result,
        Err(c::Error::ReverseProof(c::reverse::ProofError::Mismatch))
    )
}

#[test]
fn crash_after_activation_receipt_new_owner_publishes_destination_active() {
    let mut state = promoted();
    append(
        &mut state,
        "owner-2",
        Transition::RecoveredActivation {
            receipt: Box::new(receipt()),
        },
    );
    append(&mut state, "owner-2", terminal());
    assert_eq!(state.lifecycle.state, State::DestinationActive);
    assert_eq!(state.epoch, 2);
    assert_eq!(state.owner, "owner-2");
}

#[test]
fn original_owner_still_publishes_with_its_own_proof() {
    let mut state = proof_before_activation();
    append(&mut state, "owner-1", terminal());
    assert_eq!(state.lifecycle.state, State::DestinationActive);
}

#[test]
fn promoted_owner_without_receipt_is_refused() {
    let mut state = promoted();
    assert!(mismatch(try_apply(&mut state, "owner-2", terminal())));
    assert_eq!(state.lifecycle.state, State::FinalBoundaryApplied);
}

#[test]
fn mismatched_receipts_are_refused() {
    let variants: Vec<fn(&mut ActivationReceipt)> = vec![
        |r| r.migration = "other".into(),
        |r| r.cutover = String::new(),
        |r| r.fence_epoch = 2,
        |r| r.fence_owner = "owner-2".into(),
        |r| r.destination.system_identifier = "999".into(),
        |r| r.destination.database = "other".into(),
        |r| r.activated_at_ms = 0,
    ];
    for mutate in variants {
        let mut state = promoted();
        let mut bad = receipt();
        mutate(&mut bad);
        assert!(mismatch(try_apply(
            &mut state,
            "owner-2",
            Transition::RecoveredActivation {
                receipt: Box::new(bad)
            }
        )));
        assert!(mismatch(try_apply(&mut state, "owner-2", terminal())));
        assert_eq!(state.lifecycle.state, State::FinalBoundaryApplied);
    }
}

#[test]
fn receipt_from_a_non_owner_or_same_epoch_is_refused() {
    // The original owner cannot use the recovered path (its own proof suffices).
    let mut same = proof_before_activation();
    assert!(mismatch(try_apply(
        &mut same,
        "owner-1",
        Transition::RecoveredActivation {
            receipt: Box::new(receipt())
        }
    )));
    // A non-owner entry is still rejected by the generic owner check.
    let mut state = promoted();
    assert!(
        try_apply(
            &mut state,
            "owner-1",
            Transition::RecoveredActivation {
                receipt: Box::new(receipt())
            }
        )
        .is_err()
    );
    assert!(try_apply(&mut state, "owner-1", terminal()).is_err());
    assert_eq!(state.lifecycle.state, State::FinalBoundaryApplied);
}

#[test]
fn receipt_without_retained_proof_is_refused() {
    let mut state = Materialized::default();
    append(
        &mut state,
        "owner-1",
        Transition::AcquireOwnership { epoch: 1 },
    );
    for to in [State::Assessed, State::Copying, State::CatchingUp] {
        append(
            &mut state,
            "owner-1",
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        );
    }
    append(
        &mut state,
        "owner-1",
        Transition::Lifecycle {
            event: Event::Readiness { facts: ready() },
        },
    );
    for to in [State::SourceFenced, State::FinalBoundaryApplied] {
        append(
            &mut state,
            "owner-1",
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        );
    }
    append(
        &mut state,
        "owner-2",
        Transition::AcquireOwnership { epoch: 2 },
    );
    assert!(
        try_apply(
            &mut state,
            "owner-2",
            Transition::RecoveredActivation {
                receipt: Box::new(receipt())
            }
        )
        .is_err()
    );
    assert!(try_apply(&mut state, "owner-2", terminal()).is_err());
    assert_eq!(state.lifecycle.state, State::FinalBoundaryApplied);
}

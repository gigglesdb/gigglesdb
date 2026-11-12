//! Wire-contract tests use real filesystem journals and local coordinator stores.
//! PostgreSQL setup/Restore qualification lives in pg_reverse/cutover.
mod support;

use object_store::ObjectStoreExt;
use skedaddle::{
    coordinator::{
        self, Error, Event, Materialized, State, Transition,
        journal::{Entry, Journal},
        reverse::{
            DestinationIdentity, FenceBinding, ProofError, ReverseConsistentPoint, SetupRecord,
        },
    },
    pg::{activate, fence, reverse as pg},
};
use std::collections::BTreeSet;
use support::*;

async fn append(journal: &Journal, state: &mut Materialized, transition: Transition) {
    let entry = Entry::new(state, transition, "owner").unwrap();
    journal.put(&entry).await.unwrap();
    state.apply(&entry).unwrap();
}
async fn fenced() -> (Journal, Materialized, FenceBinding) {
    fenced_on(fresh_journal("reverse-contract")).await
}
async fn fenced_on(journal: Journal) -> (Journal, Materialized, FenceBinding) {
    let mut state = Materialized::default();
    append(
        &journal,
        &mut state,
        Transition::AcquireOwnership { epoch: 1 },
    )
    .await;
    for to in [State::Assessed, State::Copying, State::CatchingUp] {
        append(
            &journal,
            &mut state,
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        )
        .await;
    }
    append(
        &journal,
        &mut state,
        Transition::Lifecycle {
            event: Event::Readiness { facts: ready() },
        },
    )
    .await;
    for to in [State::SourceFenced, State::FinalBoundaryApplied] {
        append(
            &journal,
            &mut state,
            Transition::Lifecycle {
                event: Event::Advance { to },
            },
        )
        .await;
    }
    let table = fence::Table {
        schema: "public".into(),
        name: "rows".into(),
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
        observed_at: "retained-observation".into(),
        catalog,
    });
    let binding = FenceBinding {
        migration: "migration".into(),
        epoch: 1,
        owner: "owner".into(),
        destination: DestinationIdentity {
            system_identifier: "123".into(),
            database: "destination".into(),
        },
        fence: Box::new(destination),
    };
    (journal, state, binding)
}
fn readiness(slot: &pg::SlotEvidence) -> pg::Readiness {
    // Exercise the actual serialized P11a Ready wire shape, including its private
    // fields. This is a contract fixture, not a database transport substitute.
    serde_json::from_value(serde_json::json!({"installation":"contract", "slot":slot})).unwrap()
}
async fn setup_records(
    journal: &Journal,
    state: &mut Materialized,
    binding: &FenceBinding,
) -> ReverseConsistentPoint {
    let slot = pg::SlotEvidence {
        slot: "skedaddle_contract_rev".into(),
        consistent_point: 100,
    };
    let readiness = readiness(&slot);
    for intent in [
        pg::Intent::Publication,
        pg::Intent::SlotReady(slot.clone()),
        pg::Intent::Ready(readiness.clone()),
    ] {
        append(
            journal,
            state,
            Transition::ReverseSetup {
                record: Box::new(SetupRecord {
                    binding: binding.clone(),
                    endpoint: pg::Endpoint::Destination,
                    intent,
                }),
            },
        )
        .await;
    }
    ReverseConsistentPoint {
        binding: binding.clone(),
        slot,
        readiness,
        activation_lsn: 101,
    }
}

#[tokio::test]
async fn missing_proof_refuses_destination_authority_with_exact_next_step() {
    let (journal, state, _) = fenced().await;
    let local = coordinator::local::Local::recover(scratch("reverse-missing").join("db"), &journal)
        .await
        .unwrap();
    let mut owner =
        coordinator::epoch::Coordinator::observer(journal.clone(), local, "next").unwrap();
    owner.acquire().await.unwrap();
    let seq = owner.state().applied_seq;
    let error = owner
        .commit(Transition::Lifecycle {
            event: Event::Advance {
                to: State::DestinationActive,
            },
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ReverseProof(ProofError::Missing)));
    assert!(error.to_string().ends_with(coordinator::reverse::NEXT_STEP));
    assert_eq!(owner.state().applied_seq, seq);
    assert_eq!(
        owner.state().lifecycle.authority,
        coordinator::Authority::Neither
    );
    assert_eq!(state.lifecycle.state, State::FinalBoundaryApplied);
    assert!(matches!(
        journal.authenticated_reverse().await,
        Err(Error::ReverseProof(ProofError::Missing))
    ));
}

#[tokio::test]
async fn journaled_matching_proof_activates_and_survives_restart_and_checkpoint() {
    let (journal, mut state, binding) = fenced().await;
    let proof = setup_records(&journal, &mut state, &binding).await;
    // Ready alone does not grant activation authority.
    assert!(matches!(
        journal.authenticated_reverse().await,
        Err(Error::ReverseProof(ProofError::Missing))
    ));
    append(
        &journal,
        &mut state,
        Transition::ReverseConsistentPoint {
            proof: Box::new(proof),
        },
    )
    .await;
    journal.checkpoint(&state).await.unwrap();
    let token = journal.authenticated_reverse().await.unwrap();
    let mut current = (*binding.fence).clone();
    current.next = activate::Step::Restore;
    let key = skedaddle::cutover::Key {
        migration: "migration".into(),
        epoch: 1,
        coordinator: "owner".into(),
        cutover: "cutover".into(),
    };
    token
        .check(&key, &current, &binding.destination, 102)
        .unwrap();
    let reopened =
        coordinator::local::Local::recover(scratch("reverse-restart").join("db"), &journal)
            .await
            .unwrap();
    assert_eq!(reopened.state(), &state);
    assert_eq!(journal.load(true).await.unwrap(), state);
    append(
        &journal,
        &mut state,
        Transition::Lifecycle {
            event: Event::Advance {
                to: State::DestinationActive,
            },
        },
    )
    .await;
    assert_eq!(
        journal.load(false).await.unwrap().lifecycle.authority,
        coordinator::Authority::Destination
    );
}

#[tokio::test]
async fn epoch_namespace_owner_fence_and_nonpredating_proofs_are_refused() {
    let (journal, mut state, binding) = fenced().await;
    let proof = setup_records(&journal, &mut state, &binding).await;
    for case in 0..7 {
        let mut bad = proof.clone();
        match case {
            0 => bad.binding.epoch += 1,
            1 => bad.binding.migration = "different".into(),
            2 => bad.binding.owner = "different".into(),
            3 => bad.binding.fence.evidence[0].observed_at = "different".into(),
            4 => bad.binding.destination.database = "different".into(),
            5 => bad.activation_lsn = bad.slot.consistent_point,
            _ => bad.activation_lsn = bad.slot.consistent_point - 1,
        }
        let entry = Entry::new(
            &state,
            Transition::ReverseConsistentPoint {
                proof: Box::new(bad),
            },
            "owner",
        )
        .unwrap();
        assert!(
            matches!(journal.put(&entry).await, Err(Error::ReverseProof(_))),
            "case {case}"
        );
    }
    append(
        &journal,
        &mut state,
        Transition::ReverseConsistentPoint {
            proof: Box::new(proof),
        },
    )
    .await;
    let token = journal.authenticated_reverse().await.unwrap();
    let key = skedaddle::cutover::Key {
        migration: "migration".into(),
        epoch: 1,
        coordinator: "owner".into(),
        cutover: "cutover".into(),
    };
    let mut current = (*binding.fence).clone();
    current.next = activate::Step::Restore;
    for case in 0..6 {
        let mut key = key.clone();
        let mut current = current.clone();
        let mut identity = binding.destination.clone();
        let mut barrier = 101;
        match case {
            0 => key.migration = "other".into(),
            1 => key.epoch += 1,
            2 => key.coordinator = "other".into(),
            3 => current.evidence.clear(),
            4 => identity.system_identifier = "other".into(),
            _ => barrier = 100,
        }
        assert!(
            token.check(&key, &current, &identity, barrier).is_err(),
            "case {case}"
        );
    }
    append(
        &journal,
        &mut state,
        Transition::AcquireOwnership { epoch: 2 },
    )
    .await;
    assert!(matches!(
        journal.authenticated_reverse().await,
        Err(Error::ReverseProof(ProofError::Mismatch))
    ));
    let entry = Entry::new(
        &state,
        Transition::Lifecycle {
            event: Event::Advance {
                to: State::DestinationActive,
            },
        },
        "owner",
    )
    .unwrap();
    assert!(matches!(
        journal.put(&entry).await,
        Err(Error::ReverseProof(ProofError::Mismatch))
    ));
}

#[tokio::test]
async fn nonjournaled_metadata_cannot_publish_a_checkpoint_or_mint_a_token() {
    let (journal, mut state, binding) = fenced().await;
    let proof = setup_records(&journal, &mut state, &binding).await;
    let reverse = state.verification.reverse.as_mut().unwrap();
    reverse.proof = Some(Box::new(proof));
    reverse.proof_seq = state.applied_seq;
    assert!(journal.checkpoint(&state).await.is_err());
    assert!(matches!(
        journal.authenticated_reverse().await,
        Err(Error::ReverseProof(ProofError::Missing))
    ));
}

#[tokio::test]
async fn raw_replay_rejects_invalid_proof_even_when_its_entry_hash_is_valid() {
    use object_store::PutMode;
    for case in 0..3 {
        let store = fs_store(scratch("reverse-invalid-wire"));
        let (journal, mut state, binding) =
            fenced_on(support::journal(store.clone(), "migration")).await;
        let mut proof = setup_records(&journal, &mut state, &binding).await;
        match case {
            0 => proof.binding.epoch += 1,
            1 => proof.binding.fence.evidence[0].observed_at = "other-fence".into(),
            _ => proof.activation_lsn = proof.slot.consistent_point,
        }
        let entry = Entry::new(
            &state,
            Transition::ReverseConsistentPoint {
                proof: Box::new(proof),
            },
            "owner",
        )
        .unwrap();
        store
            .put_opts(
                &journal.entry_key(entry.seq),
                serde_json::to_vec(&entry).unwrap().into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert!(
            matches!(journal.load(false).await, Err(Error::RecoveryRequired(_))),
            "case {case}"
        );
        assert!(journal.authenticated_reverse().await.is_err());
    }
}

#[tokio::test]
async fn checkpoint_cannot_replace_missing_raw_setup_proof() {
    let store = fs_store(scratch("reverse-pruned-proof"));
    let (journal, mut state, binding) =
        fenced_on(support::journal(store.clone(), "migration")).await;
    let proof = setup_records(&journal, &mut state, &binding).await;
    append(
        &journal,
        &mut state,
        Transition::ReverseConsistentPoint {
            proof: Box::new(proof),
        },
    )
    .await;
    journal.checkpoint(&state).await.unwrap();
    store
        .delete(&journal.entry_key(state.applied_seq - 1))
        .await
        .unwrap();
    assert!(journal.load(true).await.is_err());
    assert!(journal.authenticated_reverse().await.is_err());
    assert!(
        coordinator::local::Local::recover(scratch("reverse-pruned-recover").join("db"), &journal)
            .await
            .is_err()
    );
}

//! Store-backed configuration entries update routing descriptors.
use giggles_core::{
    range_api::{Epoch, RangeDescriptor, RangeId, RangeSpace, ReplicaDesc, ReplicaRole, range_key},
    replicated::RangeStore,
};
use giggles_replication::{
    range_state::RangeStateMachine,
    state::{AppliedPosition, ApplyOutcome, StateMachine},
};
use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};
use std::sync::Arc;

fn change(steps: &[(u64, ConfChangeType)]) -> ConfChangeV2 {
    let mut cc = ConfChangeV2::default();
    for &(node_id, ty) in steps {
        let mut single = ConfChangeSingle {
            node_id,
            ..Default::default()
        };
        single.set_change_type(ty);
        cc.mut_changes().push(single);
    }
    cc
}

#[test]
fn learner_joint_leave_and_refused_changes_follow_the_descriptor() {
    let dir = super::common::range_net::temp_dir();
    let store = Arc::new(RangeStore::create(&dir, [91; 16]).unwrap());
    let range = RangeId(2);
    let descriptor = RangeDescriptor {
        id: range,
        epoch: Epoch(1),
        start: vec![],
        end: None,
        replicas: (1..=3)
            .map(|node| ReplicaDesc {
                node,
                role: ReplicaRole::Voter,
            })
            .collect(),
    };
    store.create_range(&descriptor).unwrap();
    let key = range_key(RangeSpace::Rows, &100u64.to_be_bytes());
    store
        .split(
            range,
            Epoch(1),
            &key,
            RangeId(8),
            giggles_core::replicated::AppliedPosition { index: 1, term: 1 },
        )
        .unwrap();
    let state = RangeStateMachine::new(range, store.clone());
    let pos = |index| AppliedPosition { index, term: 1 };
    let learner = change(&[(4, ConfChangeType::AddLearnerNode)]);
    assert_eq!(
        state.apply_conf_change(Some(&learner), pos(2)).unwrap(),
        ApplyOutcome::Applied
    );
    let added = store.descriptor(range).unwrap().unwrap();
    assert_eq!(added.epoch, Epoch(3));
    assert_eq!(
        added.replicas.last().unwrap(),
        &ReplicaDesc {
            node: 4,
            role: ReplicaRole::Learner
        }
    );
    assert_eq!(store.children(range).unwrap().len(), 1);
    let mut joint = change(&[
        (4, ConfChangeType::AddNode),
        (3, ConfChangeType::RemoveNode),
    ]);
    joint.set_transition(ConfChangeTransition::Explicit);
    state.apply_conf_change(Some(&joint), pos(3)).unwrap();
    let promoted = store.descriptor(range).unwrap().unwrap();
    assert_eq!(promoted.epoch, Epoch(4));
    assert_eq!(
        promoted.replicas.iter().map(|r| r.node).collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
    assert!(
        promoted
            .replicas
            .iter()
            .all(|r| r.role == ReplicaRole::Voter)
    );
    assert!(store.children(range).unwrap().is_empty());
    assert_eq!(
        state.apply_conf_change(Some(&learner), pos(2)).unwrap(),
        ApplyOutcome::Skipped
    );
    state
        .apply_conf_change(Some(&ConfChangeV2::default()), pos(4))
        .unwrap();
    state.apply_conf_change(None, pos(5)).unwrap();
    assert_eq!(store.descriptor(range).unwrap(), Some(promoted));
    assert_eq!(state.applied().unwrap(), Some(pos(5)));
    drop(state);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

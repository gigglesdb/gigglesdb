use giggles_membership::*;
use raft::eraftpb::ConfState;
fn identity() -> Identity {
    Identity {
        group: 7,
        owner_epoch: 1,
    }
}
fn machine(capacity: usize) -> Membership {
    Membership::new(identity(), vec![1, 2, 3], capacity).unwrap()
}
fn intent() -> Intent {
    Intent {
        id: 91,
        epoch: 1,
        learner: 4,
        remove: 3,
    }
}
fn command(operation: Operation) -> Command {
    Command {
        identity: identity(),
        operation,
    }
}
fn change(step: Step) -> Change {
    Change {
        identity: identity(),
        intent: 91,
        epoch: 1,
        step,
    }
}
fn assert_reject(m: &mut Membership, c: Command, index: u64, term: u64) {
    let before = m.clone();
    assert!(m.apply(&c, index, term).is_err());
    assert_eq!(*m, before, "rejected command mutated state");
}

#[test]
fn retry_payload_conflict_and_bound_are_atomic() {
    let mut m = machine(1);
    m.apply(&command(Operation::Begin(intent())), 1, 1).unwrap();
    m.apply(&command(Operation::Begin(intent())), 2, 1).unwrap();
    assert_eq!(m.receipts.len(), 1);
    let mut conflict = intent();
    conflict.remove = 2;
    assert_reject(&mut m, command(Operation::Begin(conflict)), 3, 1);
    let mut other = intent();
    other.id = 92;
    assert_reject(&mut m, command(Operation::Begin(other)), 3, 1);
    let mut stale = intent();
    stale.epoch = 0;
    stale.id = 93;
    assert_reject(&mut m, command(Operation::Begin(stale)), 3, 1);
}

#[test]
fn readiness_has_independent_required_bindings() {
    let mut m = machine(8);
    m.apply(&command(Operation::Begin(intent())), 1, 8).unwrap();
    // The expected learner configuration is independently asserted.
    let cc = m.configuration(&change(Step::AddLearner)).unwrap();
    assert_eq!(cc.changes.len(), 1);
    assert_eq!(cc.changes[0].node_id, 4);
    let conf = ConfState {
        voters: vec![1, 2, 3],
        learners: vec![4],
        ..Default::default()
    };
    m.applied_configuration(&change(Step::AddLearner), 2, &conf)
        .unwrap();
    let good = Readiness {
        intent: 91,
        epoch: 1,
        term: 8,
        barrier: 2,
        durable_applied: 2,
    };
    for field in 0..7 {
        let mut r = good.clone();
        match field {
            0 => r.intent += 1,
            1 => r.epoch += 1,
            2 => r.term -= 1,
            3 => r.barrier = 1,
            4 => r.durable_applied = 1,
            5 => r.barrier = 3,
            6 => r.durable_applied = 3,
            _ => unreachable!(),
        };
        assert_reject(&mut m, command(Operation::Ready(r)), 3, 8);
    }
    m.apply(&command(Operation::Ready(good)), 3, 8).unwrap();
    let cc = m.configuration(&change(Step::EnterJoint)).unwrap();
    assert_eq!(cc.enter_joint(), Some(false));
    assert!(!cc.leave_joint());
}

#[test]
fn owner_fence_invalidates_ready_and_old_authority() {
    let mut m = machine(8);
    m.apply(&command(Operation::Begin(intent())), 1, 1).unwrap();
    let conf = ConfState {
        voters: vec![1, 2, 3],
        learners: vec![4],
        ..Default::default()
    };
    m.applied_configuration(&change(Step::AddLearner), 2, &conf)
        .unwrap();
    m.apply(
        &command(Operation::Ready(Readiness {
            intent: 91,
            epoch: 1,
            term: 1,
            barrier: 2,
            durable_applied: 2,
        })),
        3,
        1,
    )
    .unwrap();
    m.apply(
        &command(Operation::Fence {
            next_owner_epoch: 2,
        }),
        4,
        1,
    )
    .unwrap();
    assert!(m.progress().unwrap().readiness.is_none());
    assert_reject(&mut m, command(Operation::Begin(intent())), 5, 1);
    assert!(m.configuration(&change(Step::EnterJoint)).is_err());
    let old = m.clone();
    assert!(
        m.apply(
            &Command {
                identity: m.identity,
                operation: Operation::Fence {
                    next_owner_epoch: 1
                }
            },
            5,
            1
        )
        .is_err()
    );
    assert_eq!(m, old);
}

#[test]
fn invalid_configuration_order_identity_and_overflow_fail_closed() {
    for voters in [vec![], vec![0], vec![1, 1], (1..=32).collect()] {
        assert!(Membership::new(identity(), voters, 8).is_err());
    }
    assert!(Membership::new(identity(), vec![1], 0).is_err());
    assert!(Membership::new(identity(), vec![1], 1025).is_err());
    let mut m = machine(8);
    m.epoch = u64::MAX;
    let mut i = intent();
    i.epoch = u64::MAX;
    assert_reject(&mut m, command(Operation::Begin(i)), 1, 1);
    let mut m = machine(8);
    m.apply(&command(Operation::Begin(intent())), 1, 1).unwrap();
    for step in [Step::EnterJoint, Step::LeaveJoint] {
        assert!(m.configuration(&change(step)).is_err());
    }
    let mut c = change(Step::AddLearner);
    c.epoch = 2;
    assert!(m.configuration(&c).is_err());
    c.epoch = 1;
    c.intent = 92;
    assert!(m.configuration(&c).is_err());
    c.intent = 91;
    c.identity.group = 8;
    assert!(m.configuration(&c).is_err());
}

#[test]
fn exhausted_receipts_do_not_evict_retry_outcomes_or_reuse_nodes() {
    let mut m = machine(1);
    m.receipts.insert(
        91,
        Progress {
            intent: intent(),
            phase: Phase::Complete,
            added_at: 2,
            readiness: None,
        },
    );
    m.epoch = 2;
    let mut i = intent();
    i.id = 92;
    i.epoch = 2;
    i.learner = 5;
    assert_reject(&mut m, command(Operation::Begin(i.clone())), 8, 1);
    m.capacity = 2;
    i.learner = 4;
    assert_reject(&mut m, command(Operation::Begin(i.clone())), 8, 1);
    i.learner = 5;
    m.apply(&command(Operation::Begin(i)), 8, 1).unwrap();
    assert_eq!(m.receipts.len(), 2);
    assert_eq!(m.receipts[&91].phase, Phase::Complete);
}

#[test]
fn incorrect_library_configuration_cannot_publish_progress() {
    let mut m = machine(8);
    m.apply(&command(Operation::Begin(intent())), 1, 1).unwrap();
    let before = m.clone();
    let valid = ConfState {
        voters: vec![1, 2, 3],
        learners: vec![4],
        ..Default::default()
    };
    assert!(
        m.applied_configuration(&change(Step::AddLearner), 0, &valid)
            .is_err()
    );
    assert_eq!(m, before);
    let voter_instead_of_learner = ConfState {
        voters: vec![1, 2, 3, 4],
        ..Default::default()
    };
    assert!(
        m.applied_configuration(&change(Step::AddLearner), 2, &voter_instead_of_learner)
            .is_err()
    );
    assert_eq!(m, before);
    let valid = ConfState {
        voters: vec![1, 2, 3],
        learners: vec![4],
        ..Default::default()
    };
    m.applied_configuration(&change(Step::AddLearner), 2, &valid)
        .unwrap();
    assert!(m.change_applied(&change(Step::AddLearner)).unwrap());
    assert!(!m.change_applied(&change(Step::EnterJoint)).unwrap());
    let mut wrong = change(Step::AddLearner);
    wrong.epoch += 1;
    assert!(m.change_applied(&wrong).is_err());
}

#[test]
fn recovery_rejects_invented_active_progress_and_frontier() {
    let mut m = machine(8);
    m.apply(&command(Operation::Begin(intent())), 1, 1).unwrap();
    m.validate(1).unwrap();
    m.active = None;
    assert!(m.validate(1).is_err());
    m.active = Some(91);
    m.receipts.get_mut(&91).unwrap().phase = Phase::CatchingUp;
    assert!(m.validate(1).is_err());
    m.receipts.get_mut(&91).unwrap().added_at = 2;
    assert!(m.validate(1).is_err());
    m.validate(2).unwrap();
}

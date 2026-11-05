use super::*;
use crate::{Action, Bounds, Descriptor, Machine, VOTERS, minimum};
use giggles_membership::Phase;
use std::collections::BTreeMap;

struct Cluster(BTreeMap<u64, Node>);
fn change(group: u64, step: Step) -> Change {
    Change {
        identity: giggles_membership::Identity {
            group,
            owner_epoch: 1,
        },
        intent: 1,
        epoch: 1,
        step,
    }
}
fn descriptor() -> Descriptor {
    Descriptor {
        range: 10,
        epoch: 1,
        group: 2,
        bounds: Bounds {
            start: minimum(),
            end: None,
        },
        voters: VOTERS.to_vec(),
    }
}
impl Cluster {
    fn new() -> Self {
        Self::with_image(2, Machine::range(42, descriptor()).unwrap())
    }
    fn with_image(group: u64, image: Machine) -> Self {
        Self::with_count(group, image, 4)
    }
    fn with_count(group: u64, image: Machine, count: u64) -> Self {
        let mut nodes = BTreeMap::new();
        for id in 1..=count {
            let store = Store::simulated(id, 42, group, image.clone()).unwrap();
            store
                .provision((1..=count).map(|id| (id, format!("{id:064x}"))).collect())
                .unwrap();
            nodes.insert(id, Node::new(store).unwrap());
        }
        let mut c = Self(nodes);
        c.0.get_mut(&1).unwrap().campaign().unwrap();
        c.pump();
        c
    }
    fn pump(&mut self) -> Vec<Applied> {
        let mut applied = Vec::new();
        for _ in 0..100 {
            let mut messages = Vec::new();
            for (id, n) in &mut self.0 {
                let out = n.drain().unwrap();
                applied.extend(out.applied);
                messages.extend(out.messages.into_iter().map(|m| (*id, m)));
            }
            if messages.is_empty() {
                return applied;
            }
            for (from, m) in messages {
                self.0.get_mut(&m.to).unwrap().step(from, m).unwrap();
            }
        }
        panic!("not quiescent")
    }
    fn begin(&mut self) {
        self.0
            .get_mut(&1)
            .unwrap()
            .membership_begin(
                vec![1],
                Intent {
                    id: 1,
                    epoch: 1,
                    learner: 4,
                    remove: 1,
                },
            )
            .unwrap();
        self.pump();
        self.change(Step::AddLearner);
    }
    fn change(&mut self, step: Step) {
        let group = self.0[&1].store.membership().unwrap().identity.group;
        self.0
            .get_mut(&1)
            .unwrap()
            .membership_change(vec![2], change(group, step))
            .unwrap();
        self.pump();
    }
    fn ready(&mut self) -> Readiness {
        let leader = &self.0[&1];
        let probe = crate::transport::Probe {
            intent: 1,
            epoch: 1,
            term: leader.raw.raft.term,
            barrier: leader.store.applied().unwrap(),
        };
        self.0[&4].durable_probe(1, &probe).unwrap()
    }
    fn joint(&mut self) {
        self.begin();
        let r = self.ready();
        self.0
            .get_mut(&1)
            .unwrap()
            .accept_readiness(vec![3], 4, r)
            .unwrap();
        self.pump();
        self.change(Step::EnterJoint);
    }
}
#[test]
fn readiness_requires_actual_apply_intent_identity_term_and_progress() {
    let mut c = Cluster::new();
    c.begin();
    let r = c.ready();
    for case in 0..5 {
        let mut bad = r.clone();
        let mut from = 4;
        match case {
            0 => from = 2,
            1 => bad.term += 1,
            2 => bad.intent += 1,
            3 => bad.barrier += 100,
            _ => bad.epoch += 1,
        };
        assert!(
            c.0.get_mut(&1)
                .unwrap()
                .accept_readiness(vec![3], from, bad)
                .is_err()
        );
    }
    let probe = crate::transport::Probe {
        intent: 1,
        epoch: 1,
        term: r.term,
        barrier: r.barrier + 100,
    };
    assert!(c.0[&4].durable_probe(1, &probe).is_err());
    let probe = crate::transport::Probe {
        intent: 1,
        epoch: 1,
        term: r.term,
        barrier: r.barrier,
    };
    assert!(c.0[&4].durable_probe(2, &probe).is_err());
    c.0.get_mut(&1)
        .unwrap()
        .accept_readiness(vec![3], 4, r)
        .unwrap();
    c.pump();
    c.change(Step::EnterJoint);
    for n in c.0.values() {
        let mut conf = n.store.configuration().unwrap();
        conf.voters.sort_unstable();
        conf.voters_outgoing.sort_unstable();
        assert_eq!(conf.voters, vec![2, 3, 4]);
        assert_eq!(conf.voters_outgoing, vec![1, 2, 3]);
    }
}

#[test]
fn membership_steps_reject_wrong_intent_epoch_and_owner_without_publication() {
    let mut c = Cluster::new();
    c.begin();
    let before = c.0[&1].store.durable_bytes().unwrap();
    for case in 0..4 {
        let mut request = change(2, Step::AddLearner);
        match case {
            0 => request.intent = 99,
            1 => request.epoch = 2,
            2 => request.identity.group = 3,
            _ => request.identity.owner_epoch = 2,
        }
        assert!(
            c.0.get_mut(&1)
                .unwrap()
                .membership_change(vec![42], request)
                .is_err()
        );
        assert_eq!(c.0[&1].store.durable_bytes().unwrap(), before);
    }
}

#[test]
fn delayed_completed_resume_cannot_advance_a_new_replacement() {
    let mut c = Cluster::with_count(2, Machine::range(42, descriptor()).unwrap(), 5);
    c.joint();
    c.change(Step::LeaveJoint);
    for _ in 0..40 {
        for n in c.0.values_mut() {
            n.deterministic_tick();
        }
        c.pump();
    }
    let leader =
        c.0.iter()
            .find_map(|(id, n)| n.is_leader().then_some(*id))
            .unwrap();
    c.0.get_mut(&leader)
        .unwrap()
        .membership_begin(
            vec![40],
            Intent {
                id: 2,
                epoch: 2,
                learner: 5,
                remove: 3,
            },
        )
        .unwrap();
    c.pump();
    let before = c.0[&leader].store.durable_bytes().unwrap();
    // A resume can pause after reading intent 1 and reach the actor only now.
    // All of its already-applied stages must resolve against receipt 1.
    for step in [Step::AddLearner, Step::EnterJoint, Step::LeaveJoint] {
        assert!(
            !c.0.get_mut(&leader)
                .unwrap()
                .membership_change(vec![41], change(2, step))
                .unwrap()
        );
        assert_eq!(c.0[&leader].store.durable_bytes().unwrap(), before);
    }
    assert_eq!(
        c.0[&leader].store.membership().unwrap().receipts[&2].phase,
        Phase::Planned
    );
    let mut next = change(2, Step::AddLearner);
    next.intent = 2;
    next.epoch = 2;
    assert!(
        c.0.get_mut(&leader)
            .unwrap()
            .membership_change(vec![42], next)
            .unwrap()
    );
    c.pump();
    assert_eq!(
        c.0[&leader].store.membership().unwrap().receipts[&2].phase,
        Phase::CatchingUp
    );
}
#[test]
fn removed_leader_refuses_delayed_messages_reads_writes_and_restarts_with_full_configuration() {
    let mut c = Cluster::new();
    c.joint();
    let delayed = Message {
        from: 1,
        to: 2,
        term: c.0[&1].raw.raft.term,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    };
    c.change(Step::LeaveJoint);
    let removed = c.0.get_mut(&1).unwrap();
    assert!(!removed.is_leader());
    assert!(removed.read_barrier(vec![1]).is_err());
    assert!(
        removed
            .propose(
                vec![1],
                &Command {
                    domain: 42,
                    group: 2,
                    action: Action::TaskWrite
                }
            )
            .is_err()
    );
    assert!(c.0.get_mut(&2).unwrap().step(1, delayed).is_err());
    for (id, n) in c.0 {
        let bytes = n.store.durable_bytes().unwrap();
        let recovered = Node::new(Store::recover_simulated(&bytes, id, 42, 2).unwrap()).unwrap();
        assert_eq!(
            recovered.store.membership().unwrap().receipts[&1].phase,
            Phase::Complete
        );
        let mut voters = recovered.store.configuration().unwrap().voters;
        voters.sort_unstable();
        assert_eq!(voters, vec![2, 3, 4]);
    }
}
#[test]
fn provisioned_identity_and_snapshot_configuration_cannot_be_rebound() {
    use raft::Storage;
    let mut c = Cluster::new();
    c.joint();
    let store = &c.0[&1].store;
    assert!(
        store
            .provision((1..=4).map(|id| (id, format!("{:064x}", id + 1))).collect())
            .is_err()
    );
    let mut snapshot = store.snapshot(0, 4).unwrap();
    snapshot.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];
    assert!(c.0[&4].store.verify_snapshot(&snapshot).is_err());
    snapshot.mut_metadata().mut_conf_state().voters = vec![2, 3, 4];
    snapshot
        .mut_metadata()
        .mut_conf_state()
        .voters_outgoing
        .clear();
    assert!(c.0[&4].store.verify_snapshot(&snapshot).is_err());
}

fn split() -> crate::Split {
    let parent = descriptor();
    let at = giggles_core::Key {
        tenant: giggles_core::TenantId(1),
        row: giggles_core::RowId(50),
    };
    let mut left = parent.clone();
    left.group = 3;
    left.range = 11;
    left.epoch = 2;
    left.bounds.end = Some(at);
    let mut right = parent.clone();
    right.group = 4;
    right.range = 12;
    right.epoch = 2;
    right.bounds.start = at;
    crate::Split {
        id: 20,
        parent,
        key: at,
        left,
        right,
    }
}
#[test]
fn both_membership_split_log_orders_fence_without_directory_or_identity_repoint() {
    for membership_first in [false, true] {
        let mut c = Cluster::new();
        let leader = c.0.get_mut(&1).unwrap();
        let command = Command {
            domain: 42,
            group: 2,
            action: Action::Fence(split()),
        };
        let intent = Intent {
            id: 1,
            epoch: 1,
            learner: 4,
            remove: 1,
        };
        if membership_first {
            leader.membership_begin(vec![1], intent).unwrap();
            leader.propose(vec![2], &command).unwrap();
        } else {
            leader.propose(vec![2], &command).unwrap();
            leader.membership_begin(vec![1], intent).unwrap();
        }
        let replies = c.pump();
        assert!(replies.iter().any(|r| r.reply.code != 0));
        let state = c.0[&1].store.state().unwrap();
        let Machine::Range { data, .. } = state.image else {
            panic!()
        };
        assert_eq!(data.descriptor, descriptor());
        assert_eq!(data.fence.is_none(), membership_first);
        assert_eq!(state.membership.active.is_some(), membership_first);
    }
    let mut c = Cluster::new();
    c.joint();
    c.change(Step::LeaveJoint);
    for _ in 0..40 {
        for n in c.0.values_mut() {
            n.deterministic_tick();
        }
        c.pump();
    }
    let leader =
        c.0.iter()
            .find_map(|(id, n)| n.is_leader().then_some(*id))
            .unwrap();
    c.0.get_mut(&leader)
        .unwrap()
        .propose(
            vec![9],
            &Command {
                domain: 42,
                group: 2,
                action: Action::Fence(split()),
            },
        )
        .unwrap();
    let replies = c.pump();
    assert!(
        replies
            .iter()
            .any(|r| r.context == vec![9] && r.reply.code != 0)
    );
    let Machine::Range { data, .. } = c.0[&2].store.image().unwrap() else {
        panic!()
    };
    assert!(data.fence.is_none());
    assert_eq!(data.descriptor, descriptor());
}
#[test]
fn leadership_term_change_requires_fresh_durable_learner_proof() {
    let mut c = Cluster::new();
    c.begin();
    let r = c.ready();
    c.0.get_mut(&1)
        .unwrap()
        .accept_readiness(vec![3], 4, r.clone())
        .unwrap();
    c.pump();
    c.0.get_mut(&1).unwrap().transfer(2).unwrap();
    c.pump();
    assert!(c.0[&2].is_leader());
    assert!(c.0[&2].raw.raft.term > r.term);
    assert!(
        c.0.get_mut(&2)
            .unwrap()
            .membership_change(vec![4], change(2, Step::EnterJoint))
            .is_err()
    );
    let leader = &c.0[&2];
    let request = crate::transport::Probe {
        intent: 1,
        epoch: 1,
        term: leader.raw.raft.term,
        barrier: leader.store.applied().unwrap(),
    };
    assert!(c.0[&4].durable_probe(1, &request).is_err());
    let proof = c.0[&4].durable_probe(2, &request).unwrap();
    c.0.get_mut(&2)
        .unwrap()
        .accept_readiness(vec![5], 4, proof)
        .unwrap();
    c.pump();
    c.0.get_mut(&2)
        .unwrap()
        .membership_change(vec![6], change(2, Step::EnterJoint))
        .unwrap();
    c.pump();
    assert_eq!(
        c.0[&2]
            .store
            .membership()
            .unwrap()
            .progress()
            .unwrap()
            .phase,
        Phase::Joint
    );
}

#[test]
fn existing_child_activation_replay_remains_resumable_after_voter_replacement() {
    let split = split();
    let mut parent = Machine::range(42, descriptor()).unwrap();
    assert_eq!(
        parent
            .apply(
                &Command {
                    domain: 42,
                    group: 2,
                    action: Action::Fence(split)
                },
                1
            )
            .code,
        0
    );
    let Machine::Range { data, .. } = parent else {
        panic!()
    };
    let child = data.children().unwrap()[0].clone();
    let proof = child.provenance.clone().unwrap();
    let group = child.descriptor.group;
    let mut machine = Machine::Vacant { domain: 42, group };
    assert_eq!(
        machine
            .apply(
                &Command {
                    domain: 42,
                    group,
                    action: Action::Bootstrap(Box::new(child))
                },
                2
            )
            .code,
        0
    );
    assert_eq!(
        machine
            .apply(
                &Command {
                    domain: 42,
                    group,
                    action: Action::Activate {
                        proof: proof.clone(),
                        revision: 2
                    }
                },
                3
            )
            .code,
        0
    );
    let mut c = Cluster::with_image(group, machine.clone());
    c.joint();
    c.change(Step::LeaveJoint);
    for _ in 0..40 {
        for n in c.0.values_mut() {
            n.deterministic_tick();
        }
        c.pump();
    }
    let leader =
        c.0.iter()
            .find_map(|(id, n)| n.is_leader().then_some(*id))
            .unwrap();
    let before = c.0[&leader].store.membership().unwrap();
    c.0.get_mut(&leader)
        .unwrap()
        .propose(
            vec![42],
            &Command {
                domain: 42,
                group,
                action: Action::Activate { proof, revision: 2 },
            },
        )
        .unwrap();
    let replies = c.pump();
    assert!(
        replies
            .iter()
            .any(|r| r.context == vec![42] && r.reply.code == 0)
    );
    assert_eq!(c.0[&leader].store.image().unwrap(), machine);
    assert_eq!(c.0[&leader].store.membership().unwrap(), before);
}

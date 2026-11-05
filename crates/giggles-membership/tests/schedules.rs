use giggles_membership::{
    durable::{Fault, Node, Store},
    *,
};
use raft::{Storage, eraftpb::*};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static SEQ: AtomicU64 = AtomicU64::new(0);
fn identity() -> Identity {
    Identity {
        group: 7,
        owner_epoch: 1,
    }
}
fn initial() -> Membership {
    Membership::new(identity(), vec![1, 2, 3], 8).unwrap()
}
fn intent() -> Intent {
    Intent {
        id: 91,
        epoch: 1,
        learner: 4,
        remove: 3,
    }
}
fn begin() -> Command {
    Command {
        identity: identity(),
        operation: Operation::Begin(intent()),
    }
}
struct Net {
    nodes: BTreeMap<u64, Node>,
    paths: BTreeMap<u64, PathBuf>,
    blocked: BTreeSet<(u64, u64)>,
    queue: VecDeque<Message>,
    errors: Vec<(u64, String)>,
}
impl Net {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("artifacts")
            .join(format!(
                "run-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&root).unwrap();
        let mut s = Self {
            nodes: BTreeMap::new(),
            paths: BTreeMap::new(),
            blocked: BTreeSet::new(),
            queue: VecDeque::new(),
            errors: vec![],
        };
        for id in 1..=4 {
            let path = root.join(format!("node-{id}.redb"));
            s.nodes.insert(
                id,
                Node::new(Store::open(&path, id, initial()).unwrap()).unwrap(),
            );
            s.paths.insert(id, path);
        }
        s.nodes.get_mut(&1).unwrap().campaign().unwrap();
        s.pump();
        assert!(s.nodes[&1].is_leader());
        s
    }
    fn pump(&mut self) {
        for _ in 0..1000 {
            let mut failed = vec![];
            for (id, node) in &mut self.nodes {
                match node.drive() {
                    Ok(out) => {
                        assert!(out.rejected.is_empty(), "{:?}", out.rejected);
                        self.queue.extend(out.messages);
                    }
                    Err(e) => {
                        self.errors.push((*id, e.to_string()));
                        failed.push(*id);
                    }
                }
            }
            for id in failed {
                self.nodes.remove(&id);
            }
            if self.queue.is_empty() {
                return;
            }
            let messages = std::mem::take(&mut self.queue);
            for msg in messages {
                if !self.blocked.contains(&(msg.from, msg.to))
                    && let Some(node) = self.nodes.get_mut(&msg.to)
                {
                    node.step(msg.from, msg).unwrap();
                }
            }
        }
        panic!("schedule did not quiesce");
    }
    fn ticks(&mut self, n: usize) {
        for _ in 0..n {
            for node in self.nodes.values_mut() {
                node.tick().unwrap();
            }
            self.pump();
        }
    }
    fn leader(&self) -> u64 {
        self.nodes
            .iter()
            .find(|(_, n)| n.is_leader())
            .map(|(id, _)| *id)
            .unwrap()
    }
    fn propose(&mut self, c: Command) {
        let id = self.leader();
        self.nodes.get_mut(&id).unwrap().propose(c).unwrap();
        self.pump();
    }
    fn change(&mut self, step: Step) {
        let id = self.leader();
        let m = self.nodes[&id].store.membership().unwrap();
        self.nodes
            .get_mut(&id)
            .unwrap()
            .change(Change {
                identity: m.identity,
                intent: 91,
                epoch: 1,
                step,
            })
            .unwrap();
        self.pump();
    }
    fn ready(&mut self) {
        let id = self.leader();
        let barrier = self.nodes[&4]
            .store
            .applied()
            .unwrap()
            .min(self.nodes[&id].store.applied().unwrap());
        self.propose(Command {
            identity: identity(),
            operation: Operation::Ready(Readiness {
                intent: 91,
                epoch: 1,
                term: self.nodes[&id].term(),
                barrier,
                durable_applied: barrier,
            }),
        });
    }
    fn prepare(&mut self) {
        self.propose(begin());
        self.change(Step::AddLearner);
        self.ready();
    }
    fn restart(&mut self, id: u64) {
        self.nodes.remove(&id);
        self.nodes.insert(
            id,
            Node::new(Store::open(&self.paths[&id], id, initial()).unwrap()).unwrap(),
        );
    }
    fn partition(&mut self, a: &[u64], b: &[u64]) {
        for x in a {
            for y in b {
                self.blocked.insert((*x, *y));
                self.blocked.insert((*y, *x));
            }
        }
    }
    fn phase(&self, id: u64) -> Phase {
        self.nodes[&id].store.membership().unwrap().receipts[&91].phase
    }
}

#[test]
fn real_joint_transition_is_durable_and_retry_identity_survives_restart() {
    let mut n = Net::new();
    n.prepare();
    assert_eq!(n.nodes[&1].store.conf_state().unwrap().learners, vec![4]);
    n.change(Step::EnterJoint);
    let at = n.nodes[&1].store.applied().unwrap();
    n.change(Step::EnterJoint);
    assert_eq!(
        n.nodes[&1].store.applied().unwrap(),
        at,
        "retry must not propose another config"
    );
    let mut c = n.nodes[&1].store.conf_state().unwrap();
    c.voters_outgoing.sort_unstable();
    c.voters.sort_unstable();
    assert_eq!(c.voters_outgoing, vec![1, 2, 3]);
    assert_eq!(c.voters, vec![1, 2, 4]);
    assert!(!c.auto_leave);
    n.restart(1);
    n.ticks(25);
    n.change(Step::LeaveJoint);
    n.ticks(5);
    for id in [1, 2, 4] {
        assert_eq!(n.phase(id), Phase::Complete);
        n.restart(id);
        assert_eq!(n.nodes[&id].store.membership().unwrap().epoch, 2);
    }
    n.ticks(25);
    n.propose(begin());
    let leader = n.leader();
    let before = n.nodes[&leader].store.applied().unwrap();
    n.change(Step::LeaveJoint);
    assert_eq!(n.nodes[&leader].store.applied().unwrap(), before);
    assert!(n.errors.is_empty());
    let mut conflicting = begin();
    if let Operation::Begin(i) = &mut conflicting.operation {
        i.remove = 2;
    }
    let leader = n.leader();
    assert!(
        n.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(conflicting)
            .is_err()
    );
}

#[test]
fn learner_cannot_elect_or_supply_an_old_majority() {
    let mut n = Net::new();
    n.propose(begin());
    n.change(Step::AddLearner);
    n.nodes.remove(&2);
    n.nodes.remove(&3);
    let before = n.nodes[&1].store.applied().unwrap();
    n.propose(begin());
    n.ticks(6);
    assert_eq!(n.nodes[&1].store.applied().unwrap(), before);
    n.nodes.get_mut(&4).unwrap().campaign().unwrap();
    n.pump();
    assert!(!n.nodes[&4].is_leader());
}

#[test]
fn joint_requires_both_majorities_with_negative_control() {
    // {1,3} is an old majority only; {1,4} is a new majority only.
    // Neither may commit; {1,2} independently satisfies both and must commit.
    for (survivors, should_commit) in [(vec![1, 3], false), (vec![1, 4], false), (vec![1, 2], true)]
    {
        let mut n = Net::new();
        n.prepare();
        n.change(Step::EnterJoint);
        for id in 1..=4 {
            if !survivors.contains(&id) {
                n.nodes.remove(&id);
            }
        }
        let before = n.nodes[&1].store.applied().unwrap();
        n.propose(begin());
        n.ticks(5);
        assert_eq!(
            n.nodes[&1].store.applied().unwrap() > before,
            should_commit,
            "survivors={survivors:?}"
        );
    }
}

#[test]
fn snapshot_installs_membership_then_tail_and_readiness_is_not_match_alone() {
    let mut n = Net::new();
    n.partition(&[4], &[1, 2, 3]);
    n.propose(begin());
    n.change(Step::AddLearner);
    let leader = n.leader();
    let barrier = n.nodes[&leader].store.applied().unwrap();
    let term = n.nodes[&leader].term();
    assert!(
        n.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(Command {
                identity: identity(),
                operation: Operation::Ready(Readiness {
                    intent: 91,
                    epoch: 1,
                    term,
                    barrier,
                    durable_applied: barrier
                })
            })
            .is_err()
    );
    for _ in 0..8 {
        n.propose(begin());
    }
    for id in [1, 2, 3] {
        n.nodes[&id].store.compact().unwrap();
    }
    let compacted = n.nodes[&1].store.first_index().unwrap();
    assert!(compacted > 5);
    n.blocked.clear();
    n.ticks(8);
    assert!(n.nodes[&4].store.applied().unwrap() >= compacted - 1);
    assert_eq!(n.phase(4), Phase::CatchingUp);
    n.propose(begin());
    n.ready();
    n.change(Step::EnterJoint);
    n.change(Step::LeaveJoint);
    n.restart(4);
    assert_eq!(n.phase(4), Phase::Complete);
}

#[test]
fn stale_term_readiness_and_out_of_order_changes_reject() {
    let mut n = Net::new();
    n.propose(begin());
    let c = Change {
        identity: identity(),
        intent: 91,
        epoch: 1,
        step: Step::EnterJoint,
    };
    assert!(n.nodes.get_mut(&1).unwrap().change(c.clone()).is_err());
    n.change(Step::AddLearner);
    n.ready();
    n.nodes.remove(&1);
    n.ticks(30);
    let leader = n.leader();
    assert!(n.nodes.get_mut(&leader).unwrap().change(c).is_err());
    n.ready();
    n.change(Step::EnterJoint);
    n.change(Step::LeaveJoint);
}

#[test]
fn crash_before_and_after_each_transition_recovers_atomic_progress() {
    for stage in 0..5 {
        for fault in [Fault::BeforeCommit, Fault::AfterCommit] {
            let mut n = Net::new();
            if stage > 0 {
                n.propose(begin());
            }
            if stage > 1 {
                n.change(Step::AddLearner);
            }
            if stage > 2 {
                n.ready();
            }
            if stage > 3 {
                n.change(Step::EnterJoint);
            }
            n.nodes[&1].store.inject(fault).unwrap();
            match stage {
                0 => n.propose(begin()),
                1 => n.change(Step::AddLearner),
                2 => n.ready(),
                3 => n.change(Step::EnterJoint),
                4 => n.change(Step::LeaveJoint),
                _ => unreachable!(),
            }
            assert_eq!(n.errors.len(), 1, "stage {stage} {fault:?}");
            assert!(!n.nodes.contains_key(&1));
            n.restart(1);
            n.pump();
            n.ticks(25);
            assert_eq!(n.errors.len(), 1, "recovery must not error");
            let m = n.nodes[&1].store.membership().unwrap();
            assert_eq!(
                m.receipts[&91].phase,
                match stage {
                    0 => Phase::Planned,
                    1 | 2 => Phase::CatchingUp,
                    3 => Phase::Joint,
                    4 => Phase::Complete,
                    _ => unreachable!(),
                }
            );
            if stage == 2 {
                assert!(m.receipts[&91].readiness.is_some());
            }
        }
    }
}

#[test]
fn removed_leader_cannot_commit_after_leave_and_delayed_packets() {
    let mut n = Net::new();
    let mut cmd = begin();
    if let Operation::Begin(i) = &mut cmd.operation {
        i.remove = 1;
    }
    n.propose(cmd.clone());
    n.change(Step::AddLearner);
    n.ready();
    n.change(Step::EnterJoint);
    // Capture old leader traffic, then complete removal and elect a survivor.
    n.nodes.get_mut(&1).unwrap().propose(cmd.clone()).unwrap();
    let delayed = n.nodes.get_mut(&1).unwrap().drive().unwrap().messages;
    n.queue.extend(delayed.clone());
    n.pump();
    n.change(Step::LeaveJoint);
    n.ticks(35);
    assert!(!n.nodes[&1].is_leader());
    let new = n.leader();
    assert_ne!(new, 1);
    let before = n.nodes[&new].store.applied().unwrap();
    n.queue.extend(delayed);
    n.pump();
    n.propose(cmd);
    assert!(n.nodes[&new].store.applied().unwrap() > before);
    assert!(n.nodes.get_mut(&1).unwrap().propose(begin()).is_err());
}

#[test]
fn process_exits_at_applied_barrier_recover_real_redb() {
    for stage in 0..5 {
        for after in [false, true] {
            let mut n = Net::new();
            if stage > 0 {
                n.propose(begin());
            }
            if stage > 1 {
                n.change(Step::AddLearner);
            }
            if stage > 2 {
                n.ready();
            }
            if stage > 3 {
                n.change(Step::EnterJoint);
            }
            let before = n.nodes[&1].store.applied().unwrap();
            // Stage a committed but unapplied entry, then let a real subprocess
            // exit immediately before/after the atomic apply transaction commits.
            n.nodes[&1].store.inject(Fault::BeforeCommit).unwrap();
            match stage {
                0 => n.propose(begin()),
                1 => n.change(Step::AddLearner),
                2 => n.ready(),
                3 => n.change(Step::EnterJoint),
                4 => n.change(Step::LeaveJoint),
                _ => unreachable!(),
            }
            assert!(!n.nodes.contains_key(&1));
            let status = std::process::Command::new(env!("CARGO_BIN_EXE_membership-recover"))
                .arg(&n.paths[&1])
                .arg(if after { "after" } else { "before" })
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(if after { 87 } else { 86 }));
            let s = Store::open(&n.paths[&1], 1, initial()).unwrap();
            assert_eq!(
                s.applied().unwrap(),
                before + u64::from(after),
                "stage={stage} after={after}"
            );
            drop(s);
            n.restart(1);
            n.pump();
            assert_eq!(
                n.phase(1),
                match stage {
                    0 => Phase::Planned,
                    1 | 2 => Phase::CatchingUp,
                    3 => Phase::Joint,
                    4 => Phase::Complete,
                    _ => unreachable!(),
                }
            );
        }
    }
}

#[test]
fn pending_admin_barrier_and_owner_epoch_prevent_unreplayable_configuration() {
    let mut n = Net::new();
    n.prepare();
    n.nodes
        .get_mut(&1)
        .unwrap()
        .propose(Command {
            identity: identity(),
            operation: Operation::Fence {
                next_owner_epoch: 2,
            },
        })
        .unwrap();
    assert!(
        n.nodes
            .get_mut(&1)
            .unwrap()
            .change(Change {
                identity: identity(),
                intent: 91,
                epoch: 1,
                step: Step::EnterJoint
            })
            .is_err()
    );
    n.pump();
    assert!(n.nodes.get_mut(&1).unwrap().propose(begin()).is_err());
    let m = n.nodes[&1].store.membership().unwrap();
    assert_eq!(m.identity.owner_epoch, 2);
    assert!(m.progress().unwrap().readiness.is_none());
    n.restart(1);
    assert_eq!(
        n.nodes[&1].store.membership().unwrap().identity.owner_epoch,
        2
    );
}

#[test]
fn forged_transport_snapshot_and_future_readiness_do_not_mutate_state() {
    let mut n = Net::new();
    n.prepare();
    let before = n.nodes[&1].store.durable_bytes().unwrap();
    let mut message = Message {
        from: 2,
        to: 1,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgAppendResponse);
    assert!(n.nodes.get_mut(&1).unwrap().step(3, message).is_err());
    let mut snapshot = n.nodes[&1].store.snapshot(1, 4).unwrap();
    snapshot.mut_metadata().mut_conf_state().voters.push(99);
    let mut message = Message {
        from: 1,
        to: 4,
        ..Default::default()
    };
    message.set_snapshot(snapshot);
    message.set_msg_type(MessageType::MsgSnapshot);
    assert!(n.nodes.get_mut(&4).unwrap().step(1, message).is_err());
    let term = n.nodes[&1].term();
    let barrier = n.nodes[&1].store.applied().unwrap() + 1;
    assert!(
        n.nodes
            .get_mut(&1)
            .unwrap()
            .propose(Command {
                identity: identity(),
                operation: Operation::Ready(Readiness {
                    intent: 91,
                    epoch: 1,
                    term,
                    barrier,
                    durable_applied: barrier
                })
            })
            .is_err()
    );
    assert_eq!(n.nodes[&1].store.durable_bytes().unwrap(), before);
}

use giggles_replication::{
    error::Error,
    log_store::LogStore,
    node::{Output, Replica},
    state::MemStateMachine,
};
use raft::{StateRole, Storage, eraftpb::MessageType};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

struct Cluster {
    nodes: Vec<Replica>,
    states: Vec<Arc<MemStateMachine>>,
    dir: PathBuf,
}
impl Cluster {
    fn new(n: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "node-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let voters: Vec<_> = (1..=n).collect();
        let states: Vec<_> = (0..n).map(|_| Arc::new(MemStateMachine::new())).collect();
        let nodes = voters
            .iter()
            .map(|id| {
                Replica::new(
                    LogStore::open(&dir.join(id.to_string()), [7; 16], *id).unwrap(),
                    states[*id as usize - 1].clone(),
                    &voters,
                )
                .unwrap()
            })
            .collect();
        Self { nodes, states, dir }
    }
    fn settle(&mut self, isolated: bool) -> Vec<Output> {
        let mut outputs = Vec::new();
        for _ in 0..100 {
            let mut messages = Vec::new();
            for node in &mut self.nodes {
                let mut out = node.drive().unwrap();
                let last = node.log().last_index().unwrap();
                for m in &out.messages {
                    // Both leader append requests and follower acknowledgements are durable on return.
                    assert!(m.entries.iter().all(|e| e.index <= last));
                    if m.get_msg_type() == MessageType::MsgAppendResponse && !m.reject {
                        assert!(m.index <= last);
                    }
                }
                messages.append(&mut out.messages);
                outputs.push(out);
            }
            if messages.is_empty() {
                return outputs;
            }
            for m in messages {
                if isolated && (m.from == 1 || m.to == 1) {
                    continue;
                }
                self.nodes[m.to as usize - 1].step(m.from, m).unwrap();
            }
        }
        panic!("delivery did not quiesce");
    }
    fn elect(&mut self) {
        self.nodes[0].campaign().unwrap();
        self.settle(false);
        assert_eq!(self.nodes[0].role().state, StateRole::Leader);
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
#[test]
fn single_voter_applies_waiters_in_order() {
    let mut c = Cluster::new(1);
    c.elect();
    assert_eq!(c.nodes[0].node_id(), 1);
    c.nodes[0].propose(b"first", 11).unwrap();
    c.nodes[0].propose(b"second", 12).unwrap();
    let applied: Vec<_> = c
        .settle(false)
        .into_iter()
        .flat_map(|o| o.applied)
        .collect();
    assert_eq!(applied, vec![(11, 2), (12, 3)]);
    assert_eq!(
        c.states[0]
            .applied_batches()
            .iter()
            .map(|(_, b)| b.as_slice())
            .collect::<Vec<_>>(),
        vec![b"first".as_slice(), b"second".as_slice()]
    );
    c.nodes[0].read_index(b"read".to_vec()).unwrap();
    let reads: Vec<_> = c
        .settle(false)
        .into_iter()
        .flat_map(|o| o.read_states)
        .collect();
    assert_eq!(reads, vec![(b"read".to_vec(), 3)]);
}
#[test]
fn three_voters_replicate_and_persist_before_release() {
    let mut c = Cluster::new(3);
    c.elect();
    assert!(matches!(
        c.nodes[1].propose(b"bad", 0),
        Err(Error::NotLeader)
    ));
    assert!(matches!(
        c.nodes[1].read_index(vec![]),
        Err(Error::NotLeader)
    ));
    for waiter in 1..=2 {
        c.nodes[0].propose(&[waiter as u8], waiter).unwrap();
    }
    let applied: Vec<_> = c
        .settle(false)
        .into_iter()
        .flat_map(|o| o.applied)
        .collect();
    assert_eq!(applied, vec![(1, 2), (2, 3)]);
    for state in &c.states {
        assert_eq!(state.applied_batches(), c.states[0].applied_batches());
    }
    assert_eq!(c.states[0].applied_batches().len(), 2);
}
#[test]
fn restart_catches_up_without_reapplying() {
    let mut c = Cluster::new(3);
    c.elect();
    c.nodes[0].propose(b"old", 1).unwrap();
    c.settle(false);
    let old_count = c.states[1].applied_batches().len();
    drop(c.nodes.remove(1));
    // Keep node 2 offline while nodes 1 and 3 commit another batch.
    c.nodes[0].propose(b"new", 2).unwrap();
    for _ in 0..10 {
        let mut messages = Vec::new();
        for node in &mut c.nodes {
            messages.extend(node.drive().unwrap().messages);
        }
        for m in messages {
            if m.to != 2 {
                c.nodes
                    .iter_mut()
                    .find(|n| n.node_id() == m.to)
                    .unwrap()
                    .step(m.from, m)
                    .unwrap();
            }
        }
    }
    let reopened = Replica::new(
        LogStore::open(&c.dir.join("2"), [7; 16], 2).unwrap(),
        c.states[1].clone(),
        &[1, 2, 3],
    )
    .unwrap();
    c.nodes.insert(1, reopened);
    c.nodes[1].drive().unwrap();
    assert_eq!(c.states[1].applied_batches().len(), old_count);
    for _ in 0..2 {
        c.nodes[0].tick();
    }
    c.settle(false);
    assert_eq!(c.states[1].applied_batches(), c.states[0].applied_batches());
    assert_eq!(c.states[1].applied_batches().len(), old_count + 1);
}
#[test]
fn deposed_leader_fails_uncommitted_waiter() {
    let mut c = Cluster::new(3);
    c.elect();
    c.nodes[0].propose(b"lost", 99).unwrap();
    c.settle(true);
    for _ in 0..25 {
        c.nodes[1].set_election_timeout(10);
        c.nodes[2].set_election_timeout(19);
        c.nodes[1].tick();
        c.nodes[2].tick();
        c.settle(true);
    }
    assert_eq!(c.nodes[1].role().state, StateRole::Leader);
    for _ in 0..2 {
        c.nodes[1].tick();
    }
    let outputs = c.settle(false);
    assert_eq!(
        outputs
            .into_iter()
            .flat_map(|o| o.failed)
            .collect::<Vec<_>>(),
        vec![99]
    );
    assert!(c.states[0].applied_batches().is_empty());
}
#[test]
fn restarted_proposer_never_acks_a_recycled_context() {
    let mut c = Cluster::new(3);
    c.elect();
    c.nodes[0].propose(b"old-1", 1).unwrap();
    let old_index = c.nodes[0].last_index();
    assert_eq!(
        c.settle(false)
            .into_iter()
            .flat_map(|o| o.applied)
            .collect::<Vec<_>>(),
        vec![(1, old_index)]
    );
    c.nodes[0].propose(b"old-2", 2).unwrap();
    let overwritten_index = c.nodes[0].last_index();
    let isolated = c.settle(true);
    assert!(
        isolated
            .iter()
            .all(|o| o.applied.is_empty() && o.failed.is_empty())
    );
    for _ in 0..25 {
        c.nodes[1].set_election_timeout(10);
        c.nodes[2].set_election_timeout(19);
        c.nodes[1].tick();
        c.nodes[2].tick();
        c.settle(true);
    }
    assert_eq!(c.nodes[1].role().state, StateRole::Leader);
    for _ in 0..2 {
        c.nodes[1].tick();
    }
    let mut outputs = c.settle(false);
    assert!(c.nodes[0].applied_index() >= overwritten_index);
    assert_eq!(
        outputs
            .iter()
            .flat_map(|o| o.failed.iter().copied())
            .collect::<Vec<_>>(),
        vec![2]
    );

    drop(c.nodes.remove(0));
    c.nodes.insert(
        0,
        Replica::new(
            LogStore::open(&c.dir.join("1"), [7; 16], 1).unwrap(),
            c.states[0].clone(),
            &[1, 2, 3],
        )
        .unwrap(),
    );
    outputs.extend(c.settle(false));
    c.nodes[1].transfer_leader(1);
    outputs.extend(c.settle(false));
    assert_eq!(c.nodes[0].role().state, StateRole::Leader);
    c.nodes[0].propose(b"new", 1).unwrap();
    let new_index = c.nodes[0].last_index();
    assert!(new_index > overwritten_index);
    outputs.extend(c.settle(false));
    outputs.extend(c.settle(false));
    assert_eq!(
        outputs
            .iter()
            .flat_map(|o| o.applied.iter().copied())
            .collect::<Vec<_>>(),
        vec![(1, new_index)]
    );
    assert_eq!(
        outputs
            .iter()
            .flat_map(|o| o.failed.iter().copied())
            .collect::<Vec<_>>(),
        vec![2]
    );
    for state in &c.states {
        let batches = state.applied_batches();
        assert_eq!(
            batches
                .iter()
                .map(|(at, b)| (at.index, b.as_slice()))
                .collect::<Vec<_>>(),
            vec![
                (old_index, b"old-1".as_slice()),
                (new_index, b"new".as_slice())
            ]
        );
    }
    let context_at = |index| {
        c.nodes[0]
            .log()
            .entries(
                index,
                index + 1,
                None,
                raft::GetEntriesContext::empty(false),
            )
            .unwrap()[0]
            .context
            .clone()
    };
    assert_eq!(context_at(old_index), context_at(new_index));
}
#[test]
fn peer_validation_and_fatal_apply() {
    use raft::eraftpb::{Entry, EntryType, Message, Snapshot, SnapshotMetadata};
    let mut c = Cluster::new(1);
    c.elect();
    let base = Message {
        from: 1,
        to: 1,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    };
    let mut invalid = vec![(2, base.clone())];
    let mut m = base.clone();
    m.from = 2;
    invalid.push((1, m));
    let mut m = base.clone();
    m.to = 2;
    invalid.push((1, m));
    for msg_type in [
        MessageType::MsgHup,
        MessageType::MsgPropose,
        MessageType::MsgReadIndex,
    ] {
        let mut m = base.clone();
        m.msg_type = msg_type;
        invalid.push((1, m));
    }
    let mut m = base.clone();
    m.mut_entries().push(Entry {
        entry_type: EntryType::EntryConfChange,
        ..Default::default()
    });
    invalid.push((1, m));
    let mut m = base;
    let mut snapshot = Snapshot::default();
    snapshot.set_metadata(SnapshotMetadata {
        index: 1,
        ..Default::default()
    });
    m.set_snapshot(snapshot);
    invalid.push((1, m));
    for (from, m) in invalid {
        assert!(matches!(c.nodes[0].step(from, m), Err(Error::Corrupt(_))));
    }
    c.nodes[0].propose(b"fail", 1).unwrap();
    c.states[0].fail_next_apply();
    assert!(matches!(c.nodes[0].drive(), Err(Error::Apply(_))));
}

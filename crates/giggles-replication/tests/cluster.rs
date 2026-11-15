use crate::common::{CLUSTER, Cluster};
use giggles_replication::{entry, error::Error, log_store::LogStore};
use raft::{
    GetEntriesContext, StateRole, Storage,
    eraftpb::{Message, MessageType},
};

#[test]
fn three_voters_restart_tail_and_storage_contract() {
    let mut c = Cluster::new("restart-tail", 3, 1);
    c.elect(1);
    assert_eq!(c.leader(), Some(1));
    assert!(c.propose(2, b"not-leader").is_err());
    for payload in [b"one".as_slice(), b"two", b"three"] {
        let w = c.propose(1, payload).unwrap();
        assert!(c.committed(w));
    }
    let log = c.nodes[&1].log();
    let last = log.last_index().unwrap();
    assert_eq!(log.first_index().unwrap(), 1);
    assert_eq!(
        log.entries(1, last + 1, Some(0), GetEntriesContext::empty(false))
            .unwrap()
            .len(),
        1
    );
    assert!(
        log.entries(last + 1, last + 2, None, GetEntriesContext::empty(false))
            .is_err()
    );
    c.crash(3);
    let w = c.propose(1, b"offline").unwrap();
    assert!(c.committed(w));
    assert!(matches!(
        LogStore::open(&c.root.join("3"), CLUSTER, 4),
        Err(Error::Identity(_))
    ));
    c.restart(3);
    c.set_faults(15, 50, true);
    c.propose(1, b"faulty-delivery").unwrap();
    c.tick_all(4);
    c.heal();
    let w = c.propose(1, b"after-heal").unwrap();
    assert!(c.committed(w));
    c.nodes
        .get_mut(&1)
        .unwrap()
        .read_index(b"read".to_vec())
        .unwrap();
    c.pump();
    assert_eq!(c.reads[&1], vec![(b"read".to_vec(), c.applied[&w])]);
    c.assert_equal();
    assert_eq!(c.states[&3].applied_batches().len(), 6);
}

#[test]
fn durable_vote_restart_refuses_second_vote_in_same_term() {
    let mut c = Cluster::new("durable-vote", 3, 2);
    for (from, reject) in [(2, false), (3, true)] {
        let n = c.nodes.get_mut(&1).unwrap();
        n.step(
            from,
            Message {
                from,
                to: 1,
                term: 7,
                msg_type: MessageType::MsgRequestVote,
                ..Default::default()
            },
        )
        .unwrap();
        let out = n.drive().unwrap();
        assert!(out.messages.iter().any(|m| m.to == from
            && m.get_msg_type() == MessageType::MsgRequestVoteResponse
            && m.reject == reject));
        let hs = n.log().hard_state().unwrap();
        assert_eq!((hs.term, hs.vote), (7, 2));
        c.crash(1);
        c.restart(1);
        let hs = c.nodes[&1].log().hard_state().unwrap();
        assert_eq!((hs.term, hs.vote), (7, 2));
    }
}

#[test]
fn leader_dies_before_quorum_uncommitted_tail_is_replaced() {
    let mut c = Cluster::new("replace-tail", 3, 30);
    c.elect(1);
    c.isolate(1);
    let lost = c.propose(1, b"lost").unwrap();
    assert!(!c.committed(lost));
    let index = c.nodes[&1].log().last_index().unwrap();
    c.crash(1);
    assert!(c.failed(lost));
    c.elect(2);
    let w = c.propose(2, b"winner").unwrap();
    assert!(c.committed(w));
    c.restart(1);
    c.heal();
    let log = c.nodes[&1].log();
    let entries = log
        .entries(
            index,
            log.last_index().unwrap() + 1,
            None,
            GetEntriesContext::empty(false),
        )
        .unwrap();
    assert!(
        entries
            .iter()
            .filter(|e| !e.data.is_empty())
            .all(|e| entry::decode(CLUSTER, &e.data).unwrap().1 != b"lost")
    );
    assert!(c.failed(lost));
    assert!(!c.committed(lost));
    c.assert_equal();
    assert_eq!(c.states[&1].applied_batches()[0].1, b"winner");
}

#[test]
fn isolated_old_leader_cannot_commit_and_restores() {
    let mut c = Cluster::new("isolated-leader", 3, 4);
    c.elect(1);
    c.isolate(1);
    c.set_faults(0, 100, true);
    let lost = c.propose(1, b"lost").unwrap();
    assert!(!c.committed(lost));
    assert!(!c.failed(lost));
    c.elect(2);
    let w = c.propose(2, b"winner").unwrap();
    assert!(c.committed(w));
    c.heal();
    assert_eq!(c.nodes[&1].role().state, StateRole::Follower);
    assert!(c.failed(lost));
    assert!(!c.committed(lost));
    c.assert_equal();
    assert_eq!(c.states[&1].applied_batches()[0].1, b"winner");
}

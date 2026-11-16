//! Idle groups quiesce (GD-0075 R5-17b): an idle, fully replicated group stops
//! ticking and heartbeating; a proposal or message wakes it; a quiesced follower
//! learns of its leader's death only from node liveness, then wakes and campaigns.
use crate::common::Cluster;
use raft::StateRole;

fn quiesced(c: &Cluster) -> Vec<u64> {
    c.nodes
        .iter()
        .filter(|(_, n)| n.is_quiesced())
        .map(|(&id, _)| id)
        .collect()
}

fn quiet_cluster(name: &str) -> Cluster {
    let mut c = Cluster::new(name, 3, 5);
    c.enable_quiescence();
    c.elect(1);
    let waiter = c.propose(1, b"first").unwrap();
    c.tick_all(10);
    assert!(c.committed(waiter));
    assert_eq!(quiesced(&c), [1, 2, 3]);
    c
}

#[test]
fn quiescence_is_off_unless_enabled() {
    let mut c = Cluster::new("quiesce-off", 3, 5);
    c.elect(1);
    c.tick_all(50);
    assert!(quiesced(&c).is_empty());
}

#[test]
fn an_idle_group_quiesces_and_then_sends_nothing() {
    let mut c = quiet_cluster("quiesce-idle");
    let (delivered, commits): (u64, Vec<u64>) = (
        c.delivered,
        c.nodes.values().map(|n| n.log().durable_commits()).collect(),
    );
    c.tick_all(200);
    assert_eq!(c.delivered, delivered, "a quiesced group exchanged messages");
    let after: Vec<u64> = c.nodes.values().map(|n| n.log().durable_commits()).collect();
    assert_eq!(after, commits);
    assert_eq!(c.leader(), Some(1));
    assert_eq!(quiesced(&c), [1, 2, 3]);
}

#[test]
fn a_proposal_wakes_the_group_and_it_quiesces_again() {
    let mut c = quiet_cluster("quiesce-wake");
    let waiter = c.propose(1, b"second").unwrap();
    assert!(quiesced(&c).is_empty(), "the append woke every replica");
    c.tick_all(2);
    assert!(c.committed(waiter));
    c.tick_all(10);
    assert_eq!(quiesced(&c), [1, 2, 3]);
    c.assert_equal();
}

#[test]
fn followers_elect_a_new_leader_once_liveness_reports_the_leader_dead() {
    let mut c = quiet_cluster("quiesce-leader-dies");
    c.crash(1);
    // Without a liveness report nobody notices: quiesced followers do not tick.
    c.tick_all(100);
    assert_eq!(c.leader(), None);
    for id in [2, 3] {
        c.nodes.get_mut(&id).unwrap().node_liveness(1, false).unwrap();
    }
    // Both followers campaign at once, so the first vote may split: allow two
    // election timeouts (10 + id ticks each) after the report.
    for _ in 0..30 {
        c.pump();
        if c.leader().is_some() {
            break;
        }
        c.tick_all(1);
    }
    let leader = c.leader().expect("a new leader within two election timeouts");
    assert!([2, 3].contains(&leader));
    let waiter = c.propose(leader, b"after failover").unwrap();
    c.tick_all(2);
    assert!(c.committed(waiter));
}

#[test]
fn a_quiesced_leader_wakes_on_a_peer_liveness_change() {
    let mut c = quiet_cluster("quiesce-leader-liveness");
    c.nodes.get_mut(&1).unwrap().node_liveness(3, false).unwrap();
    assert!(!c.nodes[&1].is_quiesced());
    assert_eq!(c.nodes[&1].role().state, StateRole::Leader);
    // A follower ignores a change of a node that is not its leader.
    c.nodes.get_mut(&2).unwrap().node_liveness(3, false).unwrap();
    assert!(c.nodes[&2].is_quiesced());
    // While node 3 is reported dead the leader stays awake (it keeps
    // heartbeating, so node 3 is caught up the moment it is back).
    c.tick_all(20);
    assert!(!c.nodes[&1].is_quiesced());
    // Node 3 alive again: the group quiesces once idle.
    for id in [1, 2] {
        c.nodes.get_mut(&id).unwrap().node_liveness(3, true).unwrap();
    }
    c.tick_all(12);
    assert_eq!(quiesced(&c), [1, 2, 3]);
}

#[test]
fn a_late_quiesce_heartbeat_from_a_dead_leader_does_not_quiesce() {
    let mut c = Cluster::new("quiesce-late", 3, 5);
    c.enable_quiescence();
    c.elect(1);
    let waiter = c.propose(1, b"first").unwrap();
    c.tick_all(2);
    assert!(c.committed(waiter));
    // Liveness reports the leader dead before its quiesce heartbeat arrives.
    for id in [2, 3] {
        c.nodes.get_mut(&id).unwrap().node_liveness(1, false).unwrap();
    }
    c.tick_all(10);
    assert!(!c.nodes[&2].is_quiesced() && !c.nodes[&3].is_quiesced());
    // Alive again: after a write wakes the group (the followers may also have
    // campaigned meanwhile, as they would against a really dead leader), the
    // group quiesces as usual.
    for id in [2, 3] {
        c.nodes.get_mut(&id).unwrap().node_liveness(1, true).unwrap();
    }
    for _ in 0..60 {
        if quiesced(&c) == [1, 2, 3] {
            return;
        }
        if let Some(leader) = c.leader() {
            c.propose(leader, b"wake").unwrap();
        }
        c.tick_all(10);
    }
    panic!("group did not quiesce again: {:?}", quiesced(&c));
}

#[test]
fn a_held_leader_does_not_quiesce() {
    let mut c = Cluster::new("quiesce-hold", 3, 5);
    c.enable_quiescence();
    c.elect(1);
    c.nodes.get_mut(&1).unwrap().hold_quiescence(true);
    c.tick_all(20);
    assert!(quiesced(&c).is_empty());
    c.nodes.get_mut(&1).unwrap().hold_quiescence(false);
    c.tick_all(10);
    assert_eq!(quiesced(&c), [1, 2, 3]);
}

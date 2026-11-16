//! Idle groups make no durable log commits (GD-0075 R5-17a): a heartbeat-only
//! Ready has no entries and no hard state, so `LogStore::persist` skips it.
use crate::common::Cluster;

fn durable_commits(c: &Cluster) -> Vec<u64> {
    c.nodes.values().map(|n| n.log().durable_commits()).collect()
}

#[test]
fn an_idle_group_makes_no_durable_log_commits() {
    let mut c = Cluster::new("idle-commits", 3, 7);
    c.elect(1);
    let waiter = c.propose(1, b"one batch").unwrap();
    c.tick_all(5);
    assert!(c.committed(waiter));
    let before = durable_commits(&c);
    assert!(before.iter().all(|&n| n > 0), "{before:?}");
    // Many heartbeat rounds: messages flow, nothing new is written.
    c.tick_all(100);
    assert_eq!(c.leader(), Some(1));
    assert_eq!(durable_commits(&c), before);
    // The next write commits again.
    let waiter = c.propose(1, b"two").unwrap();
    c.tick_all(2);
    assert!(c.committed(waiter));
    assert!(durable_commits(&c).iter().zip(&before).all(|(a, b)| a > b));
}

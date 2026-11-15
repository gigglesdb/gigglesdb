use crate::common::Cluster;
use giggles_replication::membership::{AdminCommand, PeerRecord};
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeType, ConfChangeV2, ConfState, Message, MessageType,
};

fn conf(c: &Cluster, id: u64) -> ConfState {
    let mut cs = c.nodes[&id].conf_state();
    cs.voters.sort_unstable();
    cs.voters_outgoing.sort_unstable();
    cs.learners.sort_unstable();
    cs.learners_next.sort_unstable();
    cs
}
fn expected(c: &mut Cluster) {
    let cc = c.nodes[&2].membership().expected_conf_change().unwrap();
    let w = c.propose_conf_change(2, cc).unwrap();
    assert!(
        c.committed(w),
        "configuration waiter {w}: {:?}",
        c.refused(w)
    );
}
fn admin(c: &mut Cluster, cmd: AdminCommand) {
    let w = c.propose_admin(2, &cmd).unwrap();
    assert!(c.committed(w), "admin waiter {w}: {:?}", c.refused(w));
}
fn joint(name: &str) -> Cluster {
    let mut c = Cluster::new(name, 3, 7404);
    c.elect(2);
    for n in 0..4 {
        c.propose(2, &[n]).unwrap();
    }
    assert!(c.nodes[&2].reachable_voters().contains(&3));
    assert!(c.nodes[&3].reachable_voters().is_empty());
    c.snapshot(2);
    c.crash(1);
    admin(
        &mut c,
        AdminCommand::Enrol {
            lost: 1,
            peer: PeerRecord {
                id: 4,
                address: "n4".into(),
                fingerprint: "4".repeat(64),
            },
        },
    );
    c.join(4);
    expected(&mut c);
    // No pre-enrol snapshot may bootstrap the fresh node.
    assert_eq!(c.nodes[&4].applied_index(), 0);
    let snapshot = c.snapshot(2);
    for _ in 0..100 {
        if c.nodes[&4].applied_index() == c.nodes[&2].applied_index() {
            break;
        }
        c.tick_all(1);
    }
    assert_eq!(c.nodes[&4].applied_index(), c.nodes[&2].applied_index());
    assert_eq!(c.nodes[&4].log().snapshot_meta().unwrap().0, snapshot.index);
    assert_eq!(conf(&c, 4).learners, vec![4]);
    assert!(!conf(&c, 4).voters.contains(&4));
    let term = c.nodes[&4].role().term;
    c.nodes.get_mut(&4).unwrap().campaign().unwrap();
    c.tick_all(24);
    assert_eq!(c.nodes[&4].role().term, term);
    assert_eq!(c.nodes[&4].role().leader, 2);
    let matched = c.nodes[&2].peer_matched(4).unwrap();
    admin(&mut c, AdminCommand::CaughtUp { node: 4, matched });
    expected(&mut c);
    for id in [2, 3, 4] {
        let cs = conf(&c, id);
        assert_eq!(
            cs.voters
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            [2, 3, 4].into()
        );
        assert_eq!(
            cs.voters_outgoing
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            [1, 2, 3].into()
        );
    }
    c
}
fn finish(c: &mut Cluster) {
    expected(c);
    for id in [2, 3, 4] {
        let cs = conf(c, id);
        assert_eq!(
            cs.voters
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            [2, 3, 4].into()
        );
        assert!(cs.voters_outgoing.is_empty());
    }
    admin(c, AdminCommand::Retire { node: 1 });
    for node in c.nodes.values() {
        assert!(node.membership().is_retired(1));
        assert!(node.membership().active.is_none());
    }
    let w = c.propose(2, b"after replacement").unwrap();
    assert!(c.committed(w));
    c.assert_equal();
}
#[test]
fn full_replacement_in_process() {
    let mut c = joint("membership-full");
    finish(&mut c);
}
#[test]
fn joint_needs_both_majorities() {
    let mut c = joint("membership-majorities");
    c.isolate(3);
    let w = c.propose(2, b"blocked by old majority").unwrap();
    assert!(!c.committed(w));
    // New voters 2 and 4 are a majority; old voters have only 2.
    c.tick_all(4);
    assert!(!c.committed(w));
    c.heal();
    assert!(c.committed(w));
    finish(&mut c);
}
#[test]
fn out_of_order_changes_are_noops_everywhere() {
    let mut c = Cluster::new("membership-order", 3, 7405);
    c.elect(2);
    let before = conf(&c, 2);
    let mut cc = ConfChangeV2::default();
    cc.mut_changes().push(ConfChangeSingle {
        node_id: 4,
        change_type: ConfChangeType::AddLearnerNode,
        ..Default::default()
    });
    let w = c.propose_conf_change(2, cc).unwrap();
    assert!(c.refused(w).is_some());
    assert!(!c.committed(w));
    assert!(!c.failed(w));
    let w = c
        .propose_admin(
            2,
            &AdminCommand::CaughtUp {
                node: 4,
                matched: 1,
            },
        )
        .unwrap();
    assert!(c.refused(w).is_some());
    assert!(!c.committed(w));
    assert!(!c.failed(w));
    for (&id, node) in &c.nodes {
        assert_eq!(conf(&c, id), before);
        assert_eq!(node.log().conf_index().unwrap(), 0);
        assert!(node.membership().active.is_none());
    }
}
#[test]
fn restart_preserves_configuration() {
    let mut c = joint("membership-restart");
    let before = conf(&c, 2);
    let membership = c.nodes[&2].membership().clone();
    for id in [2, 3, 4] {
        c.crash(id);
    }
    for id in [2, 3, 4] {
        c.restart(id);
        assert_eq!(conf(&c, id), before);
        assert_eq!(c.nodes[&id].membership(), &membership);
    }
    c.elect(2);
    finish(&mut c);
}
#[test]
fn retired_peer_is_rejected() {
    let mut c = joint("membership-retired");
    finish(&mut c);
    let term = c.nodes[&2].role().term;
    assert!(
        c.nodes
            .get_mut(&2)
            .unwrap()
            .step(
                1,
                Message {
                    from: 1,
                    to: 2,
                    term,
                    msg_type: MessageType::MsgHeartbeat,
                    ..Default::default()
                }
            )
            .is_err()
    );
}

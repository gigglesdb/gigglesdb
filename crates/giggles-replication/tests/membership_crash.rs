use crate::common::Cluster;
use giggles_replication::membership::{AdminCommand, CATCH_UP_DISTANCE, Next, PeerRecord};
use raft::eraftpb::ConfState;

pub(super) const ADMIN: &[&str] = &["node.admin.before", "node.admin.after"];

fn conf(c: &Cluster, id: u64) -> ConfState {
    let mut conf = c.nodes[&id].conf_state();
    conf.voters.sort_unstable();
    conf.voters_outgoing.sort_unstable();
    conf.learners.sort_unstable();
    conf.learners_next.sort_unstable();
    conf
}

fn peer() -> PeerRecord {
    PeerRecord {
        id: 4,
        address: "n4".into(),
        fingerprint: "4".repeat(64),
    }
}

fn leader(c: &mut Cluster) -> u64 {
    if c.leader().is_none() {
        let id = [2, 3]
            .into_iter()
            .find(|id| c.nodes.contains_key(id))
            .unwrap();
        c.elect(id);
    }
    let id = c.leader().unwrap();
    assert_ne!(id, 1);
    id
}

// Exactly one persisted transition, deriving all progress from the current leader.
fn advance(c: &mut Cluster, lost: u64, new: PeerRecord) -> bool {
    let id = leader(c);
    let membership = c.nodes[&id].membership();
    if membership.active.is_none() && membership.is_retired(lost) {
        return false;
    }
    let waiter = match membership.next() {
        Next::Idle => c.propose_admin(id, &AdminCommand::Enrol { lost, peer: new }),
        Next::AddLearner | Next::EnterJoint | Next::LeaveJoint => {
            let cc = membership.expected_conf_change().unwrap();
            c.propose_conf_change(id, cc)
        }
        Next::CatchUp { node, added_at } => {
            let latest = c.nodes[&id].log().snapshots().unwrap().latest();
            if latest.is_none_or(|m| m.index < added_at) {
                assert!(c.snapshot(id).index >= added_at);
            }
            let mut caught_up = None;
            for _ in 0..100 {
                c.tick_all(1);
                assert_eq!(c.leader(), Some(id));
                if let Some(matched) = c.nodes[&id].peer_matched(node)
                    && matched >= added_at
                    && c.nodes[&id].last_index().saturating_sub(matched) <= CATCH_UP_DISTANCE
                {
                    caught_up = Some(matched);
                    break;
                }
            }
            let matched = caught_up.expect("learner did not catch up");
            c.propose_admin(id, &AdminCommand::CaughtUp { node, matched })
        }
        Next::Retire { node } => c.propose_admin(id, &AdminCommand::Retire { node }),
    }
    .unwrap();
    assert!(c.committed(waiter), "step refused: {:?}", c.refused(waiter));
    true
}

fn assert_writes(c: &Cluster, expected: &[Vec<u8>], ids: &[u64]) {
    for id in ids {
        let actual: Vec<_> = c.states[id]
            .applied_batches()
            .into_iter()
            .map(|(_, payload)| payload)
            .collect();
        assert_eq!(actual, expected, "acknowledged writes on node {id}");
    }
}

fn write(c: &mut Cluster, expected: &mut Vec<Vec<u8>>) {
    let id = leader(c);
    let payload = format!("acknowledged batch {}", expected.len()).into_bytes();
    let waiter = c.propose(id, &payload).unwrap();
    assert!(c.committed(waiter));
    expected.push(payload);
    assert_writes(c, expected, &[2, 3]);
}

fn setup(name: &str) -> (Cluster, Vec<Vec<u8>>) {
    let mut c = Cluster::new(name, 3, 7406);
    c.elect(2);
    let mut expected = Vec::new();
    write(&mut c, &mut expected);
    c.crash(1);
    c.join(4);
    (c, expected)
}

fn finish(c: &mut Cluster, expected: &mut Vec<Vec<u8>>) {
    for _ in 0..7 {
        if !advance(c, 1, peer()) {
            write(c, expected);
            for id in [2, 3, 4] {
                let node = &c.nodes[&id];
                let mut conf = node.conf_state();
                conf.voters.sort_unstable();
                assert_eq!(conf.voters, vec![2, 3, 4]);
                assert!(conf.voters_outgoing.is_empty());
                assert!(conf.learners.is_empty());
                assert!(conf.learners_next.is_empty());
                assert!(node.membership().is_retired(1));
                assert!(node.membership().active.is_none());
            }
            assert_writes(c, expected, &[2, 3, 4]);
            c.assert_equal();
            return;
        }
        write(c, expected);
    }
    panic!("replacement did not finish in six transitions");
}

#[test]
fn crash_inside_admin_apply_at_every_step() {
    use crate::common::CLUSTER;
    use giggles_replication::{failpoint, membership::Membership};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    // The shared harness uses one cluster identity. A child running only this test
    // keeps its failpoints from crashing concurrently running membership tests.
    const CHILD: &str = "GIGGLES_ADMIN_CRASH_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "membership_crash::crash_inside_admin_apply_at_every_step",
                "--exact",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "admin crash scenarios failed: {status}");
        return;
    }
    for (k, next) in [
        (0, Next::AddLearner),
        (2, Next::EnterJoint),
        (5, Next::Idle),
    ] {
        for &point in ADMIN {
            for victim in [2, 3] {
                let (mut c, mut expected) = setup(&format!("admin-crash-{k}-{point}-{victim}"));
                for _ in 0..k {
                    assert!(advance(&mut c, 1, peer()));
                    write(&mut c, &mut expected);
                }
                let before = c.nodes[&victim].membership().clone();
                let conf_before = conf(&c, victim);
                let conf_index = c.nodes[&victim].log().conf_index().unwrap();
                let fired = failpoint::fired(CLUSTER).len();
                failpoint::arm_node(CLUSTER, point, 1, victim);
                let crash = catch_unwind(AssertUnwindSafe(|| advance(&mut c, 1, peer())));
                failpoint::disarm(CLUSTER);
                let panic = crash.expect_err("admin entry did not crash");
                assert_eq!(
                    panic.downcast_ref::<String>().map(String::as_str),
                    Some(format!("failpoint crash: {point}").as_str()),
                    "k={k}, point={point}, node={victim}"
                );
                assert_eq!(failpoint::fired(CLUSTER)[fired..], [(point, Some(victim))]);
                assert_eq!(failpoint::hits(CLUSTER, point), 1);
                let after = c.nodes[&victim].membership().clone();
                assert_ne!(after, before);
                assert_eq!(after.next(), next);
                assert_eq!(after.completed, u64::from(k == 5));
                let persisted = if point == "node.admin.before" {
                    &before
                } else {
                    &after
                };
                let log = c.nodes[&victim].log();
                let disk = log
                    .membership()
                    .unwrap()
                    .map_or_else(Membership::default, |bytes| {
                        Membership::decode(&bytes).unwrap()
                    });
                assert_eq!(&disk, persisted, "k={k}, point={point}, node={victim}");
                if point == "node.admin.before" {
                    assert_eq!(log.conf_index().unwrap(), conf_index);
                } else {
                    assert!(log.conf_index().unwrap() > conf_index);
                }
                c.crash(victim);
                c.restart(victim);
                // Replica::new loads the durable membership without replaying entries.
                assert_eq!(c.nodes[&victim].membership(), persisted);
                assert_eq!(conf(&c, victim), conf_before);
                c.pump();
                leader(&mut c);
                c.tick_all(2);
                for id in [2, 3] {
                    assert_eq!(c.nodes[&id].membership(), &after, "replay on node {id}");
                }
                assert_writes(&c, &expected, &[2, 3]);
                finish(&mut c, &mut expected);
                for id in [2, 3, 4] {
                    assert_eq!(c.nodes[&id].membership().completed, 1);
                    assert_eq!(
                        c.nodes[&id]
                            .membership()
                            .retired
                            .iter()
                            .copied()
                            .collect::<Vec<_>>(),
                        vec![1]
                    );
                    let saved = c.nodes[&id].membership().clone();
                    c.crash(id);
                    c.restart(id);
                    assert_eq!(
                        c.nodes[&id].membership(),
                        &saved,
                        "final restart on node {id}"
                    );
                }
                assert_writes(&c, &expected, &[2, 3, 4]);
            }
        }
    }
}

#[test]
fn crash_all_at_every_step() {
    for k in 0..6 {
        let (mut c, mut expected) = setup(&format!("membership-crash-all-{k}"));
        for _ in 0..k {
            assert!(advance(&mut c, 1, peer()));
            write(&mut c, &mut expected);
        }
        let saved: Vec<_> = [2, 3, 4]
            .into_iter()
            .map(|id| (id, c.nodes[&id].membership().clone(), conf(&c, id)))
            .collect();
        for id in [2, 3, 4] {
            c.crash(id);
        }
        for (id, membership, saved_conf) in saved {
            c.restart(id);
            assert_eq!(c.nodes[&id].membership(), &membership, "k={k}, node={id}");
            assert_eq!(conf(&c, id), saved_conf, "k={k}, node={id}");
        }
        leader(&mut c);
        assert_writes(&c, &expected, &[2, 3]);
        write(&mut c, &mut expected);
        finish(&mut c, &mut expected);
    }
}

#[test]
fn leader_change_at_every_step() {
    for k in 0..6 {
        let (mut c, mut expected) = setup(&format!("membership-leader-change-{k}"));
        for _ in 0..k {
            assert!(advance(&mut c, 1, peer()));
            write(&mut c, &mut expected);
        }
        let old = leader(&mut c);
        let other = if old == 2 { 3 } else { 2 };
        let membership = c.nodes[&old].membership().clone();
        c.crash(old);
        // Both surviving old voters are needed for quorum before LeaveJoint.
        c.restart(old);
        assert_eq!(c.nodes[&old].membership(), &membership, "k={k}");
        c.elect(other);
        assert_eq!(c.leader(), Some(other), "k={k}");
        assert_eq!(c.nodes[&other].membership(), &membership, "k={k}");
        assert_writes(&c, &expected, &[2, 3]);
        write(&mut c, &mut expected);
        finish(&mut c, &mut expected);
    }
}

#[test]
fn crash_joiner_before_snapshot() {
    let (mut c, mut expected) = setup("membership-crash-joiner");
    assert!(advance(&mut c, 1, peer()));
    write(&mut c, &mut expected);
    assert!(advance(&mut c, 1, peer()));
    assert_eq!(c.nodes[&4].applied_index(), 0);
    assert_eq!(c.nodes[&4].log().snapshot_meta().unwrap().0, 0);
    c.crash(4);
    c.restart(4);
    assert!(c.nodes[&4].conf_state().voters.is_empty());
    assert_eq!(c.nodes[&4].applied_index(), 0);
    write(&mut c, &mut expected);
    finish(&mut c, &mut expected);
}

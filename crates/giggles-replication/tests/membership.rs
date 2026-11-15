use giggles_replication::membership::*;
use raft::eraftpb::{ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState};
use std::collections::BTreeSet;
fn peer(id: u64) -> PeerRecord {
    PeerRecord {
        id,
        address: format!("node-{id}:1234"),
        fingerprint: format!("{id:064x}"),
    }
}
fn conf() -> ConfState {
    ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    }
}
fn enrol() -> AdminCommand {
    AdminCommand::Enrol {
        lost: 1,
        peer: peer(4),
    }
}
fn roundtrip(m: &Membership) {
    let bytes = m.encode();
    assert_eq!(Membership::decode(&bytes).unwrap(), *m);
    for n in 0..bytes.len() {
        assert!(Membership::decode(&bytes[..n]).is_err(), "prefix {n}");
    }
    let mut extra = bytes;
    extra.push(0);
    assert!(Membership::decode(&extra).is_err());
}
fn rejected(m: &mut Membership, cmd: AdminCommand, c: &ConfState, index: u64) {
    let before = m.clone();
    assert!(m.apply_admin(&cmd, c, index).is_err());
    assert_eq!(*m, before);
}
fn advance(m: &mut Membership, index: u64) -> ConfChangeV2 {
    let cc = m.expected_conf_change().unwrap();
    assert!(m.accept_conf_change(&cc, index));
    roundtrip(m);
    cc
}
#[test]
fn full_sequence_and_retries() {
    let mut m = Membership::default();
    let c = conf();
    m.peers.insert(1, peer(1));
    assert_eq!(m.next(), Next::Idle);
    roundtrip(&m);
    m.apply_admin(&enrol(), &c, 10).unwrap();
    assert_eq!(m.next(), Next::AddLearner);
    roundtrip(&m);
    let before = m.clone();
    m.apply_admin(&enrol(), &ConfState::default(), 99).unwrap();
    assert_eq!(m, before);
    let cc = advance(&mut m, 11);
    assert_eq!(cc.changes[0].node_id, 4);
    assert_eq!(
        cc.changes[0].get_change_type(),
        ConfChangeType::AddLearnerNode
    );
    assert_eq!(
        m.next(),
        Next::CatchUp {
            node: 4,
            added_at: 11
        }
    );
    m.apply_admin(
        &AdminCommand::CaughtUp {
            node: 4,
            matched: 11,
        },
        &c,
        12,
    )
    .unwrap();
    assert_eq!(m.next(), Next::EnterJoint);
    roundtrip(&m);
    let before = m.clone();
    m.apply_admin(
        &AdminCommand::CaughtUp {
            node: 4,
            matched: 0,
        },
        &c,
        0,
    )
    .unwrap();
    assert_eq!(m, before);
    let cc = advance(&mut m, 13);
    assert_eq!(cc.get_transition(), ConfChangeTransition::Explicit);
    assert_eq!(
        cc.changes
            .iter()
            .map(|c| (c.node_id, c.get_change_type()))
            .collect::<Vec<_>>(),
        vec![
            (4, ConfChangeType::AddNode),
            (1, ConfChangeType::RemoveNode)
        ]
    );
    assert_eq!(m.next(), Next::LeaveJoint);
    let cc = advance(&mut m, 14);
    assert!(cc.changes.is_empty());
    assert_eq!(cc.get_transition(), ConfChangeTransition::Auto);
    assert_eq!(m.next(), Next::Retire { node: 1 });
    m.apply_admin(&AdminCommand::Retire { node: 1 }, &c, 15)
        .unwrap();
    assert_eq!(m.retired, BTreeSet::from([1]));
    assert!(m.is_retired(1));
    assert!(!m.is_retired(4));
    assert_eq!(
        m.retired_fingerprints,
        BTreeSet::from([peer(1).fingerprint])
    );
    assert!(!m.peers.contains_key(&1));
    assert!(m.active.is_none());
    assert_eq!(m.completed, 1);
    assert_eq!(m.next(), Next::Idle);
    roundtrip(&m);
}
#[test]
fn ordering_and_conf_matching() {
    let mut m = Membership::default();
    let c = conf();
    rejected(&mut m, AdminCommand::Retire { node: 1 }, &c, 1);
    rejected(
        &mut m,
        AdminCommand::CaughtUp {
            node: 4,
            matched: 1,
        },
        &c,
        2,
    );
    m.apply_admin(&enrol(), &c, 10).unwrap();
    let add = m.expected_conf_change().unwrap();
    let mut probe = m.clone();
    advance(&mut probe, 11);
    probe
        .apply_admin(
            &AdminCommand::CaughtUp {
                node: 4,
                matched: 11,
            },
            &c,
            12,
        )
        .unwrap();
    let joint = advance(&mut probe, 13);
    let leave = advance(&mut probe, 14);
    let mut states = vec![Membership::default(), m.clone()];
    advance(&mut m, 11);
    states.push(m.clone());
    for matched in [0, 10, 12, 13] {
        rejected(&mut m, AdminCommand::CaughtUp { node: 4, matched }, &c, 12);
    }
    rejected(
        &mut m,
        AdminCommand::CaughtUp {
            node: 5,
            matched: 11,
        },
        &c,
        12,
    );
    m.apply_admin(
        &AdminCommand::CaughtUp {
            node: 4,
            matched: 11,
        },
        &c,
        12,
    )
    .unwrap();
    states.push(m.clone());
    advance(&mut m, 13);
    states.push(m.clone());
    advance(&mut m, 14);
    states.push(m);
    for mut state in states {
        if state.next() != (Next::Retire { node: 1 }) {
            rejected(&mut state, AdminCommand::Retire { node: 1 }, &c, 20);
        }
        rejected(&mut state, AdminCommand::Retire { node: 9 }, &c, 20);
        if !matches!(state.next(), Next::CatchUp { .. } | Next::EnterJoint) {
            rejected(
                &mut state,
                AdminCommand::CaughtUp {
                    node: 4,
                    matched: 11,
                },
                &c,
                20,
            );
        }
        for cc in [&add, &joint, &leave] {
            if state.expected_conf_change().as_ref() == Some(cc) {
                continue;
            }
            let before = state.clone();
            assert!(!state.accept_conf_change(cc, 20));
            assert_eq!(state, before);
        }
        if let Some(expected) = state.expected_conf_change() {
            let mut bad = expected.clone();
            bad.set_transition(ConfChangeTransition::Implicit);
            let before = state.clone();
            assert!(!state.accept_conf_change(&bad, 20));
            assert_eq!(state, before);
            if !expected.changes.is_empty() {
                let mut bad = expected.clone();
                bad.changes[0].node_id = 99;
                assert!(!state.accept_conf_change(&bad, 20));
                assert_eq!(state, before);
                let mut bad = expected.clone();
                bad.changes[0].set_change_type(ConfChangeType::RemoveNode);
                assert!(!state.accept_conf_change(&bad, 20));
                assert_eq!(state, before);
            }
            if expected.changes.len() == 2 {
                let mut bad = expected.clone();
                bad.changes.reverse();
                assert!(!state.accept_conf_change(&bad, 20));
                assert_eq!(state, before);
            }
            let mut with_context = expected;
            with_context.context = b"ignored".to_vec().into();
            assert!(state.accept_conf_change(&with_context, 20));
        }
    }
}
#[test]
fn enrol_refusals() {
    let c = conf();
    let base = Membership::default();
    let mut retired = base.clone();
    retired.retired.insert(4);
    let mut old_fp = base.clone();
    old_fp.retired_fingerprints.insert(peer(4).fingerprint);
    let mut duplicate = base.clone();
    let mut p = peer(9);
    p.fingerprint = peer(4).fingerprint;
    duplicate.peers.insert(9, p);
    let mut existing = base.clone();
    existing.peers.insert(4, peer(4));
    let mut joint = c.clone();
    joint.voters_outgoing = vec![1, 2, 3];
    let mut learner = c.clone();
    learner.learners = vec![8];
    for (mut m, cs, cmd) in [
        (retired, c.clone(), enrol()),
        (old_fp, c.clone(), enrol()),
        (duplicate, c.clone(), enrol()),
        (existing, c.clone(), enrol()),
        (base.clone(), joint, enrol()),
        (base.clone(), learner, enrol()),
        (
            base.clone(),
            c.clone(),
            AdminCommand::Enrol {
                lost: 9,
                peer: peer(4),
            },
        ),
    ] {
        rejected(&mut m, cmd, &cs, 1);
    }
    for id in [0, 1, 2, 3] {
        rejected(
            &mut base.clone(),
            AdminCommand::Enrol {
                lost: 1,
                peer: peer(id),
            },
            &c,
            1,
        );
    }
    for fp in [
        "".to_owned(),
        "a".repeat(63),
        "A".repeat(64),
        "g".repeat(64),
        "é".repeat(32),
    ] {
        let mut p = peer(4);
        p.fingerprint = fp;
        rejected(
            &mut base.clone(),
            AdminCommand::Enrol { lost: 1, peer: p },
            &c,
            1,
        );
    }
    for address in [String::new(), "a".repeat(256), "é".repeat(128)] {
        let mut p = peer(4);
        p.address = address;
        rejected(
            &mut base.clone(),
            AdminCommand::Enrol { lost: 1, peer: p },
            &c,
            1,
        );
    }
    let mut m = base;
    m.apply_admin(&enrol(), &c, 1).unwrap();
    rejected(
        &mut m,
        AdminCommand::Enrol {
            lost: 1,
            peer: peer(5),
        },
        &c,
        2,
    );
    rejected(
        &mut m,
        AdminCommand::Enrol {
            lost: 2,
            peer: peer(4),
        },
        &c,
        2,
    );
}
#[test]
fn safety_counts_union_once() {
    let mut c = conf();
    assert!(check_safe(&c, 1, 2, &BTreeSet::from([2, 3])).is_ok());
    assert_eq!(
        check_safe(&c, 1, 2, &BTreeSet::from([2]))
            .unwrap_err()
            .to_string(),
        "would leave 1 reachable voter; at least 2 are required"
    );
    assert_eq!(
        check_safe(&c, 1, 1, &BTreeSet::from([2, 3]))
            .unwrap_err()
            .to_string(),
        "lost voter 1 is the current leader"
    );
    c.voters = vec![1, 2];
    c.voters_outgoing = vec![1, 2, 3];
    assert!(check_safe(&c, 1, 2, &BTreeSet::from([2, 3])).is_ok());
    assert!(check_safe(&c, 1, 2, &BTreeSet::from([1, 2, 9])).is_err());
}
#[test]
fn command_codec_and_bad_tags() {
    for cmd in [
        enrol(),
        AdminCommand::CaughtUp {
            node: 4,
            matched: 11,
        },
        AdminCommand::Retire { node: 1 },
    ] {
        let bytes = cmd.encode();
        assert_eq!(AdminCommand::decode(&bytes).unwrap(), cmd);
        for n in 0..bytes.len() {
            assert!(AdminCommand::decode(&bytes[..n]).is_err());
        }
        let mut b = bytes.clone();
        b.push(0);
        assert!(AdminCommand::decode(&b).is_err());
        let mut b = bytes;
        b[0] = 2;
        assert!(AdminCommand::decode(&b).is_err());
    }
    assert!(AdminCommand::decode(&[1, 0]).is_err());
    let mut b = Membership::default().encode();
    b[0] = 9;
    assert!(Membership::decode(&b).is_err());
    b[0] = 1;
    b[25] = 9;
    assert!(Membership::decode(&b).is_err());
    let mut m = Membership::default();
    m.apply_admin(&enrol(), &conf(), 1).unwrap();
    let mut b = m.encode();
    let step = b.len() - 17;
    b[step] = 99;
    assert!(Membership::decode(&b).is_err());
    let mut b = enrol().encode();
    b[20] = 255;
    assert!(AdminCommand::decode(&b).is_err());
}
#[test]
fn deterministic_replay_including_refusals() {
    let mut a = Membership::default();
    let mut b = a.clone();
    for index in 1..=10 {
        let cmd = match index {
            1 | 2 => Some(enrol()),
            3 => Some(AdminCommand::Retire { node: 1 }),
            5 => Some(AdminCommand::CaughtUp {
                node: 4,
                matched: 0,
            }),
            6 => Some(AdminCommand::CaughtUp {
                node: 4,
                matched: 4,
            }),
            9 | 10 => Some(AdminCommand::Retire { node: 1 }),
            _ => None,
        };
        if let Some(cmd) = cmd {
            assert_eq!(
                a.apply_admin(&cmd, &conf(), index),
                b.apply_admin(&cmd, &conf(), index)
            );
        } else {
            let cc = a.expected_conf_change().unwrap();
            assert_eq!(
                a.accept_conf_change(&cc, index),
                b.accept_conf_change(&cc, index)
            );
        }
        assert_eq!(a, b);
        roundtrip(&a);
    }
    assert_eq!(a.completed, 1);
}

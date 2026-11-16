//! Many Raft groups per node under seeded drop, duplicate and reorder faults
//! (GD-0075 R5-15; a four-group port of chaos.rs).
use crate::common::groups::Groups;
use raft::StateRole;
use std::collections::{BTreeMap, BTreeSet};

const GROUPS: u64 = 4;
const NODES: u64 = 3;
/// Fixed seeds for the four-group run; each covers two forced leader changes.
const SEEDS: [u64; 4] = [3, 11, 42, 2026];

// Separate from the harness RNG: changing the message schedule does not change
// which transfer or fault action a seed requests.
fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// A transfer tagged with its group, so a batch applied in the wrong group is caught.
fn transfer(group: u64, op_id: u64, from: u8, to: u8, amount: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(15);
    bytes.push(group as u8);
    bytes.extend_from_slice(&op_id.to_le_bytes());
    bytes.extend_from_slice(&[from, to]);
    bytes.extend_from_slice(&amount.to_le_bytes());
    bytes
}

// Replay one group's applied order. Insufficient funds means a no-op.
fn replay<T>(group: u64, batches: &[(T, Vec<u8>)], seed: u64) -> ([i64; 8], BTreeSet<u64>) {
    let mut balances = [100_i64; 8];
    let mut seen = BTreeSet::new();
    for (_, bytes) in batches {
        assert_eq!(bytes.len(), 15, "seed={seed} group={group}: malformed transfer");
        assert_eq!(
            u64::from(bytes[0]),
            group,
            "seed={seed}: a batch of another group was applied in group {group}"
        );
        let op = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        assert!(seen.insert(op), "seed={seed} group={group}: duplicate application {op}");
        let from = usize::from(bytes[9]);
        let to = usize::from(bytes[10]);
        assert!(from < 8 && to < 8, "seed={seed}: invalid account");
        let amount = i64::from(u32::from_le_bytes(bytes[11..].try_into().unwrap()));
        if balances[from] >= amount {
            balances[from] -= amount;
            balances[to] += amount;
        }
        assert!(balances.iter().all(|&balance| balance >= 0), "seed={seed}");
        assert_eq!(balances.iter().sum::<i64>(), 800, "seed={seed}");
    }
    (balances, seen)
}

/// Spread leadership: group g starts led by node (g - 1) % NODES + 1.
fn spread_leaders(c: &mut Groups) {
    for group in c.groups() {
        c.elect(group, (group - 1) % NODES + 1);
    }
}

#[test]
fn groups_elect_leaders_on_different_nodes_and_apply_independently() {
    let mut c = Groups::new("groups-basic", NODES, GROUPS, 1);
    spread_leaders(&mut c);
    let leaders: BTreeSet<u64> = c.groups().iter().filter_map(|&g| c.leader(g)).collect();
    assert_eq!(leaders.len(), NODES as usize, "leaders spread over every node");
    for group in c.groups() {
        let leader = c.leader(group).unwrap();
        let waiter = c.propose(group, leader, &transfer(group, group, 0, 1, 5)).unwrap();
        assert!(c.committed(waiter), "group {group}");
    }
    for group in c.groups() {
        c.assert_equal(group);
        let batches = c.batches(group, 1);
        assert_eq!(batches.len(), 1, "group {group} applied only its own batch");
        assert_eq!(batches[0].1, transfer(group, group, 0, 1, 5));
    }
}

#[test]
fn isolating_a_node_moves_only_the_leaderships_it_held() {
    let mut c = Groups::new("groups-isolate", NODES, GROUPS, 2);
    spread_leaders(&mut c);
    let before: BTreeMap<u64, u64> = c.groups().iter().map(|&g| (g, c.leader(g).unwrap())).collect();
    c.isolate(1);
    for _ in 0..60 {
        c.tick_all(1);
    }
    for (&group, &old) in &before {
        let now = (1..=NODES).find(|&n| n != 1 && c.role(group, n) == Some(StateRole::Leader));
        if old == 1 {
            assert!(now.is_some(), "group {group}: no replacement for node 1");
        } else {
            assert_eq!(now, Some(old), "group {group}: leadership on a healthy node moved");
        }
    }
    c.heal();
    for group in c.groups() {
        let leader = c.leader(group).expect("leader after heal");
        let waiter = c.propose(group, leader, &transfer(group, 99, 2, 3, 1)).unwrap();
        for _ in 0..50 {
            if c.committed(waiter) {
                break;
            }
            c.tick_all(1);
        }
        assert!(c.committed(waiter), "group {group}");
        c.assert_equal(group);
    }
}

#[test]
fn seeded_drop_duplicate_reorder_across_four_groups() {
    for seed in SEEDS {
        eprintln!("multi-group chaos seed={seed}");
        let mut c = Groups::new("groups-chaos", NODES, GROUPS, seed);
        spread_leaders(&mut c);
        c.set_faults(10, 10, true);
        let mut rng = seed;
        let mut waiters: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
        let mut submitted: BTreeMap<(u64, u64), Vec<u8>> = BTreeMap::new();
        let mut heal_at = None;
        let mut leader_changes = 0;
        let mut op = 0;
        for step in 0..160 {
            if heal_at == Some(step) {
                c.heal();
                c.set_faults(10, 10, true);
                heal_at = None;
            }
            // Two forced leader replacements: isolate the node leading group 1.
            if step == 30 || step == 100 {
                c.heal();
                for _ in 0..100 {
                    if c.leader(1).is_some() {
                        break;
                    }
                    c.tick_all(1);
                }
                c.set_faults(10, 10, true);
                let old = c.leader(1).expect("leader before forced partition");
                c.isolate(old);
                let mut replacement = None;
                for _ in 0..100 {
                    c.tick_all(1);
                    replacement = (1..=NODES)
                        .find(|&n| n != old && c.role(1, n) == Some(StateRole::Leader));
                    if replacement.is_some() {
                        break;
                    }
                }
                assert!(replacement.is_some(), "seed={seed} step={step}: no replacement leader");
                leader_changes += 1;
                heal_at = Some(step + 8);
                continue;
            }
            match random(&mut rng) % 10 {
                0..=4 => {
                    let group = 1 + random(&mut rng) % GROUPS;
                    if let Some(leader) = c.leader(group) {
                        op += 1;
                        let from = (random(&mut rng) % 8) as u8;
                        let to = ((u64::from(from) + 1 + random(&mut rng) % 7) % 8) as u8;
                        let amount = (1 + random(&mut rng) % 180) as u32;
                        let payload = transfer(group, op, from, to, amount);
                        if let Ok(waiter) = c.propose(group, leader, &payload) {
                            waiters.insert(waiter, (group, op));
                            submitted.insert((group, op), payload);
                        }
                    }
                }
                5..=6 => c.tick_all(2),
                7 if heal_at.is_none() => {
                    c.isolate(1 + random(&mut rng) % NODES);
                    heal_at = Some(step + 5);
                }
                8 if heal_at.is_none() => {
                    c.heal();
                    c.set_faults(10, 10, true);
                }
                9 => {
                    // Crash and restart any node: with leaders spread, every node
                    // usually leads some group, so this also kills leaders.
                    let node = 1 + random(&mut rng) % NODES;
                    c.crash(node);
                    c.restart(node);
                }
                _ => c.tick_all(1),
            }
        }
        assert_eq!(leader_changes, 2, "seed={seed}");
        c.heal();
        c.set_faults(0, 0, false);
        for group in c.groups() {
            for _ in 0..100 {
                if c.leader(group).is_some() {
                    break;
                }
                c.tick_all(1);
            }
            let leader = c.leader(group).expect("leader after bounded recovery");
            op += 1;
            let payload = transfer(group, op, 0, 0, 0);
            let waiter = c.propose(group, leader, &payload).expect("final no-op proposal");
            waiters.insert(waiter, (group, op));
            submitted.insert((group, op), payload);
            for _ in 0..100 {
                if c.committed(waiter) {
                    break;
                }
                c.tick_all(1);
            }
            assert!(c.committed(waiter), "seed={seed} group={group}: final no-op not committed");
        }
        let mut committed_ops = 0;
        for group in c.groups() {
            c.assert_equal(group);
            let batches = c.batches(group, 1);
            let (expected, seen) = replay(group, &batches, seed);
            committed_ops += seen.len();
            for (_, payload) in &batches {
                let id = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                assert_eq!(submitted.get(&(group, id)), Some(payload), "seed={seed} op={id}");
            }
            for node in 1..=NODES {
                let (actual, _) = replay(group, &c.batches(group, node), seed);
                assert_eq!(actual, expected, "seed={seed} group={group} node={node}");
            }
            for (&waiter, &(g, id)) in &waiters {
                if g != group {
                    continue;
                }
                assert!(
                    !(c.committed(waiter) && c.failed(waiter)),
                    "seed={seed} waiter={waiter}: committed and failed"
                );
                if c.committed(waiter) {
                    assert!(seen.contains(&id), "seed={seed} group={group}: missing op={id}");
                }
            }
            assert!(c.delivered(group) > 0, "seed={seed} group={group}: no traffic");
        }
        assert!(
            committed_ops > GROUPS as usize,
            "seed={seed}: workload committed beyond the final no-ops"
        );
    }
}

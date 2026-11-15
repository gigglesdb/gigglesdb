use crate::common::Cluster;
use raft::StateRole;
use std::collections::{BTreeMap, BTreeSet};

// Separate from the harness RNG: changing the message schedule does not change
// which transfer or fault action a seed requests.
fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn transfer(op_id: u64, from: u8, to: u8, amount: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(14);
    bytes.extend_from_slice(&op_id.to_le_bytes());
    bytes.extend_from_slice(&[from, to]);
    bytes.extend_from_slice(&amount.to_le_bytes());
    bytes
}

// Replay only the actual applied order, independently of Raft indexes, waiter
// decisions, and the workload generator. Insufficient funds means a no-op.
fn replay<T>(batches: &[(T, Vec<u8>)], seed: u64) -> ([i64; 8], BTreeSet<u64>) {
    let mut balances = [100_i64; 8];
    let mut seen = BTreeSet::new();
    for (_, bytes) in batches {
        assert_eq!(bytes.len(), 14, "seed={seed}: malformed transfer");
        let op = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        assert!(seen.insert(op), "seed={seed}: duplicate application {op}");
        let from = usize::from(bytes[8]);
        let to = usize::from(bytes[9]);
        assert!(from < 8 && to < 8, "seed={seed}: invalid account");
        let amount = i64::from(u32::from_le_bytes(bytes[10..].try_into().unwrap()));
        if balances[from] >= amount {
            balances[from] -= amount;
            balances[to] += amount;
        }
        assert!(balances.iter().all(|&balance| balance >= 0), "seed={seed}");
        assert_eq!(balances.iter().sum::<i64>(), 800, "seed={seed}");
    }
    (balances, seen)
}

#[test]
fn seeded_drop_duplicate_reorder_with_serial_oracle() {
    for seed in [1, 7, 42, 1337, 9001] {
        eprintln!("chaos seed={seed}");
        let mut c = Cluster::new("serial-oracle", 3, seed);
        c.elect(1);
        c.set_faults(10, 10, true);
        let mut rng = seed;
        let mut waiters = BTreeMap::new();
        let mut submitted = BTreeMap::new();
        let mut heal_at = None;
        let mut leader_changes = 0;
        for step in 0..200 {
            if heal_at == Some(step) {
                c.heal();
                c.set_faults(10, 10, true);
                heal_at = None;
            }
            // Guarantee two real leader replacements, rather than relying on
            // the random actions to happen to partition the current leader.
            if step == 40 || step == 120 {
                c.heal();
                c.set_faults(10, 10, true);
                let old = c.leader().expect("leader before forced partition");
                c.isolate(old);
                let mut replacement = None;
                for _ in 0..100 {
                    c.tick_all(1);
                    replacement = c.nodes.iter().find_map(|(&id, node)| {
                        (id != old && node.role().state == StateRole::Leader).then_some(id)
                    });
                    if replacement.is_some() {
                        break;
                    }
                }
                assert!(
                    replacement.is_some(),
                    "seed={seed} step={step}: no replacement leader"
                );
                leader_changes += 1;
                heal_at = Some(step + 8);
                continue;
            }
            match random(&mut rng) % 10 {
                0..=4 => {
                    if let Some(leader) = c.leader() {
                        let op = step as u64 + 1;
                        let from = (random(&mut rng) % 8) as u8;
                        let to = ((u64::from(from) + 1 + random(&mut rng) % 7) % 8) as u8;
                        let amount = (1 + random(&mut rng) % 180) as u32;
                        let payload = transfer(op, from, to, amount);
                        if let Ok(waiter) = c.propose(leader, &payload) {
                            waiters.insert(waiter, op);
                            submitted.insert(op, payload);
                        }
                    }
                }
                5..=6 => c.tick_all(2),
                7 if heal_at.is_none() => {
                    c.isolate(1 + random(&mut rng) % 3);
                    heal_at = Some(step + 5);
                }
                8 if heal_at.is_none() => {
                    c.heal();
                    c.set_faults(10, 10, true);
                }
                9 => {
                    let followers: Vec<_> = c
                        .nodes
                        .iter()
                        .filter_map(|(&id, node)| {
                            (node.role().state == StateRole::Follower).then_some(id)
                        })
                        .collect();
                    if !followers.is_empty() {
                        let id = followers[random(&mut rng) as usize % followers.len()];
                        c.crash(id);
                        c.restart(id);
                    }
                }
                _ => c.tick_all(1),
            }
        }
        assert_eq!(leader_changes, 2, "seed={seed}");
        c.heal();
        c.set_faults(0, 0, false);
        // Each tick/pump is itself bounded by the harness. Bound recovery too.
        for _ in 0..100 {
            c.tick_all(1);
            if c.leader().is_some() {
                break;
            }
        }
        let leader = c.leader().expect("leader after bounded recovery");
        let payload = transfer(201, 0, 0, 0);
        let final_waiter = c.propose(leader, &payload).expect("final no-op proposal");
        waiters.insert(final_waiter, 201);
        submitted.insert(201, payload);
        for _ in 0..100 {
            c.tick_all(1);
            if c.committed(final_waiter) {
                break;
            }
        }
        assert!(
            c.committed(final_waiter),
            "seed={seed}: final no-op not committed"
        );
        c.assert_equal();
        let batches = c.states[&1].applied_batches();
        let (expected, seen) = replay(&batches, seed);
        assert!(seen.len() > 1, "seed={seed}: no workload committed");
        for (_, payload) in &batches {
            let op = u64::from_le_bytes(payload[..8].try_into().unwrap());
            assert_eq!(submitted.get(&op), Some(payload), "seed={seed} op={op}");
        }
        for (&waiter, &op) in &waiters {
            assert!(
                !(c.committed(waiter) && c.failed(waiter)),
                "seed={seed} waiter={waiter}: committed and failed"
            );
            if c.committed(waiter) {
                assert!(
                    seen.contains(&op),
                    "seed={seed} waiter={waiter}: missing op={op}"
                );
            }
        }
        for (&id, state) in &c.states {
            let (actual, _) = replay(&state.applied_batches(), seed);
            assert_eq!(actual, expected, "seed={seed} node={id}");
            assert_eq!(actual.iter().sum::<i64>(), 800, "seed={seed} node={id}");
        }
    }
}

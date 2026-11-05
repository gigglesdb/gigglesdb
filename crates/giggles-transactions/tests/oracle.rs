mod common;
use common::*;
use giggles_transactions::*;
use std::collections::{BTreeMap, VecDeque};

/// Independent serial specification: no library locks/proofs/transition code.
#[derive(Clone)]
struct Oracle {
    left: i64,
    right: i64,
    budget: u64,
    receipts: BTreeMap<u128, i64>,
}
impl Oracle {
    fn transfer(&mut self, id: u128, amount: i64) -> bool {
        if self.receipts.contains_key(&id) {
            return true;
        }
        if self.budget == 0 || self.left < amount {
            return false;
        }
        self.left -= amount;
        self.right += amount;
        self.budget -= 1;
        self.receipts.insert(id, amount);
        true
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn pick(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

fn num(h: &Harness, range: u64, key: &[u8]) -> i64 {
    std::str::from_utf8(&h.image(range)[key])
        .unwrap()
        .parse()
        .unwrap()
}

fn observed_pair(h: &Harness, nonce: u128) -> Option<(i64, i64)> {
    // A separate branch models a complete current read attempt without affecting
    // the schedule being explored. No direct store observation is returned unless
    // every read lock exists, exactly as required of the integration adapter.
    let mut observer = h.clone();
    let t = transaction(nonce);
    observer.begin(t, &[2, 3]);
    for r in [2, 3] {
        match observer.acquire(r, t, point(b"balance", Mode::Shared)) {
            Ok(Response::LockGranted) => {}
            Ok(Response::Queued) | Err(Error::MustRollback) => return None,
            other => panic!("unexpected observer {other:?}"),
        }
    }
    Some((num(&observer, 2, b"balance"), num(&observer, 3, b"balance")))
}

#[derive(Clone)]
struct Worker {
    transaction: TransactionId,
    amount: i64,
    actions: VecDeque<(u64, Command)>,
    decision: Option<DecisionRef>,
    done: bool,
}

fn scheduled_transfers(seed: u64) {
    let _seed = SeedGuard::new(seed);
    let mut rng = Rng(seed);
    let mut h = Harness::with_limits(
        &[1, 2, 3, 4],
        Limits {
            transactions: 1024,
            ..Limits::default()
        },
    );
    for (range, key, value) in [
        (2, b"balance".as_slice(), b"1000".as_slice()),
        (3, b"balance", b"1000"),
        (4, b"task/budget", b"3"),
    ] {
        h.durable
            .0
            .get_mut(&domain(range))
            .unwrap()
            .image
            .insert(key.to_vec(), value.to_vec());
    }
    let mut oracle = Oracle {
        left: 1000,
        right: 1000,
        budget: 3,
        receipts: BTreeMap::new(),
    };
    let mut workers = Vec::new();
    for nonce in 1..=10 {
        let t = transaction(nonce);
        h.begin(t, &[2, 3, 4]);
        let lock = interval(b"task/", b"task0", Mode::Exclusive);
        let result = h.acquire(4, t, lock).unwrap();
        assert_eq!(
            result,
            if nonce == 1 {
                Response::LockGranted
            } else {
                Response::Queued
            }
        );
        workers.push(Worker {
            transaction: t,
            amount: (rng.next() % 30 + 1) as i64,
            actions: VecDeque::new(),
            decision: None,
            done: false,
        });
    }
    let mut steps = 0;
    while workers.iter().any(|w| !w.done) {
        steps += 1;
        assert!(
            steps < 20_000,
            "seed {seed} exceeded deterministic schedule bound"
        );
        let n = rng.pick(workers.len());
        let w = &mut workers[n];
        if w.done {
            continue;
        }
        if w.decision.is_none() && w.actions.is_empty() {
            if !h.machine(4).holds(
                w.transaction,
                &Span::Point(b"task/budget".to_vec()),
                Mode::Exclusive,
            ) {
                continue;
            }
            let budget = num(&h, 4, b"task/budget");
            let accepted = oracle.transfer(w.transaction.nonce, w.amount);
            assert_eq!(accepted, budget > 0);
            if !accepted {
                let d = h.decide(w.transaction, Outcome::Abort);
                w.decision = Some(d.clone());
                for r in [2, 3, 4] {
                    w.actions.push_back((r, Command::Finalize(d.clone())));
                }
            } else {
                for r in [2, 3] {
                    h.acquire(r, w.transaction, point(b"balance", Mode::Exclusive))
                        .unwrap();
                }
                let left = num(&h, 2, b"balance") - w.amount;
                let right = num(&h, 3, b"balance") + w.amount;
                h.stage(
                    2,
                    w.transaction,
                    b"balance",
                    Some(left.to_string().as_bytes()),
                );
                h.stage(
                    3,
                    w.transaction,
                    b"balance",
                    Some(right.to_string().as_bytes()),
                );
                h.stage(
                    4,
                    w.transaction,
                    b"task/budget",
                    Some((budget - 1).to_string().as_bytes()),
                );
                // Approval consumption and receipt are opaque adapter bytes,
                // under the same task authority lock and home decision.
                let approval = format!("task/approval/{}", w.transaction.nonce);
                let receipt = format!("task/receipt/{}", w.transaction.nonce);
                h.stage(4, w.transaction, approval.as_bytes(), Some(b"consumed"));
                h.stage(
                    4,
                    w.transaction,
                    receipt.as_bytes(),
                    Some(w.amount.to_string().as_bytes()),
                );
                let mut order = [2, 3, 4];
                order.rotate_left(rng.pick(3));
                for r in order {
                    w.actions.push_back((
                        r,
                        Command::Prepare {
                            transaction: w.transaction,
                        },
                    ));
                }
            }
        }
        if let Some((range, c)) = w.actions.pop_front() {
            let crash = match rng.pick(8) {
                0 => Crash::BeforeApply,
                1 => Crash::BeforePersist,
                2 => Crash::AfterPersist,
                _ => Crash::None,
            };
            let result = h
                .submit(range, c.clone(), crash)
                .unwrap_or_else(|| h.apply(range, c.clone()))
                .unwrap();
            if let Response::Prepared(p) = result {
                h.ok(1, Command::RecordPrepare(p));
            }
            // Duplicate/reordered message deliveries are intentionally frequent.
            if rng.pick(3) == 0 {
                h.ok(range, c);
            }
            if w.actions.is_empty() {
                if w.decision.is_none() {
                    let d = h.decide(w.transaction, Outcome::Commit);
                    w.decision = Some(d.clone());
                    let order = if rng.pick(2) == 0 {
                        [2, 3, 4]
                    } else {
                        [3, 2, 4]
                    };
                    for r in order {
                        w.actions.push_back((r, Command::Finalize(d.clone())));
                    }
                } else {
                    w.done = true;
                    assert_eq!(
                        (num(&h, 2, b"balance"), num(&h, 3, b"balance")),
                        (oracle.left, oracle.right)
                    );
                    assert_eq!(num(&h, 4, b"task/budget") as u64, oracle.budget);
                }
            }
        }
        let commits = workers
            .iter()
            .filter(|w| {
                w.decision
                    .as_ref()
                    .is_some_and(|d| d.outcome == Outcome::Commit)
            })
            .count();
        assert!(commits <= 3, "overspent budget seed{seed}");
        if rng.pick(3) == 0
            && let Some((a, b)) = observed_pair(&h, 10000 + steps as u128)
        {
            assert_eq!(a + b, 2000, "partial publication seed{seed}");
        }
    }
    assert_eq!(oracle.budget, 0);
    assert_eq!(oracle.receipts.len(), 3);
    let image = h.image(4);
    assert_eq!(
        image
            .keys()
            .filter(|k| k.starts_with(b"task/receipt/"))
            .count(),
        3
    );
    assert_eq!(
        image
            .keys()
            .filter(|k| k.starts_with(b"task/approval/"))
            .count(),
        3
    );
    for (id, amount) in oracle.receipts {
        assert_eq!(
            image[format!("task/receipt/{id}").as_bytes()],
            amount.to_string().as_bytes()
        );
    }
    assert_eq!(num(&h, 2, b"balance") + num(&h, 3, b"balance"), 2000);
    assert_eq!(num(&h, 4, b"task/budget"), 0);
}

#[test]
fn ten_concurrent_transfers_share_one_lifetime_budget_under_seeded_crashes() {
    for seed in 1..=48 {
        scheduled_transfers(seed);
    }
}

#[test]
fn independent_oracle_checks_abort_and_duplicate_semantics() {
    let mut oracle = Oracle {
        left: 10,
        right: 0,
        budget: 1,
        receipts: BTreeMap::new(),
    };
    assert!(oracle.transfer(1, 6));
    assert!(oracle.transfer(1, 6));
    assert!(!oracle.transfer(2, 1));
    assert_eq!(
        (
            oracle.left,
            oracle.right,
            oracle.budget,
            oracle.receipts.len()
        ),
        (4, 6, 0, 1)
    );
}

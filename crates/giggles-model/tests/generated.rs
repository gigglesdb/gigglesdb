mod common;
use common::*;
use giggles_model::*;
use std::collections::BTreeSet;
fn next(s: &mut u64) -> u64 {
    *s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *s
}

#[test]
fn ten_thousand_seeded_repair_histories() {
    // 10,000 deterministic serial histories, 40 actions each. This is NOT the
    // parent E2 concurrency/fault release campaign. Seed is printed on failure.
    for seed in 0..10_000_u64 {
        let (mut m, c) = fixture(12);
        let mut rng = seed;
        let mut spent = 0;
        let mut touched = BTreeSet::new();
        for step in 0..40 {
            let bits = next(&mut rng);
            let id = (bits >> 16) % 12;
            let key = key(id);
            match bits % 10 {
                0 => {
                    m.put(key, row(Status::Failed, 103)).unwrap();
                }
                1 => {
                    m.put(key, row(Status::Succeeded, 103)).unwrap();
                }
                2 if step > 30 => {
                    m.revoke(TaskId(1)).unwrap();
                }
                3 => {
                    m.bump_schema().unwrap();
                }
                _ => {
                    let mut r = request(step, &[id]);
                    if bits.is_multiple_of(7) {
                        r.column = Column::AttemptCount;
                    }
                    let before = m.snapshot();
                    let result = m.execute(
                        &c,
                        r.clone(),
                        Time::Certified(step + 10),
                        None,
                        bits.is_multiple_of(3),
                    );
                    let status = m.status(&c, r.operation).unwrap();
                    let OperationStatus::Decided(d) = status else {
                        panic!("seed {seed} step {step}: unresolved")
                    };
                    if let Outcome::Known(reply) = result {
                        assert_eq!(reply, d, "seed {seed} step {step}");
                    }
                    match &d {
                        Ok(receipt) => {
                            // Independent scalar oracle for this one-key workload.
                            let old = &before.rows[&key];
                            assert!(!before.tasks[&TaskId(1)].revoked, "seed {seed}");
                            assert_eq!(r.column, Column::Status, "seed {seed}");
                            assert_ne!(old.status, Status::Succeeded, "seed {seed}");
                            let delta = u64::from(old.status != Status::Queued);
                            spent += delta;
                            if delta > 0 {
                                touched.insert(key);
                            }
                            assert_eq!(receipt.charged_transitions, delta, "seed {seed}");
                            assert!(spent <= 25, "seed {seed}");
                        }
                        Err(_) => {
                            assert_eq!(m.snapshot().rows, before.rows, "seed {seed}");
                            assert_eq!(m.snapshot().tasks, before.tasks, "seed {seed}");
                        }
                    }
                    let stable = m.snapshot();
                    let retry = m.execute(&c, r.clone(), Time::Uncertain, None, false);
                    assert_eq!(retry, Outcome::Known(d), "seed {seed}");
                    assert_eq!(m.snapshot(), stable, "seed {seed}");
                    let mut conflicting = r;
                    conflicting.after = Status::Succeeded;
                    assert_eq!(
                        m.execute(&c, conflicting, Time::Certified(step + 10), None, false),
                        Outcome::Known(Err(Error::IdConflict)),
                        "seed {seed}"
                    );
                }
            }
            assert_eq!(
                m.snapshot().tasks[&TaskId(1)].transitions,
                spent,
                "seed {seed}"
            );
            assert_eq!(
                m.snapshot().tasks[&TaskId(1)].touched,
                touched,
                "seed {seed}"
            );
        }
        giggles_model::checker::verify(7, m.history(), &m.snapshot())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

#[test]
fn thousand_seeded_exact_approval_interference_histories() {
    for seed in 0..1000_u64 {
        let (mut m, c) = fixture(6);
        let r = predicate(1);
        let a = approved(&mut m, &c, &r);
        match seed % 7 {
            0 => m.put(key(0), row(Status::Failed, 103)).unwrap(), // ABA/version
            1 => m.put(key(9), row(Status::Failed, 103)).unwrap(), // phantom
            2 => m.bump_schema().unwrap(),
            3 => m.revoke_approval(a).unwrap(),
            4 => m.revoke(TaskId(1)).unwrap(),
            5 => m
                .put(
                    Key {
                        tenant: TenantId(99),
                        row: RowId(seed),
                    },
                    row(Status::Failed, 103),
                )
                .unwrap(),
            _ => {}
        }
        let before = m.snapshot();
        let outcome = m.execute(&c, r, Time::Certified(11), Some(a), false);
        assert_eq!(
            matches!(outcome, Outcome::Known(Ok(_))),
            seed % 7 >= 5,
            "seed {seed}"
        );
        if seed % 7 < 5 {
            assert_eq!(m.snapshot().rows, before.rows, "seed {seed}");
            assert_eq!(m.snapshot().tasks, before.tasks, "seed {seed}");
        }
        giggles_model::checker::verify(7, m.history(), &m.snapshot())
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

#[test]
fn thousand_seeded_ledger_histories_conserve_funds_and_holds() {
    use giggles_model::ledger::{AccountId, Ledger, State, TransferId};
    for seed in 0..1000_u64 {
        let mut rng = seed;
        let mut l = Ledger::default();
        l.open(AccountId(1), 1000).unwrap();
        l.open(AccountId(2), 1000).unwrap();
        for step in 0..100 {
            let bits = next(&mut rng);
            let id = TransferId((bits >> 16) % 20);
            let before = l.clone();
            let result = match bits % 4 {
                0 => l.reserve(
                    id,
                    AccountId(1),
                    AccountId(2),
                    u128::from((bits >> 32) % 100 + 1),
                    200,
                    step,
                ),
                1 => l.capture(id, step),
                2 => l.void(id),
                _ => l.expire(id, 200),
            };
            if result.is_err() {
                assert_eq!(l, before, "seed {seed} step {step}");
            }
            let held = l.accounts().values().map(|a| a.held).sum::<u128>();
            let available = l.accounts().values().map(|a| a.available).sum::<u128>();
            assert_eq!(held + available, 2000, "seed {seed} step {step}");
            assert_eq!(
                held,
                l.transfers()
                    .values()
                    .filter(|t| t.state == State::Pending)
                    .map(|t| t.amount)
                    .sum::<u128>(),
                "seed {seed} step {step}"
            );
            let posted = l
                .transfers()
                .values()
                .filter(|t| t.state == State::Posted)
                .map(|t| t.amount)
                .sum::<u128>();
            assert_eq!(
                l.accounts()[&AccountId(2)].available,
                1000 + posted,
                "seed {seed} step {step}"
            );
        }
    }
}

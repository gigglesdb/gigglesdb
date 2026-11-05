mod common;
use common::*;
use giggles_history::*;
use std::collections::BTreeMap;

// Separate brute-force oracle: enumerate ALL optional-commit subsets, ALL
// permutations and ALL integer serialization timestamps, then interpret the
// operation stream with a vector-backed state. No checker execution/search
// functions are used. This campaign intentionally covers no-retry tiny states;
// retry behavior has independent targeted behavioral fixtures.
fn brute(h: &History) -> bool {
    fn permutations(xs: &mut [usize], at: usize, out: &mut Vec<Vec<usize>>) {
        if at == xs.len() {
            out.push(xs.to_vec());
            return;
        }
        for j in at..xs.len() {
            xs.swap(at, j);
            permutations(xs, at + 1, out);
            xs.swap(at, j);
        }
    }
    fn execute(h: &History, order: &[usize]) -> bool {
        let mut state: Vec<(i64, i64)> = h.initial.iter().map(|(&k, &v)| (k, v)).collect();
        for &index in order {
            let a = &h.attempts[index];
            let mut observed = vec![];
            for op in &a.ops {
                match op {
                    Op::Read { key } => observed.push(Observation::Read {
                        value: state.iter().find(|(k, _)| k == key).map(|(_, v)| *v),
                    }),
                    Op::Scan { start, end } => {
                        let mut rows: Vec<_> = state
                            .iter()
                            .filter(|(k, _)| k >= start && k < end)
                            .copied()
                            .collect();
                        rows.sort();
                        observed.push(Observation::Scan { rows });
                    }
                    Op::Write { key, value } => {
                        state.retain(|(k, _)| k != key);
                        state.push((*key, *value));
                    }
                    Op::Delete { key } => state.retain(|(k, _)| k != key),
                    Op::Transfer { .. } => panic!("not in tiny campaign"),
                }
            }
            if !observed.starts_with(&a.observations) {
                return false;
            }
        }
        true
    }
    fn timestamps(h: &History, order: &[usize], at: usize, prev: u64) -> bool {
        if at == order.len() {
            return true;
        }
        let a = &h.attempts[order[at]];
        // Unlike production earliest-point propagation, enumerate all times.
        for t in prev..=h.horizon {
            let before_response = a.outcome != Outcome::Committed || t <= a.complete.unwrap();
            let before_decision =
                a.durable != Some(Outcome::Committed) || a.decision_at.is_none_or(|d| t <= d);
            if t >= a.invoke
                && before_response
                && before_decision
                && timestamps(h, order, at + 1, t)
            {
                return true;
            }
        }
        false
    }
    for subset in 0..(1usize << h.attempts.len()) {
        if h.attempts.iter().enumerate().any(|(i, a)| {
            let included = subset & (1 << i) != 0;
            (!included
                && (a.outcome == Outcome::Committed || a.durable == Some(Outcome::Committed)))
                || (included
                    && (a.outcome == Outcome::Aborted || a.durable == Some(Outcome::Aborted)))
        }) {
            continue;
        }
        let mut selected: Vec<_> = (0..h.attempts.len())
            .filter(|i| subset & (1 << i) != 0)
            .collect();
        let mut orders = vec![];
        permutations(&mut selected, 0, &mut orders);
        for order in orders {
            if timestamps(h, &order, 0, 0) && execute(h, &order) {
                return true;
            }
        }
    }
    false
}

fn variants(id: &str) -> Vec<Attempt> {
    let mut out = vec![];
    for observed in [None, Some(0), Some(1)] {
        for ops in [
            vec![Op::Read { key: 0 }],
            vec![Op::Read { key: 0 }, Op::Write { key: 0, value: 1 }],
            vec![
                Op::Scan { start: 0, end: 1 },
                Op::Write { key: 0, value: 1 },
            ],
            vec![Op::Delete { key: 0 }, Op::Read { key: 0 }],
        ] {
            let obs = if matches!(ops[0], Op::Scan { .. }) {
                Observation::Scan {
                    rows: observed.map(|v| vec![(0, v)]).unwrap_or_default(),
                }
            } else {
                Observation::Read { value: observed }
            };
            for outcome in [Outcome::Committed, Outcome::Indeterminate, Outcome::Aborted] {
                let mut a = tx(id, 0, 3, ops.clone(), vec![obs.clone()]);
                a.outcome = outcome;
                out.push(a);
            }
        }
    }
    out
}

#[test]
fn exhaustive_two_transaction_crosscheck_23328_histories() {
    let mut checked = 0;
    for initial in [State::new(), State::from([(0, 0)]), State::from([(0, 1)])] {
        for interval in 0..6 {
            for a in variants("a") {
                for mut b in variants("b") {
                    let mut a = a.clone();
                    match interval {
                        0 => {}
                        1 => {
                            a.complete = Some(1);
                            b.invoke = 2;
                        }
                        2 => {
                            b.complete = Some(1);
                            a.invoke = 2;
                        }
                        3 => {
                            a.invoke = 1;
                            a.complete = Some(1);
                            b.invoke = 1;
                            b.complete = Some(1);
                        }
                        4 => {
                            a.durable = Some(if a.outcome == Outcome::Aborted {
                                Outcome::Aborted
                            } else {
                                Outcome::Committed
                            });
                            a.decision_at = Some(1);
                        }
                        _ => {
                            b.durable = Some(Outcome::Aborted);
                        }
                    }
                    let h = History {
                        version: 1,
                        horizon: 3,
                        initial: initial.clone(),
                        invariants: Invariants::default(),
                        attempts: vec![a, b],
                    };
                    let expected = brute(&h);
                    let result = check(&h, Limits::default()).unwrap();
                    assert_eq!(
                        result.verdict,
                        if expected {
                            Verdict::Pass
                        } else {
                            Verdict::Fail
                        },
                        "{h:?}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert_eq!(checked, 23328);
}

#[test]
fn exhaustive_three_transaction_crosscheck_512_histories() {
    for bits in 0..512 {
        let attempts = (0..3)
            .map(|i| {
                let v = (bits >> (i * 3)) & 7;
                let mut a = tx(
                    &format!("t{i}"),
                    0,
                    2,
                    vec![
                        Op::Read {
                            key: (i % 2) as i64,
                        },
                        Op::Write {
                            key: ((i + 1) % 2) as i64,
                            value: 1,
                        },
                    ],
                    vec![Observation::Read {
                        value: if v & 1 == 0 { None } else { Some(1) },
                    }],
                );
                a.outcome = if v & 2 == 0 {
                    Outcome::Committed
                } else {
                    Outcome::Indeterminate
                };
                if v & 4 != 0 {
                    a.invoke = 1;
                }
                a
            })
            .collect();
        let mut h = history(attempts);
        h.horizon = 2;
        assert_eq!(verdict(&h) == Verdict::Pass, brute(&h), "{h:?}");
    }
}

fn rename_and_shift(h: &History) -> History {
    let mut changed = h.clone();
    changed.horizon = h.horizon * 3 + 17;
    let key = |k: i64| k * 2 + 31;
    changed.initial = h.initial.iter().map(|(&k, &v)| (key(k), v)).collect();
    changed.invariants.conserved_keys = h
        .invariants
        .conserved_keys
        .iter()
        .map(|k| key(*k))
        .collect();
    changed.invariants.nonnegative_keys = h
        .invariants
        .nonnegative_keys
        .iter()
        .map(|k| key(*k))
        .collect();
    let ids: BTreeMap<_, _> = h
        .attempts
        .iter()
        .map(|a| (a.logical_id.clone(), format!("renamed-{}", a.logical_id)))
        .collect();
    for a in &mut changed.attempts {
        a.id = format!("attempt-{}", a.id);
        a.logical_id = ids[&a.logical_id].clone();
        a.invoke = a.invoke * 3 + 17;
        a.complete = a.complete.map(|t| t * 3 + 17);
        a.decision_at = a.decision_at.map(|t| t * 3 + 17);
        for op in &mut a.ops {
            match op {
                Op::Read { key: k } | Op::Write { key: k, .. } | Op::Delete { key: k } => {
                    *k = key(*k)
                }
                Op::Scan { start, end } => {
                    *start = key(*start);
                    *end = key(*end);
                }
                Op::Transfer { from, to, .. } => {
                    *from = key(*from);
                    *to = key(*to);
                }
            }
        }
        for obs in &mut a.observations {
            if let Observation::Scan { rows } = obs {
                for (k, _) in rows {
                    *k = key(*k);
                }
            }
        }
    }
    changed
}

#[test]
fn metamorphic_128_seeds_positive_corrupt_ids_keys_intervals() {
    for seed in 0..128 {
        let h = generate(seed, 8).unwrap();
        assert_eq!(verdict(&h), Verdict::Pass);
        assert_eq!(verdict(&rename_and_shift(&h)), Verdict::Pass);
        let mut bad = h.clone();
        if let Observation::Scan { rows } = &mut bad.attempts[7].observations[0] {
            rows[0].1 += 1;
        }
        assert_eq!(verdict(&bad), Verdict::Fail);
        assert_eq!(verdict(&rename_and_shift(&bad)), Verdict::Fail);
        // Input order is not a serialization order.
        let mut reversed = h;
        reversed.attempts.reverse();
        assert_eq!(verdict(&reversed), Verdict::Pass);
    }
}

#[test]
fn generator_reproducibility_bounds_and_full_range_of_seeds() {
    for seed in [0, 1, 2, u64::MAX, 0xfeed_cafe] {
        let h = generate(seed, 128).unwrap();
        assert_eq!(h, generate(seed, 128).unwrap());
        assert_eq!(verdict(&h), Verdict::Pass);
    }
    assert!(generate(0, 129).is_err());
    assert_ne!(generate(0, 2).unwrap(), generate(1, 2).unwrap());
}

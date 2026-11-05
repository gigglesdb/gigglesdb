mod common;
use common::*;
use giggles_history::*;

#[test]
fn lost_acknowledged_write_and_stale_real_time_read() {
    let good = history(vec![writer("w", 0, 1, 0, 7), reader("r", 2, 3, 0, Some(7))]);
    let mut bad = good.clone();
    bad.attempts[1].observations = vec![Observation::Read { value: None }];
    assert_pair(&good, &bad);
    // Without a final observation, a checker cannot infer silent disk loss.
    assert_eq!(
        verdict(&history(vec![good.attempts[0].clone()])),
        Verdict::Pass
    );
}
#[test]
fn partial_multi_key_commit() {
    let good = history(vec![
        tx(
            "w",
            0,
            1,
            vec![
                Op::Write { key: 0, value: 7 },
                Op::Write { key: 1, value: 8 },
            ],
            vec![],
        ),
        scanner("r", 2, 3, 0, 2, vec![(0, 7), (1, 8)]),
    ]);
    let mut bad = good.clone();
    bad.attempts[1].observations = vec![Observation::Scan { rows: vec![(0, 7)] }];
    assert_pair(&good, &bad);
}
#[test]
fn write_skew() {
    let mut bad = history(vec![
        tx(
            "a",
            0,
            4,
            vec![Op::Read { key: 1 }, Op::Write { key: 0, value: 0 }],
            vec![Observation::Read { value: Some(1) }],
        ),
        tx(
            "b",
            0,
            4,
            vec![Op::Read { key: 0 }, Op::Write { key: 1, value: 0 }],
            vec![Observation::Read { value: Some(1) }],
        ),
    ]);
    bad.initial = State::from([(0, 1), (1, 1)]);
    let mut good = bad.clone();
    good.attempts[1].observations = vec![Observation::Read { value: Some(0) }];
    assert_pair(&good, &bad);
}
#[test]
fn lost_update() {
    let bad = history(vec![
        tx(
            "a",
            0,
            4,
            vec![Op::Read { key: 0 }, Op::Write { key: 0, value: 1 }],
            vec![Observation::Read { value: None }],
        ),
        tx(
            "b",
            0,
            4,
            vec![Op::Read { key: 0 }, Op::Write { key: 0, value: 1 }],
            vec![Observation::Read { value: None }],
        ),
    ]);
    let mut good = bad.clone();
    good.attempts[1].observations = vec![Observation::Read { value: Some(1) }];
    good.attempts[1].ops[1] = Op::Write { key: 0, value: 2 };
    assert_pair(&good, &bad);
}
#[test]
fn phantom_write_skew_and_missing_key_cycle() {
    let bad = history(vec![
        tx(
            "a",
            0,
            4,
            vec![
                Op::Scan { start: 0, end: 2 },
                Op::Write { key: 2, value: 1 },
            ],
            vec![Observation::Scan { rows: vec![] }],
        ),
        tx(
            "b",
            0,
            4,
            vec![Op::Read { key: 2 }, Op::Write { key: 1, value: 1 }],
            vec![Observation::Read { value: None }],
        ),
    ]);
    let mut good = bad.clone();
    good.attempts[1].observations = vec![Observation::Read { value: Some(1) }];
    assert_pair(&good, &bad);
}
#[test]
fn complete_scan_includes_absent_keys_and_half_open_bounds() {
    let good = history(vec![
        writer("w", 0, 1, 1, 5),
        writer("out", 0, 1, 2, 9),
        scanner("r", 2, 3, 0, 2, vec![(1, 5)]),
    ]);
    for rows in [vec![], vec![(1, 5), (2, 9)], vec![(0, 7), (1, 5)]] {
        let mut bad = good.clone();
        bad.attempts[2].observations = vec![Observation::Scan { rows }];
        assert_pair(&good, &bad);
    }
}
#[test]
fn read_your_writes_delete_and_recreate() {
    let good = history(vec![
        tx(
            "t",
            0,
            3,
            vec![
                Op::Write { key: 0, value: 4 },
                Op::Read { key: 0 },
                Op::Delete { key: 0 },
                Op::Read { key: 0 },
                Op::Scan { start: -1, end: 1 },
                Op::Write { key: 0, value: 5 },
            ],
            vec![
                Observation::Read { value: Some(4) },
                Observation::Read { value: None },
                Observation::Scan { rows: vec![] },
            ],
        ),
        reader("r", 4, 5, 0, Some(5)),
    ]);
    let mut bad = good.clone();
    bad.attempts[0].observations[1] = Observation::Read { value: Some(4) };
    assert_pair(&good, &bad);
}
#[test]
fn illegal_aborted_effects() {
    for durable in [None, Some(Outcome::Aborted)] {
        let mut a = writer("a", 0, 1, 0, 7);
        a.outcome = Outcome::Aborted;
        a.durable = durable;
        let good = history(vec![a, reader("r", 2, 3, 0, None)]);
        let mut bad = good.clone();
        bad.attempts[1].observations = vec![Observation::Read { value: Some(7) }];
        assert_pair(&good, &bad);
    }
}
#[test]
fn indeterminate_can_commit_or_abort_but_one_choice_must_explain_everything() {
    let mut a = writer("a", 0, 1, 0, 7);
    a.outcome = Outcome::Indeterminate;
    let yes = history(vec![a.clone(), reader("r", 2, 3, 0, Some(7))]);
    assert_eq!(verdict(&yes), Verdict::Pass);
    let no = history(vec![a.clone(), reader("r", 2, 3, 0, None)]);
    assert_eq!(verdict(&no), Verdict::Pass);
    let bad = history(vec![
        a,
        reader("r", 2, 3, 0, Some(7)),
        reader("s", 4, 5, 0, None),
    ]);
    assert_eq!(verdict(&bad), Verdict::Fail);
}
#[test]
fn timeout_does_not_bound_durable_commit() {
    let mut a = writer("a", 0, 1, 0, 7);
    a.outcome = Outcome::Indeterminate;
    let good = history(vec![
        a,
        reader("r", 2, 3, 0, None),
        reader("s", 4, 5, 0, Some(7)),
    ]);
    assert_eq!(verdict(&good), Verdict::Pass);
    let mut bad = good.clone();
    bad.attempts[0].durable = Some(Outcome::Committed);
    bad.attempts[0].decision_at = Some(1);
    assert_eq!(verdict(&bad), Verdict::Fail);
}
#[test]
fn durable_commit_without_response_is_mandatory() {
    let mut a = writer("a", 0, 1, 0, 7);
    a.complete = None;
    a.outcome = Outcome::Indeterminate;
    a.durable = Some(Outcome::Committed);
    a.decision_at = Some(1);
    let good = history(vec![a, reader("r", 2, 3, 0, Some(7))]);
    let mut bad = good.clone();
    bad.attempts[1].observations = vec![Observation::Read { value: None }];
    assert_pair(&good, &bad);
}
#[test]
fn durable_abort_and_contradictory_response_cannot_commit() {
    let good = history(vec![writer("a", 0, 1, 0, 7)]);
    let mut bad = good.clone();
    bad.attempts[0].durable = Some(Outcome::Aborted);
    assert_pair(&good, &bad);
    bad.attempts[0].outcome = Outcome::Aborted;
    bad.attempts[0].durable = Some(Outcome::Committed);
    assert_pair(&good, &bad);
}
#[test]
fn successful_retries_reuse_original_read_result() {
    let mut a = reader("a", 0, 1, 0, None);
    a.logical_id = "logical".into();
    let mut b = a.clone();
    b.id = "b".into();
    b.invoke = 4;
    b.complete = Some(5);
    let good = history(vec![a, writer("w", 2, 3, 0, 7), b]);
    assert_eq!(verdict(&good), Verdict::Pass);
    let mut bad = good.clone();
    bad.attempts[2].observations = vec![Observation::Read { value: Some(7) }];
    assert_eq!(verdict(&bad), Verdict::Fail);
}
#[test]
fn retries_do_not_double_commit_transfer() {
    let mut a = tx(
        "a",
        0,
        1,
        vec![Op::Transfer {
            from: 0,
            to: 1,
            amount: 2,
        }],
        vec![],
    );
    a.logical_id = "x".into();
    a.outcome = Outcome::Indeterminate;
    let mut b = a.clone();
    b.id = "b".into();
    b.invoke = 2;
    b.complete = Some(3);
    b.outcome = Outcome::Committed;
    let mut good = history(vec![a, b, scanner("r", 4, 5, 0, 2, vec![(0, 8), (1, 12)])]);
    good.initial = State::from([(0, 10), (1, 10)]);
    let mut bad = good.clone();
    bad.attempts[2].observations = vec![Observation::Scan {
        rows: vec![(0, 6), (1, 14)],
    }];
    assert_pair(&good, &bad);
}
#[test]
fn retry_cannot_widen_interval_past_prior_acknowledgment() {
    let a = reader("a", 0, 1, 0, Some(7));
    let mut retry = a.clone();
    retry.id = "retry".into();
    retry.invoke = 4;
    retry.complete = Some(5);
    let bad = history(vec![a, writer("w", 2, 3, 0, 7), retry]);
    assert_eq!(verdict(&bad), Verdict::Fail);
}
#[test]
fn aborted_attempt_can_restart_same_body_without_aborted_effects() {
    let mut a = writer("a", 0, 1, 0, 7);
    a.outcome = Outcome::Aborted;
    let mut b = a.clone();
    b.id = "b".into();
    b.invoke = 4;
    b.complete = Some(5);
    b.outcome = Outcome::Committed;
    let good = history(vec![
        a,
        reader("r", 2, 3, 0, None),
        b,
        reader("s", 6, 7, 0, Some(7)),
    ]);
    assert_eq!(verdict(&good), Verdict::Pass);
}
#[test]
fn candidate_unknown_observations_constrain_its_commit() {
    let mut a = reader("a", 0, 1, 0, Some(9));
    a.outcome = Outcome::Indeterminate;
    a.durable = Some(Outcome::Committed);
    let bad = history(vec![a]);
    assert_eq!(verdict(&bad), Verdict::Fail);
    let mut good = bad.clone();
    good.attempts[0].durable = None;
    assert_eq!(verdict(&good), Verdict::Pass);
}
#[test]
fn overlap_order_can_differ_from_input_order() {
    let good = history(vec![reader("a", 0, 5, 0, Some(7)), writer("z", 1, 4, 0, 7)]);
    let result = check(&good, Limits::default()).unwrap();
    assert_eq!(result.verdict, Verdict::Pass);
    assert_eq!(result.serialization[0].logical_id, "z");
}
#[test]
fn equality_of_interval_endpoints_allows_shared_instant() {
    let good = history(vec![reader("a", 1, 1, 0, Some(7)), writer("z", 1, 1, 0, 7)]);
    assert_eq!(verdict(&good), Verdict::Pass);
}
#[test]
fn transfer_atomicity_conservation_and_nonnegative() {
    let mut good = history(vec![
        tx(
            "t",
            0,
            1,
            vec![Op::Transfer {
                from: 0,
                to: 1,
                amount: 3,
            }],
            vec![],
        ),
        scanner("r", 2, 3, 0, 2, vec![(0, 7), (1, 13)]),
    ]);
    good.initial = State::from([(0, 10), (1, 10)]);
    good.invariants.conserved_keys = vec![0, 1];
    good.invariants.nonnegative_keys = vec![0, 1];
    for rows in [vec![(0, 7), (1, 10)], vec![(0, 10), (1, 13)]] {
        let mut bad = good.clone();
        bad.attempts[1].observations = vec![Observation::Scan { rows }];
        assert_pair(&good, &bad);
    }
    let mut bad = good.clone();
    bad.attempts[0].ops = vec![Op::Write { key: 0, value: 7 }];
    assert_pair(&good, &bad);
    let mut bad = good.clone();
    bad.attempts[0].ops = vec![
        Op::Write { key: 0, value: -1 },
        Op::Write { key: 1, value: 21 },
    ];
    assert_pair(&good, &bad);
}
#[test]
fn transfer_preconditions_and_overflow() {
    for initial in [
        State::from([(0, 1), (1, 0)]),
        State::from([(0, 10)]),
        State::from([(0, 10), (1, i64::MAX)]),
    ] {
        let mut bad = history(vec![tx(
            "t",
            0,
            1,
            vec![Op::Transfer {
                from: 0,
                to: 1,
                amount: 2,
            }],
            vec![],
        )]);
        bad.initial = initial;
        assert_eq!(verdict(&bad), Verdict::Fail);
        let mut good = bad;
        good.attempts[0].outcome = Outcome::Aborted;
        assert_eq!(verdict(&good), Verdict::Pass);
    }
}
#[test]
fn conservation_sum_uses_wide_arithmetic() {
    let mut h = history(vec![]);
    h.initial = State::from([(0, i64::MAX), (1, i64::MAX)]);
    h.invariants.conserved_keys = vec![0, 1];
    assert_eq!(verdict(&h), Verdict::Pass);
}
#[test]
fn receipt_budget_uniqueness_and_retry_once() {
    let mut a = writer("a", 0, 1, 0, 7);
    a.receipt = Some(Receipt {
        id: "r1".into(),
        budget: "task".into(),
        cost: 3,
    });
    let mut b = writer("b", 2, 3, 1, 8);
    b.receipt = Some(Receipt {
        id: "r2".into(),
        budget: "task".into(),
        cost: 2,
    });
    let mut good = history(vec![a, b]);
    good.invariants.require_receipts = true;
    good.invariants.budgets.insert("task".into(), 5);
    for mode in 0..4 {
        let mut bad = good.clone();
        match mode {
            0 => bad.invariants.budgets.insert("task".into(), 4),
            1 => {
                bad.attempts[1].receipt.as_mut().unwrap().id = "r1".into();
                None
            }
            2 => {
                bad.attempts[1].receipt = None;
                None
            }
            _ => {
                bad.attempts[1].receipt.as_mut().unwrap().budget = "unknown".into();
                None
            }
        };
        assert_pair(&good, &bad);
    }
    let mut retry = good.attempts[0].clone();
    retry.id = "retry".into();
    retry.invoke = 4;
    retry.complete = Some(5);
    good.attempts.push(retry);
    assert_eq!(verdict(&good), Verdict::Pass);
}
#[test]
fn budget_overflow_cannot_wrap() {
    let mut h = history(vec![writer("a", 0, 1, 0, 7), writer("b", 2, 3, 1, 8)]);
    h.invariants.budgets.insert("t".into(), u64::MAX);
    for (i, a) in h.attempts.iter_mut().enumerate() {
        a.receipt = Some(Receipt {
            id: format!("r{i}"),
            budget: "t".into(),
            cost: u64::MAX,
        });
    }
    assert_eq!(verdict(&h), Verdict::Fail);
}
#[test]
fn all_limits_are_inconclusive_not_pass_or_fail() {
    let h = history(vec![writer("a", 0, 1, 0, 7)]);
    for limits in [
        Limits {
            max_nodes: 0,
            ..Limits::default()
        },
        Limits {
            max_nodes: 1,
            ..Limits::default()
        },
        Limits {
            max_memory_bytes: 0,
            ..Limits::default()
        },
    ] {
        assert_eq!(check(&h, limits).unwrap().verdict, Verdict::Inconclusive);
    }
    assert_eq!(
        check(
            &history(vec![]),
            Limits {
                max_nodes: 0,
                ..Limits::default()
            }
        )
        .unwrap()
        .verdict,
        Verdict::Inconclusive
    );
}
#[test]
fn minimization_preserves_validity_failure_and_initial_contract() {
    let mut h = history(vec![
        writer("a", 0, 1, 0, 7),
        reader("b", 2, 3, 0, None),
        writer("irrelevant", 4, 5, 10, 11),
    ]);
    h.invariants.nonnegative_keys = vec![0];
    let (small, n) = minimize(&h, Limits::default(), 100).unwrap();
    assert_eq!(n, 3);
    assert_eq!(small.attempts.len(), 2);
    assert_eq!(small.initial, h.initial);
    assert_eq!(small.invariants, h.invariants);
    assert_eq!(verdict(&small), Verdict::Fail);
    for id in ["a", "b"] {
        let mut less = small.clone();
        less.attempts.retain(|a| a.logical_id != id);
        assert_eq!(verdict(&less), Verdict::Pass);
    }
    assert!(minimize(&history(vec![]), Limits::default(), 1).is_err());
    let (same, n) = minimize(&h, Limits::default(), 0).unwrap();
    assert_eq!(n, 0);
    assert_eq!(same, h);
}
#[test]
fn witness_is_deterministic_and_serial_time_is_feasible() {
    let h = generate(88, 12).unwrap();
    let a = check(&h, Limits::default()).unwrap();
    let b = check(&h, Limits::default()).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.serialization.len(), 12);
    for step in &a.serialization {
        let attempt = h.attempts.iter().find(|x| x.id == step.attempt_id).unwrap();
        assert!(step.at >= attempt.invoke && step.at <= attempt.decision_at.unwrap());
    }
}
#[test]
fn empty_and_all_aborted_histories() {
    assert_eq!(verdict(&history(vec![])), Verdict::Pass);
    let mut h = history(vec![writer("a", 0, 1, 0, 7)]);
    h.attempts[0].outcome = Outcome::Aborted;
    assert_eq!(verdict(&h), Verdict::Pass);
}

#[test]
fn durable_retry_evidence_cannot_discard_conflicting_observation_prefix() {
    let mut a = reader("a", 0, 1, 0, Some(9));
    a.outcome = Outcome::Indeterminate;
    a.durable = Some(Outcome::Committed);
    let mut b = reader("b", 2, 3, 0, None);
    b.logical_id = "a".into();
    let bad = history(vec![a, b]);
    assert_eq!(verdict(&bad), Verdict::Fail);
    let mut good = bad.clone();
    good.attempts[0].observations = vec![Observation::Read { value: None }];
    assert_pair(&good, &bad);
}

#[test]
fn indeterminate_prefix_may_be_incomplete_but_committed_prefix_must_match() {
    let mut a = tx(
        "a",
        0,
        1,
        vec![
            Op::Read { key: 0 },
            Op::Write { key: 0, value: 7 },
            Op::Read { key: 0 },
        ],
        vec![Observation::Read { value: None }],
    );
    a.outcome = Outcome::Indeterminate;
    a.durable = Some(Outcome::Committed);
    let good = history(vec![a, reader("r", 2, 3, 0, Some(7))]);
    assert_eq!(verdict(&good), Verdict::Pass);
    let mut bad = good.clone();
    bad.attempts[0].observations = vec![Observation::Read { value: Some(7) }];
    assert_pair(&good, &bad);
}

#[test]
fn invariant_violation_cannot_be_compensated_later() {
    let mut bad = history(vec![writer("a", 0, 1, 0, 4), writer("b", 2, 3, 0, 5)]);
    bad.initial.insert(0, 5);
    bad.invariants.conserved_keys = vec![0];
    assert_eq!(verdict(&bad), Verdict::Fail);
    let mut good = bad.clone();
    good.attempts[0].ops = vec![Op::Write { key: 0, value: 5 }];
    assert_pair(&good, &bad);
}

#[test]
fn extreme_keys_values_and_time_do_not_overflow() {
    let mut h = history(vec![
        writer("a", 0, 1, i64::MIN, i64::MIN),
        writer("b", 2, 3, i64::MAX, i64::MAX),
        reader("r", u64::MAX - 1, u64::MAX, i64::MAX, Some(i64::MAX)),
    ]);
    h.horizon = u64::MAX;
    assert_eq!(verdict(&h), Verdict::Pass);
    h.attempts.push(scanner(
        "scan",
        4,
        5,
        i64::MIN,
        i64::MAX,
        vec![(i64::MIN, i64::MIN)],
    ));
    assert_eq!(verdict(&h), Verdict::Pass);
}

#[test]
fn minimizer_drops_whole_retry_groups_and_keeps_inconclusive_candidates() {
    let mut h = history(vec![writer("a", 0, 1, 0, 7), reader("r", 2, 3, 0, None)]);
    let mut retry = h.attempts[0].clone();
    retry.id = "retry".into();
    retry.invoke = 4;
    retry.complete = Some(5);
    h.attempts.push(retry);
    let (reduced, _) = minimize(&h, Limits::default(), 128).unwrap();
    assert_eq!(reduced.attempts.len(), 3);
    assert_eq!(verdict(&reduced), Verdict::Fail);
    let limits = Limits {
        max_nodes: 0,
        ..Limits::default()
    };
    assert!(minimize(&h, limits, 128).is_err());
}

#[test]
fn memory_bound_includes_derived_scans_even_when_unknown_wire_has_no_results() {
    let mut h = history(vec![]);
    h.initial = (0..4096).map(|k| (k, k)).collect();
    for id in ["a", "b"] {
        let mut a = tx(
            id,
            0,
            1,
            vec![
                Op::Scan {
                    start: 0,
                    end: 4096
                };
                256
            ],
            vec![],
        );
        a.outcome = Outcome::Indeterminate;
        a.durable = Some(Outcome::Committed);
        h.attempts.push(a);
    }
    let result = check(&h, Limits::default()).unwrap();
    assert_eq!(result.verdict, Verdict::Inconclusive);
    assert_eq!(result.explored, 0);
    assert!(result.memory_bound > 64 * 1024 * 1024);
}

use giggles_history::record::{Recorder, TransferWorkload, controls, window};
use giggles_history::{History, Limits, Observation, Op, Outcome, State, Verdict, check};
use std::sync::Mutex;

fn execute(s: &mut State, ops: &[Op]) -> Vec<Observation> {
    let mut observations = vec![];
    for op in ops {
        match *op {
            Op::Transfer { from, to, amount } => {
                assert!(s[&from] >= amount);
                *s.get_mut(&from).unwrap() -= amount;
                *s.get_mut(&to).unwrap() += amount;
            }
            Op::Read { key } => observations.push(Observation::Read {
                value: s.get(&key).copied(),
            }),
            Op::Scan { start, end } => observations.push(Observation::Scan {
                rows: s.range(start..end).map(|(&k, &v)| (k, v)).collect(),
            }),
            _ => panic!("unexpected workload operation"),
        }
    }
    observations
}
fn verdict(h: &History, expected: Verdict) {
    assert_eq!(check(h, Limits::default()).unwrap().verdict, expected);
}
fn simulation(seed: u64, read_committed_safe: bool) -> History {
    let initial: State = (1..=8).map(|k| (k, 1000)).collect();
    let state = Mutex::new(initial.clone());
    let recorder = Recorder::new();
    std::thread::scope(|scope| {
        for thread in 0..3 {
            let (state, recorder) = (&state, &recorder);
            scope.spawn(move || {
                let workload = TransferWorkload::new(seed * 3 + thread, (1..=8).collect());
                let mut workload = if read_committed_safe {
                    workload.read_committed_safe()
                } else {
                    workload
                };
                for txn in 0..30 {
                    let ops = workload.next_ops();
                    let pending = recorder.invoke(&format!("t{thread}-{txn}"), ops.clone());
                    let mut state = state.lock().unwrap();
                    let observations = execute(&mut state, &ops);
                    recorder.complete(pending, Outcome::Committed, observations);
                }
            });
        }
    });
    let ops = vec![TransferWorkload::new(0, (1..=8).collect()).scan()];
    let p = recorder.invoke("final", ops.clone());
    recorder.complete(
        p,
        Outcome::Committed,
        execute(&mut state.lock().unwrap(), &ops),
    );
    assert_eq!(recorder.len(), 91);
    let attempts = recorder.take();
    assert!(recorder.is_empty());
    assert!(attempts.windows(2).all(|a| a[0].invoke < a[1].invoke));
    window(initial, &(1..=8).collect::<Vec<_>>(), attempts).unwrap()
}

#[test]
fn concurrent_transfers_and_negative_controls() {
    for seed in 1..=8 {
        let h = simulation(seed, false);
        verdict(&h, Verdict::Pass);
        for control in [
            controls::stale_final_read,
            controls::aborted_but_visible,
            controls::duplicate_apply,
        ] {
            let mutated = control(&h).expect("control candidate");
            assert_eq!(
                check(&mutated, Limits::default()).unwrap().verdict,
                Verdict::Fail
            );
        }
    }
}

#[test]
fn indeterminate_evidence_and_resolution_errors() {
    for durable in [Outcome::Committed, Outcome::Aborted] {
        let r = Recorder::new();
        let initial = State::from([(1, 1000), (2, 1000)]);
        let mut state = initial.clone();
        let ops = vec![Op::Transfer {
            from: 1,
            to: 2,
            amount: 1,
        }];
        let p = r.invoke("unknown", ops.clone());
        let id = p.id.clone();
        execute(&mut state, &ops);
        r.complete(p, Outcome::Indeterminate, vec![]);
        assert!(r.resolve("missing", durable).is_err());
        assert!(r.resolve(&id, Outcome::Indeterminate).is_err());
        r.resolve(&id, durable).unwrap();
        assert!(r.resolve(&id, durable).is_err());
        let scan = vec![Op::Scan { start: 1, end: 3 }];
        let p = r.invoke("final", scan.clone());
        r.complete(p, Outcome::Committed, execute(&mut state, &scan));
        let attempts = r.take();
        assert_eq!(attempts[0].complete, None);
        assert_eq!(attempts[0].decision_at, None);
        let h = window(initial, &[1, 2], attempts).unwrap();
        verdict(
            &h,
            if durable == Outcome::Committed {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
        );
    }
}

#[test]
fn bounds_and_workload_determinism() {
    let r = Recorder::default();
    for i in 0..129 {
        r.complete(r.invoke(&format!("t{i}"), vec![]), Outcome::Aborted, vec![]);
    }
    assert!(window(State::new(), &[], r.take()).is_err());
    assert_eq!(window(State::new(), &[], vec![]).unwrap().horizon, 1);
    let (mut a, mut b) = (
        TransferWorkload::new(7, vec![1, 2]),
        TransferWorkload::new(7, vec![1, 2]),
    );
    for _ in 0..100 {
        assert_eq!(a.next_ops(), b.next_ops());
    }
    let before = r.now();
    assert!(Recorder::new().now() > before);
}

#[test]
fn invalid_windows_and_missing_control_candidates() {
    let h = window(State::new(), &[], vec![]).unwrap();
    assert!(controls::stale_final_read(&h).is_none());
    assert!(controls::aborted_but_visible(&h).is_none());
    assert!(controls::duplicate_apply(&h).is_none());
    let h = giggles_history::generate(1, 1).unwrap();
    let mut attempts = h.attempts.clone();
    attempts[0].invoke = u64::MAX;
    attempts[0].complete = Some(u64::MAX);
    assert!(window(h.initial.clone(), &[0, 1], attempts).is_err());
    assert!(window(h.initial.clone(), &[0, 0], h.attempts.clone()).is_err());
    let mut attempts = h.attempts.clone();
    attempts[0].id.clear();
    assert!(window(h.initial.clone(), &[0, 1], attempts).is_err());
    let full = giggles_history::generate(1, 128).unwrap();
    assert!(controls::duplicate_apply(&full).is_none());
}

#[test]
fn read_committed_safe_workload_and_stale_read_control() {
    for seed in 1..=8 {
        let h = simulation(seed, true);
        assert!(h.attempts.iter().all(|a| match a.ops.as_slice() {
            [
                Op::Transfer { from, to, .. },
                Op::Read { key: f },
                Op::Read { key: t },
            ] => f == from && t == to,
            [Op::Scan { .. }] => true,
            _ => false,
        }));
        verdict(&h, Verdict::Pass);
        for control in [
            controls::stale_read,
            controls::aborted_but_visible,
            controls::duplicate_apply,
        ] {
            let mutated = control(&h).expect("control candidate");
            assert_eq!(
                check(&mutated, Limits::default()).unwrap().verdict,
                Verdict::Fail
            );
        }
    }
}

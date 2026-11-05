mod common;
use common::*;
use giggles_transactions::*;

fn retry(
    h: &mut Harness,
    range: u64,
    command: Command,
    fault: (usize, Crash),
    step: &mut usize,
) -> Response {
    let crash = if *step == fault.0 {
        fault.1
    } else {
        Crash::None
    };
    *step += 1;
    match h.submit(range, command.clone(), crash) {
        Some(r) => r.unwrap(),
        None => h.ok(range, command),
    }
}

fn run(fault: (usize, Crash), outcome: Outcome) -> usize {
    let mut h = Harness::new(&[1, 2, 3]);
    let t = transaction(1);
    let mut step = 0;
    retry(
        &mut h,
        1,
        Command::OpenHome {
            transaction: t,
            digest: digest(1),
        },
        fault,
        &mut step,
    );
    for r in [2, 3] {
        retry(
            &mut h,
            1,
            Command::Register {
                transaction: t,
                digest: digest(1),
                participant: domain(r),
            },
            fault,
            &mut step,
        );
    }
    let Response::Seal(s) = retry(
        &mut h,
        1,
        Command::Seal {
            transaction: t,
            digest: digest(1),
        },
        fault,
        &mut step,
    ) else {
        panic!()
    };
    for r in [2, 3] {
        retry(&mut h, r, Command::Begin(s.clone()), fault, &mut step);
        retry(
            &mut h,
            r,
            Command::Acquire {
                transaction: t,
                lock: point(b"balance", Mode::Exclusive),
            },
            fault,
            &mut step,
        );
        retry(
            &mut h,
            r,
            Command::Stage {
                transaction: t,
                intent: Intent {
                    key: b"balance".to_vec(),
                    value: Some(vec![r as u8]),
                },
            },
            fault,
            &mut step,
        );
    }
    for r in [2, 3] {
        let Response::Prepared(p) = retry(
            &mut h,
            r,
            Command::Prepare { transaction: t },
            fault,
            &mut step,
        ) else {
            panic!()
        };
        assert_eq!(h.machine(r).phase(t), Some(Phase::Prepared));
        assert!(
            h.machine(r)
                .holds(t, &Span::Point(b"balance".to_vec()), Mode::Exclusive)
        );
        assert!(h.image(r).is_empty());
        retry(&mut h, 1, Command::RecordPrepare(p), fault, &mut step);
    }
    let Response::Decision(d) = retry(
        &mut h,
        1,
        Command::Decide {
            transaction: t,
            digest: digest(1),
            outcome,
        },
        fault,
        &mut step,
    ) else {
        panic!()
    };
    // Coordinator is gone; a fresh worker sees only the durable home record.
    h.restart();
    assert_eq!(h.machine(1).outcome(t), Some(&d));
    for r in [2, 3] {
        retry(&mut h, r, Command::Finalize(d.clone()), fault, &mut step);
        assert_eq!(
            h.machine(r).phase(t),
            Some(if outcome == Outcome::Commit {
                Phase::Committed
            } else {
                Phase::Aborted
            })
        );
        let expected = if outcome == Outcome::Commit {
            Some(vec![r as u8])
        } else {
            None
        };
        assert_eq!(h.image(r).get(b"balance".as_slice()), expected.as_ref());
    }
    h.restart();
    for r in [2, 3] {
        assert_eq!(
            h.ok(r, Command::Finalize(d.clone())),
            Response::Finalized {
                outcome,
                intents: vec![]
            }
        );
        assert!(!h.machine(r).has_unresolved_work());
    }
    step
}

#[test]
fn crash_before_apply_before_atomic_persist_and_after_persist_at_every_transition() {
    let steps = run((usize::MAX, Crash::None), Outcome::Commit);
    assert_eq!(steps, 17);
    for outcome in [Outcome::Commit, Outcome::Abort] {
        for boundary in 0..steps {
            for crash in [
                Crash::BeforeApply,
                Crash::BeforePersist,
                Crash::AfterPersist,
            ] {
                let _seed =
                    SeedGuard::new((boundary as u64) * 10 + crash as u64 + 1000 * (outcome as u64));
                assert_eq!(
                    run((boundary, crash), outcome),
                    steps,
                    "{boundary} {crash:?} {outcome:?}"
                );
            }
        }
    }
}

#[test]
fn unpersisted_prepare_never_becomes_home_commit_evidence() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(1);
    let s = h.begin(t, &[2]);
    let before = h.machine(2).applied_index();
    assert!(
        h.submit(2, Command::Prepare { transaction: t }, Crash::BeforePersist)
            .is_none()
    );
    assert_eq!(h.machine(2).phase(t), Some(Phase::Active));
    let invented = PrepareRef {
        seal: s,
        participant: domain(2),
        index: before + 1,
    };
    assert_eq!(
        h.apply(1, Command::RecordPrepare(invented)),
        Err(Error::MissingEvidence)
    );
    assert_eq!(
        h.apply(
            1,
            Command::Decide {
                transaction: t,
                digest: digest(1),
                outcome: Outcome::Commit
            }
        ),
        Err(Error::MissingParticipants)
    );
}

#[test]
fn unpersisted_home_decision_never_releases_prepared_participant() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(1);
    h.begin(t, &[2]);
    h.acquire(2, t, point(b"x", Mode::Exclusive)).unwrap();
    h.stage(2, t, b"x", Some(b"new"));
    h.vote(t, 2);
    let index = h.machine(1).applied_index() + 1;
    assert!(
        h.submit(
            1,
            Command::Decide {
                transaction: t,
                digest: digest(1),
                outcome: Outcome::Commit
            },
            Crash::BeforePersist
        )
        .is_none()
    );
    assert_eq!(
        h.apply(
            2,
            Command::Finalize(DecisionRef {
                transaction: t,
                digest: digest(1),
                index,
                outcome: Outcome::Commit
            })
        ),
        Err(Error::MissingEvidence)
    );
    assert_eq!(h.machine(2).phase(t), Some(Phase::Prepared));
    assert!(h.image(2).is_empty());
    let d = h.decide(t, Outcome::Abort);
    h.ok(2, Command::Finalize(d));
    assert!(h.image(2).is_empty());
}

#[test]
fn fast_finalize_cannot_publish_partial_transfer_to_a_locked_current_reader() {
    let mut h = Harness::new(&[1, 2, 3]);
    let writer = transaction(1);
    h.begin(writer, &[2, 3]);
    for r in [2, 3] {
        h.acquire(r, writer, point(b"balance", Mode::Exclusive))
            .unwrap();
    }
    h.stage(2, writer, b"balance", Some(b"90"));
    h.stage(3, writer, b"balance", Some(b"110"));
    h.vote(writer, 2);
    h.vote(writer, 3);
    let d = h.decide(writer, Outcome::Commit);
    h.ok(2, Command::Finalize(d.clone()));
    let reader = transaction(2);
    h.begin(reader, &[2, 3]);
    assert_eq!(
        h.acquire(2, reader, point(b"balance", Mode::Shared)),
        Ok(Response::LockGranted)
    );
    assert_eq!(
        h.acquire(3, reader, point(b"balance", Mode::Shared)),
        Ok(Response::Queued)
    );
    // Whole observation is withheld until every read lock exists. No value from
    // range2 is returned to a user while range3 remains prepared.
    assert!(
        !h.machine(3)
            .holds(reader, &Span::Point(b"balance".to_vec()), Mode::Shared)
    );
    h.restart();
    h.ok(3, Command::Finalize(d));
    assert!(
        h.machine(3)
            .holds(reader, &Span::Point(b"balance".to_vec()), Mode::Shared)
    );
    let total: u64 = [2, 3]
        .into_iter()
        .map(|r| {
            std::str::from_utf8(&h.image(r)[b"balance".as_slice()])
                .unwrap()
                .parse::<u64>()
                .unwrap()
        })
        .sum();
    assert_eq!(total, 200);
}

#[test]
fn crashed_finalize_cannot_install_subset_of_local_redo_or_lose_dedupe() {
    for fault in [Crash::BeforePersist, Crash::AfterPersist] {
        let mut h = Harness::new(&[1, 2]);
        let t = transaction(1);
        h.begin(t, &[2]);
        h.acquire(2, t, interval(b"a", b"z", Mode::Exclusive))
            .unwrap();
        for k in [b"b", b"c", b"d"] {
            h.stage(2, t, k, Some(b"1"));
        }
        h.vote(t, 2);
        let d = h.decide(t, Outcome::Commit);
        assert!(h.submit(2, Command::Finalize(d.clone()), fault).is_none());
        assert_eq!(
            h.image(2).len(),
            if fault == Crash::BeforePersist { 0 } else { 3 }
        );
        h.ok(2, Command::Finalize(d));
        assert_eq!(h.image(2).len(), 3);
    }
}

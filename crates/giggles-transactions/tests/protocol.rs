mod common;
use common::*;
use giggles_transactions::*;

#[test]
fn home_requires_exact_sealed_participants_and_durable_matching_prepares() {
    let mut h = Harness::new(&[1, 2, 3, 4]);
    let t = transaction(1);
    let seal = h.begin(t, &[2, 3]);
    assert_eq!(
        h.apply(
            1,
            Command::Register {
                transaction: t,
                digest: digest(1),
                participant: domain(4)
            }
        ),
        Err(Error::WrongPhase)
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
    let mut fake = PrepareRef {
        seal: seal.clone(),
        participant: domain(2),
        index: 999,
    };
    assert_eq!(
        h.apply(1, Command::RecordPrepare(fake.clone())),
        Err(Error::MissingEvidence)
    );
    fake.participant = domain(4);
    assert_eq!(
        h.apply(1, Command::RecordPrepare(fake)),
        Err(Error::Conflict)
    );
    h.vote(t, 2);
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
    h.vote(t, 3);
    let d = h.decide(t, Outcome::Commit);
    assert_eq!(h.decide(t, Outcome::Commit), d);
    assert_eq!(
        h.apply(
            1,
            Command::Decide {
                transaction: t,
                digest: digest(1),
                outcome: Outcome::Abort
            }
        ),
        Err(Error::Conflict)
    );
}

#[test]
fn duplicate_registration_seal_begin_prepare_vote_and_finalize_are_idempotent() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(2);
    let s = h.begin(t, &[2]);
    h.ok(
        1,
        Command::OpenHome {
            transaction: t,
            digest: digest(2),
        },
    );
    h.ok(
        1,
        Command::Register {
            transaction: t,
            digest: digest(2),
            participant: domain(2),
        },
    );
    assert_eq!(
        h.ok(
            1,
            Command::Seal {
                transaction: t,
                digest: digest(2)
            }
        ),
        Response::Seal(s.clone())
    );
    h.ok(2, Command::Begin(s));
    h.acquire(2, t, point(b"a", Mode::Exclusive)).unwrap();
    h.stage(2, t, b"a", Some(b"value"));
    h.stage(2, t, b"a", Some(b"value"));
    let p = h.vote(t, 2);
    assert_eq!(h.prepare(t, 2), p);
    h.ok(1, Command::RecordPrepare(p));
    let d = h.decide(t, Outcome::Commit);
    assert_eq!(
        h.ok(2, Command::Finalize(d.clone())),
        Response::Finalized {
            outcome: Outcome::Commit,
            intents: vec![Intent {
                key: b"a".to_vec(),
                value: Some(b"value".to_vec())
            }]
        }
    );
    assert_eq!(
        h.ok(2, Command::Finalize(d)),
        Response::Finalized {
            outcome: Outcome::Commit,
            intents: vec![]
        }
    );
    assert_eq!(h.image(2).get(b"a".as_slice()), Some(&b"value".to_vec()));
    assert!(!h.machine(2).has_unresolved_work());
}

#[test]
fn identity_digest_epoch_and_conflicting_retries_fail_closed() {
    let mut h = Harness::new(&[1, 2, 3]);
    let t = transaction(3);
    let s = h.begin(t, &[2]);
    assert_eq!(
        h.apply(
            1,
            Command::OpenHome {
                transaction: t,
                digest: digest(44)
            }
        ),
        Err(Error::Conflict)
    );
    let mut wrong = t;
    wrong.home.epoch += 1;
    assert_eq!(
        h.apply(
            1,
            Command::OpenHome {
                transaction: wrong,
                digest: digest(3)
            }
        ),
        Err(Error::WrongDomain)
    );
    assert_eq!(
        h.apply(3, Command::Begin(s.clone())),
        Err(Error::WrongDomain)
    );
    let mut wrong_seal = s.clone();
    wrong_seal.digest = digest(4);
    assert_eq!(h.apply(2, Command::Begin(wrong_seal)), Err(Error::Conflict));
    h.acquire(2, t, point(b"a", Mode::Exclusive)).unwrap();
    h.stage(2, t, b"a", Some(b"first"));
    assert_eq!(
        h.apply(
            2,
            Command::Stage {
                transaction: t,
                intent: Intent {
                    key: b"a".to_vec(),
                    value: Some(b"different".to_vec())
                }
            }
        ),
        Err(Error::Conflict)
    );
    let p = h.vote(t, 2);
    let mut wrong_prepare = p.clone();
    wrong_prepare.participant.epoch += 1;
    assert_eq!(
        h.apply(1, Command::RecordPrepare(wrong_prepare)),
        Err(Error::Conflict)
    );
    let d = h.decide(t, Outcome::Commit);
    let mut bad = d.clone();
    bad.digest = digest(4);
    assert_eq!(
        h.apply(2, Command::Finalize(bad)),
        Err(Error::MissingEvidence)
    );
    let mut bad = d.clone();
    bad.outcome = Outcome::Abort;
    assert_eq!(
        h.apply(2, Command::Finalize(bad)),
        Err(Error::MissingEvidence)
    );
    h.ok(2, Command::Finalize(d));
    assert_eq!(h.apply(2, Command::Begin(s)), Err(Error::Conflict));
}

#[test]
fn prepared_transactions_survive_coordinator_death_and_never_unilaterally_abort() {
    let mut h = Harness::new(&[1, 2, 3]);
    let t = transaction(4);
    h.begin(t, &[2, 3]);
    h.acquire(2, t, interval(b"a", b"z", Mode::Shared)).unwrap();
    h.vote(t, 2);
    for _ in 0..20 {
        h.restart();
        assert_eq!(
            h.apply(2, Command::Rollback { transaction: t }),
            Err(Error::WrongPhase)
        );
        assert!(
            h.machine(2)
                .holds(t, &Span::Point(b"m".to_vec()), Mode::Shared)
        );
        assert_eq!(h.machine(2).phase(t), Some(Phase::Prepared));
    }
    // An authenticated recovery worker may durably abort at the home, even with a
    // missing vote; the participant cannot make this choice locally.
    let d = h.decide(t, Outcome::Abort);
    h.ok(2, Command::Finalize(d.clone()));
    h.ok(3, Command::Finalize(d));
    assert!(!h.machine(2).has_unresolved_work());
    assert_eq!(
        h.apply(
            1,
            Command::Decide {
                transaction: t,
                digest: digest(4),
                outcome: Outcome::Commit
            }
        ),
        Err(Error::Conflict)
    );
}

#[test]
fn missing_evidence_and_reordered_messages_cannot_publish_or_release_locks() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(5);
    let s = h.seal(t, &[2]);
    let mut m = Machine::new(domain(2), Limits::default()).unwrap();
    assert_eq!(
        m.apply_committed(1, Command::Begin(s.clone()), &NoEvidence),
        Err(Error::MissingEvidence)
    );
    h.ok(2, Command::Begin(s));
    h.acquire(2, t, point(b"x", Mode::Exclusive)).unwrap();
    h.stage(2, t, b"x", Some(b"future"));
    let fake = DecisionRef {
        transaction: t,
        digest: digest(5),
        index: 100,
        outcome: Outcome::Commit,
    };
    assert_eq!(
        h.apply(2, Command::Finalize(fake)),
        Err(Error::MissingEvidence)
    );
    assert!(
        h.machine(2)
            .holds(t, &Span::Point(b"x".to_vec()), Mode::Exclusive)
    );
    assert!(h.image(2).is_empty());
    assert_eq!(
        h.apply(
            1,
            Command::Decide {
                transaction: t,
                digest: digest(5),
                outcome: Outcome::Commit
            }
        ),
        Err(Error::MissingParticipants)
    );
    h.ok(2, Command::Rollback { transaction: t });
    assert_eq!(
        h.apply(2, Command::Prepare { transaction: t }),
        Err(Error::WrongPhase)
    );
    assert!(h.image(2).is_empty());
}

#[test]
fn rollback_before_prepare_discards_every_intent_and_retains_retry_fence() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(6);
    let s = h.begin(t, &[2]);
    h.acquire(2, t, interval(b"a", b"z", Mode::Exclusive))
        .unwrap();
    h.stage(2, t, b"b", Some(b"1"));
    h.stage(2, t, b"c", None);
    h.ok(2, Command::Rollback { transaction: t });
    h.ok(2, Command::Rollback { transaction: t });
    h.restart();
    assert!(h.image(2).is_empty());
    assert!(
        !h.machine(2)
            .holds(t, &Span::Point(b"b".to_vec()), Mode::Shared)
    );
    assert_eq!(h.apply(2, Command::Begin(s)), Err(Error::Conflict));
}

#[test]
fn empty_home_abort_and_nonempty_seal_requirements() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(7);
    h.ok(
        1,
        Command::OpenHome {
            transaction: t,
            digest: digest(7),
        },
    );
    assert_eq!(
        h.apply(
            1,
            Command::Seal {
                transaction: t,
                digest: digest(7)
            }
        ),
        Err(Error::MissingParticipants)
    );
    h.decide(t, Outcome::Abort);
    assert_eq!(
        h.apply(
            1,
            Command::Register {
                transaction: t,
                digest: digest(7),
                participant: domain(2)
            }
        ),
        Err(Error::WrongPhase)
    );
}

#[test]
fn same_range_two_epochs_and_foreign_database_registration_rejected() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(8);
    h.ok(
        1,
        Command::OpenHome {
            transaction: t,
            digest: digest(8),
        },
    );
    h.ok(
        1,
        Command::Register {
            transaction: t,
            digest: digest(8),
            participant: domain(2),
        },
    );
    let mut stale = domain(2);
    stale.epoch += 1;
    assert_eq!(
        h.apply(
            1,
            Command::Register {
                transaction: t,
                digest: digest(8),
                participant: stale
            }
        ),
        Err(Error::Conflict)
    );
    let mut foreign = domain(3);
    foreign.database = DatabaseId([11; 16]);
    assert_eq!(
        h.apply(
            1,
            Command::Register {
                transaction: t,
                digest: digest(8),
                participant: foreign
            }
        ),
        Err(Error::WrongDomain)
    );
}

#[test]
fn committed_index_dedupe_and_order_do_not_depend_on_transient_view() {
    let mut m = Machine::new(domain(1), Limits::default()).unwrap();
    let c = Command::OpenHome {
        transaction: transaction(9),
        digest: digest(9),
    };
    assert_eq!(
        m.apply_committed(0, c.clone(), &NoEvidence),
        Err(Error::StaleIndex)
    );
    assert_eq!(
        m.apply_committed(7, c.clone(), &NoEvidence),
        Ok(Response::Applied)
    );
    assert_eq!(
        m.apply_committed(7, c.clone(), &NoEvidence),
        Ok(Response::Applied)
    );
    assert_eq!(m.apply_committed(6, c, &NoEvidence), Err(Error::StaleIndex));
    assert_eq!(
        m.apply_committed(
            7,
            Command::Rollback {
                transaction: transaction(9)
            },
            &NoEvidence
        ),
        Err(Error::ConflictingIndex)
    );
    let c = Command::Prepare {
        transaction: transaction(9),
    };
    assert_eq!(
        m.apply_committed(8, c.clone(), &NoEvidence),
        Err(Error::Missing)
    );
    assert_eq!(m.applied_index(), 8);
    assert_eq!(m.apply_committed(8, c, &NoEvidence), Err(Error::Missing));
}

#[test]
fn local_home_may_also_participate_in_one_atomic_decision() {
    let mut h = Harness::new(&[1]);
    let t = transaction(10);
    h.begin(t, &[1]);
    h.acquire(1, t, point(b"task/remaining", Mode::Exclusive))
        .unwrap();
    h.stage(1, t, b"task/remaining", Some(b"0"));
    h.vote(t, 1);
    let d = h.decide(t, Outcome::Commit);
    h.ok(1, Command::Finalize(d));
    assert_eq!(
        h.image(1).get(b"task/remaining".as_slice()),
        Some(&b"0".to_vec())
    );
    assert!(!h.machine(1).has_unresolved_work());
}

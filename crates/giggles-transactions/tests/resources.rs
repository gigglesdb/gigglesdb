mod common;
use common::*;
use giggles_transactions::*;

#[test]
fn waiter_and_lock_caps_fail_atomically_without_losing_fifo_reservations() {
    let limits = Limits {
        waiters: 1,
        lock_requests: 2,
        ..Limits::default()
    };
    let mut h = Harness::with_limits(&[1, 2], limits);
    for n in 1..=3 {
        h.begin(transaction(n), &[2]);
    }
    h.acquire(2, transaction(1), point(b"x", Mode::Exclusive))
        .unwrap();
    assert_eq!(
        h.acquire(2, transaction(2), point(b"x", Mode::Shared)),
        Ok(Response::Queued)
    );
    assert_eq!(
        h.acquire(2, transaction(3), point(b"x", Mode::Exclusive)),
        Err(Error::Capacity)
    );
    h.ok(
        2,
        Command::Rollback {
            transaction: transaction(1),
        },
    );
    assert!(
        h.machine(2)
            .holds(transaction(2), &Span::Point(b"x".to_vec()), Mode::Shared)
    );
    assert_eq!(
        h.acquire(2, transaction(3), point(b"x", Mode::Exclusive)),
        Ok(Response::Queued)
    );
}

#[test]
fn per_transaction_locks_intents_keys_values_and_total_redo_are_bounded() {
    let limits = Limits {
        locks_per_transaction: 1,
        intents_per_transaction: 2,
        key_bytes: 4,
        value_bytes: 3,
        intent_bytes_per_transaction: 5,
        ..Limits::default()
    };
    let mut h = Harness::with_limits(&[1, 2], limits);
    let t = transaction(1);
    h.begin(t, &[2]);
    assert_eq!(
        h.acquire(2, t, point(b"large", Mode::Exclusive)),
        Err(Error::Invalid)
    );
    assert_eq!(
        h.acquire(2, t, interval(b"x", b"a", Mode::Shared)),
        Err(Error::Invalid)
    );
    assert_eq!(
        h.acquire(2, t, point(b"", Mode::Shared)),
        Err(Error::Invalid)
    );
    h.acquire(2, t, interval(b"a", b"z", Mode::Exclusive))
        .unwrap();
    assert_eq!(
        h.acquire(2, t, point(b"b", Mode::Exclusive)),
        Err(Error::Capacity)
    );
    assert_eq!(
        h.apply(
            2,
            Command::Stage {
                transaction: t,
                intent: Intent {
                    key: b"b".to_vec(),
                    value: Some(b"long".to_vec())
                }
            }
        ),
        Err(Error::Capacity)
    );
    h.stage(2, t, b"b", Some(b"123"));
    assert_eq!(
        h.apply(
            2,
            Command::Stage {
                transaction: t,
                intent: Intent {
                    key: b"c".to_vec(),
                    value: Some(b"1".to_vec())
                }
            }
        ),
        Err(Error::Capacity)
    );
    h.stage(2, t, b"c", None);
    assert_eq!(
        h.apply(
            2,
            Command::Stage {
                transaction: t,
                intent: Intent {
                    key: b"d".to_vec(),
                    value: None
                }
            }
        ),
        Err(Error::Capacity)
    );
    h.vote(t, 2);
    let d = h.decide(t, Outcome::Commit);
    h.ok(2, Command::Finalize(d));
    assert_eq!(h.image(2).len(), 1);
}

#[test]
fn retention_caps_never_evict_home_outcomes_or_participant_dedupe() {
    let limits = Limits {
        transactions: 2,
        participants: 1,
        ..Limits::default()
    };
    let mut h = Harness::with_limits(&[1, 2, 3], limits);
    let t = transaction(1);
    h.begin(t, &[2]);
    h.vote(t, 2);
    let d = h.decide(t, Outcome::Commit);
    h.ok(2, Command::Finalize(d.clone()));
    h.seal(transaction(2), &[2]);
    assert_eq!(
        h.apply(
            1,
            Command::OpenHome {
                transaction: transaction(3),
                digest: digest(3)
            }
        ),
        Err(Error::Capacity)
    );
    assert_eq!(
        h.apply(
            1,
            Command::Register {
                transaction: transaction(2),
                digest: digest(2),
                participant: domain(3)
            }
        ),
        Err(Error::WrongPhase)
    );
    h.restart();
    assert_eq!(h.machine(1).outcome(t), Some(&d));
    h.ok(2, Command::Finalize(d));
}

#[test]
fn participant_cap_rejects_addition_before_seal() {
    let mut h = Harness::with_limits(
        &[1, 2, 3],
        Limits {
            participants: 1,
            ..Limits::default()
        },
    );
    let t = transaction(1);
    h.ok(
        1,
        Command::OpenHome {
            transaction: t,
            digest: digest(1),
        },
    );
    h.ok(
        1,
        Command::Register {
            transaction: t,
            digest: digest(1),
            participant: domain(2),
        },
    );
    assert_eq!(
        h.apply(
            1,
            Command::Register {
                transaction: t,
                digest: digest(1),
                participant: domain(3)
            }
        ),
        Err(Error::Capacity)
    );
    h.ok(
        1,
        Command::Seal {
            transaction: t,
            digest: digest(1),
        },
    );
}

#[test]
fn command_encoding_is_stable_bounded_and_rejects_trailing_or_truncated_data() {
    let limits = Limits::default();
    let c = Command::OpenHome {
        transaction: transaction(1),
        digest: digest(1),
    };
    let bytes = encode_command(&c, &limits).unwrap();
    assert_eq!(&bytes[..4], &[1, 0, 0, 0]);
    assert_eq!(decode_command(&bytes, &limits).unwrap(), c);
    assert_eq!(
        encode_command(&decode_command(&bytes, &limits).unwrap(), &limits).unwrap(),
        bytes
    );
    for end in 0..bytes.len() {
        assert!(decode_command(&bytes[..end], &limits).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(decode_command(&trailing, &limits), Err(Error::Codec));
    let mut future = bytes.clone();
    future[0] = 2;
    assert_eq!(decode_command(&future, &limits), Err(Error::Codec));
    let small = Limits {
        encoded_bytes: 1024,
        ..limits.clone()
    };
    assert_eq!(decode_command(&vec![0; 1025], &small), Err(Error::Capacity));
    let c = Command::Stage {
        transaction: transaction(1),
        intent: Intent {
            key: vec![1],
            value: Some(vec![1; 2048]),
        },
    };
    assert_eq!(encode_command(&c, &small), Err(Error::Capacity));
    // An absurd nested sequence length must return an error under the byte limit.
    let mut malformed = encode_command(&c, &limits).unwrap();
    malformed.truncate(80);
    malformed.extend([255; 16]);
    assert!(decode_command(&malformed, &small).is_err());
}

#[test]
fn snapshots_reject_corruption_truncation_foreign_domain_limits_and_future_epoch() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(1);
    h.begin(t, &[2]);
    h.acquire(2, t, point(b"x", Mode::Exclusive)).unwrap();
    h.stage(2, t, b"x", Some(b"1"));
    h.vote(t, 2);
    let snapshot = h.machine(2).snapshot().unwrap();
    for i in [0, 4, snapshot.len() / 2, snapshot.len() - 1] {
        let mut bad = snapshot.clone();
        bad[i] ^= 0x55;
        assert_eq!(
            Machine::restore(domain(2), Limits::default(), &bad),
            Err(Error::InvalidSnapshot)
        );
    }
    for end in [0, 1, 31, snapshot.len() - 1] {
        assert_eq!(
            Machine::restore(domain(2), Limits::default(), &snapshot[..end]),
            Err(Error::InvalidSnapshot)
        );
    }
    assert_eq!(
        Machine::restore(domain(3), Limits::default(), &snapshot),
        Err(Error::InvalidSnapshot)
    );
    let mut epoch = domain(2);
    epoch.epoch += 1;
    assert_eq!(
        Machine::restore(epoch, Limits::default(), &snapshot),
        Err(Error::InvalidSnapshot)
    );
    assert_eq!(
        Machine::restore(
            domain(2),
            Limits {
                transactions: 2,
                ..Limits::default()
            },
            &snapshot
        ),
        Err(Error::InvalidSnapshot)
    );
    assert_eq!(
        Machine::restore(domain(2), Limits::default(), &snapshot)
            .unwrap()
            .phase(t),
        Some(Phase::Prepared)
    );
}

#[test]
fn invalid_identity_and_limit_values_are_rejected() {
    assert_eq!(
        Machine::new(domain(0), Limits::default()),
        Err(Error::WrongDomain)
    );
    assert_eq!(
        Machine::new(
            domain(1),
            Limits {
                waiters: 0,
                ..Limits::default()
            }
        ),
        Err(Error::Invalid)
    );
    let mut h = Harness::new(&[1]);
    assert_eq!(
        h.apply(
            1,
            Command::OpenHome {
                transaction: transaction(0),
                digest: digest(0)
            }
        ),
        Err(Error::WrongDomain)
    );
}

#[test]
fn bounded_snapshot_admission_preserves_space_for_every_admitted_resolution() {
    for cap in (2048..=8192).step_by(256) {
        let mut h = Harness::with_limits(
            &[1, 2],
            Limits {
                encoded_bytes: cap,
                ..Limits::default()
            },
        );
        let t = transaction(1);
        h.begin(t, &[2]);
        h.acquire(2, t, interval(b"a", b"z", Mode::Exclusive))
            .unwrap();
        // Fill redo until either bytes or per-transaction admission refuses growth.
        for n in 0..32 {
            let c = Command::Stage {
                transaction: t,
                intent: Intent {
                    key: format!("b{n:02}").into_bytes(),
                    value: Some(vec![1; 96]),
                },
            };
            match h.apply(2, c) {
                Ok(_) => {}
                Err(Error::Capacity) => break,
                other => panic!("{cap} {other:?}"),
            }
        }
        h.vote(t, 2);
        // Competing retained work cannot consume the reserved decision headroom.
        for n in 2..64 {
            match h.apply(
                1,
                Command::OpenHome {
                    transaction: transaction(n),
                    digest: digest(n),
                },
            ) {
                Ok(_) => {}
                Err(Error::Capacity) => break,
                other => panic!("{cap} {other:?}"),
            }
        }
        let d = h.decide(t, Outcome::Commit);
        h.ok(2, Command::Finalize(d.clone()));
        h.ok(2, Command::Finalize(d));
        assert_eq!(h.machine(2).phase(t), Some(Phase::Committed), "cap{cap}");
        assert!(h.machine(2).snapshot().unwrap().len() <= cap);
    }
}

#[test]
fn participant_uses_its_own_limits_even_when_home_allows_more_members() {
    let mut h = Harness::new(&[1, 2, 3]);
    let s = h.seal(transaction(1), &[2, 3]);
    let mut limited = Machine::new(
        domain(2),
        Limits {
            participants: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(
        limited.apply_committed(1, Command::Begin(s), &h.durable),
        Err(Error::Capacity)
    );
    assert_eq!(limited.phase(transaction(1)), None);
}

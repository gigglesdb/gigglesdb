mod common;
use common::*;
use giggles_transactions::*;

#[test]
fn shared_predicates_and_gaps_block_phantom_insert_update_and_delete() {
    for writer_key in [b"b", b"m", b"y"] {
        let mut h = Harness::new(&[1, 2]);
        let a = transaction(1);
        let b = transaction(2);
        h.begin(a, &[2]);
        h.begin(b, &[2]);
        h.acquire(2, a, interval(b"b", b"z", Mode::Shared)).unwrap();
        assert_eq!(
            h.acquire(2, b, point(writer_key, Mode::Exclusive)),
            Ok(Response::Queued)
        );
        assert_eq!(
            h.apply(
                2,
                Command::Stage {
                    transaction: b,
                    intent: Intent {
                        key: writer_key.to_vec(),
                        value: None
                    }
                }
            ),
            Err(Error::Waiting)
        );
        assert_eq!(
            h.apply(2, Command::Prepare { transaction: b }),
            Err(Error::Waiting)
        );
        h.vote(a, 2);
        let d = h.decide(a, Outcome::Commit);
        assert!(
            h.machine(2)
                .holds(a, &Span::Point(writer_key.to_vec()), Mode::Shared)
        );
        h.ok(2, Command::Finalize(d));
        assert!(
            h.machine(2)
                .holds(b, &Span::Point(writer_key.to_vec()), Mode::Exclusive)
        );
        h.stage(2, b, writer_key, None);
    }
}

#[test]
fn half_open_intervals_adjacent_gaps_and_point_boundaries_are_exact() {
    let mut h = Harness::new(&[1, 2]);
    let a = transaction(1);
    let b = transaction(2);
    h.begin(a, &[2]);
    h.begin(b, &[2]);
    h.acquire(2, a, interval(b"b", b"m", Mode::Shared)).unwrap();
    assert_eq!(
        h.acquire(2, b, interval(b"m", b"z", Mode::Exclusive)),
        Ok(Response::LockGranted)
    );
    assert!(
        h.machine(2)
            .holds(a, &Span::Point(b"b".to_vec()), Mode::Shared)
    );
    assert!(
        !h.machine(2)
            .holds(a, &Span::Point(b"m".to_vec()), Mode::Shared)
    );
    assert!(
        h.machine(2)
            .holds(b, &Span::Point(b"m".to_vec()), Mode::Exclusive)
    );
    assert_eq!(
        h.acquire(2, b, point(b"c", Mode::Exclusive)),
        Err(Error::MustRollback)
    );
}

#[test]
fn compatible_shared_readers_and_uncontended_upgrade() {
    let mut h = Harness::new(&[1, 2]);
    let a = transaction(1);
    let b = transaction(2);
    h.begin(a, &[2]);
    h.begin(b, &[2]);
    h.acquire(2, a, point(b"x", Mode::Shared)).unwrap();
    h.acquire(2, b, interval(b"a", b"z", Mode::Shared)).unwrap();
    assert_eq!(
        h.acquire(2, a, point(b"x", Mode::Exclusive)),
        Err(Error::MustRollback)
    );
    h.ok(2, Command::Rollback { transaction: b });
    assert_eq!(
        h.acquire(2, a, point(b"x", Mode::Exclusive)),
        Ok(Response::LockGranted)
    );
    h.stage(2, a, b"x", Some(b"new"));
    h.vote(a, 2);
    assert_eq!(
        h.acquire(2, a, point(b"y", Mode::Shared)),
        Err(Error::WrongPhase)
    );
    assert_eq!(
        h.acquire(2, a, point(b"x", Mode::Exclusive)),
        Ok(Response::LockGranted)
    );
}

#[test]
fn two_upgrade_and_ab_ba_deadlock_schedules_require_rollback_without_wounding() {
    for upgrades in [false, true] {
        let mut h = Harness::new(&[1, 2]);
        let a = transaction(1);
        let b = transaction(2);
        h.begin(a, &[2]);
        h.begin(b, &[2]);
        let (ka, kb, initial) = if upgrades {
            (b"x", b"x", Mode::Shared)
        } else {
            (b"a", b"b", Mode::Exclusive)
        };
        h.acquire(2, a, point(ka, initial)).unwrap();
        h.acquire(2, b, point(kb, initial)).unwrap();
        assert_eq!(
            h.acquire(2, a, point(kb, Mode::Exclusive)),
            Err(Error::MustRollback)
        );
        assert_eq!(
            h.acquire(2, b, point(ka, Mode::Exclusive)),
            Err(Error::MustRollback)
        );
        h.ok(2, Command::Rollback { transaction: b });
        h.acquire(2, a, point(kb, Mode::Exclusive)).unwrap();
        h.vote(a, 2);
        assert_eq!(
            h.apply(2, Command::Rollback { transaction: a }),
            Err(Error::WrongPhase)
        );
    }
}

#[test]
fn fifo_prevents_new_readers_bypassing_waiting_writer_and_survives_restart() {
    let mut h = Harness::new(&[1, 2]);
    for n in 1..=4 {
        h.begin(transaction(n), &[2]);
    }
    h.acquire(2, transaction(1), point(b"x", Mode::Shared))
        .unwrap();
    assert_eq!(
        h.acquire(2, transaction(2), point(b"x", Mode::Exclusive)),
        Ok(Response::Queued)
    );
    assert_eq!(
        h.acquire(2, transaction(3), point(b"x", Mode::Shared)),
        Ok(Response::Queued)
    );
    // Conservative global FIFO also keeps unrelated newcomers from bypassing.
    assert_eq!(
        h.acquire(2, transaction(4), point(b"z", Mode::Exclusive)),
        Ok(Response::Queued)
    );
    h.restart();
    h.ok(
        2,
        Command::Rollback {
            transaction: transaction(1),
        },
    );
    assert!(
        h.machine(2)
            .holds(transaction(2), &Span::Point(b"x".to_vec()), Mode::Exclusive)
    );
    assert!(
        !h.machine(2)
            .holds(transaction(3), &Span::Point(b"x".to_vec()), Mode::Shared)
    );
    h.ok(
        2,
        Command::Rollback {
            transaction: transaction(2),
        },
    );
    assert!(
        h.machine(2)
            .holds(transaction(3), &Span::Point(b"x".to_vec()), Mode::Shared)
    );
    assert!(
        h.machine(2)
            .holds(transaction(4), &Span::Point(b"z".to_vec()), Mode::Exclusive)
    );
}

#[test]
fn queued_retry_is_idempotent_and_cancellation_does_not_skip_live_head() {
    let mut h = Harness::new(&[1, 2]);
    for n in 1..=4 {
        h.begin(transaction(n), &[2]);
    }
    h.acquire(2, transaction(1), point(b"x", Mode::Exclusive))
        .unwrap();
    for n in 2..=4 {
        assert_eq!(
            h.acquire(2, transaction(n), point(b"x", Mode::Exclusive)),
            Ok(Response::Queued)
        );
    }
    for _ in 0..5 {
        assert_eq!(
            h.acquire(2, transaction(2), point(b"x", Mode::Exclusive)),
            Ok(Response::Queued)
        );
    }
    assert_eq!(
        h.acquire(2, transaction(2), point(b"y", Mode::Shared)),
        Err(Error::Conflict)
    );
    h.ok(
        2,
        Command::Rollback {
            transaction: transaction(3),
        },
    );
    h.ok(
        2,
        Command::Rollback {
            transaction: transaction(1),
        },
    );
    assert!(
        h.machine(2)
            .holds(transaction(2), &Span::Point(b"x".to_vec()), Mode::Exclusive)
    );
    assert!(
        !h.machine(2)
            .holds(transaction(4), &Span::Point(b"x".to_vec()), Mode::Exclusive)
    );
    h.ok(
        2,
        Command::Rollback {
            transaction: transaction(2),
        },
    );
    assert!(
        h.machine(2)
            .holds(transaction(4), &Span::Point(b"x".to_vec()), Mode::Exclusive)
    );
}

#[test]
fn prepared_holder_waiters_cannot_timeout_it_and_durable_abort_releases_fifo() {
    let mut h = Harness::new(&[1, 2]);
    h.begin(transaction(1), &[2]);
    h.begin(transaction(2), &[2]);
    h.acquire(2, transaction(1), interval(b"a", b"z", Mode::Exclusive))
        .unwrap();
    h.vote(transaction(1), 2);
    assert_eq!(
        h.acquire(2, transaction(2), point(b"m", Mode::Shared)),
        Ok(Response::Queued)
    );
    for _ in 0..8 {
        h.restart();
        assert_eq!(
            h.apply(
                2,
                Command::Rollback {
                    transaction: transaction(1)
                }
            ),
            Err(Error::WrongPhase)
        );
    }
    let d = h.decide(transaction(1), Outcome::Abort);
    h.ok(2, Command::Finalize(d));
    assert!(
        h.machine(2)
            .holds(transaction(2), &Span::Point(b"m".to_vec()), Mode::Shared)
    );
}

#[test]
fn noncovering_or_shared_locks_cannot_stage_writes() {
    let mut h = Harness::new(&[1, 2]);
    let t = transaction(1);
    h.begin(t, &[2]);
    h.acquire(2, t, interval(b"a", b"m", Mode::Exclusive))
        .unwrap();
    h.acquire(2, t, interval(b"m", b"z", Mode::Shared)).unwrap();
    for k in [b"m", b"z"] {
        assert_eq!(
            h.apply(
                2,
                Command::Stage {
                    transaction: t,
                    intent: Intent {
                        key: k.to_vec(),
                        value: Some(vec![])
                    }
                }
            ),
            Err(Error::WrongPhase)
        );
    }
    h.stage(2, t, b"b", Some(b""));
    h.vote(t, 2);
    let d = h.decide(t, Outcome::Commit);
    h.ok(2, Command::Finalize(d));
    assert_eq!(h.image(2).get(b"b".as_slice()), Some(&vec![]));
}

#[test]
fn exhaustive_small_span_conflicts_match_independent_set_oracle() {
    let mut spans = Vec::new();
    for k in 1..=4 {
        spans.push(Span::Point(vec![k]));
    }
    for a in 1..=4 {
        for b in a + 1..=5 {
            spans.push(Span::Interval {
                start: vec![a],
                end: vec![b],
            });
        }
    }
    for a in &spans {
        for b in &spans {
            let expected = (0..=6).any(|k| a.contains(&[k]) && b.contains(&[k]));
            assert_eq!(a.overlaps(b), expected, "{a:?} {b:?}");
            assert_eq!(a.overlaps(b), b.overlaps(a));
        }
    }
}

#[test]
fn opposite_range_acquisition_cannot_form_a_distributed_wait_cycle() {
    let mut h = Harness::new(&[1, 2, 3]);
    let low = transaction(1);
    let high = transaction(2);
    h.begin(low, &[2, 3]);
    h.begin(high, &[2, 3]);
    h.acquire(2, low, point(b"x", Mode::Exclusive)).unwrap();
    h.acquire(3, high, point(b"x", Mode::Exclusive)).unwrap();
    // Locally lock-free low-priority transaction still cannot wait on high.
    assert_eq!(
        h.acquire(3, low, point(b"x", Mode::Exclusive)),
        Err(Error::MustRollback)
    );
    assert_eq!(
        h.acquire(2, high, point(b"x", Mode::Exclusive)),
        Ok(Response::Queued)
    );
    h.ok(2, Command::Rollback { transaction: low });
    h.ok(3, Command::Rollback { transaction: low });
    assert!(
        h.machine(2)
            .holds(high, &Span::Point(b"x".to_vec()), Mode::Exclusive)
    );
    h.restart();
}

#[test]
fn fifo_predecessor_edges_also_obey_global_priority_order() {
    let mut h = Harness::new(&[1, 2]);
    for n in 1..=4 {
        h.begin(transaction(n), &[2]);
    }
    h.acquire(2, transaction(1), point(b"x", Mode::Exclusive))
        .unwrap();
    assert_eq!(
        h.acquire(2, transaction(3), point(b"x", Mode::Exclusive)),
        Ok(Response::Queued)
    );
    assert_eq!(
        h.acquire(2, transaction(2), point(b"y", Mode::Exclusive)),
        Err(Error::MustRollback)
    );
    assert_eq!(
        h.acquire(2, transaction(4), point(b"y", Mode::Exclusive)),
        Ok(Response::Queued)
    );
}

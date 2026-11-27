use super::*;

fn pending(id: u64) -> Pending {
    let (reply, _) = mpsc::channel();
    Pending {
        request: CommitRequest {
            snapshot: Ts(id),
            writes: BTreeMap::from([
                ((Keyspace::Rows, vec![1, 2]), Some(vec![3, 4, 5])),
                ((Keyspace::Rows, vec![6]), None),
            ]),
            features_or: 0,
            minimum_subversion: 0,
            raw: vec![
                RawOp::DeleteCounter { key: vec![7, 8] },
                RawOp::PutCounter {
                    key: vec![9],
                    value: vec![10, 11, 12],
                },
            ],
            validation: None,
        },
        guard: None,
        reply,
    }
}

fn ids(group: &[Message]) -> Vec<u64> {
    group.iter().map(|p| match p { Message::Commit(p) => p.request.snapshot.0, _ => panic!("expected commit") }).collect()
}

#[test]
fn request_size_counts_write_and_raw_keys_and_values() {
    assert_eq!(request_bytes(&pending(0).request), 12);
    let mut empty = pending(0).request;
    empty.writes.clear();
    empty.raw.clear();
    assert_eq!(request_bytes(&empty), 0);
}

#[test]
fn unlimited_cap_preserves_additional_request_limit() {
    assert_eq!(SequencerConfig::default().group_max_bytes, None);
    for group_max in [0, 1, 3, 64] {
        let (send, receive) = mpsc::channel();
        for id in 1..=4 {
            send.send(Message::Commit(pending(id))).unwrap();
        }
        let (group, deferred, shutdown) = gather(
            pending(0),
            &receive,
            &SequencerConfig {
                group_max,
                ..Default::default()
            },
        );
        assert_eq!(
            ids(&group),
            (0..=group_max.min(4) as u64).collect::<Vec<_>>()
        );
        assert!(deferred.is_none());
        assert!(!shutdown);
        assert_eq!(receive.try_iter().count(), 4 - group_max.min(4));
    }
}

#[test]
fn capped_groups_defer_overflow_and_preserve_order() {
    let (send, receive) = mpsc::channel();
    for id in 1..=3 {
        send.send(Message::Commit(pending(id))).unwrap();
    }
    let config = SequencerConfig {
        group_max_bytes: Some(24),
        ..Default::default()
    };
    let (group, deferred, shutdown) = gather(pending(0), &receive, &config);
    assert_eq!(ids(&group), vec![0, 1]);
    assert_eq!(
        group
            .iter()
            .map(Message::bytes)
            .sum::<usize>(),
        24
    );
    assert!(!shutdown);
    let Some(Message::Commit(next)) = deferred else {
        panic!("expected deferred commit");
    };
    assert_eq!(next.request.snapshot, Ts(2));
    let (group, deferred, shutdown) = gather(next, &receive, &config);
    assert_eq!(ids(&group), vec![2, 3]);
    assert_eq!(
        group
            .iter()
            .map(Message::bytes)
            .sum::<usize>(),
        24
    );
    assert!(deferred.is_none());
    assert!(!shutdown);
}

#[test]
fn oversized_first_request_is_kept_alone() {
    for cap in [0, 11] {
        let (send, receive) = mpsc::channel();
        send.send(Message::Commit(pending(1))).unwrap();
        let (group, deferred, shutdown) = gather(
            pending(0),
            &receive,
            &SequencerConfig {
                group_max_bytes: Some(cap),
                ..Default::default()
            },
        );
        assert_eq!(ids(&group), vec![0]);
        assert!(matches!(deferred, Some(Message::Commit(p)) if p.request.snapshot == Ts(1)));
        assert!(!shutdown);
    }
}

#[test]
fn cap_preserves_counter_and_shutdown_handling() {
    for shutdown in [false, true] {
        let (send, receive) = mpsc::channel();
        send.send(Message::Commit(pending(1))).unwrap();
        let (reply, counter_reply) = mpsc::channel();
        send.send(if shutdown {
            Message::Shutdown
        } else {
            Message::Counter {
                key: vec![1],
                value: vec![2],
                features_or: 4,
                reply: reply.into(),
            }
        })
        .unwrap();
        send.send(Message::Commit(pending(2))).unwrap();
        let (group, deferred, stopping) = gather(
            pending(0),
            &receive,
            &SequencerConfig {
                group_max_bytes: Some(24),
                ..Default::default()
            },
        );
        assert_eq!(ids(&group), vec![0, 1]);
        assert_eq!(stopping, shutdown);
        if shutdown {
            assert!(deferred.is_none());
        } else {
            let Some(Message::Counter {
                key,
                value,
                features_or,
                reply,
            }) = deferred
            else {
                panic!("expected deferred counter");
            };
            assert_eq!(key, vec![1]);
            assert_eq!(value, vec![2]);
            assert_eq!(features_or, 4);
            reply.send(Ok(())).unwrap();
            assert!(counter_reply.recv().unwrap().is_ok());
        }
        assert!(
            matches!(receive.try_recv(), Ok(Message::Commit(p)) if p.request.snapshot == Ts(2))
        );
    }
}

fn guarded(id: u64) -> Pending {
    let mut p = pending(id);
    p.guard = Some(CommitGuard::capture(Ts(id), Instant::now() + Duration::from_secs(60)));
    p
}

#[test]
fn oversized_guarded_first_leaves_ordinary_queue_untouched() {
    let (send, receive) = mpsc::channel();
    send.send(Message::Commit(pending(1))).unwrap();
    let config = SequencerConfig { group_max_bytes: Some(1), ..Default::default() };
    let (group, deferred, shutdown) = gather(guarded(0), &receive, &config);
    assert_eq!(ids(&group), [0]);
    assert!(deferred.is_none());
    assert!(!shutdown);
    assert!(matches!(receive.try_recv(), Ok(Message::Commit(p)) if p.request.snapshot == Ts(1)));
}

#[test]
fn fitting_ordinaries_defer_guard_before_ordinary_tail() {
    let (send, receive) = mpsc::channel();
    for p in [pending(1), pending(2), guarded(3), pending(4)] {
        send.send(Message::Commit(p)).unwrap();
    }
    let config = SequencerConfig { group_max_bytes: Some(48), ..Default::default() };
    let (group, deferred, shutdown) = gather(pending(0), &receive, &config);
    assert_eq!(ids(&group), [0, 1, 2]);
    assert!(matches!(deferred, Some(Message::Commit(p)) if p.guard.is_some() && p.request.snapshot == Ts(3)));
    assert!(!shutdown);
    assert!(matches!(receive.try_recv(), Ok(Message::Commit(p)) if p.request.snapshot == Ts(4)));
}

#[test]
fn overflow_ordinary_precedes_guard_and_both_remain_singletons() {
    let (send, receive) = mpsc::channel();
    let mut oversized = pending(1);
    oversized.request.writes.insert((Keyspace::Rows, vec![0; 30]), None);
    send.send(Message::Commit(oversized)).unwrap();
    send.send(Message::Commit(guarded(2))).unwrap();
    let config = SequencerConfig { group_max_bytes: Some(24), ..Default::default() };
    let (group, next, shutdown) = gather(pending(0), &receive, &config);
    assert_eq!(ids(&group), [0]);
    assert!(!shutdown);
    let Some(Message::Commit(next)) = next else { panic!("expected oversized ordinary") };
    assert_eq!(next.request.snapshot, Ts(1));
    assert!(request_bytes(&next.request) > 24);
    let (group, next, shutdown) = gather(next, &receive, &config);
    assert_eq!(ids(&group), [1]);
    assert!(!shutdown);
    let Some(Message::Commit(next)) = next else { panic!("expected guarded commit") };
    assert_eq!(next.request.snapshot, Ts(2));
    assert!(next.guard.is_some());
    let (group, deferred, shutdown) = gather(next, &receive, &config);
    assert_eq!(ids(&group), [2]);
    assert!(deferred.is_none());
    assert!(!shutdown);
}

#[test]
fn sequencer_accepts_oversized_guarded_request_with_small_cap() {
    let (mem, old) = super::guard_tests::fixture();
    drop(old);
    let seq = Sequencer::open(
        Arc::new(Kv::new(mem)),
        Arc::new(ManualClock::new(10)),
        SequencerConfig { group_max_bytes: Some(1), ..Default::default() },
    ).unwrap();
    let mut request = super::guard_tests::request(0);
    request.writes.insert((Keyspace::Rows, 1u64.to_be_bytes().to_vec()), Some(vec![42]));
    assert!(request_bytes(&request) > 1);
    let guard = CommitGuard::capture(Ts(0), Instant::now() + Duration::from_secs(60));
    let worker = seq.clone();
    assert!(thread::spawn(move || worker.submit_guarded(request, guard)).join().unwrap().is_ok());
    drop(seq);
}

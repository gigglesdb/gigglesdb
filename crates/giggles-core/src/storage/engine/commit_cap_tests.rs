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
            raw: vec![
                RawOp::DeleteCounter { key: vec![7, 8] },
                RawOp::PutCounter {
                    key: vec![9],
                    value: vec![10, 11, 12],
                },
            ],
            validation: None,
        },
        reply,
    }
}

fn ids(group: &[Pending]) -> Vec<u64> {
    group.iter().map(|p| p.request.snapshot.0).collect()
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
            .map(|p| request_bytes(&p.request))
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
            .map(|p| request_bytes(&p.request))
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
                reply,
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

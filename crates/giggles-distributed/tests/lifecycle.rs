mod common;
use common::*;
use giggles_core::{Key, RowId, TenantId};
use giggles_distributed::*;

#[test]
fn half_open_full_key_space_and_directory_rejects_gaps_overlaps_duplicates() {
    let s = split();
    let d = Directory {
        revision: 2,
        routes: vec![s.left.clone(), s.right.clone()],
        intents: Default::default(),
    };
    d.validate().unwrap();
    for (k, r) in [
        (minimum(), 11),
        (key(49), 11),
        (key(50), 12),
        (
            Key {
                tenant: TenantId(u64::MAX),
                row: RowId(u64::MAX),
            },
            12,
        ),
    ] {
        assert_eq!(d.route(k).unwrap().range, r);
    }
    for change in 0..7 {
        let mut bad = d.clone();
        match change {
            0 => bad.routes[1].bounds.start = key(51),
            1 => bad.routes[1].bounds.start = key(49),
            2 => bad.routes[1].range = 11,
            3 => bad.routes[1].group = 3,
            4 => bad.routes[0].bounds.start = key(1),
            5 => bad.routes[1].bounds.end = Some(key(999)),
            _ => bad.routes[0].voters = vec![1, 2],
        };
        assert!(bad.validate().is_err());
    }
}
#[test]
fn every_command_checks_domain_group_epoch_bounds_and_tasks_are_atomic() {
    let mut m = initial(2);
    let before = m.clone();
    for change in 0..6 {
        let mut c = command(2, put(descriptor(), 1, key(1), None));
        match change {
            0 => c.domain = 43,
            1 => c.group = 3,
            2 => {
                if let Action::Put { route, .. } = &mut c.action {
                    route.epoch += 1
                }
            }
            3 => {
                if let Action::Put { route, .. } = &mut c.action {
                    route.group = 3
                }
            }
            4 => c.action = Action::TaskWrite,
            _ => c.action = Action::Move,
        };
        assert_ne!(m.apply(&c, 1).code, 0);
        assert_eq!(m, before);
    }
    let mut route = descriptor();
    route.bounds.end = Some(key(50));
    let mut narrow = Machine::range(42, route.clone()).unwrap();
    assert_ne!(
        narrow
            .apply(&command(2, put(route.clone(), 1, key(50), None)), 1)
            .code,
        0
    );
    if let Machine::Range { data, .. } = &narrow {
        assert!(
            data.check(
                &route,
                &Bounds {
                    start: key(49),
                    end: Some(key(51))
                }
            )
            .is_err()
        );
    }
    let reply = narrow.apply(&command(2, put(route, 1, key(49), None)), 2);
    assert_eq!(reply.code, 0, "{:?}", reply);
}
#[test]
fn stale_positive_negative_cache_and_redirect_limit() {
    let mut cache = RouteCache::default();
    assert!(cache.hint(key(1)).is_none());
    let Machine::Root { directory: old, .. } = initial(1) else {
        panic!()
    };
    cache.refresh(&old).unwrap();
    assert_eq!(cache.hint(key(99)).unwrap().range, 10);
    let s = split();
    let new = Directory {
        revision: 2,
        routes: vec![s.left, s.right],
        intents: Default::default(),
    };
    let mut count = 0;
    let r = cache
        .resolve(
            key(99),
            || {
                count += 1;
                Ok(new.clone())
            },
            |r| r.epoch == 2,
        )
        .unwrap();
    assert_eq!(r.range, 12);
    assert_eq!(count, 1);
    count = 0;
    assert!(
        cache
            .resolve(
                key(1),
                || {
                    count += 1;
                    Ok(old.clone())
                },
                |_| false
            )
            .is_err()
    );
    assert_eq!(count, 3);
}
#[test]
fn lifecycle_restarts_at_every_boundary_preserve_independent_data_oracle() {
    for seed in [1, 7, 91] {
        for boundary in 0..8 {
            let mut c = Cluster::new(seed);
            for k in [0, 1, 49, 50, 51, 99] {
                c.apply(2, put(descriptor(), k + 1, key(k), None));
            }
            c.apply(2, put(descriptor(), 1000, key(1), Some(0)));
            let oracle = c.data(2).image.snapshot().unwrap().rows;
            let s = split();
            let mut step = 0;
            let checkpoint = |c: &mut Cluster, step: &mut usize| {
                if *step == boundary {
                    c.restart();
                }
                *step += 1;
            };
            checkpoint(&mut c, &mut step);
            c.apply(1, Action::Begin(s.clone()));
            checkpoint(&mut c, &mut step);
            c.apply(2, Action::Fence(s.clone()));
            checkpoint(&mut c, &mut step);
            assert_ne!(
                c.submit(2, 1, put(descriptor(), 2000, key(1), Some(1))).1,
                Some(0)
            );
            let children = c.data(2).children().unwrap();
            let proofs = [
                children[0].provenance.clone().unwrap(),
                children[1].provenance.clone().unwrap(),
            ];
            for child in children {
                c.apply(child.descriptor.group, Action::Bootstrap(Box::new(child)));
                checkpoint(&mut c, &mut step);
            }
            assert_ne!(
                c.submit(3, 1, put(s.left.clone(), 2001, key(2), None)).1,
                Some(0)
            );
            c.apply(
                1,
                Action::Publish {
                    id: s.id,
                    ready: Box::new(proofs.clone()),
                },
            );
            checkpoint(&mut c, &mut step);
            for proof in proofs.clone() {
                c.apply(proof.child.group, Action::Activate { proof, revision: 2 });
                checkpoint(&mut c, &mut step);
            }
            c.restart();
            let actual = c
                .data(3)
                .image
                .snapshot()
                .unwrap()
                .rows
                .into_iter()
                .chain(c.data(4).image.snapshot().unwrap().rows)
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(oracle, actual);
            for g in [3, 4] {
                for id in VOTERS {
                    assert_eq!(c.machine(g, 1), c.machine(g, id));
                }
                let snap = c.data(g).image.snapshot().unwrap();
                assert!(
                    snap.tasks.is_empty()
                        && snap.operations.is_empty()
                        && snap.approvals.is_empty()
                );
            }
            c.apply(1, Action::Begin(s.clone()));
            c.apply(
                1,
                Action::Publish {
                    id: s.id,
                    ready: Box::new(proofs),
                },
            );
            let mut conflict = s;
            conflict.left.range = 13;
            conflict.left.group = 5;
            conflict.right.range = 14;
            conflict.right.group = 6;
            assert_ne!(c.submit(1, 1, Action::Begin(conflict)).1, Some(0));
            std::fs::write(
                c.root.join("oracle.json"),
                serde_json::to_vec(&oracle.into_iter().collect::<Vec<_>>()).unwrap(),
            )
            .unwrap();
        }
    }
}
#[test]
fn prepared_guard_wrong_bootstrap_and_duplicate_conflict() {
    let mut m = initial(2);
    let plan = giggles_distributed::transaction::Plan {
        database: 42,
        nonce: 999,
        home: descriptor(),
        participants: vec![giggles_distributed::transaction::Participant {
            route: descriptor(),
            writes: vec![],
            reads: vec![giggles_distributed::transaction::Predicate {
                bounds: descriptor().bounds,
                expected: None,
            }],
        }],
    };
    assert_eq!(
        m.apply(
            &command(
                2,
                Action::Transaction(Box::new(giggles_distributed::transaction::Request {
                    route: descriptor(),
                    step: giggles_distributed::transaction::Step::Open(plan),
                    evidence: vec![],
                }))
            ),
            7
        )
        .code,
        0
    );
    let old = m.clone();
    assert_ne!(m.apply(&command(2, Action::Fence(split())), 8).code, 0);
    assert_eq!(m, old);
    let mut c = Cluster::new(4);
    c.apply(2, put(descriptor(), 1, key(1), None));
    c.apply(2, put(descriptor(), 1, key(1), None));
    assert_eq!(c.data(2).image.snapshot().unwrap().rows[&key(1)].version, 0);
    assert_ne!(
        c.submit(2, 1, put(descriptor(), 1, key(2), None)).1,
        Some(0)
    );
    c.apply(2, Action::Fence(split()));
    let mut children = c.data(2).children().unwrap();
    assert_ne!(
        c.submit(4, 1, Action::Bootstrap(Box::new(children[0].clone())))
            .1,
        Some(0)
    );
    c.apply(3, Action::Bootstrap(Box::new(children[0].clone())));
    c.apply(3, Action::Bootstrap(Box::new(children[0].clone())));
    children[0].retries.clear();
    assert_ne!(
        c.submit(3, 1, Action::Bootstrap(Box::new(children[0].clone())))
            .1,
        Some(0)
    );
}
#[test]
fn independent_quorum_loss_leadership_and_read_barriers() {
    let mut c = Cluster::new(9);
    for a in [1, 2, 3] {
        for b in [1, 2, 3] {
            if a != b {
                c.blocked.insert((2, a, b));
            }
        }
    }
    assert_eq!(c.submit(2, 1, put(descriptor(), 1, key(1), None)).1, None);
    c.apply(1, Action::Begin(split()));
    c.tick(30);
    assert!(c.nodes[&(1, 1)].is_leader());
    assert!(!c.nodes[&(2, 1)].is_leader());
    c.blocked.clear();
    c.nodes.get_mut(&(2, 2)).unwrap().campaign().unwrap();
    c.pump();
    assert!(c.nodes[&(2, 2)].is_leader());
    let ctx = b"new-current".to_vec();
    c.nodes
        .get_mut(&(2, 2))
        .unwrap()
        .read_barrier(ctx.clone())
        .unwrap();
    c.pump();
    assert!(c.reads.contains(&(2, ctx)));
    assert_eq!(c.machine(2, 2), c.machine(2, 3));
}
#[test]
fn snapshot_and_tail_bind_domain_group_index_and_fenced_state() {
    use raft::Storage;
    let mut c = Cluster::new(88);
    for other in [1, 2] {
        c.blocked.insert((2, other, 3));
        c.blocked.insert((2, 3, other));
    }
    for n in 1..8 {
        c.apply(2, put(descriptor(), n, key(n), None));
    }
    c.apply(2, Action::Fence(split()));
    let snapshot = c.nodes[&(2, 1)].store.checkpoint().unwrap();
    assert!(c.nodes[&(3, 1)].store.verify_snapshot(&snapshot).is_err());
    assert!(
        Store::simulated(1, 43, 2, Machine::range(43, descriptor()).unwrap())
            .unwrap()
            .verify_snapshot(&snapshot)
            .is_err()
    );
    c.blocked.clear();
    c.tick(8);
    assert_eq!(c.machine(2, 1), c.machine(2, 3));
    assert!(c.nodes[&(2, 1)].store.first_index().unwrap() > 1);
    let child = c.data(2).children().unwrap()[0].clone();
    c.apply(3, Action::Bootstrap(Box::new(child.clone())));
    let proof = child.provenance.unwrap();
    c.apply(3, Action::Activate { proof, revision: 2 });
    c.apply(3, put(split().left, 100, key(1), Some(0)));
    c.restart();
    assert_eq!(c.data(3).image.snapshot().unwrap().rows[&key(1)].version, 1);
}
#[test]
fn before_after_atomic_save_faults_recover_only_durable_state() {
    for fault in [
        store::SaveFault::BeforeCommit,
        store::SaveFault::AfterCommit,
    ] {
        let mut c = Cluster::new(13);
        let node = c.nodes.get_mut(&(2, 1)).unwrap();
        let store = node.store.clone();
        let before = store.durable_bytes().unwrap();
        store.fail_next_save(fault).unwrap();
        node.propose(vec![1], &command(2, put(descriptor(), 1, key(1), None)))
            .unwrap();
        assert!(node.drain().is_err());
        assert!(store.image().is_err());
        let bytes = store.durable_bytes().unwrap();
        if fault == store::SaveFault::BeforeCommit {
            assert_eq!(bytes, before);
        } else {
            assert_ne!(bytes, before);
        }
        Store::recover_simulated(&bytes, 1, 42, 2).unwrap();
        assert!(Store::recover_simulated(&bytes, 1, 42, 3).is_err());
    }
}
#[test]
fn authenticated_envelope_and_log_entry_cannot_cross_groups() {
    use protobuf::Message;
    let mut m = raft::eraftpb::Message {
        from: 1,
        to: 2,
        ..Default::default()
    };
    m.set_msg_type(raft::eraftpb::MessageType::MsgAppend);
    let e = raft::eraftpb::Entry {
        index: 1,
        term: 1,
        data: serde_json::to_vec(&command(2, Action::TaskWrite))
            .unwrap()
            .into(),
        ..Default::default()
    };
    m.mut_entries().push(e);
    let b = transport::encode_message(42, 2, &m).unwrap();
    assert!(transport::decode_message(42, 3, &b).is_err());
    assert!(transport::decode_message(43, 2, &b).is_err());
    assert_eq!(
        transport::decode_message(42, 2, &b)
            .unwrap()
            .write_to_bytes()
            .unwrap(),
        m.write_to_bytes().unwrap()
    );
    let mut n = Node::new(Store::simulated(2, 42, 3, initial(3)).unwrap()).unwrap();
    assert!(n.step(1, m).is_err());
}

#[test]
fn both_concurrent_log_orders_preserve_exact_fenced_image() {
    for seed in [5, 14, 73] {
        for write_first in [false, true] {
            let mut c = Cluster::new(seed);
            let put = command(2, put(descriptor(), 100, key(49), None));
            let fence = command(2, Action::Fence(split()));
            let actions = if write_first {
                [put, fence]
            } else {
                [fence, put]
            };
            for (i, action) in actions.iter().enumerate() {
                c.nodes
                    .get_mut(&(2, 1))
                    .unwrap()
                    .propose(vec![200 + i as u8], action)
                    .unwrap();
            }
            c.pump();
            let parent = c.data(2);
            let rows = parent.image.snapshot().unwrap().rows;
            assert_eq!(rows.contains_key(&key(49)), write_first);
            let children = parent.children().unwrap();
            let combined = children
                .into_iter()
                .flat_map(|child| child.image.snapshot().unwrap().rows)
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(combined, rows);
            assert_eq!(c.responses[&(2, vec![200])], 0);
            assert_eq!(
                c.responses[&(2, vec![201])],
                if write_first { 0 } else { 400 }
            );
        }
    }
}

#[test]
fn maximum_valid_row_and_retry_footprint_can_bootstrap_both_children() {
    use bincode::Options;
    use giggles_core::{Status, replica::Image};
    // Construct and validate the actual canonical core image format in one
    // fixture batch, avoiding thousands of unrelated per-row fsync operations.
    let codec = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    let mut records: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
        codec.deserialize(Image::new(42).unwrap().bytes()).unwrap();
    let mut retries = std::collections::BTreeMap::new();
    for n in 0..LIMIT {
        let k = Key {
            tenant: TenantId(u64::MAX - 1),
            row: RowId(u64::MAX - n as u64),
        };
        let value = giggles_core::Row {
            status: Status::Failed,
            failed_at: u64::MAX,
            attempt_count: u64::MAX,
            version: 0,
        };
        let mut record = vec![1];
        record.extend(k.tenant.0.to_be_bytes());
        record.extend(k.row.0.to_be_bytes());
        records.insert(record, codec.serialize(&value).unwrap());
        let mut request = value;
        request.version = 999;
        retries.insert(
            u64::MAX - n as u64,
            RetryRecord {
                key: k,
                digest: digest(&(k, None::<u64>, request)).unwrap(),
                version: 0,
            },
        );
    }
    let image = Image::from_bytes(codec.serialize(&records).unwrap()).unwrap();
    let mut machine = initial(2);
    if let Machine::Range { data, .. } = &mut machine {
        data.image = image;
        data.retries = retries;
    }
    machine.validate(42, 2).unwrap();
    assert_eq!(
        machine.apply(&command(2, Action::Fence(split())), 8).code,
        0
    );
    let Machine::Range { data, .. } = machine else {
        panic!()
    };
    for child in data.children().unwrap() {
        let group = child.descriptor.group;
        let expected = child.image.snapshot().unwrap().rows;
        let encoded =
            serde_json::to_vec(&command(group, Action::Bootstrap(Box::new(child)))).unwrap();
        assert!(
            encoded.len() <= MAX_COMMAND,
            "valid child requires {} bytes but proposal cap is {MAX_COMMAND}",
            encoded.len()
        );
        let decoded: Command = serde_json::from_slice(&encoded).unwrap();
        let mut target = initial(group);
        let reply = target.apply(&decoded, 9);
        assert_eq!(reply.code, 0, "{reply:?}");
        let Machine::Range { data, .. } = target else {
            panic!()
        };
        assert_eq!(data.image.snapshot().unwrap().rows, expected);
    }
}

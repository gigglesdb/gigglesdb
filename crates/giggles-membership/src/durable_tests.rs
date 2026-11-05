use super::*;
fn record() -> Record {
    let m = Membership::new(
        Identity {
            group: 7,
            owner_epoch: 1,
        },
        vec![1, 2, 3],
        8,
    )
    .unwrap();
    let e = Entry {
        index: 1,
        term: 1,
        ..Default::default()
    };
    Record {
        node: 1,
        image: Image {
            membership: m.clone(),
            applied: 1,
            term: 1,
        },
        conf: expected_conf(&m).unwrap().write_to_bytes().unwrap(),
        hard: HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        }
        .write_to_bytes()
        .unwrap(),
        snapshot: Snapshot::default().write_to_bytes().unwrap(),
        log: BTreeMap::from([(1, e.write_to_bytes().unwrap())]),
    }
}
#[test]
fn independent_record_invariants_reject_applied_commit_term_and_log_corruption() {
    let good = record();
    good.validate().unwrap();
    for case in 0..5 {
        let mut r = good.clone();
        match case {
            0 => r.image.applied = 2,
            1 => r.image.term = 2,
            2 => r.log.clear(),
            3 => {
                r.hard = HardState {
                    term: 1,
                    commit: 2,
                    ..Default::default()
                }
                .write_to_bytes()
                .unwrap()
            }
            4 => {
                r.log.insert(
                    3,
                    Entry {
                        index: 3,
                        term: 1,
                        ..Default::default()
                    }
                    .write_to_bytes()
                    .unwrap(),
                );
            }
            _ => unreachable!(),
        }
        assert!(r.validate().is_err(), "case {case}");
    }
}
#[test]
fn snapshot_group_confstate_frontier_and_controller_must_agree() {
    let good = record().snapshot().unwrap();
    validate_snapshot(&good, 7).unwrap();
    assert!(validate_snapshot(&good, 8).is_err());
    for case in 0..5 {
        let mut s = good.clone();
        match case {
            0 => s.mut_metadata().index += 1,
            1 => s.mut_metadata().term += 1,
            2 => s.mut_metadata().mut_conf_state().voters.push(4),
            3 => {
                let mut image: Image = serde_json::from_slice(&s.data).unwrap();
                image.membership.active = Some(99);
                s.data = serde_json::to_vec(&image).unwrap().into();
            }
            4 => s.data = vec![0; MAX_SNAPSHOT + 1].into(),
            _ => unreachable!(),
        }
        assert!(validate_snapshot(&s, 7).is_err(), "case {case}");
    }
}
#[test]
fn record_rejects_snapshot_at_same_index_with_different_owner() {
    let mut r = record();
    r.snapshot = r.snapshot().unwrap().write_to_bytes().unwrap();
    r.log.clear();
    r.validate().unwrap();
    r.image.membership.identity.owner_epoch = 2;
    assert!(r.validate().is_err());
}
#[test]
fn retained_log_bound_and_compacted_tail_use_library_storage_semantics() {
    let mut r = record();
    r.snapshot = r.snapshot().unwrap().write_to_bytes().unwrap();
    r.log.clear();
    r.log.insert(
        2,
        Entry {
            index: 2,
            term: 1,
            ..Default::default()
        }
        .write_to_bytes()
        .unwrap(),
    );
    r.validate().unwrap();
    let mem = r.memory().unwrap();
    assert_eq!(mem.first_index().unwrap(), 2);
    assert_eq!(mem.term(1).unwrap(), 1);
    assert_eq!(mem.last_index().unwrap(), 2);
    assert!(matches!(
        mem.entries(1, 2, None, GetEntriesContext::empty(false)),
        Err(raft::Error::Store(StorageError::Compacted))
    ));
    for i in 3..=(MAX_LOG as u64 + 2) {
        r.log.insert(
            i,
            Entry {
                index: i,
                term: 1,
                ..Default::default()
            }
            .write_to_bytes()
            .unwrap(),
        );
    }
    assert!(r.validate().is_err());
}

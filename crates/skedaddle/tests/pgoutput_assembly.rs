use std::collections::BTreeSet;

use skedaddle::{
    event::{AssembledTxn, EventKind, OpaqueValue, Row, SourceIdentity, SourceOrigin},
    pg::{
        assembly::{AssembleError, MappedEvent, TransactionAssembler},
        pgoutput::{Begin, Column, Commit, Decoder, Message},
    },
};

fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "postgres".into(),
        system_id: "123".into(),
        lineage: "timeline-1".into(),
        db_name: "db".into(),
        slot: "slot".into(),
    }
}
fn assembler(bytes: u64, events: usize) -> TransactionAssembler {
    TransactionAssembler::new(source(), "pg:123:timeline-1:db:slot", bytes, events).unwrap()
}
fn begin() -> Message<'static> {
    Message::Begin(Begin {
        final_lsn: 100,
        commit_time: -50,
        xid: 42,
    })
}
fn commit() -> Message<'static> {
    Message::Commit(Commit {
        commit_lsn: 100,
        end_lsn: 120,
        commit_time: -50,
    })
}
fn no_mapping(_: &Message<'_>) -> Result<Vec<MappedEvent>, AssembleError> {
    panic!("Begin/Commit must not invoke mapping")
}

// This is protocol-to-opaque-value conversion for a fixed byte fixture, not a
// database fake or the P4 type mapper. Real capture supplies its own mapper.
fn map_insert(message: &Message<'_>) -> Result<Vec<MappedEvent>, AssembleError> {
    let Message::Insert {
        relation_id: 42,
        new,
    } = message
    else {
        return Err(AssembleError::Mapping("unhandled message"));
    };
    let mut after = Row::new();
    let mut unchanged_cols = BTreeSet::new();
    for (index, column) in new.iter().enumerate() {
        let name = format!("column{index}");
        let (type_id, bytes) = match column {
            Column::Null => ("test:null", None),
            Column::Text(bytes) => ("test:text", Some(bytes.to_vec())),
            Column::Binary(bytes) => ("test:binary", Some(bytes.to_vec())),
            Column::UnchangedToast => {
                unchanged_cols.insert(name);
                continue;
            }
        };
        after.insert(
            name,
            OpaqueValue {
                type_id: type_id.into(),
                bytes,
            },
        );
    }
    Ok(vec![MappedEvent {
        schema_version: 7,
        kind: EventKind::Insert,
        table: Some("public.example".into()),
        key: Row::new(),
        before: None,
        after: Some(after),
        unchanged_cols,
    }])
}

#[test]
fn decoded_transactions_assemble_with_identity_positions_ordinals_and_digest() {
    let decoder = Decoder::default();
    let insert = decoder
        .decode(b"I\0\0\0\x2aN\0\x04t\0\0\0\x01knub\0\0\0\x02\0\xff")
        .unwrap();
    let mut assembler = assembler(1024 * 1024, 10);
    assembler.push(&begin(), no_mapping).unwrap();
    assert!(assembler.push(&insert, map_insert).unwrap().is_none());
    assert!(assembler.push(&insert, map_insert).unwrap().is_none());
    let completed = assembler.push(&commit(), no_mapping).unwrap().unwrap();
    assert_eq!(completed.end_lsn, 120);
    let txn = completed.transaction;
    assert_eq!(txn.events.len(), 2);
    let mut expected_id = 42_u32.to_be_bytes().to_vec();
    expected_id.extend_from_slice(&100_u64.to_be_bytes());
    assert_eq!(txn.txn.0, expected_id);
    assert_eq!(txn.commit_position.bytes(), &100_u64.to_be_bytes());
    assert_eq!(txn.commit_time, Some(946_684_799_999_950));
    assert_eq!(txn.source, source());
    for (ordinal, event) in txn.events.iter().enumerate() {
        assert_eq!(event.ordinal_in_txn, ordinal as u64);
        assert_eq!(event.txn, txn.txn);
        assert_eq!(event.position, txn.commit_position);
        assert_eq!(event.unchanged_cols, BTreeSet::from(["column2".into()]));
        let after = event.after.as_ref().unwrap();
        assert_eq!(after["column1"].bytes, None);
        assert!(!after.contains_key("column2"));
        assert_eq!(
            after["column3"].bytes.as_deref(),
            Some(b"\0\xff".as_slice())
        );
    }
    let canonical = txn.canonical_bytes().unwrap();
    assert_eq!(AssembledTxn::from_canonical_bytes(&canonical).unwrap(), txn);
    assembler.finish().unwrap();
    assembler.push(&begin(), no_mapping).unwrap();
    assert!(
        assembler
            .push(&commit(), no_mapping)
            .unwrap()
            .unwrap()
            .transaction
            .events
            .is_empty()
    );
}

fn map_truncate(message: &Message<'_>) -> Result<Vec<MappedEvent>, AssembleError> {
    let Message::Truncate {
        relation_ids,
        cascade,
        restart_identity,
    } = message
    else {
        return Err(AssembleError::Mapping("expected truncate"));
    };
    Ok(vec![MappedEvent {
        schema_version: 7,
        kind: EventKind::Truncate {
            relations: relation_ids
                .iter()
                .map(|id| format!("public.table{id}"))
                .collect(),
            cascade: *cascade,
            restart_identity: *restart_identity,
        },
        table: None,
        key: Row::new(),
        before: None,
        after: None,
        unchanged_cols: BTreeSet::new(),
    }])
}

#[test]
fn pgoutput_bytes_preserve_one_ordered_truncate_group_flags_and_origin() {
    let decoder = Decoder::default();
    let origin = decoder.decode(b"O\0\0\0\0\0\0\0\x09remote\0").unwrap();
    for flags in 0..4 {
        let mut bytes = b"T\0\0\0\x02\0\0\0\0\x2b\0\0\0\x2a".to_vec();
        bytes[5] = flags;
        let truncate = decoder.decode(&bytes).unwrap();
        let mut assembler = assembler(10000, 10);
        assembler.push(&begin(), no_mapping).unwrap();
        assembler.push(&origin, no_mapping).unwrap();
        assembler.push(&truncate, map_truncate).unwrap();
        let txn = assembler
            .push(&commit(), no_mapping)
            .unwrap()
            .unwrap()
            .transaction;
        assert_eq!(
            txn.source_origin,
            Some(SourceOrigin {
                name: "remote".into(),
                lsn: 9
            })
        );
        assert_eq!(txn.events.len(), 1);
        assert_eq!(txn.events[0].ordinal_in_txn, 0);
        assert_eq!(
            txn.events[0].kind,
            EventKind::Truncate {
                relations: vec!["public.table43".into(), "public.table42".into()],
                cascade: flags & 1 != 0,
                restart_identity: flags & 2 != 0,
            }
        );
        assert_eq!(
            AssembledTxn::from_canonical_bytes(&txn.canonical_bytes().unwrap()).unwrap(),
            txn
        );
        assembler.push(&begin(), no_mapping).unwrap();
        assert_eq!(
            assembler
                .push(&commit(), no_mapping)
                .unwrap()
                .unwrap()
                .transaction
                .source_origin,
            None
        );
    }
}

#[test]
fn split_dropped_or_changed_truncates_poison_the_transaction() {
    let message = Message::Truncate {
        relation_ids: vec![43, 42],
        cascade: true,
        restart_identity: true,
    };
    for change in 0..5 {
        let mut assembler = assembler(10000, 10);
        assembler.push(&begin(), no_mapping).unwrap();
        assert!(matches!(
            assembler.push(&message, |msg| {
                let mut mapped = map_truncate(msg)?;
                let EventKind::Truncate {
                    relations,
                    cascade,
                    restart_identity,
                } = &mut mapped[0].kind
                else {
                    unreachable!()
                };
                match change {
                    0 => {
                        relations.pop();
                    }
                    1 => *cascade = false,
                    2 => *restart_identity = false,
                    3 => {
                        mapped.push(mapped[0].clone());
                    }
                    _ => mapped.clear(),
                }
                Ok(mapped)
            }),
            Err(AssembleError::Mapping(_))
        ));
        assert!(assembler.push(&commit(), no_mapping).is_err());
    }
}

#[test]
fn origin_requires_one_early_bounded_transaction_message() {
    let origin = Message::Origin {
        name: "upstream",
        commit_lsn: 7,
    };
    assert!(assembler(10000, 10).push(&origin, no_mapping).is_err());
    let mut duplicate = assembler(10000, 10);
    duplicate.push(&begin(), no_mapping).unwrap();
    duplicate.push(&origin, no_mapping).unwrap();
    assert!(duplicate.push(&origin, no_mapping).is_err());
    assert!(duplicate.push(&commit(), no_mapping).is_err());
    let mut limited = assembler(1, 10);
    limited.push(&begin(), no_mapping).unwrap();
    assert_eq!(
        limited.push(&origin, no_mapping),
        Err(AssembleError::LimitExceeded)
    );
    let mut late = assembler(10000, 10);
    late.push(&begin(), no_mapping).unwrap();
    late.push(
        &Message::Truncate {
            relation_ids: vec![42],
            cascade: false,
            restart_identity: false,
        },
        map_truncate,
    )
    .unwrap();
    assert!(late.push(&origin, no_mapping).is_err());
}

#[test]
fn mapper_observes_metadata_and_controls_while_assembler_retains_origin() {
    let decoder = Decoder::default();
    let metadata = decoder.decode(b"Y\0\0\0\x17\0int4\0").unwrap();
    let mut assembler = assembler(10000, 10);
    let mut observed = 0;
    assembler
        .push(&metadata, |_| {
            observed += 1;
            Ok(vec![])
        })
        .unwrap();
    assembler.push(&begin(), no_mapping).unwrap();
    for message in [
        Message::Origin {
            commit_lsn: 1,
            name: "remote",
        },
        Message::LogicalMessage {
            transactional: true,
            lsn: 90,
            prefix: "skedaddle",
            content: b"watermark",
        },
        Message::Truncate {
            cascade: true,
            restart_identity: true,
            relation_ids: vec![42],
        },
    ] {
        assembler
            .push(&message, |_| {
                observed += 1;
                if let Message::Truncate {
                    cascade,
                    restart_identity,
                    ..
                } = message
                {
                    Ok(vec![MappedEvent {
                        schema_version: 7,
                        kind: EventKind::Truncate {
                            relations: vec!["public.example".into()],
                            cascade,
                            restart_identity,
                        },
                        table: None,
                        key: Row::new(),
                        before: None,
                        after: None,
                        unchanged_cols: BTreeSet::new(),
                    }])
                } else {
                    Ok(vec![])
                }
            })
            .unwrap();
    }
    let txn = assembler
        .push(&commit(), no_mapping)
        .unwrap()
        .unwrap()
        .transaction;
    assert_eq!(observed, 3);
    assert_eq!(txn.source_origin.unwrap().name, "remote");
}

#[test]
fn mapping_failure_and_limits_poison_assembly_preventing_partial_commit() {
    let message = Message::Insert {
        relation_id: 42,
        new: vec![Column::Text(b"value")],
    };
    for (bytes, events) in [(1, 10), (10000, 1)] {
        let mut assembler = assembler(bytes, events);
        assembler.push(&begin(), no_mapping).unwrap();
        if events == 1 {
            assembler.push(&message, map_insert).unwrap();
        }
        assert!(matches!(
            assembler.push(&message, map_insert),
            Err(AssembleError::LimitExceeded)
        ));
        assert!(assembler.push(&commit(), no_mapping).is_err());
        assert!(assembler.finish().is_err());
    }
    let mut assembler = assembler(10000, 10);
    assembler.push(&begin(), no_mapping).unwrap();
    assembler.push(&message, map_insert).unwrap();
    let error = AssembleError::Mapping("unsupported logical-message prefix");
    assert_eq!(assembler.push(&message, |_| Err(error.clone())), Err(error));
    assert!(assembler.push(&commit(), no_mapping).is_err());
}

#[test]
fn incomplete_outside_nested_and_mismatched_transactions_are_rejected() {
    let insert = Message::Insert {
        relation_id: 42,
        new: vec![],
    };
    for message in [
        commit(),
        insert,
        Message::LogicalMessage {
            transactional: false,
            lsn: 1,
            prefix: "x",
            content: b"",
        },
    ] {
        let mut assembler = assembler(10000, 10);
        assert!(assembler.push(&message, |_| Ok(vec![])).is_err());
    }
    for message in [
        begin(),
        Message::Commit(Commit {
            commit_lsn: 101,
            end_lsn: 120,
            commit_time: -50,
        }),
        Message::Commit(Commit {
            commit_lsn: 100,
            end_lsn: 120,
            commit_time: -49,
        }),
    ] {
        let mut assembler = assembler(10000, 10);
        assembler.push(&begin(), no_mapping).unwrap();
        assert!(assembler.push(&message, no_mapping).is_err());
        assert!(assembler.push(&commit(), no_mapping).is_err());
    }
    let mut incomplete = assembler(10000, 10);
    incomplete.push(&begin(), no_mapping).unwrap();
    assert!(incomplete.finish().is_err());
    let mut overflow = assembler(10000, 10);
    overflow
        .push(
            &Message::Begin(Begin {
                final_lsn: 1,
                commit_time: i64::MAX,
                xid: 1,
            }),
            no_mapping,
        )
        .unwrap();
    assert!(
        overflow
            .push(
                &Message::Commit(Commit {
                    commit_lsn: 1,
                    end_lsn: 2,
                    commit_time: i64::MAX
                }),
                no_mapping
            )
            .is_err()
    );
}

#[test]
fn assembly_rejects_disabled_or_above_hard_limits() {
    for (bytes, events) in [(0, 1), (1, 0), (u64::MAX, 1), (1, usize::MAX)] {
        assert!(TransactionAssembler::new(source(), "pg", bytes, events).is_err());
    }
}

//! Hand-built fixtures follow PostgreSQL 18 protocol sections 54.4 and 54.9.
//! They require no server, captured artifacts, or environment configuration.

use skedaddle::pg::{
    frame::FrameBuffer,
    pgoutput::{
        Begin, Column, Commit, CopyData, DecodeError, Decoder, Limits, MAX_COLUMNS,
        MAX_MESSAGE_BYTES, MAX_TRUNCATE_RELATIONS, Message, OldTuple, ReplicaIdentity,
        StandbyStatusUpdate,
    },
};

fn tuple(columns: &[Column<'_>]) -> Vec<u8> {
    let mut bytes = (columns.len() as u16).to_be_bytes().to_vec();
    for column in columns {
        match column {
            Column::Null => bytes.push(b'n'),
            Column::UnchangedToast => bytes.push(b'u'),
            Column::Text(value) | Column::Binary(value) => {
                bytes.push(if matches!(column, Column::Text(_)) {
                    b't'
                } else {
                    b'b'
                });
                bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
                bytes.extend_from_slice(value);
            }
        }
    }
    bytes
}

fn dml(tag: u8, parts: &[(u8, &[Column<'_>])]) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&42_u32.to_be_bytes());
    for (tag, columns) in parts {
        bytes.push(*tag);
        bytes.extend(tuple(columns));
    }
    bytes
}

fn begin() -> Vec<u8> {
    let mut bytes = vec![b'B'];
    bytes.extend_from_slice(&0x123456789abcdef0_u64.to_be_bytes());
    bytes.extend_from_slice(&(-123_i64).to_be_bytes());
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    bytes
}

fn commit() -> Vec<u8> {
    let mut bytes = vec![b'C', 0];
    bytes.extend_from_slice(&0x123456789abcdef0_u64.to_be_bytes());
    bytes.extend_from_slice(&0x123456789abcdf00_u64.to_be_bytes());
    bytes.extend_from_slice(&(-123_i64).to_be_bytes());
    bytes
}

fn relation() -> Vec<u8> {
    let mut bytes = b"R\0\0\0\x2apublic\0odd.\"table\0f\0\x02".to_vec();
    bytes.extend_from_slice(b"\x01id\0");
    bytes.extend_from_slice(&23_u32.to_be_bytes());
    bytes.extend_from_slice(&(-1_i32).to_be_bytes());
    bytes.extend_from_slice(b"\0body\0");
    bytes.extend_from_slice(&25_u32.to_be_bytes());
    bytes.extend_from_slice(&123_i32.to_be_bytes());
    bytes
}

fn logical(transactional: bool) -> Vec<u8> {
    let mut bytes = vec![b'M', u8::from(transactional)];
    bytes.extend_from_slice(&11_u64.to_be_bytes());
    bytes.extend_from_slice(b"skedaddle\0");
    bytes.extend_from_slice(&4_u32.to_be_bytes());
    bytes.extend_from_slice(&[0, 0xff, 0x80, b'x']);
    bytes
}

fn truncate(flags: u8) -> Vec<u8> {
    let mut bytes = vec![b'T'];
    bytes.extend_from_slice(&2_u32.to_be_bytes());
    bytes.push(flags);
    bytes.extend_from_slice(&42_u32.to_be_bytes());
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    bytes
}

fn keepalive(reply: bool) -> Vec<u8> {
    let mut bytes = vec![b'k'];
    bytes.extend_from_slice(&u64::MAX.to_be_bytes());
    bytes.extend_from_slice(&(-321_i64).to_be_bytes());
    bytes.push(u8::from(reply));
    bytes
}

fn xlog(data: &[u8]) -> Vec<u8> {
    let mut bytes = vec![b'w'];
    bytes.extend_from_slice(&10_u64.to_be_bytes());
    bytes.extend_from_slice(&20_u64.to_be_bytes());
    bytes.extend_from_slice(&(-10_i64).to_be_bytes());
    bytes.extend_from_slice(data);
    bytes
}

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&(payload.len() as u32 + 4).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn fixtures() -> Vec<Vec<u8>> {
    vec![
        begin(),
        commit(),
        relation(),
        b"Y\0\0\x01\0\0mood\0".to_vec(),
        b"O\0\0\0\0\0\0\0\x0aremote\0".to_vec(),
        dml(
            b'I',
            &[(
                b'N',
                &[Column::Null, Column::Text(b""), Column::Binary(b"\0\xff")],
            )],
        ),
        dml(b'U', &[(b'N', &[Column::UnchangedToast])]),
        dml(
            b'U',
            &[
                (b'K', &[Column::Text(b"old")]),
                (b'N', &[Column::Text(b"new")]),
            ],
        ),
        dml(
            b'U',
            &[
                (b'O', &[Column::Text(b"old"), Column::Null]),
                (b'N', &[Column::Text(b"new"), Column::UnchangedToast]),
            ],
        ),
        dml(b'D', &[(b'K', &[Column::Text(b"key")])]),
        dml(b'D', &[(b'O', &[Column::Text(b"full"), Column::Null])]),
        truncate(3),
        logical(true),
        logical(false),
    ]
}

#[test]
fn begin_and_commit_use_unsigned_lsns_and_signed_pg_timestamps() {
    let decoder = Decoder::default();
    assert_eq!(
        decoder.decode(&begin()).unwrap(),
        Message::Begin(Begin {
            final_lsn: 0x123456789abcdef0,
            commit_time: -123,
            xid: u32::MAX,
        })
    );
    assert_eq!(
        decoder.decode(&commit()).unwrap(),
        Message::Commit(Commit {
            commit_lsn: 0x123456789abcdef0,
            end_lsn: 0x123456789abcdf00,
            commit_time: -123,
        })
    );
    let mut invalid = commit();
    invalid[10..18].copy_from_slice(&1_u64.to_be_bytes());
    assert!(matches!(
        decoder.decode(&invalid),
        Err(DecodeError::Invalid { .. })
    ));
}

#[test]
fn relation_type_and_origin_metadata_are_preserved() {
    let decoder = Decoder::default();
    let bytes = relation();
    let Message::Relation(r) = decoder.decode(&bytes).unwrap() else {
        panic!("relation");
    };
    assert_eq!((r.id, r.namespace, r.name), (42, "public", "odd.\"table"));
    assert_eq!(r.replica_identity, ReplicaIdentity::Full);
    assert_eq!(r.columns.len(), 2);
    assert!(r.columns[0].key);
    assert_eq!(
        (
            r.columns[0].name,
            r.columns[0].type_oid,
            r.columns[0].type_modifier
        ),
        ("id", 23, -1)
    );
    assert!(!r.columns[1].key);
    assert_eq!(
        (
            r.columns[1].name,
            r.columns[1].type_oid,
            r.columns[1].type_modifier
        ),
        ("body", 25, 123)
    );
    assert_eq!(
        decoder.decode(b"Y\0\0\x01\0\0mood\0").unwrap(),
        Message::Type {
            id: 256,
            namespace: "",
            name: "mood"
        }
    );
    assert_eq!(
        decoder.decode(b"O\0\0\0\0\0\0\0\x0aremote\0").unwrap(),
        Message::Origin {
            commit_lsn: 10,
            name: "remote"
        }
    );
    for (byte, expected) in [
        (b'd', ReplicaIdentity::Default),
        (b'n', ReplicaIdentity::Nothing),
        (b'i', ReplicaIdentity::Index),
    ] {
        let mut bytes = relation();
        // id + namespace + name end immediately before the identity byte.
        let identity = 5 + b"public\0odd.\"table\0".len();
        bytes[identity] = byte;
        let Message::Relation(r) = decoder.decode(&bytes).unwrap() else {
            unreachable!()
        };
        assert_eq!(r.replica_identity, expected);
    }
}

#[test]
fn null_toast_text_and_binary_including_empty_and_non_utf8_stay_distinct() {
    let columns = [
        Column::Null,
        Column::UnchangedToast,
        Column::Text(b""),
        Column::Binary(b""),
        Column::Text(b"\xff\0"),
        Column::Binary(b"\0\xff"),
    ];
    let bytes = dml(b'I', &[(b'N', &columns)]);
    let Message::Insert { relation_id, new } = Decoder::default().decode(&bytes).unwrap() else {
        panic!("insert");
    };
    assert_eq!(relation_id, 42);
    assert_eq!(new, columns);
    let Column::Binary(value) = new.last().unwrap() else {
        unreachable!()
    };
    // The decoder borrows payload bytes instead of copying large TOAST values.
    assert_eq!(value.as_ptr(), bytes[bytes.len() - 2..].as_ptr());
}

#[test]
fn updates_keep_old_keys_old_full_rows_and_new_only_separate() {
    let decoder = Decoder::default();
    let old = [Column::Text(b"old-key")];
    let new = [Column::Text(b"new-key"), Column::UnchangedToast];
    for tag in *b"KO" {
        let bytes = dml(b'U', &[(tag, &old), (b'N', &new)]);
        let Message::Update {
            relation_id,
            old: decoded_old,
            new: decoded_new,
        } = decoder.decode(&bytes).unwrap()
        else {
            panic!("update");
        };
        assert_eq!(relation_id, 42);
        assert_eq!(decoded_new, new);
        assert_eq!(
            decoded_old,
            Some(if tag == b'K' {
                OldTuple::Key(old.to_vec())
            } else {
                OldTuple::Full(old.to_vec())
            })
        );
    }
    let bytes = dml(b'U', &[(b'N', &new)]);
    assert_eq!(
        decoder.decode(&bytes).unwrap(),
        Message::Update {
            relation_id: 42,
            old: None,
            new: new.to_vec()
        }
    );
}

#[test]
fn deletes_and_truncate_options_are_not_collapsed() {
    let decoder = Decoder::default();
    for tag in *b"KO" {
        let bytes = dml(b'D', &[(tag, &[Column::Text(b"old")])]);
        let Message::Delete { relation_id, old } = decoder.decode(&bytes).unwrap() else {
            panic!("delete");
        };
        assert_eq!(relation_id, 42);
        assert_eq!(
            old,
            if tag == b'K' {
                OldTuple::Key(vec![Column::Text(b"old")])
            } else {
                OldTuple::Full(vec![Column::Text(b"old")])
            }
        );
    }
    for flags in 0..=3 {
        assert_eq!(
            decoder.decode(&truncate(flags)).unwrap(),
            Message::Truncate {
                cascade: flags & 1 != 0,
                restart_identity: flags & 2 != 0,
                relation_ids: vec![42, u32::MAX],
            }
        );
    }
}

#[test]
fn logical_messages_are_arbitrary_bytes_and_preserve_transactionality() {
    for transactional in [true, false] {
        let bytes = logical(transactional);
        assert_eq!(
            Decoder::default().decode(&bytes).unwrap(),
            Message::LogicalMessage {
                transactional,
                lsn: 11,
                prefix: "skedaddle",
                content: &[0, 0xff, 0x80, b'x'],
            }
        );
    }
}

#[test]
fn copy_data_wrappers_keep_reply_request_and_wal_progress_separate() {
    let decoder = Decoder::default();
    for reply in [false, true] {
        assert_eq!(
            decoder.decode_copy_data(&keepalive(reply)).unwrap(),
            CopyData::PrimaryKeepalive {
                wal_end: u64::MAX,
                server_time: -321,
                reply_requested: reply,
            }
        );
    }
    let message = begin();
    let bytes = xlog(&message);
    let CopyData::XLogData {
        wal_start,
        wal_end,
        server_time,
        data,
    } = decoder.decode_copy_data(&bytes).unwrap()
    else {
        panic!("XLogData");
    };
    assert_eq!((wal_start, wal_end, server_time), (10, 20, -10));
    assert_eq!(data, message);
    assert!(matches!(decoder.decode(data), Ok(Message::Begin(_))));
    assert!(matches!(
        decoder.decode_copy_data(&xlog(&[])),
        Err(DecodeError::Truncated { .. })
    ));
    let mut invalid = keepalive(false);
    invalid[17] = 2;
    assert!(matches!(
        decoder.decode_copy_data(&invalid),
        Err(DecodeError::Invalid { .. })
    ));
    invalid[17] = 0;
    invalid.push(0);
    assert_eq!(
        decoder.decode_copy_data(&invalid),
        Err(DecodeError::TrailingBytes(1))
    );
    assert_eq!(
        decoder.decode_copy_data(b"?"),
        Err(DecodeError::UnsupportedTag(b'?'))
    );
}

#[test]
fn streaming_and_two_phase_messages_fail_with_actionable_typed_errors() {
    let decoder = Decoder::default();
    for tag in b"SEcAp" {
        let error = decoder.decode(&[*tag]).unwrap_err();
        assert_eq!(error, DecodeError::StreamingDisabled(*tag));
        assert!(error.to_string().contains("streaming off"));
    }
    for tag in b"bPKr" {
        assert_eq!(
            decoder.decode(&[*tag]),
            Err(DecodeError::UnsupportedTwoPhase(*tag))
        );
    }
    assert_eq!(decoder.decode(b"?"), Err(DecodeError::UnsupportedTag(b'?')));
}

#[test]
fn tuple_grammar_flags_and_identifier_encoding_are_strict() {
    let decoder = Decoder::default();
    let col = [Column::Null];
    for bytes in [
        dml(b'I', &[(b'K', &col)]),
        dml(b'I', &[(b'N', &col), (b'N', &col)]),
        dml(b'U', &[(b'K', &col)]),
        dml(b'U', &[(b'O', &col), (b'K', &col), (b'N', &col)]),
        dml(b'U', &[(b'N', &col), (b'O', &col)]),
        dml(b'D', &[(b'N', &col)]),
        dml(b'D', &[(b'K', &col), (b'O', &col)]),
        b"I\0\0\0\x2aN\0\x01?".to_vec(),
        b"Y\0\0\0\x01public\0unterminated".to_vec(),
        b"Y\0\0\0\x01public\0\xff\0".to_vec(),
    ] {
        assert!(decoder.decode(&bytes).is_err(), "accepted {bytes:?}");
    }
    let mut bad_commit = commit();
    bad_commit[1] = 1;
    let mut bad_message = logical(true);
    bad_message[1] = 2;
    let mut bad_relation = relation();
    let column_flags = 5 + b"public\0odd.\"table\0".len() + 3;
    bad_relation[column_flags] = 2;
    for bytes in [bad_commit, bad_message, bad_relation, truncate(4)] {
        assert!(matches!(
            decoder.decode(&bytes),
            Err(DecodeError::Invalid { .. })
        ));
    }
}

#[test]
fn hard_and_lowered_limits_reject_before_count_based_allocation() {
    for limits in [
        Limits {
            message_bytes: 0,
            ..Limits::default()
        },
        Limits {
            message_bytes: MAX_MESSAGE_BYTES + 1,
            ..Limits::default()
        },
        Limits {
            columns: MAX_COLUMNS + 1,
            ..Limits::default()
        },
        Limits {
            truncate_relations: MAX_TRUNCATE_RELATIONS + 1,
            ..Limits::default()
        },
    ] {
        assert!(matches!(
            Decoder::new(limits),
            Err(DecodeError::InvalidLimits)
        ));
    }
    let decoder = Decoder::new(Limits {
        message_bytes: 64,
        columns: 1,
        truncate_relations: 1,
    })
    .unwrap();
    for bytes in [
        relation(),
        dml(b'I', &[(b'N', &[Column::Null, Column::Null])]),
        truncate(0),
    ] {
        assert!(matches!(
            decoder.decode(&bytes),
            Err(DecodeError::LimitExceeded { .. })
        ));
    }
    assert!(matches!(
        decoder.decode(&[0; 65]),
        Err(DecodeError::LimitExceeded { .. })
    ));
    assert!(matches!(
        decoder.decode(b"I\0\0\0\x01N\0\x01t\xff\xff\xff\xff"),
        Err(DecodeError::LimitExceeded { .. })
    ));
    assert!(matches!(
        Decoder::default().decode(b"T\xff\xff\xff\xff\0"),
        Err(DecodeError::LimitExceeded { .. })
    ));
    let max_columns = vec![Column::Null; MAX_COLUMNS];
    assert!(
        Decoder::default()
            .decode(&dml(b'I', &[(b'N', &max_columns)]))
            .is_ok()
    );
    let mut too_many = b"I\0\0\0\x01N".to_vec();
    too_many.extend_from_slice(&((MAX_COLUMNS + 1) as u16).to_be_bytes());
    assert!(matches!(
        Decoder::default().decode(&too_many),
        Err(DecodeError::LimitExceeded { .. })
    ));
    let message = begin();
    let exact = Decoder::new(Limits {
        message_bytes: message.len(),
        ..Limits::default()
    })
    .unwrap();
    assert!(exact.decode(&message).is_ok());
    assert!(exact.decode_copy_data(&xlog(&message)).is_ok());
}

#[test]
fn every_fixture_truncation_and_trailing_byte_is_rejected() {
    let decoder = Decoder::default();
    for bytes in fixtures() {
        assert!(decoder.decode(&bytes).is_ok(), "invalid fixture {bytes:?}");
        for length in 0..bytes.len() {
            assert!(
                decoder.decode(&bytes[..length]).is_err(),
                "accepted truncated {bytes:?} at {length}"
            );
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert_eq!(
            decoder.decode(&trailing),
            Err(DecodeError::TrailingBytes(1))
        );
    }
    for bytes in [keepalive(false), xlog(&begin())] {
        // XLogData's body is opaque; only wrapper header/body-presence truncation
        // can be rejected here. The logical decoder rejects partial messages.
        let until = if bytes[0] == b'w' { 26 } else { bytes.len() };
        for length in 0..until {
            assert!(decoder.decode_copy_data(&bytes[..length]).is_err());
        }
    }
}

#[test]
fn deterministic_corruption_and_random_truncation_never_panic() {
    let fixtures = fixtures();
    let decoder = Decoder::default();
    let mut seed = 0x5eed_0049_0000_0001_u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for _ in 0..12_000 {
        let original = &fixtures[random() as usize % fixtures.len()];
        let mut bytes = original.clone();
        for _ in 0..1 + random() % 4 {
            let index = random() as usize % bytes.len();
            bytes[index] ^= random() as u8;
        }
        let length = random() as usize % (bytes.len() + 1);
        bytes.truncate(length);
        // Any mutation can still be valid (for example a changed text byte).
        // All outcomes must be a typed Result, never a panic or unbounded loop.
        let _ = decoder.decode(&bytes);
        let _ = decoder.decode_copy_data(&bytes);
        let mut buffer = FrameBuffer::default();
        let _ = buffer.push(&bytes);
        let _ = buffer.next_frame();
        let _ = buffer.finish();
    }
}

#[test]
fn status_update_matches_wire_fixture_and_enforces_manifest_durability() {
    let update = StandbyStatusUpdate {
        write_lsn: 80,
        flush_lsn: 100,
        apply_lsn: 70,
        client_time: -1,
        reply_requested: true,
    };
    let mut expected = vec![b'r'];
    for value in [80_u64, 100, 70] {
        expected.extend_from_slice(&value.to_be_bytes());
    }
    expected.extend_from_slice(&(-1_i64).to_be_bytes());
    expected.push(1);
    assert_eq!(update.encode(100).unwrap().as_slice(), expected);
    assert_eq!(
        update.encode_copy_data(100).unwrap().as_slice(),
        frame(b'd', &expected)
    );
    assert_eq!(update.encode(99), Err(DecodeError::UnsafeAcknowledgement));
    for invalid in [
        StandbyStatusUpdate {
            write_lsn: 101,
            ..update
        },
        StandbyStatusUpdate {
            apply_lsn: 101,
            ..update
        },
    ] {
        assert_eq!(invalid.encode(100), Err(DecodeError::UnsafeAcknowledgement));
    }
    let keepalive_bytes = keepalive(true);
    let keepalive = Decoder::default()
        .decode_copy_data(&keepalive_bytes)
        .unwrap();
    assert!(matches!(
        keepalive,
        CopyData::PrimaryKeepalive {
            wal_end: u64::MAX,
            ..
        }
    ));
    // Neither a keepalive nor captured progress changes the supplied flush.
    assert_eq!(update.encode(100).unwrap().as_slice(), expected);
    let zero = StandbyStatusUpdate {
        write_lsn: 0,
        flush_lsn: 0,
        apply_lsn: 0,
        client_time: 0,
        reply_requested: false,
    };
    assert_eq!(zero.encode(0).unwrap()[33], 0);
}

#[test]
fn frame_buffer_handles_every_fragment_boundary_and_coalesced_frames() {
    let first = frame(b'd', &keepalive(true));
    let second = frame(b'd', &xlog(&begin()));
    for split in 0..=first.len() {
        let mut buffer = FrameBuffer::default();
        assert_eq!(buffer.push(&first[..split]).unwrap(), split);
        if split < first.len() {
            assert!(buffer.next_frame().unwrap().is_none());
        }
        assert_eq!(buffer.push(&first[split..]).unwrap(), first.len() - split);
        let decoded = buffer.next_frame().unwrap().unwrap();
        assert_eq!(decoded.tag, b'd');
        assert_eq!(decoded.payload, keepalive(true));
        assert_eq!(buffer.buffered_len(), 0);
        buffer.finish().unwrap();
    }
    let mut together = first.clone();
    together.extend_from_slice(&second);
    let mut buffer = FrameBuffer::default();
    assert_eq!(buffer.push(&together).unwrap(), first.len());
    assert_eq!(buffer.push(&second).unwrap(), 0);
    assert_eq!(
        buffer.next_frame().unwrap().unwrap().payload,
        keepalive(true)
    );
    assert_eq!(buffer.push(&together[first.len()..]).unwrap(), second.len());
    assert_eq!(
        buffer.next_frame().unwrap().unwrap().payload,
        xlog(&begin())
    );
    buffer.finish().unwrap();
    // One byte at a time models cancellation/resumption after every read.
    for byte in &first {
        assert_eq!(buffer.push(&[*byte]).unwrap(), 1);
    }
    assert_eq!(
        buffer.next_frame().unwrap().unwrap().payload,
        keepalive(true)
    );
}

#[test]
fn frame_buffer_rejects_bad_headers_oversize_and_partial_eof() {
    for length in [0_u32, 1, 3, u32::MAX, MAX_MESSAGE_BYTES as u32 + 30] {
        let mut header = vec![b'd'];
        header.extend_from_slice(&length.to_be_bytes());
        let mut buffer = FrameBuffer::default();
        assert!(buffer.push(&header).is_err());
        assert!(buffer.next_frame().is_err());
        assert!(buffer.push(&frame(b'd', &[])).is_err());
        assert!(buffer.finish().is_err());
        assert_eq!(buffer.buffered_len(), 5);
    }
    let bytes = frame(b'd', &keepalive(false));
    for length in 1..bytes.len() {
        let mut buffer = FrameBuffer::default();
        assert_eq!(buffer.push(&bytes[..length]).unwrap(), length);
        assert!(buffer.next_frame().unwrap().is_none());
        assert!(matches!(
            buffer.finish(),
            Err(DecodeError::Truncated { .. })
        ));
    }
    let mut buffer = FrameBuffer::default();
    buffer.push(&frame(b'c', &[])).unwrap();
    assert_eq!(
        buffer.next_frame().unwrap().unwrap().payload,
        Vec::<u8>::new()
    );
    buffer.finish().unwrap();
}

use super::*;

const T: TaskObjectId = TaskObjectId(0x0102_0304_0506_0708);

/// Hex with whitespace and `|` ignored; `NN*K` repeats byte NN K times.
fn h(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for token in text.split(|c: char| c.is_whitespace() || c == '|').filter(|t| !t.is_empty()) {
        if let Some((byte, count)) = token.split_once('*') {
            let byte = u8::from_str_radix(byte, 16).unwrap();
            out.extend(std::iter::repeat_n(byte, count.parse().unwrap()));
        } else {
            assert!(token.len() % 2 == 0, "odd hex token {token}");
            for i in (0..token.len()).step_by(2) {
                out.push(u8::from_str_radix(&token[i..i + 2], 16).unwrap());
            }
        }
    }
    out
}

fn committed_outcome() -> OutcomeRecord {
    OutcomeRecord {
        flags: 0,
        identity: b"ab".to_vec(),
        request_fp: [0x11; 32],
        accepted_fp: [0x22; 32],
        decided_lower_ms: 1,
        decided_upper_ms: 2,
        retain_until_ms: 3,
        payload: OutcomePayload::Committed { task: TaskObjectId(5), seq: 6 },
    }
}

fn receipt() -> ReceiptRecord {
    ReceiptRecord {
        decision: 1,
        request_kind: 1,
        flags: 0,
        task: T,
        seq: 7,
        def_version: 1,
        catalog_version: 2,
        decided_ms: 3,
        request_fp: [0x33; 32],
        effects_digest: [0x44; 32],
        rows_affected: 1,
        distinct_charged: 2,
        transitions_charged: 3,
        noop_rows: 4,
        distinct_after: 5,
        transitions_after: 6,
        preview: 0,
        undo_mode: 0,
        restored: 0,
        conflicts: 0,
        superseded: 0,
        op_id: b"op".to_vec(),
        issuer: "i".into(),
        executor: "e".into(),
        approver: None,
    }
}

fn preview_header() -> PreviewHeader {
    PreviewHeader {
        kind: 2,
        state: 0,
        created_ms: 1,
        expires_ms: 2,
        def_version: 3,
        table: 9,
        schema_fp: [0xAA; 32],
        clock_pin: i64::MIN,
        digest: [0xBB; 32],
        counts: [1, 2, 3, 4],
        applied_seq: 0,
        creator: "r".into(),
        scope: Some(PreviewScope { kind: 1, outcome: [0xCC; 32] }),
        surface: 0,
        text: "x".into(),
        params: Vec::new(),
    }
}

fn read_log() -> ReadLogRecord {
    ReadLogRecord {
        wall_ms: 1,
        snapshot_ts: 2,
        credential_fingerprint: [0xEE; 32],
        def_version: 3,
        catalog_version: 4,
        entry: 1,
        class: 0,
        statement_sha256: [0xFF; 32],
        text_len: 5,
        text: "sel".into(),
        param_count: 1,
        table_id: 9,
        columns: vec![ReadColumn { column_id: 2, position_bits: 3 }],
        rows_scanned: 10,
        charged_rows_read: 1,
        charged_rows_returned: 2,
        charged_bytes: 3,
        released_rows: 4,
        released_bytes: 5,
        masked_values: 6,
        withheld_untrusted: 7,
        untrusted_released: 8,
        pages: 1,
        outcome: 1,
        sqlstate: *b"00000",
        left_rows_read: 11,
        left_rows_returned: 12,
        left_bytes: 13,
    }
}

/// (key, encoded value, hand-written key bytes, hand-written value bytes, namespace).
type Golden = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Namespace);

fn goldens() -> Vec<Golden> {
    let outcome_hash: [u8; 32] = Sha256::digest([&[0u8; 16][..], &b"ab"[..]].concat()).into();
    vec![
        (
            mutation_head_key(T),
            MutationHead {
                def_version: 1,
                distinct_rows: 2,
                transitions: 3,
                next_seq: 4,
                next_preview: 5,
                denials: 6,
                last_decided_ms: 7,
                stored_previews: 8,
            }
            .encode(),
            h("20 0102030405060708 00"),
            h("01 | 0000000000000001 0000000000000002 0000000000000003 0000000000000004
                 0000000000000005 0000000000000006 0000000000000007 | 00000008"),
            Namespace::MutationHead,
        ),
        (
            disclosure_head_key(T),
            DisclosureHead {
                rows_read: 1,
                rows_returned: 2,
                bytes_returned: 3,
                next_read_seq: 4,
                withheld_values: 5,
            }
            .encode(),
            h("20 0102030405060708 01"),
            h("01 | 0000000000000001 0000000000000002 0000000000000003 0000000000000004
                 0000000000000005"),
            Namespace::DisclosureHead,
        ),
        (
            membership_key(T, 9, &[0xAA, 0xBB]),
            Membership { first_seq: 1, last_seq: 2, transitions: 3 }.encode(),
            h("21 0102030405060708 0000000000000009 aabb"),
            h("01 | 0000000000000001 0000000000000002 | 00000003"),
            Namespace::Membership,
        ),
        (
            outcome_key([0; 16], b"ab"),
            committed_outcome().encode(),
            [&[0x22u8][..], &outcome_hash].concat(),
            h("01 | 02 00 | 0002 6162 | 11*32 | 22*32
                 | 0000000000000001 0000000000000002 0000000000000003
                 | 00000010 0000000000000005 0000000000000006"),
            Namespace::Outcome,
        ),
        (
            receipt_key(T, 7),
            receipt().encode(),
            h("23 0102030405060708 0000000000000007"),
            h("01 | 01 01 00 | 0102030405060708 0000000000000007 0000000000000001
                 0000000000000002 0000000000000003 | 33*32 | 44*32
                 | 00000001 00000002 00000003 00000004
                 | 0000000000000005 0000000000000006 0000000000000000
                 | 00 00000000 00000000 00000000
                 | 02 6f70 | 01 69 | 01 65 | 00"),
            Namespace::Receipt,
        ),
        (
            change_header_key(T, 7),
            ChangeHeader {
                flags: 0,
                kind: 1,
                outcome_key: [0x55; 32],
                table: 9,
                row_count: 2,
                salt: [0x66; 16],
                effects_digest: [0x77; 32],
                decided_ms: 3,
            }
            .encode(),
            h("24 0102030405060708 0000000000000007"),
            h("01 | 00 01 | 55*32 | 0000000000000009 | 00000002 | 66*16 | 77*32
                 | 0000000000000003"),
            Namespace::ChangeHeader,
        ),
        (
            change_row_key(T, 7, 1),
            ChangeRow {
                flags: 0,
                table: 9,
                pk: vec![0xAA, 0xBB],
                version_before: 1,
                version_written: 2,
                before_hash: [0x88; 32],
                after_hash: [0x99; 32],
                before: vec![1, 2, 3],
            }
            .encode(),
            h("25 0102030405060708 0000000000000007 00000001"),
            h("01 | 00 | 0000000000000009 | 0002 aabb | 0000000000000001 0000000000000002
                 | 88*32 | 99*32 | 00000003 010203"),
            Namespace::ChangeRow,
        ),
        (
            LEDGER_HEADER_KEY.to_vec(),
            LedgerHeader { once_upper_watermark_s: 10, once_identities: 11, migrated_v1: true }
                .encode(),
            h("26 00"),
            h("01 | 000000000000000a 000000000000000b | 01"),
            Namespace::LedgerHeader,
        ),
        (
            preview_key(T, 5),
            preview_header().encode(),
            h("27 0102030405060708 0000000000000005"),
            h("01 | 02 00 | 0000000000000001 0000000000000002 0000000000000003 0000000000000009
                 | aa*32 | 8000000000000000 | bb*32 | 00000001 00000002 00000003 00000004
                 | 0000000000000000 | 00000001 72 | 01 cc*32 | 00 | 00000001 78 | 00000000"),
            Namespace::PreviewHeader,
        ),
        (
            preview_row_key(T, 5, 0),
            PreviewRow {
                class: 0,
                table: 9,
                pk: vec![0xAA],
                version_read: 4,
                aux: 0,
                after: vec![1, 2],
            }
            .encode(),
            h("28 0102030405060708 0000000000000005 00000000"),
            h("01 | 00 | 0000000000000009 | 00000001 aa | 0000000000000004 0000000000000000
                 | 00000002 0102"),
            Namespace::PreviewRow,
        ),
        (
            approval_key(T, 5),
            ApprovalRecord {
                state: 1,
                approver: "ap".into(),
                approver_oid: 10,
                digest: [0xDD; 32],
                approved_ms: 5,
                consumed_seq: 0,
            }
            .encode(),
            h("29 0102030405060708 0000000000000005"),
            h("01 | 01 | 00000002 6170 | 0000000a | dd*32 | 0000000000000005 0000000000000000"),
            Namespace::Approval,
        ),
        (
            read_log_key(T, 3),
            read_log().encode(),
            h("2a 0102030405060708 0000000000000003"),
            h("01 | 0000000000000001 0000000000000002 | ee*32 | 0000000000000003 0000000000000004
                 | 01 00 | ff*32 | 00000005 | 0003 73656c | 0001 | 0000000000000009
                 | 0001 0000000000000002 0003 | 000000000000000a
                 | 0000000000000001 0000000000000002 0000000000000003
                 | 0000000000000004 0000000000000005
                 | 0000000000000006 0000000000000007 0000000000000008
                 | 00000001 | 01 3030303030
                 | 000000000000000b 000000000000000c 000000000000000d"),
            Namespace::ReadLog,
        ),
        (
            debit_key(T, 3, 2),
            DebitRecord {
                wall_ms: 1,
                rows_read: 2,
                rows_returned: 3,
                bytes: 4,
                reserved_rows: 5,
                reserved_bytes: 6,
            }
            .encode(),
            h("2b 0102030405060708 0000000000000003 00000002"),
            h("01 | 0000000000000001 0000000000000002 0000000000000003 0000000000000004
                 0000000000000005 0000000000000006"),
            Namespace::Debit,
        ),
    ]
}

#[test]
fn golden_bytes_every_namespace() {
    let goldens = goldens();
    let namespaces: std::collections::BTreeSet<u8> = goldens.iter().map(|g| g.0[0]).collect();
    assert_eq!(namespaces, (NS_MIN..=NS_MAX).collect());
    for (key, value, key_bytes, value_bytes, namespace) in goldens {
        assert_eq!(key, key_bytes, "{namespace:?} key");
        assert_eq!(value, value_bytes, "{namespace:?} value");
        assert_eq!(classify_key(&key).unwrap(), namespace);
        validate(&key, &value).unwrap();
    }
    // The "exactly N bytes" rows of table D-A, recomputed for the final field lists.
    let lengths: Vec<(Namespace, usize)> =
        goldens_by_namespace().into_iter().map(|(ns, v)| (ns, v.len())).collect();
    for (namespace, len) in [
        (Namespace::MutationHead, 61),
        (Namespace::DisclosureHead, 41),
        (Namespace::Membership, 21),
        (Namespace::ChangeHeader, 103),
        (Namespace::LedgerHeader, 18),
        (Namespace::Debit, 49),
    ] {
        assert!(lengths.contains(&(namespace, len)), "{namespace:?} is {len} bytes");
    }
}

fn goldens_by_namespace() -> Vec<(Namespace, Vec<u8>)> {
    goldens().into_iter().map(|(_, value, _, _, ns)| (ns, value)).collect()
}

#[test]
fn round_trip_all() {
    let mut outcomes = vec![committed_outcome()];
    for payload in [
        OutcomePayload::Once(b"GDOU\x01payload".to_vec()),
        OutcomePayload::Denied { task: T, rule: 7, denial_no: 3 },
        OutcomePayload::BudgetExceeded { task: T, budget: 2, denial_no: 4 },
    ] {
        outcomes.push(OutcomeRecord { payload, ..committed_outcome() });
    }
    outcomes.push(OutcomeRecord {
        flags: OUTCOME_PAYLOAD_BLANKED,
        identity: vec![0x7F; MAX_IDENTITY],
        payload: OutcomePayload::Once(Vec::new()),
        ..committed_outcome()
    });
    for record in outcomes {
        assert_eq!(OutcomeRecord::decode(&record.encode()).unwrap(), record);
    }
    let mut full = receipt();
    full.approver = Some("approver".into());
    full.undo_mode = 2;
    full.restored = 3;
    full.conflicts = 4;
    full.superseded = 5;
    full.op_id = vec![0xAB; MAX_OPERATION_ID];
    full.issuer = "é".repeat(100);
    for record in [receipt(), full] {
        assert_eq!(ReceiptRecord::decode(&record.encode()).unwrap(), record);
        validate(&receipt_key(record.task, record.seq), &record.encode()).unwrap();
    }
    let blanked = ChangeRow {
        flags: ROW_NOOP | ROW_BEFORE_BLANKED,
        pk: vec![1; 300],
        ..Default::default()
    };
    assert_eq!(ChangeRow::decode(&blanked.encode()).unwrap(), blanked);
    let undo = ChangeHeader { kind: 2, flags: CHANGE_BEFORE_BLANKED, ..Default::default() };
    assert_eq!(ChangeHeader::decode(&undo.encode()).unwrap(), undo);
    let change = PreviewHeader {
        kind: 1,
        state: 1,
        scope: None,
        surface: 1,
        clock_pin: -5,
        text: "UPDATE t SET a = $1".into(),
        params: vec![9; 40],
        ..preview_header()
    };
    for record in [preview_header(), change] {
        assert_eq!(PreviewHeader::decode(&record.encode()).unwrap(), record);
    }
    for class in [1, 2] {
        let row = PreviewRow { class, pk: vec![3], aux: 77, ..Default::default() };
        assert_eq!(PreviewRow::decode(&row.encode()).unwrap(), row);
    }
    for state in 1..=3 {
        let approval = ApprovalRecord { state, consumed_seq: 9, ..Default::default() };
        assert_eq!(ApprovalRecord::decode(&approval.encode()).unwrap(), approval);
    }
    let mut log = read_log();
    log.columns = (0..50).map(|i| ReadColumn { column_id: i, position_bits: i as u16 }).collect();
    log.text = "ü".repeat(512);
    log.text_len = 4096;
    log.outcome = 5;
    assert_eq!(ReadLogRecord::decode(&log.encode()).unwrap(), log);
    let empty = ReadLogRecord::default();
    assert_eq!(ReadLogRecord::decode(&empty.encode()).unwrap(), empty);
    for (key, value, _, _, _) in goldens() {
        let again = match classify_key(&key).unwrap() {
            Namespace::MutationHead => MutationHead::decode(&value).unwrap().encode(),
            Namespace::DisclosureHead => DisclosureHead::decode(&value).unwrap().encode(),
            Namespace::Membership => Membership::decode(&value).unwrap().encode(),
            Namespace::Outcome => OutcomeRecord::decode(&value).unwrap().encode(),
            Namespace::Receipt => ReceiptRecord::decode(&value).unwrap().encode(),
            Namespace::ChangeHeader => ChangeHeader::decode(&value).unwrap().encode(),
            Namespace::ChangeRow => ChangeRow::decode(&value).unwrap().encode(),
            Namespace::LedgerHeader => LedgerHeader::decode(&value).unwrap().encode(),
            Namespace::PreviewHeader => PreviewHeader::decode(&value).unwrap().encode(),
            Namespace::PreviewRow => PreviewRow::decode(&value).unwrap().encode(),
            Namespace::Approval => ApprovalRecord::decode(&value).unwrap().encode(),
            Namespace::ReadLog => ReadLogRecord::decode(&value).unwrap().encode(),
            Namespace::Debit => DebitRecord::decode(&value).unwrap().encode(),
        };
        assert_eq!(again, value);
    }
}

#[test]
fn key_lengths_and_prefix_freedom() {
    let keys: Vec<Vec<u8>> = goldens().into_iter().map(|g| g.0).collect();
    for key in &keys {
        let namespace = classify_key(key).unwrap();
        let expected = match namespace {
            Namespace::MutationHead | Namespace::DisclosureHead => 10,
            Namespace::Membership => 17 + 2,
            Namespace::Outcome => 33,
            Namespace::LedgerHeader => 2,
            Namespace::Receipt
            | Namespace::ChangeHeader
            | Namespace::PreviewHeader
            | Namespace::Approval
            | Namespace::ReadLog => 17,
            Namespace::ChangeRow | Namespace::PreviewRow | Namespace::Debit => 21,
        };
        assert_eq!(key.len(), expected, "{namespace:?}");
        // One byte more or less is never a key of the same namespace, except
        // that a longer membership key is a different primary key.
        let mut longer = key.clone();
        longer.push(0);
        assert_eq!(
            classify_key(&longer).is_ok(),
            namespace == Namespace::Membership,
            "{namespace:?} longer"
        );
        assert_eq!(
            classify_key(&key[..key.len() - 1]).is_ok(),
            namespace == Namespace::Membership && key.len() - 1 > 17,
            "{namespace:?} shorter"
        );
    }
    // Fixed-width keys are never prefixes of each other.
    for a in &keys {
        for b in &keys {
            if a != b {
                assert!(!b.starts_with(a), "{a:02x?} is a prefix of {b:02x?}");
            }
        }
    }
    // A membership key needs at least one primary-key byte.
    assert!(classify_key(&h("21 0102030405060708 0000000000000009")).is_err());
    for bad in [
        vec![],
        vec![0x1F],
        vec![0x2C, 0],
        vec![0x26, 1],
        vec![0x26],
        h("20 0102030405060708 02"),
    ] {
        assert!(matches!(classify_key(&bad), Err(CoreError::Corrupt(_))), "{bad:02x?}");
    }
    // Task prefixes cover exactly one task.
    for ns in [0x20, 0x21, 0x23, 0x24, 0x25, 0x27, 0x28, 0x29, 0x2A, 0x2B] {
        let (start, end) = task_prefix(ns, T);
        let inside = keys.iter().filter(|k| k[0] == ns).collect::<Vec<_>>();
        assert!(!inside.is_empty());
        for key in inside {
            assert!(start.as_slice() <= key.as_slice() && key.as_slice() < end.as_slice());
        }
        let next = task_prefix(ns, TaskObjectId(T.0 + 1));
        assert_eq!(next.0, end);
        assert!(task_prefix(ns, TaskObjectId(T.0 - 1)).1 <= start);
        assert_eq!(task_prefix(ns, TaskObjectId(u64::MAX)).1, vec![ns + 1]);
    }
    assert_eq!(task_prefix(0x22, T), (vec![0x22], vec![0x23]));
    assert_eq!(task_prefix(0x26, T), (vec![0x26], vec![0x27]));
}

#[test]
fn trailing_and_short_values_rejected() {
    for (key, value, _, _, namespace) in goldens() {
        let mut trailing = value.clone();
        trailing.push(0);
        assert!(
            matches!(validate(&key, &trailing), Err(CoreError::Corrupt(_))),
            "{namespace:?} trailing"
        );
        for len in 0..value.len() {
            assert!(
                matches!(validate(&key, &value[..len]), Err(CoreError::Corrupt(_))),
                "{namespace:?} truncated to {len}"
            );
        }
    }
    // A length prefix larger than the input fails before allocating it.
    let mut huge = h("01 01 00 0002 6162 11*32 22*32");
    huge.extend([0; 24]);
    huge.extend(u32::MAX.to_be_bytes());
    assert!(matches!(OutcomeRecord::decode(&huge), Err(CoreError::Corrupt(_))));
    let mut row = ChangeRow { before: vec![1], ..Default::default() }.encode();
    let at = row.len() - 5;
    row[at..at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(ChangeRow::decode(&row), Err(CoreError::Corrupt(_))));
    // Field bounds: empty op id, oversized read-log text, empty pk, bad UTF-8.
    let mut empty_op = receipt().encode();
    let at = empty_op.len() - 8;
    assert_eq!(&empty_op[at..at + 3], h("02 6f70").as_slice());
    empty_op.splice(at..at + 3, [0]);
    assert!(matches!(ReceiptRecord::decode(&empty_op), Err(CoreError::Corrupt(_))));
    let mut long = read_log();
    long.text = "a".repeat(MAX_READ_LOG_TEXT);
    long.text_len = MAX_READ_LOG_TEXT as u32;
    let mut bytes = long.encode();
    assert_eq!(ReadLogRecord::decode(&bytes).unwrap(), long);
    let at = 1 + 8 + 8 + 32 + 8 + 8 + 1 + 1 + 32 + 4;
    bytes[at..at + 2].copy_from_slice(&((MAX_READ_LOG_TEXT + 1) as u16).to_be_bytes());
    bytes.insert(at + 2, b'a');
    assert!(matches!(ReadLogRecord::decode(&bytes), Err(CoreError::Corrupt(_))));
    let mut short_text = read_log();
    short_text.text_len = 2;
    assert!(ReadLogRecord::decode(&short_text.encode()).is_err());
    let pkless = PreviewRow { pk: Vec::new(), ..Default::default() }.encode();
    assert!(matches!(PreviewRow::decode(&pkless), Err(CoreError::Corrupt(_))));
    let mut bad_utf8 = ApprovalRecord { approver: "ab".into(), state: 1, ..Default::default() }
        .encode();
    bad_utf8[6] = 0xFF;
    assert!(matches!(ApprovalRecord::decode(&bad_utf8), Err(CoreError::Corrupt(_))));
    // Enumerated fields outside their sets.
    let mut ledger = LedgerHeader::default().encode();
    ledger[17] = 2;
    assert!(LedgerHeader::decode(&ledger).is_err());
    assert!(ChangeHeader::decode(&ChangeHeader { kind: 3, ..Default::default() }.encode()).is_err());
    assert!(ChangeHeader::decode(&ChangeHeader { kind: 1, flags: 2, ..Default::default() }.encode()).is_err());
    let unblanked = ChangeRow { flags: ROW_BEFORE_BLANKED, before: vec![1], ..Default::default() };
    assert!(ChangeRow::decode(&unblanked.encode()).is_err());
    let missing = ChangeRow { flags: 0, before: Vec::new(), ..Default::default() };
    assert!(ChangeRow::decode(&missing.encode()).is_err());
    assert!(PreviewRow::decode(&PreviewRow { class: 3, pk: vec![1], ..Default::default() }.encode()).is_err());
    let after_on_conflict = PreviewRow { class: 1, pk: vec![1], after: vec![1], ..Default::default() };
    assert!(PreviewRow::decode(&after_on_conflict.encode()).is_err());
    assert!(ApprovalRecord::decode(&ApprovalRecord::default().encode()).is_err());
    assert!(PreviewHeader::decode(&PreviewHeader { kind: 3, ..preview_header() }.encode()).is_err());
    assert!(PreviewHeader::decode(&PreviewHeader { state: 2, ..preview_header() }.encode()).is_err());
    assert!(Membership::decode(&Membership { first_seq: 2, last_seq: 1, transitions: 1 }.encode()).is_err());
    let mut entry = read_log();
    entry.entry = 5;
    assert!(ReadLogRecord::decode(&entry.encode()).is_err());
    // A receipt stored under another task or seq is corrupt.
    assert!(validate(&receipt_key(T, 8), &receipt().encode()).is_err());
    assert!(validate(&receipt_key(TaskObjectId(1), 7), &receipt().encode()).is_err());
}

#[test]
fn unknown_version_unsupported() {
    for (key, value, _, _, namespace) in goldens() {
        for version in [0u8, 2, 0x7F, 0xFF] {
            let mut other = value.clone();
            other[0] = version;
            assert!(
                matches!(validate(&key, &other), Err(CoreError::Unsupported)),
                "{namespace:?} version {version}"
            );
            // Even a bare version byte is unsupported rather than short.
            assert!(matches!(validate(&key, &[version]), Err(CoreError::Unsupported)));
        }
    }
}

#[test]
fn outcome_payload_per_kind() {
    let cases = [
        (OutcomePayload::Once(b"GDOU".to_vec()), 1, h("47444f55")),
        (
            OutcomePayload::Committed { task: TaskObjectId(5), seq: 6 },
            2,
            h("0000000000000005 0000000000000006"),
        ),
        (
            OutcomePayload::Denied { task: TaskObjectId(5), rule: 0x0102, denial_no: 9 },
            3,
            h("0000000000000005 0102 0000000000000009"),
        ),
        (
            OutcomePayload::BudgetExceeded { task: TaskObjectId(5), budget: 3, denial_no: 1 },
            4,
            h("0000000000000005 0003 0000000000000001"),
        ),
    ];
    // kind byte at 1, flags at 2, identity 2+2, fps 64, times 24, payload length 4.
    let payload_at = 1 + 2 + 4 + 64 + 24;
    for (payload, kind, bytes) in cases {
        assert_eq!(payload.kind(), kind);
        let record = OutcomeRecord { payload, ..committed_outcome() };
        let encoded = record.encode();
        assert_eq!(encoded[1], kind);
        assert_eq!(
            &encoded[payload_at..payload_at + 4],
            (bytes.len() as u32).to_be_bytes().as_slice()
        );
        assert_eq!(&encoded[payload_at + 4..], bytes.as_slice());
        assert_eq!(OutcomeRecord::decode(&encoded).unwrap(), record);
        if kind != 1 {
            // A fixed-size payload one byte short or long is corrupt.
            for delta in [-1i64, 1] {
                let mut wrong = encoded[..payload_at].to_vec();
                let len = (bytes.len() as i64 + delta) as usize;
                wrong.extend((len as u32).to_be_bytes());
                wrong.extend(std::iter::repeat_n(0, len));
                assert!(matches!(OutcomeRecord::decode(&wrong), Err(CoreError::Corrupt(_))));
            }
            // Flags belong to kind 1 only.
            let mut flagged = encoded.clone();
            flagged[2] = OUTCOME_PAYLOAD_BLANKED;
            assert!(OutcomeRecord::decode(&flagged).is_err());
        }
    }
    let mut unknown = committed_outcome().encode();
    for kind in [0, 5, 0xFF] {
        unknown[1] = kind;
        assert!(matches!(OutcomeRecord::decode(&unknown), Err(CoreError::Corrupt(_))));
    }
    // Kind 1: blanked iff empty; unknown flag bits are corrupt.
    for (flags, payload) in [(0, Vec::new()), (OUTCOME_PAYLOAD_BLANKED, vec![1]), (2, vec![1])] {
        let record = OutcomeRecord {
            flags,
            payload: OutcomePayload::Once(payload),
            ..committed_outcome()
        };
        assert!(OutcomeRecord::decode(&record.encode()).is_err(), "flags {flags}");
    }
    let oversized = OutcomeRecord {
        payload: OutcomePayload::Once(vec![0; MAX_OUTCOME_PAYLOAD + 1]),
        ..committed_outcome()
    };
    assert!(OutcomeRecord::decode(&oversized.encode()).is_err());
    let inverted = OutcomeRecord { decided_lower_ms: 3, decided_upper_ms: 2, ..committed_outcome() };
    assert!(OutcomeRecord::decode(&inverted.encode()).is_err());
    let mut no_identity = committed_outcome().encode();
    no_identity.splice(3..7, [0, 0]);
    assert!(OutcomeRecord::decode(&no_identity).is_err());
}

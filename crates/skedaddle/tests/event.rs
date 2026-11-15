use std::{
    cmp::Ordering,
    collections::BTreeSet,
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
};

use sha2::{Digest, Sha256};
use skedaddle::event::{
    AssembledTxn, Event, EventError, EventKind, OpaqueValue, Position, PositionOrder,
    PositionWireV1, Redelivery, RedeliveryCheck, Row, SourceIdentity, SourceOrigin, StreamingTxn,
    TransactionMetadata, TxnId,
};

#[test]
fn position_wire_v1_roundtrips_opaque_domain_and_bytes() {
    let position = Position::new("stream/東京/é", vec![0, 15, 128, 254, 255]);
    let encoded = serde_json::to_string(&PositionWireV1::from(&position)).unwrap();
    assert_eq!(
        encoded,
        r#"{"order_domain":"stream/東京/é","bytes":"000f80feff"}"#
    );
    let wire: PositionWireV1 = serde_json::from_str(&encoded).unwrap();
    assert_eq!(Position::try_from(wire).unwrap(), position);
    let empty = Position::new("", vec![]);
    let wire = serde_json::to_vec(&PositionWireV1::from(&empty)).unwrap();
    assert_eq!(
        Position::try_from(serde_json::from_slice::<PositionWireV1>(&wire).unwrap()).unwrap(),
        empty
    );
}

#[test]
fn position_wire_v1_rejects_malformed_json_and_noncanonical_hex() {
    for malformed in [
        r#"{"bytes":"00"}"#,
        r#"{"order_domain":"d"}"#,
        r#"{"order_domain":"d","bytes":[0]}"#,
        r#"{"order_domain":"d","bytes":"00","extra":1}"#,
        r#"{"order_domain":"d","bytes":"00","bytes":"01"}"#,
        r#"{"order_domain":null,"bytes":"00"}"#,
        r#"{"order_domain":"d","bytes":"00""#,
    ] {
        assert!(serde_json::from_str::<PositionWireV1>(malformed).is_err());
    }
    for (bytes, reason) in [
        ("0", "hex length"),
        ("FF", "lowercase hex"),
        ("gg", "lowercase hex"),
        ("é", "lowercase hex"),
        ("0 ", "lowercase hex"),
    ] {
        let json = serde_json::json!({"order_domain": "d", "bytes": bytes});
        let wire = serde_json::from_value::<PositionWireV1>(json).unwrap();
        assert_eq!(Position::try_from(wire), Err(reason));
    }
}

fn metadata(txn: &AssembledTxn) -> TransactionMetadata {
    TransactionMetadata {
        source: txn.source.clone(),
        txn: txn.txn.clone(),
        commit_position: txn.commit_position.clone(),
        commit_time: txn.commit_time,
    }
}

struct ShortStorage(Cursor<Vec<u8>>);
impl Read for ShortStorage {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let size = out.len().min(3);
        self.0.read(&mut out[..size])
    }
}
impl Write for ShortStorage {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(&bytes[..bytes.len().min(3)])
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Seek for ShortStorage {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}

#[test]
fn streaming_canonical_bytes_and_digests_match_v3_with_short_io() {
    let mut event_bytes = ShortStorage(Cursor::new(Vec::new()));
    sample().write_canonical(&mut event_bytes).unwrap();
    assert_eq!(event_bytes.0.into_inner(), unhex(GOLDEN));
    for time in [None, Some(i64::MIN), Some(0), Some(i64::MAX)] {
        for count in [0, 1, 7] {
            let events: Vec<_> = (0..count)
                .map(|ordinal| Event {
                    ordinal_in_txn: ordinal,
                    ..sample()
                })
                .collect();
            let mut txn = assembled(events.clone());
            txn = AssembledTxn::new(
                txn.source,
                txn.txn,
                txn.commit_position,
                time,
                events.clone(),
            )
            .unwrap();
            let mut streamed = StreamingTxn::from_events(
                metadata(&txn),
                count as u32,
                events.into_iter().map(Ok),
                ShortStorage(Cursor::new(Vec::new())),
            )
            .unwrap();
            assert_eq!(streamed.event_count(), count as u32);
            assert_eq!(streamed.digest(), txn.digest);
            let expected = txn.canonical_bytes().unwrap();
            assert_eq!(streamed.canonical_len(), expected.len() as u64);
            for _ in 0..2 {
                let mut actual = Vec::new();
                streamed
                    .canonical_reader()
                    .unwrap()
                    .read_to_end(&mut actual)
                    .unwrap();
                assert_eq!(actual, expected);
            }
            let mut actual = ShortStorage(Cursor::new(Vec::new()));
            txn.write_canonical(&mut actual).unwrap();
            assert_eq!(actual.0.into_inner(), expected);
        }
    }
}

#[test]
fn streaming_transactions_reject_membership_order_count_and_storage_errors() {
    let txn = assembled(vec![]);
    let make = |count, events: Vec<Result<Event, EventError>>| {
        StreamingTxn::from_events(metadata(&txn), count, events, Cursor::new(Vec::new()))
    };
    assert!(matches!(
        make(1, vec![]),
        Err(EventError::InvalidTransaction(_))
    ));
    assert!(matches!(
        make(0, vec![Ok(sample())]),
        Err(EventError::InvalidTransaction(_))
    ));
    assert!(matches!(
        make(2, vec![Ok(sample()), Ok(sample())]),
        Err(EventError::InvalidTransaction(_))
    ));
    let mut bad = sample();
    bad.source.stream.push('x');
    assert!(matches!(
        make(1, vec![Ok(bad)]),
        Err(EventError::InvalidTransaction(_))
    ));
    let mut bad = sample();
    bad.txn.0.push(0);
    assert!(matches!(
        make(1, vec![Ok(bad)]),
        Err(EventError::InvalidTransaction(_))
    ));
    let mut bad = sample();
    bad.position = Position::new("other", vec![]);
    assert!(matches!(
        make(1, vec![Ok(bad)]),
        Err(EventError::PositionDomainMismatch)
    ));
    assert!(matches!(
        make(1, vec![Err(EventError::DigestMismatch)]),
        Err(EventError::DigestMismatch)
    ));
    assert!(matches!(
        StreamingTxn::from_events(metadata(&txn), 0, [], Cursor::new(vec![0])),
        Err(EventError::InvalidTransaction(
            "stream storage must be empty"
        ))
    ));
    struct Failed;
    impl Write for Failed {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::WriteZero.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert!(matches!(
        sample().write_canonical(&mut Failed),
        Err(EventError::Io(io::ErrorKind::WriteZero))
    ));
    let mut changed = assembled(vec![sample()]);
    changed.events[0].table = None;
    assert_eq!(
        changed.write_canonical(&mut io::sink()),
        Err(EventError::DigestMismatch)
    );
}

fn value(type_id: &str, bytes: Option<&[u8]>) -> OpaqueValue {
    OpaqueValue {
        type_id: type_id.into(),
        bytes: bytes.map(Vec::from),
    }
}

fn sample() -> Event {
    Event {
        source: SourceIdentity {
            engine: "pg".into(),
            system_id: "s".into(),
            lineage: "l".into(),
            db_name: "db".into(),
            stream: "slot".into(),
        },
        txn: TxnId(vec![1, 2]),
        ordinal_in_txn: 7,
        position: Position::new("pg/s/l/db/slot", vec![9]),
        schema_version: 3,
        kind: EventKind::Update,
        table: Some("public.t".into()),
        key: Row::from([("id".into(), value("pg/int4", Some(b"42")))]),
        before: Some(Row::from([("v".into(), value("pg/text", None))])),
        after: Some(Row::from([("v".into(), value("pg/text", Some(b"")))])),
        unchanged_cols: BTreeSet::from(["toast".into()]),
    }
}

fn assembled(events: Vec<Event>) -> AssembledTxn {
    let event = sample();
    AssembledTxn::new(event.source, event.txn, event.position, Some(-123), events).unwrap()
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

// Independently generated using Python struct.pack and hashlib; this is the
// entire record, including its integrity trailer, not a self-derived oracle.
// Original v1 bytes retained as a compatibility rejection fixture.
const GOLDEN_V1: &str = "534b45560001010000000270670000000173000000016c00000002646200000004736c6f7400000002010200000000000000070000000e70672f732f6c2f64622f736c6f74000000010900000000000000030101000000087075626c69632e74000000010000000269640000000770672f696e743401000000023432010000000100000001760000000770672f7465787400010000000100000001760000000770672f7465787401000000000000000100000005746f6173747c216845296d9d1350679a590b28fae78f412e4b27e5b49ca9ea034967aaaaad";
const GOLDEN_V2: &str = "534b45560002010000000270670000000173000000016c00000002646200000004736c6f7400000002010200000000000000070000000e70672f732f6c2f64622f736c6f74000000010900000000000000030101000000087075626c69632e74000000010000000269640000000770672f696e743401000000023432010000000100000001760000000770672f7465787400010000000100000001760000000770672f7465787401000000000000000100000005746f617374d65c365a277a873447b3a7867845dce22956778e52c75f9e2b9d542fad2c8103";
const GOLDEN_TXN_V2: &str = "534b45560002020000000270670000000173000000016c00000002646200000004736c6f740000000101000000016400000001090000000000007f080fc05aceac5ed7d42a4a27caf5e76f6db40bfea4ed1305531b6b376ee78b";
const GOLDEN: &str = "534b45560003010000000270670000000173000000016c00000002646200000004736c6f7400000002010200000000000000070000000e70672f732f6c2f64622f736c6f74000000010900000000000000030101000000087075626c69632e74000000010000000269640000000770672f696e743401000000023432010000000100000001760000000770672f7465787400010000000100000001760000000770672f7465787401000000000000000100000005746f617374c3fe1fc0fb06d3be2f4e0737205ae2350251e904e913db8f62a6bb0565b52a5f";

#[test]
fn canonical_encoding_is_stable() {
    assert_eq!(sample().canonical_bytes().unwrap(), unhex(GOLDEN));
    let mut a = sample();
    a.key.insert("z".into(), value("binary", Some(&[0, 255])));
    a.key.insert("a".into(), value("binary", None));
    let mut b = a.clone();
    b.key = a.key.clone().into_iter().rev().collect();
    assert_eq!(a.canonical_bytes().unwrap(), b.canonical_bytes().unwrap());
}

#[test]
fn encoding_round_trips() {
    for kind in [
        EventKind::Insert,
        EventKind::Update,
        EventKind::Delete,
        EventKind::Truncate {
            relations: vec!["public.a".into(), "public.b".into()],
            cascade: true,
            restart_identity: true,
        },
        EventKind::DdlBoundary,
        EventKind::Watermark,
        EventKind::FinalMarker,
        EventKind::ChunkRows,
    ] {
        let mut event = sample();
        if matches!(kind, EventKind::Truncate { .. }) {
            event.table = None;
            event.key.clear();
            event.before = None;
            event.after = None;
            event.unchanged_cols.clear();
        }
        event.kind = kind;
        assert_eq!(
            Event::from_canonical_bytes(&event.canonical_bytes().unwrap()).unwrap(),
            event
        );
    }
    for commit_time in [None, Some(i64::MIN), Some(0), Some(i64::MAX)] {
        for events in [vec![], vec![sample()]] {
            let mut txn = assembled(events.clone());
            txn = AssembledTxn::new(
                txn.source,
                txn.txn,
                txn.commit_position,
                commit_time,
                events,
            )
            .unwrap();
            let bytes = txn.canonical_bytes().unwrap();
            assert_eq!(AssembledTxn::from_canonical_bytes(&bytes).unwrap(), txn);
        }
    }
    let mut event = sample();
    event.table = None;
    event.before = None;
    event.after = None;
    event.key.clear();
    event.unchanged_cols.clear();
    event.ordinal_in_txn = u64::MAX;
    event.schema_version = u64::MAX;
    event.position = Position::new("多字节", vec![0, 255, 0]);
    assert_eq!(
        Event::from_canonical_bytes(&event.canonical_bytes().unwrap()).unwrap(),
        event
    );
}

#[test]
fn unknown_version_is_rejected() {
    for mut bytes in [
        sample().canonical_bytes().unwrap(),
        assembled(vec![sample()]).canonical_bytes().unwrap(),
    ] {
        bytes[4..6].copy_from_slice(&99u16.to_be_bytes());
        assert_eq!(
            Event::from_canonical_bytes(&bytes),
            Err(EventError::UnsupportedVersion(99))
        );
        assert_eq!(
            AssembledTxn::from_canonical_bytes(&bytes),
            Err(EventError::UnsupportedVersion(99))
        );
    }
}

#[test]
fn truncated_or_corrupted_encoding_never_panics() {
    for bytes in [
        sample().canonical_bytes().unwrap(),
        assembled(vec![sample()]).canonical_bytes().unwrap(),
    ] {
        let transaction = bytes[6] == 2;
        let decode = |bytes: &[u8]| {
            if transaction {
                AssembledTxn::from_canonical_bytes(bytes).map(|_| ())
            } else {
                Event::from_canonical_bytes(bytes).map(|_| ())
            }
        };
        for end in 0..bytes.len() {
            assert!(decode(&bytes[..end]).is_err(), "prefix length {end}");
        }
        for index in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes.clone();
                bad[index] ^= 1 << bit;
                assert!(decode(&bad).is_err(), "byte {index}, bit {bit}");
            }
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }
}

fn reseal(bytes: &mut [u8]) {
    let end = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&digest);
}

#[test]
fn malformed_fields_rejected_even_with_valid_checksum() {
    let original = sample().canonical_bytes().unwrap();
    // First source string length, first source UTF-8 byte, event kind, option tag.
    for (index, replacement) in [(7, 255), (11, 255), (82, 255), (83, 255)] {
        let mut bytes = original.clone();
        bytes[index] = replacement;
        reseal(&mut bytes);
        assert!(
            Event::from_canonical_bytes(&bytes).is_err(),
            "index {index}"
        );
    }
    let mut trailing = original[..original.len() - 32].to_vec();
    trailing.push(0);
    trailing.extend([0; 32]);
    reseal(&mut trailing);
    assert!(Event::from_canonical_bytes(&trailing).is_err());
}

#[test]
fn duplicate_or_unsorted_columns_are_rejected() {
    let mut event = sample();
    event.key = Row::from([
        ("aaa".into(), value("binary", None)),
        ("bbb".into(), value("binary", None)),
    ]);
    event.unchanged_cols = BTreeSet::from(["ccc".into(), "ddd".into()]);
    for (needle, replacement) in [
        (b"bbb", b"aaa"),
        (b"bbb", b"000"),
        (b"ddd", b"ccc"),
        (b"ddd", b"000"),
    ] {
        let mut bytes = event.canonical_bytes().unwrap();
        let offset = bytes.windows(3).position(|b| b == needle).unwrap();
        bytes[offset..offset + 3].copy_from_slice(replacement);
        reseal(&mut bytes);
        assert!(Event::from_canonical_bytes(&bytes).is_err());
    }
}

#[test]
fn accepted_mutations_have_exactly_one_canonical_representation() {
    for bytes in [
        sample().canonical_bytes().unwrap(),
        assembled(vec![sample()]).canonical_bytes().unwrap(),
    ] {
        let transaction = bytes[6] == 2;
        for index in 7..bytes.len() - 32 {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            reseal(&mut changed);
            // Some changes are valid new content. Those must re-encode exactly;
            // invalid structure must return an error without panicking.
            if transaction {
                if let Ok(txn) = AssembledTxn::from_canonical_bytes(&changed) {
                    assert_eq!(txn.canonical_bytes().unwrap(), changed);
                }
            } else if let Ok(event) = Event::from_canonical_bytes(&changed) {
                assert_eq!(event.canonical_bytes().unwrap(), changed);
            }
        }
    }
}

#[test]
fn deterministic_arbitrary_inputs_never_panic() {
    // Dependency-free bounded adversarial corpus, including valid envelopes
    // around arbitrary fields to exercise the parser past its checksum gate.
    let mut seed = 0x5eed_u64;
    for length in 0..512 {
        let mut bytes = vec![0; length];
        for byte in &mut bytes {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (seed >> 32) as u8;
        }
        let _ = Event::from_canonical_bytes(&bytes);
        let _ = AssembledTxn::from_canonical_bytes(&bytes);
        if length >= 39 {
            bytes[..6].copy_from_slice(b"SKEV\0\x01");
            bytes[6] = 1;
            reseal(&mut bytes);
            assert!(Event::from_canonical_bytes(&bytes).is_err());
            bytes[6] = 2;
            reseal(&mut bytes);
            assert!(AssembledTxn::from_canonical_bytes(&bytes).is_err());
        }
    }
}

#[test]
fn digest_identity() {
    let event = sample();
    assert_eq!(
        event.digest().unwrap().to_vec(),
        unhex("c3fe1fc0fb06d3be2f4e0737205ae2350251e904e913db8f62a6bb0565b52a5f")
    );
    let bytes = event.canonical_bytes().unwrap();
    assert_eq!(
        event.digest().unwrap().as_slice(),
        &bytes[bytes.len() - 32..]
    );
    let txn = assembled(vec![event.clone()]);
    let bytes = txn.canonical_bytes().unwrap();
    assert_eq!(txn.digest.as_slice(), &bytes[bytes.len() - 32..]);
    let expected: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - 32]).into();
    assert_eq!(txn.digest, expected);
    assert_eq!(assembled(vec![event.clone()]).digest, txn.digest);
    let mut changed = event;
    changed.unchanged_cols.insert("another".into());
    assert_ne!(assembled(vec![changed]).digest, txn.digest);
}

#[test]
fn redelivery_identical_is_noop() {
    let event = sample();
    let txn = assembled(vec![event.clone()]);
    let mut check = RedeliveryCheck::default();
    assert_eq!(check.check_event(&event).unwrap(), Redelivery::New);
    assert_eq!(check.check_event(&event).unwrap(), Redelivery::Noop);
    assert_eq!(check.check_transaction(&txn).unwrap(), Redelivery::New);
    assert_eq!(check.check_transaction(&txn).unwrap(), Redelivery::Noop);
    assert_eq!(check.check_event(&event).unwrap(), Redelivery::Noop);
    let mut check = RedeliveryCheck::default();
    assert_eq!(check.check_transaction(&txn).unwrap(), Redelivery::New);
    assert_eq!(check.check_event(&event).unwrap(), Redelivery::Noop);
}

#[test]
fn redelivery_conflicting_is_error() {
    let event = sample();
    let mut changed = event.clone();
    changed
        .after
        .as_mut()
        .unwrap()
        .insert("v".into(), value("pg/text", Some(b"changed")));
    let mut check = RedeliveryCheck::default();
    check.check_event(&event).unwrap();
    assert!(matches!(
        check.check_event(&changed),
        Err(EventError::RedeliveryConflict { .. })
    ));
    assert!(matches!(
        check.check_transaction(&assembled(vec![changed.clone()])),
        Err(EventError::RedeliveryConflict { .. })
    ));
    check
        .check_transaction(&assembled(vec![event.clone()]))
        .unwrap();
    assert!(matches!(
        check.check_transaction(&assembled(vec![changed])),
        Err(EventError::RedeliveryConflict { .. })
    ));
    let mut extra = event;
    extra.ordinal_in_txn += 1;
    assert!(matches!(
        check.check_event(&extra),
        Err(EventError::RedeliveryConflict { .. })
    ));
}

#[test]
fn failed_redelivery_check_is_atomic() {
    let mut first = sample();
    first.ordinal_in_txn = 1;
    let second = sample();
    let mut changed = second.clone();
    changed.schema_version += 1;
    let mut check = RedeliveryCheck::default();
    check.check_event(&second).unwrap();
    assert!(
        check
            .check_transaction(&assembled(vec![first.clone(), changed]))
            .is_err()
    );
    first.schema_version += 1;
    assert_eq!(check.check_event(&first).unwrap(), Redelivery::New);
    assert!(
        check
            .check_transaction(&assembled(vec![first.clone()]))
            .is_err()
    );
    assert_eq!(
        check
            .check_transaction(&assembled(vec![first, second]))
            .unwrap(),
        Redelivery::New
    );
}

#[test]
fn source_identity_scopes_redelivery() {
    let first = sample();
    let mut second = first.clone();
    second.source.lineage = "new lineage".into();
    second.schema_version += 1;
    let mut check = RedeliveryCheck::default();
    assert_eq!(check.check_event(&first).unwrap(), Redelivery::New);
    assert_eq!(check.check_event(&second).unwrap(), Redelivery::New);
}

struct ReverseByteOrder;

impl PositionOrder for ReverseByteOrder {
    fn domain(&self) -> &str {
        "connector/stream/lineage"
    }
    fn compare(&self, left: &[u8], right: &[u8]) -> Result<Ordering, EventError> {
        if left.len() != 1 || right.len() != 1 {
            return Err(EventError::InvalidEncoding("connector position"));
        }
        Ok(right[0].cmp(&left[0]))
    }
}

#[test]
fn positions_compare_only_through_connector_order() {
    let a = Position::new(ReverseByteOrder.domain(), vec![1]);
    let b = Position::new(ReverseByteOrder.domain(), vec![2]);
    assert_eq!(a.compare(&b, &ReverseByteOrder).unwrap(), Ordering::Greater);
    assert_eq!(b.compare(&a, &ReverseByteOrder).unwrap(), Ordering::Less);
    assert_eq!(a.compare(&a, &ReverseByteOrder).unwrap(), Ordering::Equal);
    let unrelated = Position::new("another stream", vec![1]);
    assert_eq!(
        a.compare(&unrelated, &ReverseByteOrder),
        Err(EventError::PositionDomainMismatch)
    );
    assert_eq!(
        unrelated.compare(&unrelated, &ReverseByteOrder),
        Err(EventError::PositionDomainMismatch)
    );
    let malformed = Position::new(ReverseByteOrder.domain(), vec![]);
    assert!(a.compare(&malformed, &ReverseByteOrder).is_err());
}

#[test]
fn transaction_rejects_mismatched_or_repeated_events() {
    let event = sample();
    for change in 0..4 {
        let mut changed = event.clone();
        match change {
            0 => changed.source.stream.push('x'),
            1 => changed.txn.0.push(0),
            2 => changed.position = Position::new("another stream", vec![]),
            _ => {}
        }
        let events = if change == 3 {
            vec![event.clone(), changed]
        } else {
            vec![changed]
        };
        assert!(
            AssembledTxn::new(
                event.source.clone(),
                event.txn.clone(),
                event.position.clone(),
                None,
                events
            )
            .is_err()
        );
    }
    let mut stale = assembled(vec![event]);
    stale.events[0].schema_version += 1;
    assert_eq!(stale.canonical_bytes(), Err(EventError::DigestMismatch));
    assert_eq!(
        RedeliveryCheck::default().check_transaction(&stale),
        Err(EventError::DigestMismatch)
    );
}

#[test]
fn toast_null_empty_and_absent_remain_distinct() {
    let event = sample();
    let decoded = Event::from_canonical_bytes(&event.canonical_bytes().unwrap()).unwrap();
    assert!(decoded.unchanged_cols.contains("toast"));
    assert!(!decoded.after.as_ref().unwrap().contains_key("toast"));
    assert_eq!(decoded.before.unwrap()["v"].bytes, None);
    assert_eq!(decoded.after.unwrap()["v"].bytes, Some(vec![]));
    let mut null = event.clone();
    null.after.as_mut().unwrap().get_mut("v").unwrap().bytes = None;
    let mut absent = event.clone();
    absent.after = None;
    assert_ne!(event.digest().unwrap(), null.digest().unwrap());
    assert_ne!(event.digest().unwrap(), absent.digest().unwrap());
}

fn truncate(relations: &[&str], cascade: bool, restart_identity: bool) -> Event {
    Event {
        kind: EventKind::Truncate {
            relations: relations.iter().map(|r| (*r).into()).collect(),
            cascade,
            restart_identity,
        },
        table: None,
        key: Row::new(),
        before: None,
        after: None,
        unchanged_cols: BTreeSet::new(),
        ..sample()
    }
}

#[test]
fn grouped_truncate_and_origin_roundtrip_and_cover_every_new_field_in_digest() {
    let event = truncate(&["public.b", "public.a"], true, true);
    let bytes = event.canonical_bytes().unwrap();
    assert_eq!(Event::from_canonical_bytes(&bytes).unwrap(), event);
    assert_eq!(
        Event::read_canonical(Cursor::new(&bytes), bytes.len() as u64).unwrap(),
        event
    );
    for changed in [
        truncate(&["public.a", "public.b"], true, true),
        truncate(&["public.b"], true, true),
        truncate(&["public.b", "public.a"], false, true),
        truncate(&["public.b", "public.a"], true, false),
    ] {
        assert_ne!(event.digest().unwrap(), changed.digest().unwrap());
    }
    let make = |origin| {
        AssembledTxn::new_with_origin(
            event.source.clone(),
            event.txn.clone(),
            event.position.clone(),
            Some(17),
            origin,
            vec![event.clone()],
        )
        .unwrap()
    };
    let txn = make(Some(SourceOrigin {
        name: "upstream".into(),
        lsn: u64::MAX,
    }));
    for origin in [
        None,
        Some(SourceOrigin {
            name: "other".into(),
            lsn: u64::MAX,
        }),
        Some(SourceOrigin {
            name: "upstream".into(),
            lsn: 0,
        }),
    ] {
        assert_ne!(txn.digest, make(origin).digest);
    }
    let bytes = txn.canonical_bytes().unwrap();
    assert_eq!(AssembledTxn::from_canonical_bytes(&bytes).unwrap(), txn);
    use skedaddle::buffer::format::{
        Limits, SegmentDecoder, SegmentEncoder, SegmentHeader, SegmentRecord,
    };
    let record = SegmentRecord::Transaction(txn.clone());
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: "truncate-origin".into(),
            stream: "source".into(),
            epoch: 1,
            first_pos: txn.commit_position.clone(),
            last_pos: txn.commit_position.clone(),
            record_count: 1,
        },
        Limits::default(),
    )
    .unwrap();
    encoder.write_record(&record).unwrap();
    let segment = encoder.finish().unwrap().0;
    let mut decoder = SegmentDecoder::new(segment.as_slice(), Limits::default()).unwrap();
    assert_eq!(decoder.next_record().unwrap(), Some(record));
    decoder.finish().unwrap();
    let mut decoder = SegmentDecoder::new(segment.as_slice(), Limits::default()).unwrap();
    let decoded = decoder
        .next_transaction_into(Cursor::new(Vec::new()))
        .unwrap()
        .unwrap();
    assert_eq!(decoded.source_origin(), txn.source_origin.as_ref());
    assert_eq!(decoded.digest(), txn.digest);
    decoder.finish().unwrap();
    let mut stream = StreamingTxn::from_events_with_origin(
        metadata(&txn),
        txn.source_origin.clone(),
        1,
        [Ok(event)],
        ShortStorage(Cursor::new(Vec::new())),
    )
    .unwrap();
    assert_eq!(stream.digest(), txn.digest);
    let mut actual = Vec::new();
    stream
        .canonical_reader()
        .unwrap()
        .read_to_end(&mut actual)
        .unwrap();
    assert_eq!(actual, bytes);
    let decoded = StreamingTxn::from_storage(ShortStorage(Cursor::new(actual))).unwrap();
    assert_eq!(decoded.source_origin(), txn.source_origin.as_ref());
    assert_eq!(decoded.digest(), txn.digest);
    let mut stale = txn;
    stale.source_origin.as_mut().unwrap().lsn = 1;
    assert_eq!(stale.canonical_bytes(), Err(EventError::DigestMismatch));
}

#[test]
fn truncate_rejects_empty_duplicate_or_ambiguous_groups_and_noncanonical_flags() {
    for relations in [&[][..], &[""][..], &["a", "a"][..]] {
        assert!(truncate(relations, false, false).canonical_bytes().is_err());
    }
    for change in 0..5 {
        let mut event = truncate(&["a", "b"], false, false);
        match change {
            0 => event.table = Some("a".into()),
            1 => event.key = sample().key,
            2 => event.before = Some(Row::new()),
            3 => event.after = Some(Row::new()),
            _ => {
                event.unchanged_cols.insert("v".into());
            }
        }
        assert!(event.canonical_bytes().is_err());
    }
    let bytes = truncate(&["a", "b"], false, false)
        .canonical_bytes()
        .unwrap();
    // Fixed sample envelope: event tag at 82, count + two length-prefixed
    // single-byte relation names, then two strictly boolean flag bytes.
    for index in [97, 98] {
        let mut bad = bytes.clone();
        bad[index] = 2;
        reseal(&mut bad);
        assert!(Event::from_canonical_bytes(&bad).is_err());
        assert!(Event::read_canonical(Cursor::new(&bad), bad.len() as u64).is_err());
    }
    let mut duplicate = bytes;
    duplicate[96] = b'a';
    reseal(&mut duplicate);
    assert!(Event::from_canonical_bytes(&duplicate).is_err());
}

#[test]
fn v1_records_are_explicitly_rejected_in_owned_streaming_and_buffer_paths() {
    let event = unhex(GOLDEN_V1);
    assert_eq!(
        Event::from_canonical_bytes(&event),
        Err(EventError::UnsupportedVersion(1))
    );
    assert_eq!(
        Event::read_canonical(Cursor::new(&event), event.len() as u64),
        Err(EventError::UnsupportedVersion(1))
    );
    let mut txn = assembled(vec![]).canonical_bytes().unwrap();
    // v1's empty transaction has no source-origin option before the count.
    txn.remove(txn.len() - 37);
    txn[4..6].copy_from_slice(&1_u16.to_be_bytes());
    reseal(&mut txn);
    assert_eq!(
        AssembledTxn::from_canonical_bytes(&txn),
        Err(EventError::UnsupportedVersion(1))
    );
    assert!(matches!(
        StreamingTxn::from_storage(Cursor::new(&txn)),
        Err(EventError::UnsupportedVersion(1))
    ));
    use skedaddle::buffer::format::{FormatError, SegmentRecord};
    assert!(matches!(
        SegmentRecord::from_canonical_bytes(&txn),
        Err(FormatError::Event(EventError::UnsupportedVersion(1)))
    ));
}

#[test]
fn v2_records_are_explicitly_rejected_in_owned_streaming_and_buffer_paths() {
    // Captured v2 event and transaction bytes, including their original digests.
    let event = unhex(GOLDEN_V2);
    assert_eq!(
        Event::from_canonical_bytes(&event),
        Err(EventError::UnsupportedVersion(2))
    );
    assert_eq!(
        Event::read_canonical(Cursor::new(&event), event.len() as u64),
        Err(EventError::UnsupportedVersion(2))
    );
    let txn = unhex(GOLDEN_TXN_V2);
    assert_eq!(
        AssembledTxn::from_canonical_bytes(&txn),
        Err(EventError::UnsupportedVersion(2))
    );
    assert!(matches!(
        StreamingTxn::from_storage(Cursor::new(&txn)),
        Err(EventError::UnsupportedVersion(2))
    ));
    use skedaddle::buffer::format::{FormatError, SegmentRecord};
    assert!(matches!(
        SegmentRecord::from_canonical_bytes(&txn),
        Err(FormatError::Event(EventError::UnsupportedVersion(2)))
    ));
}

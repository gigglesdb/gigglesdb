use std::{cmp::Ordering, collections::BTreeSet};

use sha2::{Digest, Sha256};
use skedaddle::event::{
    AssembledTxn, Event, EventError, EventKind, OpaqueValue, Position, PositionOrder, Redelivery,
    RedeliveryCheck, Row, SourceIdentity, TxnId,
};

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
            slot: "slot".into(),
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
const GOLDEN: &str = concat!(
    "534b45560001010000000270670000000173000000016c00000002646200000004736c6f74",
    "00000002010200000000000000070000000e70672f732f6c2f64622f736c6f740000000109",
    "00000000000000030101000000087075626c69632e740000000100000002696400000007",
    "70672f696e743401000000023432010000000100000001760000000770672f7465787400",
    "010000000100000001760000000770672f7465787401000000000000000100000005746f617374",
    "7c216845296d9d1350679a590b28fae78f412e4b27e5b49ca9ea034967aaaaad",
);

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
        EventKind::Truncate,
        EventKind::DdlBoundary,
        EventKind::Watermark,
        EventKind::FinalMarker,
        EventKind::ChunkRows,
    ] {
        let mut event = sample();
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
        unhex("7c216845296d9d1350679a590b28fae78f412e4b27e5b49ca9ea034967aaaaad")
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
            0 => changed.source.slot.push('x'),
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

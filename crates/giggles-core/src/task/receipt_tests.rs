use super::*;
use crate::relational::SqlRow;

const T: TaskObjectId = TaskObjectId(0x0102_0304_0506_0708);
const TABLE: u64 = 0x10;
const SEQ: u64 = 3;
const SALT: [u8; 16] = [0x5A; 16];

fn physical(table: u64, id: i64) -> Vec<u8> {
    let mut key = table.to_be_bytes().to_vec();
    key.extend(id.to_be_bytes());
    key
}

fn row(id: i64, status: &str, version: u64) -> SqlRow {
    SqlRow { values: vec![Value::Int(id), Value::Text(status.into())], version }
}

fn sha(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Rows 2 (open -> closed, versions 4 -> 9) and 1 (no-op, 6 -> 9), recorded
/// out of key order.
fn two_rows() -> Effects {
    let mut e = Effects::default();
    e.record(TABLE, &physical(TABLE, 2), Some(&row(2, "open", 4)), Some(&row(2, "closed", 9))).unwrap();
    e.record(TABLE, &physical(TABLE, 1), Some(&row(1, "held", 6)), Some(&row(1, "held", 9))).unwrap();
    e
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn digest_golden_two_rows() {
    let (header, rows, digest) = build_change(T, SEQ, TABLE, &two_rows(), SALT, 1_000).unwrap();
    assert_eq!(rows.len(), 2);

    // Hand-built preimage.
    let before1 = payload(&row(1, "held", 6).values).unwrap();
    let before2 = payload(&row(2, "open", 4).values).unwrap();
    let after2 = payload(&row(2, "closed", 9).values).unwrap();
    let salted = |p: &[u8]| {
        let mut v = SALT.to_vec();
        v.extend_from_slice(p);
        sha(&v)
    };
    let mut expected = b"GDTASKCHG\x01".to_vec();
    expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    expected.extend_from_slice(&3u64.to_be_bytes());
    expected.extend_from_slice(&0x10u64.to_be_bytes());
    expected.extend_from_slice(&[0, 0, 0, 2]);
    // Row 1: no-op, after hash equals before hash.
    expected.extend_from_slice(&[0, 8]);
    expected.extend_from_slice(&1i64.to_be_bytes());
    expected.extend_from_slice(&6u64.to_be_bytes());
    expected.extend_from_slice(&9u64.to_be_bytes());
    expected.push(1);
    expected.extend_from_slice(&salted(&before1));
    expected.extend_from_slice(&salted(&before1));
    // Row 2.
    expected.extend_from_slice(&[0, 8]);
    expected.extend_from_slice(&2i64.to_be_bytes());
    expected.extend_from_slice(&4u64.to_be_bytes());
    expected.extend_from_slice(&9u64.to_be_bytes());
    expected.push(0);
    expected.extend_from_slice(&salted(&before2));
    expected.extend_from_slice(&salted(&after2));
    assert_eq!(expected.len(), 38 + 2 * (2 + 8 + 8 + 8 + 1 + 64));
    assert_eq!(digest_preimage(T, SEQ, TABLE, &rows), expected);
    assert_eq!(digest, sha(&expected));

    // Frozen value: a change to the row payload encoding or the preimage breaks this.
    assert_eq!(hex(&digest), GOLDEN);

    assert_eq!(header.effects_digest, digest);
    assert_eq!((header.flags, header.kind, header.table, header.row_count), (0, 1, TABLE, 2));
    assert_eq!((header.salt, header.decided_ms, header.outcome_key), (SALT, 1_000, [0; 32]));
    assert_eq!(rows[1].before, before2);
    // Records stay valid through their codecs.
    assert_eq!(ChangeHeader::decode(&header.encode()).unwrap(), header);
    for r in &rows {
        assert_eq!(&ChangeRow::decode(&r.encode()).unwrap(), r);
    }
}

const GOLDEN: &str = "598397b0d057dcd67d63943d3c749ac9f66148d4208b8cfd843629e198e150be";

#[test]
fn row_order_is_physical_key_order() {
    let mut e = Effects::default();
    for id in [300i64, -5, 7, 0x0100, 2] {
        e.record(TABLE, &physical(TABLE, id), Some(&row(id, "a", 1)), Some(&row(id, "b", 2))).unwrap();
    }
    let (_, rows, digest) = build_change(T, SEQ, TABLE, &e, SALT, 0).unwrap();
    let keys: Vec<Vec<u8>> = rows.iter().map(|r| r.pk.clone()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert_eq!(keys.len(), 5);

    // Any other order gives a different digest and fails verification.
    let mut reversed = rows.clone();
    reversed.reverse();
    assert_ne!(effects_digest(T, SEQ, TABLE, &reversed), digest);
    let (header, rows, _) = build_change(T, SEQ, TABLE, &e, SALT, 0).unwrap();
    assert!(verify_digest(T, SEQ, &header, &rows));
    assert!(!verify_digest(T, SEQ, &header, &reversed));
}

#[test]
fn digest_verifies_after_blanking() {
    let (header, rows, _) = build_change(T, SEQ, TABLE, &two_rows(), SALT, 0).unwrap();
    assert!(verify_digest(T, SEQ, &header, &rows));

    let mut blanked_header = header.clone();
    blanked_header.flags |= super::super::records::CHANGE_BEFORE_BLANKED;
    let blanked: Vec<ChangeRow> = rows
        .iter()
        .map(|r| ChangeRow { flags: r.flags | ROW_BEFORE_BLANKED, before: Vec::new(), ..r.clone() })
        .collect();
    for r in &blanked {
        assert_eq!(&ChangeRow::decode(&r.encode()).unwrap(), r);
    }
    assert!(verify_digest(T, SEQ, &blanked_header, &blanked));

    // Tampering is still caught: a hash, a version, a dropped row, a wrong seq or task.
    let mut bad = blanked.clone();
    bad[0].after_hash[0] ^= 1;
    assert!(!verify_digest(T, SEQ, &blanked_header, &bad));
    let mut bad = blanked.clone();
    bad[1].version_written += 1;
    assert!(!verify_digest(T, SEQ, &blanked_header, &bad));
    assert!(!verify_digest(T, SEQ, &blanked_header, &blanked[..1]));
    assert!(!verify_digest(T, SEQ + 1, &blanked_header, &blanked));
    assert!(!verify_digest(TaskObjectId(9), SEQ, &blanked_header, &blanked));

    // A kept before-value that does not match its hash fails.
    let mut bad = rows.clone();
    bad[1].before = payload(&row(2, "forged", 4).values).unwrap();
    assert!(!verify_digest(T, SEQ, &header, &bad));
}

#[test]
fn salt_changes_hashes() {
    let (h1, r1, d1) = build_change(T, SEQ, TABLE, &two_rows(), SALT, 0).unwrap();
    let (h2, r2, d2) = build_change(T, SEQ, TABLE, &two_rows(), [0xA5; 16], 0).unwrap();
    assert_ne!(d1, d2);
    for (a, b) in r1.iter().zip(&r2) {
        assert_ne!(a.before_hash, b.before_hash);
        assert_ne!(a.after_hash, b.after_hash);
        assert_eq!(a.before, b.before);
    }
    // Each header verifies only its own rows.
    assert!(verify_digest(T, SEQ, &h1, &r1));
    assert!(verify_digest(T, SEQ, &h2, &r2));
    assert!(!verify_digest(T, SEQ, &h1, &r2));
    // The hash is sha256(salt ‖ payload).
    assert_eq!(r1[1].after_hash, row_hash(&SALT, &payload(&row(2, "closed", 9).values).unwrap()));
}

#[test]
fn noop_flag_is_in_digest() {
    let (_, rows, digest) = build_change(T, SEQ, TABLE, &two_rows(), SALT, 0).unwrap();
    assert_eq!(rows[0].flags, ROW_NOOP);
    assert_eq!(rows[1].flags, 0);
    let mut flipped = rows.clone();
    flipped[0].flags = 0;
    assert_ne!(effects_digest(T, SEQ, TABLE, &flipped), digest);
    // Bit 1 (before blanked) is not part of the digest.
    let mut blanked = rows.clone();
    blanked[0].flags |= ROW_BEFORE_BLANKED;
    assert_eq!(effects_digest(T, SEQ, TABLE, &blanked), digest);
}

#[test]
fn minimal_view_has_no_roles_or_before_values() {
    let (header, rows, digest) = build_change(T, SEQ, TABLE, &two_rows(), SALT, 1_000).unwrap();
    let record = ReceiptRecord {
        decision: DECISION_COMMITTED,
        request_kind: 1,
        flags: 0,
        task: T,
        seq: SEQ,
        def_version: 2,
        catalog_version: 11,
        decided_ms: 1_000,
        request_fp: [0x22; 32],
        effects_digest: digest,
        rows_affected: 2,
        distinct_charged: 1,
        transitions_charged: 1,
        noop_rows: 1,
        distinct_after: 5,
        transitions_after: 8,
        preview: 0,
        undo_mode: 0,
        restored: 0,
        conflicts: 0,
        superseded: 0,
        op_id: b"op-7".to_vec(),
        issuer: "ops_lead".into(),
        executor: "agent_inc436".into(),
        approver: Some("dba_on_call".into()),
    };
    let full = Receipt::from_record(&record, Some(ChangeRecord { header, rows: rows.clone() }));
    assert!(!full.is_minimal());
    assert_eq!(full.issuer(), Some("ops_lead"));
    assert_eq!(full.executor(), Some("agent_inc436"));
    assert_eq!(full.approver(), Some("dba_on_call"));
    assert_eq!(full.record(), Some(&record));
    assert_eq!(full.change().unwrap().rows[1].before, rows[1].before);

    let min = full.minimal();
    assert!(min.is_minimal());
    assert_eq!((min.issuer(), min.executor(), min.approver()), (None, None, None));
    assert!(min.record().is_none());
    assert!(min.change().is_none());
    assert_eq!(min.operation_id, b"op-7");
    assert_eq!((min.decision, min.seq, min.rows_affected), (DECISION_COMMITTED, SEQ, 2));
    assert_eq!((min.distinct_charged, min.transitions_charged, min.noop_rows), (1, 1, 1));
    assert_eq!((min.effects_digest, min.request_fp, min.decided_ms), (digest, [0x22; 32], 1_000));
    // The minimal view's debug text carries no role names or row payloads.
    let text = format!("{min:?}");
    assert!(!text.contains("ops_lead") && !text.contains("agent_inc436") && !text.contains("dba_on_call"));
}

#[test]
fn identity_change_and_other_table_are_denied() {
    let mut e = Effects::default();
    e.record(TABLE, &physical(TABLE, 1), None, Some(&row(1, "new", 1))).unwrap();
    assert!(matches!(
        build_change(T, SEQ, TABLE, &e, SALT, 0),
        Err(TaskError::Denied(DenyRule::PrimaryKeyChange))
    ));
    assert!(matches!(
        build_change(T, SEQ, TABLE + 1, &two_rows(), SALT, 0),
        Err(TaskError::Denied(DenyRule::OtherTable))
    ));
    // Zero-row operations still get a header and a digest.
    let (header, rows, digest) = build_change(T, SEQ, TABLE, &Effects::default(), SALT, 0).unwrap();
    assert!(rows.is_empty());
    assert_eq!(header.row_count, 0);
    assert!(verify_digest(T, SEQ, &header, &rows));
    assert_eq!(digest, header.effects_digest);
}

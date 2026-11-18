use super::*;

const T: TaskObjectId = TaskObjectId(0x0102_0304_0506_0708);

fn op(bytes: &[u8]) -> OperationId {
    OperationId::new(bytes.to_vec()).unwrap()
}

fn sha(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[test]
fn operation_id_bounds() {
    assert!(OperationId::new(Vec::new()).is_none());
    assert!(OperationId::new(vec![7; 1]).is_some());
    assert!(OperationId::new(vec![7; 128]).is_some());
    assert!(OperationId::new(vec![7; 129]).is_none());
}

#[test]
fn identity_golden() {
    assert_eq!(
        task_identity(T, &op(b"ab")),
        vec![0x02, 1, 2, 3, 4, 5, 6, 7, 8, 0x00, 0x02, b'a', b'b']
    );
    let long = task_identity(TaskObjectId(1), &op(&[0xEE; 128]));
    assert_eq!(&long[..11], &[0x02, 0, 0, 0, 0, 0, 0, 0, 1, 0x00, 0x80]);
    assert_eq!(long.len(), 11 + 128);
}

#[test]
fn fingerprint_golden_direct_apply_undo() {
    let mut prefix = b"GDTASKREQ\x01".to_vec();
    prefix.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);

    // Direct: text B32, param_count u16, each param B32.
    let direct = RequestBody::Direct { intent: None, text: b"UPDATE t SET v = $1",
        params: vec![vec![0xAA, 0xBB], Vec::new()],
    };
    let mut expected = prefix.clone();
    expected.push(1);
    expected.extend_from_slice(&[0, 0, 0, 19]);
    expected.extend_from_slice(b"UPDATE t SET v = $1");
    expected.extend_from_slice(&[0, 2]);
    expected.extend_from_slice(&[0, 0, 0, 2, 0xAA, 0xBB]);
    expected.extend_from_slice(&[0, 0, 0, 0]);
    assert_eq!(fingerprint_preimage(T, &direct), expected);
    assert_eq!(request_fingerprint(T, &direct), sha(&expected));

    // Apply: preview u64, digest [32].
    let apply = RequestBody::Apply { preview: 0x0A0B, digest: [0x5C; 32] };
    let mut expected = prefix.clone();
    expected.push(2);
    expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0x0A, 0x0B]);
    expected.extend_from_slice(&[0x5C; 32]);
    assert_eq!(fingerprint_preimage(T, &apply), expected);
    assert_eq!(expected.len(), 10 + 8 + 1 + 8 + 32);
    assert_eq!(request_fingerprint(T, &apply), sha(&expected));

    // Undo: preview u64, digest [32], mode u8.
    let undo = RequestBody::Undo { preview: 0x0A0B, digest: [0x5C; 32], mode: 3 };
    let mut expected_undo = expected.clone();
    expected_undo[18] = 3;
    expected_undo.push(3);
    assert_eq!(fingerprint_preimage(T, &undo), expected_undo);
    assert_eq!(request_fingerprint(T, &undo), sha(&expected_undo));
}

#[test]
fn direct_and_apply_differ_for_same_rows() {
    // Same task, and an apply whose body bytes are a prefix-compatible
    // rendering of the direct body: only the kind byte separates them.
    let apply = RequestBody::Apply { preview: 1, digest: [0; 32] };
    let undo = RequestBody::Undo { preview: 1, digest: [0; 32], mode: 0 };
    let direct = RequestBody::Direct { intent: None, text: b"UPDATE t SET v = 1 WHERE id = 1", params: Vec::new() };
    let fps = [
        request_fingerprint(T, &direct),
        request_fingerprint(T, &apply),
        request_fingerprint(T, &undo),
    ];
    assert_ne!(fps[0], fps[1]);
    assert_ne!(fps[1], fps[2]);
    assert_ne!(fps[0], fps[2]);
    // The task is hashed too.
    assert_ne!(request_fingerprint(TaskObjectId(9), &apply), fps[1]);
}

#[test]
fn param_change_changes_fingerprint() {
    let text: &[u8] = b"UPDATE t SET v = $1 WHERE id = $2";
    let base = RequestBody::Direct { intent: None, text, params: vec![vec![1], vec![2]] };
    let changed = RequestBody::Direct { intent: None, text, params: vec![vec![1], vec![3]] };
    let fewer = RequestBody::Direct { intent: None, text, params: vec![vec![1]] };
    // Moving a byte across the param boundary must not collide.
    let shifted = RequestBody::Direct { intent: None, text, params: vec![vec![1, 2], Vec::new()] };
    let base_fp = request_fingerprint(T, &base);
    assert_eq!(base_fp, request_fingerprint(T, &base.clone()));
    for other in [&changed, &fewer, &shifted] {
        assert_ne!(base_fp, request_fingerprint(T, other));
    }
    let other_text = RequestBody::Direct { intent: None, text: b"UPDATE t SET v = $1 WHERE id = $2 ",
        params: vec![vec![1], vec![2]],
    };
    assert_ne!(base_fp, request_fingerprint(T, &other_text));
}

#[cfg(feature = "engine-rocksdb")]
mod engine {
    use super::*;
    use crate::task::records::OutcomePayload;

    fn directory() -> std::path::PathBuf {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        std::env::temp_dir().join(format!("giggles-ledger-{:032x}", u128::from_be_bytes(nonce)))
    }

    fn record(task: TaskObjectId, op_id: &OperationId, seq: u64) -> OutcomeRecord {
        OutcomeRecord {
            flags: 0,
            identity: task_identity(task, op_id),
            request_fp: [0x11; 32],
            accepted_fp: [0x11; 32],
            decided_lower_ms: 1_000,
            decided_upper_ms: 1_000,
            retain_until_ms: 2_000,
            payload: OutcomePayload::Committed { task, seq },
        }
    }

    fn commit(db: &V2Db, key: Vec<u8>, record: &OutcomeRecord) {
        let mut txn = db.begin(Isolation::Serializable);
        stage(&mut txn, key, record).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn lookup_absent_is_none() {
        let path = directory();
        let db = V2Db::create(&path).unwrap();
        let key = task_outcome_key(&db, T, &op(b"op-1"));
        assert_eq!(lookup(&db, &key).unwrap(), None);
    }

    #[test]
    fn lookup_sees_committed_record() {
        let path = directory();
        let db = V2Db::create(&path).unwrap();
        let id = op(b"op-1");
        let key = task_outcome_key(&db, T, &id);
        let stored = record(T, &id, 4);

        // Staged but uncommitted: not visible.
        let mut txn = db.begin(Isolation::Serializable);
        stage(&mut txn, key.clone(), &stored).unwrap();
        assert_eq!(lookup(&db, &key).unwrap(), None);
        txn.commit().unwrap();

        assert_eq!(lookup(&db, &key).unwrap(), Some(stored));
    }

    #[test]
    fn lookup_never_writes() {
        let path = directory();
        let db = V2Db::create(&path).unwrap();
        let id = op(b"op-1");
        let key = task_outcome_key(&db, T, &id);
        commit(&db, key.clone(), &record(T, &id, 1));

        let before = db.sequencer.visible_ts();
        for _ in 0..3 {
            assert!(lookup(&db, &key).unwrap().is_some());
            let absent = task_outcome_key(&db, T, &op(b"missing"));
            assert!(lookup(&db, &absent).unwrap().is_none());
        }
        assert_eq!(db.sequencer.visible_ts(), before);
    }

    #[test]
    fn same_op_on_two_tasks_is_two_keys() {
        let path = directory();
        let db = V2Db::create(&path).unwrap();
        let id = op(b"shared-op");
        let other = TaskObjectId(T.0 + 1);
        let key_a = task_outcome_key(&db, T, &id);
        let key_b = task_outcome_key(&db, other, &id);
        assert_ne!(key_a, key_b);

        let rec_a = record(T, &id, 1);
        commit(&db, key_a.clone(), &rec_a);
        assert_eq!(lookup(&db, &key_b).unwrap(), None);

        let rec_b = record(other, &id, 7);
        commit(&db, key_b.clone(), &rec_b);
        assert_eq!(lookup(&db, &key_a).unwrap(), Some(rec_a));
        assert_eq!(lookup(&db, &key_b).unwrap(), Some(rec_b));
    }

    #[test]
    fn stage_rejects_non_outcome_key() {
        let path = directory();
        let db = V2Db::create(&path).unwrap();
        let id = op(b"op-1");
        let mut txn = db.begin(Isolation::Serializable);
        let err = stage(&mut txn, vec![0x23; 17], &record(T, &id, 1)).unwrap_err();
        assert!(matches!(err, CoreError::Corrupt(_)));
    }
}

#[test]
fn direct_intent_suffix_golden() {
    let body = |intent| RequestBody::Direct {
        text: b"UPDATE t SET v = $1",
        params: vec![vec![0xAA, 0xBB], Vec::new()],
        intent,
    };
    let base = body(None);
    let bytes = fingerprint_preimage(T, &base);
    let one = body(Some(RowIntent::One));
    let capped = body(Some(RowIntent::AtMost(0x0102)));
    assert_eq!(
        fingerprint_preimage(T, &one),
        [bytes.as_slice(), &[1]].concat()
    );
    assert_eq!(
        fingerprint_preimage(T, &capped),
        [bytes.as_slice(), &[2, 0, 0, 0, 0, 0, 0, 1, 2]].concat()
    );
    let fps = [&base, &one, &capped].map(|b| request_fingerprint(T, b));
    assert_ne!(fps[0], fps[1]);
    assert_ne!(fps[0], fps[2]);
    assert_ne!(fps[1], fps[2]);
    assert_ne!(
        request_fingerprint(T, &body(Some(RowIntent::AtMost(3)))),
        request_fingerprint(T, &body(Some(RowIntent::AtMost(4))))
    );
}

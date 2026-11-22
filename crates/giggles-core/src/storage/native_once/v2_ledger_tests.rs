//! GD-0061 R3-625: native `once` on the shared ledger. Publication writes a
//! kind-1 `22` record and counts it in `26 00`; admission and lookup read the
//! `22` records; compaction sets the payload-blanked flag; the first
//! publication on a V2ONCE store migrates `[07]` in its own commit.
use super::*;
use crate::operations::{Bindings, Engine as NativeEngine, Request, ValueExpr, once::*};
use crate::storage::engine::{
    kv::{Direction, Keyspace},
    meta,
    txn::{Isolation, Transaction},
};
use crate::task::records::{
    self, LEDGER_HEADER_KEY, LedgerHeader, NS_OUTCOME, OUTCOME_PAYLOAD_BLANKED, OutcomePayload,
    OutcomeRecord, TaskObjectId,
};
use std::ops::Bound;

struct Dir(std::path::PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn fixture() -> (Dir, Admin) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = crate::test_artifact_root();
    fs::create_dir_all(&root).unwrap();
    let dir = Dir(root.join(format!("r3-625-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), crate::Engine::V2).unwrap();
    admin.sql_session()
        .execute("CREATE TABLE orders(id BIGINT PRIMARY KEY, note TEXT)", &[], None).unwrap();
    (dir, admin)
}
fn db(admin: &Admin) -> crate::storage::EngineLease {
    admin.test_v2()
}
fn domain(admin: &Admin) -> u128 {
    u128::from_be_bytes(db(admin).meta.domain)
}
fn at(lower: u64, upper: u64) -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: lower, upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
fn lease(lower: u64, upper: u64) -> NativeResult<NativeClockLease> {
    Ok(NativeClockLease { lower_unix_seconds: lower, upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
/// An order insert keyed by the client's order reference.
fn order(admin: &Admin, reference: &str, id: i64, retention: u64) -> OnceRequest {
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        retention_seconds: retention,
        identity: OnceIdentity { database_domain: domain(admin), tenant: 42,
            namespace: "checkout".into(), key: reference.as_bytes().to_vec() },
        body: OnceBody::Operation(Request::Insert { table: "orders".into(), values: vec![
            ("id".into(), ValueExpr::Literal(relational::Value::Int(id))),
            ("note".into(), ValueExpr::Literal(relational::Value::Text(format!("order {reference}")))),
        ] }),
    }
}
fn rows(admin: &Admin) -> usize {
    admin.sql_session().execute("SELECT id FROM orders", &[], None).unwrap().rows.len()
}
fn header(admin: &Admin) -> Option<LedgerHeader> {
    db(admin).begin(Isolation::RepeatableRead).get(Keyspace::Kernel, &LEDGER_HEADER_KEY).unwrap()
        .map(|value| LedgerHeader::decode(&value).unwrap())
}
fn features(admin: &Admin) -> u64 {
    meta::validate_meta(db(admin).kv.as_ref()).unwrap().features
}
/// Every `22` record, key and decoded value.
fn outcomes(txn: &mut Transaction) -> Vec<(Vec<u8>, OutcomeRecord)> {
    txn.scan(Keyspace::Kernel, Bound::Included(&[NS_OUTCOME][..]), Bound::Excluded(&[NS_OUTCOME + 1][..]),
        Direction::Forward, None).unwrap()
        .map(|item| { let (key, value) = item.unwrap(); (key, OutcomeRecord::decode(&value).unwrap()) })
        .collect()
}
fn latest_outcomes(admin: &Admin) -> Vec<(Vec<u8>, OutcomeRecord)> {
    outcomes(&mut db(admin).begin(Isolation::RepeatableRead))
}
fn empty_v1_ledger(admin: &Admin) -> bool {
    let bytes = db(admin).begin(Isolation::RepeatableRead).get(Keyspace::Kernel, &[7]).unwrap().unwrap();
    NativeOutcomes::from_bytes(&bytes).unwrap().is_empty()
}

#[test]
fn replay_after_reopen_from_22_record() {
    let (dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    let request = order(&admin, "A-1001", 1, 600);
    let first = engine.execute_once(&request, &Bindings::new(), &|| at(100, 110)).unwrap();
    assert!(!first.replayed);
    assert_eq!(first.retry_until_seconds, 710);
    // One kind-1 `22` record under this domain; `[07]` stays the empty v1 ledger.
    let stored = latest_outcomes(&admin);
    assert_eq!(stored.len(), 1);
    let (key, record) = &stored[0];
    assert_eq!(record.payload.kind(), 1);
    assert_eq!(record.flags, 0);
    assert_eq!(record.identity[0], 1, "native once identity tag");
    assert_eq!(*key, records::outcome_key(domain(&admin).to_be_bytes(), &record.identity));
    assert_eq!((record.decided_lower_ms, record.decided_upper_ms, record.retain_until_ms), (100_000, 110_000, 710_000));
    assert_eq!(record.accepted_fp, first.accepted_fingerprint);
    assert!(empty_v1_ledger(&admin));
    assert_eq!(header(&admin), Some(LedgerHeader { once_upper_watermark_s: 110, once_identities: 1, migrated_v1: false }));
    assert_ne!(features(&admin) & meta::PROTECTED_RECORDS, 0);
    assert_ne!(features(&admin) & meta::NATIVE_OUTCOMES, 0);
    drop(engine);
    drop(admin);

    // Reopen runs the open checks; the retry replays from the `22` record.
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let engine = NativeEngine::new(admin.clone());
    let visible = db(&admin).sequencer.visible_ts();
    let mut expected = first;
    expected.replayed = true;
    assert_eq!(engine.execute_once(&request, &Bindings::new(), &|| panic!("replay needs no clock")), Ok(expected.clone()));
    assert_eq!(engine.outcome(&request.identity), Ok(OnceStatus::Retained(expected)));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible, "replay writes nothing");
    assert_eq!(rows(&admin), 1);
}

#[test]
fn conflicting_intent_still_conflicts() {
    let (dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    let original = order(&admin, "A-2002", 2, 600);
    engine.execute_once(&original, &Bindings::new(), &|| at(100, 110)).unwrap();
    // Same reference, different order body: a different submission.
    let changed = order(&admin, "A-2002", 3, 600);
    assert_eq!(engine.execute_once(&changed, &Bindings::new(), &|| at(100, 110)), Err(OnceError::IdempotencyConflict));
    drop(engine);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let engine = NativeEngine::new(admin.clone());
    assert_eq!(engine.execute_once(&changed, &Bindings::new(), &|| at(100, 110)), Err(OnceError::IdempotencyConflict));
    assert!(engine.execute_once(&original, &Bindings::new(), &|| at(100, 110)).unwrap().replayed);
    assert_eq!(rows(&admin), 1);
    assert_eq!(header(&admin).unwrap().once_identities, 1);
}

#[test]
fn terminal_outcome_recorded() {
    let (dir, admin) = fixture();
    let identity = NativeOnceKey { database_domain: domain(&admin), identity: vec![1, 0, 0, 0, 9] };
    let terminal = super::tests::terminal_payload();
    let NativeOnceAdmission::Owner(owner) = admin.admit_native_once_engine(identity.clone(), [5; 32]).unwrap()
        else { panic!("owner") };
    let mut session = admin.sql_session();
    let setup = session.begin_once_engine(None).unwrap();
    session.execute("INSERT INTO orders VALUES(7, 'discarded')", &[], None).unwrap();
    let (record, replayed) = session.publish_native_once_engine(Some(setup), owner, [6; 32], terminal.clone(), 60,
        NativePublishMode::Terminal, &mut || lease(100, 110)).unwrap();
    assert!(!replayed);
    assert_eq!(record.result.as_ref(), Some(&terminal));
    // The terminal error is the recorded outcome; the body's write is gone.
    assert_eq!(rows(&admin), 0);
    let stored = latest_outcomes(&admin);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].1.payload, OutcomePayload::Once(terminal.clone()));
    assert_eq!((stored[0].1.request_fp, stored[0].1.accepted_fp), ([5; 32], [6; 32]));
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(admin.lookup_native_once_engine(&identity).unwrap(), NativeOnceLookup::Retained(record.clone()));
    assert!(matches!(admin.admit_native_once_engine(identity.clone(), [5; 32]).unwrap(),
        NativeOnceAdmission::Replay(replay) if replay == record));
    assert!(matches!(admin.admit_native_once_engine(identity, [7; 32]), Err(NativeOnceError::IdempotencyConflict)));
}

#[test]
fn compaction_blanks_payload_keeps_identity() {
    let (dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    let short = order(&admin, "A-3003", 3, 10);
    let long = order(&admin, "A-3004", 4, 600);
    engine.execute_once(&short, &Bindings::new(), &|| at(100, 110)).unwrap();
    engine.execute_once(&long, &Bindings::new(), &|| at(100, 110)).unwrap();
    let before = latest_outcomes(&admin);
    // The short window ends at 120: not expired at 120, expired at 121.
    assert_eq!(engine.compact_outcomes(&|| at(120, 125)), Ok(0));
    assert_eq!(engine.compact_outcomes(&|| at(121, 125)), Ok(1));
    let after = latest_outcomes(&admin);
    assert_eq!(after.len(), 2);
    let blanked: Vec<_> = after.iter().filter(|(_, r)| r.flags & OUTCOME_PAYLOAD_BLANKED != 0).collect();
    assert_eq!(blanked.len(), 1);
    let (key, record) = blanked[0];
    let (_, original) = before.iter().find(|(k, _)| k == key).unwrap();
    assert_eq!(record.payload, OutcomePayload::Once(Vec::new()));
    assert_eq!((&record.identity, record.request_fp, record.accepted_fp), (&original.identity, original.request_fp, original.accepted_fp));
    assert_eq!((record.decided_lower_ms, record.decided_upper_ms, record.retain_until_ms),
        (original.decided_lower_ms, original.decided_upper_ms, 120_000));
    // The identity still counts; the watermark moved to the lease's upper bound.
    assert_eq!(header(&admin), Some(LedgerHeader { once_upper_watermark_s: 125, once_identities: 2, migrated_v1: false }));
    assert_eq!(engine.compact_outcomes(&|| at(121, 124)), Err(OnceError::ClockUncertain));
    drop(engine);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let engine = NativeEngine::new(admin.clone());
    assert_eq!(engine.outcome(&short.identity), Ok(OnceStatus::Expired { retry_until_seconds: 120 }));
    assert_eq!(engine.execute_once(&short, &Bindings::new(), &|| panic!("body must not run")),
        Err(OnceError::KeyExpired { retry_until_seconds: 120 }));
    assert!(matches!(engine.outcome(&long.identity), Ok(OnceStatus::Retained(_))));
    assert_eq!(engine.compact_outcomes(&|| at(130, 135)), Ok(0), "blanked records are not counted again");
    assert_eq!(rows(&admin), 2);
}

/// A V2ONCE store with 1,023 identities in `[07]`: the next publication
/// migrates them in its own commit and takes the last slot; the header then
/// refuses further admissions, blanked or not.
#[test]
fn cap_counts_header() {
    let (_dir, admin) = fixture();
    let mut v1 = NativeOutcomes::default();
    for n in 0..MAX_NATIVE_RECORDS - 1 {
        let key = NativeOnceKey { database_domain: domain(&admin), identity: [&[1u8][..], &(n as u32).to_be_bytes()].concat() };
        v1.insert_outcome(key, [1; 32], [2; 32], super::tests::terminal_payload(), 10, lease(100, 110).unwrap()).unwrap();
    }
    let mut txn = db(&admin).begin(Isolation::RepeatableRead);
    let metadata = Metadata { format: 4, domain: domain(&admin), schema: 0, last_time: 0, next_approval: 0, origin: Some(77) };
    txn.put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap()).unwrap();
    txn.put(Keyspace::Kernel, vec![7], v1.to_bytes().unwrap()).unwrap();
    txn.require_features(meta::NATIVE_OUTCOMES);
    txn.require_subversion(meta::NATIVE_SUBVERSION);
    txn.commit().unwrap();
    assert_eq!(header(&admin), None);

    let engine = NativeEngine::new(admin.clone());
    let last = order(&admin, "A-last", 1, 600);
    engine.execute_once(&last, &Bindings::new(), &|| at(200, 210)).unwrap();
    assert_eq!(header(&admin), Some(LedgerHeader {
        once_upper_watermark_s: 210, once_identities: MAX_NATIVE_RECORDS as u64, migrated_v1: true }));
    assert!(empty_v1_ledger(&admin));
    assert_eq!(latest_outcomes(&admin).len(), MAX_NATIVE_RECORDS);
    for (key, entry) in &v1.entries {
        assert_eq!(admin.lookup_native_once_engine(key).unwrap(), NativeOnceLookup::Retained(entry.record.clone()));
    }
    let over = order(&admin, "A-over", 2, 600);
    assert_eq!(engine.execute_once(&over, &Bindings::new(), &|| at(200, 210)), Err(OnceError::RetentionUnavailable));
    // Blanking frees no slot.
    assert_eq!(engine.compact_outcomes(&|| at(300, 310)), Ok(MAX_NATIVE_RECORDS - 1));
    assert_eq!(header(&admin).unwrap().once_identities, MAX_NATIVE_RECORDS as u64);
    assert_eq!(engine.execute_once(&over, &Bindings::new(), &|| at(300, 310)), Err(OnceError::RetentionUnavailable));
    assert!(engine.execute_once(&last, &Bindings::new(), &|| at(300, 310)).unwrap().replayed);
    assert_eq!(rows(&admin), 1);
}

#[test]
fn task_and_once_identities_never_collide() {
    let (_dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    let request = order(&admin, "A-5005", 5, 10);
    engine.execute_once(&request, &Bindings::new(), &|| at(100, 110)).unwrap();
    // A task operation outcome in the same `22` namespace.
    let task = TaskObjectId(5005);
    let op = crate::task::ledger::OperationId::new(b"A-5005".to_vec()).unwrap();
    let task_key = crate::task::ledger::task_outcome_key(&db(&admin), task, &op);
    let task_record = OutcomeRecord {
        flags: 0,
        identity: crate::task::ledger::task_identity(task, &op),
        request_fp: [8; 32], accepted_fp: [8; 32],
        decided_lower_ms: 100_000, decided_upper_ms: 100_000, retain_until_ms: 100_000,
        payload: OutcomePayload::Committed { task, seq: 1 },
    };
    let mut txn = db(&admin).begin(Isolation::RepeatableRead);
    crate::task::ledger::stage(&mut txn, task_key.clone(), &task_record).unwrap();
    txn.commit().unwrap();

    let stored = latest_outcomes(&admin);
    assert_eq!(stored.len(), 2);
    let (once_key, once_record) = stored.iter().find(|(_, r)| r.payload.kind() == 1).unwrap();
    assert_ne!(*once_key, task_key);
    assert_eq!(once_record.identity[0], 1);
    assert_eq!(task_record.identity[0], crate::task::ledger::TASK_IDENTITY_TAG);
    assert!(matches!(engine.outcome(&request.identity), Ok(OnceStatus::Retained(_))));
    assert_eq!(crate::task::ledger::lookup(&db(&admin), &task_key).unwrap(), Some(task_record.clone()));
    // A native identity carrying a task tag is refused on the shared ledger.
    let forged = NativeOnceKey { database_domain: domain(&admin), identity: task_record.identity.clone() };
    assert!(matches!(admin.admit_native_once_engine(forged.clone(), [1; 32]), Err(NativeOnceError::RetentionUnavailable)));
    assert_eq!(admin.lookup_native_once_engine(&forged), Err(NativeOnceError::RetentionUnavailable));
    // Compaction blanks the once payload only; the task outcome is untouched
    // and is not counted as a once identity.
    assert_eq!(engine.compact_outcomes(&|| at(500, 510)), Ok(1));
    assert_eq!(crate::task::ledger::lookup(&db(&admin), &task_key).unwrap(), Some(task_record));
    assert_eq!(header(&admin).unwrap().once_identities, 1);
    assert_eq!(engine.outcome(&request.identity), Ok(OnceStatus::Expired { retry_until_seconds: 120 }));
}

/// The commit gate asks a `22` once receipt for the bits `[07]` would need
/// (NATIVE_OUTCOMES, and a valid GDOU payload); a blanked payload needs only
/// PROTECTED_RECORDS. The wide bit is covered end to end by
/// `v2_publish_tests::wide_receipt_sets_feature_bit_in_its_commit_and_replays_after_reopen`.
#[test]
fn commit_gate_checks_once_payload_bits() {
    use crate::storage::engine::content_check::required_bits_for_kernel_write;
    let (_dir, admin) = fixture();
    let identity = vec![1, 2, 3];
    let key = records::outcome_key(domain(&admin).to_be_bytes(), &identity);
    let record = |payload: Vec<u8>, flags: u8| OutcomeRecord {
        flags, identity: identity.clone(), request_fp: [1; 32], accepted_fp: [2; 32],
        decided_lower_ms: 0, decided_upper_ms: 0, retain_until_ms: 0, payload: OutcomePayload::Once(payload),
    }.encode();
    let narrow = required_bits_for_kernel_write(&key, Some(&record(super::tests::terminal_payload(), 0)), 0).unwrap();
    assert_eq!(narrow & (meta::PROTECTED_RECORDS | meta::NATIVE_OUTCOMES), meta::PROTECTED_RECORDS | meta::NATIVE_OUTCOMES);
    assert_eq!(narrow & meta::WIDE_ONCE_RECEIPTS, 0);
    let blanked = required_bits_for_kernel_write(&key, Some(&record(Vec::new(), OUTCOME_PAYLOAD_BLANKED)), 0).unwrap();
    assert_eq!(blanked, meta::PROTECTED_RECORDS);
    assert!(required_bits_for_kernel_write(&key, Some(&record(b"not a receipt".to_vec(), 0)), 0).is_err());
}

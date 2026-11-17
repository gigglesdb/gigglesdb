use super::*;
use crate::operations::{Bindings, Engine as NativeEngine, Request, once::*};
use crate::storage::engine::meta;
use crate::task::records::{self, Namespace};

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
    let dir = Dir(root.join(format!("r3-624-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), crate::Engine::V2).unwrap();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    (dir, admin)
}
fn db(admin: &Admin) -> &crate::storage::v2::V2Db {
    let Backend::V2(db) = &admin.inner.backend else { panic!("v2") };
    db
}
fn domain(admin: &Admin) -> u128 {
    u128::from_be_bytes(db(admin).meta.domain)
}
fn at(lower: u64, upper: u64) -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: lower, upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
/// A real `once` insert of row `id`, retained for `retention` seconds.
fn insert_once(admin: &Admin, engine: &NativeEngine, id: i64, retention: u64) -> OnceIdentity {
    let identity = OnceIdentity { database_domain: domain(admin), tenant: 7, namespace: "orders".into(),
        key: format!("order-{id}").into_bytes() };
    let request = OnceRequest { contract_version: ONCE_CONTRACT_VERSION, retention_seconds: retention,
        identity: identity.clone(),
        body: OnceBody::Operation(Request::Insert { table: "items".into(), values: vec![
            ("id".into(), crate::operations::ValueExpr::Literal(relational::Value::Int(id)))] }) };
    engine.execute_once(&request, &Bindings::new(), &|| at(100, 110)).unwrap();
    identity
}
fn kernel_get(admin: &Admin, key: &[u8]) -> Option<Vec<u8>> {
    db(admin).begin(Isolation::RepeatableRead).get(Keyspace::Kernel, key).unwrap()
}
fn ledger(admin: &Admin) -> NativeOutcomes {
    v2::read_v2_ledger(db(admin)).unwrap().expect("ledger")
}
fn features(admin: &Admin) -> u64 {
    db(admin).kv.get(Keyspace::Meta, b"features").unwrap()
        .map_or(0, |value| u64::from_be_bytes(value.try_into().unwrap()))
}
fn seed(admin: &Admin, ledger: &NativeOutcomes) {
    let mut txn = db(admin).begin(Isolation::RepeatableRead);
    let metadata = Metadata { format: 4, domain: domain(admin), schema: 0, last_time: 0, next_approval: 0, origin: Some(77) };
    txn.put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap()).unwrap();
    txn.put(Keyspace::Kernel, vec![7], ledger.to_bytes().unwrap()).unwrap();
    txn.require_features(meta::NATIVE_OUTCOMES);
    txn.require_subversion(meta::NATIVE_SUBVERSION);
    txn.commit().unwrap();
}
/// Every entry of `before` has its `22` record, field for field.
fn assert_moved(admin: &Admin, before: &NativeOutcomes) {
    for (key, entry) in &before.entries {
        let raw_key = records::outcome_key(key.database_domain.to_be_bytes(), &key.identity);
        let value = kernel_get(admin, &raw_key).expect("22 record");
        records::validate(&raw_key, &value).unwrap();
        let record = OutcomeRecord::decode(&value).unwrap();
        assert_eq!(record.identity, key.identity);
        assert_eq!(record.request_fp, entry.record.submitted_fingerprint);
        assert_eq!(record.accepted_fp, entry.record.accepted_fingerprint);
        assert_eq!(record.decided_lower_ms, entry.decided_lower_unix_seconds * 1000);
        assert_eq!(record.decided_upper_ms, entry.decided_upper_unix_seconds * 1000);
        assert_eq!(record.retain_until_ms, entry.record.retry_until_seconds * 1000);
        assert_eq!(record.payload.kind(), 1);
        assert_eq!(record.flags & OUTCOME_PAYLOAD_BLANKED != 0, entry.record.result.is_none());
        assert_eq!(native_record(&record).unwrap(), entry.record);
        assert_eq!(lookup_v2(db(admin), key).unwrap().as_ref(), Some(&entry.record));
    }
}

#[test]
fn all_entries_and_tombstones_move() {
    let (_dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    insert_once(&admin, &engine, 1, 10);
    insert_once(&admin, &engine, 2, 10);
    insert_once(&admin, &engine, 3, 600);
    // Retry window of the two short entries ends at 120; compaction blanks them.
    assert_eq!(engine.compact_outcomes(&|| at(121, 125)), Ok(2));
    let before = ledger(&admin);
    assert_eq!(before.entries.len(), 3);
    assert_eq!(before.entries.values().filter(|e| e.record.result.is_none()).count(), 2);
    let keys: Vec<NativeOnceKey> = before.entries.keys().cloned().collect();
    let pre = keys.iter().map(|k| lookup_v2(db(&admin), k).unwrap()).collect::<Vec<_>>();

    let migration = admin.migrate_native_once_v2().unwrap();
    assert_eq!(migration, Migration::Migrated(LedgerHeader {
        once_upper_watermark_s: 125, once_identities: 3, migrated_v1: true }));
    assert_moved(&admin, &before);
    // Lookups give the same answers from the 22 records as from the blob.
    let post = keys.iter().map(|k| lookup_v2(db(&admin), k).unwrap()).collect::<Vec<_>>();
    assert_eq!(post, pre);
    assert!(post.iter().all(Option::is_some));
    // [07] is the empty v1 ledger, never a tombstone.
    assert_eq!(kernel_get(&admin, &[7]).unwrap(), NativeOutcomes::default().to_bytes().unwrap());
    assert!(ledger(&admin).is_empty());
    assert_ne!(features(&admin) & PROTECTED_RECORDS, 0);
    let absent = NativeOnceKey { database_domain: domain(&admin), identity: vec![1, 2, 3] };
    assert_eq!(lookup_v2(db(&admin), &absent).unwrap(), None);
}

#[test]
fn watermark_and_count_preserved() {
    let (_dir, admin) = fixture();
    let current = domain(&admin);
    let mut v1 = NativeOutcomes::default();
    for n in 0u32..40 {
        // A quarter of the entries come from an earlier database domain.
        let database_domain = if n % 4 == 0 { current ^ 1 } else { current };
        let key = NativeOnceKey { database_domain, identity: n.to_be_bytes().to_vec() };
        let lease = super::super::tests::clock(100 + u64::from(n), 110 + u64::from(n)).unwrap();
        v1.insert_outcome(key, [n as u8; 32], [2; 32], super::super::tests::terminal_payload(),
            10 + u64::from(n), lease).unwrap();
    }
    assert_eq!(v1.compact(super::super::tests::clock(140, 900).unwrap()), 10);
    assert_eq!(v1.upper_watermark_unix_seconds, 900);
    seed(&admin, &v1);
    let header = LedgerHeader { once_upper_watermark_s: 900, once_identities: 40, migrated_v1: true };
    assert_eq!(admin.migrate_native_once_v2().unwrap(), Migration::Migrated(header.clone()));
    let stored = kernel_get(&admin, &LEDGER_HEADER_KEY).unwrap();
    assert_eq!(stored.len(), 18);
    assert_eq!(records::classify_key(&LEDGER_HEADER_KEY).unwrap(), Namespace::LedgerHeader);
    assert_eq!(LedgerHeader::decode(&stored).unwrap(), header);
    assert_moved(&admin, &v1);
    // Historical-domain entries keep their own domain in the key.
    let old = NativeOnceKey { database_domain: current ^ 1, identity: 0u32.to_be_bytes().to_vec() };
    assert!(kernel_get(&admin, &records::outcome_key(current.to_be_bytes(), &old.identity)).is_none());
    assert!(lookup_v2(db(&admin), &old).unwrap().is_some());
}

#[test]
fn crash_before_commit_leaves_blob() {
    let (dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    insert_once(&admin, &engine, 1, 10);
    insert_once(&admin, &engine, 2, 600);
    let blob = kernel_get(&admin, &[7]).unwrap();
    let visible = db(&admin).sequencer.visible_ts();
    admin.set_crash_point(CrashPoint::ErrorBeforeCommit).unwrap();
    assert!(matches!(admin.migrate_native_once_v2(), Err(CoreError::Storage(_))));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
    assert_eq!(kernel_get(&admin, &[7]).unwrap(), blob);
    assert_eq!(kernel_get(&admin, &LEDGER_HEADER_KEY), None);
    let before = NativeOutcomes::from_bytes(&blob).unwrap();
    for key in before.entries.keys() {
        assert_eq!(kernel_get(&admin, &records::outcome_key(key.database_domain.to_be_bytes(), &key.identity)), None);
    }
    assert_eq!(features(&admin) & PROTECTED_RECORDS, 0);
    // Reopened, the store is the unmigrated one, and the next run moves it.
    drop(engine);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(kernel_get(&admin, &[7]).unwrap(), blob);
    assert!(matches!(admin.migrate_native_once_v2().unwrap(), Migration::Migrated(_)));
    assert_moved(&admin, &before);
}

#[test]
fn second_run_is_noop() {
    let (_dir, admin) = fixture();
    // No ledger: nothing to move and nothing written.
    let visible = db(&admin).sequencer.visible_ts();
    assert_eq!(admin.migrate_native_once_v2().unwrap(), Migration::NoLedger);
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
    assert_eq!(kernel_get(&admin, &LEDGER_HEADER_KEY), None);
    let engine = NativeEngine::new(admin.clone());
    insert_once(&admin, &engine, 1, 10);
    let Migration::Migrated(header) = admin.migrate_native_once_v2().unwrap() else { panic!("migrated") };
    let visible = db(&admin).sequencer.visible_ts();
    assert_eq!(admin.migrate_native_once_v2().unwrap(), Migration::AlreadyMigrated(header));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
    assert_eq!(kernel_get(&admin, &[7]).unwrap(), NativeOutcomes::default().to_bytes().unwrap());
}

#[test]
fn open_checks_pass_after_migration() {
    let (dir, admin) = fixture();
    let engine = NativeEngine::new(admin.clone());
    insert_once(&admin, &engine, 1, 10);
    insert_once(&admin, &engine, 2, 600);
    assert_eq!(engine.compact_outcomes(&|| at(121, 125)), Ok(1));
    let before = ledger(&admin);
    assert!(matches!(admin.migrate_native_once_v2().unwrap(), Migration::Migrated(_)));
    drop(engine);
    drop(admin);
    // Open runs the bounded protected scan and the native content check.
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_moved(&admin, &before);
    admin.sql_session().execute("INSERT INTO items VALUES(9)", &[], None).unwrap();
    // Beside 26 00, the commit boundary keeps [07] empty and 26 00 live.
    let mut txn = db(&admin).begin(Isolation::RepeatableRead);
    txn.put(Keyspace::Kernel, vec![7], before.to_bytes().unwrap()).unwrap();
    assert!(txn.commit().is_err());
    let mut txn = db(&admin).begin(Isolation::RepeatableRead);
    txn.delete(Keyspace::Kernel, LEDGER_HEADER_KEY.to_vec()).unwrap();
    txn.require_features(PROTECTED_RECORDS);
    assert!(txn.commit().is_err());
    assert_eq!(kernel_get(&admin, &[7]).unwrap(), NativeOutcomes::default().to_bytes().unwrap());
    drop(admin);
    Admin::open(&dir.0, Limits::default()).unwrap();
}

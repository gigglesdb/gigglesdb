use super::*;
use crate::storage::engine::{gc::{Gc, GcConfig}, kv::Keyspace, txn::Isolation};
use crate::operations::{Bindings, Engine as NativeEngine, Request, once::*};
struct Dir(std::path::PathBuf);
impl Drop for Dir { fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); } }
fn fixture() -> (Dir, Admin) {
    let mut nonce = [0; 16]; getrandom::fill(&mut nonce).unwrap();
    let root = crate::test_artifact_root(); fs::create_dir_all(&root).unwrap();
    let dir = Dir(root.join(format!("t12-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), crate::Engine::V2).unwrap();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    (dir, admin)
}
fn db(admin: &Admin) -> &crate::storage::v2::V2Db {
    let Backend::V2(db) = &admin.inner.backend else { panic!("v2") }; db
}
fn at(lower: u64, upper: u64) -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: lower, upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
fn request(admin: &Admin) -> OnceRequest {
    OnceRequest { contract_version: ONCE_CONTRACT_VERSION, retention_seconds: 10,
        identity: OnceIdentity { database_domain: u128::from_be_bytes(db(admin).meta.domain),
            tenant: 1, namespace: "compact".into(), key: vec![1] },
        body: OnceBody::Operation(Request::Insert { table: "items".into(), values: vec![("id".into(), crate::operations::ValueExpr::Literal(relational::Value::Int(1)))] }) }
}
fn start(admin: &Admin) -> (NativeEngine, OnceRequest) {
    let engine = NativeEngine::new(admin.clone()); let request = request(admin);
    engine.execute_once(&request, &Bindings::new(), &|| at(100, 110)).unwrap();
    (engine, request)
}
#[test]
fn boundary_expiry_reopen_watermark_and_pinned_gc() {
    let (dir, admin) = fixture(); let (engine, request) = start(&admin);
    let mut pinned = db(&admin).begin(Isolation::RepeatableRead);
    let prior = pinned.get(Keyspace::Kernel, &[7]).unwrap().unwrap();
    let visible = db(&admin).sequencer.visible_ts();
    assert_eq!(engine.compact_outcomes(&|| at(120, 125)), Ok(0));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
    assert!(matches!(engine.outcome(&request.identity), Ok(OnceStatus::Retained(_))));
    assert_eq!(engine.compact_outcomes(&|| at(121, 125)), Ok(1));
    assert_eq!(pinned.get(Keyspace::Kernel, &[7]).unwrap(), Some(prior));
    assert_eq!(engine.outcome(&request.identity), Ok(OnceStatus::Expired { retry_until_seconds: 120 }));
    let visible = db(&admin).sequencer.visible_ts();
    assert_eq!(engine.execute_once(&request, &Bindings::new(), &|| panic!("body must not run")),
        Err(OnceError::KeyExpired { retry_until_seconds: 120 }));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
    drop(pinned);
    let gc = Gc::open(db(&admin).kv.clone(), db(&admin).sequencer.clone(), GcConfig { retention: Duration::ZERO }).unwrap();
    gc.advance().unwrap();
    while !gc.step(Keyspace::Kernel, 100).unwrap().wrapped {}
    let mut latest = db(&admin).begin(Isolation::RepeatableRead);
    let bytes = latest.get(Keyspace::Kernel, &[7]).unwrap().unwrap();
    assert!(NativeOutcomes::from_bytes(&bytes).unwrap().entries.values().all(|e| e.record.result.is_none()));
    drop(latest); drop(gc); drop(engine); drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap(); let engine = NativeEngine::new(admin.clone());
    assert_eq!(engine.outcome(&request.identity), Ok(OnceStatus::Expired { retry_until_seconds: 120 }));
    assert_eq!(engine.compact_outcomes(&|| at(121, 124)), Err(OnceError::ClockUncertain));
}
#[test]
fn rejected_clocks_write_nothing_and_unrelated_sql_can_commit() {
    let (_dir, admin) = fixture(); let (engine, request) = start(&admin);
    let visible = db(&admin).sequencer.visible_ts();
    assert_eq!(engine.compact_outcomes(&|| at(100, 109)), Err(OnceError::ClockUncertain));
    assert_eq!(engine.compact_outcomes(&|| Ok(OnceTime { lower_unix_seconds: 121, upper_unix_seconds: 125,
        valid_until: Instant::now() })), Err(OnceError::ClockUncertain));
    let mut overflow = request.clone(); overflow.identity.key = vec![2];
    overflow.body = OnceBody::Operation(Request::Insert { table: "items".into(), values: vec![("id".into(), crate::operations::ValueExpr::Literal(relational::Value::Int(3)))] });
    assert_eq!(engine.execute_once(&overflow, &Bindings::new(), &|| at(u64::MAX, u64::MAX)), Err(OnceError::ClockUncertain));
    assert_eq!(engine.outcome(&overflow.identity), Ok(OnceStatus::Absent));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
    assert_eq!(engine.compact_outcomes(&|| {
        admin.sql_session().execute("INSERT INTO items VALUES(2)", &[], None).unwrap(); at(121, 125)
    }), Ok(1));
    assert_eq!(admin.sql_session().execute("SELECT * FROM items", &[], None).unwrap().rows.len(), 2);
}
#[test]
fn tombstones_keep_all_slots_and_overflow_insertion_writes_nothing() {
    use crate::storage::engine::meta;
    let (_dir, admin) = fixture();
    let mut ledger = NativeOutcomes::default();
    let domain = u128::from_be_bytes(db(&admin).meta.domain);
    let mut overflow = NativeOutcomes::default();
    assert_eq!(overflow.insert_outcome(NativeOnceKey { database_domain: domain, identity: vec![1] },
        [1;32], [2;32], super::super::tests::terminal_payload(), 10,
        NativeClockLease { lower_unix_seconds: u64::MAX, upper_unix_seconds: u64::MAX,
            valid_until: Instant::now() + Duration::from_secs(10) }), Err(NativeOnceError::ClockUncertain));
    assert!(overflow.entries.is_empty());
    for n in 0..MAX_NATIVE_RECORDS {
        ledger.insert_outcome(NativeOnceKey { database_domain: domain, identity: n.to_be_bytes().to_vec() },
            [1;32], [2;32], super::super::tests::terminal_payload(), 10, super::super::tests::clock(100,110).unwrap()).unwrap();
    }
    let mut txn = db(&admin).begin(Isolation::RepeatableRead);
    let metadata = Metadata { format: 4, domain, schema: 0, last_time: 0, next_approval: 0, origin: Some(77) };
    txn.put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap()).unwrap();
    txn.put(Keyspace::Kernel, vec![7], ledger.to_bytes().unwrap()).unwrap();
    txn.require_features(meta::NATIVE_OUTCOMES); txn.require_subversion(meta::NATIVE_SUBVERSION); txn.commit().unwrap();
    assert_eq!(NativeEngine::new(admin.clone()).compact_outcomes(&|| at(121,125)), Ok(MAX_NATIVE_RECORDS));
    assert!(matches!(admin.admit_native_once_engine(NativeOnceKey { database_domain: domain, identity: vec![255] }, [1;32]),
        Err(NativeOnceError::RetentionUnavailable)));
}

#[test]
fn absent_ledger_does_not_call_clock_or_write() {
    let (_dir, admin) = fixture(); let engine = NativeEngine::new(admin.clone());
    let visible = db(&admin).sequencer.visible_ts();
    assert_eq!(engine.compact_outcomes(&|| panic!("absent ledger needs no clock")), Ok(0));
    assert_eq!(db(&admin).sequencer.visible_ts(), visible);
}

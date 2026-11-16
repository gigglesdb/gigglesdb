#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, CrashPoint, Limits, Engine as StorageEngine,
    operations::{Bindings, Engine, Request, ValueExpr, once::{ONCE_CONTRACT_VERSION,
        OnceBody, OnceError, OnceIdentity, OnceRequest, OnceStatus, OnceTime, OnceClockUnavailable}},
    relational::Value as V};
use std::{path::PathBuf, sync::{Arc, Barrier}, time::{Duration, Instant}};
struct Dir(PathBuf);
impl Drop for Dir { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn fixture() -> (Dir, Admin, Engine) {
    let mut nonce = [0; 16]; getrandom::fill(&mut nonce).unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("t10b-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), StorageEngine::V2).unwrap();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY, n BIGINT)", &[], None).unwrap();
    admin.sql_session().execute("CREATE INDEX items_n ON items(n)", &[], None).unwrap();
    let engine = Engine::new(admin.clone()); (dir, admin, engine)
}
fn request(admin: &Admin) -> OnceRequest {
    OnceRequest { contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity { database_domain: admin.authority_metadata().unwrap().0, tenant: 42,
            namespace: "t10b".into(), key: b"one".to_vec() }, retention_seconds: 60,
        body: OnceBody::Operation(Request::Insert { table: "items".into(),
            values: vec![("id".into(), ValueExpr::Literal(V::Int(1))),
                ("n".into(), ValueExpr::Literal(V::Int(10)))] }) }
}
fn clock() -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: 1000, upper_unix_seconds: 1001,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
fn count(admin: &Admin, filter: &str) -> usize {
    admin.sql_session().execute(&format!("SELECT id,n FROM items {filter}"), &[], None).unwrap().rows.len()
}
#[test]
fn success_replay_reopen_and_identity_checks() {
    let (dir, admin, engine) = fixture(); let original = request(&admin);
    let first = engine.execute_once(&original, &Bindings::new(), &clock).unwrap();
    assert!(!first.replayed); assert!(first.outcome.is_ok());
    assert_eq!(count(&admin, "WHERE n=10"), 1);
    let mut expected = first; expected.replayed = true;
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Retained(expected.clone())));
    assert_eq!(engine.execute_once(&original, &Bindings::new(), &clock), Ok(expected.clone()));
    let mut changed = original.clone(); changed.retention_seconds += 1;
    assert_eq!(engine.execute_once(&changed, &Bindings::new(), &clock), Err(OnceError::IdempotencyConflict));
    changed = original.clone(); changed.identity.database_domain ^= 1;
    assert_eq!(engine.execute_once(&changed, &Bindings::new(), &clock), Err(OnceError::DomainMismatch));
    drop(engine); drop(admin);
    let reopened = Admin::open(&dir.0, Limits::default()).unwrap(); let engine = Engine::new(reopened.clone());
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Retained(expected.clone())));
    assert_eq!(engine.execute_once(&original, &Bindings::new(), &clock), Ok(expected));
    assert_eq!(count(&reopened, "WHERE n=10"), 1); assert_eq!(count(&reopened, ""), 1);
}
#[test]
fn concurrent_identical_submissions_execute_once() {
    let (_dir, admin, engine) = fixture(); let original = request(&admin); let barrier = Arc::new(Barrier::new(4));
    let threads: Vec<_> = (0..4).map(|_| {
        let engine = Engine::new(admin.clone()); let request = original.clone(); let barrier = barrier.clone();
        std::thread::spawn(move || { barrier.wait(); engine.execute_once(&request, &Bindings::new(), &clock) })
    }).collect();
    let results: Vec<_> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|result| matches!(result, Ok(receipt) if !receipt.replayed)).count(), 1);
    assert!(results.iter().all(|result| result.is_ok() || *result == Err(OnceError::InProgress)));
    assert_eq!(count(&admin, "WHERE n=10"), 1);
    assert!(matches!(engine.outcome(&original.identity), Ok(OnceStatus::Retained(_))));
}
#[test]
fn expired_lease_consumes_neither_row_nor_identity() {
    let (_dir, admin, engine) = fixture(); let original = request(&admin);
    assert_eq!(engine.execute_once(&original, &Bindings::new(), &|| {
        let mut time = clock()?; time.valid_until = Instant::now(); Ok(time)
    }), Err(OnceError::ClockUncertain));
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent)); assert_eq!(count(&admin, ""), 0);
    engine.execute_once(&original, &Bindings::new(), &clock).unwrap();
}
#[test]
fn lost_ack_fences_until_reopen_and_retains_exactly_one_row() {
    let (dir, admin, engine) = fixture(); let original = request(&admin);
    admin.set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
    assert_eq!(engine.execute_once(&original, &Bindings::new(), &clock), Err(OnceError::Unknown));
    assert_eq!(engine.execute_once(&original, &Bindings::new(), &clock),
        Err(OnceError::Operation(giggles_core::operations::Error::Core(giggles_core::CoreError::Fenced.to_string()))));
    drop(engine); drop(admin);
    let reopened = Admin::open(&dir.0, Limits::default()).unwrap(); let engine = Engine::new(reopened.clone());
    assert!(matches!(engine.outcome(&original.identity), Ok(OnceStatus::Retained(_))));
    assert!(engine.execute_once(&original, &Bindings::new(), &clock).unwrap().replayed);
    assert_eq!(count(&reopened, "WHERE n=10"), 1); assert_eq!(count(&reopened, ""), 1);
}

#[test]
fn transaction_body_commits_and_retains_identity() {
    let (_dir, admin, engine) = fixture(); let mut original = request(&admin);
    let OnceBody::Operation(operation) = original.body else { unreachable!() };
    original.body = OnceBody::Transaction(giggles_core::operations::Transaction { read_only: false,
        steps: vec![giggles_core::operations::TransactionStep::Execute(operation)], emit: None });
    assert!(engine.execute_once(&original, &Bindings::new(), &clock).unwrap().outcome.is_ok());
    assert!(matches!(engine.outcome(&original.identity), Ok(OnceStatus::Retained(_))));
    assert_eq!(count(&admin, ""), 1);
}

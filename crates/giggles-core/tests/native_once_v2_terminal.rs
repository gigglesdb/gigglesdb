#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Cancellation, DecisionPoint, Engine as StorageEngine, Limits,
    operations::{Bindings, Compare, EmitExpr, EmittedValue, Engine, Error, Expr, Intent, LocalRef, Request, Transaction,
        TransactionStep, ValueExpr, once::{ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable,
            OnceIdentity, OnceOutcome, OnceRequest, OnceStatus, OnceTime}}, relational::Value as V};
use std::{path::PathBuf, sync::Arc, time::{Duration, Instant}};
struct Dir(PathBuf);
impl Drop for Dir { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn fixture(storage: StorageEngine) -> (Dir, Admin, Engine) {
    let mut nonce = [0; 16]; getrandom::fill(&mut nonce).unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("t11-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), storage).unwrap();
    for sql in ["CREATE TABLE items(id BIGINT PRIMARY KEY, n BIGINT)",
        "CREATE INDEX items_n ON items(n)", "INSERT INTO items VALUES(1,10),(2,20)"] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    let engine = Engine::new(admin.clone()); (dir, admin, engine)
}
fn clock() -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: 1000, upper_unix_seconds: 1001,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
fn val(v: V) -> Expr { Expr::Value(ValueExpr::Literal(v)) }
fn int(v: i64) -> Expr { val(V::Int(v)) }
fn eq_id(v: i64) -> Expr { Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Eq, Box::new(int(v))) }
fn insert(id: i64) -> Request { Request::Insert { table: "items".into(), values: vec![
    ("id".into(), ValueExpr::Literal(V::Int(id))), ("n".into(), ValueExpr::Literal(V::Int(30)))] } }
fn block(steps: Vec<TransactionStep>, emit: Option<Vec<EmitExpr>>) -> OnceBody {
    OnceBody::Transaction(Transaction { read_only: false, steps, emit })
}
fn request(admin: &Admin, body: OnceBody) -> OnceRequest {
    OnceRequest { contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity { database_domain: admin.authority_metadata().unwrap().0,
            tenant: 42, namespace: "t11".into(), key: b"one".to_vec() }, retention_seconds: 60, body }
}
fn rows(admin: &Admin, filter: &str) -> Vec<giggles_core::relational::SqlRow> {
    admin.sql_session().execute(&format!("SELECT id,n FROM items {filter} ORDER BY id"), &[], None).unwrap().rows
}
#[test]
fn all_terminal_errors_discard_prior_block_writes_and_replay_after_reopen() {
    for expected in [Error::NotFound, Error::AmbiguousMatch, Error::ConditionFailed,
        Error::TooManyMatches, Error::RequirementFailed] {
        for preceding_insert in [false, true] {
        let (dir, admin, engine) = fixture(StorageEngine::V2);
        let before = rows(&admin, ""); let index_before = rows(&admin, "WHERE n>=10");
        let terminal = if expected == Error::RequirementFailed {
            TransactionStep::Require(val(V::Bool(false)))
        } else {
            TransactionStep::Execute(Request::UpdateExpressions { table: "items".into(),
                filter: if expected == Error::NotFound { eq_id(99) }
                    else if expected == Error::ConditionFailed { eq_id(1) } else { val(V::Bool(true)) },
                guard: (expected == Error::ConditionFailed).then(|| val(V::Bool(false))),
                values: vec![("n".into(), int(5))],
                intent: if expected == Error::TooManyMatches { Intent::Many { cap: ValueExpr::Literal(V::Int(1)) } }
                    else { Intent::One } })
        };
        let body = if preceding_insert { block(vec![TransactionStep::Execute(insert(3)), terminal], None) }
            else { match terminal { TransactionStep::Execute(operation) => OnceBody::Operation(operation),
                step => block(vec![step], None) } };
        let original = request(&admin, body);
        let mut receipt = engine.execute_once(&original, &Bindings::new(), &clock).unwrap();
        assert!(!receipt.replayed); assert_eq!(receipt.outcome, Err(expected.clone()));
        assert_eq!(rows(&admin, ""), before); assert_eq!(rows(&admin, "WHERE n>=10"), index_before);
        receipt.replayed = true;
        drop(engine); drop(admin);
        let reopened = Admin::open(&dir.0, Limits::default()).unwrap(); let engine = Engine::new(reopened.clone());
        assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Retained(receipt.clone())));
        assert_eq!(engine.execute_once(&original, &Bindings::new(), &|| panic!("replay clock")), Ok(receipt));
        assert_eq!(rows(&reopened, ""), before); assert_eq!(rows(&reopened, "WHERE n>=10"), index_before);
        }
    }
}
#[test]
fn two_statement_success_matches_legacy_emit_descriptors() {
    let mut results = Vec::new();
    let mut versions = Vec::new();
    for storage in [StorageEngine::Legacy, StorageEngine::V2] {
        let (_dir, admin, engine) = fixture(storage);
        let body = block(vec![TransactionStep::Let { name: "a".into(), request: Request::Returning {
            request: Box::new(insert(3)), projection: vec![] } },
            TransactionStep::Let { name: "b".into(), request: Request::Returning {
                request: Box::new(insert(4)), projection: vec![] } }],
            Some(vec![EmitExpr::Row("b".into()), EmitExpr::Row("a".into()),
                EmitExpr::Scalar(Expr::Local(LocalRef::Changed { local: "a".into() }))]));
        let mut receipt = engine.execute_once(&request(&admin, body), &Bindings::new(), &clock).unwrap();
        let Ok(OnceOutcome::Transaction(outcome)) = &mut receipt.outcome else { panic!("transaction") };
        // Row versions are backend-specific concurrency tokens; compare descriptors and logical values.
        let mut tokens = Vec::new();
        for emitted in &mut outcome.emitted {
            if let EmittedValue::Row { row, .. } = emitted {
                assert_ne!(row.version, 0); tokens.push(row.version); row.version = 0;
            }
        }
        versions.push(tokens);
        assert_eq!(rows(&admin, "WHERE n=30").len(), 2);
        results.push((receipt.description.output, receipt.outcome));
    }
    assert_ne!(versions[0], versions[1]);
    assert_eq!(results[0], results[1]);
}
#[test]
fn bind_and_cancel_failures_consume_no_key_and_retry_succeeds() {
    let (_dir, admin, engine) = fixture(StorageEngine::V2);
    let mut original = request(&admin, OnceBody::Operation(Request::Insert { table: "missing".into(), values: vec![] }));
    assert!(engine.execute_once(&original, &Bindings::new(), &clock).is_err());
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
    original.body = block(vec![TransactionStep::Execute(insert(3)),
        TransactionStep::Require(val(V::Bool(false)))], None);
    let cancel = Cancellation::default(); let contender = cancel.clone();
    admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == DecisionPoint::BeforeDecision { assert!(contender.cancel()); }
    })));
    assert!(cancel.scope(|| engine.execute_once(&original, &Bindings::new(), &clock)).is_err());
    admin.set_decision_hook(None);
    assert_eq!(engine.outcome(&original.identity), Ok(OnceStatus::Absent));
    assert_eq!(rows(&admin, "WHERE n=30").len(), 0);
    assert_eq!(engine.execute_once(&original, &Bindings::new(), &clock).unwrap().outcome, Err(Error::RequirementFailed));
}

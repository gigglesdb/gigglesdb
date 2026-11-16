#![cfg(feature = "engine-rocksdb")]
use giggles_core::{
    Admin, Engine as StorageEngine, Limits,
    operations::{Bindings, Engine, Error, Expr, Request, ValueExpr, once::*},
    relational::{DataType, Value as V},
};
use std::{path::PathBuf, time::{Duration, Instant}};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn fixture() -> (Dir, Admin, Engine) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("t14-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), StorageEngine::V2).unwrap();
    admin.sql_session().execute(
        "CREATE TABLE items(id BIGINT PRIMARY KEY, n NUMERIC, d DATE, ts TIMESTAMP)", &[], None,
    ).unwrap();
    admin.sql_session().execute(
        "INSERT INTO items VALUES(1,1.25,DATE '2000-01-02',TIMESTAMP '2000-01-02 03:04:05.123456')", &[], None,
    ).unwrap();
    let engine = Engine::new(admin.clone());
    (dir, admin, engine)
}
fn at(lower: u64, upper: u64) -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime { lower_unix_seconds: lower, upper_unix_seconds: upper,
        valid_until: Instant::now() + Duration::from_secs(10) })
}
fn request(admin: &Admin, empty: bool) -> OnceRequest {
    let mut values = admin.sql_session().execute("SELECT id,n,d,ts FROM items WHERE id=1", &[], None)
        .unwrap().rows.remove(0).values;
    values[0] = V::Int(2);
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity { database_domain: admin.authority_metadata().unwrap().0,
            tenant: 14, namespace: "t14.replay".into(), key: vec![u8::from(empty)] },
        retention_seconds: if empty { 10 } else { 60 },
        body: OnceBody::Operation(Request::Returning {
            request: Box::new(Request::InsertMany {
                table: "items".into(), columns: ["id", "n", "d", "ts"].map(String::from).to_vec(),
                rows: if empty { vec![] } else { vec![values.into_iter()
                    .map(|value| Expr::Value(ValueExpr::Literal(value))).collect()] },
                cap: ValueExpr::Literal(V::Int(if empty { 0 } else { 1 })),
            }), projection: vec![],
        }),
    }
}
fn with_sql_parser_forbidden<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    { giggles_core::relational::with_sql_parser_forbidden(work) }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    { work() }
}
fn replay(engine: &Engine, request: &OnceRequest, first: &OnceReceipt) {
    let expected = OnceReceipt { replayed: true, ..first.clone() };
    let repeated = with_sql_parser_forbidden(|| engine.execute_once(request, &Bindings::new(),
        &|| panic!("retained receipt must replay without clock or binding"))).unwrap();
    // Equality includes accepting fingerprint, description, deadline, row versions and outcome.
    assert_eq!(repeated, expected);
    assert_eq!(engine.outcome(&request.identity).unwrap(), OnceStatus::Retained(expected));
}

#[test]
fn typed_temporal_and_empty_receipts_reopen_and_ignore_renamed_dropped_recreated_catalog() {
    let (dir, admin, engine) = fixture();
    let requests = [request(&admin, false), request(&admin, true)];
    let receipts: Vec<_> = requests.iter().map(|r| with_sql_parser_forbidden(||
        engine.execute_once(r, &Bindings::new(), &|| at(1000, 1001))).unwrap()).collect();
    for (index, receipt) in receipts.iter().enumerate() {
        let OnceSchema::Operation(columns) = &receipt.description.output else { panic!("operation") };
        assert_eq!(columns.iter().map(|c| c.data_type.clone()).collect::<Vec<_>>(),
            vec![DataType::BigInt, DataType::Numeric, DataType::Date, DataType::Timestamp]);
        let OnceOutcome::Operation(outcome) = receipt.outcome.as_ref().unwrap() else { panic!("operation") };
        assert_eq!(outcome.rows.len(), usize::from(index == 0));
        if index == 0 {
            assert!(matches!(outcome.rows[0].values.as_slice(),
                [V::Int(2), V::Numeric(_), V::Date(_), V::Timestamp(_)]));
        }
    }
    drop(engine); drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    for (request, receipt) in requests.iter().zip(&receipts) { replay(&engine, request, receipt); }
    for ddl in ["ALTER TABLE items RENAME TO renamed", "DROP TABLE renamed",
        "CREATE TABLE items(unrelated TEXT PRIMARY KEY, extra BOOLEAN)"] {
        admin.sql_session().execute(ddl, &[], None).unwrap();
        for (request, receipt) in requests.iter().zip(&receipts) { replay(&engine, request, receipt); }
    }
    // Public metadata exposes domain/origin, but not V2 feature bits.
}

#[test]
fn pinned_sql_reader_survives_compaction_and_public_reclaim_then_reopen() {
    let (dir, admin, engine) = fixture();
    let live = request(&admin, false);
    let expired = request(&admin, true);
    let retained = engine.execute_once(&live, &Bindings::new(), &|| at(1000, 1001)).unwrap();
    engine.execute_once(&expired, &Bindings::new(), &|| at(1000, 1001)).unwrap();
    let mut reader = admin.sql_session();
    // Public SQL transactions use ReadCommitted; the unit supplement pins Kernel RR.
    reader.execute("BEGIN", &[], None).unwrap();
    let before = reader.execute("SELECT id,n,d,ts FROM items ORDER BY id", &[], None).unwrap().rows;
    assert_eq!(engine.compact_outcomes(&|| at(1012, 1013)), Ok(1));
    assert_eq!(reader.execute("SELECT id,n,d,ts FROM items ORDER BY id", &[], None).unwrap().rows, before);
    replay(&engine, &live, &retained);
    reader.execute("ROLLBACK", &[], None).unwrap();
    drop(reader);
    admin.reclaim_storage().unwrap();
    drop(engine); drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    assert_eq!(engine.outcome(&expired.identity).unwrap(), OnceStatus::Expired { retry_until_seconds: 1011 });
    assert_eq!(engine.execute_once(&expired, &Bindings::new(), &|| panic!("expired key")),
        Err(OnceError::KeyExpired { retry_until_seconds: 1011 }));
    replay(&engine, &live, &retained);
}

#[test]
fn observed_errors_use_existing_public_variants() {
    fn tag(error: OnceError) -> u8 {
        match error {
            OnceError::DomainMismatch => 0, OnceError::InProgress => 1,
            OnceError::IdempotencyConflict => 2, OnceError::ClockUncertain => 3,
            OnceError::RetentionUnavailable => 4, OnceError::KeyExpired { .. } => 5,
            OnceError::Unknown => 6, OnceError::Operation(_) => 7,
            // Existing local codec error predates V2 activation; it is not a new wire tag.
            OnceError::InvalidOutcome => panic!("unexpected local codec error"),
        }
    }
    let (_dir, admin, engine) = fixture();
    let original = request(&admin, false);
    engine.execute_once(&original, &Bindings::new(), &|| at(1000, 1001)).unwrap();
    let mut changed = original.clone();
    changed.retention_seconds += 1;
    assert_eq!(tag(engine.execute_once(&changed, &Bindings::new(), &|| panic!("conflict clock")).unwrap_err()), 2);
    changed.identity.database_domain ^= 1;
    assert_eq!(tag(engine.outcome(&changed.identity).unwrap_err()), 0);
    let mut fresh = request(&admin, true);
    assert_eq!(tag(engine.execute_once(&fresh, &Bindings::new(), &|| Err(OnceClockUnavailable)).unwrap_err()), 3);
    fresh.retention_seconds = 0;
    assert_eq!(tag(engine.execute_once(&fresh, &Bindings::new(), &|| panic!("retention clock")).unwrap_err()), 4);
    fresh.retention_seconds = 10;
    fresh.body = OnceBody::Operation(Request::Read { table: "items".into(), filter: None,
        order: vec![], limit: None, projection: vec![] });
    let error = engine.execute_once(&fresh, &Bindings::new(), &|| panic!("unsupported clock")).unwrap_err();
    assert_eq!(error, OnceError::Operation(Error::Unsupported));
    assert_eq!(tag(error), 7);
}

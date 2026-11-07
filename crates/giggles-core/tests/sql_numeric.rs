mod common;
use common::*;
use giggles_core::{relational::*, *};
fn n(s: &str) -> Value {
    Value::Numeric(Numeric::parse(s).unwrap())
}
#[test]
fn numeric_keys_match_sql_order_and_equality() {
    let inputs = [
        "-Infinity",
        "-100",
        "-1.01",
        "-1",
        "-0.0001",
        "0",
        "0.0001",
        "1",
        "1.01",
        "100",
        "Infinity",
        "NaN",
    ];
    let keys = inputs
        .iter()
        .map(|s| encode_key(&[n(s)]).unwrap())
        .collect::<Vec<_>>();
    assert!(keys.windows(2).all(|k| k[0] < k[1]));
    assert_eq!(encode_key(&[n("1.0")]), encode_key(&[n("1.000")]));
    for bits in [0, 1 << 63] {
        assert_eq!(
            encode_key(&[Value::Float8(Float8::from_bits(bits))]),
            encode_key(&[Value::Float8(Float8::from_bits(0))])
        );
    }
    for bits in [f64::NAN.to_bits(), u64::MAX, 0x7ff0000000000001] {
        assert_eq!(
            encode_key(&[Value::Float8(Float8::from_bits(bits))]),
            encode_key(&[Value::Float8(Float8::from_bits(f64::NAN.to_bits()))])
        );
    }
}
#[test]
fn typed_sql_params_sums_uniqueness_and_reopen() {
    let path = fresh("numeric");
    let a = Admin::create(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE nums (id NUMERIC PRIMARY KEY, n BIGINT, r REAL, d DOUBLE PRECISION)",
        &[],
        None,
    )
    .unwrap();
    let p = s.prepare("INSERT INTO nums VALUES($1,$2,$3,$4)").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![
            DataType::Numeric,
            DataType::BigInt,
            DataType::Real,
            DataType::DoublePrecision
        ]
    );
    for id in ["1.00", "2"] {
        s.execute_prepared(
            &p,
            &[
                n(id),
                Value::Int(i64::MAX),
                Value::Float4(Float4::parse("-0").unwrap()),
                Value::Float8(Float8::parse("NaN").unwrap()),
            ],
            None,
        )
        .unwrap();
    }
    assert!(
        s.execute("INSERT INTO nums VALUES('1.000',0,0,0)", &[], None)
            .is_err()
    );
    let p = s
        .prepare("SELECT sum(n),sum(id),min(r),max(d) FROM nums")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        DataType::Numeric
    );
    let result = s.execute_prepared(&p, &[], None).unwrap();
    assert_eq!(result.rows[0].values[0], n("18446744073709551614"));
    assert_eq!(result.rows[0].values[1], n("3.00"));
    let export = fresh("numeric-export");
    a.export(&export).unwrap();
    let restored = Admin::restore(&export, fresh("numeric-restore"), Limits::default()).unwrap();
    assert_eq!(
        restored
            .sql_session()
            .execute("SELECT sum(n) FROM nums", &[], None)
            .unwrap()
            .rows[0]
            .values[0],
        n("18446744073709551614")
    );
    drop(s);
    drop(a);
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        s.execute("SELECT id FROM nums ORDER BY id LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
}
#[test]
fn scalar_numeric_casts_and_literals() {
    let a = Admin::create(fresh("numeric-scalar"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let r=s.execute("SELECT abs('-1.250'::numeric), 123456789012345678901234567890.001, '-0'::real, 'NaN'::double precision",&[],None).unwrap();
    assert_eq!(r.rows[0].values[0], n("1.250"));
    assert_eq!(r.rows[0].values[1], n("123456789012345678901234567890.001"));
}

#[test]
fn scalar_rounding_limits_and_float_specials() {
    let a = Admin::create(fresh("numeric-functions"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let r=s.execute("SELECT round('1.25'::numeric,1),trunc('-1.29'::numeric,1),floor('-1.1'::numeric),ceil('1.1'::numeric),sign('NaN'::numeric),abs('-Infinity'::double precision)",&[],None).unwrap();
    assert_eq!(
        &r.rows[0].values[..5],
        &[n("1.3"), n("-1.2"), n("-2"), n("2"), n("NaN")]
    );
    assert!(Numeric::parse(&format!("1{}", "0".repeat(giggles_pg_types::MAX_DIGITS))).is_err());
    assert!(Numeric::parse("1e-16384").is_err());
    assert_eq!(Numeric::parse("0.00000").unwrap().to_string(), "0.00000");
}

#[test]
fn native_keyed_numeric_intent_outcome_and_replay() {
    use giggles_core::operations::{Bindings, Engine, Request, ValueExpr, once::*};
    use std::time::{Duration, Instant};
    let a = Admin::create(fresh("numeric-native-once"), Limits::default()).unwrap();
    a.sql_session()
        .execute(
            "CREATE TABLE typed(id NUMERIC PRIMARY KEY,r REAL,d DOUBLE PRECISION)",
            &[],
            None,
        )
        .unwrap();
    let request = OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: a.snapshot().unwrap().domain,
            tenant: 1,
            namespace: "numeric.jobs".into(),
            key: vec![1],
        },
        retention_seconds: 60,
        body: OnceBody::Operation(Request::Returning {
            request: Box::new(Request::Insert {
                table: "typed".into(),
                values: vec![
                    ("id".into(), ValueExpr::Literal(n("9007199254740993.00100"))),
                    (
                        "r".into(),
                        ValueExpr::Literal(Value::Float4(Float4::from_bits(0x80000000))),
                    ),
                    (
                        "d".into(),
                        ValueExpr::Literal(Value::Float8(Float8::from_bits(0x7ff8000000000042))),
                    ),
                ],
            }),
            projection: vec![],
        }),
    };
    let e = Engine::new(a.clone());
    let clock = || {
        Ok(OnceTime {
            lower_unix_seconds: 1000,
            upper_unix_seconds: 1001,
            valid_until: Instant::now() + Duration::from_secs(10),
        })
    };
    let first = e.execute_once(&request, &Bindings::new(), &clock).unwrap();
    let no_clock = || -> Result<OnceTime, OnceClockUnavailable> { panic!("replay needs no clock") };
    let replay = e
        .execute_once(&request, &Bindings::new(), &no_clock)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(first.accepted_fingerprint, replay.accepted_fingerprint);
    assert_eq!(first.outcome, replay.outcome);
    let OnceSchema::Operation(columns) = replay.description.output else {
        panic!("operation schema")
    };
    assert_eq!(
        columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![DataType::Numeric, DataType::Real, DataType::DoublePrecision]
    );
    let mut changed = request.clone();
    let OnceBody::Operation(Request::Returning { request: body, .. }) = &mut changed.body else {
        panic!("returning")
    };
    let Request::Insert { values, .. } = body.as_mut() else {
        panic!("insert")
    };
    values[0].1 = ValueExpr::Literal(n("9007199254740993.00200"));
    assert_eq!(
        e.execute_once(&changed, &Bindings::new(), &no_clock),
        Err(OnceError::IdempotencyConflict)
    );
}

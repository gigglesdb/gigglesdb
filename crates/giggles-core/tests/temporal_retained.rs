mod common;
use giggles_core::{
    Admin, Limits,
    operations::{Bindings, ConflictAction, Engine, Expr, Request, ValueExpr, once::*},
};
use std::time::{Duration, Instant};
#[test]
fn temporal_native_retained_empty_descriptors_survive_export_and_reopen() {
    retained(true);
}
#[test]
fn temporal_native_retained_values_preserve_components_on_reopen() {
    retained(false);
}
fn retained(empty: bool) {
    use giggles_core::relational::{Date, Interval, PgTime, TimeTz, Timestamp, TimestampTz, Value};
    let values = vec![
        Value::Int(2),
        Value::Date(Date::from_days(-1).unwrap()),
        Value::Time(PgTime::from_microseconds(86400000000).unwrap()),
        Value::TimeTz(TimeTz::new(PgTime::from_microseconds(1).unwrap(), 20700).unwrap()),
        Value::Timestamp(Timestamp::from_microseconds(-1).unwrap()),
        Value::TimestampTz(TimestampTz::from_microseconds(-1).unwrap()),
        Value::Interval(Interval::new(1, -30, 1)),
    ];
    let path = common::fresh("temporal-retained-empty");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.sql_session().execute("CREATE TABLE t(id BIGINT PRIMARY KEY, d DATE, t TIME, z TIMETZ, ts TIMESTAMP, tz TIMESTAMPTZ, i INTERVAL)", &[], None).unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO t(id) VALUES(1)", &[], None)
        .unwrap();
    let request = OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: admin.snapshot().unwrap().domain,
            tenant: 42,
            namespace: "temporal.test".into(),
            key: b"empty".to_vec(),
        },
        retention_seconds: 60,
        body: OnceBody::Operation(Request::Returning {
            request: Box::new(Request::OnConflict {
                request: Box::new(Request::InsertMany {
                    table: "t".into(),
                    columns: if empty {
                        vec!["id".into()]
                    } else {
                        ["id", "d", "t", "z", "ts", "tz", "i"]
                            .map(String::from)
                            .to_vec()
                    },
                    rows: if empty {
                        vec![vec![Expr::Value(ValueExpr::Literal(Value::Int(1)))]]
                    } else {
                        vec![
                            values
                                .iter()
                                .cloned()
                                .map(|v| Expr::Value(ValueExpr::Literal(v)))
                                .collect(),
                        ]
                    },
                    cap: ValueExpr::Literal(giggles_core::relational::Value::Int(1)),
                }),
                target: vec!["id".into()],
                action: ConflictAction::Nothing,
            }),
            projection: vec![],
        }),
    };
    let engine = Engine::new(admin.clone());
    let first = engine
        .execute_once(&request, &Bindings::new(), &|| {
            Ok(OnceTime {
                lower_unix_seconds: 1000,
                upper_unix_seconds: 1001,
                valid_until: Instant::now() + Duration::from_secs(10),
            })
        })
        .unwrap();
    let OnceOutcome::Operation(outcome) = first.outcome.as_ref().unwrap() else {
        panic!()
    };
    if empty {
        assert!(outcome.rows.is_empty());
    } else {
        assert_eq!(outcome.rows[0].values, values);
        let Value::Interval(i) = outcome.rows[0].values[6] else {
            panic!()
        };
        assert!(i.same_components(Interval::new(1, -30, 1)));
    }
    assert_eq!(outcome.columns.len(), 7);

    let export = common::fresh("temporal-retained-empty-export");
    admin.export(&export).unwrap();
    let bytes = std::fs::read(&export).unwrap();
    assert!((15..=26).contains(&u32::from_be_bytes(bytes[8..12].try_into().unwrap())));
    drop(engine);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let engine = Engine::new(admin);
    let replay = engine
        .execute_once(&request, &Bindings::new(), &|| {
            panic!("retained replay must not read clock")
        })
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, first.outcome);
}

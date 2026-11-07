mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn casts_truncation_interval_zones_and_typed_nulls() {
    let a = Admin::create(fresh("temporal-overloads"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let r=s.execute("SELECT timestamptz '2024-03-10 02:30:00 America/New_York'::text, timestamp '0001-01-01 BC'::text, time '12:34:56-15:59'::text, timezone(interval '90 minutes', timestamp '2000-01-01')::text, date_trunc('day', timestamptz '2024-03-10 07:30:00+00', 'America/New_York')::text", &[], None).unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![
            Value::Text("2024-03-10 07:30:00+00".into()),
            Value::Text("0001-01-01 00:00:00 BC".into()),
            Value::Text("12:34:56".into()),
            Value::Text("1999-12-31 22:30:00+00".into()),
            Value::Text("2024-03-10 05:00:00+00".into())
        ]
    );
    let p=s.prepare("SELECT date_trunc($1::text, $2::timestamptz, $3::text), timezone($4::interval, $5::timestamp)").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![
            DataType::Text,
            DataType::TimestampTz,
            DataType::Text,
            DataType::Interval,
            DataType::Timestamp
        ]
    );
    assert_eq!(
        s.execute_prepared(
            &p,
            &[
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null
            ],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        vec![Value::Null, Value::Null]
    );
    assert!(
        s.execute(
            "SELECT timezone(interval '1 day', timestamp '2000-01-01')",
            &[],
            None
        )
        .is_err()
    );
}

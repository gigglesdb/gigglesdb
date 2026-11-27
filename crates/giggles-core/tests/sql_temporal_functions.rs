mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn temporal_projection_casts_null_parameters_and_nesting() {
    let a = Admin::create(fresh("temporal-functions"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let r = s.execute("SELECT date '2000-01-01', time '24:00:00', timestamp '2000-01-01 00:00:00', NULL::interval", &[], None).unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![
            Value::Date(Date::from_days(0).unwrap()),
            Value::Time(PgTime::from_microseconds(86400000000).unwrap()),
            Value::Timestamp(Timestamp::from_microseconds(0).unwrap()),
            Value::Null
        ]
    );
    let p = s
        .prepare("SELECT date_part('year', $1::timestamp)")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![DataType::Timestamp]
    );
    let r = s
        .execute_prepared(
            &p,
            &[Value::Timestamp(Timestamp::from_microseconds(0).unwrap())],
            None,
        )
        .unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![Value::Float8(Float8::parse("2000").unwrap())]
    );
    assert_eq!(
        s.execute_prepared(&p, &[Value::Null], None).unwrap().rows[0].values,
        vec![Value::Null]
    );
    let r = s
        .execute(
            "SELECT date_part('hour', date_trunc('hour', timestamp '2000-01-01 12:34:56'))",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![Value::Float8(Float8::parse("12").unwrap())]
    );
}
#[test]
fn temporal_key_codec_fails_closed() {
    for (index, value) in [
        Value::Date(Date::from_days(0).unwrap()),
        Value::Time(PgTime::from_microseconds(0).unwrap()),
        Value::TimeTz(TimeTz::new(PgTime::from_microseconds(0).unwrap(), 0).unwrap()),
        Value::Timestamp(Timestamp::from_microseconds(0).unwrap()),
        Value::TimestampTz(TimestampTz::from_microseconds(0).unwrap()),
    ]
    .into_iter()
    .enumerate()
    {
        // PG 18.6 accepts these key types (basis.md); this additive codec is GigglesDB-specific.
        // PG 18.6 psql -A -t:
        // 2000-01-01|00:00:00|00:00:00+00|2000-01-01 00:00:00|2000-01-01 00:00:00+00
        let bytes = encode_key(&[value]).unwrap();
        assert_eq!(bytes[0], 8 + index as u8);
        assert_eq!(bytes.len(), if index == 2 { 13 } else { 9 });
    }
    assert_eq!(
        encode_key(&[Value::Interval(Interval::new(0, 0, 0))]),
        Err(SqlError::Unsupported)
    );
}

#[test]
fn extract_timezone_calendar_and_justify_match_pg18() {
    let a = Admin::create(fresh("temporal-calendar"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let r=s.execute("SELECT extract(year from timestamp '2000-01-01 00:00:00'),timezone('America/New_York', timestamp '2000-01-01 12:00:00')::text,age(timestamp '2001-02-03 00:00:00',timestamp '2000-01-01 00:00:00')::text,justify_days(interval '31 days')::text,justify_hours(interval '25 hours')::text,make_date(2000,2,29)::text",&[],None).unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![
            Value::Numeric(Numeric::parse("2000").unwrap()),
            Value::Text("2000-01-01 17:00:00+00".into()),
            Value::Text("1 year 1 mon 2 days".into()),
            Value::Text("1 mon 1 day".into()),
            Value::Text("1 day 01:00:00".into()),
            Value::Text("2000-02-29".into())
        ]
    );
    let r=s.execute("SELECT date_bin(interval '15 minutes',timestamp '2000-01-01 12:34:56',timestamp '2000-01-01 00:00:00')::text,justify_interval(interval '1 mon -30 days')::text",&[],None).unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![
            Value::Text("2000-01-01 12:30:00".into()),
            Value::Text("00:00:00".into())
        ]
    );
}

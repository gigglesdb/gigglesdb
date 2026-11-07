mod common;
use giggles_core::{Admin, Limits, relational::Value};
#[test]
fn constructors_match_pinned_pg18_calendar_rounding_and_zone_oracles() {
    let a = Admin::create(common::fresh("temporal-constructors"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (query, expected) in [
        ("SELECT make_time(24,0,0)::text", "24:00:00"),
        (
            "SELECT make_timestamp(-1,1,1,0,0,0)::text",
            "0001-01-01 00:00:00 BC",
        ),
        (
            "SELECT make_timestamptz(2024,11,3,1,30,0,'America/New_York')::text",
            "2024-11-03 06:30:00+00",
        ),
        (
            "SELECT make_interval(1,-2,3,-4,5,-6,0.0000015)::text",
            "10 mons 17 days 04:54:00.000002",
        ),
        ("SELECT make_interval()::text", "00:00:00"),
    ] {
        assert_eq!(
            s.execute(query, &[], None).unwrap().rows[0].values,
            vec![Value::Text(expected.into())],
            "{query}"
        );
    }
    assert_eq!(
        s.execute("SELECT make_time(NULL::integer,0,0)", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Null]
    );
    for (query, expected) in [
        (
            "SELECT CAST('24:00:00' AS time without time zone)::text",
            "24:00:00",
        ),
        (
            "SELECT time with time zone '12:34:56+05:45'::text",
            "12:34:56+05:45",
        ),
        (
            "SELECT CAST('1969-12-31 23:59:59.999999' AS timestamp without time zone)::text",
            "1969-12-31 23:59:59.999999",
        ),
        (
            "SELECT timestamp with time zone '2000-01-01 01:00:00+01'::text",
            "2000-01-01 00:00:00+00",
        ),
    ] {
        assert_eq!(
            s.execute(query, &[], None).unwrap().rows[0].values,
            vec![Value::Text(expected.into())],
            "{query}"
        );
    }
}

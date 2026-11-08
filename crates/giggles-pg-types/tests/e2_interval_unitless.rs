//! E2b′: a number with no unit is seconds (PostgreSQL DecodeInterval).
//! PG18.6 capture: J:/gigglesdb-work/e2/oracle/e2b-interval.out and e2b-interval-each.out.
use giggles_pg_types::Error;
use giggles_pg_types::temporal::Interval;

#[test]
fn unitless_numbers_are_seconds_like_postgres_18() {
    for (input, expected) in [
        ("0", "00:00:00"),
        (" 0 ", "00:00:00"),
        ("-0", "00:00:00"),
        ("5", "00:00:05"),
        ("-5", "-00:00:05"),
        ("+3", "00:00:03"),
        ("1.5", "00:00:01.5"),
        ("0.5", "00:00:00.5"),
        ("-1.5", "-00:00:01.5"),
        ("0.000001", "00:00:00.000001"),
        ("1.0000005", "00:00:01"),
        ("100000", "27:46:40"),
        ("9223372036854", "2562047788:00:54"),
        ("@ 5", "00:00:05"),
        ("1 day 2", "1 day 00:00:02"),
        ("1 day -5", "1 day -00:00:05"),
        ("5 minutes 3", "00:05:03"),
        ("2 1:00", "2 days 01:00:00"),
        ("1:30", "01:30:00"),
        ("0 seconds", "00:00:00"),
        // e2b-interval-order.out: `ago` after a unit or clock field still negates.
        ("5 seconds ago", "-00:00:05"),
        ("1 day ago", "-1 days"),
        ("1:30 ago", "-01:30:00"),
        ("5 1:00 ago", "-5 days -01:00:00"),
        ("1 day 2 hours", "1 day 02:00:00"),
    ] {
        let value = Interval::parse(input).unwrap_or_else(|e| panic!("{input:?}: {e:?}"));
        assert_eq!(value.to_string(), expected, "{input:?}");
    }
}

#[test]
fn postgres_18_unitless_rejections_are_syntax_errors() {
    // PG18.6: 22007 invalid input syntax for type interval. A unitless number is seconds
    // only as the final field: before `ago`, a unit or another number it is rejected
    // (e2b-interval-ago.out, e2b-interval-order.out).
    for input in [
        "",
        "x",
        "1 2",
        "1 2 3",
        "1.5.5",
        "1e2",
        "5 ago",
        "1 day 2 ago",
        "@ 5 ago",
        "0 ago",
        "-1.5 ago",
        "1 2 ago",
        "2 1 day",
        "5 3 minutes",
        "1 day 2 3 hours",
        "3 1 day 2",
    ] {
        assert_eq!(Interval::parse(input), Err(Error::Syntax), "{input:?}");
    }
}

//! E2b′: a number with no unit is seconds (PostgreSQL DecodeInterval).
//! PG18.6 capture: J:/gigglesdb-work/e2/oracle/e2b-interval.out and e2b-interval-each.out.
use giggles_pg_types::Error;
use giggles_pg_types::temporal::{Interval, IntervalStyle};

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

#[test]
fn bare_fraction_and_trailing_dot_numbers_match_postgres_18() {
    // PG18.6 capture: J:/gigglesdb-work/streamE/pg18.txt. ParseDateTime reads a field that
    // starts with '.' as a number with a zero whole part, and '1.' has an empty fraction.
    for (input, expected) in [
        (".5", "00:00:00.5"),
        ("1.", "00:00:01"),
        ("-1.", "-00:00:01"),
        (".", "00:00:00"),
        ("@ .5", "00:00:00.5"),
        (".5 seconds", "00:00:00.5"),
        (". seconds", "00:00:00"),
        ("1. seconds", "00:00:01"),
        ("1. day", "1 day"),
        ("-1. day", "-1 days"),
        (".5 day", "12:00:00"),
        (".5 minutes", "00:00:30"),
        (".5 hours", "00:30:00"),
        (".5 week", "3 days 12:00:00"),
        (".5 mons", "15 days"),
        (".5 years", "6 mons"),
        ("1 day .5", "1 day 00:00:00.5"),
        ("1 day 1.", "1 day 00:00:01"),
        ("1 day .", "1 day"),
        ("1.5 minutes .5", "00:01:30.5"),
        ("1.5 minutes .5 seconds", "00:01:30.5"),
        (".5 seconds ago", "-00:00:00.5"),
        (".123456789", "00:00:00.123457"),
        (".0000005", "00:00:00"),
        // A zero fraction does not claim the millisecond and microsecond fields.
        ("1. seconds 5 milliseconds", "00:00:01.005"),
        ("1.0 seconds 5 milliseconds", "00:00:01.005"),
        (". seconds 5 milliseconds", "00:00:00.005"),
    ] {
        let value = Interval::parse(input).unwrap_or_else(|e| panic!("{input:?}: {e:?}"));
        assert_eq!(value.to_string(), expected, "{input:?}");
    }
}

#[test]
fn postgres_18_bare_fraction_rejections_are_syntax_errors() {
    // PG18.6: 22007. A sign directly before '.' is a bad field in ParseDateTime.
    for input in [
        "-.5",
        "+.5",
        "-.",
        "-.5 day",
        "1 day -.5",
        "- day",
        ". 5",
        "1..5",
        "..5",
        ".5.",
        ".5e1",
        "1.e1",
        ".5 1",
        "1 .5",
        "00:00:00 .5",
        ".5 ago",
        "1.5 seconds 5 milliseconds",
    ] {
        assert_eq!(Interval::parse(input), Err(Error::Syntax), "{input:?}");
    }
    // The SQL-standard leading sign does not rescue '-.5' either.
    for input in ["-.5", "-.5 day"] {
        assert_eq!(
            Interval::parse_with_style(input, IntervalStyle::SqlStandard),
            Err(Error::Syntax),
            "{input:?}"
        );
    }
}

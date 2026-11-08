//! Fixed PostgreSQL 18.6 arithmetic observations; no running oracle is required.
use giggles_pg_types::temporal::*;

fn interval(s: &str) -> Interval {
    Interval::parse(s).unwrap_or_else(|e| panic!("interval {s:?}: {e:?}"))
}
fn date(s: &str) -> Date {
    Date::parse(s, &ParserSettings::default()).unwrap()
}
fn timestamp(s: &str) -> Timestamp {
    Timestamp::parse(s, &ParserSettings::default()).unwrap()
}
fn timestamptz(s: &str) -> TimestampTz {
    TimestampTz::parse(s, &ParserSettings::default()).unwrap()
}
fn time(s: &str) -> Time {
    Time::parse(s).unwrap()
}
fn timetz(s: &str) -> TimeTz {
    TimeTz::parse(s, &ParserSettings::default()).unwrap()
}
fn check_interval(actual: Result<Interval, TemporalFunctionError>, expected: &str) {
    let actual = actual.unwrap();
    assert_eq!(actual.to_string(), expected);
    assert!(
        actual.same_components(interval(expected)),
        "expected PG18.6 {expected}, got {actual} ({actual:?})"
    );
}
fn range<T: std::fmt::Debug>(actual: Result<T, TemporalFunctionError>) {
    assert!(
        matches!(actual, Err(TemporalFunctionError::Range)),
        "{actual:?}"
    );
}

#[test]
fn interval_add_subtract_and_negate() {
    check_interval(
        interval("1 month").checked_add(interval("2 days 03:00")),
        "1 mon 2 days 03:00:00",
    );
    check_interval(
        interval("1 day").checked_sub(interval("25 hours")),
        "1 day -25:00:00",
    );
    check_interval(
        interval("1 year 2 days 03:00").checked_neg(),
        "-1 years -2 days -03:00:00",
    );
    for input in ["-2147483648 days", "-178956970 years -8 months"] {
        range(interval(input).checked_neg());
    }
    let inf = Interval::INFINITY;
    let neg = Interval::NEG_INFINITY;
    check_interval(inf.checked_add(interval("1 day")), "infinity");
    range(inf.checked_add(neg));
    range(inf.checked_sub(inf));
    check_interval(neg.checked_sub(inf), "-infinity");
    check_interval(neg.checked_add(neg), "-infinity");
    for input in ["178000000 years", "2562047788 hours"] {
        range(interval(input).checked_add(interval(input)));
    }
}

#[test]
fn interval_multiply_pg18() {
    for (input, factor, expected) in [
        ("1 month", 1.5, "1 mon 15 days"),
        ("1 day", 0.5, "12:00:00"),
        ("1 hour", 3.0, "03:00:00"),
        ("1 month 1 day", 2.0, "2 mons 2 days"),
        ("1 month 1 day 1 hour", 2.5, "2 mons 17 days 14:30:00"),
        ("1 month", 0.3333333, "9 days 23:59:59.9136"),
        ("1 mon 2 days 03:00", -1.5, "-1 mons -18 days -04:30:00"),
        ("1 day 1 second", 0.000001, "00:00:00.086401"),
        ("1 month 1 day", 1.75, "1 mon 24 days 06:00:00"),
        ("1 day", f64::INFINITY, "infinity"),
        ("-1 day", f64::INFINITY, "-infinity"),
        ("infinity", 2.0, "infinity"),
        ("infinity", -1.0, "-infinity"),
    ] {
        check_interval(interval(input).mul_f64(factor), expected);
    }
    for (input, factor) in [
        ("1 day", f64::NAN),
        ("infinity", 0.0),
        ("infinity", -0.0),
        ("1 month", 1e10),
        ("1 day", 2147483648.0),
    ] {
        range(interval(input).mul_f64(factor));
    }
}

#[test]
fn interval_divide_pg18() {
    for (input, divisor, expected) in [
        ("1 month", 3.0, "10 days"),
        ("1 day", 7.0, "03:25:42.857143"),
        ("1 year 1 day", 2.5, "4 mons 24 days 09:36:00"),
        ("1 second", 3.0, "00:00:00.333333"),
        ("1 mon", -3.0, "-10 days"),
        ("infinity", 2.0, "infinity"),
        ("1 day", f64::INFINITY, "00:00:00"),
    ] {
        check_interval(interval(input).div_f64(divisor), expected);
    }
    range(Interval::INFINITY.div_f64(f64::INFINITY));
    range(interval("1 day").div_f64(f64::NAN));
    for divisor in [0.0, -0.0] {
        assert_eq!(
            interval("1 day").div_f64(divisor),
            Err(TemporalFunctionError::DivisionByZero)
        );
    }
}

#[test]
fn interval_edge_components_and_infinite_factors() {
    check_interval(Interval::new(0, 0, 1).mul_f64(0.5), "00:00:00");
    check_interval(Interval::new(0, 0, 3).mul_f64(0.5), "00:00:00.000002");
    // Finite operands must not accidentally manufacture an infinity sentinel.
    range(Interval::new(i32::MAX, i32::MAX, i64::MAX - 1).checked_add(Interval::new(0, 0, 1)));
    range(Interval::new(i32::MIN, i32::MIN, i64::MIN + 1).checked_sub(Interval::new(0, 0, 1)));
    for factor in [0.0, -0.0] {
        check_interval(interval("-1 mon 2 days -03:00").mul_f64(factor), "00:00:00");
    }
    // PG18.6 takes the sign of the 30-day-month total, as interval comparison does.
    for (value, factor, expected) in [
        (interval("1 month -40 days"), f64::INFINITY, "-infinity"),
        (interval("-1 month 40 days"), f64::NEG_INFINITY, "-infinity"),
        (Interval::new(0, 1, -USECS_PER_DAY * 2), f64::INFINITY, "-infinity"),
        (Interval::new(0, -1, USECS_PER_DAY * 2), f64::INFINITY, "infinity"),
    ] {
        check_interval(value.mul_f64(factor), expected);
    }
    range(interval("1 day -24 hours").mul_f64(f64::INFINITY));
    range(Interval::new(0, 0, 0).mul_f64(f64::INFINITY));
}

#[test]
fn timestamp_differences_pg18() {
    for (left, right, expected) in [
        ("2024-03-01 00:00", "2024-01-01 12:00", "59 days 12:00:00"),
        (
            "2024-01-01 00:00",
            "2024-01-03 01:02:03.5",
            "-2 days -01:02:03.5",
        ),
        ("2024-01-02 03:00", "2024-01-01 02:00", "1 day 01:00:00"),
        ("2024-01-02 01:00", "2024-01-01 23:00", "02:00:00"),
        ("infinity", "2024-01-01 00:00", "infinity"),
        ("2024-01-01 00:00", "infinity", "-infinity"),
        ("-infinity", "infinity", "-infinity"),
    ] {
        check_interval(timestamp(left).diff(timestamp(right)), expected);
    }
    range(Timestamp::INFINITY.diff(Timestamp::INFINITY));
    range(Timestamp::NEG_INFINITY.diff(Timestamp::NEG_INFINITY));
    range(timestamp("294276-12-31 00:00").diff(timestamp("4714-11-24 00:00 BC")));
    check_interval(
        timestamptz("2024-03-11 05:00Z").diff(timestamptz("2024-03-10 06:00Z")),
        "23:00:00",
    );
    range(TimestampTz::INFINITY.diff(TimestampTz::INFINITY));
    range(TimestampTz::NEG_INFINITY.diff(TimestampTz::NEG_INFINITY));
}

#[test]
fn date_day_arithmetic_pg18() {
    assert_eq!(
        date("2024-01-31").add_days(1).unwrap().to_string(),
        "2024-02-01"
    );
    assert_eq!(
        date("2024-03-01").sub_days(1).unwrap().to_string(),
        "2024-02-29"
    );
    assert_eq!(Date::INFINITY.add_days(1).unwrap(), Date::INFINITY);
    range(date("5874897-12-31").add_days(1));
    range(date("4714-11-24 BC").sub_days(1));
    range(date("2024-01-01").add_days(2147483647));
    range(date("2024-01-01").sub_days(i64::MIN));
    range(date("2024-01-01").add_days(i64::MAX));
    assert_eq!(date("2024-03-01").diff(date("2024-01-01")).unwrap(), 60);
    assert_eq!(date("2024-01-01").diff(date("2024-03-01")).unwrap(), -60);
    assert_eq!(
        date("5874897-12-31").diff(date("4714-11-24 BC")).unwrap(),
        2147483493
    );
    range(Date::INFINITY.diff(date("2024-01-01")));
    // Reaching a reserved infinity integer through finite addition is an error.
    let epoch = Date::from_days(0).unwrap();
    range(epoch.add_days(i64::from(i32::MAX)));
    range(epoch.add_days(i64::from(i32::MIN)));
}

#[test]
fn date_plus_time_and_timetz_pg18() {
    for (day, clock, expected) in [
        ("2024-01-01", "10:30", "2024-01-01 10:30:00"),
        ("2024-12-31", "24:00", "2025-01-01 00:00:00"),
        (
            "294276-12-31",
            "23:59:59.999999",
            "294276-12-31 23:59:59.999999",
        ),
        ("infinity", "10:30", "infinity"),
    ] {
        assert_eq!(
            date(day).add_time(time(clock)).unwrap().to_string(),
            expected
        );
    }
    for (day, clock, expected) in [
        ("2024-01-01", "10:00+02", "2024-01-01 08:00:00Z"),
        ("2024-07-01", "10:00-07", "2024-07-01 17:00:00Z"),
        ("-infinity", "10:00+02", "-infinity"),
    ] {
        let actual = date(day).add_timetz(timetz(clock)).unwrap();
        assert_eq!(actual, timestamptz(expected));
        assert_eq!(actual.to_string(), timestamptz(expected).to_string());
    }
}

#[test]
fn time_interval_arithmetic_pg18() {
    for (clock, span, expected) in [
        ("23:00", "2 hours", "01:00:00"),
        ("10:00", "1 month 1 day 1 hour", "11:00:00"),
        ("10:00", "49 hours 30 minutes", "11:30:00"),
        ("24:00", "0 seconds", "00:00:00"),
    ] {
        assert_eq!(
            time(clock)
                .add_interval(interval(span))
                .unwrap()
                .to_string(),
            expected
        );
    }
    for (clock, span, expected) in [
        ("01:00", "2 hours", "23:00:00"),
        ("10:00", "-49 hours", "11:00:00"),
        ("00:00", "0.000001 seconds", "23:59:59.999999"),
    ] {
        assert_eq!(
            time(clock)
                .sub_interval(interval(span))
                .unwrap()
                .to_string(),
            expected
        );
    }
    range(time("10:00").add_interval(Interval::INFINITY));
    range(time("10:00").sub_interval(Interval::NEG_INFINITY));
    check_interval(Ok(time("10:00").diff(time("12:30"))), "-02:30:00");
    check_interval(Ok(time("24:00").diff(time("00:00"))), "24:00:00");
    for clock in ["00:00", "12:34:56.123456", "24:00"] {
        let value = time(clock);
        assert!(value
            .to_interval()
            .same_components(Interval::new(0, 0, value.microseconds())));
    }
}

#[test]
fn time_wrap_handles_full_i64_interval_component() {
    for (micros, added, subtracted) in [
        (i64::MIN, "19:59:05.224191", "04:00:54.775807"),
        (i64::MAX, "04:00:54.775806", "19:59:05.224192"),
    ] {
        let clock = time("23:59:59.999999");
        let span = Interval::new(0, 0, micros);
        assert_eq!(clock.add_interval(span).unwrap().to_string(), added);
        assert_eq!(clock.sub_interval(span).unwrap().to_string(), subtracted);
    }
}

#[test]
fn timetz_interval_arithmetic_preserves_offset() {
    for (clock, span, expected) in [
        ("23:00+02", "2 hours", "01:00:00+02"),
        ("10:00+05:30", "1 day 1 hour", "11:00:00+05:30"),
    ] {
        let original = timetz(clock);
        let actual = original.add_interval(interval(span)).unwrap();
        assert_eq!(actual.to_string(), expected);
        assert_eq!(actual.offset_east(), original.offset_east());
    }
    let original = timetz("01:00-05");
    let actual = original
        .sub_interval(interval("2 hours 30 minutes"))
        .unwrap();
    assert_eq!(actual.to_string(), "22:30:00-05");
    assert_eq!(actual.offset_east(), original.offset_east());
    range(original.add_interval(Interval::INFINITY));
}

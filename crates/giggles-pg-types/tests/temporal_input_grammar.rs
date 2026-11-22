use giggles_pg_types::{Error, temporal::*};

fn assert_text<T: ToString>(actual: Result<T, Error>, expected: &str, input: &str) {
    match expected {
        "22007" => assert_eq!(actual.err(), Some(Error::Syntax), "{input}"),
        "22008" => assert_eq!(actual.err(), Some(Error::Range), "{input}"),
        "22023" => assert_eq!(actual.err(), Some(Error::UnknownTimeZone), "{input}"),
        _ => assert_eq!(
            actual.map(|v| v.to_string()),
            Ok(expected.into()),
            "{input}"
        ),
    }
}

fn parser(date_order: DateOrder) -> ParserSettings {
    ParserSettings {
        date_order,
        offset_east: 0,
    }
}

fn columns<const N: usize>(line: &str) -> [&str; N] {
    let mut columns = line.split('|');
    let row = std::array::from_fn(|_| columns.next().unwrap());
    assert!(columns.next().is_none(), "{line}");
    row
}

// PostgreSQL 18.6, o4-input.out and o7-sessabbr.out. Columns are MDY, DMY, YMD.
const DATES: &str = "\
20240101|2024-01-01|2024-01-01|2024-01-01
J2451187|1999-01-08|1999-01-08|1999-01-08
Jan 2 2024|2024-01-02|2024-01-02|2024-01-02
January 2, 2024|2024-01-02|2024-01-02|2024-01-02
2 Jan 2024|2024-01-02|2024-01-02|2024-01-02
2-Jan-2024|2024-01-02|2024-01-02|2024-01-02
2024-Jan-02|2024-01-02|2024-01-02|2024-01-02
Jan-02-2024|2024-01-02|2024-01-02|2024-01-02
01/02/2024|2024-01-02|2024-02-01|22008
1/2/2024|2024-01-02|2024-02-01|22008
01/02/24|2024-01-02|2024-02-01|2001-02-24
2024/01/02|2024-01-02|2024-01-02|2024-01-02
01-02-2024|2024-01-02|2024-02-01|22008
01.02.2024|2024-01-02|2024-02-01|22008
2024.01.02|2024-01-02|2024-01-02|2024-01-02
13/01/2024|22008|2024-01-13|22008
01/13/2024|2024-01-13|22008|22008
240102|2024-01-02|2024-01-02|2024-01-02
2024001|22008|22008|22008
2024.002|2024-01-02|2024-01-02|2024-01-02
Feb 30 2024|22008|22008|22008
Foo 2 2024|22007|22007|22007
J0|4714-11-24 BC|4714-11-24 BC|4714-11-24 BC
Jan 2 2024 BC|2024-01-02 BC|2024-01-02 BC|2024-01-02 BC
Tue Jan 02 2024|2024-01-02|2024-01-02|2024-01-02
Monday 2024-01-01|22007|22007|22007
Mon 2024-01-01|22007|22007|22007
Monday, 2024-01-01|22007|22007|22007
Monday 01/02/2024|22007|22007|22007
Monday 2024.01.01|22007|22007|22007
Monday Jan-01-2024|22007|22007|22007
Monday 20240101|2024-01-01|2024-01-01|2024-01-01
2024-01-01 Monday|2024-01-01|2024-01-01|2024-01-01
19990108|1999-01-08|1999-01-08|1999-01-08
990108|1999-01-08|1999-01-08|1999-01-08
January 8, 99 BC|0099-01-08 BC|0099-01-08 BC|22008
1999-Jan-08|1999-01-08|1999-01-08|1999-01-08
Jan 08 99|1999-01-08|1999-01-08|22008
08-Jan-1999|1999-01-08|1999-01-08|1999-01-08
Jan 2|22007|22007|22007
2024 Jan 2|2024-01-02|2024-01-02|2024-01-02
01-01-01|2001-01-01|2001-01-01|2001-01-01
1/2/3|2003-01-02|2003-02-01|2001-02-03
Jan 32 2024|22008|22008|22008
2024-13-01|22008|22008|22008
J|22007|22007|22007
01/02/2024 garbage|22007|22007|22007
2024-01-02 03:04:05 PST|2024-01-02|2024-01-02|2024-01-02";

#[test]
fn calendar_input_matches_each_date_order() {
    for line in DATES.lines() {
        let [input, mdy, dmy, ymd] = columns(line);
        for (order, expected) in [
            (DateOrder::Mdy, mdy),
            (DateOrder::Dmy, dmy),
            (DateOrder::Ymd, ymd),
        ] {
            assert_text(Date::parse(input, &parser(order)), expected, input);
        }
    }
}

// PostgreSQL 18.6 with TimeZone UTC and DateStyle ISO, MDY.
const TIMESTAMPS: &str = "\
2024-01-02 03:04:05 PST|2024-01-02 11:04:05+00|2024-01-02 03:04:05
2024-07-02 03:04:05 PST|2024-07-02 11:04:05+00|2024-07-02 03:04:05
2024-07-02 03:04:05 PDT|2024-07-02 10:04:05+00|2024-07-02 03:04:05
2024-01-02 03:04:05 EST5EDT|2024-01-02 08:04:05+00|2024-01-02 03:04:05
2024-07-02 03:04:05 EST5EDT|2024-07-02 07:04:05+00|2024-07-02 03:04:05
2024-01-02 03:04:05 CET|2024-01-02 02:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 MSK|2024-01-02 00:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 UTC|2024-01-02 03:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 GMT|2024-01-02 03:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 Z|2024-01-02 03:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 z|2024-01-02 03:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 EST|2024-01-02 08:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 AEST|2024-01-01 17:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 IST|2024-01-02 01:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 America/New_York|2024-01-02 08:04:05+00|2024-01-02 03:04:05
Jan 2 2024 03:04:05 PST|2024-01-02 11:04:05+00|2024-01-02 03:04:05
Tue Jan 02 03:04:05 2024 PST|2024-01-02 11:04:05+00|2024-01-02 03:04:05
01/02/2024 03:04:05 PST|2024-01-02 11:04:05+00|2024-01-02 03:04:05
02.01.2024 03:04:05 CET|2024-02-01 02:04:05+00|2024-02-01 03:04:05
J2451187 12:00|1999-01-08 12:00:00+00|1999-01-08 12:00:00
20240102 030405|2024-01-02 03:04:05+00|2024-01-02 03:04:05
20240102T030405|2024-01-02 03:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 pst|2024-01-02 11:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 PST8PDT|2024-01-02 11:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 EST5|2024-01-02 08:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 UTC+3|2024-01-02 06:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 CST6CDT|2024-01-02 09:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 MST7MDT|2024-01-02 10:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 XYZ|22007|22007
1999-01-08 04:05:06 +02 PST|22007|22007
2024-01-02 3pm|22007|22007
Mon 2024-01-01 03:04|22007|22007
2024-01-02 03:04:05 1999|22007|22007
Jan 2 2024|2024-01-02 00:00:00+00|2024-01-02 00:00:00
2 January 2024 10:00|2024-01-02 10:00:00+00|2024-01-02 10:00:00
J2451187|1999-01-08 00:00:00+00|1999-01-08 00:00:00
2024-01-02 03:04:05 AD|2024-01-02 03:04:05+00|2024-01-02 03:04:05
0044-03-15 BC|0044-03-15 00:00:00+00 BC|0044-03-15 00:00:00 BC
epoch|1970-01-01 00:00:00+00|1970-01-01 00:00:00
2024-01-02 03:04:05 GEST|2024-01-01 23:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 Foo/Bar|22023|22023
1900-07-02 03:04:05 EST5EDT|1900-07-02 07:04:05+00|1900-07-02 03:04:05
2024-01-02 03:04:05 Europe/Berlin|2024-01-02 02:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 europe/berlin|2024-01-02 02:04:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05+05:30|2024-01-01 21:34:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 -0530|2024-01-02 08:34:05+00|2024-01-02 03:04:05
2024-01-02 03:04:05 GMT+3|2024-01-02 06:04:05+00|2024-01-02 03:04:05";

#[test]
fn timestamp_input_with_and_without_zones() {
    let p = parser(DateOrder::Mdy);
    let session = SessionSettings {
        date_order: p.date_order,
        zone: SessionZone::Fixed(0),
    };
    for line in TIMESTAMPS.lines() {
        let [input, utc, local] = columns(line);
        assert_text(Timestamp::parse(input, &p), local, input);
        assert_text(TimestampTz::parse(input, &p), utc, input);
        assert_text(TimestampTz::parse_in_session(input, &session), utc, input);
    }
}

#[test]
fn session_abbreviations_take_precedence_over_default() {
    for line in [
        "UTC|1800-01-02 03:04:05 LMT|22007",
        "Asia/Kolkata|2024-01-02 03:04:05 IST|2024-01-02 03:04:05+05:30",
        "Asia/Kolkata|1800-01-02 03:04:05 LMT|1800-01-02 03:04:05+05:53:28",
        "America/Los_Angeles|2024-01-02 03:04:05 PST|2024-01-02 03:04:05-08",
        "America/Los_Angeles|2024-07-02 03:04:05 PDT|2024-07-02 03:04:05-07",
        "America/Los_Angeles|2024-01-02 03:04:05|2024-01-02 03:04:05-08",
        "America/Los_Angeles|1800-01-02 03:04:05 LMT|1800-01-02 03:04:05-07:52:58",
    ] {
        let [zone, input, expected] = columns(line);
        let zone = SessionZone::named(zone).unwrap();
        let session = SessionSettings {
            date_order: DateOrder::Mdy,
            zone,
        };
        let actual =
            TimestampTz::parse_in_session(input, &session).and_then(|v| v.format_in_zone(zone));
        assert_text(actual, expected, input);
    }
}

#[test]
fn time_input_accepts_fixed_abbreviations_and_dated_zones() {
    let p = parser(DateOrder::Mdy);
    for (input, expected) in [
        ("03:04:05 PST", "03:04:05-08"),
        ("03:04:05 CET", "03:04:05+01"),
        ("03:04:05 ab-5", "03:04:05+05"),
        ("03:04:05 a+5", "03:04:05-05"),
        ("03:04:05 EST5EDT", "22007"),
        ("2024-01-02 03:04:05 PST", "03:04:05-08"),
        ("2024-07-02 03:04:05 EST5EDT", "03:04:05-04"),
        ("03:04:05 XYZ", "22007"),
        // PostgreSQL 18.6: weekday words are invalid time input (22007).
        ("Mon 03:04:05+02", "22007"),
        ("03:04:05 Monday", "22007"),
        ("Mon 2024-01-02 03:04:05+02", "22007"),
    ] {
        assert_text(TimeTz::parse(input, &p), expected, input);
    }
    for (input, expected) in [
        ("2024-01-02 03:04:05 PST", "03:04:05"),
        ("2024-01-02 03:04:05 XYZ", "22007"),
        ("allballs", "00:00:00"),
        ("03:04:05 GEST", "03:04:05"),
        ("12:00:60", "12:01:00"),
        ("12:59:59.9999999 PM", "13:00:00"),
        ("11:59:59.9999999 PM", "24:00:00"),
        ("12:59:59.9999999 AM", "01:00:00"),
        ("Monday 03:04:05", "22007"),
        ("2024-01-02 Mon 03:04:05", "22007"),
        ("Tue Jan 02 2024 03:04:05", "22007"),
    ] {
        assert_text(Time::parse(input), expected, input);
    }
}

#[test]
fn iso_roundtrips_across_calendar_orders_and_offsets() {
    for order in [DateOrder::Mdy, DateOrder::Dmy, DateOrder::Ymd] {
        let p = parser(order);
        let zone = SessionZone::named("America/New_York").unwrap();
        let session = SessionSettings {
            date_order: order,
            zone,
        };
        for offset in [0, 3600, -19_800, 20_725] {
            let display_zone = SessionZone::fixed(offset).unwrap();
            for day in (-730_600_i32..100_000).step_by(3_113) {
                let date = Date::from_days(day).unwrap();
                let us =
                    i64::from(day) * USECS_PER_DAY + i64::from(day.rem_euclid(84_000)) * 1_000_001;
                let stamp = Timestamp::from_microseconds(us).unwrap();
                let instant = TimestampTz::from_microseconds(us).unwrap();
                let iso_with_offset = instant.format_in_zone(display_zone).unwrap();
                assert_eq!(Date::parse(&date.to_string(), &p), Ok(date));
                assert_eq!(Timestamp::parse(&stamp.to_string(), &p), Ok(stamp));
                assert_eq!(
                    TimestampTz::parse_in_session(&iso_with_offset, &session),
                    Ok(instant)
                );
            }
        }
    }
}

#[test]
fn timetz_uses_session_and_dynamic_abbreviations() {
    for (zone, input, expected) in [
        ("Asia/Kolkata", "03:04:05 IST", "03:04:05+05:30"),
        ("Asia/Kolkata", "03:04:05 LMT", "03:04:05+05:53:28"),
        ("America/Los_Angeles", "03:04:05 PST", "03:04:05-08"),
        ("UTC", "03:04:05 PST", "03:04:05-08"),
        ("UTC", "03:04:05 MSK", "03:04:05+03"),
        ("UTC", "2011-06-01 03:04:05 MSK", "03:04:05+04"),
    ] {
        let settings = SessionSettings {
            date_order: DateOrder::Mdy,
            zone: SessionZone::named(zone).unwrap(),
        };
        let transaction =
            TimestampTz::parse("2024-01-02 12:00+00", &ParserSettings::default()).unwrap();
        assert_text(
            TimeTz::parse_in_session(input, &settings, transaction.microseconds()),
            expected,
            input,
        );
    }
}

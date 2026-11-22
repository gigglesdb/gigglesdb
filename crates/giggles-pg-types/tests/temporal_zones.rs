use giggles_pg_types::temporal::*;
use std::process::Command;
fn p() -> ParserSettings {
    ParserSettings::default()
}
fn local(s: &str) -> Timestamp {
    Timestamp::parse(s, &p()).unwrap()
}

#[test]
fn datestyle_uses_historical_timezone_designations() {
    use DateOrder::{Dmy, Mdy};
    use DateStyle::{German, Iso, Postgres, Sql};
    let instants = [
        "2024-01-02 03:04:05.678+00",
        "0044-03-15 03:04:05+00 BC",
        "2024-07-04 12:00:00+00",
    ].map(|s| TimestampTz::parse(s, &p()).unwrap());
    for (name, style, order, expected) in [
        ("America/Los_Angeles", Iso, Mdy, [
            "2024-01-01 19:04:05.678-08", "0044-03-14 19:11:07-07:52:58 BC", "2024-07-04 05:00:00-07"]),
        ("America/Los_Angeles", Sql, Mdy, [
            "01/01/2024 19:04:05.678 PST", "03/14/0044 19:11:07 LMT BC", "07/04/2024 05:00:00 PDT"]),
        ("America/Los_Angeles", Sql, Dmy, [
            "01/01/2024 19:04:05.678 PST", "14/03/0044 19:11:07 LMT BC", "04/07/2024 05:00:00 PDT"]),
        ("America/Los_Angeles", Postgres, Mdy, [
            "Mon Jan 01 19:04:05.678 2024 PST", "Thu Mar 14 19:11:07 0044 LMT BC", "Thu Jul 04 05:00:00 2024 PDT"]),
        ("America/Los_Angeles", Postgres, Dmy, [
            "Mon 01 Jan 19:04:05.678 2024 PST", "Thu 14 Mar 19:11:07 0044 LMT BC", "Thu 04 Jul 05:00:00 2024 PDT"]),
        ("America/Los_Angeles", German, Dmy, [
            "01.01.2024 19:04:05.678 PST", "14.03.0044 19:11:07 LMT BC", "04.07.2024 05:00:00 PDT"]),
        ("Asia/Kolkata", Sql, Mdy, [
            "01/02/2024 08:34:05.678 IST", "03/15/0044 08:57:33 LMT BC", "07/04/2024 17:30:00 IST"]),
        ("America/Sao_Paulo", Sql, Mdy, [
            "01/02/2024 00:04:05.678 -03", "03/14/0044 23:57:37 LMT BC", "07/04/2024 09:00:00 -03"]),
        ("America/Sao_Paulo", Postgres, Mdy, [
            "Tue Jan 02 00:04:05.678 2024 -03", "Thu Mar 14 23:57:37 0044 LMT BC", "Thu Jul 04 09:00:00 2024 -03"]),
        ("Europe/Berlin", Sql, Mdy, [
            "01/02/2024 04:04:05.678 CET", "03/15/0044 03:57:33 LMT BC", "07/04/2024 14:00:00 CEST"]),
        ("Europe/Berlin", German, Dmy, [
            "02.01.2024 04:04:05.678 CET", "15.03.0044 03:57:33 LMT BC", "04.07.2024 14:00:00 CEST"]),
    ] {
        let zone = SessionZone::setting(name).unwrap().0;
        assert_eq!(instants.map(|instant| instant.format_with_style(zone, style, order).unwrap()),
            expected, "{name} {style:?} {order:?}");
        for special in ["infinity", "-infinity"] {
            let value = TimestampTz::parse(special, &p()).unwrap();
            assert_eq!(value.format_with_style(zone, style, order).unwrap(), special);
        }
    }
}

#[test]
fn datestyle_formats_fixed_offsets_and_named_standard_and_dst_zones() {
    use DateStyle::{German, Postgres, Sql};
    let instants = ["2024-01-02 03:04:05+00", "2024-07-02 03:04:05+00"]
        .map(|s| TimestampTz::parse(s, &p()).unwrap());
    let fixed = SessionZone::interval_setting(19_800).unwrap().1;
    for (name, style, expected) in [
        (fixed.as_str(), Sql, ["01/02/2024 08:34:05 +05:30", "07/02/2024 08:34:05 +05:30"]),
        (fixed.as_str(), Postgres, ["Tue Jan 02 08:34:05 2024 +05:30", "Tue Jul 02 08:34:05 2024 +05:30"]),
        (fixed.as_str(), German, ["02.01.2024 08:34:05 +05:30", "02.07.2024 08:34:05 +05:30"]),
        ("-7", Sql, ["01/01/2024 20:04:05 -07", "07/01/2024 20:04:05 -07"]),
        ("EST5EDT", Sql, ["01/01/2024 22:04:05 EST", "07/01/2024 23:04:05 EDT"]),
        ("Etc/GMT+3", Sql, ["01/02/2024 00:04:05 -03", "07/02/2024 00:04:05 -03"]),
        ("GMT", Sql, ["01/02/2024 03:04:05 GMT", "07/02/2024 03:04:05 GMT"]),
    ] {
        let zone = SessionZone::setting(name).unwrap().0;
        assert_eq!(instants.map(|instant| instant.format_with_style(zone, style, DateOrder::Mdy).unwrap()),
            expected, "{name} {style:?}");
    }
}
#[test]
fn explicit_context_and_limits() {
    assert_eq!(TZDATA_VERSION, "2026f");
    let z = SessionZone::named("America/New_York").unwrap();
    for (s, out) in [
        ("2024-03-10 02:30", "2024-03-10 07:30:00+00"),
        ("2024-11-03 01:30", "2024-11-03 06:30:00+00"),
    ] {
        assert_eq!(z.to_instant(local(s)).unwrap().to_string(), out);
    }
    assert!(z.to_instant(local("0001-01-01 00:00 BC")).is_ok());
    assert!(z.to_instant(local("2100-01-01 00:00")).is_ok());
    assert!(SessionZone::named(&"x".repeat(257)).is_err());
    assert!(SessionZone::named("America/Unknown").is_err());
    assert!(TimeTz::parse("12:00-053045", &p()).is_err());
    for (date, offset) in [("2024-01-01", -18000), ("2024-07-01", -14400)] {
        assert_eq!(
            TimeTz::in_zone_on_date(
                Time::parse("12:00").unwrap(),
                Date::parse(date, &p()).unwrap(),
                z
            )
            .unwrap()
            .offset_east(),
            offset
        );
    }
    let settings = SessionSettings {
        date_order: DateOrder::Ymd,
        zone: z,
    };
    let v = TimestampTz::parse_in_session("2024-11-03 01:30", &settings).unwrap();
    assert_eq!(v.format_in_zone(z).unwrap(), "2024-11-03 01:30:00-05");
    assert_eq!(
        TimestampTz::parse_in_session("2024-11-03 01:30 America/New_York", &settings).unwrap(),
        v
    );
    for s in ["infinity", "-infinity"] {
        assert_eq!(
            TimestampTz::parse_in_session(s, &settings)
                .unwrap()
                .format_in_zone(z)
                .unwrap(),
            s
        );
    }
}
#[test]
#[ignore = "requires owned pinned PG18 oracle dd68-temporal-zones-oracle"]
fn live_pg_zones() {
    for zone in [
        "America/Chicago",
        "America/New_York",
        "Australia/Lord_Howe",
        "Pacific/Apia",
        "UTC",
        "Europe/Moscow",
        "Europe/Dublin",
    ] {
        let z = SessionZone::named(zone).unwrap();
        for s in [
            "2024-03-10 02:30",
            "2024-11-03 01:30",
            "2024-04-07 01:45",
            "2024-10-06 02:15",
            "2011-12-30 12:00",
            "2014-10-26 01:30",
            "1880-01-01 12:00",
            "1969-12-31 23:59:59.999999",
            "2000-02-29 24:00",
            "2024-12-31 23:59:59.9999999",
        ] {
            let query = format!("set timezone='UTC'; select ('{s} {zone}'::timestamptz)::text;");
            let output = Command::new("docker")
                .args([
                    "exec",
                    "dd68-temporal-zones-oracle",
                    "psql",
                    "-U",
                    "postgres",
                    "-Atq",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    &query,
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let expected = String::from_utf8(output.stdout).unwrap().trim().to_owned();
            let actual = z.to_instant(local(s)).unwrap();
            assert_eq!(actual.to_string(), expected, "{s} {zone}");
            // Instant -> local -> output, also independently checked against PG.
            let query = format!("set timezone='{zone}'; select ('{expected}'::timestamptz)::text;");
            let output = Command::new("docker")
                .args([
                    "exec",
                    "dd68-temporal-zones-oracle",
                    "psql",
                    "-U",
                    "postgres",
                    "-Atq",
                    "-c",
                    &query,
                ])
                .output()
                .unwrap();
            assert_eq!(
                actual.format_in_zone(z).unwrap(),
                String::from_utf8(output.stdout).unwrap().trim(),
                "format {s} {zone}"
            );
        }
    }
}

#[test]
fn grammar_and_precision() {
    assert_eq!(Time::parse("12:01 AM").unwrap().to_string(), "00:01:00");
    assert_eq!(Time::parse("01:01 pm").unwrap().to_string(), "13:01:00");
    assert_eq!(
        TimeTz::parse("12:00-0530", &p()).unwrap().offset_east(),
        -19800
    );
    assert_eq!(
        Interval::parse("@ 1 year -2 days 03:00 ago")
            .unwrap()
            .months(),
        -12
    );
    assert_eq!(
        Time::parse("23:59:59.999999")
            .unwrap()
            .with_precision(0)
            .unwrap()
            .microseconds(),
        USECS_PER_DAY
    );
    for v in [-1500000, -500000, 500000, 1500000] {
        assert_eq!(
            Timestamp::from_microseconds(v)
                .unwrap()
                .with_precision(0)
                .unwrap()
                .microseconds(),
            v.signum() * ((v.abs() + 500000) / 1000000) * 1000000
        );
    }
}
#[test]
#[ignore = "requires owned pinned PG18 oracle dd68-temporal-zones-oracle"]
fn live_pg_grammar_precision_and_timetz() {
    let sql = |query: &str| {
        let o = Command::new("docker")
            .args([
                "exec",
                "dd68-temporal-zones-oracle",
                "psql",
                "-U",
                "postgres",
                "-Atq",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                query,
            ])
            .output()
            .unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        String::from_utf8(o.stdout).unwrap().trim().to_owned()
    };
    assert_eq!(sql("show server_version_num"), "180000");
    for s in ["12:01 AM", "01:01 pm", "12:59:59.9999999 PM"] {
        assert_eq!(
            Time::parse(s).unwrap().to_string(),
            sql(&format!("select '{s}'::time::text"))
        );
    }
    for s in ["12:00-0530", "12:00+1030"] {
        assert_eq!(
            TimeTz::parse(s, &p()).unwrap().to_string(),
            sql(&format!("select '{s}'::timetz::text"))
        );
    }
    for s in ["@ 1 year -2 days 03:00 ago", "1 day -02:00 ago"] {
        assert_eq!(
            Interval::parse(s).unwrap().to_string(),
            sql(&format!("select '{s}'::interval::text"))
        );
    }
    for s in [
        "1999-12-31 23:59:59.5",
        "2000-01-01 00:00:00.5",
        "2024-12-31 23:59:59.999999",
    ] {
        for precision in 0..=6 {
            assert_eq!(
                local(s).with_precision(precision).unwrap().to_string(),
                sql(&format!("select '{s}'::timestamp({precision})::text"))
            );
        }
    }
    for zone in ["America/New_York", "Australia/Lord_Howe"] {
        for date in ["2024-01-01", "2024-07-01"] {
            let actual = TimeTz::in_zone_on_date(
                Time::parse("12:00").unwrap(),
                Date::parse(date, &p()).unwrap(),
                SessionZone::named(zone).unwrap(),
            )
            .unwrap();
            assert_eq!(
                actual.to_string(),
                sql(&format!("select '{date} 12:00 {zone}'::timetz::text"))
            );
        }
    }
}
#[test]
fn adversarial_temporal_images() {
    for b in [i64::MIN + 1, MIN_TIMESTAMP - 1, END_TIMESTAMP, i64::MAX - 1] {
        assert!(Timestamp::from_binary(&b.to_be_bytes()).is_err());
        assert!(TimestampTz::from_binary(&b.to_be_bytes()).is_err());
    }
    let mut b = [0u8; 12];
    b[8..].copy_from_slice(&i32::MIN.to_be_bytes());
    assert!(TimeTz::from_binary(&b).is_err());
    for s in [
        "2001-02-29 00:00",
        "2000-01-01 24:00:01",
        "2000-01-01 12:00 America/Invalid",
        "2000-01-01 12:00 UTC garbage",
    ] {
        let settings = SessionSettings {
            date_order: DateOrder::Mdy,
            zone: SessionZone::named("America/Chicago").unwrap(),
        };
        assert!(TimestampTz::parse_in_session(s, &settings).is_err());
    }
    for order in [DateOrder::Ymd, DateOrder::Mdy, DateOrder::Dmy] {
        let settings = ParserSettings {
            date_order: order,
            offset_east: 0,
        };
        for s in ["0001-01-01 BC", "2000-02-29", "5874897-12-31"] {
            let v = Date::parse(s, &settings).unwrap();
            assert_eq!(Date::parse(&v.to_string(), &settings).unwrap(), v);
        }
    }
}

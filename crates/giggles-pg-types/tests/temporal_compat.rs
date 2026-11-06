use giggles_pg_types::temporal::*;
use std::process::Command;
fn pg(query: &str) -> String {
    let o = Command::new("docker")
        .args([
            "exec",
            "dd68-temporal-compat-oracle",
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
    String::from_utf8(o.stdout).unwrap().trim().into()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
#[test]
fn exact_interval_forms_and_identity() {
    for (s, m, d, u) in [
        ("P1Y2M3DT4H5M6.123456S", 14, 3, 14_706_123_456),
        ("1.5 mons", 1, 15, 0),
        ("0.0000015 seconds", 0, 0, 1),
        ("1.5 weeks", 0, 10, 43_200_000_000),
        ("-1-2", -14, 0, 0),
        ("1.1 years", 13, 0, 0),
        ("-0.5 days", 0, 0, -43_200_000_000),
    ] {
        let v = Interval::parse(s).unwrap();
        assert!(v.same_components(Interval::new(m, d, u)), "{s}: {v:?}");
    }
    assert_eq!(Interval::new(1, 0, 0), Interval::new(0, 30, 0));
    assert!(!Interval::new(1, 0, 0).same_components(Interval::new(0, 30, 0)));
    for s in ["P", "PT", "P1Q", "1-12", "NaN seconds", "infinity seconds"] {
        assert!(Interval::parse(s).is_err(), "{s}");
    }
    for p in 0..=6 {
        assert!(
            Interval::INFINITY
                .with_precision(p)
                .unwrap()
                .same_components(Interval::INFINITY)
        );
    }
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_full_range_zones() {
    assert_eq!(pg("show server_version_num"), "180000");
    let names = pg(
        "select name from pg_timezone_names where name not like 'posix/%' and name not like 'right/%' order by name",
    );
    let p = ParserSettings::default();
    let mut cases = Vec::new();
    for name in names.lines() {
        if let Ok(zone) = SessionZone::named(name) {
            for year in [
                "0001", "1800", "1883", "1900", "1945", "1970", "2038", "2099", "2100", "2400",
                "10000", "294000",
            ] {
                for month in ["01", "07"] {
                    let s = format!("{year}-{month}-15 12:34:56.123456");
                    cases.push((s, name.to_string(), zone));
                }
            }
            cases.push((
                "0001-01-15 12:34:56.123456 BC".into(),
                name.to_string(),
                zone,
            ));
        }
    }
    // Batch SQL stays below Windows command-line limit.
    for batch in cases.chunks(80) {
        let values = batch
            .iter()
            .map(|(s, n, _)| format!("('{s}'::timestamp,'{n}')"))
            .collect::<Vec<_>>()
            .join(",");
        let rows = pg(&format!(
            "set timezone='UTC'; select (t at time zone z)::text,encode(timestamptz_send(t at time zone z),'hex'),((t at time zone z) at time zone z)::text from (values {values}) v(t,z)"
        ));
        for ((s, name, zone), row) in batch.iter().zip(rows.lines()) {
            let fields: Vec<_> = row.split('|').collect();
            let local = Timestamp::parse(s, &p).unwrap();
            let instant = zone.to_instant(local).unwrap();
            assert_eq!(instant.to_string(), fields[0], "{name} {s}");
            assert_eq!(hex(&instant.to_binary()), fields[1], "{name} {s}");
            assert_eq!(
                zone.to_local(instant).unwrap().to_string(),
                fields[2],
                "{name} {s}"
            );
        }
    }
    eprintln!("qualified {} local/UTC/text/binary zone cases", cases.len());
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_intervals_dates_and_precision() {
    for s in [
        "P1Y2M3DT4H5M6.123456S",
        "P-1Y-2M3DT-4H5M-6S",
        "1.5 mons",
        "1.5 weeks",
        "-1-2",
        "1.1 years",
        "1.9 years",
        "0.1 mon",
        "-0.5 days",
        "1.25 hours",
        "1.125 minutes",
    ] {
        let v = Interval::parse(s).unwrap();
        let row = pg(&format!(
            "select '{s}'::interval::text,encode(interval_send('{s}'::interval),'hex')"
        ));
        let fields: Vec<_> = row.split('|').collect();
        assert_eq!(v.to_string(), fields[0], "{s}");
        assert_eq!(hex(&v.to_binary()), fields[1], "{s}");
    }
    for (style, label) in [
        (DateStyle::Iso, "ISO"),
        (DateStyle::Sql, "SQL"),
        (DateStyle::Postgres, "Postgres"),
        (DateStyle::German, "German"),
    ] {
        for (order, o) in [
            (DateOrder::Mdy, "MDY"),
            (DateOrder::Dmy, "DMY"),
            (DateOrder::Ymd, "YMD"),
        ] {
            for s in [
                "0001-02-03 BC",
                "2000-02-29",
                "5874897-12-31",
                "infinity",
                "-infinity",
            ] {
                let v = Date::parse(s, &ParserSettings::default()).unwrap();
                assert_eq!(
                    v.format_with_style(style, order),
                    pg(&format!(
                        "set datestyle='{label},{o}'; select '{s}'::date::text"
                    )),
                    "{s} {label} {o}"
                );
            }
        }
    }
    for u in [
        -86_399_999_999,
        -1_500_000,
        -500_000,
        500_000,
        1_500_000,
        86_399_999_999,
    ] {
        for p in 0..=6 {
            let v = Interval::new(14, -3, u);
            assert_eq!(
                v.with_precision(p).unwrap().to_string(),
                pg(&format!("select '{}'::interval(6)::interval({p})::text", v))
            );
        }
    }
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_output_styles_and_transition_adversaries() {
    for (style, label) in [
        (IntervalOutputStyle::Postgres, "postgres"),
        (IntervalOutputStyle::Iso8601, "iso_8601"),
        (IntervalOutputStyle::SqlStandard, "sql_standard"),
        (IntervalOutputStyle::PostgresVerbose, "postgres_verbose"),
    ] {
        for m in [-14, -1, 0, 1, 14] {
            for d in [-3, 0, 3] {
                for u in [-14_706_123_456, 0, 14_706_123_456] {
                    let v = Interval::new(m, d, u);
                    assert_eq!(
                        v.format_with_style(style),
                        pg(&format!(
                            "set intervalstyle='{label}'; select '{}'::interval::text",
                            v
                        )),
                        "{style:?} {v:?}"
                    );
                }
            }
        }
    }
    for (name, date) in [
        ("Australia/Lord_Howe", "2024-10-06 02:15"),
        ("Australia/Lord_Howe", "2024-04-07 01:45"),
        ("Pacific/Apia", "2011-12-30 12:00"),
        ("Europe/Moscow", "2014-10-26 01:30"),
        ("America/New_York", "2100-03-14 02:30"),
        ("America/New_York", "2100-11-07 01:30"),
    ] {
        let z = SessionZone::named(name).unwrap();
        let v = Timestamp::parse(date, &ParserSettings::default()).unwrap();
        assert_eq!(
            z.to_instant(v).unwrap().to_string(),
            pg(&format!(
                "set timezone='UTC';select ('{date}'::timestamp at time zone '{name}')::text"
            )),
            "{name} {date}"
        );
    }
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_interval_typmod_fields() {
    for (field, label) in [
        (IntervalField::Year, "year"),
        (IntervalField::Month, "month"),
        (IntervalField::Day, "day"),
        (IntervalField::Hour, "hour"),
        (IntervalField::Minute, "minute"),
        (IntervalField::Second, "second"),
    ] {
        for s in [
            "1 year 2 mons -3 days 04:05:06.999999",
            "-1 years -2 mons 3 days -04:05:06.999999",
        ] {
            let v = Interval::parse(s).unwrap();
            let ty = if label == "second" {
                "second(3)"
            } else {
                label
            };
            assert_eq!(
                v.with_fields(field, 3).unwrap().to_string(),
                pg(&format!("select '{s}'::interval {ty}::text")),
                "{s} {label}"
            );
        }
    }
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_time_carry_errors_and_order() {
    for s in [
        "00:00 AM",
        "00:00 PM",
        "12:59:59.9999999 PM",
        "11:59:59.9999999 PM",
        "23:59:59.9999999",
        "24:00:00.0000001",
        "24:00:00.0000005",
    ] {
        let v = Time::parse(s).unwrap();
        assert_eq!(
            v.to_string(),
            pg(&format!("select '{s}'::time::text")),
            "{s}"
        );
    }
    for s in [
        "13:00 AM",
        "24:00 AM",
        "24:00:00.0000006",
        "25:00",
        "12:60",
        "-01:00",
    ] {
        assert!(Time::parse(s).is_err(), "{s}");
        assert_eq!(
            pg(&format!("select pg_input_is_valid('{s}','time')")),
            "f",
            "{s}"
        );
    }
    for (a, b) in [
        (Interval::new(1, 0, 0), Interval::new(0, 30, 0)),
        (Interval::new(0, 1, 0), Interval::new(0, 0, 86_400_000_000)),
        (Interval::NEG_INFINITY, Interval::new(i32::MIN, 0, 0)),
    ] {
        let expected = match a.cmp(&b) {
            std::cmp::Ordering::Less => "-1",
            std::cmp::Ordering::Equal => "0",
            std::cmp::Ordering::Greater => "1",
        };
        assert_eq!(
            expected,
            pg(&format!(
                "select case when '{}'::interval < '{}'::interval then -1 when '{}'::interval = '{}'::interval then 0 else 1 end",
                a, b, a, b
            ))
        );
    }
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_interval_duplicate_fields() {
    for s in [
        "1 day 2 days",
        "1 hour 02:00",
        "1 mon 2 months",
        "1 year 2 years",
        "1.5 seconds 2 microseconds",
    ] {
        assert!(Interval::parse(s).is_err(), "{s}");
        assert_eq!(
            pg(&format!("select pg_input_is_valid('{s}','interval')")),
            "f",
            "{s}"
        );
    }
    for s in [
        "P1Y2Y",
        "P1M1Y",
        "P1W2D",
        "PT1H1H",
        "1 week 2 days",
        "1.5 years 1 mon",
        "1 second 2 microseconds",
    ] {
        let v = Interval::parse(s).unwrap();
        assert_eq!(
            v.to_string(),
            pg(&format!("select '{s}'::interval::text")),
            "{s}"
        );
    }
}

#[test]
fn grammar_continuation_components() {
    for s in ["P0001-02-03T04:05:06", "P00010203T040506", "1-2 3 04:05:06"] {
        assert!(
            Interval::parse(s)
                .unwrap()
                .same_components(Interval::new(14, 3, 14_706_000_000))
        );
    }
    assert!(
        Interval::parse_with_style("-1-2 3 04:05:06", IntervalStyle::SqlStandard)
            .unwrap()
            .same_components(Interval::new(-14, -3, -14_706_000_000))
    );
    for s in [
        "P",
        "PT",
        "P0001-02-03Q04:05:06",
        "1 day 2 03:04:05",
        "2147483648 01:00:00",
    ] {
        assert!(Interval::parse(s).is_err(), "{s}");
    }
    assert!(SessionZone::named("aMeRiCa/nEw_yOrK").is_ok());
    assert!(SessionZone::named("America/No_Such_Zone").is_err());
}
#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_grammar_continuation() {
    for style in [IntervalStyle::Postgres, IntervalStyle::SqlStandard] {
        let setting = if style == IntervalStyle::SqlStandard {
            "sql_standard"
        } else {
            "postgres"
        };
        for s in [
            "1 02:03:04",
            "-1 02:03:04",
            "1-2 3 04:05:06",
            "-1-2 3 04:05:06",
            "-1-2 +3 04:05:06",
            "P0001-02-03T04:05:06",
            "P00010203T040506",
            "P0000-99-99T99:99:99",
            "2147483647 00:00:00",
        ] {
            let v = Interval::parse_with_style(s, style).unwrap();
            assert_eq!(
                hex(&v.to_binary()),
                pg(&format!(
                    "set intervalstyle='{setting}'; select encode(interval_send('{s}'::interval),'hex')"
                )),
                "{s} {setting}"
            );
        }
    }
    let p = ParserSettings::default();
    for (style, label) in [
        (DateStyle::Iso, "ISO"),
        (DateStyle::Sql, "SQL"),
        (DateStyle::Postgres, "Postgres"),
        (DateStyle::German, "German"),
    ] {
        for (order, o) in [
            (DateOrder::Mdy, "MDY"),
            (DateOrder::Dmy, "DMY"),
            (DateOrder::Ymd, "YMD"),
        ] {
            for s in [
                "2024-02-03 04:05:06.123456",
                "0001-02-03 04:05:06 BC",
                "294276-12-31 23:59:59.999999",
                "infinity",
                "-infinity",
            ] {
                assert_eq!(
                    Timestamp::parse(s, &p)
                        .unwrap()
                        .format_with_style(style, order),
                    pg(&format!(
                        "set datestyle='{label},{o}'; select '{s}'::timestamp::text"
                    )),
                    "{s} {label} {o}"
                );
            }
            let t = TimeTz::parse("23:59:59.123456-05:30", &p).unwrap();
            assert_eq!(
                t.format_with_style(style, order),
                pg(&format!(
                    "set datestyle='{label},{o}'; select '23:59:59.123456-05:30'::timetz::text"
                ))
            );
        }
    }
    for name in ["aMeRiCa/nEw_yOrK", "aUsTrAlIa/lOrD_hOwE", "pAcIfIc/aPiA"] {
        let z = SessionZone::named(name).unwrap();
        for s in [
            "2100-03-14 01:59:59.999999",
            "2100-03-14 02:00:00",
            "2100-03-14 03:00:00",
            "2024-11-03 01:00:00",
            "2024-11-03 01:59:59.999999",
            "2011-12-30 00:00:00",
            "0001-01-01 00:00:00 BC",
            "294000-12-31 23:59:59.999999",
        ] {
            assert_eq!(
                hex(&z
                    .to_instant(Timestamp::parse(s, &p).unwrap())
                    .unwrap()
                    .to_binary()),
                pg(&format!(
                    "select encode(timestamptz_send('{s}'::timestamp at time zone '{name}'),'hex')"
                )),
                "{s} {name}"
            );
        }
    }
}

#[test]
#[ignore = "requires owned pinned PG18 compatibility oracle"]
fn pg_grammar_errors_and_endpoints() {
    for s in [
        "P0001-02-03Q04:05:06",
        "P00010203T040506junk",
        "1 day 2 03:04:05",
        "2147483648 01:00:00",
        "1 25:60:00",
    ] {
        let o = Command::new("docker")
            .args([
                "exec",
                "dd68-temporal-compat-oracle",
                "psql",
                "-U",
                "postgres",
                "-Atq",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                &format!("select '{s}'::interval"),
            ])
            .output()
            .unwrap();
        assert!(!o.status.success(), "PG accepted {s}");
        assert!(Interval::parse(s).is_err(), "local accepted {s}");
    }
    let p = ParserSettings::default();
    for (name, s) in [
        ("America/New_York", "4714-11-24 00:00:00 BC"),
        ("Asia/Tokyo", "294276-12-31 23:59:59.999999"),
    ] {
        let z = SessionZone::named(name).unwrap();
        let v = z.to_instant(Timestamp::parse(s, &p).unwrap()).unwrap();
        assert_eq!(
            hex(&v.to_binary()),
            pg(&format!(
                "select encode(timestamptz_send('{s}'::timestamp at time zone '{name}'),'hex')"
            ))
        );
    }
}

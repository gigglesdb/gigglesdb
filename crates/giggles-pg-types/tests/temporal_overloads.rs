use giggles_pg_types::temporal::*;
#[test]
fn deterministic_input_and_zone_overloads() {
    let p = ParserSettings::default();
    for s in ["2000-01-01", "0001-01-01 BC", "4713-01-01 BC"] {
        let t = Timestamp::parse(s, &p).unwrap();
        assert_eq!(
            t,
            Timestamp::parse(
                &format!(
                    "{} 00:00:00{}",
                    s.strip_suffix(" BC").unwrap_or(s),
                    if s.ends_with(" BC") { " BC" } else { "" }
                ),
                &p
            )
            .unwrap()
        );
        assert_eq!(
            TimestampTz::parse(s, &p).unwrap().microseconds(),
            t.microseconds()
        );
    }
    assert_eq!(
        Time::parse("12:34:56-15:59").unwrap().to_string(),
        "12:34:56"
    );
    for s in ["12:34:56-16", "12:34:56+01:60", "12:34:56garbage"] {
        assert!(Time::parse(s).is_err());
    }
    let settings = SessionSettings {
        date_order: DateOrder::default(),
        zone: SessionZone::Fixed(0),
    };
    for (local, expected) in [
        (
            "2024-03-10 02:30:00 America/New_York",
            "2024-03-10 07:30:00+00",
        ),
        (
            "2024-11-03 01:30:00 America/New_York",
            "2024-11-03 06:30:00+00",
        ),
    ] {
        assert_eq!(
            TimestampTz::parse_in_session(local, &settings)
                .unwrap()
                .to_string(),
            expected
        );
    }
    let t = Timestamp::parse("2000-01-01", &p).unwrap();
    let z = Interval::new(0, 0, 5_400_000_000);
    let instant = t.timezone_interval(z).unwrap();
    assert_eq!(instant.to_string(), "1999-12-31 22:30:00+00");
    assert_eq!(instant.timezone_interval(z).unwrap(), t);
    assert!(t.timezone_interval(Interval::new(0, 1, 0)).is_err());
    assert_eq!(
        Timestamp::INFINITY.timezone_interval(z).unwrap(),
        TimestampTz::INFINITY
    );
    assert_eq!(
        Timestamp::INFINITY
            .timezone_interval(Interval::new(1, 0, 0))
            .unwrap(),
        TimestampTz::INFINITY
    );
    let time = TimeTz::parse("01:00:00+02", &p).unwrap();
    assert_eq!(
        time.timezone_interval(z).unwrap().to_string(),
        "00:30:00+01:30"
    );
}

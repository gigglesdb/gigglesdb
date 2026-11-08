use giggles_pg_types::{
    Error,
    temporal::{
        ParserSettings, SessionSettings, SessionZone, Time, TimeTz, Timestamp, TimestampTz,
    },
};

fn assert_parsers_error(time: &str, error: Error) {
    let settings = ParserSettings::default();
    let session = SessionSettings {
        date_order: Default::default(),
        zone: SessionZone::Fixed(0),
    };
    let timestamp = format!("2024-01-01 {time}");
    assert_eq!(Time::parse(time), Err(error.clone()), "time: {time}");
    assert_eq!(
        TimeTz::parse(time, &settings),
        Err(error.clone()),
        "timetz: {time}"
    );
    assert_eq!(
        Timestamp::parse(&timestamp, &settings),
        Err(error.clone()),
        "timestamp: {timestamp}"
    );
    assert_eq!(
        TimestampTz::parse(&timestamp, &settings),
        Err(error.clone()),
        "timestamptz: {timestamp}"
    );
    assert_eq!(
        TimestampTz::parse_in_session(&timestamp, &session),
        Err(error),
        "session timestamptz: {timestamp}"
    );
}

#[test]
fn explicit_numeric_offset_field_bounds() {
    for time in [
        "12:00+16",
        "12:00-16",
        "12:00+16:00",
        "12:00+1600",
        "12:00 +16",
        "12:00+15:60",
        "12:00+05:60",
        "12:00+5:30:60",
    ] {
        assert_parsers_error(time, Error::TimeZoneDisplacement);
    }
}

#[test]
fn maximum_valid_numeric_offsets_are_accepted() {
    let settings = ParserSettings::default();
    let session = SessionSettings {
        date_order: Default::default(),
        zone: SessionZone::Fixed(0),
    };
    for offset in ["+15:59", "+15:59:59", "-15:59:59"] {
        let time = format!("12:00{offset}");
        let timestamp = format!("2024-01-01 {time}");
        assert_eq!(Time::parse(&time), Time::parse("12:00"), "{time}");
        assert!(TimeTz::parse(&time, &settings).is_ok(), "{time}");
        assert!(
            Timestamp::parse(&timestamp, &settings).is_ok(),
            "{timestamp}"
        );
        assert!(
            TimestampTz::parse(&timestamp, &settings).is_ok(),
            "{timestamp}"
        );
        assert!(
            TimestampTz::parse_in_session(&timestamp, &session).is_ok(),
            "{timestamp}"
        );
    }
}

#[test]
fn negative_fields_and_extra_fields_remain_range() {
    // The first '+' is the offset sign; the following '-' makes the hour negative.
    for time in [
        "12:00+-5",
        "12:00+5:-1",
        "12:00+5:30:-1",
        "12:00+5:30:00:00",
        "12:00+16:-1",
        "12:00+16:00:00:00",
    ] {
        assert_parsers_error(time, Error::Range);
    }
}
